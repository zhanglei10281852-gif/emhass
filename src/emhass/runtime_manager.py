"""
Runtime configuration activation boundary.

This module owns three things that must change together when configuration is
saved while the service runs:

1. The persisted configuration files (secrets / config.json / params.pkl),
   staged and committed so a crash mid-switch can only ever restore the last
   fully activated configuration (``ActivationStore``).
2. The single continual-publish background task and its RetrieveHass
   (``PublishManager``).
3. The in-process active configuration snapshot (``RuntimeManager``), which
   orchestrates validation, staging, Home Assistant connection preparation,
   publish handover and the atomic commit.

Only :class:`RuntimeManager` is needed by the web layer; the other classes are
split out for testing and keep each failure/resource path explicit.
"""

import asyncio
import copy
import dataclasses
import logging
import os
import pathlib
import pickle
import shutil
import uuid

import aiofiles
import orjson
import yaml

from emhass import connection_manager, utils
from emhass.command_line import continual_publish
from emhass.retrieve_hass import RetrieveHass

#: Bounded wait for a cancelled publish task to finish its current cycle.
PUBLISH_STOP_TIMEOUT = 10.0

#: Directory (under data_path) holding the pending-activation marker and
#: rollback copies. Temp files themselves sit next to their destination so the
#: final promotion is an intra-directory ``os.replace`` (works across add-on
#: mounts, unlike cross-device renames).
ACTIVATION_DIR_NAME = ".activation"
PENDING_FILE_NAME = "pending.json"


def promote_file(src: pathlib.Path, dest: pathlib.Path) -> None:
    """
    Move ``src`` onto ``dest`` across filesystems.

    Rollback copies live under data_path while destinations may be /app and
    /share mounts: a direct ``os.replace`` can fail with EXDEV. Copy to a
    sibling temp name first, then atomically replace inside dest's directory.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / f"{dest.name}.restore.{uuid.uuid4().hex}"
    try:
        shutil.copyfile(src, tmp)
        os.replace(tmp, dest)
    except BaseException:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


class ActivationError(Exception):
    """Base class for activation failures; the old configuration stays active."""


class ActivationValidationError(ActivationError):
    """Candidate configuration could not be built/validated (nothing written)."""


class ActivationConnectionError(ActivationError):
    """The new Home Assistant connection could not be established."""


class PublishStopError(ActivationError):
    """The previous continual-publish task did not stop within the timeout."""


class ActivationCommitError(ActivationError):
    """Committing staged files failed; best-effort rollback was attempted."""


class ActivationStore:
    """
    Stage/commit/recover configuration files as one recoverable transaction.

    Lifecycle::

        tx = await store.stage(specs)      # backup + temp writes + pending marker
        ...prepare runtime resources...    # nothing active is touched yet
        await tx.commit()                  # atomic promotions, clear marker
        # or:
        await tx.abort()                   # remove temps/marker, keep active files

    On process start, :meth:`recover_pending` restores the backed-up files
    whenever a marker from an interrupted switch is found.
    """

    def __init__(self, data_path: pathlib.Path, logger: logging.Logger | None = None):
        self.data_path = pathlib.Path(data_path)
        self.work_dir = self.data_path / ACTIVATION_DIR_NAME
        self.logger = logger or logging.getLogger(__name__)

    async def stage(self, file_specs: list[dict]) -> "ActivationTransaction":
        """
        Back up existing destinations and stage new bytes.

        Each spec: ``{"dest": Path, "name": str, "content": bytes}``.
        Existing files are byte-copied to the rollback area (cross-mount safe);
        new content is written to ``<dest>.tmp.<generation>`` next to the
        destination. A pending marker is written first so a crash at any point
        is recoverable on restart.
        """
        generation = uuid.uuid4().hex
        rollback_dir = self.work_dir / "rollback" / generation
        rollback_dir.mkdir(parents=True, exist_ok=True)
        records = []
        for spec in file_specs:
            dest = pathlib.Path(spec["dest"])
            records.append(
                {
                    "dest": str(dest),
                    "name": spec["name"],
                    "existed": dest.exists(),
                    "tmp": str(dest.parent / f"{dest.name}.tmp.{generation}"),
                    "rollback": str(rollback_dir / spec["name"]),
                }
            )
        # Marker first: recovery keys off this file, and entries describe the
        # whole intended set even if the process dies while staging later ones.
        await self._write_json_atomic(
            self.work_dir / PENDING_FILE_NAME,
            {"generation": generation, "files": records},
        )
        for spec, record in zip(file_specs, records, strict=True):
            dest = pathlib.Path(record["dest"])
            if record["existed"]:
                # Atomic backup placement: a crash mid-copy can never leave a
                # truncated rollback that recovery would copy over a good file.
                backup_tmp = f"{record['rollback']}.tmp"
                shutil.copyfile(dest, backup_tmp)
                os.replace(backup_tmp, record["rollback"])
            async with aiofiles.open(record["tmp"], "wb") as fid:
                await fid.write(spec["content"])
        return ActivationTransaction(self, generation, records)

    async def recover_pending(self) -> bool:
        """
        Restore files after an interrupted activation, if a marker exists.

        :return: True when a pending activation was found and restored.
        """
        pending_path = self.work_dir / PENDING_FILE_NAME
        if not pending_path.is_file():
            # No switch in flight: drop work-area leftovers but never touch
            # destination files.
            if self.work_dir.exists():
                shutil.rmtree(self.work_dir, ignore_errors=True)
            return False
        try:
            async with aiofiles.open(pending_path, "rb") as fid:
                pending = orjson.loads(await fid.read())
        except (OSError, orjson.JSONDecodeError) as exc:
            self.logger.warning(
                "Ignoring unreadable activation marker %s: %s", pending_path, exc
            )
            return False
        self.logger.warning(
            "Found interrupted configuration activation (%s); restoring previous files",
            pending.get("generation"),
        )
        failures = 0
        for record in pending.get("files", []):
            tmp_path = pathlib.Path(record["tmp"])
            if tmp_path.exists():
                tmp_path.unlink()
            dest = pathlib.Path(record["dest"])
            try:
                if record.get("existed"):
                    rollback = pathlib.Path(record["rollback"])
                    if rollback.is_file():
                        # Cross-mount safe: copy beside the destination first.
                        promote_file(rollback, dest)
                    else:
                        # Marker written but backup lost before any promotion:
                        # the destination is untouched; leave it in place.
                        self.logger.warning(
                            "Rollback copy missing for %s; leaving current file in place",
                            dest,
                        )
                elif dest.exists():
                    # The file did not exist at the last complete activation.
                    dest.unlink()
                # Remove leftovers from a recovery that itself crashed between
                # copyfile and the same-directory replace.
                for orphan in dest.parent.glob(f"{dest.name}.restore.*"):
                    try:
                        orphan.unlink()
                    except OSError:
                        self.logger.warning("Could not remove orphan restore temp %s", orphan)
            except OSError as exc:
                # Keep the marker so a next restart retries this file; never
                # continue silently with a half-restored generation.
                failures += 1
                self.logger.error("Failed to restore %s: %s", dest, exc)
        if failures:
            raise ActivationCommitError(
                f"{failures} configuration file(s) could not be restored; "
                "pending marker kept for retry"
            )
        shutil.rmtree(self.work_dir, ignore_errors=True)
        return True

    async def _write_json_atomic(self, dest: pathlib.Path, payload: dict) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.parent / f"{dest.name}.tmp.{uuid.uuid4().hex}"
        async with aiofiles.open(tmp, "wb") as fid:
            await fid.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        os.replace(tmp, dest)


class ActivationTransaction:
    """One staged set of file changes; commit or abort exactly once."""

    def __init__(
        self, store: ActivationStore, generation: str, records: list[dict]
    ) -> None:
        self._store = store
        self.generation = generation
        self._records = records
        self._finished = False

    async def commit(self) -> None:
        """Promote staged files in order, then clear the marker and backups."""
        if self._finished:
            return
        try:
            for record in self._records:
                os.replace(record["tmp"], record["dest"])
        except OSError as exc:
            # Best-effort in-process restore to the previous complete state.
            self._rollback_now()
            self._finished = True
            raise ActivationCommitError(
                f"Failed to commit configuration file {record['dest']}: {exc}"
            ) from exc
        self._cleanup()
        self._finished = True

    async def abort(self) -> None:
        """Discard staged content; destination files were never modified."""
        if self._finished:
            return
        self._cleanup()
        self._finished = True

    def _rollback_now(self) -> None:
        for record in self._records:
            tmp_path = pathlib.Path(record["tmp"])
            if tmp_path.exists():
                tmp_path.unlink()
            dest = pathlib.Path(record["dest"])
            if record.get("existed"):
                rollback = pathlib.Path(record["rollback"])
                if rollback.is_file():
                    try:
                        # Cross-mount safe promotion (rollback lives in data_path).
                        promote_file(rollback, dest)
                    except OSError:
                        self._store.logger.warning(
                            "Rollback promote failed for %s", dest
                        )
            elif dest.exists():
                try:
                    dest.unlink()
                except OSError:
                    self._store.logger.warning("Rollback unlink failed for %s", dest)

    def _cleanup(self) -> None:
        for record in self._records:
            tmp_path = pathlib.Path(record["tmp"])
            if tmp_path.exists():
                tmp_path.unlink()
        # Everything for this transaction (and the now-empty work area) goes.
        shutil.rmtree(self._store.work_dir, ignore_errors=True)


def publish_is_enabled(params: dict) -> bool:
    """Read the continual_publish flag from built params."""
    return bool(params.get("retrieve_hass_conf", {}).get("continual_publish", False))


class PublishManager:
    """
    Sole owner of the continual-publish background task and its RetrieveHass.

    At most one task ever runs; each (re)start builds a fresh runner context
    from the active params so time zone, frequency and target rh always match
    the active configuration. The runner context intentionally exposes only
    the keys ``continual_publish`` / ``publish_json`` actually read
    (``rh`` and ``retrieve_hass_conf``).
    """

    def __init__(
        self,
        emhass_conf: dict,
        entity_path: pathlib.Path,
        logger: logging.Logger | None = None,
        stop_timeout: float = PUBLISH_STOP_TIMEOUT,
    ) -> None:
        self._emhass_conf = emhass_conf
        self._entity_path = pathlib.Path(entity_path)
        self.logger = logger or logging.getLogger(__name__)
        self.stop_timeout = stop_timeout
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._rh: RetrieveHass | None = None
        self._params: dict | None = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def current_rh(self) -> RetrieveHass | None:
        return self._rh if self.is_running else None

    @property
    def current_params(self) -> dict | None:
        return self._params if self.is_running else None

    def _build_context(self, params: dict) -> tuple[dict, RetrieveHass]:
        # get_yaml_parse mutates timestamps/timezone in place: parse a copy so
        # the shared active params is never mutated and parsing runs exactly
        # once per runner build (a second call would fail on Timedelta args).
        retrieve_hass_conf, _, _ = utils.get_yaml_parse(copy.deepcopy(params), self.logger)
        rh = RetrieveHass(
            retrieve_hass_conf["hass_url"],
            retrieve_hass_conf["long_lived_token"],
            retrieve_hass_conf["optimization_time_step"],
            retrieve_hass_conf["time_zone"],
            params,
            self._emhass_conf,
            self.logger,
        )
        return {"rh": rh, "retrieve_hass_conf": retrieve_hass_conf}, rh

    async def _run(self, context: dict, rh: RetrieveHass) -> None:
        try:
            await continual_publish(context, self._entity_path, self.logger)
        except asyncio.CancelledError:
            self.logger.info("Continual publish task stopped")
            raise
        except Exception:
            # The loop itself already retries per cycle; reaching here means it
            # exited unexpectedly: log so it is visible in action logs.
            self.logger.exception("Continual publish task exited unexpectedly")
        finally:
            await rh.close()

    async def start(self, params: dict) -> None:
        """Start the runner with active params; caller must stop any old one."""
        async with self._lock:
            if self.is_running:
                raise RuntimeError("Continual publish task is already running")
            context, rh = self._build_context(params)
            self._rh = rh
            self._params = params
            self._task = asyncio.create_task(
                self._run(context, rh), name="emhass-continual-publish"
            )
            self.logger.info("Continual publish task started")

    async def stop(self, timeout: float | None = None) -> None:
        """
        Cancel the running task and wait for it to finish within the timeout.

        :raises PublishStopError: the task does not finish in time; it is left
            alive and no new task must be started.
        """
        async with self._lock:
            task = self._task
            if task is None or task.done():
                self._task = None
                self._rh = None
                self._params = None
                return
            timeout = self.stop_timeout if timeout is None else timeout
            # asyncio.wait (not wait_for): a task that ignores cancellation must
            # remain alive and be reported, instead of blocking the switch.
            task.cancel()
            done, _pending = await asyncio.wait((task,), timeout=timeout)
            if not done:
                raise PublishStopError(
                    "Previous continual publish task did not stop within "
                    f"{timeout}s; keeping the old task in service"
                )
            # Retrieve a non-cancellation exception so it is not logged as an
            # unretrieved task error.
            if not task.cancelled() and (exc := task.exception()) is not None:
                self.logger.warning("Continual publish task exited with error: %s", exc)
            self._task = None
            self._rh = None
            self._params = None

    async def start_if_enabled(self, params: dict) -> None:
        """Bootstrap path: start only when the active config enables it."""
        if not self.is_running and publish_is_enabled(params):
            await self.start(params)

    async def shutdown(self) -> None:
        """Service-stop path: cancel, wait briefly, close rh regardless."""
        async with self._lock:
            if not self.is_running:
                return
            task = self._task
            rh = self._rh
            task.cancel()
            done, _pending = await asyncio.wait((task,), timeout=self.stop_timeout)
            if not done:
                self.logger.warning(
                    "Continual publish task did not finish during shutdown"
                )
            elif not task.cancelled() and (exc := task.exception()) is not None:
                self.logger.warning("Continual publish task exited with error: %s", exc)
            if rh is not None:
                await rh.close()
            self._task = None
            self._rh = None
            self._params = None


@dataclasses.dataclass
class ActiveConfig:
    """The one configuration generation currently serving requests."""

    params: dict
    # Full in-memory secret set used to build params (ENV/options/yaml merged).
    secrets: dict
    config: dict
    # Exactly the content persisted in secrets_emhass.yaml (file-scoped keys).
    # Tracked from boot so a post-commit rollback can restore (or delete, when
    # the file did not exist before the first secrets activation) it.
    secrets_file: dict | None = None
    secrets_file_existed: bool = True


def _identity(rh_conf: dict) -> tuple:
    return (rh_conf.get("hass_url"), (rh_conf.get("long_lived_token") or "").strip())


class RuntimeManager:
    """
    Serializes and owns configuration activations.

    ``activate`` is the single boundary: validate -> stage (recoverable) ->
    prepare Home Assistant connection -> stop old publisher -> commit files ->
    swap in-memory state/WS generation -> start new publisher. A failure before
    the swap leaves the previous ActiveConfig fully in service.
    """

    def __init__(
        self,
        emhass_conf: dict,
        entity_path: pathlib.Path,
        logger: logging.Logger | None = None,
        on_secrets_activated=None,
    ) -> None:
        self._emhass_conf = emhass_conf
        self.logger = logger or logging.getLogger(__name__)
        self._on_secrets_activated = on_secrets_activated
        self.store = ActivationStore(emhass_conf["data_path"], self.logger)
        self.publish = PublishManager(emhass_conf, entity_path, self.logger)
        self._lock = asyncio.Lock()
        self.active: ActiveConfig | None = None

    # -- lifecycle hooks used by web_server startup/shutdown ---------------

    async def startup_recover(self) -> bool:
        """Restore files interrupted mid-switch. Call before building config."""
        return await self.store.recover_pending()

    def set_active(
        self,
        params: dict,
        secrets: dict,
        config: dict,
        secrets_file: dict | None = None,
        secrets_file_existed: bool = True,
    ) -> None:
        self.active = ActiveConfig(
            params=params,
            secrets=secrets,
            config=config,
            secrets_file=secrets_file,
            secrets_file_existed=secrets_file_existed,
        )

    async def bootstrap_publish(self) -> None:
        """Start continual publishing if the restored active config asks for it."""
        if self.active is not None:
            await self.publish.start_if_enabled(self.active.params)

    async def shutdown(self) -> None:
        await self.publish.shutdown()

    # -- activation ----------------------------------------------------------

    async def activate(
        self,
        config: dict | None = None,
        secrets_patch: dict | None = None,
        secrets_file: dict | None = None,
    ) -> ActiveConfig:
        """
        Activate a new configuration generation.

        :param config: Full config.json content (``/set-config``); None leaves
            the active config file untouched (``/configuration`` secrets save).
        :param secrets_patch: Secret keys to update; None leaves secrets file.
        :param secrets_file: The exact dict to persist to secrets_emhass.yaml
            (file-scoped keys only). Required when secrets_patch is provided;
            the in-memory candidate secrets still includes ENV/options values.
        :raises ActivationError: any failure; the old generation stays active.
        """
        async with self._lock:
            return await self._activate_locked(config, secrets_patch, secrets_file)

    async def _activate_locked(
        self,
        config: dict | None,
        secrets_patch: dict | None,
        secrets_file: dict | None,
    ) -> ActiveConfig:
        if self.active is None:
            raise ActivationValidationError("No active configuration to update")
        previous = self.active

        # 1. Build the candidate in memory (endpoint validations already ran).
        candidate_secrets = dict(previous.secrets)
        if secrets_patch:
            candidate_secrets.update(secrets_patch)
        candidate_config = config if config is not None else previous.config
        candidate_params = await utils.build_params(
            self._emhass_conf, candidate_secrets, candidate_config, self.logger
        )
        if type(candidate_params) is bool:
            raise ActivationValidationError("Failed to build params for candidate config")
        if secrets_patch and secrets_file is None:
            raise ActivationValidationError(
                "secrets_file content is required when updating secrets"
            )

        old_conf = previous.params.get("retrieve_hass_conf", {})
        new_conf = candidate_params.get("retrieve_hass_conf", {})
        identity_changed = _identity(new_conf) != _identity(old_conf)
        ws_was = bool(old_conf.get("use_websocket", False))
        ws_will = bool(new_conf.get("use_websocket", False))
        publish_was = publish_is_enabled(previous.params)
        publish_will = publish_is_enabled(candidate_params)
        tz_or_freq_changed = (
            old_conf.get("time_zone") != new_conf.get("time_zone")
            or old_conf.get("optimization_time_step")
            != new_conf.get("optimization_time_step")
        )
        publish_restart = publish_will and (
            not publish_was or identity_changed or tz_or_freq_changed
        )
        publish_stop_needed = publish_was and (not publish_will or publish_restart)
        # A WS generation is only prepared when the identity actually changes
        # or WS is newly enabled. Same-identity saves (incl. ordinary param
        # edits) must not make network calls; a dropped active connection is
        # re-established lazily by the next action.
        prepare_ws = ws_will and (identity_changed or not ws_was)

        # 2. Persist recoverably before touching any runtime resource.
        specs = self._build_file_specs(
            candidate_params, config, secrets_patch, secrets_file
        )
        try:
            tx = await self.store.stage(specs)
        except OSError as exc:
            raise ActivationCommitError(f"Could not stage configuration: {exc}") from exc

        # Steps 3-4 prepare resources non-destructively. Any failure (including
        # request cancellation) discards the candidate and clears staging, so
        # the previous generation keeps serving with no leaked connection.
        ws_holder = None
        try:
            # 3a. New WebSocket generation, connected but not yet active.
            if prepare_ws:
                try:
                    ws_holder = await connection_manager.prepare_client(
                        new_conf["hass_url"],
                        new_conf["long_lived_token"],
                        self.logger,
                    )
                except Exception as exc:
                    raise ActivationConnectionError(
                        f"Home Assistant WebSocket connection failed: {exc}"
                    ) from exc
            # 3b. REST mode with a new identity: one authenticated config probe
            # reusing the production REST path (local/no-token mode skips).
            elif ws_will is False and identity_changed:
                # Parse a copy: get_yaml_parse mutates timestamps in place and
                # the candidate params must stay pristine for commit/start.
                try:
                    parsed, _, _ = utils.get_yaml_parse(
                        copy.deepcopy(candidate_params), self.logger
                    )
                    probe_rh = RetrieveHass(
                        parsed["hass_url"],
                        parsed["long_lived_token"],
                        parsed["optimization_time_step"],
                        parsed["time_zone"],
                        candidate_params,
                        self._emhass_conf,
                        self.logger,
                    )
                except Exception as exc:
                    raise ActivationValidationError(
                        f"Could not build connection probe: {exc}"
                    ) from exc
                try:
                    reachable = await probe_rh.get_ha_config()
                finally:
                    await probe_rh.close()
                if not reachable:
                    raise ActivationConnectionError(
                        "Home Assistant is not reachable with the new URL/token"
                    )

            # 4. Stop the outgoing publisher before takeover. A task that
            # cannot be stopped aborts activation: no second task is started.
            if publish_stop_needed:
                await self.publish.stop()
        except BaseException:
            if ws_holder is not None:
                await connection_manager.discard_client(ws_holder)
            await tx.abort()
            raise

        # 5. Commit files; on disk failure restore immediately and keep old
        # runtime generation untouched.
        try:
            await tx.commit()
        except ActivationCommitError:
            await self._restore_previous(previous, publish_was)
            if ws_holder is not None:
                await connection_manager.discard_client(ws_holder)
            raise

        # 6. Swap the in-memory active state and the WS generation. A failure
        # here is post-commit: roll files and runtime state back together.
        try:
            self.active = ActiveConfig(
                params=candidate_params,
                secrets=candidate_secrets,
                config=candidate_config,
                # This activation carries explicit secrets content when
                # present; otherwise propagate the previous generation's
                # tracking so a later post-commit rollback can restore or
                # remove the yaml across an activation chain.
                secrets_file=(
                    dict(secrets_file)
                    if secrets_file is not None
                    else previous.secrets_file
                ),
                secrets_file_existed=(
                    True if secrets_file is not None else previous.secrets_file_existed
                ),
            )
            if self._on_secrets_activated is not None:
                self._on_secrets_activated(candidate_secrets)
            if prepare_ws:
                await connection_manager.activate_prepared(ws_holder)
            elif ws_was and not ws_will:
                await connection_manager.retire_active()

            # 7. Start the incoming publisher. Failure rolls the whole
            # activation back so file state and runtime state never diverge.
            if publish_restart:
                try:
                    await self.publish.start(candidate_params)
                except Exception as exc:
                    raise ActivationError(
                        f"Continual publish could not be started: {exc}"
                    ) from exc
        except BaseException:
            self.logger.exception("Activation takeover failed; restoring previous config")
            await self._restore_previous(previous, publish_was)
            if ws_holder is not None:
                await connection_manager.discard_client(ws_holder)
            raise

        self.logger.info(
            "Configuration activated (ws=%s, continual_publish=%s)",
            ws_will,
            publish_will,
        )
        return self.active

    # -- helpers -------------------------------------------------------------

    def _build_file_specs(
        self,
        candidate_params: dict,
        config: dict | None,
        secrets_patch: dict | None,
        secrets_file: dict | None,
    ) -> list[dict]:
        # Commit order: secrets -> config -> params. params.pkl is the last
        # promoted, so every non-terminal crash window has params matching the
        # older generation; the pending marker makes recovery order-independent
        # regardless.
        specs: list[dict] = []
        if secrets_patch is not None:
            secrets_path = self._emhass_conf.get(
                "secrets_path", pathlib.Path("/app/secrets_emhass.yaml")
            )
            # Persist only file-scoped secret keys, never ENV/options-derived
            # values: the file has higher precedence on every future boot.
            specs.append(
                {
                    "dest": secrets_path,
                    "name": "secrets_emhass.yaml",
                    "content": yaml.safe_dump(
                        secrets_file or {}, default_flow_style=False
                    ).encode(),
                }
            )
        if config is not None:
            specs.append(
                {
                    "dest": self._emhass_conf["config_path"],
                    "name": "config.json",
                    "content": orjson.dumps(config, option=orjson.OPT_INDENT_2),
                }
            )
        specs.append(
            {
                "dest": self._emhass_conf["data_path"] / "params.pkl",
                "name": "params.pkl",
                "content": pickle.dumps(
                    (self._emhass_conf["config_path"], candidate_params)
                ),
            }
        )
        return specs

    async def _restore_previous(self, previous: ActiveConfig, publish_was: bool) -> None:
        """Best-effort return to the previous generation after a late failure."""
        self.logger.warning("Activation failed after takeover; restoring previous config")
        try:
            restore_specs = []
            secrets_path = self._emhass_conf.get(
                "secrets_path", pathlib.Path("/app/secrets_emhass.yaml")
            )
            if previous.secrets_file_existed and previous.secrets_file is not None:
                # Restore the previous file-scoped content.
                restore_specs.append(
                    {
                        "dest": secrets_path,
                        "name": "secrets_emhass.yaml",
                        "content": yaml.safe_dump(
                            previous.secrets_file, default_flow_style=False
                        ).encode(),
                    }
                )
            elif not previous.secrets_file_existed:
                # No secrets file existed at the previous activation: anything
                # the failed activation created must be removed directly (the
                # consumed marker cannot delete it anymore).
                try:
                    pathlib.Path(secrets_path).unlink(missing_ok=True)
                except OSError:
                    self.logger.exception("Could not remove restored secrets file")
            # existed=True with no parseable tracked content: leave in place
            # rather than guessing (the disk file predates the activation).
            restore_specs.extend(
                [
                    {
                        "dest": self._emhass_conf["config_path"],
                        "name": "config.json",
                        "content": orjson.dumps(
                            previous.config, option=orjson.OPT_INDENT_2
                        ),
                    },
                    {
                        "dest": self._emhass_conf["data_path"] / "params.pkl",
                        "name": "params.pkl",
                        "content": pickle.dumps(
                            (self._emhass_conf["config_path"], previous.params)
                        ),
                    },
                ]
            )
            tx = await self.store.stage(restore_specs)
            await tx.commit()
        except Exception:
            self.logger.exception("Could not restore previous files")
        self.active = previous
        if self._on_secrets_activated is not None:
            self._on_secrets_activated(previous.secrets)
        prev_conf = previous.params.get("retrieve_hass_conf", {})
        try:
            if prev_conf.get("use_websocket", False):
                holder = await connection_manager.prepare_client(
                    prev_conf["hass_url"],
                    prev_conf["long_lived_token"],
                    self.logger,
                )
                await connection_manager.activate_prepared(holder)
            else:
                await connection_manager.retire_active()
        except Exception:
            self.logger.exception("Could not restore previous WebSocket connection")
        if publish_was and not self.publish.is_running:
            try:
                await self.publish.start(previous.params)
            except Exception:
                self.logger.exception("Could not restart previous continual publish task")
