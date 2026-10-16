"""Tests for the runtime configuration activation boundary."""

import asyncio
import logging
import pathlib
import pickle
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pandas as pd

from emhass.runtime_manager import (
    ActivationCommitError,
    ActivationConnectionError,
    ActivationError,
    ActivationStore,
    PublishManager,
    PublishStopError,
    RuntimeManager,
    publish_is_enabled,
)

logger = logging.getLogger(__name__)


class TestActivationStore(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        # Three separate directories mimic add-on mounts (share / app / data).
        self.share = self.root / "share"
        self.app = self.root / "app"
        self.data = self.root / "data"
        for directory in (self.share, self.app, self.data):
            directory.mkdir()
        self.config_path = self.share / "config.json"
        self.secrets_path = self.app / "secrets_emhass.yaml"
        self.params_path = self.data / "params.pkl"
        self.store = ActivationStore(self.data, logger)

    async def asyncTearDown(self):
        self._tmp.cleanup()

    def _specs(self, *, config=b"CONFIG-B", secrets=b"SECRETS-B", params=b"PARAMS-B"):
        return [
            {"dest": self.secrets_path, "name": "secrets_emhass.yaml", "content": secrets},
            {"dest": self.config_path, "name": "config.json", "content": config},
            {"dest": self.params_path, "name": "params.pkl", "content": params},
        ]

    async def test_stage_then_commit_promotes_everything(self):
        self.config_path.write_bytes(b"CONFIG-A")
        self.secrets_path.write_bytes(b"SECRETS-A")
        self.params_path.write_bytes(b"PARAMS-A")

        tx = await self.store.stage(self._specs())

        # Staging must not touch the active files.
        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-A")
        self.assertEqual(self.secrets_path.read_bytes(), b"SECRETS-A")
        self.assertEqual(self.params_path.read_bytes(), b"PARAMS-A")
        self.assertTrue((self.store.work_dir / "pending.json").is_file())

        await tx.commit()

        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-B")
        self.assertEqual(self.secrets_path.read_bytes(), b"SECRETS-B")
        self.assertEqual(self.params_path.read_bytes(), b"PARAMS-B")
        # Staging area and sibling temp files are gone.
        self.assertFalse(self.store.work_dir.exists())
        self.assertEqual(list(self.share.glob("*.tmp.*")), [])
        self.assertEqual(list(self.app.glob("*.tmp.*")), [])

    async def test_abort_leaves_active_files_untouched(self):
        self.config_path.write_bytes(b"CONFIG-A")

        tx = await self.store.stage(self._specs())
        await tx.abort()

        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-A")
        self.assertFalse(self.secrets_path.exists())
        self.assertFalse(self.params_path.exists())
        self.assertFalse((self.store.work_dir / "pending.json").exists())

    async def test_recover_restores_partial_commit_with_pending_marker(self):
        """Crash between os.replace calls: marker + rollback restore state A."""
        self.config_path.write_bytes(b"CONFIG-A")
        self.secrets_path.write_bytes(b"SECRETS-A")
        self.params_path.write_bytes(b"PARAMS-A")

        tx = await self.store.stage(self._specs())
        # Simulate the process dying after secrets+config were promoted but
        # before params.pkl: marker and rollback still on disk.
        import os

        os.replace(tx._records[0]["tmp"], tx._records[0]["dest"])
        os.replace(tx._records[1]["tmp"], tx._records[1]["dest"])

        recovered = await self.store.recover_pending()
        self.assertTrue(recovered)

        self.assertEqual(self.secrets_path.read_bytes(), b"SECRETS-A")
        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-A")
        self.assertEqual(self.params_path.read_bytes(), b"PARAMS-A")
        self.assertFalse((self.store.work_dir / "pending.json").exists())
        # Idempotent: second startup sees nothing pending.
        self.assertFalse(await self.store.recover_pending())

    async def test_recover_removes_absent_destinations(self):
        """First-ever activation crashed mid-commit: B-only files get deleted."""
        self.secrets_path.write_bytes(b"SECRETS-A")
        # config.json and params.pkl did not exist under state A.

        tx = await self.store.stage(self._specs())
        import os

        os.replace(tx._records[1]["tmp"], tx._records[1]["dest"])  # config.json promoted
        self.assertTrue(self.config_path.exists())

        await self.store.recover_pending()

        self.assertEqual(self.secrets_path.read_bytes(), b"SECRETS-A")
        self.assertFalse(self.config_path.exists())
        self.assertFalse(self.params_path.exists())

    async def test_recover_without_marker_leaves_destinations_alone(self):
        self.config_path.write_bytes(b"CONFIG-A")
        self.params_path.write_bytes(b"PARAMS-A")

        recovered = await self.store.recover_pending()

        self.assertFalse(recovered)
        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-A")
        self.assertEqual(self.params_path.read_bytes(), b"PARAMS-A")

    async def test_commit_failure_rolls_back_and_raises(self):
        self.config_path.write_bytes(b"CONFIG-A")
        self.params_path.write_bytes(b"PARAMS-A")

        tx = await self.store.stage(self._specs())
        original_replace = __import__("os").replace
        calls = {"n": 0}

        def flaky(src, dst):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("disk full")
            return original_replace(src, dst)

        with patch("emhass.runtime_manager.os.replace", side_effect=flaky):
            with self.assertRaises(ActivationCommitError):
                await tx.commit()

        # First promotion (secrets, newly created) rolled back -> removed;
        # pre-existing files keep their A content.
        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-A")
        self.assertEqual(self.params_path.read_bytes(), b"PARAMS-A")
        self.assertFalse(self.secrets_path.exists())
        # The marker deliberately survives so a crash inside the in-process
        # rollback still completes recovery on next startup; recovery itself
        # is idempotent and keeps state A.
        self.assertTrue((self.store.work_dir / "pending.json").exists())
        self.assertTrue(await self.store.recover_pending())
        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-A")
        self.assertEqual(self.params_path.read_bytes(), b"PARAMS-A")
        self.assertFalse(self.secrets_path.exists())
        self.assertFalse((self.store.work_dir / "pending.json").exists())

    async def test_recovery_works_when_rollback_is_on_another_mount(self):
        """N-00: cross-device renames (EXDEV) must not break crash recovery.

        Rollback copies live under data_path while secrets/config live on
        other mounts; recovery promotes via copyfile + same-directory replace.
        """
        import errno
        import os as real_os

        self.config_path.write_bytes(b"CONFIG-A")
        self.secrets_path.write_bytes(b"SECRETS-A")
        self.params_path.write_bytes(b"PARAMS-A")

        tx = await self.store.stage(self._specs())
        # Crash after secrets + config were promoted, before params.
        real_os.replace(tx._records[0]["tmp"], tx._records[0]["dest"])
        real_os.replace(tx._records[1]["tmp"], tx._records[1]["dest"])

        real_replace = real_os.replace

        def cross_device_safe_replace(src, dst, *args, **kwargs):
            if pathlib.Path(src).parent != pathlib.Path(dst).parent:
                raise OSError(errno.EXDEV, "Invalid cross-device link")
            return real_replace(src, dst, *args, **kwargs)

        with patch("emhass.runtime_manager.os.replace", side_effect=cross_device_safe_replace):
            recovered = await self.store.recover_pending()

        self.assertTrue(recovered)
        self.assertEqual(self.secrets_path.read_bytes(), b"SECRETS-A")
        self.assertEqual(self.config_path.read_bytes(), b"CONFIG-A")
        self.assertEqual(self.params_path.read_bytes(), b"PARAMS-A")
        self.assertFalse((self.store.work_dir / "pending.json").exists())
        # No sibling restore temp files are left behind on any destination.
        self.assertEqual(list(self.share.glob("*.restore.*")), [])
        self.assertEqual(list(self.app.glob("*.restore.*")), [])

    async def test_recovery_removes_orphan_restore_temp(self):
        """R3-N2: a crash between copyfile and replace leaves a sibling tmp
        that the next recovery must sweep."""
        self.secrets_path.write_bytes(b"SECRETS-A")
        tx = await self.store.stage(self._specs())
        # Simulate the crash: secrets promoted, plus an orphan restore tmp.
        import os

        os.replace(tx._records[0]["tmp"], tx._records[0]["dest"])
        orphan = self.app / "secrets_emhass.yaml.restore.abc123"
        orphan.write_bytes(b"STALE")
        self.assertTrue(await self.store.recover_pending())
        self.assertEqual(self.secrets_path.read_bytes(), b"SECRETS-A")
        self.assertFalse(orphan.exists())

    async def test_pending_marker_is_valid_json(self):
        tx = await self.store.stage(self._specs())
        payload = orjson.loads(
            (self.store.work_dir / "pending.json").read_bytes()
        )
        self.assertEqual(len(payload["files"]), 3)
        self.assertEqual(payload["generation"], tx.generation)
        await tx.abort()


class FakeRH:
    """Stand-in for RetrieveHass recording its build args and close state."""

    instances: list["FakeRH"] = []

    def __init__(self, hass_url, token, freq, time_zone, params, emhass_conf, logger):
        self.hass_url = hass_url
        self.token = token
        self.freq = freq
        self.time_zone = time_zone
        self.params = params
        self.closed = False
        FakeRH.instances.append(self)

    async def close(self):
        self.closed = True


def _conf(url="http://ha-a/api", tz="UTC", enabled=True, freq_minutes=30):
    import pandas as pd

    return {
        "hass_url": url,
        "long_lived_token": "tok-a",
        "optimization_time_step": pd.to_timedelta(freq_minutes, "minutes"),
        "time_zone": tz,
        "method_ts_round": "nearest",
        "continual_publish": enabled,
    }


def _params(**conf_kwargs):
    conf = _conf(**conf_kwargs)
    return {"retrieve_hass_conf": conf}, conf


class TestPublishManager(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        FakeRH.instances = []
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.entity_path = pathlib.Path(self._tmp.name) / "entities"
        self.entity_path.mkdir()
        self.captured_contexts = []
        # Tests assign the conf that get_yaml_parse should resolve to.
        self.current_conf = _conf()

        async def fake_loop(context, entity_path, logger):
            self.captured_contexts.append(context)
            try:
                await asyncio.Event().wait()  # runs until cancelled
            except asyncio.CancelledError:
                raise

        self.fake_loop = fake_loop

    def _begin_patches(self, loop):
        """Patch runtime collaborators for the whole test (runner outlives the
        start() call, so the patchers cannot be scoped to that call alone)."""
        p1 = patch(
            "emhass.runtime_manager.utils.get_yaml_parse",
            side_effect=lambda *a, **k: (self.current_conf, {}, {}),
        )
        p2 = patch("emhass.runtime_manager.RetrieveHass", FakeRH)
        p3 = patch("emhass.runtime_manager.continual_publish", side_effect=loop)
        for patcher in (p1, p2, p3):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _manager(self, stop_timeout=0.2):
        return PublishManager(
            emhass_conf={"data_path": self.entity_path.parent},
            entity_path=self.entity_path,
            logger=logger,
            stop_timeout=stop_timeout,
        )

    async def _start(self, manager, params):
        await manager.start(params)
        # Let the runner reach its waiting point.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    def _runner_tasks(self):
        return [t for t in asyncio.all_tasks() if t.get_name() == "emhass-continual-publish"]

    async def test_stop_and_start_swaps_runner_with_new_context(self):
        manager = self._manager()
        self._begin_patches(self.fake_loop)
        params_a, conf_a = _params(url="http://ha-a/api", tz="UTC", freq_minutes=30)
        params_b, conf_b = _params(url="http://ha-b/api", tz="Asia/Tokyo", freq_minutes=5)

        self.current_conf = conf_a
        await self._start(manager, params_a)
        self.assertTrue(manager.is_running)
        old_rh = manager.current_rh
        self.assertEqual(len(self._runner_tasks()), 1)

        # The activation orchestration drives stop then start with new params.
        self.current_conf = conf_b
        await manager.stop()
        await manager.start(params_b)
        await asyncio.sleep(0)

        # Exactly one task; old runner gone and its rh closed.
        self.assertEqual(len(self._runner_tasks()), 1)
        self.assertTrue(old_rh.closed)
        new_rh = manager.current_rh
        self.assertIsNotNone(new_rh)
        self.assertIsNot(old_rh, new_rh)
        self.assertEqual(new_rh.hass_url, "http://ha-b/api")
        self.assertEqual(str(new_rh.time_zone), "Asia/Tokyo")
        self.assertEqual(new_rh.freq.total_seconds(), 300)
        self.assertIs(manager.current_params, params_b)

        await manager.shutdown()
        self.assertEqual(self._runner_tasks(), [])

    async def test_disable_stops_task_and_closes_rh(self):
        manager = self._manager()
        self._begin_patches(self.fake_loop)
        params_a, conf_a = _params(enabled=True)
        self.current_conf = conf_a
        await self._start(manager, params_a)
        old_rh = manager.current_rh
        params_off, conf_off = _params(enabled=False)

        self.current_conf = conf_off
        await manager.stop()
        await asyncio.sleep(0)

        self.assertFalse(manager.is_running)
        self.assertEqual(self._runner_tasks(), [])
        self.assertTrue(old_rh.closed)

    async def test_enable_starts_when_disabled(self):
        manager = self._manager()
        self._begin_patches(self.fake_loop)
        params_off, _ = _params(enabled=False)
        self.current_conf = _conf(enabled=False)
        await manager.start_if_enabled(params_off)
        self.assertFalse(manager.is_running)

        params_on, conf_on = _params(enabled=True)
        self.current_conf = conf_on
        await manager.start_if_enabled(params_on)
        await asyncio.sleep(0)
        self.assertTrue(manager.is_running)
        self.assertEqual(len(self._runner_tasks()), 1)

        await manager.shutdown()

    async def test_unstoppable_task_blocks_new_start(self):
        # Release flag lets the test clean up an otherwise cancellation-proof
        # task; while unset every cancellation is deliberately ignored.
        release = asyncio.Event()

        async def stubborn(context, entity_path, logger):
            while True:
                try:
                    await asyncio.shield(release.wait())
                    return
                except asyncio.CancelledError:
                    if release.is_set():
                        raise
                    continue

        manager = self._manager(stop_timeout=0.05)
        self._begin_patches(stubborn)
        params_a, conf_a = _params(url="http://ha-a/api")
        params_b, _conf_b = _params(url="http://ha-b/api")

        self.current_conf = conf_a
        await manager.start(params_a)
        await asyncio.sleep(0)
        old_task = manager._task
        old_rh = manager.current_rh

        with self.assertRaises(PublishStopError):
            await manager.stop()

        # Old task is still the one and only runner; no new rh was built for B.
        self.assertTrue(manager.is_running)
        self.assertIs(manager._task, old_task)
        self.assertEqual(len(self._runner_tasks()), 1)
        self.assertIs(manager.current_rh, old_rh)
        self.assertEqual(len(FakeRH.instances), 1)

        # Test cleanup: release the shielded wait so the proof task can finish.
        release.set()
        await asyncio.wait_for(old_task, timeout=1)
        manager._task = None
        manager._rh = None
        manager._params = None

    async def test_publish_is_enabled_helper(self):
        self.assertTrue(publish_is_enabled({"retrieve_hass_conf": {"continual_publish": True}}))
        self.assertFalse(publish_is_enabled({"retrieve_hass_conf": {}}))
        self.assertFalse(publish_is_enabled({}))


def make_params(
    url="http://ha-a/api",
    token="tok-a",
    *,
    ws=False,
    publish=False,
    tz="UTC",
    freq_minutes=30,
):
    return {
        "optim_conf": {"costfun": "profit", "logging_level": "INFO"},
        "retrieve_hass_conf": {
            "hass_url": url,
            "long_lived_token": token,
            "use_websocket": ws,
            "continual_publish": publish,
            "time_zone": tz,
            "optimization_time_step": pd.to_timedelta(freq_minutes, "minutes"),
            "method_ts_round": "nearest",
        },
    }


class ProbeRH:
    """Stand-in RetrieveHass used by the REST activation probe."""

    next_result = True
    instances: list["ProbeRH"] = []

    def __init__(self, *args, **kwargs):
        self.closed = False
        self.args = args
        ProbeRH.instances.append(self)

    async def get_ha_config(self):
        return ProbeRH.next_result

    async def close(self):
        self.closed = True


class TestRuntimeManagerActivate(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        ProbeRH.instances = []
        ProbeRH.next_result = True
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = pathlib.Path(self._tmp.name)
        (root / "data").mkdir()
        self.config_path = root / "share" / "config.json"
        self.config_path.parent.mkdir()
        self.secrets_path = root / "app" / "secrets_emhass.yaml"
        self.secrets_path.parent.mkdir()
        self.emhass_conf = {
            "data_path": root / "data",
            "config_path": self.config_path,
            "secrets_path": self.secrets_path,
        }
        self.secrets_applied = []
        self.manager = RuntimeManager(
            self.emhass_conf,
            entity_path=root / "data" / "entities",
            logger=logger,
            on_secrets_activated=lambda s: self.secrets_applied.append(dict(s)),
        )
        # Mocked publisher: tests assert stop/start transitions explicitly.
        self.pub = MagicMock()
        self.pub.is_running = False
        self.pub.stop = AsyncMock()
        self.pub.start = AsyncMock()
        self.manager.publish = self.pub

        self.secrets_a = {"hass_url": "http://ha-a/api", "long_lived_token": "tok-a"}
        self.config_a = {"costfun": "profit"}
        self.manager.set_active(make_params(), dict(self.secrets_a), dict(self.config_a))

        # build_params derives identity from the secrets it is given and the
        # ws/publish flags from config, mirroring the real merge.
        async def fake_build(emhass_conf, secrets, config, logger):
            p = make_params(
                url=secrets["hass_url"],
                token=secrets["long_lived_token"],
                ws=bool(config.get("use_websocket", False)),
                publish=bool(config.get("continual_publish", False)),
                tz=config.get("time_zone", "UTC"),
            )
            # costfun is a normal config parameter and must survive activation.
            p["optim_conf"]["costfun"] = config.get("costfun", "profit")
            return p

        def fake_parse(params, logger):
            return params["retrieve_hass_conf"], params["optim_conf"], {}

        self._patches = [
            patch("emhass.runtime_manager.utils.build_params", side_effect=fake_build),
            patch("emhass.runtime_manager.utils.get_yaml_parse", side_effect=fake_parse),
            patch("emhass.runtime_manager.RetrieveHass", ProbeRH),
        ]
        for patcher in self._patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.cm_patch = patch("emhass.runtime_manager.connection_manager")
        self.cm = self.cm_patch.start()
        self.addCleanup(self.cm_patch.stop)
        self.cm.prepare_client = AsyncMock(return_value="HOLDER-B")
        self.cm.discard_client = AsyncMock()
        self.cm.activate_prepared = AsyncMock()
        self.cm.retire_active = AsyncMock()

    def _committed(self):
        params_path = self.emhass_conf["data_path"] / "params.pkl"
        with open(params_path, "rb") as fid:
            _, params = pickle.loads(fid.read())
        return params

    @staticmethod
    def _secrets_kw(url="http://ha-b/api", token="tok-b"):
        patch = {"hass_url": url}
        if token is not None:
            patch["long_lived_token"] = token
        return {"secrets_patch": patch, "secrets_file": dict(patch)}

    async def test_full_activation_ws_identity_and_publish_swap(self):
        # Active: WS on, publishing on. Switch URL/token via secrets patch.
        config_ws = {"costfun": "profit", "use_websocket": True, "continual_publish": True}
        self.manager.set_active(
            make_params(ws=True, publish=True), dict(self.secrets_a), dict(config_ws)
        )
        self.pub.is_running = True

        active = await self.manager.activate(**self._secrets_kw())

        self.cm.prepare_client.assert_awaited_once_with(
            "http://ha-b/api", "tok-b", logger
        )
        self.cm.activate_prepared.assert_awaited_once_with("HOLDER-B")
        self.pub.stop.assert_awaited_once()
        self.pub.start.assert_awaited_once()
        self.assertEqual(self.pub.start.call_args.args[0]["retrieve_hass_conf"]["hass_url"],
                         "http://ha-b/api")
        self.assertEqual(active.params["retrieve_hass_conf"]["hass_url"], "http://ha-b/api")
        self.assertEqual(self._committed()["retrieve_hass_conf"]["hass_url"], "http://ha-b/api")
        self.assertEqual(self.secrets_applied[-1]["hass_url"], "http://ha-b/api")
        self.assertIn("http://ha-b/api", self.secrets_path.read_text())
        # No staging leftovers.
        self.assertFalse((self.emhass_conf["data_path"] / ".activation").exists())

    async def test_enable_publish_via_config_starts_runner(self):
        # Same identity, REST: no probe and no WS work; publish starts.
        active = await self.manager.activate(
            config={"costfun": "profit", "continual_publish": True}
        )
        self.cm.prepare_client.assert_not_awaited()
        self.cm.retire_active.assert_not_awaited()
        self.pub.stop.assert_not_awaited()
        self.pub.start.assert_awaited_once()
        self.assertTrue(active.params["retrieve_hass_conf"]["continual_publish"])
        self.assertEqual(orjson.loads(self.config_path.read_bytes())["continual_publish"], True)

    async def test_disable_ws_retires_active_generation(self):
        self.manager.set_active(
            make_params(ws=True), dict(self.secrets_a), dict(self.config_a)
        )
        await self.manager.activate(config={"costfun": "profit", "use_websocket": False})
        self.cm.prepare_client.assert_not_awaited()
        self.cm.retire_active.assert_awaited_once()

    async def test_ws_prepare_failure_keeps_old_config_and_publisher(self):
        config_ws = {"costfun": "profit", "use_websocket": True, "continual_publish": True}
        self.manager.set_active(
            make_params(ws=True, publish=True), dict(self.secrets_a), dict(config_ws)
        )
        self.pub.is_running = True
        self.cm.prepare_client.side_effect = Exception("auth failed")

        with self.assertRaises(ActivationConnectionError):
            await self.manager.activate(**self._secrets_kw(token=None))

        self.pub.stop.assert_not_awaited()
        self.pub.start.assert_not_awaited()
        self.cm.activate_prepared.assert_not_awaited()
        self.assertEqual(self.manager.active.params["retrieve_hass_conf"]["hass_url"],
                         "http://ha-a/api")
        self.assertFalse((self.emhass_conf["data_path"] / "params.pkl").exists())
        self.assertFalse((self.emhass_conf["data_path"] / ".activation").exists())

    async def test_rest_probe_failure_keeps_old_config(self):
        ProbeRH.next_result = False
        with self.assertRaises(ActivationConnectionError):
            await self.manager.activate(**self._secrets_kw())
        self.assertEqual(len(ProbeRH.instances), 1)
        self.assertTrue(ProbeRH.instances[0].closed)
        self.pub.stop.assert_not_awaited()
        self.assertEqual(self.manager.active.params["retrieve_hass_conf"]["hass_url"],
                         "http://ha-a/api")
        self.assertFalse((self.emhass_conf["data_path"] / "params.pkl").exists())

    async def test_unstoppable_publish_aborts_without_new_runner(self):
        self.manager.set_active(
            make_params(publish=True), dict(self.secrets_a), dict(self.config_a)
        )
        self.pub.is_running = True
        self.pub.stop.side_effect = PublishStopError("nope")

        with self.assertRaises(PublishStopError):
            await self.manager.activate(**self._secrets_kw())
        self.pub.start.assert_not_awaited()
        # Probe ran before publish stop and was closed; files/state stay at A.
        self.assertTrue(ProbeRH.instances[0].closed)
        self.assertEqual(self.manager.active.params["retrieve_hass_conf"]["hass_url"],
                         "http://ha-a/api")
        self.assertFalse((self.emhass_conf["data_path"] / "params.pkl").exists())

    async def test_ordinary_params_save_has_no_connection_or_publish_side_effects(self):
        active = await self.manager.activate(config={"costfun": "profit", "battery_capacity": 9})
        self.cm.prepare_client.assert_not_awaited()
        self.cm.activate_prepared.assert_not_awaited()
        self.cm.retire_active.assert_not_awaited()
        self.pub.stop.assert_not_awaited()
        self.pub.start.assert_not_awaited()
        self.assertEqual(len(ProbeRH.instances), 0)
        self.assertEqual(active.params["optim_conf"]["costfun"], "profit")
        self.assertEqual(self._committed()["optim_conf"]["costfun"], "profit")
        self.assertFalse((self.emhass_conf["data_path"] / ".activation").exists())

    async def test_costfun_change_takes_effect_immediately(self):
        """Ordinary config parameters (e.g. costfun) are not pinned to old values."""
        active = await self.manager.activate(
            config={"costfun": "self-consumption", "battery_capacity": 9}
        )
        self.assertEqual(active.params["optim_conf"]["costfun"], "self-consumption")
        self.assertEqual(
            self._committed()["optim_conf"]["costfun"], "self-consumption"
        )

    async def test_ws_same_identity_ordinary_save_makes_no_connection(self):
        """WS enabled + same URL/token: do not build or verify any connection."""
        config_ws = {"costfun": "profit", "use_websocket": True}
        self.manager.set_active(
            make_params(ws=True), dict(self.secrets_a), dict(config_ws)
        )
        await self.manager.activate(
            config={"costfun": "profit", "use_websocket": True, "battery_capacity": 9}
        )
        self.cm.prepare_client.assert_not_awaited()
        self.cm.activate_prepared.assert_not_awaited()
        self.cm.retire_active.assert_not_awaited()

    async def test_secrets_yaml_excludes_env_derived_values(self):
        """Only file-scoped keys are persisted; merged ENV values stay in memory."""
        # The active in-memory secret set carries an ENV/options-derived key.
        self.manager.set_active(
            make_params(),
            {**self.secrets_a, "time_zone": "UTC", "SUPERVISOR_TOKEN": "env-secret"},
            dict(self.config_a),
        )
        await self.manager.activate(
            **self._secrets_kw(),
        )
        on_disk = self.secrets_path.read_text()
        self.assertIn("hass_url: http://ha-b/api", on_disk)
        self.assertNotIn("SUPERVISOR_TOKEN", on_disk)
        self.assertNotIn("time_zone", on_disk)

    async def test_secrets_patch_does_not_rewrite_config_file(self):
        self.config_path.write_bytes(b'{"costfun": "profit"}')
        before = self.config_path.read_bytes()
        await self.manager.activate(**self._secrets_kw())
        self.assertEqual(self.config_path.read_bytes(), before)
        self.assertEqual(self._committed()["retrieve_hass_conf"]["hass_url"], "http://ha-b/api")
        self.assertEqual(self.manager.active.config, self.config_a)

    async def test_post_commit_failure_removes_secrets_file_absent_at_boot(self):
        """N-03: first secrets activation + post-commit failure must not leave
        a B yaml when no secrets file existed at boot."""
        # Boot generation: publish enabled (config carries the flag), no
        # secrets file on disk.
        self.manager.set_active(
            make_params(publish=True),
            dict(self.secrets_a),
            {"costfun": "profit", "continual_publish": True},
            secrets_file=None,
            secrets_file_existed=False,
        )
        self.pub.is_running = True
        self.pub.stop = AsyncMock()
        self.pub.start = AsyncMock(side_effect=RuntimeError("runner boom"))

        self.assertFalse(self.secrets_path.exists())
        with self.assertRaises(ActivationError):
            await self.manager.activate(**self._secrets_kw())

        # Rolled back: B yaml removed, in-memory active back to A.
        self.assertFalse(self.secrets_path.exists())
        self.assertEqual(
            self.manager.active.params["retrieve_hass_conf"]["hass_url"],
            "http://ha-a/api",
        )

    async def test_post_commit_failure_restores_previous_secrets_file(self):
        """N-03: a boot-present secrets file is restored with its A content."""
        self.secrets_path.write_text("hass_url: http://ha-a/api\n")
        self.manager.set_active(
            make_params(publish=True),
            dict(self.secrets_a),
            {"costfun": "profit", "continual_publish": True},
            secrets_file={"hass_url": "http://ha-a/api"},
            secrets_file_existed=True,
        )
        self.pub.is_running = True
        self.pub.stop = AsyncMock()
        self.pub.start = AsyncMock(side_effect=RuntimeError("runner boom"))

        with self.assertRaises(ActivationError):
            await self.manager.activate(**self._secrets_kw())

        on_disk = self.secrets_path.read_text()
        self.assertIn("http://ha-a/api", on_disk)
        self.assertNotIn("http://ha-b/api", on_disk)

    async def test_chained_rollback_after_intermediate_setconfig_absent_yaml(self):
        """R3-N1: boot(no yaml) -> ordinary set-config -> secrets activation
        failing post-commit must remove the yaml the failed activation wrote."""
        self.manager.set_active(
            make_params(publish=False),
            dict(self.secrets_a),
            dict(self.config_a),
            secrets_file=None,
            secrets_file_existed=False,
        )
        # First start (G1) succeeds; the G2 start fails after commit.
        self.pub.start = AsyncMock(side_effect=[None, RuntimeError("runner boom")])
        # G1: a normal config save that enables continual publishing.
        await self.manager.activate(
            config={"costfun": "profit", "continual_publish": True}
        )
        self.assertFalse(self.secrets_path.exists())
        with self.assertRaises(ActivationError):
            await self.manager.activate(**self._secrets_kw())

        self.assertFalse(self.secrets_path.exists())
        self.assertEqual(
            self.manager.active.params["retrieve_hass_conf"]["hass_url"],
            "http://ha-a/api",
        )
        self.assertEqual(
            self._committed()["retrieve_hass_conf"]["hass_url"], "http://ha-a/api"
        )

    async def test_chained_rollback_after_intermediate_setconfig_restores_yaml(self):
        """R3-N1 present-yaml variant: rollback restores the A yaml content."""
        self.secrets_path.write_text("hass_url: http://ha-a/api\n")
        self.manager.set_active(
            make_params(publish=False),
            dict(self.secrets_a),
            dict(self.config_a),
            secrets_file={"hass_url": "http://ha-a/api"},
            secrets_file_existed=True,
        )
        self.pub.start = AsyncMock(side_effect=[None, RuntimeError("runner boom")])
        await self.manager.activate(
            config={"costfun": "profit", "continual_publish": True}
        )
        with self.assertRaises(ActivationError):
            await self.manager.activate(**self._secrets_kw())

        on_disk = self.secrets_path.read_text()
        self.assertIn("http://ha-a/api", on_disk)
        self.assertNotIn("http://ha-b/api", on_disk)

    async def test_concurrent_activations_end_in_one_consistent_generation(self):
        ProbeRH.next_result = True
        barrier = asyncio.Event()

        async def gated_probe(self):
            await barrier.wait()
            return True

        with patch.object(ProbeRH, "get_ha_config", gated_probe):
            task_b = asyncio.create_task(
                self.manager.activate(**self._secrets_kw("http://ha-b/api", "tok-b"))
            )
            await asyncio.sleep(0.05)
            # B is inside the probe, holding the activation lock: files remain A.
            self.assertFalse((self.emhass_conf["data_path"] / "params.pkl").exists())
            task_c = asyncio.create_task(
                self.manager.activate(**self._secrets_kw("http://ha-c/api", "tok-c"))
            )
            await asyncio.sleep(0.05)
            barrier.set()
            results = await asyncio.gather(task_b, task_c)

        final_url = self._committed()["retrieve_hass_conf"]["hass_url"]
        self.assertIn(final_url, {"http://ha-b/api", "http://ha-c/api"})
        self.assertEqual(final_url, results[-1].params["retrieve_hass_conf"]["hass_url"])
        self.assertEqual(
            final_url,
            self.manager.active.params["retrieve_hass_conf"]["hass_url"],
        )
        self.assertIn(final_url, self.secrets_path.read_text())
        self.assertFalse((self.emhass_conf["data_path"] / ".activation").exists())


class TestActivationEndToEnd(unittest.IsolatedAsyncioTestCase):
    """
    Full activation play with the real build_params/persistence pipeline:
    enable publish -> change HA URL (probe) -> bad URL rejected -> disable
    publish -> recover marker check. HA itself is never contacted.
    """

    async def asyncSetUp(self):
        ProbeRH.instances = []
        ProbeRH.next_result = True
        repo_root = pathlib.Path(__file__).resolve().parents[1]
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = pathlib.Path(self._tmp.name)
        for name in ("data", "share", "app"):
            (root / name).mkdir()
        self.emhass_conf = {
            "data_path": root / "data",
            "config_path": root / "share" / "config.json",
            "secrets_path": root / "app" / "secrets_emhass.yaml",
            "root_path": repo_root / "src/emhass",
            "defaults_path": repo_root / "src/emhass/data/config_defaults.json",
            "associations_path": repo_root / "src/emhass/data/associations.csv",
        }
        from emhass import utils

        self.utils = utils
        self.repo_logger = logging.getLogger("test_e2e_activation")
        self.config_a = await utils.build_config(
            self.emhass_conf, self.repo_logger, str(self.emhass_conf["defaults_path"])
        )
        _ok, self.secrets_a = await utils.build_secrets(
            self.emhass_conf, self.repo_logger, no_response=True
        )
        self.params_a = await utils.build_params(
            self.emhass_conf, self.secrets_a, self.config_a, self.repo_logger
        )
        self.assertIsNot(self.params_a, False)

        self.manager = RuntimeManager(
            self.emhass_conf,
            entity_path=root / "data" / "entities",
            logger=self.repo_logger,
        )
        self.manager.set_active(self.params_a, dict(self.secrets_a), dict(self.config_a))

        # HA never contacted: probe rh is a fake, WS layer is mocked, and the
        # continual loop parks on an event until cancelled.
        self.loop_contexts = []

        async def parked_loop(context, entity_path, logger):
            self.loop_contexts.append(context)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise

        self.cm_patch = patch("emhass.runtime_manager.connection_manager")
        self.cm = self.cm_patch.start()
        self.addCleanup(self.cm_patch.stop)
        self.cm.prepare_client = AsyncMock(return_value="HOLDER")
        self.cm.discard_client = AsyncMock()
        self.cm.activate_prepared = AsyncMock()
        self.cm.retire_active = AsyncMock()
        for patcher in (
            patch("emhass.runtime_manager.RetrieveHass", ProbeRH),
            patch("emhass.runtime_manager.continual_publish", side_effect=parked_loop),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _params_file(self):
        with open(self.emhass_conf["data_path"] / "params.pkl", "rb") as fid:
            _, params = pickle.loads(fid.read())
        return params

    async def test_activation_play(self):
        # 1. Enable continual publishing through a config save.
        config_b = dict(self.config_a)
        config_b["continual_publish"] = True
        await self.manager.activate(config=config_b)
        self.assertTrue(self.manager.publish.is_running)
        await asyncio.sleep(0)
        self.assertEqual(len(self.loop_contexts), 1)
        self.assertTrue(self._params_file()["retrieve_hass_conf"]["continual_publish"])

        # 2. Change the HA URL: probe succeeds, runner restarts with new rh.
        await self.manager.activate(
            secrets_patch={"hass_url": "http://ha-b/api", "long_lived_token": "tok-b"},
            secrets_file={"hass_url": "http://ha-b/api", "long_lived_token": "tok-b"},
        )
        self.assertEqual(
            self.manager.active.params["retrieve_hass_conf"]["hass_url"], "http://ha-b/api"
        )
        self.assertEqual(
            self._params_file()["retrieve_hass_conf"]["hass_url"], "http://ha-b/api"
        )
        self.assertTrue(self.manager.publish.is_running)
        await asyncio.sleep(0)
        self.assertEqual(len(self.loop_contexts), 2)
        self.assertEqual(self.loop_contexts[-1]["rh"].args[0], "http://ha-b/api")

        # 3. A bad URL fails the probe: old config and old runner stay in service.
        ProbeRH.next_result = False
        with self.assertRaises(ActivationConnectionError):
            await self.manager.activate(
                secrets_patch={"hass_url": "http://ha-dead/api", "long_lived_token": "tok-x"},
                secrets_file={
                    "hass_url": "http://ha-dead/api",
                    "long_lived_token": "tok-x",
                },
            )
        ProbeRH.next_result = True
        self.assertEqual(
            self.manager.active.params["retrieve_hass_conf"]["hass_url"], "http://ha-b/api"
        )
        self.assertEqual(
            self._params_file()["retrieve_hass_conf"]["hass_url"], "http://ha-b/api"
        )
        self.assertTrue(self.manager.publish.is_running)
        await asyncio.sleep(0)
        self.assertEqual(len(self.loop_contexts), 2)

        # 4. Disable publishing: no runner task survives and its rh is closed.
        config_c = dict(config_b)
        config_c["continual_publish"] = False
        await self.manager.activate(config=config_c)
        self.assertFalse(self.manager.publish.is_running)
        runners = [
            t for t in asyncio.all_tasks() if t.get_name() == "emhass-continual-publish"
        ]
        self.assertEqual(runners, [])
        self.assertTrue(self.loop_contexts[-1]["rh"].closed)

        # 5. Clean shutdown leaves nothing behind; no marker on the happy path.
        await self.manager.shutdown()
        self.assertFalse((self.emhass_conf["data_path"] / ".activation").exists())
        # And a restart with no pending marker reports nothing to recover.
        self.assertFalse(await self.manager.startup_recover())

    async def test_recovery_before_build_secrets_restores_old_identity(self):
        """F-02: a promoted secrets file with a live marker must roll back
        before build_secrets reads it (initialize ordering contract)."""
        import os

        import yaml

        from emhass.runtime_manager import ActivationStore

        secrets_path = self.emhass_conf["secrets_path"]
        url_a = self.secrets_a["hass_url"]
        token_a = self.secrets_a["long_lived_token"]
        secrets_path.write_text(
            yaml.safe_dump({"hass_url": url_a, "long_lived_token": token_a})
        )

        # Crash window: secrets (the first promoted file) is already B while
        # the pending marker and A rollback are still on disk.
        store = ActivationStore(self.emhass_conf["data_path"], self.repo_logger)
        tx = await store.stage(
            [
                {
                    "dest": secrets_path,
                    "name": "secrets_emhass.yaml",
                    "content": yaml.safe_dump(
                        {"hass_url": "http://ha-b/api", "long_lived_token": "tok-b"}
                    ).encode(),
                }
            ]
        )
        os.replace(tx._records[0]["tmp"], tx._records[0]["dest"])
        self.assertEqual(yaml.safe_load(secrets_path.read_text())["hass_url"],
                         "http://ha-b/api")

        # Startup runs recovery BEFORE build_secrets.
        self.assertTrue(await store.recover_pending())
        _ok, restored = await self.utils.build_secrets(
            self.emhass_conf,
            self.repo_logger,
            secrets_path=secrets_path,
            no_response=True,
        )
        self.assertEqual(restored["hass_url"], url_a)
        self.assertEqual(restored["long_lived_token"], token_a)


if __name__ == "__main__":
    unittest.main()
