import asyncio
import logging
import ssl
import time
import urllib.parse as urlparse
from typing import Any

import orjson
import websockets

logger = logging.getLogger(__name__)


class WebSocketError(Exception):
    pass


class AuthenticationError(WebSocketError):
    pass


class ConnectionError(WebSocketError):
    pass


class RequestError(WebSocketError):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class AsyncWebSocketClient:
    def __init__(
        self,
        hass_url: str,
        long_lived_token: str,
        logger: logging.Logger | None = None,
        ping_interval: int = 30,
        reconnect_delay: int = 5,
        max_reconnect_attempts: int = 5,
    ):
        self.hass_url = hass_url
        self.token = long_lived_token.strip()
        self.logger = logger or logging.getLogger("AsyncWebSocketClient")
        self.ping_interval = ping_interval
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts

        self._ws = None
        self._connected = False
        self._authenticated = False
        self._id = 0
        # Pending command futures, keyed by Home Assistant message id. Only ever
        # holds requests for the live connection generation (see ``_epoch``).
        self._pending: dict[int, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._ping_task: asyncio.Task | None = None
        self._recv_task: asyncio.Task | None = None
        # Bumped on every teardown / transport death. Receiver and ping loops
        # capture their generation's epoch and must never touch state of a
        # newer generation, so a dying old connection cannot resolve requests
        # that belong to a reconnected one.
        self._epoch = 0
        # Best-effort close tasks for sockets that died on their own; tracked
        # so shutdown/reconnect can join them instead of leaking tasks.
        self._bg_closes: set[asyncio.Task] = set()
        self._reconnects = 0

    @property
    def websocket_url(self) -> str:
        # For supervisor API, use the dedicated websocket endpoint
        if self.hass_url.startswith("http://supervisor/core/api"):
            return "ws://supervisor/core/websocket"
        elif self.hass_url.startswith("https://supervisor/core/api"):
            return "wss://supervisor/core/websocket"
        else:
            # Standard Home Assistant instance
            parsed = urlparse.urlparse(self.hass_url)
            ws_scheme = "wss" if parsed.scheme == "https" else "ws"
            return f"{ws_scheme}://{parsed.netloc}/api/websocket"

    @property
    def connected(self) -> bool:
        return self._connected and self._authenticated

    def _get_ssl_context(self) -> ssl.SSLContext | None:
        if self.websocket_url.startswith("wss://"):
            return True  # Use Python's secure defaults
        return None

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    async def startup(self):
        """Connect and authenticate."""
        async with self._lock:
            await self._connect()

    async def shutdown(self):
        """Cleanly close connection, waiting for receiver and ping tasks."""
        async with self._lock:
            await self._teardown_locked("Connection closed")

    async def reconnect(self):
        """Force a reconnect."""
        async with self._lock:
            await self._reconnect_locked()

    async def _reconnect_locked(self):
        """Reconnect while the caller holds ``_lock``."""
        await self._teardown_locked("Connection closed")
        await asyncio.sleep(1)
        try:
            await asyncio.wait_for(self._connect(), timeout=10.0)
        except TimeoutError as e:
            self.logger.error("Reconnection timed out")
            raise ConnectionError("Reconnection timed out") from e
        except Exception as e:
            self.logger.error(f"Reconnection failed: {e}")
            raise

    async def _connect(self):
        """Internal connect/authenticate and start background tasks.

        Caller must hold ``_lock``. Any previous connection generation must
        have been torn down (``_teardown_locked``) before calling this.
        """
        ws = None
        try:
            ssl_ctx = self._get_ssl_context()
            ws = await websockets.connect(
                self.websocket_url, ssl=ssl_ctx, ping_interval=None, max_size=None
            )
            # Authenticate
            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
            data = orjson.loads(msg)
            if data.get("type") != "auth_required":
                raise AuthenticationError("No auth_required")
            auth = orjson.dumps({"type": "auth", "access_token": self.token}).decode()
            await ws.send(auth)
            resp = orjson.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            if resp.get("type") != "auth_ok":
                raise AuthenticationError("Invalid auth")
            # New connection generation: fresh id space and pending table.
            self._id = 0
            self._pending = {}
            self._ws = ws
            epoch = self._epoch
            self._authenticated = True
            self._connected = True
            self._recv_task = asyncio.create_task(self._receiver(epoch, ws))
            self._ping_task = asyncio.create_task(self._ping_loop(epoch, ws))
            self._reconnects = 0
            self.logger.info("Connected and authenticated")
        except BaseException:
            self._connected = False
            self._authenticated = False
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass
            raise

    async def _teardown_locked(self, reason: str):
        """Tear down the current connection generation.

        Caller must hold ``_lock``. Background tasks never acquire ``_lock``,
        so it is safe to join them here. Every pending request gets a definite
        result (``ConnectionError``) and no receiver/ping/close task survives.
        """
        # Invalidate the generation first: receiver/ping callbacks that are
        # still scheduled become no-ops and cannot touch the (fresh) state.
        self._epoch += 1
        self._connected = False
        self._authenticated = False
        self._sweep_pending(ConnectionError(reason))
        ping_task, self._ping_task = self._ping_task, None
        recv_task, self._recv_task = self._recv_task, None
        ws, self._ws = self._ws, None
        if ping_task is not None:
            ping_task.cancel()
        if recv_task is not None:
            recv_task.cancel()
        if ws is not None:
            try:
                await ws.close()
            except Exception as e:
                self.logger.debug(f"Error while closing websocket: {e}")
        if ping_task is not None or recv_task is not None:
            await asyncio.gather(ping_task, recv_task, return_exceptions=True)
        if self._bg_closes:
            await asyncio.gather(*self._bg_closes, return_exceptions=True)

    def _sweep_pending(self, exc: Exception):
        """Fail and clear every pending future. Never awaits."""
        table, self._pending = self._pending, {}
        for fut in table.values():
            if not fut.done() and not fut.cancelled():
                fut.set_exception(exc)

    def _spawn_ws_close(self, ws):
        """Close a dead socket in the background without leaking the task."""
        task = asyncio.ensure_future(self._close_ws_silent(ws))
        self._bg_closes.add(task)
        task.add_done_callback(self._bg_closes.discard)

    @staticmethod
    async def _close_ws_silent(ws):
        try:
            await ws.close()
        except Exception:
            pass

    def _mark_transport_dead(self, epoch: int, reason: str):
        """Handle a connection that died on its own (recv/ping failure).

        Synchronous on purpose: there must be no await (and therefore no
        interleaving with a locked teardown/connect) between observing the
        death and failing the in-flight requests.
        """
        if epoch != self._epoch:
            return
        # Retire this generation so the peer task (cancelled below) and any
        # lingering callback go quiet instead of sweeping newer state.
        self._epoch += 1
        self._connected = False
        self._authenticated = False
        self._sweep_pending(ConnectionError(reason))
        current = asyncio.current_task()
        ping_task, recv_task = self._ping_task, self._recv_task
        if ping_task is not None and ping_task is not current:
            ping_task.cancel()
        if recv_task is not None and recv_task is not current:
            recv_task.cancel()
        ws, self._ws = self._ws, None
        if ws is not None:
            self._spawn_ws_close(ws)
        self.logger.info("WebSocket connection lost; pending requests failed")

    async def _ping_loop(self, epoch: int, ws):
        """Keep-alive heartbeat. Uses the id-less HA ping; the server answers
        with an id-less ``pong`` which the receiver ignores."""
        try:
            while epoch == self._epoch and self._connected:
                await asyncio.sleep(self.ping_interval)
                if epoch != self._epoch or self._ws is not ws or not self._connected:
                    return
                await ws.send(orjson.dumps({"type": "ping"}).decode())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if epoch == self._epoch:
                self.logger.debug(f"WebSocket heartbeat failed: {e}")
                self._mark_transport_dead(epoch, "Connection lost")

    async def _receiver(self, epoch: int, ws):
        """Receive messages and route them to pending futures.

        Only genuine transport errors end this loop; protocol/framing-level
        surprises (late responses, unmatched ids, malformed JSON) are logged
        and skipped so one stale request can never take down the shared
        connection used by concurrent commands.
        """
        try:
            while True:
                message = await ws.recv()
                self._dispatch(message, epoch)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if epoch == self._epoch:
                self.logger.debug(f"WebSocket receiver stopped: {e}")
                self._mark_transport_dead(epoch, "Connection lost")

    def _dispatch(self, message: Any, epoch: int):
        """Route one received frame. Never raises into the receiver loop."""
        if epoch != self._epoch:
            return
        try:
            data = orjson.loads(message)
        except Exception as e:
            self.logger.warning(f"Ignoring malformed websocket message: {e}")
            return
        mid = data.get("id")
        if mid is None:
            # Id-less pong (heartbeat), auth_ok, unsolicited events, etc.
            return
        fut = self._pending.pop(mid, None)
        if fut is None:
            # Response for a request that already timed out / was cancelled.
            # Safely discard: it must not reach a newer request and must not
            # kill the receiver.
            self.logger.debug(
                f"Discarding late/unmatched websocket response id={mid} "
                f"type={data.get('type')}"
            )
            return
        if fut.cancelled():
            self.logger.debug(f"Discarding response for cancelled request id={mid}")
            return
        if data.get("type") == "result" and data.get("success") is False:
            err = data.get("error") or {}
            try:
                fut.set_exception(
                    RequestError(err.get("code", ""), err.get("message", ""))
                )
            except asyncio.InvalidStateError:
                pass
        else:
            try:
                fut.set_result(data.get("result", None))
            except asyncio.InvalidStateError:
                pass

    async def send(self, msg_type: str, **kwargs) -> Any:
        """Send a command and await its response.

        The pending entry is removed as soon as the caller stops waiting
        (timeout or cancellation), so a late server response is simply
        discarded instead of completing a dead future and killing the shared
        receiver.
        """
        async with self._lock:
            if not self.connected:
                await self._reconnect_locked()
            mid = self._next_id()
            payload = {"id": mid, "type": msg_type, **kwargs}
            fut = asyncio.get_running_loop().create_future()
            self._pending[mid] = fut
            try:
                await self._ws.send(orjson.dumps(payload).decode())
            except Exception as e:
                # Transport write failed: release this request and tear the
                # dead generation down so every other in-flight request fails
                # fast; subsequent sends reconnect through the global manager.
                self._pending.pop(mid, None)
                self._mark_transport_dead(self._epoch, "Connection lost")
                if not fut.done() and not fut.cancelled():
                    fut.set_exception(ConnectionError(str(e)))
                    fut.exception()  # mark retrieved to avoid GC warnings
                raise ConnectionError(str(e)) from e
        try:
            return await fut
        except asyncio.CancelledError:
            # Caller deadline (asyncio.wait_for) or task cancellation. Task
            # cancellation already cancelled ``fut``; the essential cleanup is
            # releasing the id slot so the late response is dropped.
            self._pending.pop(mid, None)
            raise

    # Convenience API methods:
    async def get_config(self) -> dict[str, Any]:
        return await self.send("get_config")

    async def get_states(self) -> list[dict[str, Any]]:
        return await self.send("get_states")

    async def get_state(self, entity_id: str) -> dict[str, Any] | None:
        states = await self.get_states()
        return next((s for s in states if s["entity_id"] == entity_id), None)

    async def call_service(
        self, domain: str, service: str, service_data: dict = None, target: dict = None
    ):
        return await self.send(
            "call_service",
            domain=domain,
            service=service,
            service_data=service_data or {},
            target=target or {},
        )

    async def get_history(self, **kwargs):
        return await self.send("history/history_during_period", **kwargs)

    async def get_statistics(self, **kwargs):
        return await self.send("recorder/statistics_during_period", **kwargs)

    async def ping_time(self) -> float:
        start = time.perf_counter()
        await self.send("ping")
        return (time.perf_counter() - start) * 1000
