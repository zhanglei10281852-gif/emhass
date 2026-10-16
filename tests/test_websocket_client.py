import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import orjson

from emhass.websocket_client import (
    AsyncWebSocketClient,
    AuthenticationError,
    ConnectionError as WSConnectionError,
    RequestError,
)

# Disable logging for tests
logging.basicConfig(level=logging.CRITICAL)


class FakeWebSocket:
    """Queue-driven websocket stand-in that can simulate transport death."""

    def __init__(self, response_queue: asyncio.Queue):
        self.response_queue = response_queue
        self.sent: list[str] = []
        self.closed = False

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        item = await self.response_queue.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self):
        self.closed = True


class TestWebSocketClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.hass_url = "http://localhost:8123"
        self.token = "test_long_lived_token"
        self.client = AsyncWebSocketClient(self.hass_url, self.token)
        # Mock logger
        self.client.logger = MagicMock()

        # Create a queue for controlling message flow
        self.response_queue = asyncio.Queue()

    def test_init(self):
        self.assertEqual(self.client.hass_url, self.hass_url)
        self.assertEqual(self.client.token, self.token)
        self.assertEqual(self.client.websocket_url, "ws://localhost:8123/api/websocket")

    def _setup_mock_ws(self, mock_connect):
        """Helper to setup the websocket mock with a Queue-based recv."""
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock()
        mock_ws.close = AsyncMock()
        # The recv method will simply pull from our queue
        # This forces the client to wait until WE put something in the queue
        mock_ws.recv.side_effect = self.response_queue.get
        mock_connect.return_value = mock_ws
        return mock_ws

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_startup_success(self, mock_connect):
        mock_ws = self._setup_mock_ws(mock_connect)
        # Queue the auth handshake messages
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(
            orjson.dumps({"type": "auth_ok", "ha_version": "2023.1.1"}).decode()
        )
        # Run startup
        await self.client.startup()
        self.assertTrue(self.client.connected)
        self.assertTrue(self.client._authenticated)
        # Verify auth message was sent
        expected_auth = orjson.dumps({"type": "auth", "access_token": self.token}).decode()
        mock_ws.send.assert_called_with(expected_auth)
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_startup_auth_fail(self, mock_connect):
        self._setup_mock_ws(mock_connect)
        # Queue the fail sequence
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(
            orjson.dumps({"type": "auth_invalid", "message": "Bad token"}).decode()
        )
        with self.assertRaises(AuthenticationError):
            await self.client.startup()
        self.assertFalse(self.client.connected)

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_send_and_receive(self, mock_connect):
        mock_ws = self._setup_mock_ws(mock_connect)
        # Auth Handshake
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()
        # Prepare the result for the NEXT command
        # We put this in the queue BEFORE calling send, but because the listener loop
        # is currently waiting on the empty queue, it won't consume it instantly
        # until we yield control.
        await self.response_queue.put(
            orjson.dumps(
                {"id": 1, "type": "result", "success": True, "result": {"some": "data"}}
            ).decode()
        )
        # Send command
        result = await self.client.send("get_config")
        self.assertEqual(result, {"some": "data"})
        # Verify ID 1 was used
        call_args = mock_ws.send.call_args_list
        # call_args[0] is auth, call_args[1] is get_config
        sent_json = orjson.loads(call_args[1][0][0])
        self.assertEqual(sent_json["id"], 1)
        await self.client.shutdown()

    async def test_send_reconnects_when_disconnected(self):
        """A send from a disconnected client reconnects without re-acquiring its lock."""
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock()
        mock_ws.close = AsyncMock()

        async def connect():
            self.client._ws = mock_ws
            self.client._connected = True
            self.client._authenticated = True

        async def send_message(message):
            payload = orjson.loads(message)
            self.client._pending[payload["id"]].set_result({"ok": True})

        mock_ws.send.side_effect = send_message
        self.client._connect = AsyncMock(side_effect=connect)

        result = await asyncio.wait_for(self.client.send("ping"), timeout=2.0)

        self.assertEqual(result, {"ok": True})
        self.client._connect.assert_awaited_once()
        mock_ws.send.assert_awaited_once()
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_convenience_methods(self, mock_connect):
        self._setup_mock_ws(mock_connect)
        # Auth Phase
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()
        # Test get_states (ID 1)
        # Queue result
        await self.response_queue.put(
            orjson.dumps(
                {
                    "id": 1,
                    "type": "result",
                    "success": True,
                    "result": [{"entity_id": "sensor.test", "state": "10"}],
                }
            ).decode()
        )
        states = await self.client.get_states()
        self.assertEqual(states[0]["entity_id"], "sensor.test")
        # Test get_state (ID 2)
        # Note: get_state calls get_states internally, so it consumes an ID
        await self.response_queue.put(
            orjson.dumps(
                {
                    "id": 2,
                    "type": "result",
                    "success": True,
                    "result": [{"entity_id": "sensor.test", "state": "10"}],
                }
            ).decode()
        )
        state = await self.client.get_state("sensor.test")
        self.assertEqual(state["state"], "10")
        # Test call_service (ID 3)
        await self.response_queue.put(
            orjson.dumps({"id": 3, "type": "result", "success": True, "result": None}).decode()
        )
        await self.client.call_service("switch", "turn_on")
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_send_propagates_error_result(self, mock_connect):
        """Queued response with success: False should surface as error from send()."""
        self._setup_mock_ws(mock_connect)
        # Auth Handshake
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(
            orjson.dumps({"type": "auth_ok", "ha_version": "2023.1.1"}).decode()
        )
        await self.client.startup()
        # Queue a FAILED result for ID 1
        error_payload = {
            "id": 1,
            "type": "result",
            "success": False,
            "error": {"code": "invalid_format", "message": "Invalid JSON"},
        }
        await self.response_queue.put(orjson.dumps(error_payload).decode())
        # Sending should raise RequestError
        with self.assertRaises(RequestError):
            await self.client.send("test_command")
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_send_skips_non_matching_messages(self, mock_connect):
        """Listener should ignore unrelated messages until matching response arrives."""
        self._setup_mock_ws(mock_connect)
        # Auth Handshake
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()
        # Queue a "noise" message (ID 999) followed by the Real Response (ID 1)
        noise_payload = {"id": 999, "type": "result", "success": True, "result": "noise"}
        real_payload = {"id": 1, "type": "result", "success": True, "result": {"foo": "bar"}}
        await self.response_queue.put(orjson.dumps(noise_payload).decode())
        await self.response_queue.put(orjson.dumps(real_payload).decode())
        # Send command (ID 1)
        # The client should implicitly skip ID 999 and return ID 1
        result = await self.client.send("test_command")
        self.assertEqual(result, {"foo": "bar"})
        await self.client.shutdown()


    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_timeout_releases_pending_and_late_response_discarded(self, mock_connect):
        """A wait_for timeout must remove the pending slot; the late response
        must be discarded without killing the shared receiver."""
        self._setup_mock_ws(mock_connect)
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(
                self.client.send("recorder/statistics_during_period"), timeout=0.2
            )

        # The timed-out request is released immediately...
        self.assertEqual(self.client._pending, {})
        # ...and the shared receiver / heartbeat are still alive.
        self.assertFalse(self.client._recv_task.done())
        self.assertFalse(self.client._recv_task.cancelled())
        self.assertFalse(self.client._ping_task.done())

        # Late response for the dead id arrives first, then a fresh command's
        # response. The late one must be dropped and the new one delivered.
        await self.response_queue.put(
            orjson.dumps({"id": 1, "type": "result", "success": True, "result": "late"}).decode()
        )
        await self.response_queue.put(
            orjson.dumps({"id": 2, "type": "result", "success": True, "result": "fresh"}).decode()
        )
        result = await self.client.send("get_config")
        self.assertEqual(result, "fresh")
        self.assertEqual(self.client._pending, {})
        self.assertFalse(self.client._recv_task.done())
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_timeout_does_not_affect_concurrent_commands(self, mock_connect):
        """One command timing out must not fail or disturb other in-flight
        commands on the same shared connection."""
        self._setup_mock_ws(mock_connect)
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()

        task_slow = asyncio.create_task(self.client.send("get_config"))
        task_other = asyncio.create_task(self.client.send("get_states"))
        await asyncio.sleep(0.05)
        self.assertEqual(set(self.client._pending), {1, 2})

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(task_slow, timeout=0.2)

        # Only the timed-out request was released; the other keeps waiting.
        self.assertEqual(set(self.client._pending), {2})
        self.assertFalse(task_other.done())

        # Late response for id 1 followed by the real response for id 2.
        await self.response_queue.put(
            orjson.dumps({"id": 1, "type": "result", "success": True, "result": "late"}).decode()
        )
        await self.response_queue.put(
            orjson.dumps(
                {"id": 2, "type": "result", "success": True, "result": [{"entity_id": "x"}]}
            ).decode()
        )
        states = await task_other
        self.assertEqual(states, [{"entity_id": "x"}])
        self.assertEqual(self.client._pending, {})
        self.assertFalse(self.client._recv_task.done())
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_transport_death_fails_all_pending_and_connection_rebuilds(
        self, mock_connect
    ):
        """A real transport break must fail every in-flight request fast and
        allow later sends to rebuild an authenticated connection."""
        fake_ws = FakeWebSocket(self.response_queue)
        mock_connect.return_value = fake_ws
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()

        task_one = asyncio.create_task(self.client.send("get_config"))
        task_two = asyncio.create_task(self.client.send("get_states"))
        await asyncio.sleep(0.05)
        self.assertEqual(set(self.client._pending), {1, 2})

        # Receiver observes the broken transport.
        await self.response_queue.put(RuntimeError("connection closed by peer"))
        await asyncio.sleep(0.1)

        self.assertFalse(self.client.connected)
        self.assertEqual(self.client._pending, {})
        for task in (task_one, task_two):
            with self.assertRaises(WSConnectionError):
                await task
        # Both background tasks of the dead generation are gone and the dead
        # socket was closed rather than leaked.
        self.assertTrue(self.client._recv_task.done())
        self.assertTrue(self.client._ping_task.done())
        self.assertTrue(fake_ws.closed)
        await asyncio.sleep(0)
        self.assertEqual(self.client._bg_closes, set())

        # A subsequent send rebuilds the connection through the same client.
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock(
            side_effect=lambda message: self.client._pending[
                orjson.loads(message)["id"]
            ].set_result({"rebuilt": True})
        )
        mock_ws.close = AsyncMock()

        async def fake_connect():
            self.client._ws = mock_ws
            self.client._id = 0
            self.client._pending = {}
            self.client._connected = True
            self.client._authenticated = True

        self.client._connect = AsyncMock(side_effect=fake_connect)
        result = await asyncio.wait_for(self.client.send("get_config"), timeout=5.0)
        self.assertEqual(result, {"rebuilt": True})
        self.client._connect.assert_awaited_once()
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_shutdown_joins_tasks_and_ends_pending(self, mock_connect):
        """Explicit shutdown must converge receiver + ping tasks and give every
        pending request a definite ConnectionError result."""
        self._setup_mock_ws(mock_connect)
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()

        pending_task = asyncio.create_task(self.client.send("get_config"))
        await asyncio.sleep(0.05)
        self.assertEqual(set(self.client._pending), {1})

        await self.client.shutdown()

        with self.assertRaises(WSConnectionError):
            await pending_task
        self.assertEqual(self.client._pending, {})
        self.assertIsNone(self.client._recv_task)
        self.assertIsNone(self.client._ping_task)
        self.assertEqual(self.client._bg_closes, set())
        self.assertFalse(self.client.connected)

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_malformed_frame_does_not_kill_receiver(self, mock_connect):
        """A single unparseable / unexpected frame must not terminate the
        shared receiver; subsequent commands still complete."""
        self._setup_mock_ws(mock_connect)
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()

        await self.response_queue.put("this is not json")
        await self.response_queue.put(
            orjson.dumps({"id": 1, "type": "result", "success": True, "result": "ok"}).decode()
        )
        result = await self.client.send("get_config")
        self.assertEqual(result, "ok")
        self.assertFalse(self.client._recv_task.done())
        await self.client.shutdown()

    @patch("emhass.websocket_client.websockets.connect", new_callable=AsyncMock)
    async def test_cancelled_caller_releases_slot(self, mock_connect):
        """Direct task cancellation (not just wait_for) releases the pending
        slot and a late response is discarded safely."""
        self._setup_mock_ws(mock_connect)
        await self.response_queue.put(orjson.dumps({"type": "auth_required"}).decode())
        await self.response_queue.put(orjson.dumps({"type": "auth_ok"}).decode())
        await self.client.startup()

        task = asyncio.create_task(self.client.send("get_config"))
        await asyncio.sleep(0.05)
        self.assertEqual(set(self.client._pending), {1})
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.client._pending, {})

        await self.response_queue.put(
            orjson.dumps({"id": 1, "type": "result", "success": True, "result": "late"}).decode()
        )
        await self.response_queue.put(
            orjson.dumps({"id": 2, "type": "result", "success": True, "result": "next"}).decode()
        )
        result = await self.client.send("get_config")
        self.assertEqual(result, "next")
        self.assertFalse(self.client._recv_task.done())
        await self.client.shutdown()


if __name__ == "__main__":
    unittest.main()
