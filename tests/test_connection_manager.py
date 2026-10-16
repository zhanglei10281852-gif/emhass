#!/usr/bin/env python3

import asyncio
import pathlib
import unittest
from unittest.mock import AsyncMock, patch

import emhass.connection_manager as cm
from emhass import utils
from emhass.connection_manager import (
    activate_prepared,
    close_global_connection,
    discard_client,
    get_websocket_client,
    is_connected,
    prepare_client,
    release,
    retain,
    retire_active,
)
from emhass.websocket_client import AsyncWebSocketClient, ConnectionError

# The root folder
root = pathlib.Path(utils.get_root(__file__, num_parent=2))
# Build emhass_conf paths
emhass_conf = {}
emhass_conf["data_path"] = root / "data/"
emhass_conf["root_path"] = root / "src/emhass/"
emhass_conf["defaults_path"] = emhass_conf["root_path"] / "data/config_defaults.json"

# Create logger
logger, ch = utils.get_logger(__name__, emhass_conf, save_to_file=False)


class _FakeStartupFailure(Exception):
    """Concrete failure injected in place of a real WS startup error."""


class TestConnectionManager(unittest.IsolatedAsyncioTestCase):
    """Test connection manager functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.hass_url = "http://homeassistant.local:8123/api"
        self.token = "test_token_123"

        # Reset global client state before each test
        cm._global_client = None
        cm._active_holder = None
        cm._retired_holders.clear()

    async def asyncTearDown(self):
        """Clean up after each test."""
        # Ensure global connection is closed
        await close_global_connection()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_first_time(self, mock_client_class):
        """Test getting WebSocket client for the first time."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.connected = True
        mock_client.startup = AsyncMock()
        mock_client_class.return_value = mock_client

        # Get the client
        client = await get_websocket_client(self.hass_url, self.token, logger)

        # Verify client was created and started up
        self.assertEqual(client, mock_client)
        mock_client_class.assert_called_once_with(
            hass_url=self.hass_url, long_lived_token=self.token, logger=logger
        )
        mock_client.startup.assert_called_once()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_reuse_existing(self, mock_client_class):
        """Test reusing existing WebSocket client."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.connected = True
        mock_client.startup = AsyncMock()
        mock_client_class.return_value = mock_client

        # Get the client twice
        client1 = await get_websocket_client(self.hass_url, self.token, logger)
        client2 = await get_websocket_client(self.hass_url, self.token, logger)

        # Verify same client is returned and startup is called only once
        self.assertEqual(client1, client2)
        self.assertEqual(client1, mock_client)
        mock_client_class.assert_called_once()  # Should only be called once
        mock_client.startup.assert_called_once()  # Should only be called once

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_reconnect_when_disconnected(self, mock_client_class):
        """Test reconnecting when existing client is disconnected."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup = AsyncMock()
        mock_client.reconnect = AsyncMock()
        mock_client_class.return_value = mock_client

        # First call - client is connected
        mock_client.connected = True
        client1 = await get_websocket_client(self.hass_url, self.token, logger)

        # Second call - client is now disconnected
        mock_client.connected = False
        client2 = await get_websocket_client(self.hass_url, self.token, logger)

        # Verify same client is returned but reconnect was called
        self.assertEqual(client1, client2)
        mock_client.startup.assert_called_once()
        mock_client.reconnect.assert_called_once()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_startup_timeout(self, mock_client_class):
        """Test handling startup timeout."""
        # Mock the AsyncWebSocketClient with startup timeout
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup.side_effect = TimeoutError()
        mock_client_class.return_value = mock_client

        # Should raise ConnectionError due to timeout
        with self.assertRaises(ConnectionError) as cm:
            await get_websocket_client(self.hass_url, self.token, logger)

        self.assertIn("startup timed out", str(cm.exception))

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_startup_failure(self, mock_client_class):
        """Test handling startup failure."""
        # Mock the AsyncWebSocketClient with startup failure
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup.side_effect = Exception("Connection failed")
        mock_client_class.return_value = mock_client

        # Should raise the original exception
        with self.assertRaises(Exception) as cm:
            await get_websocket_client(self.hass_url, self.token, logger)

        self.assertIn("Connection failed", str(cm.exception))

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_reconnect_timeout(self, mock_client_class):
        """Test handling reconnect timeout."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup = AsyncMock()
        mock_client.reconnect.side_effect = TimeoutError()
        mock_client_class.return_value = mock_client

        # First call - successful startup
        mock_client.connected = True
        await get_websocket_client(self.hass_url, self.token, logger)

        # Second call - reconnect timeout
        mock_client.connected = False
        with self.assertRaises(ConnectionError) as cm:
            await get_websocket_client(self.hass_url, self.token, logger)

        self.assertIn("reconnect timed out", str(cm.exception))

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_reconnect_failure(self, mock_client_class):
        """Test handling reconnect failure."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup = AsyncMock()
        mock_client.reconnect.side_effect = Exception("Reconnect failed")
        mock_client_class.return_value = mock_client

        # First call - successful startup
        mock_client.connected = True
        await get_websocket_client(self.hass_url, self.token, logger)

        # Second call - reconnect failure
        mock_client.connected = False
        with self.assertRaises(Exception) as cm:
            await get_websocket_client(self.hass_url, self.token, logger)

        self.assertIn("Reconnect failed", str(cm.exception))

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_close_global_connection(self, mock_client_class):
        """Test closing the global connection."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.connected = True
        mock_client.startup = AsyncMock()
        mock_client.shutdown = AsyncMock()
        mock_client_class.return_value = mock_client

        # Get a client first
        await get_websocket_client(self.hass_url, self.token, logger)

        # Close the connection
        await close_global_connection()

        # Verify shutdown was called
        mock_client.shutdown.assert_called_once()

        # Verify global client is reset
        import emhass.connection_manager as cm

        self.assertIsNone(cm._global_client)

    async def test_close_global_connection_when_none(self):
        """Test closing global connection when there is no client."""
        # Should not raise exception when no client exists
        await close_global_connection()

        # Verify global client is still None
        import emhass.connection_manager as cm

        self.assertIsNone(cm._global_client)

    def test_is_connected_no_client(self):
        """Test is_connected when no client exists."""
        # Reset global client
        import emhass.connection_manager as cm

        cm._global_client = None

        # Should return False
        self.assertFalse(is_connected())

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_is_connected_with_client(self, mock_client_class):
        """Test is_connected with existing client."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup = AsyncMock()
        mock_client_class.return_value = mock_client

        # Test with connected client
        mock_client.connected = True
        await get_websocket_client(self.hass_url, self.token, logger)
        self.assertTrue(is_connected())

        # Test with disconnected client
        mock_client.connected = False
        self.assertFalse(is_connected())

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_concurrent_client_access(self, mock_client_class):
        """Test concurrent access to get_websocket_client."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.connected = True
        mock_client.startup = AsyncMock()
        mock_client_class.return_value = mock_client

        # Create multiple concurrent requests
        async def get_client():
            return await get_websocket_client(self.hass_url, self.token, logger)

        tasks = [get_client() for _ in range(5)]
        clients = await asyncio.gather(*tasks)

        # All should return the same client instance
        for client in clients:
            self.assertEqual(client, mock_client)

        # Client should only be created once
        mock_client_class.assert_called_once()
        mock_client.startup.assert_called_once()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_get_websocket_client_without_logger(self, mock_client_class):
        """Test getting WebSocket client without logger."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.connected = True
        mock_client.startup = AsyncMock()
        mock_client_class.return_value = mock_client

        # Get the client without logger
        await get_websocket_client(self.hass_url, self.token)

        # Verify client was created with None logger
        mock_client_class.assert_called_once_with(
            hass_url=self.hass_url, long_lived_token=self.token, logger=None
        )

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_client_reset_after_startup_failure(self, mock_client_class):
        """Test that global client is reset after startup failure."""
        # Mock the AsyncWebSocketClient with startup failure
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup.side_effect = Exception("Startup failed")
        mock_client_class.return_value = mock_client

        # Should raise exception and reset global client
        with self.assertRaises(Exception) as cm:
            await get_websocket_client(self.hass_url, self.token, logger)

        # Verify global client is reset
        import emhass.connection_manager as cm

        self.assertIsNone(cm._global_client)

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_client_reset_after_reconnect_failure(self, mock_client_class):
        """Test that global client is reset after reconnect failure."""
        # Mock the AsyncWebSocketClient
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.startup = AsyncMock()
        mock_client.reconnect.side_effect = Exception("Reconnect failed")
        mock_client_class.return_value = mock_client

        # First call - successful
        mock_client.connected = True
        await get_websocket_client(self.hass_url, self.token, logger)

        # Second call - reconnect fails
        mock_client.connected = False
        with self.assertRaises(Exception) as cm:
            await get_websocket_client(self.hass_url, self.token, logger)

        # Verify global client is reset
        import emhass.connection_manager as cm

        self.assertIsNone(cm._global_client)

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_lock_prevents_race_conditions(self, mock_client_class):
        """Test that the async lock prevents race conditions."""
        # Mock the AsyncWebSocketClient with slow startup
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.connected = True
        mock_client_class.return_value = mock_client

        async def slow_startup():
            await asyncio.sleep(0.1)  # Simulate slow startup

        mock_client.startup = slow_startup

        # Start two concurrent requests
        task1 = asyncio.create_task(get_websocket_client(self.hass_url, self.token, logger))
        task2 = asyncio.create_task(get_websocket_client(self.hass_url, self.token, logger))

        clients = await asyncio.gather(task1, task2)

        # Both should return the same client
        self.assertEqual(clients[0], clients[1])
        # Client should only be created once
        mock_client_class.assert_called_once()


    @staticmethod
    def _make_mock_client():
        """A distinct, fully-async mock for one WebSocket generation."""
        mock_client = AsyncMock(spec=AsyncWebSocketClient)
        mock_client.connected = True
        mock_client.startup = AsyncMock()
        mock_client.shutdown = AsyncMock()
        mock_client.reconnect = AsyncMock()
        return mock_client

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_different_identity_swaps_generation(self, mock_client_class):
        """A new URL/token installs a new generation and retires the old one."""
        client_a = self._make_mock_client()
        client_b = self._make_mock_client()
        mock_client_class.side_effect = [client_a, client_b]

        first = await get_websocket_client(self.hass_url, self.token, logger)
        second = await get_websocket_client("http://other-hass:8123/api", "other_token", logger)

        self.assertIs(second, client_b)
        self.assertIsNot(first, second)
        # The old generation has no in-flight users: closed immediately, after
        # the new one was already installed.
        client_a.shutdown.assert_awaited_once()
        client_b.shutdown.assert_not_awaited()
        self.assertIs(cm._global_client, client_b)
        self.assertTrue(is_connected())

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_retained_old_generation_closes_on_release(self, mock_client_class):
        """An in-flight user keeps the old generation alive until release()."""
        client_a = self._make_mock_client()
        client_b = self._make_mock_client()
        mock_client_class.side_effect = [client_a, client_b]

        client_a = await get_websocket_client(self.hass_url, self.token, logger)
        await retain(client_a)
        client_b = await get_websocket_client(
            "http://other-hass:8123/api", "other_token", logger
        )

        # Retired but still referenced: must not be closed yet.
        client_a.shutdown.assert_not_awaited()
        self.assertIs(cm._global_client, client_b)

        await release(client_a)
        client_a.shutdown.assert_awaited_once()
        # New generation stays unaffected.
        client_b.shutdown.assert_not_awaited()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_retained_generation_force_closed_after_grace(self, mock_client_class):
        """A never-released retired generation is closed after the grace period."""
        client_a = self._make_mock_client()
        client_b = self._make_mock_client()
        mock_client_class.side_effect = [client_a, client_b]

        with patch("emhass.connection_manager.RETIRE_GRACE_SECONDS", 0.01):
            client_a = await get_websocket_client(self.hass_url, self.token, logger)
            await retain(client_a)
            await get_websocket_client("http://other-hass:8123/api", "other_token", logger)
            client_a.shutdown.assert_not_awaited()
            await asyncio.sleep(0.05)

        client_a.shutdown.assert_awaited_once()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_new_generation_failure_keeps_old_active(self, mock_client_class):
        """If the replacement cannot start, the old generation stays active."""
        client_a = self._make_mock_client()
        client_b = self._make_mock_client()
        client_b.startup.side_effect = _FakeStartupFailure("auth failed")
        mock_client_class.side_effect = [client_a, client_b]

        client_a = await get_websocket_client(self.hass_url, self.token, logger)
        with self.assertRaises(_FakeStartupFailure):
            await get_websocket_client("http://other-hass:8123/api", "other_token", logger)

        self.assertIs(cm._global_client, client_a)
        self.assertTrue(is_connected())
        client_a.shutdown.assert_not_awaited()
        client_b.shutdown.assert_not_awaited()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_prepare_discard_and_activate(self, mock_client_class):
        """Prepared candidates are invisible until activated; discard closes them."""
        candidate_client = self._make_mock_client()
        mock_client_class.return_value = candidate_client

        holder = await prepare_client(self.hass_url, self.token, logger)
        self.assertIs(holder.client, candidate_client)
        self.assertFalse(is_connected())
        self.assertIsNone(cm._global_client)

        await discard_client(holder)
        candidate_client.shutdown.assert_awaited_once()

        # A second candidate, this time activated.
        active_client = self._make_mock_client()
        mock_client_class.return_value = active_client
        holder = await prepare_client("http://new:8123/api", "t", logger)
        await activate_prepared(holder)
        self.assertTrue(is_connected())
        self.assertIs(cm._global_client, active_client)

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_prepare_reuses_active_identity(self, mock_client_class):
        """Preparing the same identity returns the connected active generation."""
        active_client = self._make_mock_client()
        mock_client_class.return_value = active_client

        await get_websocket_client(self.hass_url, self.token, logger)
        holder = await prepare_client(self.hass_url, self.token, logger)
        self.assertIs(holder.client, active_client)
        mock_client_class.assert_called_once()
        # Discarding the active holder is a no-op.
        await discard_client(holder)
        active_client.shutdown.assert_not_awaited()

    @patch("emhass.connection_manager.AsyncWebSocketClient")
    async def test_retire_active_and_global_close(self, mock_client_class):
        """retire_active detaches; close_global_connection also closes retired gens."""
        client_a = self._make_mock_client()
        client_b = self._make_mock_client()
        mock_client_class.side_effect = [client_a, client_b]

        # A retained, retired generation plus a fresh active one.
        client_a = await get_websocket_client("http://a:8123/api", "ta", logger)
        await retain(client_a)
        client_b = await get_websocket_client("http://b:8123/api", "tb", logger)
        client_a.shutdown.assert_not_awaited()

        await close_global_connection()
        client_a.shutdown.assert_awaited_once()
        client_b.shutdown.assert_awaited_once()
        self.assertIsNone(cm._global_client)
        self.assertIsNone(cm._active_holder)
        self.assertEqual(cm._retired_holders, set())
        self.assertFalse(is_connected())

        # retire_active on its own.
        client_c = self._make_mock_client()
        mock_client_class.side_effect = [client_c]
        await get_websocket_client("http://c:8123/api", "tc", logger)
        await retire_active()
        self.assertIsNone(cm._global_client)
        client_c.shutdown.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
