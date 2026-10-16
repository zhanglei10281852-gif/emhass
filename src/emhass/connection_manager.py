import asyncio
import logging

from emhass.websocket_client import AsyncWebSocketClient, ConnectionError

#: Delay after which a retired WebSocket generation is force-closed even if
#: in-flight users still reference it. Data retrieval happens at the start of
#: an action, so this comfortably covers outstanding requests while bounding
#: how long an old generation can linger.
RETIRE_GRACE_SECONDS = 30.0

_global_client: AsyncWebSocketClient | None = None
_lock = asyncio.Lock()

# Active generation served to new callers; ``_global_client`` mirrors its
# client for backwards compatibility with code and tests reading the module
# attribute directly.
_active_holder: "_ClientHolder | None" = None
# Generations removed from the active slot but potentially still referenced by
# in-flight RetrieveHass instances (shutting down once released or after the
# grace period).
_retired_holders: set["_ClientHolder"] = set()


class _ClientHolder:
    """A WebSocket client generation with its identity and use references."""

    def __init__(self, client: AsyncWebSocketClient, hass_url: str, token: str):
        self.client = client
        # The client strips the token itself; normalize the identity the same
        # way so a stray whitespace difference cannot force a new generation.
        self.identity = (hass_url, (token or "").strip())
        self.refs = 0
        self.retired = False
        self.watcher: asyncio.Task | None = None


async def _create_and_start_client(
    hass_url: str, token: str, logger: logging.Logger | None
) -> AsyncWebSocketClient:
    """Helper to create and start a new WebSocket client."""
    if logger:
        logger.debug(f"Creating new WebSocket client for {hass_url}")
    client = AsyncWebSocketClient(hass_url=hass_url, long_lived_token=token, logger=logger)
    try:
        await asyncio.wait_for(client.startup(), timeout=15.0)
        if logger:
            logger.info("WebSocket client started successfully")
        return client
    except TimeoutError:
        if logger:
            logger.error("WebSocket startup timed out")
        raise ConnectionError("WebSocket startup timed out") from None
    except Exception as e:
        if logger:
            logger.error(f"WebSocket startup failed: {e}")
        raise


async def _reconnect_client(client: AsyncWebSocketClient, logger: logging.Logger | None) -> None:
    """Helper to reconnect an existing WebSocket client."""
    if logger:
        logger.debug("WebSocket client exists but not connected, attempting reconnect")
    try:
        await asyncio.wait_for(client.reconnect(), timeout=15.0)
        if logger:
            logger.info("WebSocket client reconnected successfully")
    except TimeoutError:
        if logger:
            logger.error("WebSocket reconnect timed out")
        raise ConnectionError("WebSocket reconnect timed out") from None
    except Exception as e:
        if logger:
            logger.error(f"WebSocket reconnect failed: {e}")
        raise


def _sync_global_client() -> None:
    """Keep the legacy module attribute aligned with the active holder."""
    global _global_client
    _global_client = _active_holder.client if _active_holder is not None else None


async def _shutdown_holder_locked(holder: "_ClientHolder") -> None:
    """Shut a holder down. Caller holds ``_lock``."""
    if holder.watcher is not None:
        holder.watcher.cancel()
        holder.watcher = None
    try:
        await holder.client.shutdown()
    except Exception:
        # Ignore errors during shutdown (mirrors close_global_connection).
        pass
    _retired_holders.discard(holder)


async def _grace_shutdown(holder: "_ClientHolder") -> None:
    """Background watcher: force-close a retired holder after the grace period."""
    try:
        await asyncio.sleep(RETIRE_GRACE_SECONDS)
    except asyncio.CancelledError:
        # Released early (or service shutdown): release()/close handles closing.
        return
    async with _lock:
        if holder in _retired_holders:
            await _shutdown_holder_locked(holder)


def _find_holder_locked(client: AsyncWebSocketClient) -> "_ClientHolder | None":
    if _active_holder is not None and _active_holder.client is client:
        return _active_holder
    for holder in _retired_holders:
        if holder.client is client:
            return holder
    return None


async def _retire(holder: "_ClientHolder | None") -> None:
    """Move a generation out of service; close it now or once users release."""
    if holder is None:
        return
    global _active_holder
    async with _lock:
        if holder.retired:
            return
        holder.retired = True
        if _active_holder is holder:
            _active_holder = None
            _sync_global_client()
        _retired_holders.add(holder)
        if holder.refs == 0:
            await _shutdown_holder_locked(holder)
        else:
            holder.watcher = asyncio.create_task(_grace_shutdown(holder))


async def prepare_client(
    hass_url: str, token: str, logger: logging.Logger | None = None
) -> _ClientHolder:
    """
    Create and authenticate a new WebSocket generation without installing it.

    Used by the configuration activation boundary: the new connection must be
    proven working before it can take over. If a connected active generation
    with the same identity already exists it is returned instead (no duplicate
    connection).

    :return: A holder; pass to :func:`activate_prepared` or
        :func:`discard_client`.
    """
    identity = (hass_url, (token or "").strip())
    async with _lock:
        if (
            _active_holder is not None
            and _active_holder.identity == identity
            and _active_holder.client.connected
        ):
            return _active_holder
        client = await _create_and_start_client(hass_url, token, logger)
        return _ClientHolder(client, hass_url, token)


async def discard_client(holder: "_ClientHolder") -> None:
    """
    Close a prepared candidate that will not be activated.

    Closing the active generation through this call is intentionally a no-op.
    """
    async with _lock:
        if holder is _active_holder:
            return
        await _shutdown_holder_locked(holder)


async def activate_prepared(holder: "_ClientHolder") -> None:
    """
    Install a prepared generation as the active one.

    The previous generation is retired (closed after in-flight users release)
    only once the new one is installed and usable.
    """
    global _active_holder
    async with _lock:
        if holder is _active_holder:
            return
        previous = _active_holder
        _active_holder = holder
        _retired_holders.discard(holder)
        holder.retired = False
        if holder.watcher is not None:
            holder.watcher.cancel()
            holder.watcher = None
        _sync_global_client()
    await _retire(previous)


async def retire_active() -> None:
    """Retire the active generation without a replacement (WebSocket disabled)."""
    global _active_holder
    async with _lock:
        previous = _active_holder
        _active_holder = None
        _sync_global_client()
    await _retire(previous)


async def retain(client: AsyncWebSocketClient) -> None:
    """
    Mark an in-flight user as holding a client generation.

    Paired with :func:`release`; a retired generation with outstanding users is
    kept alive until released (bounded by the grace period).
    """
    async with _lock:
        holder = _find_holder_locked(client)
        if holder is not None:
            holder.refs += 1


async def release(client: AsyncWebSocketClient) -> None:
    """Release a :func:`retain` reference, closing the generation if retired."""
    async with _lock:
        holder = _find_holder_locked(client)
        if holder is None:
            return
        holder.refs = max(0, holder.refs - 1)
        if holder.retired and holder.refs == 0:
            await _shutdown_holder_locked(holder)


async def is_generation_served(client: AsyncWebSocketClient) -> bool:
    """
    Whether the manager still tracks a client generation (active or retired).

    A False result means the generation was force-closed (e.g. retirement
    grace elapsed) and reconnecting it would open an untracked socket; the
    caller should fail/use the REST fallback instead.
    """
    async with _lock:
        return _find_holder_locked(client) is not None


async def get_websocket_client(
    hass_url: str, token: str, logger: logging.Logger | None = None
) -> AsyncWebSocketClient:
    """
    Get the global WebSocket client for the given Home Assistant identity.

    Same URL+token as the active generation: reuse it (reconnect if dropped).
    Different identity: start a new generation first; only once it is
    authenticated does the previous generation retire. Startup failure leaves
    the previous generation in service.

    :raises ConnectionError: If a connection cannot be established
    """
    global _active_holder
    identity = (hass_url, (token or "").strip())
    async with _lock:
        if _active_holder is None:
            client = await _create_and_start_client(hass_url, token, logger)
            _active_holder = _ClientHolder(client, hass_url, token)
            _sync_global_client()
            return client
        if _active_holder.identity == identity:
            if _active_holder.client.connected:
                if logger:
                    logger.debug("Using existing connected WebSocket client")
                return _active_holder.client
            try:
                await _reconnect_client(_active_holder.client, logger)
            except Exception:
                # The client already ran its own cleanup during reconnect; drop
                # the dead generation so the next call builds a fresh one.
                _active_holder = None
                _sync_global_client()
                raise
            return _active_holder.client
        # Different identity: build the replacement while the old generation
        # keeps serving. On failure the active holder is untouched.
        client = await _create_and_start_client(hass_url, token, logger)
        new_holder = _ClientHolder(client, hass_url, token)
        previous = _active_holder
        _active_holder = new_holder
        _sync_global_client()
    await _retire(previous)
    return client


async def close_global_connection():
    """
    Close the active connection and every retired generation immediately.

    Service-shutdown path: do not wait for the retirement grace period.
    """
    global _active_holder
    async with _lock:
        holders = []
        if _active_holder is not None:
            holders.append(_active_holder)
        holders.extend(_retired_holders)
        _active_holder = None
        _retired_holders.clear()
        _sync_global_client()
        for holder in holders:
            await _shutdown_holder_locked(holder)


def is_connected() -> bool:
    """
    Check if the active WebSocket client is connected.

    :return: True if connected, False otherwise
    :rtype: bool
    """
    return _active_holder is not None and _active_holder.client.connected
