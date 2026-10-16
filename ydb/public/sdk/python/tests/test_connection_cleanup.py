import threading
from unittest.mock import Mock

import pytest

from ydb import connection, pool


@pytest.mark.parametrize("cleanup_method", ["cleanup", "cleanup_outdated"])
def test_cleanup_during_disconnect(monkeypatch, cleanup_method):
    channel = Mock()
    monkeypatch.setattr(connection, "channel_factory", lambda *args, **kwargs: channel)
    monkeypatch.setattr(connection, "_stubs_list", [])
    cached_connection = connection.Connection("localhost:1")
    cache = pool.ConnectionsCache()
    disconnect_entered = threading.Event()
    cleanup_entered = threading.Event()
    errors = []

    # Pause a real close while it holds the connection lock, immediately before
    # its cache removal callback. Cleanup must be able to release the cache lock
    # before waiting for this connection.
    def before_remove(_):
        if threading.current_thread() is disconnect_thread:
            disconnect_entered.set()
            assert cleanup_entered.wait(5), "cache cleanup did not reach connection.close"

    cached_connection.add_cleanup_callback(before_remove)
    cache.add(cached_connection)
    cache.make_outdated(cached_connection)
    original_close = connection.Connection.close

    def close(self):
        if threading.current_thread() is cleanup_thread:
            cleanup_entered.set()
        return original_close(self)

    monkeypatch.setattr(connection.Connection, "close", close)

    def run(action):
        try:
            action()
        except BaseException as error:
            errors.append(error)

    # Daemon threads let the failing baseline exit even if the lock inversion
    # deadlocks both workers. Each parameter runs in its own test process.
    disconnect_thread = threading.Thread(target=run, args=(cached_connection.close,), daemon=True)
    cleanup_thread = threading.Thread(target=run, args=(getattr(cache, cleanup_method),), daemon=True)
    disconnect_thread.start()
    assert disconnect_entered.wait(5), "disconnect did not reach its cleanup callback"
    cleanup_thread.start()
    disconnect_thread.join(5)
    cleanup_thread.join(5)
    assert not disconnect_thread.is_alive(), "disconnect deadlocked with cache cleanup"
    assert not cleanup_thread.is_alive(), "cache cleanup deadlocked with disconnect"
    assert not errors, errors
    assert not cache.connections
    assert not cache.connections_by_node_id
    assert not cache.outdated
    channel.close.assert_called_once()
