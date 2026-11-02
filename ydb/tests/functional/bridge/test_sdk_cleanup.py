import threading
from unittest.mock import Mock

from ydb import connection, pool

from common import BridgeKiKiMRTest


def test_stop_driver_during_disconnect(monkeypatch):
    channel = Mock()
    monkeypatch.setattr(connection, "channel_factory", lambda *args, **kwargs: channel)
    monkeypatch.setattr(connection, "_stubs_list", [])
    cached_connection = connection.Connection("localhost:1")
    cache = pool.ConnectionsCache()
    disconnect_entered = threading.Event()
    stop_entered = threading.Event()
    errors = []

    def before_remove(_):
        if threading.current_thread() is disconnect_thread:
            disconnect_entered.set()
            assert stop_entered.wait(5), "driver cleanup did not reach connection.close"

    cached_connection.add_cleanup_callback(before_remove)
    cache.add(cached_connection)
    cache.make_outdated(cached_connection)
    original_close = connection.Connection.close

    def close(self):
        if threading.current_thread() is stop_thread:
            stop_entered.set()
        return original_close(self)

    monkeypatch.setattr(connection.Connection, "close", close)
    driver = Mock()
    driver._store = cache
    driver.stop.side_effect = lambda: cache.cleanup()

    def run(action):
        try:
            action()
        except BaseException as error:
            errors.append(error)

    # Each case has its own process. Daemon threads allow a deadlocked baseline
    # to report the failure instead of hanging the test runner at shutdown.
    disconnect_thread = threading.Thread(target=run, args=(cached_connection.close,), daemon=True)
    stop_thread = threading.Thread(
        target=run, args=(lambda: BridgeKiKiMRTest.stop_driver(driver),), daemon=True,
    )
    disconnect_thread.start()
    assert disconnect_entered.wait(5), "disconnect did not reach its cleanup callback"
    stop_thread.start()
    disconnect_thread.join(5)
    stop_thread.join(5)
    assert not disconnect_thread.is_alive(), "disconnect deadlocked with driver shutdown"
    assert not stop_thread.is_alive(), "driver shutdown deadlocked with disconnect"
    assert not errors, errors
    assert not cache.connections
    assert not cache.connections_by_node_id
    assert not cache.outdated
    driver.stop.assert_called_once()
    channel.close.assert_called_once()
