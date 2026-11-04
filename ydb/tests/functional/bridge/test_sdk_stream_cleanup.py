import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ydb import connection, pool


class StreamCall:
    def __init__(self, completed=False, complete_on_subscribe=False):
        self.messages = iter((1, 2))
        self.completed = completed
        self.complete_on_subscribe = complete_on_subscribe
        self.callbacks = []

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.messages)
        except StopIteration:
            self.complete()
            raise

    def add_callback(self, callback):
        if self.completed:
            return False
        self.callbacks.append(callback)
        if self.complete_on_subscribe:
            self.complete()
        return True

    def complete(self):
        self.completed = True
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback()

    def cancel(self):
        self.complete()


class TestStreamConnectionCleanup(unittest.TestCase):
    def setUp(self):
        self.channel = Mock()
        self.closed = threading.Event()
        self.channel.close.side_effect = self.closed.set
        for name, value in (("channel_factory", lambda *args, **kwargs: self.channel), ("_stubs_list", ())):
            patched = patch.object(connection, name, value)
            patched.start()
            self.addCleanup(patched.stop)
        metadata = patch.object(connection, "_construct_metadata", return_value=[])
        metadata.start()
        self.addCleanup(metadata.stop)
        self.connection = connection.Connection("localhost:1")
        self.cache = pool.ConnectionsCache()
        self.cache.add(self.connection)

    def call(self, response, wrap_result=None):
        stub = type("Stub", (), {})
        self.connection._stub_instances[stub] = SimpleNamespace(ReadTable=lambda *args, **kwargs: response)
        return self.connection(SimpleNamespace(), stub, "ReadTable", wrap_result=wrap_result)

    def cleanup_outdated(self):
        self.cache.make_outdated(self.connection)
        self.cache.cleanup_outdated()
        self.assertFalse(self.cache.connections)
        self.assertFalse(self.cache.outdated)

    def assert_closed(self):
        self.assertTrue(self.closed.wait(5), "channel was not closed")
        self.channel.close.assert_called_once_with()

    def assert_finished(self):
        self.assertEqual(self.connection.calls, 0)
        self.assertFalse(self.connection._call_states)

    def test_cleanup_preserves_active_stream(self):
        stream = self.call(StreamCall())
        self.assertEqual(next(stream), 1)
        self.cleanup_outdated()
        self.channel.close.assert_not_called()
        self.assertEqual(list(stream), [2])
        self.assert_finished()
        self.assert_closed()

    def test_cancellation_releases_connection(self):
        stream = self.call(StreamCall())
        self.cleanup_outdated()
        self.channel.close.assert_not_called()
        stream.cancel()
        self.assert_finished()
        self.assert_closed()

    def test_rpc_completion_releases_unconsumed_stream(self):
        response = StreamCall()
        stream = self.call(response)
        self.cleanup_outdated()
        self.channel.close.assert_not_called()
        response.complete()
        self.assert_finished()
        self.assert_closed()
        self.assertEqual(list(stream), [1, 2])

    def test_cleanup_waits_for_all_streams(self):
        first, second = self.call(StreamCall()), self.call(StreamCall())
        self.cleanup_outdated()
        first.cancel()
        self.assertEqual(self.connection.calls, 1)
        self.channel.close.assert_not_called()
        second.cancel()
        self.assert_finished()
        self.assert_closed()

    def test_already_completed_rpc_releases_once(self):
        self.call(StreamCall(completed=True))
        self.assert_finished()
        self.cleanup_outdated()
        self.assert_closed()

    def test_rpc_completing_during_subscription_releases_once(self):
        self.call(StreamCall(complete_on_subscribe=True))
        self.assert_finished()
        self.cleanup_outdated()
        self.assert_closed()

    def test_unary_call_releases_connection(self):
        self.assertEqual(self.call(42), 42)
        self.assert_finished()
        self.cleanup_outdated()
        self.assert_closed()

    def test_wrapper_failure_releases_connection(self):
        def fail(*args):
            raise ValueError("invalid response")

        with self.assertRaisesRegex(ValueError, "invalid response"):
            self.call(StreamCall(), wrap_result=fail)
        self.assert_finished()
        self.cleanup_outdated()
        self.assert_closed()

    def test_completion_callback_does_not_wait_for_channel_drain(self):
        response = StreamCall()
        self.call(response)
        self.cleanup_outdated()
        self.channel.close.assert_not_called()
        callback_returned = threading.Event()
        errors = []

        def close_channel():
            if not callback_returned.wait(5):
                errors.append("channel.close blocked its completion callback")
            self.closed.set()

        self.channel.close.side_effect = close_channel

        def complete():
            try:
                response.complete()
            finally:
                callback_returned.set()

        completion = threading.Thread(target=complete, daemon=True)
        completion.start()
        self.assertTrue(callback_returned.wait(5), "completion callback blocked on channel.close")
        completion.join(5)
        self.assertFalse(completion.is_alive())
        self.assert_closed()
        self.assertFalse(errors, errors)
        self.assert_finished()
