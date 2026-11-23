import unittest
from unittest.mock import patch

from ydb.tests.fq.streaming_common import common


class TestMessageAcceptor(unittest.TestCase):
    def test_ordered_groups_can_interleave(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a', 'b'], ordered_group=1)
        acceptor.accept(['x', 'y'], ordered_group=2)
        acceptor.advance(['x', 'a', 'b', 'y'])
        self.assertEqual(len(acceptor), 0)

    def test_rejects_invalid_messages(self):
        for messages in (['b'], ['a', 'c'], ['a', 'a'], ['unknown']):
            with self.subTest(messages=messages):
                acceptor = common.MessageAcceptor()
                acceptor.accept(['a', 'b', 'c'])
                with self.assertRaises(AssertionError):
                    acceptor.advance(messages)

    def test_rejects_duplicate_expectations(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a'])
        with self.assertRaises(AssertionError):
            acceptor.accept(['a'], ordered_group=1)

    def test_append_without_restart(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a'])
        acceptor.advance(['a'])
        acceptor.accept(['b'])
        self.assertNotEqual(len(acceptor), 0)
        acceptor.advance(['b'])
        self.assertEqual(len(acceptor), 0)
        with self.assertRaises(AssertionError):
            acceptor.advance(['b'])

    def test_restart_allows_any_seen_suffix(self):
        for replay in ([], ['c'], ['b', 'c'], ['a', 'b', 'c']):
            with self.subTest(replay=replay):
                acceptor = common.MessageAcceptor()
                acceptor.accept(['a', 'b', 'c'])
                acceptor.advance(['a', 'b', 'c'])
                acceptor.reset()
                acceptor.accept(['d'])
                acceptor.advance(replay + ['d'])
                self.assertEqual(len(acceptor), 0)

    def test_restart_cannot_skip_unseen_messages(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a', 'b', 'c'])
        acceptor.advance(['a'])
        acceptor.reset()
        with self.assertRaises(AssertionError):
            acceptor.advance(['c'])

    def test_restart_cannot_move_backwards(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a', 'b', 'c'])
        acceptor.advance(['a', 'b', 'c'])
        acceptor.reset()
        acceptor.advance(['b', 'c'])
        acceptor.reset()
        with self.assertRaises(AssertionError):
            acceptor.advance(['a'])

    def test_replay_must_finish_even_when_other_groups_are_ready(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a', 'b'], ordered_group=1)
        acceptor.advance(['a', 'b'])
        acceptor.reset()
        acceptor.accept(['x'], ordered_group=2)
        acceptor.advance(['a', 'x'])
        self.assertEqual(len(acceptor), 1)
        acceptor.advance(['b'])
        self.assertEqual(len(acceptor), 0)

    def test_group_without_replay_does_not_block_completion(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a', 'b'], ordered_group=1)
        acceptor.advance(['a', 'b'])
        acceptor.reset()
        acceptor.accept(['x'], ordered_group=2)
        acceptor.advance(['x'])
        self.assertEqual(len(acceptor), 0)

    def test_reader_waits_for_replay_suffix(self):
        acceptor = common.MessageAcceptor()
        acceptor.accept(['a', 'b'], ordered_group=1)
        acceptor.advance(['a', 'b'])
        acceptor.reset()
        acceptor.accept(['x'], ordered_group=2)
        with patch.object(common, 'read_stream', side_effect=[['a'], ['x'], ['b']]) as reader:
            common.read_and_check_data(None, '/query', acceptor, 'endpoint', '/Root', 'consumer', 'topic')
        self.assertEqual([call.kwargs['messages_count'] for call in reader.call_args_list], [1, 2, 1])
        self.assertEqual(len(acceptor), 0)
