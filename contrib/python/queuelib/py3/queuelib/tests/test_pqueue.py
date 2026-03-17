from pathlib import Path

import pytest

from queuelib.pqueue import PriorityQueue
from queuelib.queue import (
    FifoDiskQueue,
    FifoMemoryQueue,
    FifoSQLiteQueue,
    LifoDiskQueue,
    LifoMemoryQueue,
    LifoSQLiteQueue,
)
from queuelib.tests import QueuelibTestCase, track_closed


class PQueueTestMixin:
    def setUp(self):
        QueuelibTestCase.setUp(self)
        self.q = PriorityQueue(self.qfactory)

    def qfactory(self, prio):
        raise NotImplementedError

    def test_len_nonzero(self):
        assert not self.q
        assert len(self.q) == 0
        self.q.push(b"a", 3)
        assert self.q
        self.q.push(b"b", 1)
        self.q.push(b"c", 2)
        self.q.push(b"d", 1)
        assert len(self.q) == 4
        self.q.pop()
        self.q.pop()
        self.q.pop()
        self.q.pop()
        assert not self.q
        assert len(self.q) == 0

    def test_close(self):
        self.q.push(b"a", 3)
        self.q.push(b"b", 1)
        self.q.push(b"c", 2)
        self.q.push(b"d", 1)
        iqueues = self.q.queues.values()
        assert sorted(self.q.close()) == [1, 2, 3]
        assert all(q.closed for q in iqueues)

    def test_close_return_active(self):
        self.q.push(b"b", 1)
        self.q.push(b"c", 2)
        self.q.push(b"a", 3)
        self.q.pop()
        assert sorted(self.q.close()) == [2, 3]

    def test_popped_internal_queues_closed(self):
        self.q.push(b"a", 3)
        self.q.push(b"b", 1)
        self.q.push(b"c", 2)
        p1queue = self.q.queues[1]
        assert self.q.pop() == b"b"
        self.q.close()
        assert p1queue.closed


class FifoTestMixin:
    def test_push_pop_peek_noprio(self):
        assert self.q.peek() is None
        self.q.push(b"a")
        self.q.push(b"b")
        self.q.push(b"c")
        assert self.q.peek() == b"a"
        assert self.q.pop() == b"a"
        assert self.q.peek() == b"b"
        assert self.q.pop() == b"b"
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() is None
        assert self.q.pop() is None

    def test_push_pop_peek_prio(self):
        assert self.q.peek() is None
        self.q.push(b"a", 3)
        self.q.push(b"b", 1)
        self.q.push(b"c", 2)
        self.q.push(b"d", 1)
        assert self.q.peek() == b"b"
        assert self.q.pop() == b"b"
        assert self.q.peek() == b"d"
        assert self.q.pop() == b"d"
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() == b"a"
        assert self.q.pop() == b"a"
        assert self.q.peek() is None
        assert self.q.pop() is None


class LifoTestMixin:
    def test_push_pop_peek_noprio(self):
        assert self.q.peek() is None
        self.q.push(b"a")
        self.q.push(b"b")
        self.q.push(b"c")
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() == b"b"
        assert self.q.pop() == b"b"
        assert self.q.peek() == b"a"
        assert self.q.pop() == b"a"
        assert self.q.peek() is None
        assert self.q.pop() is None

    def test_push_pop_peek_prio(self):
        assert self.q.peek() is None
        self.q.push(b"a", 3)
        self.q.push(b"b", 1)
        self.q.push(b"c", 2)
        self.q.push(b"d", 1)
        assert self.q.peek() == b"d"
        assert self.q.pop() == b"d"
        assert self.q.peek() == b"b"
        assert self.q.pop() == b"b"
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() == b"a"
        assert self.q.pop() == b"a"
        assert self.q.peek() is None
        assert self.q.pop() is None


class FifoMemoryPriorityQueueTest(PQueueTestMixin, FifoTestMixin, QueuelibTestCase):
    def qfactory(self, prio):
        return track_closed(FifoMemoryQueue)()


class LifoMemoryPriorityQueueTest(PQueueTestMixin, LifoTestMixin, QueuelibTestCase):
    def qfactory(self, prio):
        return track_closed(LifoMemoryQueue)()


class DiskTestMixin:
    def test_nonserializable_object_one(self):
        with pytest.raises(TypeError):
            self.q.push(lambda x: x, 0)
        assert self.q.close() == []

    def test_nonserializable_object_many_close(self):
        self.q.push(b"a", 3)
        self.q.push(b"b", 1)
        with pytest.raises(TypeError):
            self.q.push(lambda x: x, 0)
        self.q.push(b"c", 2)
        assert self.q.pop() == b"b"
        assert sorted(self.q.close()) == [2, 3]

    def test_nonserializable_object_many_pop(self):
        self.q.push(b"a", 3)
        self.q.push(b"b", 1)
        with pytest.raises(TypeError):
            self.q.push(lambda x: x, 0)
        self.q.push(b"c", 2)
        assert self.q.pop() == b"b"
        assert self.q.pop() == b"c"
        assert self.q.pop() == b"a"
        assert self.q.pop() is None
        assert self.q.close() == []

    def test_reopen_with_prio(self):
        q1 = PriorityQueue(self.qfactory)
        q1.push(b"a", 3)
        q1.push(b"b", 1)
        q1.push(b"c", 2)
        active = q1.close()
        q2 = PriorityQueue(self.qfactory, startprios=active)
        assert q2.pop() == b"b"
        assert q2.pop() == b"c"
        assert q2.pop() == b"a"
        assert not q2.close()


class FifoDiskPriorityQueueTest(
    PQueueTestMixin, FifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, prio):
        path = Path(self.qdir, str(prio))
        return track_closed(FifoDiskQueue)(path)


class LifoDiskPriorityQueueTest(
    PQueueTestMixin, LifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, prio):
        path = Path(self.qdir, str(prio))
        return track_closed(LifoDiskQueue)(path)


class FifoSQLitePriorityQueueTest(
    PQueueTestMixin, FifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, prio):
        path = Path(self.qdir, str(prio))
        return track_closed(FifoSQLiteQueue)(path)


class LifoSQLitePriorityQueueTest(
    PQueueTestMixin, LifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, prio):
        path = Path(self.qdir, str(prio))
        return track_closed(LifoSQLiteQueue)(path)
