from __future__ import annotations

from abc import abstractmethod
from typing import Any
from unittest import mock

import pytest

from queuelib.queue import (
    BaseQueue,
    FifoDiskQueue,
    FifoMemoryQueue,
    FifoSQLiteQueue,
    LifoDiskQueue,
    LifoMemoryQueue,
    LifoSQLiteQueue,
)
from queuelib.tests import QueuelibTestCase


class DummyQueue:
    def __init__(self) -> None:
        self.q: list[Any] = []

    def push(self, obj: Any) -> None:
        self.q.append(obj)

    def pop(self) -> Any | None:
        return self.q.pop() if self.q else None

    def peek(self) -> Any | None:
        return self.q[-1] if self.q else None

    def close(self) -> None:
        pass

    def __len__(self):
        return len(self.q)


class InterfaceTest(QueuelibTestCase):
    def test_queue(self):
        queue = BaseQueue()
        with pytest.raises(NotImplementedError):
            queue.push(b"")
        with pytest.raises(NotImplementedError):
            queue.peek()
        with pytest.raises(NotImplementedError):
            queue.pop()
        with pytest.raises(NotImplementedError):
            len(queue)
        queue.close()

    def test_issubclass(self):
        assert not issubclass(list, BaseQueue)
        assert not issubclass(int, BaseQueue)
        assert not issubclass(QueuelibTestCase, BaseQueue)
        assert issubclass(DummyQueue, BaseQueue)
        assert issubclass(FifoMemoryQueue, BaseQueue)
        assert issubclass(LifoMemoryQueue, BaseQueue)
        assert issubclass(FifoDiskQueue, BaseQueue)
        assert issubclass(LifoDiskQueue, BaseQueue)
        assert issubclass(FifoSQLiteQueue, BaseQueue)
        assert issubclass(LifoSQLiteQueue, BaseQueue)

    def test_isinstance(self):
        assert not isinstance(1, BaseQueue)
        assert not isinstance([], BaseQueue)
        assert isinstance(DummyQueue(), BaseQueue)
        assert isinstance(FifoMemoryQueue(), BaseQueue)
        assert isinstance(LifoMemoryQueue(), BaseQueue)
        for cls in [FifoDiskQueue, LifoDiskQueue, FifoSQLiteQueue, LifoSQLiteQueue]:
            queue = cls(self.tempfilename())
            assert isinstance(queue, BaseQueue)
            queue.close()


class QueueTestMixin:
    @abstractmethod
    def queue(self) -> BaseQueue:
        raise NotImplementedError

    def test_empty(self):
        """Empty queue test"""
        q = self.queue()
        assert q.pop() is None
        q.close()

    def test_single_pushpop(self):
        q = self.queue()
        q.push(b"a")
        assert q.pop() == b"a"
        q.close()

    def test_binary_element(self):
        elem = (
            b"\x80\x02}q\x01(U\x04bodyq\x02U\x00U\t_encodingq\x03U\x05utf-"
            b"8q\x04U\x07cookiesq\x05}q\x06U\x04metaq\x07}q\x08U\x07header"
            b"sq\t}U\x03urlq\nX\x15\x00\x00\x00file:///tmp/tmphDJYsgU\x0bd"
            b"ont_filterq\x0b\x89U\x08priorityq\x0cK\x00U\x08callbackq\rNU"
            b"\x06methodq\x0eU\x03GETq\x0fU\x07errbackq\x10Nu."
        )
        q = self.queue()
        q.push(elem)
        assert q.pop() == elem
        q.close()

    def test_len(self):
        q = self.queue()
        assert len(q) == 0
        q.push(b"a")
        assert len(q) == 1
        q.push(b"b")
        q.push(b"c")
        assert len(q) == 3
        q.pop()
        q.pop()
        q.pop()
        assert len(q) == 0
        q.close()

    def test_peek_one_element(self):
        q = self.queue()
        assert q.peek() is None
        q.push(b"a")
        assert q.peek() == b"a"
        assert q.pop() == b"a"
        assert q.peek() is None
        q.close()


class FifoTestMixin:
    def test_push_pop1(self):
        """Basic push/pop test"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.pop() == b"a"
        assert q.pop() == b"b"
        assert q.pop() == b"c"
        assert q.pop() is None
        q.close()

    def test_push_pop2(self):
        """Test interleaved push and pops"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        q.push(b"d")
        assert q.pop() == b"a"
        assert q.pop() == b"b"
        q.push(b"e")
        assert q.pop() == b"c"
        assert q.pop() == b"d"
        assert q.pop() == b"e"
        q.close()

    def test_peek_fifo(self):
        q = self.queue()
        assert q.peek() is None
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.peek() == b"a"
        assert q.peek() == b"a"
        assert q.pop() == b"a"
        assert q.peek() == b"b"
        assert q.peek() == b"b"
        assert q.pop() == b"b"
        assert q.peek() == b"c"
        assert q.peek() == b"c"
        assert q.pop() == b"c"
        assert q.peek() is None
        q.close()


class LifoTestMixin:
    def test_push_pop1(self):
        """Basic push/pop test"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.pop() == b"c"
        assert q.pop() == b"b"
        assert q.pop() == b"a"
        assert q.pop() is None
        q.close()

    def test_push_pop2(self):
        """Test interleaved push and pops"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        q.push(b"d")
        assert q.pop() == b"d"
        assert q.pop() == b"c"
        q.push(b"e")
        assert q.pop() == b"e"
        assert q.pop() == b"b"
        assert q.pop() == b"a"
        q.close()

    def test_peek_lifo(self):
        q = self.queue()
        assert q.peek() is None
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.peek() == b"c"
        assert q.peek() == b"c"
        assert q.pop() == b"c"
        assert q.peek() == b"b"
        assert q.peek() == b"b"
        assert q.pop() == b"b"
        assert q.peek() == b"a"
        assert q.peek() == b"a"
        assert q.pop() == b"a"
        assert q.peek() is None
        q.close()


class PersistentTestMixin:
    chunksize = 100000

    @pytest.mark.xfail(
        reason="Reenable once Scrapy.squeues stops extending from this testsuite"
    )
    def test_non_bytes_raises_typeerror(self):
        q = self.queue()
        with pytest.raises(TypeError):
            q.push(0)
        with pytest.raises(TypeError):
            q.push("")
        with pytest.raises(TypeError):
            q.push(None)
        with pytest.raises(TypeError):
            q.push(lambda x: x)
        q.close()

    def test_text_in_windows(self):
        e1 = b"\r\n"
        q = self.queue()
        q.push(e1)
        q.close()
        q = self.queue()
        e2 = q.pop()
        assert e1 == e2
        q.close()

    def test_close_open(self):
        """Test closing and re-opening keeps state"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        q.push(b"d")
        q.pop()
        q.pop()
        q.close()
        del q

        q = self.queue()
        assert len(q) == 2
        q.push(b"e")
        q.pop()
        q.pop()
        q.close()
        del q

        q = self.queue()
        assert q.pop() is not None
        assert len(q) == 0
        q.close()

    def test_cleanup(self):
        """Test queue dir is removed if queue is empty"""
        q = self.queue()
        values = [b"0", b"1", b"2", b"3", b"4"]
        assert self.qpath.exists()
        for x in values:
            q.push(x)

        for _ in values:
            q.pop()
        q.close()
        assert not self.qpath.exists()


class FifoMemoryQueueTest(FifoTestMixin, QueueTestMixin, QueuelibTestCase):
    def queue(self):
        return FifoMemoryQueue()


class LifoMemoryQueueTest(LifoTestMixin, QueueTestMixin, QueuelibTestCase):
    def queue(self):
        return LifoMemoryQueue()


class FifoDiskQueueTest(
    FifoTestMixin, PersistentTestMixin, QueueTestMixin, QueuelibTestCase
):
    def queue(self):
        return FifoDiskQueue(self.qpath, chunksize=self.chunksize)

    def test_not_szhdr(self):
        q = self.queue()
        q.push(b"something")
        with (
            self.tempfilename().open("w+") as empty_file,
            mock.patch.object(q, "tailf", empty_file),
        ):
            assert q.peek() is None
            assert q.pop() is None
        q.close()

    def test_chunks(self):
        """Test chunks are created and removed"""
        values = [b"0", b"1", b"2", b"3", b"4"]
        q = self.queue()
        for x in values:
            q.push(x)

        chunks = list(self.qpath.glob("q*"))
        assert len(chunks) == 5 // self.chunksize + 1
        for _ in values:
            q.pop()

        chunks = list(self.qpath.glob("q*"))
        assert len(chunks) == 1
        q.close()


class ChunkSize1FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 1


class ChunkSize2FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 2


class ChunkSize3FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 3


class ChunkSize4FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 4


class LifoDiskQueueTest(
    LifoTestMixin, PersistentTestMixin, QueueTestMixin, QueuelibTestCase
):
    def queue(self):
        return LifoDiskQueue(self.qpath)

    def test_file_size_shrinks(self):
        """Test size of queue file shrinks when popping items"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.close()
        size = self.qpath.stat().st_size
        q = self.queue()
        q.pop()
        q.close()
        assert self.qpath.stat().st_size, size


class FifoSQLiteQueueTest(
    FifoTestMixin, PersistentTestMixin, QueueTestMixin, QueuelibTestCase
):
    def queue(self):
        return FifoSQLiteQueue(self.qpath)


class LifoSQLiteQueueTest(
    LifoTestMixin, PersistentTestMixin, QueueTestMixin, QueuelibTestCase
):
    def queue(self):
        return LifoSQLiteQueue(self.qpath)
