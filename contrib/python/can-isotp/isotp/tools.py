__all__ = ['Timer', 'FiniteByteGenerator']
import time
from typing import Optional, Generator
from isotp.errors import BadGeneratorError
import itertools
import types


class Timer:
    start_time: Optional[int]
    timeout: int

    def __init__(self, timeout: float) -> None:
        self.set_timeout(timeout)
        self.start_time = None

    def set_timeout(self, timeout: float) -> None:
        self.timeout = int(timeout * 1e9)

    def start(self, timeout: Optional[float] = None) -> None:
        if timeout is not None:
            self.set_timeout(timeout)
        self.start_time = time.perf_counter_ns()

    def stop(self) -> None:
        self.start_time = None

    def elapsed(self) -> float:
        if self.start_time is not None:
            return float(time.perf_counter_ns() - self.start_time) / 1.0e9
        else:
            return 0

    def elapsed_ns(self) -> int:
        if self.start_time is not None:
            return time.perf_counter_ns() - self.start_time
        else:
            return 0

    def remaining_ns(self) -> int:
        if self.is_stopped():
            return 0
        return max(0, self.timeout - self.elapsed_ns())

    def remaining(self) -> float:
        return float(self.remaining_ns()) / 1e9

    def is_timed_out(self) -> bool:
        if self.is_stopped():
            return False
        else:
            return self.elapsed_ns() > self.timeout or self.timeout == 0

    def is_stopped(self) -> bool:
        return self.start_time == None


class FiniteByteGenerator:
    _gen: Generator[int, None, None]
    _size: int
    _consumed: int
    _depleted: bool

    def __init__(self, gen: Generator[int, None, None], size: int):
        if not isinstance(gen, types.GeneratorType):
            raise ValueError("Given data tuple must be a pair of generator, size (int)")

        if not isinstance(size, int):
            raise ValueError("Given data tuple must be a pair of generator, size (int)")

        if size < 0:
            raise ValueError("Given data size must a be a positive integer")

        self._gen = gen
        self._size = size
        self._consumed = 0
        self._depleted = False

    def total_length(self) -> int:
        return self._size

    def remaining_size(self) -> int:
        return self._size - self._consumed

    def depleted(self) -> bool:
        return self.remaining_size() <= 0 or self._depleted

    def consume(self, size: int, enforce_exact: bool = True) -> bytearray:
        data = bytearray(itertools.islice(self._gen, size))
        self._consumed += len(data)
        if self._consumed > self._size:
            raise BadGeneratorError("Consumed more data than specified size")
        if len(data) < size:
            self._depleted = True
            if enforce_exact:
                raise BadGeneratorError(f"Did not read the requested amount of data. Tried to read {size}, got {len(data)}")
        return data
