# Test for memory leaks surrounding deletion of values or
# bad cleanups.
# SEE: https://github.com/aio-libs/multidict/issues/1232
# We want to make sure that bad predictions or bougus claims
# of memory leaks can be prevented in the future.

import gc
import os

import psutil
from multidict import MultiDict


def trim_ram() -> None:
    """Forces python garbage collection."""
    gc.collect()


process = psutil.Process(os.getpid())


def get_memory_usage() -> int:
    memory_info = process.memory_info()
    return memory_info.rss // (1024 * 1024)


initial_memory_usage = get_memory_usage()

keys = [f"X-Any-{i}" for i in range(100)]
headers = {key: key * 2 for key in keys}


def check_for_leak() -> None:
    trim_ram()
    usage = get_memory_usage() - initial_memory_usage
    assert usage < 50, f"Memory leaked at: {usage} MB"


def _test_pop() -> None:
    for _ in range(10):
        for _ in range(100):
            result = MultiDict(headers)
            for k in keys:
                result.pop(k)
        check_for_leak()


def _test_popall() -> None:
    for _ in range(10):
        for _ in range(100):
            result = MultiDict(headers)
            for k in keys:
                result.popall(k)
        check_for_leak()


def _test_popone() -> None:
    for _ in range(10):
        for _ in range(100):
            result = MultiDict(headers)
            for k in keys:
                result.popone(k)
        check_for_leak()


# SEE: https://github.com/aio-libs/multidict/issues/1273
def _test_pop_with_default() -> None:
    # XXX: mypy wants an annotation so the only
    # thing we can do here is pass the headers along.
    result = MultiDict(headers)
    for i in range(1_000_000):
        result.pop(f"missing_key_{i}", None)
    check_for_leak()


def _test_del() -> None:
    for _ in range(10):
        for _ in range(100):
            result = MultiDict(headers)
            for k in keys:
                del result[k]
        check_for_leak()


def _run_isolated_case() -> None:
    _test_pop()
    _test_popall()
    _test_popone()
    _test_pop_with_default()
    _test_del()


if __name__ == "__main__":
    _run_isolated_case()
