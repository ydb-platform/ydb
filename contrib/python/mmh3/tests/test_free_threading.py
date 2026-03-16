# pylint: disable=missing-module-docstring,missing-function-docstring
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import mmh3


def run_threaded(func: Callable[..., Any], num_threads: int = 8) -> None:
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(func) for _ in range(num_threads)]
        for future in futures:
            future.result()  # wait for all threads to complete


def test_parallel_hasher_mmh3_32_update() -> None:
    hasher = mmh3.mmh3_32()

    def closure() -> None:
        for _ in range(1000):
            hasher.update(b"foo")

    run_threaded(closure, num_threads=8)

    assert hasher.sintdigest() == mmh3.hash(b"foo" * 8000)


def test_parallel_hasher_mmh3_x64_128_update() -> None:
    hasher = mmh3.mmh3_x64_128()

    def closure() -> None:
        for _ in range(1000):
            hasher.update(b"foo")

    run_threaded(closure, num_threads=8)

    assert hasher.sintdigest() == mmh3.hash128(b"foo" * 8000, x64arch=True, signed=True)


def test_parallel_hasher_mmh3_x86_128_update() -> None:
    hasher = mmh3.mmh3_x86_128()

    def closure() -> None:
        for _ in range(1000):
            hasher.update(b"foo")

    run_threaded(closure, num_threads=8)

    assert hasher.sintdigest() == mmh3.hash128(
        b"foo" * 8000, x64arch=False, signed=True
    )
