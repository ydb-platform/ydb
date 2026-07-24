"""Audited power-of-two bitonic sorting-network topology."""

from __future__ import annotations

from collections.abc import Iterator


def padded_size(count: int) -> int:
    """Smallest power of two containing ``count`` inputs."""

    if type(count) is not int or count <= 0:
        raise ValueError("sorting-network input count must be positive")
    return 1 << (count - 1).bit_length()


def comparator_count(count: int) -> int:
    """Exact comparator count of :func:`comparators`."""

    size = padded_size(count)
    levels = size.bit_length() - 1
    return size * levels * (levels + 1) // 4


def comparators(count: int) -> Iterator[tuple[int, int, bool]]:
    """Yield ``(left, right, ascending)`` bitonic compare-exchanges."""

    size = padded_size(count)
    width = 2
    while width <= size:
        stride = width // 2
        while stride:
            for left in range(size):
                right = left ^ stride
                if right > left:
                    yield left, right, (left & width) == 0
            stride //= 2
        width *= 2
