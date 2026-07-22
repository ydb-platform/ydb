"""Finite exact quotient of YDB's byte-lexicographic string order.

YDB orders both ``String`` and ``Utf8`` values by unsigned bytes, without
normalization or collation.  The verifier only observes strings through
equality and order.  Given the fixed literal values and an upper bound ``M``
on nonliteral terms, this module builds a finite set of ordered concrete
representatives that preserves every such observation.

There are ``M`` representatives in every infinite open interval between
literals.  A byte-lexicographic interval is finite only below ``NUL**k`` or
between ``a`` and ``a + NUL**k``; those intervals contain only the intervening
prefixes, so they contribute ``min(M, interval size)`` representatives.

For any assignment to at most ``M`` nonliteral terms, sort the distinct values
inside each literal interval and map them to that interval's representatives.
This fixes literals and preserves all equalities and comparisons.  Conversely,
every rank denotes its listed concrete value.  Thus the quotient is exact for
the verifier's string operations, not merely an over-approximation.

Literals enter as Python strings and are encoded strictly as UTF-8.  Appending
NUL bytes to a complete UTF-8 value remains valid UTF-8, so witnesses from this
universe are directly replayable even though arbitrary YDB ``String`` source
values may contain non-UTF-8 bytes.

The caller must include every simultaneously observable nonliteral string term
in ``M``.  Construction fails closed before allocation if the exact universe
would exceed the public rank, total encoded-byte, or per-value audit budgets
below.  The per-value budget is shared with witness inspection and replay, so
every representative accepted here remains replayable.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field
from typing import Iterable


_NUL = b"\x00"
MAX_REPRESENTATIVES = 65_536
MAX_REPRESENTATIVE_BYTES = 64 * 1024 * 1024
MAX_STRING_BYTES = 1_000_000


@dataclass(frozen=True, slots=True, init=False)
class StringOrderUniverse:
    """Ordered concrete representatives and their zero-based integer ranks."""

    representatives: tuple[str, ...]
    _encoded: tuple[bytes, ...] = field(repr=False)

    def __init__(
        self,
        literals: Iterable[str],
        max_nonliteral_terms: int,
    ) -> None:
        if type(max_nonliteral_terms) is not int or max_nonliteral_terms < 0:
            raise ValueError("max_nonliteral_terms must be a nonnegative integer")
        if isinstance(literals, (str, bytes)):
            raise ValueError("literals must be an iterable of strings")

        try:
            literal_set: set[bytes] = set()
            literal_bytes = 0
            for value in literals:
                encoded = _encode(value)
                if encoded not in literal_set:
                    literal_set.add(encoded)
                    literal_bytes += len(encoded)
                    _check_budget(len(literal_set), literal_bytes, len(encoded))
        except TypeError as error:
            raise ValueError("literals must be an iterable of strings") from error

        encoded_literals = tuple(sorted(literal_set))
        encoded = _build(encoded_literals, max_nonliteral_terms)
        representatives = tuple(value.decode("utf-8") for value in encoded)
        object.__setattr__(self, "representatives", representatives)
        object.__setattr__(self, "_encoded", encoded)

    @property
    def encoded_representatives(self) -> tuple[bytes, ...]:
        return self._encoded

    def __len__(self) -> int:
        return len(self.representatives)

    def rank(self, value: str) -> int:
        """Return the rank of a value already represented by the universe."""

        encoded = _encode(value)
        rank = bisect_left(self._encoded, encoded)
        if rank == len(self._encoded) or self._encoded[rank] != encoded:
            raise ValueError("string is not represented by this universe")
        return rank

    def representative(self, rank: int) -> str:
        if type(rank) is not int or not 0 <= rank < len(self.representatives):
            raise ValueError("string rank is outside the representative universe")
        return self.representatives[rank]


def _encode(value: str) -> bytes:
    if type(value) is not str:
        raise ValueError("string literal must be a Python string")
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("string literal is not valid Unicode/UTF-8") from error


def _build(literals: tuple[bytes, ...], limit: int) -> tuple[bytes, ...]:
    required_size = _required_size(literals, limit)
    _check_budget(*required_size)
    if not literals:
        encoded = tuple(_NUL * length for length in range(limit))
    else:
        result = list(_below(literals[0], limit))
        for index, literal in enumerate(literals):
            result.append(literal)
            if index + 1 < len(literals):
                result.extend(_between(literal, literals[index + 1], limit))
        result.extend(_above(literals[-1], limit))
        encoded = tuple(result)

    actual_size = (
        len(encoded),
        sum(map(len, encoded)),
        max(map(len, encoded), default=0),
    )
    if actual_size != required_size:
        raise AssertionError("string representative preflight size is not exact")
    if not all(left < right for left, right in zip(encoded, encoded[1:])):
        raise AssertionError("string representative construction is not ordered")
    return encoded


def _check_budget(count: int, encoded_bytes: int, longest_value: int) -> None:
    if count > MAX_REPRESENTATIVES:
        raise ValueError(
            "string representative universe requires "
            f"{count} ranks; limit is {MAX_REPRESENTATIVES}"
        )
    if encoded_bytes > MAX_REPRESENTATIVE_BYTES:
        raise ValueError(
            "string representative universe requires "
            f"{encoded_bytes} encoded bytes; limit is {MAX_REPRESENTATIVE_BYTES}"
        )
    if longest_value > MAX_STRING_BYTES:
        raise ValueError(
            "string representative universe requires a value of "
            f"{longest_value} encoded bytes; limit is {MAX_STRING_BYTES}"
        )


def _required_size(literals: tuple[bytes, ...], limit: int) -> tuple[int, int, int]:
    if not literals:
        return limit, _sequence_sum(0, limit), max(0, limit - 1)

    count = len(literals)
    encoded_bytes = sum(map(len, literals))
    longest_value = max(map(len, literals))

    below_count, first_length = _below_shape(literals[0], limit)
    count += below_count
    encoded_bytes += _sequence_sum(first_length, below_count)
    if below_count:
        longest_value = max(longest_value, first_length + below_count - 1)

    for lower, upper in zip(literals, literals[1:]):
        interval_count = _between_count(lower, upper, limit)
        count += interval_count
        encoded_bytes += interval_count * len(lower) + _sequence_sum(
            1,
            interval_count,
        )
        if interval_count:
            longest_value = max(longest_value, len(lower) + interval_count)

    count += limit
    encoded_bytes += limit * len(literals[-1]) + _sequence_sum(1, limit)
    if limit:
        longest_value = max(longest_value, len(literals[-1]) + limit)
    return count, encoded_bytes, longest_value


def _below_shape(upper: bytes, limit: int) -> tuple[int, int]:
    leading_nuls = len(upper) - len(upper.lstrip(_NUL))
    if leading_nuls == len(upper):
        return min(limit, len(upper)), 0
    return limit, leading_nuls + 1


def _between_count(lower: bytes, upper: bytes, limit: int) -> int:
    suffix = upper[len(lower) :] if upper.startswith(lower) else b""
    if suffix and not suffix.strip(_NUL):
        return min(limit, len(suffix) - 1)
    return limit


def _sequence_sum(first: int, count: int) -> int:
    return count * (2 * first + count - 1) // 2


def _below(upper: bytes, limit: int) -> tuple[bytes, ...]:
    count, first_length = _below_shape(upper, limit)
    return tuple(
        _NUL * length
        for length in range(first_length, first_length + count)
    )


def _between(lower: bytes, upper: bytes, limit: int) -> tuple[bytes, ...]:
    count = _between_count(lower, upper, limit)
    return tuple(lower + _NUL * length for length in range(1, count + 1))


def _above(lower: bytes, limit: int) -> tuple[bytes, ...]:
    return tuple(lower + _NUL * length for length in range(1, limit + 1))
