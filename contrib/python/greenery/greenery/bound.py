from __future__ import annotations

__all__ = (
    "Bound",
    "INF",
)

from dataclasses import dataclass


@dataclass(frozen=True)
class Bound:
    """An integer but sometimes also possibly infinite (None)"""

    v: int | None

    def __post_init__(self, /) -> None:
        if self.v is not None and self.v < 0:
            raise ValueError(f"Invalid bound: {self.v!r}")

    def __repr__(self, /) -> str:
        return f"Bound({self.v!r})"

    def __str__(self, /) -> str:
        if self.v is None:
            # This only happens for an unlimited upper bound
            return ""
        return str(self.v)

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.v == other.v

    def __hash__(self, /) -> int:
        return hash(self.v)

    def __lt__(self, other: Bound, /) -> bool:
        if self.v is None:
            return False
        if other.v is None:
            return True
        return self.v < other.v

    def __ge__(self, other: Bound, /) -> bool:
        return not self < other

    def __mul__(self, other: Bound, /) -> Bound:
        """Multiply this bound by another"""
        if Bound(0) in (self, other):
            return Bound(0)
        if self.v is None or other.v is None:
            return INF
        return Bound(self.v * other.v)

    def __add__(self, other: Bound, /) -> Bound:
        """Add this bound to another"""
        if self.v is None or other.v is None:
            return INF
        return Bound(self.v + other.v)

    def __sub__(self, other: Bound, /) -> Bound:
        """
        Subtract another bound from this one.
        Caution: this operation is not meaningful for all bounds.
        """
        if other.v is None:
            if self.v is not None:
                raise ArithmeticError(f"Can't subtract {other!r} from {self!r}")

            # Infinity minus infinity is zero. This has to be true so that
            # we can for example subtract Multiplier(Bound(0), INF) from
            # Multiplier(Bound(1), INF) to get Multiplier(Bound(1), Bound(1))
            return Bound(0)
        if self.v is None:
            return INF
        try:
            return Bound(self.v - other.v)
        except ValueError as e:
            raise ArithmeticError(*e.args) from e

    def copy(self, /) -> Bound:
        return Bound(self.v)


# Use this for cases where no upper bound is needed
INF = Bound(None)
