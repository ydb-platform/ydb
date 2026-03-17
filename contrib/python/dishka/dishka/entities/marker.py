from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

from dishka.exception_base import InvalidMarkerError


class BaseMarker:
    """
    A marker used to conditionally activate dependencies.

    BaseMarkers can be combined using logical operators:
    - ~marker (NOT)
    - marker1 | marker2 (OR)
    - marker1 & marker2 (AND)
    """
    def __invert__(self) -> Any:
        return NotMarker(self)

    def __or__(self, other: "BaseMarker") -> "BaseMarker":
        if other == self:
            return self
        return OrMarker(self, other)

    def __and__(self, other: "BaseMarker") -> "BaseMarker":
        if other == self:
            return self
        return AndMarker(self, other)


@dataclass(frozen=True, slots=True)
class Marker(BaseMarker):
    value: Any

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value!r})"


@dataclass(frozen=True, slots=True)
class BoolMarker(BaseMarker):
    value: bool

    def __invert__(self) -> Any:
        return BoolMarker(not self.value)

    def __and__(self, other: "BaseMarker") -> "BaseMarker":
        if self.value:
            return other
        return BoolMarker(False)

    def __or__(self, other: "BaseMarker") -> "BaseMarker":
        if self.value:
            return BoolMarker(True)
        return other


@dataclass(frozen=True, slots=True)
class NotMarker(BaseMarker):
    marker: BaseMarker

    def __invert__(self) -> "BaseMarker":
        # Double negation: ~~marker -> marker
        if isinstance(self.marker, NotMarker):
            return self.marker.marker
        return self.marker

    def __repr__(self) -> str:
        return f"~{self.marker!r}"


@dataclass(frozen=True, slots=True)
class BinOpMarker(BaseMarker):
    left: BaseMarker
    right: BaseMarker
    op: ClassVar[str]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BinOpMarker):
            return NotImplemented
        if type(self) is not type(other):
            return False
        return bool(
            (self.left == other.left and self.right == other.right) or
            (self.right == other.left and self.left == other.right),
        )

    def __ne__(self, other: object) -> bool:
        eq = self.__eq__(other)
        if eq is NotImplemented:
            return NotImplemented
        return not eq

    def __hash__(self) -> int:
        return hash(self.left) + hash(self.right)

    def __repr__(self) -> str:
        return f"({self.left!r} {self.op} {self.right!r})"


@dataclass(frozen=True, slots=True, repr=False, eq=False)
class OrMarker(BinOpMarker):
    op: ClassVar[str] = "|"

    def __invert__(self) -> BaseMarker:
        # De Morgan's law: ~(A | B) = ~A & ~B
        return AndMarker(~self.left, ~self.right)


@dataclass(frozen=True, slots=True, repr=False, eq=False)
class AndMarker(BinOpMarker):
    op: ClassVar[str] = "&"

    def __invert__(self) -> BaseMarker:
        # De Morgan's law: ~(A & B) = ~A | ~B
        return OrMarker(~self.left, ~self.right)


def or_markers(*markers: BaseMarker | None) -> BaseMarker | None:
    if not markers:
        return None
    current_marker = markers[0]
    if current_marker is None:
        return None
    for marker in markers:
        if not marker:
            return None
        current_marker |= marker
    return current_marker


def combine_when(
    provider_when: BaseMarker | None,
    source_when: BaseMarker | None,
) -> BaseMarker | None:
    if provider_when is None:
        return source_when
    if source_when is None:
        return provider_when
    return provider_when & source_when


@dataclass(frozen=True, slots=True)
class Has(Marker):
    """
    Special marker for checking if a type is available in the graph.

    Used to check if a dependency can be created or is registered.
    """
    def __repr__(self) -> str:
        return f"Has({self.value})"


@dataclass(frozen=True, slots=True)
class HasContext(Marker):
    """
    Special marker for checking if a type is available in current context.
    """
    def __repr__(self) -> str:
        return f"HasContext({self.value})"


def unpack_marker(marker: BaseMarker | None) -> Iterator[Marker]:
    match marker:
        case Marker():
            yield marker
        case NotMarker():
            yield from unpack_marker(marker.marker)
        case BinOpMarker():
            yield from unpack_marker(marker.left)
            yield from unpack_marker(marker.right)
        case BoolMarker():
            return
        case None:
            return
        case _:
            raise InvalidMarkerError(marker)
