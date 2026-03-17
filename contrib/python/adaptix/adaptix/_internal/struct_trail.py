from collections import deque
from collections.abc import Reversible, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar, Union

from .feature_requirement import HAS_NATIVE_EXC_GROUP


class TrailElementMarker:
    pass


@dataclass(frozen=True)
class Attr(TrailElementMarker):
    name: str

    def __repr__(self):
        return f"{type(self).__name__}({self.name!r})"


@dataclass(frozen=True)
class ItemKey(TrailElementMarker):
    key: Any

    def __repr__(self):
        return f"{type(self).__name__}({self.key!r})"


# TrailElement describes how to extract the next object from the source.
# By default, you must subscribe a source to get the next object,
# except with TrailElementMarker children that define custom way to extract values.
# For example, Attr means that the next value must be gotten by attribute access
TrailElement = Union[str, int, Any, TrailElementMarker]
Trail = Sequence[TrailElement]

T = TypeVar("T")


def append_trail(obj: T, trail_element: TrailElement) -> T:
    """Append a trail element to object. Trail stores in special attribute,
    if an object does not allow adding 3rd-party attributes, do nothing.
    Element inserting to start of the path (it is built in reverse order)
    """
    try:
        # noinspection PyProtectedMember
        trail = obj._adaptix_struct_trail  # type: ignore[attr-defined]
    except AttributeError:
        obj._adaptix_struct_trail = deque([trail_element])  # type: ignore[attr-defined]
    else:
        trail.appendleft(trail_element)
    return obj


def extend_trail(obj: T, sub_trail: Reversible[TrailElement]) -> T:
    """Extend a trail with a sub trail. Trail stores in special attribute,
    if an object does not allow adding 3rd-party attributes, do nothing.
    Sub path inserting to start (it is built in reverse order)
    """
    try:
        # noinspection PyProtectedMember
        trail = obj._adaptix_struct_trail  # type: ignore[attr-defined]
    except AttributeError:
        obj._adaptix_struct_trail = deque(sub_trail)  # type: ignore[attr-defined]
    else:
        trail.extendleft(reversed(sub_trail))
    return obj


def get_trail(obj: object) -> Trail:
    """Retrieve trail from an object. Trail stores in special private attribute that never be accessed directly"""
    try:
        # noinspection PyProtectedMember
        return obj._adaptix_struct_trail  # type: ignore[attr-defined]
    except AttributeError:
        return deque()


BaseExcT = TypeVar("BaseExcT", bound=BaseException)

if HAS_NATIVE_EXC_GROUP:
    def render_trail_as_note(exc: BaseExcT) -> BaseExcT:
        trail = get_trail(exc)
        if trail:
            exc.add_note(f"Exception was caused at {list(trail)}")
        return exc
else:
    def render_trail_as_note(exc: BaseExcT) -> BaseExcT:
        trail = get_trail(exc)
        if trail:
            if hasattr(exc, "__notes__"):
                exc.__notes__.append(f"Exception was caused at {list(trail)}")
            else:
                exc.__notes__ = [f"Exception was caused at {list(trail)}"]
        return exc
