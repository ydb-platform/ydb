"""Keyset/Marker types"""

from __future__ import annotations

from typing import Any, NamedTuple, Optional, Tuple, Union

Keyset = Tuple[Any, ...]
"""A tuple with as many entries as you have sort keys, representing a place in
a sorted resultset."""


class Marker(NamedTuple):
    """A keyset along with a direction (`True` means backwards), marking a place
    you could start paginating a sorted resultset and the direction to paginate
    in.

    If place is `None`, the marker represents the first page of the resultset
    (or the last page if `backwards` is `True`)."""

    place: Optional[Keyset] = None
    backwards: bool = False


# Methods that accept markers also accept plain tuples, so we use this type hint:
MarkerLike = Union[Marker, Tuple[Optional[Keyset], bool]]
