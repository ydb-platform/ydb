"""Paging data structures and bookmark handling."""

from __future__ import annotations

import csv
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    overload,
)

from .serial import BadBookmark, Serial
from .types import Keyset, Marker, MarkerLike

SERIALIZER_SETTINGS = dict(
    lineterminator=str(""),
    delimiter=str("~"),
    doublequote=False,
    escapechar=str("\\"),
    quoting=csv.QUOTE_NONE,
)

s = Serial(**SERIALIZER_SETTINGS)


T = TypeVar("T")


def custom_bookmark_type(
    type: Type[T],  # TODO: rename this in a major release
    code: str,
    deserializer: Optional[Callable[[str], T]] = None,
    serializer: Optional[Callable[[T], str]] = None,
):
    """Register (de)serializers for bookmarks to use for a custom type.

    :param type: Python type to register.
    :paramtype type: type
    :param code: A short alphabetic code to use to identify this type in serialized bookmarks.
    :paramtype code: str
    :param serializer: A function mapping `type` values to strings. Default is
        `str`.
    :param deserializer: Inverse for `serializer`. Default is the `type`
        constructor."""
    s.register_type(type, code, deserializer=deserializer, serializer=serializer)


@overload
def serialize_bookmark(marker: MarkerLike) -> str: ...


@overload
def serialize_bookmark(marker: None) -> None: ...


def serialize_bookmark(marker: Optional[MarkerLike]) -> Optional[str]:
    """Serialize a place marker to a bookmark string.

    :returns: A CSV-like string using ``~`` as a separator."""
    if marker is None:
        # Not sure if we actually need to support None as an input, but someone
        # could be relying on it...
        return None
    x, backwards = marker
    ss = s.serialize_values(x)
    direction = "<" if backwards else ">"
    return direction + ss


def unserialize_bookmark(bookmark: str) -> Marker:
    """Deserialize a bookmark string to a place marker.

    :param bookmark: A string in the format produced by
        :func:`serialize_bookmark`.
    :returns: A marker pair as described in :func:`serialize_bookmark`.
    """
    if not bookmark:
        return Marker(None, False)

    direction = bookmark[0]

    if direction not in (">", "<"):
        raise BadBookmark(
            "Malformed bookmark string: doesn't start with a direction marker"
        )

    backwards = direction == "<"
    cells = s.unserialize_values(bookmark[1:])  # might raise BadBookmark
    return Marker(None if cells is None else tuple(cells), backwards)


_Row = TypeVar("_Row", bound=Sequence, covariant=True)


class Page(
    list, Sequence[_Row]
):  # Can't subclass List[_Row] directly because _Row is covariant
    """A :class:`list` of result rows with access to paging information and
    some convenience methods."""

    paging: Paging[_Row]
    """The :class:`Paging` information describing how this page relates to the
    whole resultset."""

    _keys: Optional[List[str]]

    def __init__(self, iterable: Iterable[_Row], paging: Paging[_Row], keys=None):
        super().__init__(iterable)
        self.paging = paging
        self._keys = keys

    def scalar(self):
        """Assuming paging was called with ``per_page=1`` and a single-column
        query, return the single value."""
        return self.one()[0]

    def one(self) -> _Row:
        """Assuming paging was called with ``per_page=1``, return the single
        row on this page."""
        c = len(self)

        if c < 1:
            raise RuntimeError("tried to select one but zero rows returned")
        elif c > 1:
            raise RuntimeError("too many rows returned")
        else:
            return self[0]

    def keys(self) -> Optional[List[str]]:
        """Equivalent of :meth:`sqlalchemy.engine.ResultProxy.keys`: returns
        the list of string keys for rows."""
        return self._keys


class Paging(Generic[_Row]):
    """Metadata describing the position of a page in a collection.
    Most properties return a page marker.
    Prefix these properties with ``bookmark_`` to get the serialized version of
    that page marker.

    Unless you're extending sqlakeyset you should not be constructing this
    class directly - use sqlakeyset.get_page or sqlakeyset.select_page to
    acquire a Page object, then access page.paging to get the paging
    metadata.
    """

    rows: List[_Row]
    per_page: int
    backwards: bool
    _places: List[Keyset]

    def __init__(
        self,
        rows: List[_Row],
        per_page: int,
        backwards: bool,
        current_place: Optional[Keyset],
        places: List[Keyset],
    ):
        self.original_rows = rows

        if rows and not places:
            raise ValueError

        self.per_page = per_page
        self.backwards = backwards

        excess = rows[per_page:]
        rows = rows[:per_page]

        self.rows = rows
        self.place_0 = current_place

        if rows:
            self.place_1 = places[0]
            self.place_n = places[len(rows) - 1]
        else:
            self.place_1 = None
            self.place_n = None

        if excess:
            self.place_nplus1 = places[len(rows)]
        else:
            self.place_nplus1 = None

        four = [self.place_0, self.place_1, self.place_n, self.place_nplus1]
        # Now that we've extracted the before/beyond places, trim the places
        # list to align with the rows list, so that _get_keys_at produces
        # correct results in all cases.
        self._places = places[:per_page]

        if backwards:
            self._places.reverse()
            self.rows.reverse()
            four.reverse()

        self.before, self.first, self.last, self.beyond = four

    @property
    def has_next(self) -> bool:
        """Boolean flagging whether there are more rows after this page (in the
        original query order)."""
        return bool(self.beyond)

    @property
    def has_previous(self) -> bool:
        """Boolean flagging whether there are more rows before this page (in the
        original query order)."""
        return bool(self.before)

    @property
    def next(self) -> Marker:
        """Marker for the next page (in the original query order)."""
        return Marker(self.last or self.before)

    @property
    def previous(self) -> Marker:
        """Marker for the previous page (in the original query order)."""
        return Marker(self.first or self.beyond, backwards=True)

    @property
    def current_forwards(self) -> Marker:
        """Marker for the current page in forwards direction."""
        return Marker(self.before)

    @property
    def current_backwards(self) -> Marker:
        """Marker for the current page in backwards direction."""
        return Marker(self.beyond, backwards=True)

    @property
    def current(self) -> Marker:
        """Marker for the current page in the current paging direction."""
        if self.backwards:
            return self.current_backwards
        else:
            return self.current_forwards

    @property
    def current_opposite(self) -> Marker:
        """Marker for the current page in the opposite of the current
        paging direction."""
        if self.backwards:
            return self.current_forwards
        else:
            return self.current_backwards

    @property
    def further(self) -> Marker:
        """Marker for the following page in the current paging direction."""
        if self.backwards:
            return self.previous
        else:
            return self.next

    @property
    def has_further(self) -> bool:
        """Boolean flagging whether there are more rows before this page in the
        current paging direction."""
        if self.backwards:
            return self.has_previous
        else:
            return self.has_next

    @property
    def is_full(self) -> bool:
        """Boolean flagging whether this page contains as many rows as were
        requested in ``per_page``."""
        return len(self.rows) == self.per_page

    def get_marker_at(self, i) -> Marker:
        """Get the marker for item at the given row index."""
        return Marker(self._places[i], self.backwards)

    def get_bookmark_at(self, i):
        """Get the bookmark for item at the given row index."""
        return serialize_bookmark(self.get_marker_at(i))

    def items(self) -> Iterable[Tuple[Marker, Any]]:
        """Iterates over the items in the page, returning a tuple ``(marker,
        item)`` for each."""
        for i, row in enumerate(self.rows):
            yield self.get_marker_at(i), row

    def bookmark_items(self):
        """Iterates over the items in the page, returning a tuple ``(bookmark,
        item)`` for each."""
        for i, row in enumerate(self.rows):
            yield self.get_bookmark_at(i), row

    # The remaining properties are just convenient shorthands to avoid manually
    # calling serialize_bookmark.
    @property
    def bookmark_next(self) -> str:
        """Bookmark for the next page (in the original query order)."""
        return serialize_bookmark(self.next)

    @property
    def bookmark_previous(self) -> str:
        """Bookmark for the previous page (in the original query order)."""
        return serialize_bookmark(self.previous)

    @property
    def bookmark_current_forwards(self) -> str:
        """Bookmark for the current page in forwards direction."""
        return serialize_bookmark(self.current_forwards)

    @property
    def bookmark_current_backwards(self) -> str:
        """Bookmark for the current page in backwards direction."""
        return serialize_bookmark(self.current_backwards)

    @property
    def bookmark_current(self) -> str:
        """Bookmark for the current page in the current paging direction."""
        return serialize_bookmark(self.current)

    @property
    def bookmark_current_opposite(self) -> str:
        """Bookmark for the current page in the opposite of the current
        paging direction."""
        return serialize_bookmark(self.current_opposite)

    @property
    def bookmark_further(self) -> str:
        """Bookmark for the following page in the current paging direction."""
        return serialize_bookmark(self.further)
