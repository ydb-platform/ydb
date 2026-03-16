# Copyright (c) 2015-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, TypeVar, Iterator, Any
from uuid import uuid4
import functools
import html
from .juliandate import juliandate, calendardate
from .binarydata import hex_strings_to_bytes, bytes_to_hexstr

escape = functools.partial(html.escape, quote=True)


def float2transparency(value: float) -> int:
    """
    Returns DXF transparency value as integer in the range from ``0`` to ``255``, where
    ``0`` is 100% transparent and ``255`` is opaque.

    Args:
        value: transparency value as float in the range from ``0`` to ``1``, where ``0``
           is opaque and ``1`` is 100% transparency.

    """
    return int((1.0 - float(value)) * 255) | 0x02000000


def transparency2float(value: int) -> float:
    """
    Returns transparency value as float from ``0`` to ``1``, ``0`` for no transparency
    (opaque) and ``1`` for 100% transparency.

    Args:
        value: DXF integer transparency value, ``0`` for 100% transparency and ``255``
            for opaque

    """
    # 255 -> 0.
    # 0 -> 1.
    return 1.0 - float(int(value) & 0xFF) / 255.0


def set_flag_state(flags: int, flag: int, state: bool = True) -> int:
    """Set/clear binary `flag` in data `flags`.

    Args:
        flags: data value
        flag: flag to set/clear
        state: ``True`` for setting, ``False`` for clearing

    """
    if state:
        flags = flags | flag
    else:
        flags = flags & ~flag
    return flags


def guid() -> str:
    """Returns a general unique ID, based on :func:`uuid.uuid4`.

    This function creates a GUID for the header variables $VERSIONGUID and
    $FINGERPRINTGUID, which matches the AutoCAD pattern
    ``{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}``.

    """
    return "{" + str(uuid4()).upper() + "}"


T = TypeVar("T")


def take2(iterable: Iterable[T]) -> Iterator[tuple[T, T]]:
    """Iterate `iterable` as non-overlapping pairs.

    :code:`take2('ABCDEFGH') -> AB CD EF GH`

    """
    sentinel = object()
    store: Any = sentinel
    for item in iterable:
        if store is sentinel:
            store = item
        else:
            yield store, item
            store = sentinel


def pairwise(iterable: Iterable[T], close=False) -> Iterator[tuple[T, T]]:
    """Iterate `iterable` as consecutive overlapping pairs.
    This is similar to ``itertools.pairwise()`` in Python 3.10 but with `close` option.

    :code:`polygon_segments('ABCDEFG') -> AB BC CD DE EF FG`
    :code:`polygon_segments('ABCDEFG', True) -> AB BC CD DE EF FG GA`

    """
    sentinel = object()
    first: Any = sentinel
    store: Any = sentinel
    item: Any = sentinel
    for item in iterable:
        if store is sentinel:
            store = item
            first = item
        else:
            yield store, item
            store = item
    if close and first is not sentinel and item is not first:
        yield item, first


def suppress_zeros(s: str, leading: bool = False, trailing: bool = True):
    """Suppress trailing and/or leading ``0`` of string `s`.

    Args:
         s: data string
         leading: suppress leading ``0``
         trailing: suppress trailing ``0``

    """
    # is anything to do?
    if (not leading) and (not trailing):
        return s

    # if `s` represents zero
    if float(s) == 0.0:
        return "0"

    # preserve sign
    if s[0] in "-+":
        sign = s[0]
        s = s[1:]
    else:
        sign = ""

    # strip zeros
    if leading:
        s = s.lstrip("0")
    if trailing and "." in s:
        s = s.rstrip("0")

    # remove comma if no decimals follow
    if s[-1] in ".,":
        s = s[:-1]

    return sign + s


def normalize_text_angle(angle: float, fix_upside_down=True) -> float:
    """
    Normalizes text `angle` to the range from 0 to 360 degrees and fixes upside down text angles.

    Args:
        angle: text angle in degrees
        fix_upside_down: rotate upside down text angle about 180 degree

    """
    angle = angle % 360.0  # normalize angle (0 .. 360)
    if fix_upside_down and (90 < angle <= 270):  # flip text orientation
        angle -= 180
        angle = angle % 360.0  # normalize again
    return angle
