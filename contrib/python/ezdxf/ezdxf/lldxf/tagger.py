# Copyright (c) 2016-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, TextIO, Iterator, Any, Optional, Sequence
import struct
from .types import (
    DXFTag,
    DXFVertex,
    DXFBinaryTag,
    BYTES,
    INT16,
    INT32,
    INT64,
    DOUBLE,
    POINT_CODES,
    TYPE_TABLE,
    BINARY_DATA,
    is_point_code,
)
from .const import DXFStructureError
from ezdxf.tools.codepage import toencoding


def internal_tag_compiler(s: str) -> Iterable[DXFTag]:
    """Yields DXFTag() from trusted (internal) source - relies on
    well-formed and error free DXF format. Does not skip comment
    tags (group code == 999).

    Args:
        s: DXF unicode string, lines separated by universal line endings '\n'

    """
    assert isinstance(s, str)
    lines: list[str] = s.split("\n")
    # split() creates an extra item, if s ends with '\n',
    # but lines[-1] can be an empty string!!!
    if s.endswith("\n"):
        lines.pop()
    pos: int = 0
    count: int = len(lines)
    point: tuple[float, ...]
    while pos < count:
        code = int(lines[pos])
        value = lines[pos + 1]
        pos += 2
        if code in POINT_CODES:
            # next tag; y-axis is mandatory - internal_tag_compiler relies on
            # well formed DXF strings:
            y = lines[pos + 1]
            pos += 2
            if pos < count:
                # next tag; z coordinate just for 3d points
                z_code = int(lines[pos])
                z = lines[pos + 1]
            else:  # if string s ends with a 2d point
                z_code, z = -1, ""
            if z_code == code + 20:  # 3d point
                pos += 2
                point = (float(value), float(y), float(z))
            else:  # 2d point
                point = (float(value), float(y))
            yield DXFVertex(code, point)  # 2d/3d point
        elif code in BINARY_DATA:
            yield DXFBinaryTag.from_string(code, value)
        else:  # single value tag: int, float or string
            yield DXFTag(code, TYPE_TABLE.get(code, str)(value))


# No performance advantage by processing binary data!
#
# Profiling result for just reading DXF data (profiling/raw_data_reading.py):
# Loading the example file "torso_uniform.dxf" (50MB) by readline() from a
# text stream with decoding takes ~0.65 seconds longer than loading the same
# file as binary data.
#
# Text :1.30s vs Binary data: 0.65s)
# This is twice the time, but without any processing, ascii_tags_loader() takes
# ~5.3 seconds to process this file.
#
# And this performance advantage is more than lost by the necessary decoding
# of the binary data afterwards, even much fewer strings have to be decoded,
# because numeric data like group codes and vertices doesn't need to be
# decoded.
#
# I assume the runtime overhead for calling Python functions is the reason.


def ascii_tags_loader(stream: TextIO, skip_comments: bool = True) -> Iterator[DXFTag]:
    """Yields :class:``DXFTag`` objects from a text `stream` (untrusted
    external source) and does not optimize coordinates. Comment tags (group
    code == 999) will be skipped if argument `skip_comments` is `True`.
    ``DXFTag.code`` is always an ``int`` and ``DXFTag.value`` is always an
    unicode string without a trailing '\n'.
    Works with file system streams and :class:`StringIO` streams, only required
    feature is the :meth:`readline` method.

    Args:
        stream: text stream
        skip_comments: skip comment tags (group code == 999) if `True`

    Raises:
        DXFStructureError: Found invalid group code.

    """
    line: int = 1
    eof = False
    yield_comments = not skip_comments
    # localize attributes
    readline = stream.readline
    _DXFTag = DXFTag
    # readline() returns an empty string at EOF, not exception will be raised!
    while not eof:
        code: str = readline()
        if code:  # empty string indicates EOF
            try:
                group_code = int(code)
            except ValueError:
                raise DXFStructureError(f'Invalid group code "{code}" at line {line}.')
        else:
            return

        value: str = readline()
        if value:  # empty string indicates EOF
            value = value.rstrip("\n")
            if group_code == 0 and value == "EOF":
                eof = True  # yield EOF tag but ignore any data beyond EOF
            if group_code != 999 or yield_comments:
                yield _DXFTag(group_code, value)
            line += 2
        else:
            return


def binary_tags_loader(
    data: bytes, errors: str = "surrogateescape"
) -> Iterator[DXFTag]:
    """Yields :class:`DXFTag` or :class:`DXFBinaryTag` objects from binary DXF
    `data` (untrusted external source) and does not optimize coordinates.
    ``DXFTag.code`` is always an ``int`` and ``DXFTag.value`` is either an
    unicode string,``float``, ``int`` or ``bytes`` for binary chunks.

    Args:
        data: binary DXF data
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD: "\ufffd"
            - "strict" to raise an :class:`UnicodeDecodeError`

    Raises:
        DXFStructureError: Not a binary DXF file
        DXFVersionError: Unsupported DXF version
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    if data[:22] != b"AutoCAD Binary DXF\r\n\x1a\x00":
        raise DXFStructureError("Not a binary DXF data structure.")

    def scan_params():
        dxfversion = "AC1009"
        encoding = "cp1252"
        try:
            # Limit search to first 1024 bytes - an arbitrary number
            # start index for 1-byte group code
            start = data.index(b"$ACADVER", 22, 1024) + 10
        except ValueError:
            pass  # HEADER var $ACADVER not present
        else:
            if data[start] != 65:  # not 'A' = 2-byte group code
                start += 1
            dxfversion = data[start : start + 6].decode()

        if dxfversion >= "AC1021":
            encoding = "utf8"
        else:
            try:
                # Limit search to first 1024 bytes - an arbitrary number
                # start index for 1-byte group code
                start = data.index(b"$DWGCODEPAGE", 22, 1024) + 14
            except ValueError:
                pass  # HEADER var $DWGCODEPAGE not present
            else:  # name schema is 'ANSI_xxxx'
                if data[start] != 65:  # not 'A' = 2-byte group code
                    start += 1
                end = start + 5
                while data[end] != 0:
                    end += 1
                codepage = data[start:end].decode()
                encoding = toencoding(codepage)

        return encoding, dxfversion

    encoding, dxfversion = scan_params()
    r12 = dxfversion <= "AC1009"
    index: int = 22
    data_length: int = len(data)
    unpack = struct.unpack_from
    value: Any

    while index < data_length:
        # decode next group code
        code = data[index]
        if r12:
            if code == 255:  # extended data
                code = (data[index + 2] << 8) | data[index + 1]
                index += 3
            else:
                index += 1
        else:  # 2-byte group code
            code = (data[index + 1] << 8) | code
            index += 2

        # decode next value
        if code in BINARY_DATA:
            length = data[index]
            index += 1
            value = data[index : index + length]
            index += length
            yield DXFBinaryTag(code, value)
        else:
            if code in INT16:
                value = unpack("<h", data, offset=index)[0]
                index += 2
            elif code in DOUBLE:
                value = unpack("<d", data, offset=index)[0]
                index += 8
            elif code in INT32:
                value = unpack("<i", data, offset=index)[0]
                index += 4
            elif code in INT64:
                value = unpack("<q", data, offset=index)[0]
                index += 8
            elif code in BYTES:
                value = data[index]
                index += 1
            else:  # zero terminated string
                start_index = index
                end_index = data.index(b"\x00", start_index)
                s = data[start_index:end_index]
                index = end_index + 1
                value = s.decode(encoding, errors=errors)
            yield DXFTag(code, value)


# invalid point codes if not part of a point started with 1010, 1011, 1012, 1013
INVALID_POINT_CODES = {1020, 1021, 1022, 1023, 1030, 1031, 1032, 1033}


def tag_compiler(tags: Iterator[DXFTag]) -> Iterator[DXFTag]:
    """Compiles DXF tag values imported by ascii_tags_loader() into Python
    types.

    Raises DXFStructureError() for invalid float values and invalid coordinate
    values.

    Expects DXF coordinates written in x, y[, z] order, this is not required by
    the DXF standard, but nearly all CAD applications write DXF coordinates that
    (sane) way, there are older CAD applications (namely an older QCAD version)
    that write LINE coordinates in x1, x2, y1, y2 order, which does not work
    with tag_compiler(). For this cases use tag_reorder_layer() from the repair
    module to reorder the LINE coordinates::

        tag_compiler(tag_reorder_layer(ascii_tags_loader(stream)))

    Args:
        tags: DXF tag generator e.g. ascii_tags_loader()

    Raises:
        DXFStructureError: Found invalid DXF tag or unexpected coordinate order.

    """

    def error_msg(tag):
        return (
            f'Invalid tag (code={tag.code}, value="{tag.value}") ' f"near line: {line}."
        )

    undo_tag: Optional[DXFTag] = None
    line: int = 0
    point: tuple[float, ...]
    # Silencing mypy by "type: ignore", because this is a work horse function
    # and should not be slowed down by isinstance(...) checks or unnecessary
    # cast() calls
    while True:
        try:
            if undo_tag is not None:
                x = undo_tag
                undo_tag = None
            else:
                x = next(tags)
                line += 2
            code: int = x.code
            if code in POINT_CODES:
                # y-axis is mandatory
                y = next(tags)
                line += 2
                if y.code != code + 10:  # like 20 for base x-code 10
                    raise DXFStructureError(
                        f"Missing required y coordinate near line: {line}."
                    )
                # z-axis just for 3d points
                z = next(tags)
                line += 2
                try:
                    # z-axis like (30, 0.0) for base x-code 10
                    if z.code == code + 20:
                        point = (float(x.value), float(y.value), float(z.value))
                    else:
                        point = (float(x.value), float(y.value))
                        undo_tag = z
                except ValueError:
                    raise DXFStructureError(
                        f"Invalid floating point values near line: {line}."
                    )
                yield DXFVertex(code, point)
            elif code in BINARY_DATA:
                # Maybe pre compiled in low level tagger (binary DXF):
                if isinstance(x, DXFBinaryTag):
                    tag = x
                else:
                    try:
                        tag = DXFBinaryTag.from_string(code, x.value)
                    except ValueError:
                        raise DXFStructureError(
                            f"Invalid binary data near line: {line}."
                        )
                yield tag
            else:  # Just a single tag
                try:
                    # Fast path!
                    if code == 0:
                        value = x.value.strip()
                    else:
                        value = x.value
                    yield DXFTag(code, TYPE_TABLE.get(code, str)(value))
                except ValueError:
                    # ProE stores int values as floats :((
                    if TYPE_TABLE.get(code, str) is int:
                        try:
                            yield DXFTag(code, int(float(x.value)))
                        except ValueError:
                            raise DXFStructureError(error_msg(x))
                    else:
                        raise DXFStructureError(error_msg(x))
        except StopIteration:
            return


def json_tag_loader(
    data: Sequence[Any], skip_comments: bool = True
) -> Iterator[DXFTag]:
    """Yields :class:``DXFTag`` objects from a JSON data structure (untrusted
    external source) and does not optimize coordinates. Comment tags (group
    code == 999) will be skipped if argument `skip_comments` is `True`.
    ``DXFTag.code`` is always an ``int`` and ``DXFTag.value`` is always an
    unicode string without a trailing ``\n``.

    The expected JSON format is a list of [group-code, value] pairs where each pair is
    a DXF tag. The `compact` and the `verbose` format is supported. 

    Args:
        data: JSON data structure as a sequence of [group-code, value] pairs
        skip_comments: skip comment tags (group code == 999) if `True`

    Raises:
        DXFStructureError: Found invalid group code or value type.

    """
    yield_comments = not skip_comments
    _DXFTag = DXFTag
    for tag_number, (code, value) in enumerate(data):
        if not isinstance(code, int):
            raise DXFStructureError(
                f'Invalid group code "{code}" in tag number {tag_number}.'
            )
        if is_point_code(code) and isinstance(value, (list, tuple)):
            # yield coordinates as single tags
            for index, coordinate in enumerate(value):
                yield _DXFTag(code + index * 10, coordinate)
            continue
        if code != 999 or yield_comments:
            yield _DXFTag(code, value)
        if code == 0 and value == "EOF":
            return
