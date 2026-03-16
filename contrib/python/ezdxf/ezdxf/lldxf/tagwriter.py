# Copyright (c) 2018-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Any, TextIO, TYPE_CHECKING, Union, Iterable, BinaryIO
import abc

from .types import TAG_STRING_FORMAT, cast_tag_value, DXFVertex
from .types import BYTES, INT16, INT32, INT64, DOUBLE, BINARY_DATA
from .tags import DXFTag, Tags
from .const import LATEST_DXF_VERSION
from ezdxf.tools import take2
import struct

if TYPE_CHECKING:
    from ezdxf.lldxf.extendedtags import ExtendedTags
    from ezdxf.entities import DXFEntity

__all__ = [
    "TagWriter",
    "BinaryTagWriter",
    "TagCollector",
    "basic_tags_from_text",
    "AbstractTagWriter",
]
CRLF = b"\r\n"


class AbstractTagWriter:
    # Options for functions using an inherited class for DXF export:
    dxfversion = LATEST_DXF_VERSION
    write_handles = True
    # Force writing optional values if equal to default value when True.
    # True is only used for testing scenarios!
    force_optional = False

    # Start of low level interface:
    @abc.abstractmethod
    def write_tag(self, tag: DXFTag) -> None: ...

    @abc.abstractmethod
    def write_tag2(self, code: int, value: Any) -> None: ...

    @abc.abstractmethod
    def write_str(self, s: str) -> None: ...

    # End of low level interface

    # Tag export based on low level tag export:
    def write_tags(self, tags: Union[Tags, ExtendedTags]) -> None:
        for tag in tags:
            self.write_tag(tag)

    def write_vertex(self, code: int, vertex: Iterable[float]) -> None:
        for index, value in enumerate(vertex):
            self.write_tag2(code + index * 10, value)


class TagWriter(AbstractTagWriter):
    """Writes DXF tags into a text stream."""

    def __init__(
        self,
        stream: TextIO,
        dxfversion: str = LATEST_DXF_VERSION,
        write_handles: bool = True,
    ):
        self._stream: TextIO = stream
        self.dxfversion: str = dxfversion
        self.write_handles: bool = write_handles
        self.force_optional: bool = False

    # Start of low level interface:
    def write_tag(self, tag: DXFTag) -> None:
        self._stream.write(tag.dxfstr())

    def write_tag2(self, code: int, value: Any) -> None:
        self._stream.write(TAG_STRING_FORMAT % (code, value))

    def write_str(self, s: str) -> None:
        self._stream.write(s)

    # End of low level interface

    def write_vertex(self, code: int, vertex: Iterable[float]) -> None:
        """Optimized vertex export."""
        write = self._stream.write
        for index, value in enumerate(vertex):
            write(TAG_STRING_FORMAT % (code + index * 10, value))


class BinaryTagWriter(AbstractTagWriter):
    """Write binary encoded DXF tags into a binary stream.

    .. warning::

        DXF files containing ``ACSH_SWEEP_CLASS`` entities and saved as Binary
        DXF by `ezdxf` can not be opened with AutoCAD, this is maybe also true
        for other 3rd party entities. BricsCAD opens this binary DXF files
        without complaining, but saves the ``ACSH_SWEEP_CLASS`` entities as
        ``ACAD_PROXY_OBJECT`` when writing back, so error analyzing is not
        possible without the full version of AutoCAD.

        I have no clue why, because converting this DXF files from binary
        format back to ASCII format by `ezdxf` produces a valid DXF for
        AutoCAD - so all required information is preserved.

        Two examples available:

            - AutodeskSamples\visualization_-_condominium_with_skylight.dxf
            - AutodeskSamples\visualization_-_conference_room.dxf

    """

    def __init__(
        self,
        stream: BinaryIO,
        dxfversion=LATEST_DXF_VERSION,
        write_handles: bool = True,
        encoding="utf8",
    ):
        self._stream = stream
        self.dxfversion = dxfversion
        self.write_handles = write_handles
        self._encoding = encoding  # output encoding
        self._r12 = self.dxfversion <= "AC1009"

    def write_signature(self) -> None:
        self._stream.write(b"AutoCAD Binary DXF\r\n\x1a\x00")

    # Start of low level interface:
    def write_tag(self, tag: DXFTag) -> None:
        if isinstance(tag, DXFVertex):
            for code, value in tag.dxftags():
                self.write_tag2(code, value)
        else:
            self.write_tag2(tag.code, tag.value)

    def write_str(self, s: str) -> None:
        data = s.split("\n")
        for code, value in take2(data):
            self.write_tag2(int(code), value)

    def write_tag2(self, code: int, value: Any) -> None:
        # Binary DXF files do not support comments!
        assert code != 999
        if code in BINARY_DATA:
            self._write_binary_chunks(code, value)
            return
        stream = self._stream

        # write group code
        if self._r12:
            # Special group code handling if DXF R12 and older
            if code >= 1000:  # extended data
                stream.write(b"\xff")
                # always 2-byte group code for extended data
                stream.write(code.to_bytes(2, "little"))
            else:
                stream.write(code.to_bytes(1, "little"))
        else:  # for R2000+ do not need a leading 0xff in front of extended data
            stream.write(code.to_bytes(2, "little"))
        # write tag content
        if code in BYTES:
            stream.write(int(value).to_bytes(1, "little"))
        elif code in INT16:
            stream.write(int(value).to_bytes(2, "little", signed=True))
        elif code in INT32:
            stream.write(int(value).to_bytes(4, "little", signed=True))
        elif code in INT64:
            stream.write(int(value).to_bytes(8, "little", signed=True))
        elif code in DOUBLE:
            stream.write(struct.pack("<d", float(value)))
        else:  # write zero terminated string
            stream.write(str(value).encode(self._encoding, errors="dxfreplace"))
            stream.write(b"\x00")

    # End of low level interface

    def _write_binary_chunks(self, code: int, data: bytes) -> None:
        # Split binary data into small chunks, 127 bytes is the
        # regular size of binary data in ASCII DXF files.
        CHUNK_SIZE = 127
        index = 0
        size = len(data)
        stream = self._stream

        while index < size:
            # write group code
            if self._r12 and code >= 1000:  # extended data, just 1004?
                stream.write(b"\xff")  # extended data marker
            # binary data does not exist in regular R12 entities,
            # only 2-byte group codes required
            stream.write(code.to_bytes(2, "little"))

            # write max CHUNK_SIZE bytes of binary data in one tag
            chunk = data[index : index + CHUNK_SIZE]
            # write actual chunk size
            stream.write(len(chunk).to_bytes(1, "little"))
            stream.write(chunk)
            index += CHUNK_SIZE


class TagCollector(AbstractTagWriter):
    """Collect DXF tags as DXFTag() entities for testing."""

    def __init__(
        self,
        dxfversion: str = LATEST_DXF_VERSION,
        write_handles: bool = True,
        optional: bool = True,
    ):
        self.tags: list[DXFTag] = []
        self.dxfversion: str = dxfversion
        self.write_handles: bool = write_handles
        self.force_optional: bool = optional

    # Start of low level interface:
    def write_tag(self, tag: DXFTag) -> None:
        if hasattr(tag, "dxftags"):
            self.tags.extend(tag.dxftags())
        else:
            self.tags.append(tag)

    def write_tag2(self, code: int, value: Any) -> None:
        self.tags.append(DXFTag(code, cast_tag_value(int(code), value)))

    def write_str(self, s: str) -> None:
        self.write_tags(Tags.from_text(s))

    # End of low level interface

    def has_all_tags(self, other: TagCollector):
        return all(tag in self.tags for tag in other.tags)

    def reset(self):
        self.tags = []

    @classmethod
    def dxftags(cls, entity: DXFEntity, dxfversion=LATEST_DXF_VERSION):
        collector = cls(dxfversion=dxfversion)
        entity.export_dxf(collector)
        return Tags(collector.tags)


def basic_tags_from_text(text: str) -> list[DXFTag]:
    """Returns all tags from `text` as basic DXFTags(). All complex tags are
    resolved into basic (code, value) tags (e.g. DXFVertex(10, (1, 2, 3)) ->
    DXFTag(10, 1), DXFTag(20, 2), DXFTag(30, 3).

    Args:
        text: DXF data as string

    Returns: List of basic DXF tags (code, value)

    """
    collector = TagCollector()
    collector.write_tags(Tags.from_text(text))
    return collector.tags


class JSONTagWriter(AbstractTagWriter):
    """Writes DXF tags in JSON format into a text stream.

    The `compact` format is a list of ``[group-code, value]`` pairs where each pair is 
    a DXF tag. The group-code has to be an integer and the value has to be a string, 
    integer, float or list of floats for vertices. 

    The `verbose` format (`compact` is ``False``) is a list of ``[group-code, value]`` 
    pairs where each pair is a 1:1 representation of a DXF tag. The group-code has to be 
    an integer and the value has to be a string.

    """

    JSON_HEADER = "[\n"
    JSON_STRING = '[{0}, "{1}"],\n'
    JSON_NUMBER = '[{0}, {1}],\n'
    VERTEX_TAG_FORMAT = "[{0}, {1}],\n"

    def __init__(
        self,
        stream: TextIO,
        dxfversion: str = LATEST_DXF_VERSION,
        write_handles=True,
        compact=True,
    ):
        self._stream = stream
        self.dxfversion = str(dxfversion)
        self.write_handles = bool(write_handles)
        self.force_optional = False
        self.compact = bool(compact)
        self._stream.write(self.JSON_HEADER)

    def write_tag(self, tag: DXFTag) -> None:
        if isinstance(tag, DXFVertex):
            if self.compact:
                vertex = ",".join(str(value) for _, value in tag.dxftags())
                self._stream.write(
                    self.VERTEX_TAG_FORMAT.format(tag.code, f"[{vertex}]")
                )
            else:
                for code, value in tag.dxftags():
                    self.write_tag2(code, value)
        else:
            self.write_tag2(tag.code, tag.value)

    def write_tag2(self, code: int, value: Any) -> None:
        if code == 0 and value == "EOF":
            self._stream.write('[0, "EOF"]\n]\n')  # no trailing comma!
            return
        if self.compact and isinstance(value, (float, int)):
            self._stream.write(self.JSON_NUMBER.format(code, value))
            return
        self._stream.write(self.JSON_STRING.format(code, value))

    def write_str(self, s: str) -> None:
        self.write_tags(Tags.from_text(s))
