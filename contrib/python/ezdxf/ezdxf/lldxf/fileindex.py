# Copyright (c) 2020-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, NamedTuple, BinaryIO

from .const import DXFStructureError
from ezdxf.tools.codepage import toencoding


class IndexEntry(NamedTuple):
    code: int
    value: str
    location: int
    line: int


class FileStructure:
    """DXF file structure representation stored as file locations.

    Store all DXF structure tags and some other tags as :class:`IndexEntry`
    tuples:

        - code: group code
        - value: tag value as string
        - location: file location as int
        - line: line number as int

    Indexed tags:

        - structure tags, every tag with group code 0
        - section names, (2, name) tag following a (0, SECTION) tag
        - entity handle tags with group code 5, the DIMSTYLE handle group code
          105 is also stored as group code 5

    """

    def __init__(self, filename: str):
        # stores the file system name of the DXF document.
        self.filename: str = filename
        # DXF version if header variable $ACADVER is present, default is DXFR12
        self.version: str = "AC1009"
        # Python encoding required to read the DXF document as text file.
        self.encoding: str = "cp1252"
        self.index: list[IndexEntry] = []

    def print(self) -> None:
        print(f"Filename: {self.filename}")
        print(f"DXF Version: {self.version}")
        print(f"encoding: {self.encoding}")
        for entry in self.index:
            print(f"Line: {entry.line} - ({entry.code}, {entry.value})")

    def get(self, code: int, value: str, start: int = 0) -> int:
        """Returns index of first entry matching `code` and `value`."""
        self_index = self.index
        index: int = start
        count: int = len(self_index)
        while index < count:
            entry = self_index[index]
            if entry.code == code and entry.value == value:
                return index
            index += 1
        raise ValueError(f"No entry for tag ({code}, {value}) found.")

    def fetchall(
        self, code: int, value: str, start: int = 0
    ) -> Iterable[IndexEntry]:
        """Iterate over all specified entities.

        e.g. fetchall(0, 'LINE') returns an iterator for all LINE entities.

        """
        for entry in self.index[start:]:
            if entry.code == code and entry.value == value:
                yield entry


def load(filename: str) -> FileStructure:
    """Load DXF file structure for file `filename`, the file has to be seekable.

    Args:
        filename: file system file name

    Raises:
        DXFStructureError: Invalid or incomplete DXF file.

    """
    file_structure = FileStructure(filename)
    file: BinaryIO = open(filename, mode="rb")
    line: int = 1
    eof: bool = False
    header: bool = False
    index: list[IndexEntry] = []
    prev_code: int = -1
    prev_value: bytes = b""
    structure = None  # the current structure tag: 'SECTION', 'LINE', ...

    def load_tag() -> tuple[int, bytes]:
        nonlocal line
        try:
            code = int(file.readline())
        except ValueError:
            raise DXFStructureError(f"Invalid group code in line {line}")

        if code < 0 or code > 1071:
            raise DXFStructureError(f"Invalid group code {code} in line {line}")
        value = file.readline().rstrip(b"\r\n")
        line += 2
        return code, value

    def load_header_var() -> str:
        _, value = load_tag()
        return value.decode()

    while not eof:
        location = file.tell()
        tag_line = line
        try:
            code, value = load_tag()
            if header and code == 9:
                if value == b"$ACADVER":
                    file_structure.version = load_header_var()
                elif value == b"$DWGCODEPAGE":
                    file_structure.encoding = toencoding(load_header_var())
                continue
        except IOError:
            break

        if code == 0:
            # All structure tags have group code == 0, store file location
            structure = value
            index.append(IndexEntry(0, value.decode(), location, tag_line))
            eof = value == b"EOF"

        elif code == 2 and prev_code == 0 and prev_value == b"SECTION":
            # Section name is the tag (2, name) following the (0, SECTION) tag.
            header = value == b"HEADER"
            index.append(IndexEntry(2, value.decode(), location, tag_line))

        elif code == 5 and structure != b"DIMSTYLE":
            # Entity handles have always group code 5.
            index.append(IndexEntry(5, value.decode(), location, tag_line))

        elif code == 105 and structure == b"DIMSTYLE":
            # Except the DIMSTYLE table entry has group code 105.
            index.append(IndexEntry(5, value.decode(), location, tag_line))

        prev_code = code
        prev_value = value

    file.close()
    if not eof:
        raise DXFStructureError(f"Unexpected end of file.")

    if file_structure.version >= "AC1021":  # R2007 and later
        file_structure.encoding = "utf-8"
    file_structure.index = index
    return file_structure
