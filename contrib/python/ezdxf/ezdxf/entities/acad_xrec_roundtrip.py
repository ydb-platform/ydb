# Copyright (c) 2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations

from ezdxf.entities import XRecord
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.types import DXFTag

__all__ = ["RoundtripXRecord"]

SECTION_MARKER_CODE = 102
NOT_FOUND = -1

class RoundtripXRecord:
    """Helper class for ACAD Roundtrip Data.

    The data is stored in an XRECORD, in sections separated by tags
    (102, "ACAD_SECTION_NAME").

    Example for inverted clipping path of SPATIAL_FILTER objects:

    ...
    100
    AcDbXrecord
    280
    1
    102
    ACAD_INVERTEDCLIP_ROUNDTRIP
    10
    399.725563048036
    20
    233.417786599994
    30
    0.0
    ...
    102
    ACAD_INVERTEDCLIP_ROUNDTRIP_COMPARE
    10
    399.725563048036
    20
    233.417786599994
    ...

    """

    def __init__(self, xrecord: XRecord | None = None) -> None:
        if xrecord is None:
            xrecord = XRecord()
        self.xrecord = xrecord

    def has_section(self, key: str) -> bool:
        """Returns True if an entry section for key is present."""
        for code, value in self.xrecord.tags:
            if code == SECTION_MARKER_CODE and value == key:
                return True
        return False

    def set_section(self, key: str, tags: Tags) -> None:
        """Set content of section `key` to `tags`. Replaces the content of an existing section."""
        xrecord_tags = self.xrecord.tags
        start, end = find_section(xrecord_tags, key)
        if start == NOT_FOUND:
            xrecord_tags.append(DXFTag(SECTION_MARKER_CODE, key))
            xrecord_tags.extend(tags)
        else:
            xrecord_tags[start + 1 : end] = tags

    def get_section(self, key: str) -> Tags:
        """Returns the content of section `key`."""
        xrecord_tags = self.xrecord.tags
        start, end = find_section(xrecord_tags, key)
        if start != NOT_FOUND:
            return xrecord_tags[start + 1 : end]
        return Tags()

    def discard(self, key: str) -> None:
        """Removes section `key`, section `key` doesn't have to exist."""
        xrecord_tags = self.xrecord.tags
        start, end = find_section(xrecord_tags, key)
        if start != NOT_FOUND:
            del xrecord_tags[start:end]


def find_section(tags: Tags, key: str) -> tuple[int, int]:
    """Returns the start- and end index of section `key`.

    Returns (-1, -1) if the section does not exist.
    """
    start = NOT_FOUND
    for index, tag in enumerate(tags):
        if tag.code != 102:
            continue
        if tag.value == key:
            start = index
        elif start != NOT_FOUND:
            return start, index
    if start != NOT_FOUND:
        return start, len(tags)
    return NOT_FOUND, NOT_FOUND
