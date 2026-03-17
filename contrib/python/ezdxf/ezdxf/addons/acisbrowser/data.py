#  Copyright (c) 2022-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterator, Sequence

from ezdxf.acis.sat import parse_sat, SatEntity
from ezdxf.acis.sab import parse_sab, SabEntity


class AcisData:
    def __init__(self, name: str = "unknown", handle: str = ""):
        self.lines: list[str] = []
        self.name: str = name
        self.handle: str = handle


class BinaryAcisData(AcisData):
    def __init__(self, data: bytes, name: str, handle: str):
        super().__init__(name, handle)
        self.lines = list(make_sab_records(data))


class TextAcisData(AcisData):
    def __init__(self, data: Sequence[str], name: str, handle: str):
        super().__init__(name, handle)
        self.lines = list(make_sat_records(data))


def ptr_str(e):
    return "~" if e.is_null_ptr else str(e)


def make_sat_records(data: Sequence[str]) -> Iterator[str]:
    builder = parse_sat(data)
    yield from builder.header.dumps()
    builder.reset_ids()
    for entity in builder.entities:
        content = [str(entity)]
        content.append(ptr_str(entity.attributes))
        for field in entity.data:
            if isinstance(field, SatEntity):
                content.append(ptr_str(field))
            else:
                content.append(field)
        yield " ".join(content)


def make_sab_records(data: bytes) -> Iterator[str]:
    builder = parse_sab(data)
    yield from builder.header.dumps()
    builder.reset_ids()
    for entity in builder.entities:
        content = [str(entity)]
        content.append(ptr_str(entity.attributes))
        for tag in entity.data:
            if isinstance(tag.value, SabEntity):
                content.append(ptr_str(tag.value))
            else:
                content.append(f"{tag.value}<{tag.tag}>")
        yield " ".join(content)
