#  Copyright (c) 2021-2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterator, NamedTuple, Iterable, Optional
from difflib import SequenceMatcher
import enum

from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.types import DXFVertex, DXFTag

__all__ = ["diff_tags", "print_diff", "OpCode"]

# https://docs.python.org/3/library/difflib.html


class OpCode(enum.Enum):
    replace = enum.auto()
    delete = enum.auto()
    insert = enum.auto()
    equal = enum.auto()


class Operation(NamedTuple):
    opcode: OpCode
    i1: int
    i2: int
    j1: int
    j2: int


CONVERT = {
    "replace": OpCode.replace,
    "delete": OpCode.delete,
    "insert": OpCode.insert,
    "equal": OpCode.equal,
}


def convert_opcodes(opcodes) -> Iterator[Operation]:
    for tag, i1, i2, j1, j2 in opcodes:
        yield Operation(CONVERT[tag], i1, i2, j1, j2)


def round_tags(tags: Tags, ndigits: int) -> Iterator[DXFTag]:
    for tag in tags:
        if isinstance(tag, DXFVertex):
            yield DXFVertex(tag.code, (round(d, ndigits) for d in tag.value))
        elif isinstance(tag.value, float):
            yield DXFTag(tag.code, round(tag.value, ndigits))
        else:
            yield tag


def diff_tags(
    a: Tags, b: Tags, ndigits: Optional[int] = None
) -> Iterator[Operation]:
    if ndigits is not None:
        a = Tags(round_tags(a, ndigits))
        b = Tags(round_tags(b, ndigits))

    sequencer = SequenceMatcher(a=a, b=b)
    return convert_opcodes(sequencer.get_opcodes())


def print_diff(a: Tags, b: Tags, operations: Iterable[Operation]):
    t1: DXFTag
    t2: DXFTag
    print()
    for op in operations:
        if op.opcode == OpCode.insert:
            for t1 in b[op.j1 : op.j2]:
                print(f"insert a[{op.i1}:{op.i2}]: {t1}")
        elif op.opcode == OpCode.replace:
            for index, t1 in enumerate(a[op.i1 : op.i2], op.i1):
                print(f"replace a[{index}]: {t1}")
            for index, t2 in enumerate(b[op.j1 : op.j2], op.j1):
                print(f"     by b[{index}]: {t2}")
        elif op.opcode == OpCode.delete:
            for index, t1 in enumerate(a[op.i1 : op.i2], op.i1):
                print(f"delete a[{index}]: {t1}")
