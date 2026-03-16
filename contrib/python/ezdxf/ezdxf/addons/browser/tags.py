# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from typing import Iterable, Sequence

from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.types import (
    DXFTag,
    DXFVertex,
    POINT_CODES,
    TYPE_TABLE,
)


def tag_compiler(tags: Tags) -> Iterable[DXFTag]:
    """Special tag compiler for the DXF browser.

    This compiler should never fail and always return printable tags:

        - invalid point coordinates are returned as float("nan")
        - invalid ints are returned as string
        - invalid floats are returned as string

    """

    def to_float(v: str) -> float:
        try:
            return float(v)
        except ValueError:
            return float("NaN")

    count = len(tags)
    index = 0
    while index < count:
        code, value = tags[index]
        if code in POINT_CODES:
            try:
                y_code, y_value = tags[index + 1]
            except IndexError:  # x-coord as last tag
                yield DXFTag(code, to_float(value))
                return

            if y_code != code + 10:  # not an y-coord?
                yield DXFTag(code, to_float(value))  # x-coord as single tag
                index += 1
                continue

            try:
                z_code, z_value = tags[index + 2]
            except IndexError:  # no z-coord exist
                z_code = 0
                z_value = 0
            point: Sequence[float]
            if z_code == code + 20:  # is a z-coord?
                point = (to_float(value), to_float(y_value), to_float(z_value))
                index += 3
            else:  # a valid 2d point(x, y)
                point = (to_float(value), to_float(y_value))
                index += 2
            yield DXFVertex(code, point)
        else:  # a single tag
            try:
                if code == 0:
                    value = value.strip()
                yield DXFTag(code, TYPE_TABLE.get(code, str)(value))
            except ValueError:
                yield DXFTag(code, str(value))  # just as string
            index += 1


def compile_tags(tags: Tags) -> Tags:
    return Tags(tag_compiler(tags))
