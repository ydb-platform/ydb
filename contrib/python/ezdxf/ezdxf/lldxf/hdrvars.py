# Copyright (c) 2010-2022, Manfred Moitzi
# License: MIT License
from typing import Sequence, Union, Callable, Any, NamedTuple, Optional
from .types import DXFVertex, DXFTag, cast_tag_value


def SingleValue(value: Union[str, float], code: int = 1) -> DXFTag:
    return DXFTag(code, cast_tag_value(code, value))


def Point2D(value: Sequence[float]) -> DXFVertex:
    return DXFVertex(10, (value[0], value[1]))


def Point3D(value: Sequence[float]) -> DXFVertex:
    return DXFVertex(10, (value[0], value[1], value[2]))


class HeaderVarDef(NamedTuple):
    name: str
    code: int
    factory: Callable[[Any], Any]
    mindxf: str
    maxdxf: str
    priority: int
    default: Optional[Any] = None
