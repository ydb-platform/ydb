# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, TYPE_CHECKING, Iterable, Any
import math
from ezdxf.math import UVec, Vec2, has_clockwise_orientation
from ezdxf.entities import LWPolyline, DXFEntity

if TYPE_CHECKING:
    from ezdxf.layouts import BaseLayout


REVCLOUD_PROPS = "RevcloudProps"
REQUIRED_BULGE = 0.520567050552


def points(
    vertices: Iterable[UVec],
    segment_length: float,
    *,
    bulge: float = REQUIRED_BULGE,  # required to be recognized as REVCLOUD
    start_width: float = 0.0,
    end_width: float = 0.0,
) -> list[Sequence[float]]:
    """Returns the points for a :class:`~ezdxf.entities.LWPolyline` entity to render a
    revision cloud, similar to the REVCLOUD command in CAD applications.

    Args:
        vertices: corner points of a polygon
        segment_length: approximate segment length
        bulge: LWPOLYLINE bulge value
        start_width: start width of the segment arc
        end_width: end width of the segment arc, CAD applications use 0.1 * segment_length
            for a calligraphy effect

    """
    if segment_length < 1e-6:
        raise ValueError("segment length too small.")
    _vertices = _to_vec2_list(vertices)
    lw_points: list[Sequence[float]] = []

    for s, e in zip(_vertices, _vertices[1:]):
        lw_points.append((s.x, s.y, start_width, end_width, bulge))
        diff = e - s
        length = diff.magnitude
        if length <= segment_length:
            continue

        count = math.ceil(length / segment_length)
        _segment_length = length / count
        offset = diff.normalize(_segment_length)
        for _ in range(count - 1):
            s += offset
            lw_points.append((s.x, s.y, start_width, end_width, bulge))
    return lw_points


def add_entity(
    layout: BaseLayout,
    vertices: Iterable[UVec],
    segment_length: float,
    *,
    calligraphy=True,
    dxfattribs: Any = None,
) -> LWPolyline:
    """Adds a revision cloud as :class:`~ezdxf.entities.LWPolyline` entity to `layout`,
    similar to the REVCLOUD command in CAD applications.

    Args:
        layout: target layout
        vertices: corner points of a polygon
        segment_length: approximate segment length
        calligraphy: ``True`` for a calligraphy effect
        dxfattribs: additional DXF attributes

    """
    _vertices = _to_vec2_list(vertices)
    bulge = REQUIRED_BULGE
    if has_clockwise_orientation(_vertices):
        bulge = -bulge

    end_width = segment_length * 0.1 if calligraphy else 0.0
    lw_points = points(_vertices, segment_length, bulge=bulge, end_width=end_width)
    lwp = layout.add_lwpolyline(lw_points, close=True, dxfattribs=dxfattribs)
    lwp.set_xdata(REVCLOUD_PROPS, [(1070, 0), (1040, segment_length)])
    doc = layout.doc
    if doc is not None and not doc.appids.has_entry(REVCLOUD_PROPS):
        doc.appids.add(REVCLOUD_PROPS)
    return lwp


def is_revcloud(entity: DXFEntity) -> bool:
    """Returns ``True`` when the given entity represents a revision cloud."""
    if not isinstance(entity, LWPolyline):
        return False
    lwpolyline: LWPolyline = entity
    if not lwpolyline.is_closed:
        return False
    if not lwpolyline.has_xdata(REVCLOUD_PROPS):
        return False
    return all(
        abs(REQUIRED_BULGE - abs(p[0])) < 0.02
        for p in lwpolyline.get_points(format="b")
    )


def _to_vec2_list(vertices: Iterable[UVec]) -> list[Vec2]:
    _vertices: list[Vec2] = Vec2.list(vertices)
    if len(_vertices) < 3:
        raise ValueError("3 or more points required.")
    if not _vertices[0].isclose(_vertices[-1]):
        _vertices.append(_vertices[0])
    if len(_vertices) < 4:
        raise ValueError("3 or more points required.")
    return _vertices
