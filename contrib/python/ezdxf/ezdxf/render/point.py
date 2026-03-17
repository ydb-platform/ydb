# Copyright (c) 2020-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import cast
import math
from ezdxf.entities import factory, Point, DXFGraphic
from ezdxf.math import Vec3, UCS, NULLVEC


def virtual_entities(
    point: Point, pdsize: float = 1, pdmode: int = 0
) -> list[DXFGraphic]:
    """Yields point graphic as DXF primitives LINE and CIRCLE entities.
    The dimensionless point is rendered as zero-length line!

    Check for this condition::

        e.dxftype() == 'LINE' and e.dxf.start.isclose(e.dxf.end)

    if the rendering engine can't handle zero-length lines.


    Args:
        point: DXF POINT entity
        pdsize: point size in drawing units
        pdmode: point styling mode, see :class:`~ezdxf.entities.Point` class

    """

    def add_line_symmetrical(offset: Vec3):
        dxfattribs["start"] = ucs.to_wcs(-offset)
        dxfattribs["end"] = ucs.to_wcs(offset)
        entities.append(cast(DXFGraphic, factory.new("LINE", dxfattribs)))

    def add_line(s: Vec3, e: Vec3):
        dxfattribs["start"] = ucs.to_wcs(s)
        dxfattribs["end"] = ucs.to_wcs(e)
        entities.append(cast(DXFGraphic, factory.new("LINE", dxfattribs)))

    center = point.dxf.location
    # This is not a real OCS! Defines just the point orientation,
    # location is in WCS!
    ocs = point.ocs()
    ucs = UCS(origin=center, ux=ocs.ux, uz=ocs.uz)

    # The point angle is clockwise oriented:
    ucs = ucs.rotate_local_z(math.radians(-point.dxf.angle))

    entities: list[DXFGraphic] = []
    gfx = point.graphic_properties()

    radius = pdsize * 0.5
    has_circle = bool(pdmode & 32)
    has_square = bool(pdmode & 64)
    style = pdmode & 7

    dxfattribs = dict(gfx)
    if style == 0:  # . dimensionless point as zero-length line
        add_line_symmetrical(NULLVEC)
    # style == 1: no point symbol
    elif style == 2:  # + cross
        add_line_symmetrical(Vec3(pdsize, 0))
        add_line_symmetrical(Vec3(0, pdsize))
    elif style == 3:  # x cross
        add_line_symmetrical(Vec3(pdsize, pdsize))
        add_line_symmetrical(Vec3(pdsize, -pdsize))
    elif style == 4:  # ' tick
        add_line(NULLVEC, Vec3(0, radius))
    if has_square:
        x1 = -radius
        x2 = radius
        y1 = -radius
        y2 = radius
        add_line(Vec3(x1, y1), Vec3(x2, y1))
        add_line(Vec3(x2, y1), Vec3(x2, y2))
        add_line(Vec3(x2, y2), Vec3(x1, y2))
        add_line(Vec3(x1, y2), Vec3(x1, y1))
    if has_circle:
        dxfattribs = dict(gfx)
        if point.dxf.hasattr("extrusion"):
            dxfattribs["extrusion"] = ocs.uz
            dxfattribs["center"] = ocs.from_wcs(center)
        else:
            dxfattribs["center"] = center
        dxfattribs["radius"] = radius
        entities.append(cast(DXFGraphic, factory.new("CIRCLE", dxfattribs)))

    return entities
