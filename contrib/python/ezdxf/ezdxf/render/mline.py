# Copyright (c) 2020-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, cast, Sequence, Any
from itertools import chain
from ezdxf.entities import factory, MLineStyle
from ezdxf.math import Vec3, OCS
import logging

if TYPE_CHECKING:
    from ezdxf.entities import MLine, DXFGraphic, Hatch, Line, Arc

__all__ = ["virtual_entities"]
logger = logging.getLogger("ezdxf")


# The MLINE geometry stored in vertices, is the final geometry,
# scaling factor, justification and MLineStyle settings are already
# applied.


def _dxfattribs(mline) -> dict[str, Any]:
    attribs = mline.graphic_properties()
    # True color value of MLINE is ignored by CAD applications:
    if "true_color" in attribs:
        del attribs["true_color"]
    return attribs


def virtual_entities(mline: MLine) -> list[DXFGraphic]:
    """Yields 'virtual' parts of MLINE as LINE, ARC and HATCH entities.

    These entities are located at the original positions, but are not stored
    in the entity database, have no handle and are not assigned to any
    layout.
    """

    def filling() -> Hatch:
        attribs = _dxfattribs(mline)
        attribs["color"] = style.dxf.fill_color
        attribs["elevation"] = Vec3(ocs.from_wcs(bottom_border[0])).replace(
            x=0.0, y=0.0
        )
        attribs["extrusion"] = mline.dxf.extrusion
        hatch = cast("Hatch", factory.new("HATCH", dxfattribs=attribs, doc=doc))
        bulges: list[float] = [0.0] * (len(bottom_border) * 2)
        points = chain(
            Vec3.generate(ocs.points_from_wcs(bottom_border)),
            Vec3.generate(ocs.points_from_wcs(reversed(top_border))),
        )
        if not closed:
            if style.get_flag_state(style.END_ROUND):
                bulges[len(bottom_border) - 1] = 1.0
            if style.get_flag_state(style.START_ROUND):
                bulges[-1] = 1.0
        lwpoints = ((v.x, v.y, bulge) for v, bulge in zip(points, bulges))
        hatch.paths.add_polyline_path(lwpoints, is_closed=True)
        return hatch

    def start_cap() -> list[DXFGraphic]:
        entities: list[DXFGraphic] = []
        if style.get_flag_state(style.START_SQUARE):
            entities.extend(create_miter(miter_points[0]))
        if style.get_flag_state(style.START_ROUND):
            entities.extend(round_caps(0, top_index, bottom_index))
        if (
            style.get_flag_state(style.START_INNER_ARC)
            and len(style.elements) > 3
        ):
            start_index = ordered_indices[-2]
            end_index = ordered_indices[1]
            entities.extend(round_caps(0, start_index, end_index))
        return entities

    def end_cap() -> list[DXFGraphic]:
        entities: list[DXFGraphic] = []
        if style.get_flag_state(style.END_SQUARE):
            entities.extend(create_miter(miter_points[-1]))
        if style.get_flag_state(style.END_ROUND):
            entities.extend(round_caps(-1, bottom_index, top_index))
        if (
            style.get_flag_state(style.END_INNER_ARC)
            and len(style.elements) > 3
        ):
            start_index = ordered_indices[1]
            end_index = ordered_indices[-2]
            entities.extend(round_caps(-1, start_index, end_index))
        return entities

    def round_caps(miter_index: int, start_index: int, end_index: int):
        color1 = style.elements[start_index].color
        color2 = style.elements[end_index].color
        start = ocs.from_wcs(miter_points[miter_index][start_index])
        end = ocs.from_wcs(miter_points[miter_index][end_index])
        return _arc_caps(start, end, color1, color2)

    def _arc_caps(
        start: Vec3, end: Vec3, color1: int, color2: int
    ) -> Sequence[Arc]:
        attribs = _dxfattribs(mline)
        center = start.lerp(end)
        radius = (end - start).magnitude / 2.0
        angle = (start - center).angle_deg
        attribs["center"] = center
        attribs["radius"] = radius
        attribs["color"] = color1
        attribs["start_angle"] = angle
        attribs["end_angle"] = angle + (180 if color1 == color2 else 90)
        arc1 = cast("Arc", factory.new("ARC", dxfattribs=attribs, doc=doc))
        if color1 == color2:
            return (arc1,)
        attribs["start_angle"] = angle + 90
        attribs["end_angle"] = angle + 180
        attribs["color"] = color2
        arc2 = cast("Arc", factory.new("ARC", dxfattribs=attribs, doc=doc))
        return arc1, arc2

    def lines() -> list[Line]:
        prev = None
        _lines: list[Line] = []
        attribs = _dxfattribs(mline)

        for miter in miter_points:
            if prev is not None:
                for index, element in enumerate(style.elements):
                    attribs["start"] = prev[index]
                    attribs["end"] = miter[index]
                    attribs["color"] = element.color
                    attribs["linetype"] = element.linetype
                    _lines.append(
                        cast(
                            "Line",
                            factory.new("LINE", dxfattribs=attribs, doc=doc),
                        )
                    )
            prev = miter
        return _lines

    def display_miter():
        _lines = []
        skip = set()
        skip.add(len(miter_points) - 1)
        if not closed:
            skip.add(0)
        for index, miter in enumerate(miter_points):
            if index not in skip:
                _lines.extend(create_miter(miter))
        return _lines

    def create_miter(miter) -> list[Line]:
        _lines: list[Line] = []
        attribs = _dxfattribs(mline)
        top = miter[top_index]
        bottom = miter[bottom_index]
        zero = bottom.lerp(top)
        element = style.elements[top_index]
        attribs["start"] = top
        attribs["end"] = zero
        attribs["color"] = element.color
        attribs["linetype"] = element.linetype
        _lines.append(
            cast("Line", factory.new("LINE", dxfattribs=attribs, doc=doc))
        )
        element = style.elements[bottom_index]
        attribs["start"] = bottom
        attribs["end"] = zero
        attribs["color"] = element.color
        attribs["linetype"] = element.linetype
        _lines.append(
            cast("Line", factory.new("LINE", dxfattribs=attribs, doc=doc))
        )
        return _lines

    entities: list[DXFGraphic] = []
    if not mline.is_alive or mline.doc is None or len(mline.vertices) < 2:
        return entities

    style: MLineStyle = mline.style  # type: ignore
    if style is None:
        return entities

    doc = mline.doc
    ocs = OCS(mline.dxf.extrusion)
    element_count = len(style.elements)
    closed = mline.is_closed
    ordered_indices = style.ordered_indices()
    bottom_index = ordered_indices[0]
    top_index = ordered_indices[-1]
    bottom_border: list[Vec3] = []
    top_border: list[Vec3] = []
    miter_points: list[list[Vec3]] = []

    for vertex in mline.vertices:
        offsets = vertex.line_params
        if len(offsets) != element_count:
            logger.debug(
                f"Invalid line parametrization for vertex {len(miter_points)} "
                f"in {str(mline)}."
            )
            return entities
        location = vertex.location
        miter_direction = vertex.miter_direction
        miter = []
        for offset in offsets:
            try:
                length = offset[0]
            except IndexError:  # DXFStructureError?
                length = 0
            miter.append(location + miter_direction * length)
        miter_points.append(miter)
        top_border.append(miter[top_index])
        bottom_border.append(miter[bottom_index])

    if closed:
        miter_points.append(miter_points[0])
        top_border.append(top_border[0])
        bottom_border.append(bottom_border[0])

    if not closed:
        entities.extend(start_cap())

    entities.extend(lines())

    if style.get_flag_state(style.MITER):
        entities.extend(display_miter())

    if not closed:
        entities.extend(end_cap())

    if style.get_flag_state(style.FILL):
        entities.insert(0, filling())

    return entities
