# Copyright (c) 2020-2025, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Union
import logging
import math

from ezdxf.entities import factory
from ezdxf.lldxf.const import VERTEXNAMES
from ezdxf.math import Vec3, bulge_to_arc, OCS

if TYPE_CHECKING:
    from ezdxf.entities import LWPolyline, Polyline, Line, Arc, Face3d, Polymesh

logger = logging.getLogger("ezdxf")


def virtual_lwpolyline_entities(
    lwpolyline: LWPolyline,
) -> Iterable[Union[Line, Arc]]:
    """Yields 'virtual' entities of LWPOLYLINE as LINE or ARC objects.

    These entities are located at the original positions, but are not stored in
    the entity database, have no handle and are not assigned to any layout.

    (internal API)

    """
    assert lwpolyline.dxftype() == "LWPOLYLINE"

    points = lwpolyline.get_points("xyb")
    if len(points) < 2:
        return

    if lwpolyline.closed:
        points.append(points[0])

    yield from _virtual_polyline_entities(
        points=points,
        elevation=lwpolyline.dxf.elevation,
        extrusion=lwpolyline.dxf.get("extrusion", None),
        dxfattribs=lwpolyline.graphic_properties(),
        doc=lwpolyline.doc,
    )


def virtual_polyline_entities(
    polyline: Polyline,
) -> Iterable[Union[Line, Arc, Face3d]]:
    """Yields 'virtual' entities of POLYLINE as LINE, ARC or 3DFACE objects.

    These entities are located at the original positions, but are not stored in
    the entity database, have no handle and are not assigned to any layout.

    (internal API)

    """
    assert polyline.dxftype() == "POLYLINE"
    if polyline.is_2d_polyline:
        return virtual_polyline2d_entities(polyline)
    elif polyline.is_3d_polyline:
        return virtual_polyline3d_entities(polyline)
    elif polyline.is_polygon_mesh:
        return virtual_polymesh_entities(polyline)
    elif polyline.is_poly_face_mesh:
        return virtual_polyface_entities(polyline)
    return []


def virtual_polyline2d_entities(
    polyline: Polyline,
) -> Iterable[Union[Line, Arc]]:
    """Yields 'virtual' entities of 2D POLYLINE as LINE or ARC objects.

    These entities are located at the original positions, but are not stored in
    the entity database, have no handle and are not assigned to any layout.

    (internal API)

    """
    assert polyline.dxftype() == "POLYLINE"
    assert polyline.is_2d_polyline
    if len(polyline.vertices) < 2:
        return

    points = [
        (v.dxf.location.x, v.dxf.location.y, v.dxf.bulge) for v in polyline.vertices
    ]
    if polyline.is_closed:
        points.append(points[0])

    yield from _virtual_polyline_entities(
        points=points,
        elevation=Vec3(polyline.dxf.get("elevation", (0, 0, 0))).z,
        extrusion=polyline.dxf.get("extrusion", None),
        dxfattribs=polyline.graphic_properties(),
        doc=polyline.doc,
    )


def _virtual_polyline_entities(
    points, elevation: float, extrusion: Vec3, dxfattribs: dict, doc
) -> Iterable[Union[Line, Arc]]:
    ocs = OCS(extrusion) if extrusion else OCS()
    prev_point: Vec3 | None = None
    prev_bulge: float = 0.0

    for x, y, bulge in points:
        point = Vec3(x, y, elevation)
        if prev_point is None:
            prev_point = point
            prev_bulge = bulge
            continue

        attribs = dict(dxfattribs)
        if prev_bulge != 0.0:
            center, start_angle, end_angle, radius = bulge_to_arc(
                prev_point, point, prev_bulge
            )
            if radius > 0:
                attribs["center"] = Vec3(center.x, center.y, elevation)
                attribs["radius"] = radius
                attribs["start_angle"] = math.degrees(start_angle)
                attribs["end_angle"] = math.degrees(end_angle)
                if extrusion:
                    attribs["extrusion"] = extrusion
                yield factory.new(dxftype="ARC", dxfattribs=attribs, doc=doc)  # type: ignore
        else:
            attribs["start"] = ocs.to_wcs(prev_point)
            attribs["end"] = ocs.to_wcs(point)
            yield factory.new(dxftype="LINE", dxfattribs=attribs, doc=doc)  # type: ignore
        prev_point = point
        prev_bulge = bulge


def virtual_polyline3d_entities(polyline: Polyline) -> Iterable[Line]:
    """Yields 'virtual' entities of 3D POLYLINE as LINE objects.

    This entities are located at the original positions, but are not stored in
    the entity database, have no handle and are not assigned to any layout.

    (internal API)

    """
    assert polyline.dxftype() == "POLYLINE"
    assert polyline.is_3d_polyline
    if len(polyline.vertices) < 2:
        return
    doc = polyline.doc
    vertices = polyline.vertices
    dxfattribs = polyline.graphic_properties()
    start = -1 if polyline.is_closed else 0
    for index in range(start, len(vertices) - 1):
        dxfattribs["start"] = vertices[index].dxf.location
        dxfattribs["end"] = vertices[index + 1].dxf.location
        yield factory.new(dxftype="LINE", dxfattribs=dxfattribs, doc=doc)  # type: ignore


def virtual_polymesh_entities(polyline: Polyline) -> Iterable[Face3d]:
    """Yields 'virtual' entities of POLYMESH as 3DFACE objects.

    This entities are located at the original positions, but are not stored in
    the entity database, have no handle and are not assigned to any layout.

    (internal API)

    """
    polymesh: "Polymesh" = polyline  # type: ignore
    assert polymesh.dxftype() == "POLYLINE"
    assert polymesh.is_polygon_mesh

    doc = polymesh.doc
    mesh = polymesh.get_mesh_vertex_cache()
    dxfattribs = polymesh.graphic_properties()
    m_count = polymesh.dxf.m_count
    n_count = polymesh.dxf.n_count
    m_range = m_count - int(not polymesh.is_m_closed)
    n_range = n_count - int(not polymesh.is_n_closed)

    for m in range(m_range):
        for n in range(n_range):
            next_m = (m + 1) % m_count
            next_n = (n + 1) % n_count

            dxfattribs["vtx0"] = mesh[m, n]
            dxfattribs["vtx1"] = mesh[next_m, n]
            dxfattribs["vtx2"] = mesh[next_m, next_n]
            dxfattribs["vtx3"] = mesh[m, next_n]
            yield factory.new(dxftype="3DFACE", dxfattribs=dxfattribs, doc=doc)  # type: ignore


def virtual_polyface_entities(polyline: Polyline) -> Iterable[Face3d]:
    """Yields 'virtual' entities of POLYFACE as 3DFACE objects.

    This entities are located at the original positions, but are not stored in
    the entity database, have no handle and are not assigned to any layout.

    (internal API)

    """
    assert polyline.dxftype() == "POLYLINE"
    assert polyline.is_poly_face_mesh

    doc = polyline.doc
    vertices = polyline.vertices
    base_attribs = polyline.graphic_properties()

    face_records = (v for v in vertices if v.is_face_record)
    for face in face_records:
        # check if vtx0, vtx1 and vtx2 exist
        for name in VERTEXNAMES[:-1]:
            if not face.dxf.hasattr(name):
                logger.info(
                    f"skipped face {str(face)} with less than 3 vertices"
                    f"in PolyFaceMesh(#{str(polyline.dxf.handle)})"
                )
                continue
                # Alternate solutions: return a face with less than 3 vertices
                # as LINE (breaks the method signature) or as degenerated 3DFACE
                # (vtx0, vtx1, vtx1, vtx1)

        face3d_attribs = dict(base_attribs)
        face3d_attribs.update(face.graphic_properties())
        invisible = 0
        pos = 1
        indices = (
            (face.dxf.get(name), name) for name in VERTEXNAMES if face.dxf.hasattr(name)
        )
        for index, name in indices:
            # vertex indices are 1-based, negative indices indicate invisible edges
            if index < 0:
                index = abs(index)
                invisible += pos
            # python list `vertices` is 0-based
            face3d_attribs[name] = vertices[index - 1].dxf.location
            # vertex index bit encoded: 1=0b0001, 2=0b0010, 3=0b0100, 4=0b1000
            pos <<= 1

        if "vtx3" not in face3d_attribs:
            # A triangle face ends with two identical vertices vtx2 and vtx3.
            # This is a requirement defined by AutoCAD.
            face3d_attribs["vtx3"] = face3d_attribs["vtx2"]

        face3d_attribs["invisible"] = invisible
        yield factory.new(dxftype="3DFACE", dxfattribs=face3d_attribs, doc=doc)  # type: ignore
