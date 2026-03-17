# Copyright (c) 2020-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator, cast

from ezdxf import ARROWS
from ezdxf.entities import factory
from ezdxf.lldxf.const import BYBLOCK
from ezdxf.math import Vec3, fit_points_to_cad_cv

if TYPE_CHECKING:
    from ezdxf.entities import DXFGraphic, Leader, Insert, Spline, Dimension, Line


def virtual_entities(leader: Leader) -> Iterator[DXFGraphic]:
    # Source: https://atlight.github.io/formats/dxf-leader.html
    # GDAL: DXF LEADER implementation:
    # https://github.com/OSGeo/gdal/blob/master/gdal/ogr/ogrsf_frmts/dxf/ogrdxf_leader.cpp
    # LEADER DXF Reference:
    # http://help.autodesk.com/view/OARX/2018/ENU/?guid=GUID-396B2369-F89F-47D7-8223-8B7FB794F9F3
    assert leader.dxftype() == "LEADER"

    vertices = Vec3.list(leader.vertices)  # WCS
    if len(vertices) < 2:
        # This LEADER entities should be removed by the auditor if loaded or
        # ignored at exporting, if created by an ezdxf-user (log).
        raise ValueError("More than 1 vertex required.")
    dxf = leader.dxf
    doc = leader.doc

    # Some default values depend on the measurement system
    # 0/1 = imperial/metric
    if doc:
        measurement = doc.header.get("$MEASUREMENT", 0)
    else:
        measurement = 0

    # Set default styling attributes values:
    dimtad = 1
    dimgap = 0.625 if measurement else 0.0625
    dimscale = 1.0
    dimclrd = dxf.color
    dimltype = dxf.linetype
    dimlwd = dxf.lineweight
    override = None

    if doc:
        # get styling attributes from associated DIMSTYLE and/or XDATA override
        override = leader.override()
        dimtad = override.get("dimtad", dimtad)
        dimgap = override.get("dimgap", dimgap)
        dimscale = override.get("dimscale", dimscale)
        if dimscale == 0.0:  # special but unknown meaning
            dimscale = 1.0
        dimclrd = override.get("dimclrd", dimclrd)
        dimltype = override.get("dimltype", dimltype)
        dimlwd = override.get("dimlwd", dimlwd)

    text_width = dxf.text_width
    hook_line_vector = Vec3(dxf.horizontal_direction)
    has_text_annotation = dxf.annotation_type == 0

    if has_text_annotation and dxf.has_hookline:
        if dxf.hookline_direction == 1:
            hook_line_vector = -hook_line_vector
        if dimtad != 0 and text_width > 0:
            hook_line = hook_line_vector * (dimgap * dimscale + text_width)
            vertices.append(vertices[-1] + hook_line)

    dxfattribs = leader.graphic_properties()
    dxfattribs["color"] = dimclrd
    dxfattribs["linetype"] = dimltype
    dxfattribs["lineweight"] = dimlwd

    if dxfattribs.get("color") == BYBLOCK:
        dxfattribs["color"] = dxf.block_color

    if dxf.path_type == 1:  # Spline
        start_tangent = vertices[1] - vertices[0]
        end_tangent = vertices[-1] - vertices[-2]
        bspline = fit_points_to_cad_cv(vertices, tangents=[start_tangent, end_tangent])
        spline = cast("Spline", factory.new("SPLINE", doc=doc))
        spline.apply_construction_tool(bspline)
        yield spline
    else:
        attribs = dict(dxfattribs)
        prev = vertices[0]
        for vertex in vertices[1:]:
            attribs["start"] = prev
            attribs["end"] = vertex
            yield cast(
                "Line",
                factory.new(dxftype="LINE", dxfattribs=attribs, doc=doc),
            )
            prev = vertex

    if dxf.has_arrowhead and override:
        arrow_name = override.get("dimldrblk", "")
        if arrow_name is None:
            return
        size = override.get("dimasz", 2.5 if measurement else 0.1875) * dimscale
        rotation = (vertices[0] - vertices[1]).angle_deg
        if doc and arrow_name in doc.blocks:
            dxfattribs.update(
                {
                    "name": arrow_name,
                    "insert": vertices[0],
                    "rotation": rotation,
                    "xscale": size,
                    "yscale": size,
                    "zscale": size,
                }
            )
            # create a virtual block reference
            insert = cast(
                "Insert", factory.new("INSERT", dxfattribs=dxfattribs, doc=doc)
            )
            yield from insert.virtual_entities()
        else:  # render standard arrows
            yield from ARROWS.virtual_entities(
                name=arrow_name,
                insert=vertices[0],
                size=size,
                rotation=rotation,
                dxfattribs=dxfattribs,
            )
