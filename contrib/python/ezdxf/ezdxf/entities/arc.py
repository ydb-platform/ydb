# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator
import math
import numpy as np

from ezdxf.math import (
    Vec3,
    Matrix44,
    ConstructionArc,
    arc_angle_span_deg,
)
from ezdxf.math.transformtools import OCSTransform

from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    group_code_mapping,
    merge_group_code_mappings,
)
from ezdxf.lldxf.const import DXF12, SUBCLASS_MARKER
from .dxfentity import base_class
from .dxfgfx import acdb_entity
from .circle import acdb_circle, Circle, merged_circle_group_codes
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["Arc"]

acdb_arc = DefSubclass(
    "AcDbArc",
    {
        "start_angle": DXFAttr(50, default=0),
        "end_angle": DXFAttr(51, default=360),
    },
)

acdb_arc_group_codes = group_code_mapping(acdb_arc)
merged_arc_group_codes = merge_group_code_mappings(
    merged_circle_group_codes, acdb_arc_group_codes
)


@register_entity
class Arc(Circle):
    """DXF ARC entity"""

    DXFTYPE = "ARC"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_circle, acdb_arc)
    MERGED_GROUP_CODES = merged_arc_group_codes

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        # AcDbEntity export is done by parent class
        # AcDbCircle export is done by parent class
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_arc.name)
        self.dxf.export_dxf_attribs(tagwriter, ["start_angle", "end_angle"])

    @property
    def start_point(self) -> Vec3:
        """Returns the start point of the arc in :ref:`WCS`, takes the :ref:`OCS` into
        account.
        """
        v = list(self.vertices([self.dxf.start_angle]))
        return v[0]

    @property
    def end_point(self) -> Vec3:
        """Returns the end point of the arc in :ref:`WCS`, takes the :ref:`OCS` into
        account.
        """
        v = list(self.vertices([self.dxf.end_angle]))
        return v[0]

    def angles(self, num: int) -> Iterator[float]:
        """Yields `num` angles from start- to end angle in degrees in counter-clockwise
        orientation. All angles are normalized in the range from [0, 360).
        """
        if num < 2:
            raise ValueError("num >= 2")
        start = self.dxf.start_angle % 360
        stop = self.dxf.end_angle % 360
        if stop <= start:
            stop += 360
        for angle in np.linspace(start, stop, num=num, endpoint=True):
            yield angle % 360

    def flattening(self, sagitta: float) -> Iterator[Vec3]:
        """Approximate the arc by vertices in :ref:`WCS`, the argument `sagitta`_
        defines the maximum distance from the center of an arc segment to the center of
        its chord.

        .. _sagitta: https://en.wikipedia.org/wiki/Sagitta_(geometry)
        """
        arc = self.construction_tool()
        ocs = self.ocs()
        elevation = Vec3(self.dxf.center).z
        if ocs.transform:
            to_wcs = ocs.points_to_wcs
        else:
            to_wcs = Vec3.generate

        yield from to_wcs(Vec3(p.x, p.y, elevation) for p in arc.flattening(sagitta))

    def transform(self, m: Matrix44) -> Arc:
        """Transform ARC entity by transformation matrix `m` inplace.
        Raises ``NonUniformScalingError()`` for non-uniform scaling.
        """
        ocs = OCSTransform(self.dxf.extrusion, m)
        super()._transform(ocs)
        s: float = self.dxf.start_angle
        e: float = self.dxf.end_angle
        if not math.isclose(arc_angle_span_deg(s, e), 360.0):
            (
                self.dxf.start_angle,
                self.dxf.end_angle,
            ) = ocs.transform_ccw_arc_angles_deg(s, e)
        self.post_transform(m)
        return self

    def construction_tool(self) -> ConstructionArc:
        """Returns the 2D construction tool :class:`ezdxf.math.ConstructionArc` but the
        extrusion vector is ignored.
        """
        dxf = self.dxf
        return ConstructionArc(
            dxf.center,
            dxf.radius,
            dxf.start_angle,
            dxf.end_angle,
        )

    def apply_construction_tool(self, arc: ConstructionArc) -> Arc:
        """Set ARC data from the construction tool :class:`ezdxf.math.ConstructionArc`
        but the extrusion vector is ignored.
        """
        dxf = self.dxf
        dxf.center = Vec3(arc.center)
        dxf.radius = arc.radius
        dxf.start_angle = arc.start_angle
        dxf.end_angle = arc.end_angle
        return self  # floating interface
