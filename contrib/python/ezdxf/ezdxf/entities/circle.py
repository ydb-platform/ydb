# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Optional, Iterator
import math
import numpy as np

from ezdxf.lldxf import validator
from ezdxf.math import (
    Vec3,
    Matrix44,
    NULLVEC,
    Z_AXIS,
    arc_segment_count,
)
from ezdxf.math.transformtools import OCSTransform, NonUniformScalingError
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
    merge_group_code_mappings,
)
from ezdxf.lldxf.const import DXF12, SUBCLASS_MARKER, DXFValueError
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import (
    DXFGraphic,
    acdb_entity,
    add_entity,
    replace_entity,
    elevation_to_z_axis,
    acdb_entity_group_codes,
)
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entities import Ellipse, Spline

__all__ = ["Circle"]

acdb_circle = DefSubclass(
    "AcDbCircle",
    {
        "center": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # AutCAD/BricsCAD: Radius is <= 0 is valid
        "radius": DXFAttr(40, default=1),
        # Elevation is a legacy feature from R11 and prior, do not use this
        # attribute, store the entity elevation in the z-axis of the vertices.
        # ezdxf does not export the elevation attribute!
        "elevation": DXFAttr(38, default=0, optional=True),
        "thickness": DXFAttr(39, default=0, optional=True),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
    },
)

acdb_circle_group_codes = group_code_mapping(acdb_circle)
merged_circle_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes, acdb_circle_group_codes
)


@register_entity
class Circle(DXFGraphic):
    """DXF CIRCLE entity"""

    DXFTYPE = "CIRCLE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_circle)
    MERGED_GROUP_CODES = merged_circle_group_codes

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, self.MERGED_GROUP_CODES)
            if processor.r12:
                # Transform elevation attribute from R11 to z-axis values:
                elevation_to_z_axis(dxf, ("center",))
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_circle.name)
        self.dxf.export_dxf_attribs(
            tagwriter, ["center", "radius", "thickness", "extrusion"]
        )

    def vertices(self, angles: Iterable[float]) -> Iterator[Vec3]:
        """Yields the vertices of the circle of all given `angles` as
        :class:`~ezdxf.math.Vec3` instances in :ref:`WCS`.

        Args:
            angles: iterable of angles in :ref:`OCS` as degrees, angle goes
                counter-clockwise around the extrusion vector, and the OCS x-axis
                defines 0-degree.

        """
        ocs = self.ocs()
        radius: float = abs(self.dxf.radius)  # AutoCAD ignores the sign too
        center = Vec3(self.dxf.center)
        for angle in angles:
            yield ocs.to_wcs(Vec3.from_deg_angle(angle, radius) + center)

    def flattening(self, sagitta: float) -> Iterator[Vec3]:
        """Approximate the circle by vertices in :ref:`WCS` as :class:`~ezdxf.math.Vec3`
        instances. The argument `sagitta`_ is the maximum distance from the center of an
        arc segment to the center of its chord. Yields a closed polygon where the start
        vertex is equal to the end  vertex!

        .. _sagitta: https://en.wikipedia.org/wiki/Sagitta_(geometry)
        """
        radius = abs(self.dxf.radius)
        if radius > 0.0:
            count = arc_segment_count(radius, math.tau, sagitta)
            yield from self.vertices(np.linspace(0.0, 360.0, count + 1))

    def transform(self, m: Matrix44) -> Circle:
        """Transform the CIRCLE entity by transformation matrix `m` inplace.
        Raises ``NonUniformScalingError()`` for non-uniform scaling.
        """
        circle = self._transform(OCSTransform(self.dxf.extrusion, m))
        self.post_transform(m)
        return circle

    def _transform(self, ocs: OCSTransform) -> Circle:
        dxf = self.dxf
        if ocs.scale_uniform:
            dxf.extrusion = ocs.new_extrusion
            dxf.center = ocs.transform_vertex(dxf.center)
            # old_ocs has a uniform scaled xy-plane, direction of radius-vector
            # in the xy-plane is not important, choose x-axis for no reason:
            dxf.radius = ocs.transform_length((dxf.radius, 0, 0))
            if dxf.hasattr("thickness"):
                # thickness vector points in the z-direction of the old_ocs,
                # thickness can be negative
                dxf.thickness = ocs.transform_thickness(dxf.thickness)
        else:
            # Caller has to catch this Exception and convert this
            # CIRCLE/ARC into an ELLIPSE.
            raise NonUniformScalingError(
                "CIRCLE/ARC does not support non uniform scaling"
            )

        return self

    def translate(self, dx: float, dy: float, dz: float) -> Circle:
        """Optimized CIRCLE/ARC translation about `dx` in x-axis, `dy` in
        y-axis and `dz` in z-axis, returns `self` (floating interface).
        """
        ocs = self.ocs()
        self.dxf.center = ocs.from_wcs(Vec3(dx, dy, dz) + ocs.to_wcs(self.dxf.center))
        # Avoid Matrix44 instantiation if not required:
        if self.is_post_transform_required:
            self.post_transform(Matrix44.translate(dx, dy, dz))
        return self

    def to_ellipse(self, replace=True) -> Ellipse:
        """Convert the CIRCLE/ARC entity to an :class:`~ezdxf.entities.Ellipse` entity.

        Adds the new ELLIPSE entity to the entity database and to the same layout as
        the source entity.

        Args:
            replace: replace (delete) source entity by ELLIPSE entity if ``True``

        """
        from ezdxf.entities import Ellipse

        layout = self.get_layout()
        if layout is None:
            raise DXFValueError("valid layout required")
        ellipse = Ellipse.from_arc(self)
        if replace:
            replace_entity(self, ellipse, layout)
        else:
            add_entity(ellipse, layout)
        return ellipse

    def to_spline(self, replace=True) -> Spline:
        """Convert the CIRCLE/ARC entity to a :class:`~ezdxf.entities.Spline` entity.

        Adds the new SPLINE entity to the entity database and to the same layout as the
        source entity.

        Args:
            replace: replace (delete) source entity by SPLINE entity if ``True``

        """
        from ezdxf.entities import Spline

        layout = self.get_layout()
        if layout is None:
            raise DXFValueError("valid layout required")
        spline = Spline.from_arc(self)
        if replace:
            replace_entity(self, spline, layout)
        else:
            add_entity(spline, layout)
        return spline
