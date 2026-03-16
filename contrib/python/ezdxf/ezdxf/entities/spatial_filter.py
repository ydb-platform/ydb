# Copyright (c) 2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Iterable
from typing_extensions import Self
import logging

from ezdxf.lldxf import const, validator
from ezdxf.lldxf.attributes import (
    DXFAttributes,
    DefSubclass,
    DXFAttr,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.math import UVec, Vec2, Matrix44, Z_AXIS, NULLVEC
from ezdxf.entities import factory
from .dxfentity import SubclassProcessor, base_class
from .dxfobj import DXFObject
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.lldxf.tags import Tags
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["SpatialFilter"]

# The HEADER variable $XCLIPFRAME determines if the clipping path polygon is displayed 
# and plotted:
#   0 - not displayed, not plotted
#   1 - displayed, not plotted
#   2 - displayed and plotted

logger = logging.getLogger("ezdxf")
AcDbFilter = "AcDbFilter"
AcDbSpatialFilter = "AcDbSpatialFilter"

acdb_filter = DefSubclass(AcDbFilter, {})
acdb_spatial_filter = DefSubclass(
    AcDbSpatialFilter,
    {
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        "origin": DXFAttr(11, xtype=XType.point3d, default=NULLVEC),
        "is_clipping_enabled": DXFAttr(
            71, default=1, validator=validator.is_integer_bool, fixer=RETURN_DEFAULT
        ),
        "has_front_clipping_plane": DXFAttr(
            72, default=0, validator=validator.is_integer_bool, fixer=RETURN_DEFAULT
        ),
        "front_clipping_plane_distance": DXFAttr(40, default=0.0),
        "has_back_clipping_plane": DXFAttr(
            73, default=0, validator=validator.is_integer_bool, fixer=RETURN_DEFAULT
        ),
        "back_clipping_plane_distance": DXFAttr(41, default=0.0),
    },
)
acdb_spatial_filter_group_codes = group_code_mapping(acdb_spatial_filter)


@factory.register_entity
class SpatialFilter(DXFObject):
    DXFTYPE = "SPATIAL_FILTER"
    DXFATTRIBS = DXFAttributes(base_class, acdb_filter, acdb_spatial_filter)
    MIN_DXF_VERSION_FOR_EXPORT = const.DXF2000

    def __init__(self) -> None:
        super().__init__()
        # clipping path vertices in OCS coordinates
        self._boundary_vertices: tuple[Vec2, ...] = tuple()

        # This matrix is the inverse of the original block reference (insert entity)
        # transformation.  The original block reference transformation is the one that
        # is applied to all entities in the block when the block reference is regenerated.
        self._inverse_insert_matrix = Matrix44()

        # This matrix transforms points into the coordinate system of the clip boundary.
        self._transform_matrix = Matrix44()

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, SpatialFilter)
        # immutable data
        entity._boundary_vertices = self._boundary_vertices
        entity._inverse_insert_matrix = self._inverse_insert_matrix
        entity._transform_matrix = self._transform_matrix

    @property
    def boundary_vertices(self) -> tuple[Vec2, ...]:
        """Returns the clipping path vertices in OCS coordinates."""
        return self._boundary_vertices

    def set_boundary_vertices(self, vertices: Iterable[UVec]) -> None:
        """Set the clipping path vertices in OCS coordinates."""
        self._boundary_vertices = tuple(Vec2(v) for v in vertices)
        if len(self._boundary_vertices) < 2:
            raise const.DXFValueError("2 or more vertices required")

    @property
    def inverse_insert_matrix(self) -> Matrix44:
        """Returns the inverse insert matrix.

        This matrix is the inverse of the original block reference (insert entity)
        transformation.  The original block reference transformation is the one that
        is applied to all entities in the block when the block reference is regenerated.
        """
        return self._inverse_insert_matrix.copy()

    def set_inverse_insert_matrix(self, m: Matrix44) -> None:
        self._inverse_insert_matrix = m.copy()

    @property
    def transform_matrix(self) -> Matrix44:
        """Returns the transform matrix.

        This matrix transforms points into the coordinate system of the clip boundary.
        """
        return self._transform_matrix.copy()

    def set_transform_matrix(self, m: Matrix44) -> None:
        self._transform_matrix = m.copy()

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.find_subclass(AcDbSpatialFilter)
            if tags:
                try:
                    self._load_boundary_data(tags)
                except IndexError:
                    logger.warning(
                        f"Not enough matrix values in SPATIAL_FILTER(#{processor.handle})"
                    )
                processor.fast_load_dxfattribs(
                    dxf, acdb_spatial_filter_group_codes, subclass=tags
                )
            else:
                logger.warning(
                    f"Required subclass 'AcDbSpatialFilter' in object "
                    f"SPATIAL_FILTER(#{processor.handle}) not present"
                )
        return dxf

    def _load_boundary_data(self, tags: Tags) -> None:
        def to_matrix(v: list[float]) -> Matrix44:
            # raises IndexError if not enough matrix values exist
            return Matrix44(
                # fmt: off
                [
                    v[0], v[4], v[8], 0.0,
                    v[1], v[5], v[9], 0.0,
                    v[2], v[6], v[10], 0.0,
                    v[3], v[7], v[11], 1.0,
                ]
                # fmt: on
            )

        self._boundary_vertices = tuple(Vec2(tag.value) for tag in tags.find_all(10))
        matrix_values = [float(tag.value) for tag in tags.find_all(40)]
        # use only last 24 values
        self._inverse_insert_matrix = to_matrix(matrix_values[-24:-12])
        self._transform_matrix = to_matrix(matrix_values[-12:])

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""

        def write_matrix(m: Matrix44) -> None:
            for index in range(3):
                for value in m.get_col(index):
                    tagwriter.write_tag2(40, value)

        super().export_entity(tagwriter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, AcDbFilter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, AcDbSpatialFilter)
        tagwriter.write_tag2(70, len(self._boundary_vertices))
        for vertex in self._boundary_vertices:
            tagwriter.write_vertex(10, vertex)
        self.dxf.export_dxf_attribs(
            tagwriter, ["extrusion", "origin", "is_clipping_enabled"]
        )
        has_front_clipping = self.dxf.has_front_clipping_plane
        tagwriter.write_tag2(72, has_front_clipping)
        if has_front_clipping:  
            # AutoCAD does no accept tag 40, if front clipping is disabled
            tagwriter.write_tag2(40, self.dxf.front_clipping_plane_distance)
        has_back_clipping = self.dxf.has_back_clipping_plane
        tagwriter.write_tag2(73, has_back_clipping)
        if has_back_clipping:  
            # AutoCAD does no accept tag 41, if back clipping is disabled
            tagwriter.write_tag2(41, self.dxf.back_clipping_plane_distance)

        write_matrix(self._inverse_insert_matrix)
        write_matrix(self._transform_matrix)
