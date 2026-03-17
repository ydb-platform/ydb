# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
import logging
import math

from ezdxf.math import Vec2, UCS, NULLVEC
from ezdxf.lldxf import const
from ezdxf.entities import DimStyleOverride, Dimension
from .dim_base import (
    BaseDimensionRenderer,
    get_required_defpoint,
    compile_mtext,
)

if TYPE_CHECKING:
    from ezdxf.eztypes import GenericLayoutType

__all__ = ["OrdinateDimension"]
logger = logging.getLogger("ezdxf")


class OrdinateDimension(BaseDimensionRenderer):
    # Required defpoints:
    # defpoint = origin (group code 10)
    # defpoint2 = feature location (group code 13)
    # defpoint3 = end of leader (group code 14)
    # user text location is ignored (group code 11) and replaced by default
    # location calculated by the ezdxf renderer:

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        # The local coordinate system is defined by origin and the
        # horizontal_direction in OCS:
        self.origin_ocs: Vec2 = get_required_defpoint(dimension, "defpoint")
        self.feature_location_ocs: Vec2 = get_required_defpoint(
            dimension, "defpoint2"
        )
        self.end_of_leader_ocs: Vec2 = get_required_defpoint(
            dimension, "defpoint3"
        )
        # Horizontal direction in clockwise orientation, see DXF reference
        # for group code 51:
        self.horizontal_dir = -dimension.dxf.get("horizontal_direction", 0.0)
        self.rotation = math.radians(self.horizontal_dir)
        self.local_x_axis = Vec2.from_angle(self.rotation)
        self.local_y_axis = self.local_x_axis.orthogonal()
        self.x_type = bool(  # x-type is set!
            dimension.dxf.get("dimtype", 0) & const.DIM_ORDINATE_TYPE
        )
        super().__init__(dimension, ucs, override)

        # Measurement directions can be opposite to local x- or y-axis
        self.leader_vec_ocs = self.end_of_leader_ocs - self.feature_location_ocs
        leader_x_vec = self.local_x_axis.project(self.leader_vec_ocs)
        leader_y_vec = self.local_y_axis.project(self.leader_vec_ocs)
        try:
            self.measurement_direction: Vec2 = leader_x_vec.normalize()
        except ZeroDivisionError:
            self.measurement_direction = Vec2(1, 0)
        try:
            self.measurement_orthogonal: Vec2 = leader_y_vec.normalize()
        except ZeroDivisionError:
            self.measurement_orthogonal = Vec2(0, 1)

        if not self.x_type:
            self.measurement_direction, self.measurement_orthogonal = (
                self.measurement_orthogonal,
                self.measurement_direction,
            )

        self.update_measurement()
        if self.tol.has_limits:
            self.tol.update_limits(self.measurement.value)

        # Text width and -height is required first, text location and -rotation
        # are not valid yet:
        self.text_box = self.init_text_box()

        # Set text location and rotation:
        self.measurement.text_location = self.get_default_text_location()
        self.measurement.text_rotation = self.get_default_text_rotation()

        # Update text box location and -rotation:
        self.text_box.center = self.measurement.text_location
        self.text_box.angle = self.measurement.text_rotation
        self.geometry.set_text_box(self.text_box)

        # Update final text location in the DIMENSION entity:
        self.dimension.dxf.text_midpoint = self.measurement.text_location

    def get_default_text_location(self) -> Vec2:
        if self.x_type:
            text_vertical_shifting_dir = -self.local_x_axis
        else:
            text_vertical_shifting_dir = self.local_y_axis

        # user text location is not supported and ignored:
        return (
            self.end_of_leader_ocs
            + self.measurement_orthogonal * (self.text_box.width * 0.5)
            + text_vertical_shifting_dir
            * self.measurement.text_vertical_distance()
        )

    def get_default_text_rotation(self) -> float:
        # user text rotation is not supported and ignored:
        return (90.0 if self.x_type else 0.0) + self.horizontal_dir

    def update_measurement(self) -> None:
        feature_location_vec: Vec2 = self.feature_location_ocs - self.origin_ocs
        # ordinate measurement is always absolute:
        self.measurement.update(
            self.local_x_axis.project(feature_location_vec).magnitude
            if self.x_type
            else self.local_y_axis.project(feature_location_vec).magnitude
        )

    def get_defpoints(self) -> list[Vec2]:
        return [
            self.origin_ocs,
            self.feature_location_ocs,
            self.end_of_leader_ocs,
        ]

    def transform_ucs_to_wcs(self) -> None:
        """Transforms dimension definition points into WCS or if required into
        OCS.
        """

        def from_ucs(attr, func):
            point = dxf.get(attr, NULLVEC)
            dxf.set(attr, func(point))

        dxf = self.dimension.dxf
        ucs = self.geometry.ucs
        from_ucs("defpoint", ucs.to_wcs)
        from_ucs("defpoint2", ucs.to_wcs)
        from_ucs("defpoint3", ucs.to_wcs)
        from_ucs("text_midpoint", ucs.to_ocs)

        # Horizontal direction in clockwise orientation, see DXF reference
        # for group code 51:
        dxf.horizontal_direction = -ucs.to_ocs_angle_deg(self.horizontal_dir)

    def render(self, block: GenericLayoutType) -> None:
        """Main method to create dimension geometry of basic DXF entities in the
        associated BLOCK layout.

        Args:
            block: target BLOCK for rendering

        """
        super().render(block)
        self.add_ordinate_leader()
        measurement = self.measurement
        if measurement.text:
            if self.geometry.supports_dxf_r2000:
                text = compile_mtext(measurement, self.tol)
            else:
                text = measurement.text
            self.add_measurement_text(
                text, measurement.text_location, measurement.text_rotation
            )
        self.geometry.add_defpoints(self.get_defpoints())

    def add_ordinate_leader(self) -> None:
        # DXF attributes from first extension line not from dimension line!
        attribs = self.extension_lines.dxfattribs(1)
        # The ordinate leader is normal to the measurement direction.
        # leader direction and text direction:
        direction = self.measurement_orthogonal
        leg_size = self.arrows.arrow_size * 2.0
        #            /---1---TEXT
        # x----0----/
        # d0 = distance from feature location (x) to 1st upward junction
        d0 = direction.project(self.leader_vec_ocs).magnitude - 2.0 * leg_size

        start0 = (
            self.feature_location_ocs + direction * self.extension_lines.offset
        )
        end0 = self.feature_location_ocs + direction * max(leg_size, d0)
        start1 = self.end_of_leader_ocs - direction * leg_size
        end1 = self.end_of_leader_ocs
        if self.measurement.vertical_placement != 0:
            end1 += direction * self.text_box.width

        self.add_line(start0, end0, dxfattribs=attribs)
        self.add_line(end0, start1, dxfattribs=attribs)
        self.add_line(start1, end1, dxfattribs=attribs)

    def add_measurement_text(
        self, dim_text: str, pos: Vec2, rotation: float
    ) -> None:
        """Add measurement text to dimension BLOCK.

        Args:
            dim_text: dimension text
            pos: text location
            rotation: text rotation in degrees

        """
        attribs = self.measurement.dxfattribs()
        self.add_text(dim_text, pos=pos, rotation=rotation, dxfattribs=attribs)
