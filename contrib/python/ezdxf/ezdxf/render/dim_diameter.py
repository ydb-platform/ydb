# Copyright (c) 2018-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from ezdxf.math import Vec2, UCS
from ezdxf.entities.dimstyleoverride import DimStyleOverride

from .dim_radius import (
    RadiusDimension,
    add_center_mark,
    Measurement,
    RadiusMeasurement,
)

if TYPE_CHECKING:
    from ezdxf.entities import Dimension

PREFIX = "Ø"


class DiameterDimension(RadiusDimension):
    """
    Diameter dimension line renderer.

    Supported render types:
    - default location inside, text aligned with diameter dimension line
    - default location inside horizontal text
    - default location outside, text aligned with diameter dimension line
    - default location outside horizontal text
    - user defined location, text aligned with diameter dimension line
    - user defined location horizontal text

    Args:
        dimension: DXF entity DIMENSION
        ucs: user defined coordinate system
        override: dimension style override management object

    """

    def init_measurement(self, color: int, scale: float) -> Measurement:
        return RadiusMeasurement(self.dim_style, color, scale, PREFIX)

    def _center(self):
        return Vec2(self.dimension.dxf.defpoint).lerp(
            self.dimension.dxf.defpoint4
        )

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        # Diameter dimension has the same styles for inside text as radius dimension, except for the
        # measurement text
        super().__init__(dimension, ucs, override)
        self.point_on_circle2 = Vec2(self.dimension.dxf.defpoint)

    def add_text(
        self, text: str, pos: Vec2, rotation: float, dxfattribs
    ) -> None:
        # escape diameter sign
        super().add_text(text.replace(PREFIX, "%%c"), pos, rotation, dxfattribs)

    def get_default_text_location(self) -> Vec2:
        """Returns default text midpoint based on `text_valign` and
        `text_outside`.
        """
        measurement = self.measurement
        if measurement.text_is_outside and measurement.text_outside_horizontal:
            return super().get_default_text_location()

        text_direction = Vec2.from_deg_angle(measurement.text_rotation)
        vertical_direction = text_direction.orthogonal(ccw=True)
        vertical_distance = measurement.text_vertical_distance()
        if measurement.text_is_inside:
            text_midpoint = self.center
        else:
            hdist = (
                self._total_text_width / 2.0
                + self.arrows.arrow_size
                + measurement.text_gap
            )
            text_midpoint = self.point_on_circle + (self.dim_line_vec * hdist)
        return text_midpoint + (vertical_direction * vertical_distance)

    def _add_arrow_1(self, rotate=False):
        if not self.arrows.suppress1:
            return self.add_arrow(self.point_on_circle, rotate=rotate)
        else:
            return self.point_on_circle

    def _add_arrow_2(self, rotate=True):
        if not self.arrows.suppress2:
            return self.add_arrow(self.point_on_circle2, rotate=rotate)
        else:
            return self.point_on_circle2

    def render_default_location(self) -> None:
        """Create dimension geometry at the default dimension line locations."""
        measurement = self.measurement
        if measurement.text_is_outside:
            connection_point1 = self._add_arrow_1(rotate=True)
            if self.outside_text_force_dimline:
                self.add_diameter_dim_line(
                    connection_point1, self._add_arrow_2()
                )
            else:
                add_center_mark(self)
            if measurement.text_outside_horizontal:
                self.add_horiz_ext_line_default(connection_point1)
            else:
                self.add_radial_ext_line_default(connection_point1)
        else:
            connection_point1 = self._add_arrow_1(rotate=False)
            if measurement.text_movement_rule == 1:
                # move text, add leader -> dimline from text to point on circle
                self.add_radial_dim_line_from_text(
                    self.center, connection_point1
                )
                add_center_mark(self)
            else:
                # dimline from center to point on circle
                self.add_diameter_dim_line(
                    connection_point1, self._add_arrow_2()
                )

    def render_user_location(self) -> None:
        """Create dimension geometry at user defined dimension locations."""
        measurement = self.measurement
        preserve_outside = measurement.text_is_outside
        leader = measurement.text_movement_rule != 2
        if not leader:
            # render dimension line like text inside
            measurement.text_is_outside = False
        # add arrow symbol (block references)
        connection_point1 = self._add_arrow_1(
            rotate=measurement.text_is_outside
        )

        if measurement.text_is_outside:
            if self.outside_text_force_dimline:
                self.add_radial_dim_line(self.point_on_circle)
            else:
                add_center_mark(self)
            if measurement.text_outside_horizontal:
                self.add_horiz_ext_line_user(connection_point1)
            else:
                self.add_radial_ext_line_user(connection_point1)
        else:
            if measurement.text_inside_horizontal:
                self.add_horiz_ext_line_user(connection_point1)
            else:
                if measurement.text_movement_rule == 2:  # move text, no leader!
                    # dimline across the circle
                    connection_point2 = self._add_arrow_2(rotate=True)
                    self.add_line(
                        connection_point1,
                        connection_point2,
                        dxfattribs=self.dimension_line.dxfattribs(),
                        remove_hidden_lines=True,
                    )
                else:
                    # move text, add leader -> dimline from text to point on circle
                    self.add_radial_dim_line_from_text(
                        measurement.user_location, connection_point1
                    )
                    add_center_mark(self)

        measurement.text_is_outside = preserve_outside

    def add_diameter_dim_line(self, start: Vec2, end: Vec2) -> None:
        """Add diameter dimension line."""
        attribs = self.dimension_line.dxfattribs()
        self.add_line(start, end, dxfattribs=attribs, remove_hidden_lines=True)
