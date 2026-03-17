# Copyright (c) 2018-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from ezdxf.math import Vec2, UCS, UVec
from ezdxf.tools import normalize_text_angle
from ezdxf.render.arrows import ARROWS, connection_point
from ezdxf.entities.dimstyleoverride import DimStyleOverride
from ezdxf.lldxf.const import DXFInternalEzdxfError
from ezdxf.render.dim_base import (
    BaseDimensionRenderer,
    Measurement,
    LengthMeasurement,
    compile_mtext,
)

if TYPE_CHECKING:
    from ezdxf.entities import Dimension
    from ezdxf.eztypes import GenericLayoutType


class RadiusMeasurement(LengthMeasurement):
    def __init__(
        self, dim_style: DimStyleOverride, color: int, scale: float, prefix: str
    ):
        super().__init__(dim_style, color, scale)
        self.text_prefix = prefix

    def format_text(self, value: float) -> str:
        text = super().format_text(value)
        if text and text[0] != self.text_prefix:
            text = self.text_prefix + text
        return text


class RadiusDimension(BaseDimensionRenderer):
    """
    Radial dimension line renderer.

    Supported render types:
    - default location inside, text aligned with radial dimension line
    - default location inside horizontal text
    - default location outside, text aligned with radial dimension line
    - default location outside horizontal text
    - user defined location, text aligned with radial dimension line
    - user defined location horizontal text

    Args:
        dimension: DXF entity DIMENSION
        ucs: user defined coordinate system
        override: dimension style override management object

    """

    # Super class of DiameterDimension
    def _center(self):
        return Vec2(self.dimension.dxf.defpoint)

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        super().__init__(dimension, ucs, override)
        dimtype = self.dimension.dimtype
        measurement = self.measurement
        if dimtype == 3:
            self.is_diameter_dim = True
        elif dimtype == 4:
            self.is_radius_dim = True
        else:
            raise DXFInternalEzdxfError(f"Invalid dimension type {dimtype}")

        self.center: Vec2 = self._center()  # override in diameter dimension
        self.point_on_circle: Vec2 = Vec2(self.dimension.dxf.defpoint4)
        # modify parameters for special scenarios
        if measurement.user_location is None:
            if (
                measurement.text_is_inside
                and measurement.text_inside_horizontal
                and measurement.text_movement_rule == 1
            ):  # move text, add leader
                # use algorithm for user define dimension line location
                measurement.user_location = self.center.lerp(
                    self.point_on_circle
                )
                measurement.text_valign = 0  # text vertical centered

        direction = self.point_on_circle - self.center
        self.dim_line_vec = direction.normalize()
        self.dim_line_angle = self.dim_line_vec.angle_deg
        self.radius = direction.magnitude
        # get_measurement() works for radius and diameter dimension
        measurement.update(self.dimension.get_measurement())
        self.outside_default_distance = self.radius + 2 * self.arrows.arrow_size
        self.outside_default_defpoint = self.center + (
            self.dim_line_vec * self.outside_default_distance
        )
        self.outside_text_force_dimline = bool(self.dim_style.get("dimtofl", 1))
        # final dimension text (without limits or tolerance)

        # default location is outside, if not forced to be inside
        measurement.text_is_outside = not measurement.force_text_inside
        # text_outside: user defined location, overrides default location
        if measurement.user_location is not None:
            measurement.text_is_outside = self.is_location_outside(
                measurement.user_location
            )

        self._total_text_width: float = 0.0
        if measurement.text:
            # text width and required space
            self._total_text_width = self.total_text_width()
            if self.tol.has_limits:
                # limits show the upper and lower limit of the measurement as
                # stacked values and with the size of tolerances
                self.tol.update_limits(measurement.value)

        # default rotation is angle of dimension line, from center to point on circle.
        rotation = self.dim_line_angle
        if measurement.text_is_outside and measurement.text_outside_horizontal:
            rotation = 0.0
        elif measurement.text_is_inside and measurement.text_inside_horizontal:
            rotation = 0.0

        # final absolute text rotation (x-axis=0)
        measurement.text_rotation = normalize_text_angle(
            rotation, fix_upside_down=True
        )

        # final text location
        measurement.text_location = self.get_text_location()
        self.geometry.set_text_box(self.init_text_box())
        # write final text location into DIMENSION entity
        if measurement.user_location:
            self.dimension.dxf.text_midpoint = measurement.user_location
        # default locations
        elif (
            measurement.text_is_outside and measurement.text_outside_horizontal
        ):
            self.dimension.dxf.text_midpoint = self.outside_default_defpoint
        else:
            self.dimension.dxf.text_midpoint = measurement.text_location

    def init_measurement(self, color: int, scale: float) -> Measurement:
        return RadiusMeasurement(self.dim_style, color, scale, "R")

    def get_text_location(self) -> Vec2:
        """Returns text midpoint from user defined location or default text
        location.
        """
        if self.measurement.user_location is not None:
            return self.get_user_defined_text_location()
        else:
            return self.get_default_text_location()

    def get_default_text_location(self) -> Vec2:
        """Returns default text midpoint based on `text_valign` and
        `text_outside`
        """
        measurement = self.measurement
        if measurement.text_is_outside and measurement.text_outside_horizontal:
            hdist = self._total_text_width / 2.0
            if (
                measurement.vertical_placement == 0
            ):  # shift text horizontal if vertical centered
                hdist += self.arrows.arrow_size
            angle = self.dim_line_angle % 360.0  # normalize 0 .. 360
            if 90.0 < angle <= 270.0:
                hdist = -hdist
            return self.outside_default_defpoint + Vec2(
                (hdist, measurement.text_vertical_distance())
            )

        text_direction = Vec2.from_deg_angle(measurement.text_rotation)
        vertical_direction = text_direction.orthogonal(ccw=True)
        vertical_distance = measurement.text_vertical_distance()
        if measurement.text_is_inside:
            hdist = (self.radius - self.arrows.arrow_size) / 2.0
            text_midpoint = self.center + (self.dim_line_vec * hdist)
        else:
            hdist = (
                self._total_text_width / 2.0
                + self.arrows.arrow_size
                + measurement.text_gap
            )
            text_midpoint = self.point_on_circle + (self.dim_line_vec * hdist)
        return text_midpoint + (vertical_direction * vertical_distance)

    def get_user_defined_text_location(self) -> Vec2:
        """Returns text midpoint for user defined dimension location."""
        measurement = self.measurement
        assert isinstance(measurement.user_location, Vec2)
        text_outside_horiz = (
            measurement.text_is_outside and measurement.text_outside_horizontal
        )
        text_inside_horiz = (
            measurement.text_is_inside and measurement.text_inside_horizontal
        )
        if text_outside_horiz or text_inside_horiz:
            hdist = self._total_text_width / 2.0
            if (
                measurement.vertical_placement == 0
            ):  # shift text horizontal if vertical centered
                hdist += self.arrows.arrow_size
            if measurement.user_location.x <= self.point_on_circle.x:
                hdist = -hdist
            vdist = measurement.text_vertical_distance()
            return measurement.user_location + Vec2((hdist, vdist))
        else:
            text_normal_vec = Vec2.from_deg_angle(
                measurement.text_rotation
            ).orthogonal()
            return (
                measurement.user_location
                + text_normal_vec * measurement.text_vertical_distance()
            )

    def is_location_outside(self, location: Vec2) -> bool:
        radius = (location - self.center).magnitude
        return radius > self.radius

    def render(self, block: GenericLayoutType) -> None:
        """Create dimension geometry of basic DXF entities in the associated
        BLOCK layout.
        """
        # call required to setup some requirements
        super().render(block)
        measurement = self.measurement
        if not self.dimension_line.suppress1:
            if measurement.user_location is not None:
                self.render_user_location()
            else:
                self.render_default_location()

        # add measurement text as last entity to see text fill properly
        if measurement.text:
            if self.geometry.supports_dxf_r2000:
                text = compile_mtext(self.measurement, self.tol)
            else:
                text = measurement.text
            self.add_measurement_text(
                text, measurement.text_location, measurement.text_rotation
            )

        # add POINT entities at definition points
        self.geometry.add_defpoints([self.center, self.point_on_circle])

    def render_default_location(self) -> None:
        """Create dimension geometry at the default dimension line locations."""
        measurement = self.measurement
        if not self.arrows.suppress1:
            arrow_connection_point = self.add_arrow(
                self.point_on_circle, rotate=measurement.text_is_outside
            )
        else:
            arrow_connection_point = self.point_on_circle

        if measurement.text_is_outside:
            if self.outside_text_force_dimline:
                self.add_radial_dim_line(self.point_on_circle)
            else:
                add_center_mark(self)
            if measurement.text_outside_horizontal:
                self.add_horiz_ext_line_default(arrow_connection_point)
            else:
                self.add_radial_ext_line_default(arrow_connection_point)
        else:
            if measurement.text_movement_rule == 1:
                # move text, add leader -> dimline from text to point on circle
                self.add_radial_dim_line_from_text(
                    self.center.lerp(self.point_on_circle),
                    arrow_connection_point,
                )
                add_center_mark(self)
            else:
                # dimline from center to point on circle
                self.add_radial_dim_line(arrow_connection_point)

    def render_user_location(self) -> None:
        """Create dimension geometry at user defined dimension locations."""
        measurement = self.measurement
        preserve_outside = measurement.text_is_outside
        leader = measurement.text_movement_rule != 2
        if not leader:
            measurement.text_is_outside = (
                False  # render dimension line like text inside
            )
        # add arrow symbol (block references)
        if not self.arrows.suppress1:
            arrow_connection_point = self.add_arrow(
                self.point_on_circle, rotate=measurement.text_is_outside
            )
        else:
            arrow_connection_point = self.point_on_circle
        if measurement.text_is_outside:
            if self.outside_text_force_dimline:
                self.add_radial_dim_line(self.point_on_circle)
            else:
                add_center_mark(self)
            if measurement.text_outside_horizontal:
                self.add_horiz_ext_line_user(arrow_connection_point)
            else:
                self.add_radial_ext_line_user(arrow_connection_point)
        else:
            if measurement.text_inside_horizontal:
                self.add_horiz_ext_line_user(arrow_connection_point)
            else:
                if measurement.text_movement_rule == 2:  # move text, no leader!
                    # dimline from center to point on circle
                    self.add_radial_dim_line(arrow_connection_point)
                else:
                    # move text, add leader -> dimline from text to point on circle
                    self.add_radial_dim_line_from_text(
                        measurement.user_location, arrow_connection_point
                    )
                    add_center_mark(self)

        measurement.text_is_outside = preserve_outside

    def add_arrow(self, location, rotate: bool) -> Vec2:
        """Add arrow or tick to dimension line, returns dimension line connection point."""
        arrows = self.arrows
        attribs = arrows.dxfattribs()

        arrow_name = arrows.arrow1_name
        if arrows.tick_size > 0.0:  # oblique stroke, but double the size
            self.add_blockref(
                ARROWS.oblique,
                insert=location,
                rotation=self.dim_line_angle,
                scale=arrows.tick_size * 2.0,
                dxfattribs=attribs,
            )
        else:
            scale = arrows.arrow_size
            angle = self.dim_line_angle
            if rotate:
                angle += 180.0

            self.add_blockref(
                arrow_name,
                insert=location,
                scale=scale,
                rotation=angle,
                dxfattribs=attribs,
            )
            location = connection_point(arrow_name, location, scale, angle)
        return location

    def add_radial_dim_line(self, end: UVec) -> None:
        """Add radial dimension line."""
        attribs = self.dimension_line.dxfattribs()
        self.add_line(
            self.center, end, dxfattribs=attribs, remove_hidden_lines=True
        )

    def add_radial_dim_line_from_text(self, start, end: UVec) -> None:
        """Add radial dimension line, starting point at the measurement text."""
        attribs = self.dimension_line.dxfattribs()
        hshift = self._total_text_width / 2
        if self.measurement.vertical_placement != 0:  # not center
            hshift = -hshift
        self.add_line(
            start + self.dim_line_vec * hshift,
            end,
            dxfattribs=attribs,
            remove_hidden_lines=False,
        )

    def add_horiz_ext_line_default(self, start: UVec) -> None:
        """Add horizontal outside extension line from start for default
        locations.
        """
        attribs = self.dimension_line.dxfattribs()
        self.add_line(start, self.outside_default_defpoint, dxfattribs=attribs)
        if self.measurement.vertical_placement == 0:
            hdist = self.arrows.arrow_size
        else:
            hdist = self._total_text_width
        angle = self.dim_line_angle % 360.0  # normalize 0 .. 360
        if 90 < angle <= 270:
            hdist = -hdist
        end = self.outside_default_defpoint + Vec2((hdist, 0))
        self.add_line(self.outside_default_defpoint, end, dxfattribs=attribs)

    def add_horiz_ext_line_user(self, start: UVec) -> None:
        """Add horizontal extension line from start for user defined locations."""
        measurement = self.measurement
        assert isinstance(measurement.user_location, Vec2)
        attribs = self.dimension_line.dxfattribs()
        self.add_line(start, measurement.user_location, dxfattribs=attribs)
        if measurement.vertical_placement == 0:
            hdist = self.arrows.arrow_size
        else:
            hdist = self._total_text_width
        if measurement.user_location.x <= self.point_on_circle.x:
            hdist = -hdist
        end = measurement.user_location + Vec2((hdist, 0))
        self.add_line(measurement.user_location, end, dxfattribs=attribs)

    def add_radial_ext_line_default(self, start: UVec) -> None:
        """Add radial outside extension line from start for default locations."""
        attribs = self.dimension_line.dxfattribs()
        length = self.measurement.text_gap + self._total_text_width
        end = start + self.dim_line_vec * length
        self.add_line(start, end, dxfattribs=attribs, remove_hidden_lines=True)

    def add_radial_ext_line_user(self, start: UVec) -> None:
        """Add radial outside extension line from start for user defined location."""
        attribs = self.dimension_line.dxfattribs()
        length = self._total_text_width / 2.0
        if self.measurement.vertical_placement == 0:
            length = -length
        end = self.measurement.user_location + self.dim_line_vec * length
        self.add_line(start, end, dxfattribs=attribs)

    def add_measurement_text(
        self, dim_text: str, pos: Vec2, rotation: float
    ) -> None:
        """Add measurement text to dimension BLOCK."""
        attribs = self.measurement.dxfattribs()
        self.add_text(dim_text, pos=pos, rotation=rotation, dxfattribs=attribs)

    def transform_ucs_to_wcs(self) -> None:
        """
        Transforms dimension definition points into WCS or if required into OCS.

        Can not be called in __init__(), because inherited classes may be need unmodified values.

        """

        def from_ucs(attr, func):
            point = self.dimension.get_dxf_attrib(attr)
            self.dimension.set_dxf_attrib(attr, func(point))

        ucs = self.geometry.ucs
        from_ucs("defpoint", ucs.to_wcs)
        from_ucs("defpoint4", ucs.to_wcs)
        from_ucs("text_midpoint", ucs.to_ocs)


def add_center_mark(dim: RadiusDimension) -> None:
    """Add center mark/lines to radius and diameter dimensions.

    Args:
        dim: RadiusDimension or DiameterDimension renderer
    """
    dim_type = dim.dimension.dimtype
    if dim_type == 4:  # Radius Dimension
        radius = dim.measurement.raw_value
    elif dim_type == 3:  # Diameter Dimension
        radius = dim.measurement.raw_value / 2.0
    else:
        raise TypeError(f"Invalid dimension type: {dim_type}")

    mark_size = dim.dim_style.get("dimcen", 0)
    if mark_size == 0:
        return

    center_lines = False
    if mark_size < 0:
        mark_size = abs(mark_size)
        center_lines = True
    center = Vec2(dim.center)

    # draw center mark
    mark_x_vec = Vec2((mark_size, 0))
    mark_y_vec = Vec2((0, mark_size))
    # use only  color and ignore linetype!
    dxfattribs = {"color": dim.dimension_line.color}
    dim.add_line(center - mark_x_vec, center + mark_x_vec, dxfattribs)
    dim.add_line(center - mark_y_vec, center + mark_y_vec, dxfattribs)

    if center_lines:
        size = mark_size + radius
        if size < 2 * mark_size:
            return  # not enough space for center lines
        start_x_vec = mark_x_vec * 2
        start_y_vec = mark_y_vec * 2
        end_x_vec = Vec2((size, 0))
        end_y_vec = Vec2((0, size))
        dim.add_line(center + start_x_vec, center + end_x_vec, dxfattribs)
        dim.add_line(center - start_x_vec, center - end_x_vec, dxfattribs)
        dim.add_line(center + start_y_vec, center + end_y_vec, dxfattribs)
        dim.add_line(center - start_y_vec, center - end_y_vec, dxfattribs)
