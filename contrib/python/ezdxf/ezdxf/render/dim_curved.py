# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from abc import abstractmethod
import logging
import math

from ezdxf.math import (
    Vec2,
    Vec3,
    NULLVEC,
    UCS,
    decdeg2dms,
    arc_angle_span_rad,
    xround,
)
from ezdxf.entities import DimStyleOverride, Dimension, DXFEntity
from .dim_base import (
    BaseDimensionRenderer,
    get_required_defpoint,
    format_text,
    apply_dimpost,
    Tolerance,
    Measurement,
    LengthMeasurement,
    compile_mtext,
    order_leader_points,
    get_center_leader_points,
)
from ezdxf.render.arrows import ARROWS, arrow_length
from ezdxf.tools.text import is_upside_down_text_angle
from ezdxf.math import intersection_line_line_2d

if TYPE_CHECKING:
    from ezdxf.eztypes import GenericLayoutType

__all__ = ["AngularDimension", "Angular3PDimension", "ArcLengthDimension"]
logger = logging.getLogger("ezdxf")

ARC_PREFIX = "( "


def has_required_attributes(entity: DXFEntity, attrib_names: list[str]):
    has = entity.dxf.hasattr
    return all(has(attrib_name) for attrib_name in attrib_names)


GRAD = 200.0 / math.pi
DEG = 180.0 / math.pi


def format_angular_text(
    value: float,
    angle_units: int,
    dimrnd: Optional[float],
    dimdec: int,
    dimzin: int,
    dimdsep: str,
) -> str:
    def decimal_format(_value: float) -> str:
        return format_text(
            _value,
            dimrnd=dimrnd,
            dimdec=dimdec,
            dimzin=dimzin,
            dimdsep=dimdsep,
        )

    def dms_format(_value: float) -> str:
        if dimrnd is not None:
            _value = xround(_value, dimrnd)
        d, m, s = decdeg2dms(_value)
        if dimdec > 4:
            places = dimdec - 5
            s = round(s, places)
            return f"{d:.0f}°{m:.0f}'{decimal_format(s)}\""
        if dimdec > 2:
            return f"{d:.0f}°{m:.0f}'{s:.0f}\""
        if dimdec > 0:
            return f"{d:.0f}°{m:.0f}'"
        return f"{d:.0f}°"

    # angular_unit:
    # 0 = Decimal degrees
    # 1 = Degrees/minutes/seconds
    # 2 = Grad
    # 3 = Radians
    text = ""
    if angle_units == 0:
        text = decimal_format(value * DEG) + "°"
    elif angle_units == 1:
        text = dms_format(value * DEG)
    elif angle_units == 2:
        text = decimal_format(value * GRAD) + "g"
    elif angle_units == 3:
        text = decimal_format(value) + "r"
    return text


_ANGLE_UNITS = [
    DEG,
    DEG,
    GRAD,
    1.0,
]


def to_radians(value: float, dimaunit: int) -> float:
    try:
        return value / _ANGLE_UNITS[dimaunit]
    except IndexError:
        return value / DEG


class AngularTolerance(Tolerance):
    def __init__(
        self,
        dim_style: DimStyleOverride,
        cap_height: float = 1.0,
        width_factor: float = 1.0,
        dim_scale: float = 1.0,
        angle_units: int = 0,
    ):
        self.angular_units = angle_units
        super().__init__(dim_style, cap_height, width_factor, dim_scale)
        # Tolerance values are interpreted in dimaunit:
        # dimtp 1 means 1 degree for dimaunit = 0 or 1, but 1 radians for
        # dimaunit = 3
        # format_text() requires radians as input:
        self.update_tolerance_text(
            to_radians(self.maximum, angle_units),
            to_radians(self.minimum, angle_units),
        )

    def format_text(self, value: float) -> str:
        """Rounding and text formatting of tolerance `value`, removes leading
        and trailing zeros if necessary.

        """
        return format_angular_text(
            value=value,
            angle_units=self.angular_units,
            dimrnd=None,
            dimdec=self.decimal_places,
            dimzin=self.suppress_zeros,
            dimdsep=self.text_decimal_separator,
        )

    def update_limits(self, measurement: float) -> None:
        # measurement is in radians, tolerance values are interpreted in
        # dimaunit: dimtp 1 means 1 degree for dimaunit = 0 or 1,
        # but 1 radians for dimaunit = 3
        # format_text() requires radians as input:
        upper_limit = measurement + to_radians(self.maximum, self.angular_units)
        lower_limit = measurement - to_radians(self.minimum, self.angular_units)
        self.text_upper = self.format_text(upper_limit)
        self.text_lower = self.format_text(lower_limit)
        self.text_width = self.get_text_width(self.text_upper, self.text_lower)


class AngleMeasurement(Measurement):
    def update(self, raw_measurement_value: float) -> None:
        self.raw_value = raw_measurement_value
        self.value = raw_measurement_value
        self.text = self.text_override(raw_measurement_value)

    def format_text(self, value: float) -> str:
        text = format_angular_text(
            value=value,
            angle_units=self.angle_units,
            dimrnd=None,
            dimdec=self.angular_decimal_places,
            dimzin=self.angular_suppress_zeros << 2,  # convert to dimzin value
            dimdsep=self.decimal_separator,
        )
        if self.text_post_process_format:
            text = apply_dimpost(text, self.text_post_process_format)
        return text


def fits_into_arc_span(length: float, radius: float, arc_span: float) -> bool:
    required_arc_span: float = length / radius
    return arc_span > required_arc_span


class _CurvedDimensionLine(BaseDimensionRenderer):
    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        super().__init__(dimension, ucs, override)
        # Common parameters for all sub-classes:
        # Use hidden line detection for dimension line:
        # Disable expensive hidden line calculation if possible!
        self.remove_hidden_lines_of_dimline = True
        self.center_of_arc: Vec2 = self.get_center_of_arc()
        self.dim_line_radius: float = self.get_dim_line_radius()
        self.ext1_dir: Vec2 = self.get_ext1_dir()
        self.start_angle_rad: float = self.ext1_dir.angle
        self.ext2_dir: Vec2 = self.get_ext2_dir()
        self.end_angle_rad: float = self.ext2_dir.angle

        # Angle between extension lines for all curved dimensions:
        # equal to the angle measurement of angular dimensions
        self.arc_angle_span_rad: float = arc_angle_span_rad(
            self.start_angle_rad, self.end_angle_rad
        )
        self.center_angle_rad = (
            self.start_angle_rad + self.arc_angle_span_rad / 2.0
        )

        # Additional required parameters but calculated later by sub-classes:
        self.ext1_start = Vec2()  # start of 1st extension line
        self.ext2_start = Vec2()  # start of 2nd extension line

        # Class specific setup:
        self.update_measurement()
        if self.tol.has_limits:
            self.tol.update_limits(self.measurement.value)

        # Text width and -height is required first, text location and -rotation
        # are not valid yet:
        self.text_box = self.init_text_box()

        # Place arrows outside?
        self.arrows_outside = False

        self.setup_text_and_arrow_fitting()
        self.setup_text_location()

        # update text box location and -rotation:
        self.text_box.center = self.measurement.text_location
        self.text_box.angle = self.measurement.text_rotation
        self.geometry.set_text_box(self.text_box)

        # Update final text location in the DIMENSION entity:
        self.dimension.dxf.text_midpoint = self.measurement.text_location

    @property
    def ocs_center_of_arc(self) -> Vec3:
        return self.geometry.ucs.to_ocs(Vec3(self.center_of_arc))

    @property
    def dim_midpoint(self) -> Vec2:
        """Return the midpoint of the dimension line."""
        return self.center_of_arc + Vec2.from_angle(
            self.center_angle_rad, self.dim_line_radius
        )

    @abstractmethod
    def update_measurement(self) -> None:
        """Setup measurement text."""
        ...

    @abstractmethod
    def get_ext1_dir(self) -> Vec2:
        """Return the direction of the 1st extension line == start angle."""
        ...

    @abstractmethod
    def get_ext2_dir(self) -> Vec2:
        """Return the direction of the 2nd extension line == end angle."""
        ...

    @abstractmethod
    def get_center_of_arc(self) -> Vec2:
        """Return the center of the arc."""
        ...

    @abstractmethod
    def get_dim_line_radius(self) -> float:
        """Return the distance from the center of the arc to the dimension line
        location
        """
        ...

    @abstractmethod
    def get_defpoints(self) -> list[Vec2]:
        ...

    def transform_ucs_to_wcs(self) -> None:
        """Transforms dimension definition points into WCS or if required into
        OCS.
        """

        def from_ucs(attr, func):
            if dxf.is_supported(attr):
                point = dxf.get(attr, NULLVEC)
                dxf.set(attr, func(point))

        dxf = self.dimension.dxf
        ucs = self.geometry.ucs
        from_ucs("defpoint", ucs.to_wcs)
        from_ucs("defpoint2", ucs.to_wcs)
        from_ucs("defpoint3", ucs.to_wcs)
        from_ucs("defpoint4", ucs.to_wcs)
        from_ucs("defpoint5", ucs.to_wcs)
        from_ucs("text_midpoint", ucs.to_ocs)

    def default_location(self, shift: float = 0.0) -> Vec2:
        radius = (
            self.dim_line_radius
            + self.measurement.text_vertical_distance()
            + shift
        )
        text_radial_dir = Vec2.from_angle(self.center_angle_rad)
        return self.center_of_arc + text_radial_dir * radius

    def setup_text_and_arrow_fitting(self) -> None:
        # self.text_box.width includes the gaps between text and dimension line
        # Is the measurement text without the arrows too wide to fit between the
        # extension lines?
        self.measurement.is_wide_text = not fits_into_arc_span(
            self.text_box.width, self.dim_line_radius, self.arc_angle_span_rad
        )

        required_text_and_arrows_space: float = (
            # The suppression of the arrows is not taken into account:
            self.text_box.width
            + 2.0 * self.arrows.arrow_size
        )

        # dimatfit: measurement text fitting rule is ignored!
        # Place arrows outside?
        self.arrows_outside = not fits_into_arc_span(
            required_text_and_arrows_space,
            self.dim_line_radius,
            self.arc_angle_span_rad,
        )
        # Place measurement text outside?
        self.measurement.text_is_outside = not fits_into_arc_span(
            required_text_and_arrows_space * 1.1,  # add some extra space
            self.dim_line_radius,
            self.arc_angle_span_rad,
        )

        if (
            self.measurement.text_is_outside
            and self.measurement.user_text_rotation is None
        ):
            # Intersection of the measurement text with the dimension line is
            # not possible:
            self.remove_hidden_lines_of_dimline = False

    def setup_text_location(self) -> None:
        """Setup geometric text properties (location, rotation) and the TextBox
        object.
        """
        # dimtix: measurement.force_text_inside is ignored
        # dimtih: measurement.text_inside_horizontal is ignored
        # dimtoh: measurement.text_outside_horizontal is ignored

        # text radial direction = center -> text
        text_radial_dir: Vec2  # text "vertical" direction
        measurement = self.measurement

        # determine text location:
        at_default_location: bool = measurement.user_location is None
        has_text_shifting: bool = bool(
            measurement.text_shift_h or measurement.text_shift_v
        )
        if at_default_location:
            # place text in the "horizontal" center of the dimension line at the
            # default location defined by measurement.text_valign (dimtad):
            text_radial_dir = Vec2.from_angle(self.center_angle_rad)
            shift_text_upwards: float = 0.0
            if measurement.text_is_outside:
                # reset vertical alignment to "above"
                measurement.text_valign = 1
                if measurement.is_wide_text:
                    # move measurement text "above" the extension line endings:
                    shift_text_upwards = self.extension_lines.extension_above
            measurement.text_location = self.default_location(
                shift=shift_text_upwards
            )
            if (
                measurement.text_valign > 0 and not has_text_shifting
            ):  # not in the center and no text shifting is applied
                # disable expensive hidden line calculation
                self.remove_hidden_lines_of_dimline = False
        else:
            # apply dimtmove: measurement.text_movement_rule
            user_location = measurement.user_location
            assert isinstance(user_location, Vec2)
            if measurement.relative_user_location:
                user_location += self.dim_midpoint
            measurement.text_location = user_location
            if measurement.text_movement_rule == 0:
                # Moves the dimension line with dimension text and
                # aligns the text direction perpendicular to the connection
                # line from the arc center to the text center:
                self.dim_line_radius = (
                    self.center_of_arc - user_location
                ).magnitude
                # Attributes about the text and arrow fitting have to be
                # updated now:
                self.setup_text_and_arrow_fitting()
            elif measurement.text_movement_rule == 1:
                # Adds a leader when dimension text, text direction is
                # "horizontal" or user text rotation if given.
                # Leader location is defined by dimtad (text_valign):
                # "center" - connects to the left or right center of the text
                # "below" - add a line below the text
                if measurement.user_text_rotation is None:
                    # override text rotation
                    measurement.user_text_rotation = 0.0
                measurement.text_is_outside = True  # by definition
            elif measurement.text_movement_rule == 2:
                # Allows text to be moved freely without a leader and
                # aligns the text direction perpendicular to the connection
                # line from the arc center to the text center:
                measurement.text_is_outside = True  # by definition
            text_radial_dir = (
                measurement.text_location - self.center_of_arc
            ).normalize()

        # set text "horizontal":
        text_tangential_dir = text_radial_dir.orthogonal(ccw=False)

        if at_default_location and has_text_shifting:
            # Apply text relative shift (ezdxf only feature)
            if measurement.text_shift_h:
                measurement.text_location += (
                    text_tangential_dir * measurement.text_shift_h
                )
            if measurement.text_shift_v:
                measurement.text_location += (
                    text_radial_dir * measurement.text_shift_v
                )

        # apply user text rotation; rotation in degrees:
        if measurement.user_text_rotation is None:
            rotation = text_tangential_dir.angle_deg
        else:
            rotation = measurement.user_text_rotation

        if not self.geometry.requires_extrusion:
            # todo: extrusion vector (0, 0, -1)?
            # Practically all DIMENSION entities are 2D entities,
            # where OCS == WCS, check WCS text orientation:
            wcs_angle = self.geometry.ucs.to_ocs_angle_deg(rotation)
            if is_upside_down_text_angle(wcs_angle):
                measurement.has_upside_down_correction = True
                rotation += 180.0  # apply to UCS rotation!
        measurement.text_rotation = rotation

    def get_leader_points(self) -> tuple[Vec2, Vec2]:
        # Leader location is defined by dimtad (text_valign):
        # "center":
        # - connects to the left or right vertical center of the text
        # - distance between text and leader line is measurement.text_gap (dimgap)
        #   and is already included in the text_box corner points
        # - length of "leg": arrows.arrow_size
        # "below" - add a line below the text
        if self.measurement.text_valign == 0:  # "center"
            return get_center_leader_points(
                self.dim_midpoint, self.text_box, self.arrows.arrow_size
            )
        else:  # "below"
            c0, c1, c2, c3 = self.text_box.corners
            if self.measurement.has_upside_down_correction:
                p1, p2 = c2, c3
            else:
                p1, p2 = c0, c1
            return order_leader_points(self.dim_midpoint, p1, p2)

    def render(self, block: GenericLayoutType) -> None:
        """Main method to create dimension geometry of basic DXF entities in the
        associated BLOCK layout.

        Args:
            block: target BLOCK for rendering

        """
        super().render(block)
        self.add_extension_lines()
        adjust_start_angle, adjust_end_angle = self.add_arrows()

        measurement = self.measurement
        if measurement.text:
            if self.geometry.supports_dxf_r2000:
                text = compile_mtext(measurement, self.tol)
            else:
                text = measurement.text
            self.add_measurement_text(
                text, measurement.text_location, measurement.text_rotation
            )
            if measurement.has_leader:
                p1, p2 = self.get_leader_points()
                self.add_leader(self.dim_midpoint, p1, p2)
        self.add_dimension_line(adjust_start_angle, adjust_end_angle)
        self.geometry.add_defpoints(self.get_defpoints())

    def add_extension_lines(self) -> None:
        ext_lines = self.extension_lines
        if not ext_lines.suppress1:
            self._add_ext_line(
                self.ext1_start, self.ext1_dir, ext_lines.dxfattribs(1)
            )
        if not ext_lines.suppress2:
            self._add_ext_line(
                self.ext2_start, self.ext2_dir, ext_lines.dxfattribs(2)
            )

    def _add_ext_line(self, start: Vec2, direction: Vec2, dxfattribs) -> None:
        ext_lines = self.extension_lines
        center = self.center_of_arc
        radius = self.dim_line_radius
        ext_above = ext_lines.extension_above
        is_inside = (start - center).magnitude > radius

        if ext_lines.has_fixed_length:
            ext_below = ext_lines.length_below
            if is_inside:
                ext_below, ext_above = ext_above, ext_below
            start = center + direction * (radius - ext_below)
        else:
            offset = ext_lines.offset
            if is_inside:
                ext_above = -ext_above
                offset = -offset
            start += direction * offset
        end = center + direction * (radius + ext_above)
        self.add_line(start, end, dxfattribs=dxfattribs)

    def add_arrows(self) -> tuple[float, float]:
        """Add arrows or ticks to dimension.

        Returns: dimension start- and end angle offsets to adjust the
            dimension line

        """
        arrows = self.arrows
        attribs = arrows.dxfattribs()
        radius = self.dim_line_radius
        if abs(radius) < 1e-12:
            return 0.0, 0.0

        start = self.center_of_arc + self.ext1_dir * radius
        end = self.center_of_arc + self.ext2_dir * radius
        angle1 = self.ext1_dir.orthogonal().angle_deg
        angle2 = self.ext2_dir.orthogonal().angle_deg
        outside = self.arrows_outside
        arrow1 = not arrows.suppress1
        arrow2 = not arrows.suppress2
        start_angle_offset = 0.0
        end_angle_offset = 0.0
        if arrows.tick_size > 0.0:  # oblique stroke, but double the size
            if arrow1:
                self.add_blockref(
                    ARROWS.oblique,
                    insert=start,
                    rotation=angle1,
                    scale=arrows.tick_size * 2.0,
                    dxfattribs=attribs,
                )
            if arrow2:
                self.add_blockref(
                    ARROWS.oblique,
                    insert=end,
                    rotation=angle2,
                    scale=arrows.tick_size * 2.0,
                    dxfattribs=attribs,
                )
        else:
            arrow_size = arrows.arrow_size
            # Note: The arrow blocks are correct as they are!
            # The arrow head is tilted to match the connection point of the
            # dimension line (even for datum arrows).
            # tilting angle = 1/2 of the arc angle defined by the arrow length
            arrow_tilt: float = arrow_size / radius * 0.5 * DEG
            start_angle = angle1 + 180.0
            end_angle = angle2
            if outside:
                start_angle += 180.0
                end_angle += 180.0
                arrow_tilt = -arrow_tilt
            scale = arrow_size
            if arrow1:
                self.add_blockref(
                    arrows.arrow1_name,
                    insert=start,
                    scale=scale,
                    rotation=start_angle + arrow_tilt,
                    dxfattribs=attribs,
                )  # reverse
            if arrow2:
                self.add_blockref(
                    arrows.arrow2_name,
                    insert=end,
                    scale=scale,
                    rotation=end_angle - arrow_tilt,
                    dxfattribs=attribs,
                )
            if not outside:
                # arrows inside extension lines:
                # adjust angles for the remaining dimension line
                if arrow1:
                    start_angle_offset = (
                        arrow_length(arrows.arrow1_name, arrow_size) / radius
                    )
                if arrow2:
                    end_angle_offset = (
                        arrow_length(arrows.arrow2_name, arrow_size) / radius
                    )
        return start_angle_offset, end_angle_offset

    def add_dimension_line(
        self,
        start_offset: float,
        end_offset: float,
    ) -> None:
        # Start- and end angle adjustments have to be limited between the
        # extension lines.
        # Negative offset extends the dimension line outside!
        start_angle: float = self.start_angle_rad
        end_angle: float = self.end_angle_rad
        arrows = self.arrows
        size = arrows.arrow_size
        radius = self.dim_line_radius
        max_adjustment: float = abs(self.arc_angle_span_rad) / 2.0

        if start_offset > max_adjustment:
            start_offset = 0.0
        if end_offset > max_adjustment:
            end_offset = 0.0

        self.add_arc(
            self.center_of_arc,
            radius,
            start_angle + start_offset,
            end_angle - end_offset,
            dxfattribs=self.dimension_line.dxfattribs(),
            # hidden line detection if text is not placed outside:
            remove_hidden_lines=self.remove_hidden_lines_of_dimline,
        )
        if self.arrows_outside and not arrows.has_ticks:
            # add arrow extension lines
            start_offset, end_offset = arrow_offset_angles(
                arrows.arrow1_name, size, radius
            )
            self.add_arrow_extension_line(
                start_angle - end_offset,
                start_angle - start_offset,
            )
            start_offset, end_offset = arrow_offset_angles(
                arrows.arrow1_name, size, radius
            )
            self.add_arrow_extension_line(
                end_angle + start_offset,
                end_angle + end_offset,
            )

    def add_arrow_extension_line(self, start_angle: float, end_angle: float):
        self.add_arc(
            self.center_of_arc,
            self.dim_line_radius,
            start_angle=start_angle,
            end_angle=end_angle,
            dxfattribs=self.dimension_line.dxfattribs(),
        )

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


class _AngularCommonBase(_CurvedDimensionLine):
    def init_tolerance(
        self, scale: float, measurement: Measurement
    ) -> Tolerance:
        return AngularTolerance(
            self.dim_style,
            cap_height=measurement.text_height,
            width_factor=measurement.text_width_factor,
            dim_scale=scale,
            angle_units=measurement.angle_units,
        )

    def init_measurement(self, color: int, scale: float) -> Measurement:
        return AngleMeasurement(
            self.dim_style, self.default_color, self.dim_scale
        )

    def update_measurement(self) -> None:
        self.measurement.update(self.arc_angle_span_rad)


class AngularDimension(_AngularCommonBase):
    """
    Angular dimension line renderer. The dimension line is defined by two lines.

    Supported render types:

    - default location above
    - default location center
    - user defined location, text aligned with dimension line
    - user defined location horizontal text

    Args:
        dimension: DIMENSION entity
        ucs: user defined coordinate system
        override: dimension style override management object

    """

    # Required defpoints:
    # defpoint = start point of 1st leg (group code 10)
    # defpoint4 = end point of 1st leg (group code 15)
    # defpoint3 = start point of 2nd leg (group code 14)
    # defpoint2 = end point of 2nd leg (group code 13)
    # defpoint5 = location of dimension line (group code 16)

    # unsupported or ignored features (at least by BricsCAD):
    # dimtih: text inside horizontal
    # dimtoh: text outside horizontal
    # dimjust: text position horizontal
    # dimdle: dimline extension

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        self.leg1_start = get_required_defpoint(dimension, "defpoint")
        self.leg1_end = get_required_defpoint(dimension, "defpoint4")
        self.leg2_start = get_required_defpoint(dimension, "defpoint3")
        self.leg2_end = get_required_defpoint(dimension, "defpoint2")
        self.dim_line_location = get_required_defpoint(dimension, "defpoint5")
        super().__init__(dimension, ucs, override)
        # The extension line parameters depending on the location of the
        # dimension line related to the definition point.
        # Detect the extension start point.
        # Which definition point is closer to the dimension line:
        self.ext1_start = detect_closer_defpoint(
            direction=self.ext1_dir,
            base=self.dim_line_location,
            p1=self.leg1_start,
            p2=self.leg1_end,
        )
        self.ext2_start = detect_closer_defpoint(
            direction=self.ext2_dir,
            base=self.dim_line_location,
            p1=self.leg2_start,
            p2=self.leg2_end,
        )

    def get_defpoints(self) -> list[Vec2]:
        return [
            self.leg1_start,
            self.leg1_end,
            self.leg2_start,
            self.leg2_end,
            self.dim_line_location,
        ]

    def get_center_of_arc(self) -> Vec2:
        center = intersection_line_line_2d(
            (self.leg1_start, self.leg1_end),
            (self.leg2_start, self.leg2_end),
        )
        if center is None:
            logger.warning(
                f"Invalid colinear or parallel angle legs found in {self.dimension})"
            )
            # This case can not be created by the GUI in BricsCAD, but DXF
            # files can contain any shit!
            # The interpolation of the end-points is an arbitrary choice and
            # maybe not the best choice!
            center = self.leg1_end.lerp(self.leg2_end)
        return center

    def get_dim_line_radius(self) -> float:
        return (self.dim_line_location - self.center_of_arc).magnitude

    def get_ext1_dir(self) -> Vec2:
        center = self.center_of_arc
        start = (
            self.leg1_end
            if self.leg1_start.isclose(center)
            else self.leg1_start
        )
        return (start - center).normalize()

    def get_ext2_dir(self) -> Vec2:
        center = self.center_of_arc
        start = (
            self.leg2_end
            if self.leg2_start.isclose(center)
            else self.leg2_start
        )
        return (start - center).normalize()


class Angular3PDimension(_AngularCommonBase):
    """
    Angular dimension line renderer. The dimension line is defined by three
    points.

    Supported render types:

    - default location above
    - default location center
    - user defined location, text aligned with dimension line
    - user defined location horizontal text

    Args:
        dimension: DIMENSION entity
        ucs: user defined coordinate system
        override: dimension style override management object

    """

    # Required defpoints:
    # defpoint = location of dimension line (group code 10)
    # defpoint2 = 1st leg (group code 13)
    # defpoint3 = 2nd leg (group code 14)
    # defpoint4 = center of angle (group code 15)

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        self.dim_line_location = get_required_defpoint(dimension, "defpoint")
        self.leg1_start = get_required_defpoint(dimension, "defpoint2")
        self.leg2_start = get_required_defpoint(dimension, "defpoint3")
        self.center_of_arc = get_required_defpoint(dimension, "defpoint4")
        super().__init__(dimension, ucs, override)
        self.ext1_start = self.leg1_start
        self.ext2_start = self.leg2_start

    def get_defpoints(self) -> list[Vec2]:
        return [
            self.dim_line_location,
            self.leg1_start,
            self.leg2_start,
            self.center_of_arc,
        ]

    def get_center_of_arc(self) -> Vec2:
        return self.center_of_arc

    def get_dim_line_radius(self) -> float:
        return (self.dim_line_location - self.center_of_arc).magnitude

    def get_ext1_dir(self) -> Vec2:
        return (self.leg1_start - self.center_of_arc).normalize()

    def get_ext2_dir(self) -> Vec2:
        return (self.leg2_start - self.center_of_arc).normalize()


class ArcLengthMeasurement(LengthMeasurement):
    def format_text(self, value: float) -> str:
        text = format_text(
            value=value,
            dimrnd=self.text_round,
            dimdec=self.decimal_places,
            dimzin=self.suppress_zeros,
            dimdsep=self.decimal_separator,
        )
        if self.has_arc_length_prefix:
            text = ARC_PREFIX + text
        if self.text_post_process_format:
            text = apply_dimpost(text, self.text_post_process_format)
        return text


class ArcLengthDimension(_CurvedDimensionLine):
    """Arc length dimension line renderer.
    Requires DXF R2004.

    Supported render types:

    - default location above
    - default location center
    - user defined location, text aligned with dimension line
    - user defined location horizontal text

    Args:
        dimension: DXF entity DIMENSION
        ucs: user defined coordinate system
        override: dimension style override management object

    """

    # Required defpoints:
    # defpoint = location of dimension line (group code 10)
    # defpoint2 = 1st arc point (group code 13)
    # defpoint3 = 2nd arc point (group code 14)
    # defpoint4 = center of arc (group code 15)

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        self.dim_line_location = get_required_defpoint(dimension, "defpoint")
        self.leg1_start = get_required_defpoint(dimension, "defpoint2")
        self.leg2_start = get_required_defpoint(dimension, "defpoint3")
        self.center_of_arc = get_required_defpoint(dimension, "defpoint4")
        self.arc_radius = (self.leg1_start - self.center_of_arc).magnitude
        super().__init__(dimension, ucs, override)
        self.ext1_start = self.leg1_start
        self.ext2_start = self.leg2_start

    def get_defpoints(self) -> list[Vec2]:
        return [
            self.dim_line_location,
            self.leg1_start,
            self.leg2_start,
            self.center_of_arc,
        ]

    def init_measurement(self, color: int, scale: float) -> Measurement:
        return ArcLengthMeasurement(
            self.dim_style, self.default_color, self.dim_scale
        )

    def get_center_of_arc(self) -> Vec2:
        return self.center_of_arc

    def get_dim_line_radius(self) -> float:
        return (self.dim_line_location - self.center_of_arc).magnitude

    def get_ext1_dir(self) -> Vec2:
        return (self.leg1_start - self.center_of_arc).normalize()

    def get_ext2_dir(self) -> Vec2:
        return (self.leg2_start - self.center_of_arc).normalize()

    def update_measurement(self) -> None:
        angle = arc_angle_span_rad(self.start_angle_rad, self.end_angle_rad)
        arc_length = angle * self.arc_radius
        self.measurement.update(arc_length)


def detect_closer_defpoint(
    direction: Vec2, base: Vec2, p1: Vec2, p2: Vec2
) -> Vec2:
    # Calculate the projected distance onto the (normalized) direction vector:
    d0 = direction.dot(base)
    d1 = direction.dot(p1)
    d2 = direction.dot(p2)
    # Which defpoint is closer to the base point (d0)?
    if abs(d1 - d0) <= abs(d2 - d0):
        return p1
    return p2


def arrow_offset_angles(
    arrow_name: str, size: float, radius: float
) -> tuple[float, float]:
    start_offset: float = 0.0
    end_offset: float = size / radius
    length = arrow_length(arrow_name, size)
    if length > 0.0:
        start_offset = length / radius
        end_offset *= 2.0
    return start_offset, end_offset
