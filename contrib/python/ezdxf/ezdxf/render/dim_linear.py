# Copyright (c) 2018-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, cast, Optional
import math
from ezdxf.math import Vec3, Vec2, UVec, ConstructionRay, UCS
from ezdxf.render.arrows import ARROWS, connection_point
from ezdxf.entities.dimstyleoverride import DimStyleOverride

from .dim_base import (
    BaseDimensionRenderer,
    LengthMeasurement,
    Measurement,
    compile_mtext,
    order_leader_points,
)

if TYPE_CHECKING:
    from ezdxf.entities import Dimension
    from ezdxf.eztypes import GenericLayoutType


class LinearDimension(BaseDimensionRenderer):
    """Linear dimension line renderer, used for horizontal, vertical, rotated
    and aligned DIMENSION entities.

    Args:
        dimension: DXF entity DIMENSION
        ucs: user defined coordinate system
        override: dimension style override management object

    """

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        super().__init__(dimension, ucs, override)
        measurement = self.measurement
        if measurement.text_movement_rule == 0:
            # moves the dimension line with dimension text, this makes no sense
            # for ezdxf (just set `base` argument)
            measurement.text_movement_rule = 2

        self.oblique_angle: float = self.dimension.get_dxf_attrib(
            "oblique_angle", 90
        )
        self.dim_line_angle: float = self.dimension.get_dxf_attrib("angle", 0)
        self.dim_line_angle_rad: float = math.radians(self.dim_line_angle)
        self.ext_line_angle: float = self.dim_line_angle + self.oblique_angle
        self.ext_line_angle_rad: float = math.radians(self.ext_line_angle)

        # text is aligned to dimension line
        measurement.text_rotation = self.dim_line_angle
        # text above extension line, is always aligned with extension lines
        if measurement.text_halign in (3, 4):
            measurement.text_rotation = self.ext_line_angle

        self.ext1_line_start = Vec2(self.dimension.dxf.defpoint2)
        self.ext2_line_start = Vec2(self.dimension.dxf.defpoint3)

        ext1_ray = ConstructionRay(
            self.ext1_line_start, angle=self.ext_line_angle_rad
        )
        ext2_ray = ConstructionRay(
            self.ext2_line_start, angle=self.ext_line_angle_rad
        )
        dim_line_ray = ConstructionRay(
            self.dimension.dxf.defpoint, angle=self.dim_line_angle_rad
        )

        self.dim_line_start: Vec2 = dim_line_ray.intersect(ext1_ray)
        self.dim_line_end: Vec2 = dim_line_ray.intersect(ext2_ray)
        self.dim_line_center: Vec2 = self.dim_line_start.lerp(self.dim_line_end)

        if self.dim_line_start == self.dim_line_end:
            self.dim_line_vec = Vec2.from_angle(self.dim_line_angle_rad)
        else:
            self.dim_line_vec = (
                self.dim_line_end - self.dim_line_start
            ).normalize()

        # set dimension defpoint to expected location - 3D vertex required!
        self.dimension.dxf.defpoint = Vec3(self.dim_line_start)

        raw_measurement = (self.dim_line_end - self.dim_line_start).magnitude
        measurement.update(raw_measurement)

        # only for linear dimension in multi point mode
        self.multi_point_mode = self.dim_style.pop("multi_point_mode", False)

        # 1 .. move wide text up
        # 2 .. move wide text down
        # None .. ignore
        self.move_wide_text: Optional[bool] = self.dim_style.pop(
            "move_wide_text", None
        )

        # actual text width in drawing units
        self._total_text_width: float = 0

        # arrows
        self.required_arrows_space: float = (
            2 * self.arrows.arrow_size + measurement.text_gap
        )
        self.arrows_outside: bool = self.required_arrows_space > raw_measurement

        # text location and rotation
        if measurement.text:
            # text width and required space
            self._total_text_width = self.total_text_width()
            if self.tol.has_limits:
                # limits show the upper and lower limit of the measurement as
                # stacked values and with the size of tolerances
                self.tol.update_limits(self.measurement.value)

            if self.multi_point_mode:
                # ezdxf has total control about vertical text position in multi
                # point mode
                measurement.text_vertical_position = 0.0

            if (
                measurement.text_valign == 0
                and abs(measurement.text_vertical_position) < 0.7
            ):
                # vertical centered text needs also space for arrows
                required_space = (
                    self._total_text_width + 2 * self.arrows.arrow_size
                )
            else:
                required_space = self._total_text_width
            measurement.is_wide_text = required_space > raw_measurement

            if not measurement.force_text_inside:
                # place text outside if wide text and not forced inside
                measurement.text_is_outside = measurement.is_wide_text
            elif measurement.is_wide_text and measurement.text_halign < 3:
                # center wide text horizontal
                measurement.text_halign = 0

            # use relative text shift to move wide text up or down in multi
            # point mode
            if (
                self.multi_point_mode
                and measurement.is_wide_text
                and self.move_wide_text
            ):
                shift_value = measurement.text_height + measurement.text_gap
                if self.move_wide_text == 1:  # move text up
                    measurement.text_shift_v = shift_value
                    if (
                        measurement.vertical_placement == -1
                    ):  # text below dimension line
                        # shift again
                        measurement.text_shift_v += shift_value
                elif self.move_wide_text == 2:  # move text down
                    measurement.text_shift_v = -shift_value
                    if (
                        measurement.vertical_placement == 1
                    ):  # text above dimension line
                        # shift again
                        measurement.text_shift_v -= shift_value

            # get final text location - no altering after this line
            measurement.text_location = self.get_text_location()

            # text rotation override
            rotation: float = measurement.text_rotation
            if measurement.user_text_rotation is not None:
                rotation = measurement.user_text_rotation
            elif (
                measurement.text_is_outside
                and measurement.text_outside_horizontal
            ):
                rotation = 0.0
            elif (
                measurement.text_is_inside
                and measurement.text_inside_horizontal
            ):
                rotation = 0.0
            measurement.text_rotation = rotation

            text_box = self.init_text_box()
            self.geometry.set_text_box(text_box)
            if measurement.has_leader:
                p1, p2, *_ = text_box.corners
                self.leader1, self.leader2 = order_leader_points(
                    self.dim_line_center, p1, p2
                )
                # not exact what BricsCAD (AutoCAD) expect, but close enough
                self.dimension.dxf.text_midpoint = self.leader1
            else:
                # write final text location into DIMENSION entity
                self.dimension.dxf.text_midpoint = measurement.text_location

    def init_measurement(self, color: int, scale: float) -> Measurement:
        return LengthMeasurement(
            self.dim_style, self.default_color, self.dim_scale
        )

    def render(self, block: GenericLayoutType) -> None:
        """Main method to create dimension geometry of basic DXF entities in the
        associated BLOCK layout.

        Args:
            block: target BLOCK for rendering

        """
        # call required to setup some requirements
        super().render(block)

        # add extension line 1
        ext_lines = self.extension_lines
        measurement = self.measurement
        if not ext_lines.suppress1:
            above_ext_line1 = measurement.text_halign == 3
            start, end = self.extension_line_points(
                self.ext1_line_start, self.dim_line_start, above_ext_line1
            )
            self.add_line(start, end, dxfattribs=ext_lines.dxfattribs(1))

        # add extension line 2
        if not ext_lines.suppress2:
            above_ext_line2 = measurement.text_halign == 4
            start, end = self.extension_line_points(
                self.ext2_line_start, self.dim_line_end, above_ext_line2
            )
            self.add_line(start, end, dxfattribs=ext_lines.dxfattribs(2))

        # add arrow symbols (block references), also adjust dimension line start
        # and end point
        dim_line_start, dim_line_end = self.add_arrows()

        # add dimension line
        self.add_dimension_line(dim_line_start, dim_line_end)

        # add measurement text as last entity to see text fill properly
        if measurement.text:
            if self.geometry.supports_dxf_r2000:
                text = compile_mtext(measurement, self.tol)
            else:
                text = measurement.text
            self.add_measurement_text(
                text, measurement.text_location, measurement.text_rotation
            )
            if measurement.has_leader:
                self.add_leader(
                    self.dim_line_center, self.leader1, self.leader2
                )

        # add POINT entities at definition points
        self.geometry.add_defpoints(
            [self.dim_line_start, self.ext1_line_start, self.ext2_line_start]
        )

    def get_text_location(self) -> Vec2:
        """Get text midpoint in UCS from user defined location or default text
        location.

        """
        # apply relative text shift as user location override without leader
        measurement = self.measurement
        if measurement.has_relative_text_movement:
            location = self.default_text_location()
            location = measurement.apply_text_shift(
                location, measurement.text_rotation
            )
            self.location_override(location)

        if measurement.user_location is not None:
            location = measurement.user_location
            if measurement.relative_user_location:
                location = self.dim_line_center + location
            # define overridden text location as outside
            measurement.text_is_outside = True
        else:
            location = self.default_text_location()

        return location

    def default_text_location(self) -> Vec2:
        """Calculate default text location in UCS based on `self.text_halign`,
        `self.text_valign` and `self.text_outside`

        """
        start = self.dim_line_start
        end = self.dim_line_end
        measurement = self.measurement
        halign = measurement.text_halign
        # positions the text above and aligned with the first/second extension line
        ext_lines = self.extension_lines
        if halign in (3, 4):
            # horizontal location
            hdist = measurement.text_gap + measurement.text_height / 2.0
            hvec = self.dim_line_vec * hdist
            location = (start if halign == 3 else end) - hvec
            # vertical location
            vdist = ext_lines.extension_above + self._total_text_width / 2.0
            location += Vec2.from_deg_angle(self.ext_line_angle).normalize(
                vdist
            )
        else:
            # relocate outside text to center location
            if measurement.text_is_outside:
                halign = 0

            if halign == 0:
                location = self.dim_line_center  # center of dimension line
            else:
                hdist = (
                    self._total_text_width / 2.0
                    + self.arrows.arrow_size
                    + measurement.text_gap
                )
                if (
                    halign == 1
                ):  # positions the text next to the first extension line
                    location = start + (self.dim_line_vec * hdist)
                else:  # positions the text next to the second extension line
                    location = end - (self.dim_line_vec * hdist)

            if measurement.text_is_outside:  # move text up
                vdist = (
                    ext_lines.extension_above
                    + measurement.text_gap
                    + measurement.text_height / 2.0
                )
            else:
                # distance from extension line to text midpoint
                vdist = measurement.text_vertical_distance()
            location += self.dim_line_vec.orthogonal().normalize(vdist)

        return location

    def add_arrows(self) -> tuple[Vec2, Vec2]:
        """
        Add arrows or ticks to dimension.

        Returns: dimension line connection points

        """
        arrows = self.arrows
        attribs = arrows.dxfattribs()
        start = self.dim_line_start
        end = self.dim_line_end
        outside = self.arrows_outside
        arrow1 = not arrows.suppress1
        arrow2 = not arrows.suppress2

        if arrows.tick_size > 0.0:  # oblique stroke, but double the size
            if arrow1:
                self.add_blockref(
                    ARROWS.oblique,
                    insert=start,
                    rotation=self.dim_line_angle,
                    scale=arrows.tick_size * 2.0,
                    dxfattribs=attribs,
                )
            if arrow2:
                self.add_blockref(
                    ARROWS.oblique,
                    insert=end,
                    rotation=self.dim_line_angle,
                    scale=arrows.tick_size * 2.0,
                    dxfattribs=attribs,
                )
        else:
            scale = arrows.arrow_size
            start_angle = self.dim_line_angle + 180.0
            end_angle = self.dim_line_angle
            if outside:
                start_angle, end_angle = end_angle, start_angle

            if arrow1:
                self.add_blockref(
                    arrows.arrow1_name,
                    insert=start,
                    scale=scale,
                    rotation=start_angle,
                    dxfattribs=attribs,
                )  # reverse
            if arrow2:
                self.add_blockref(
                    arrows.arrow2_name,
                    insert=end,
                    scale=scale,
                    rotation=end_angle,
                    dxfattribs=attribs,
                )

            if not outside:
                # arrows inside extension lines: adjust connection points for
                # the remaining dimension line
                if arrow1:
                    start = connection_point(
                        arrows.arrow1_name, start, scale, start_angle
                    )
                if arrow2:
                    end = connection_point(
                        arrows.arrow2_name, end, scale, end_angle
                    )
            else:
                # add additional extension lines to arrows placed outside of
                # dimension extension lines
                self.add_arrow_extension_lines()
        return start, end

    def add_arrow_extension_lines(self):
        """Add extension lines to arrows placed outside of dimension extension
        lines. Called by `self.add_arrows()`.

        """

        def has_arrow_extension(name: str) -> bool:
            return (
                (name is not None)
                and (name in ARROWS)
                and (name not in ARROWS.ORIGIN_ZERO)
            )

        attribs = self.dimension_line.dxfattribs()
        arrows = self.arrows
        start = self.dim_line_start
        end = self.dim_line_end
        arrow_size = arrows.arrow_size

        if not arrows.suppress1 and has_arrow_extension(arrows.arrow1_name):
            self.add_line(
                start - self.dim_line_vec * arrow_size,
                start - self.dim_line_vec * (2 * arrow_size),
                dxfattribs=attribs,
            )

        if not arrows.suppress2 and has_arrow_extension(arrows.arrow2_name):
            self.add_line(
                end + self.dim_line_vec * arrow_size,
                end + self.dim_line_vec * (2 * arrow_size),
                dxfattribs=attribs,
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
        self.add_text(dim_text, pos, rotation, dict())

    def add_dimension_line(self, start: Vec2, end: Vec2) -> None:
        """Add dimension line to dimension BLOCK, adds extension DIMDLE if
        required, and uses DIMSD1 or DIMSD2 to suppress first or second part of
        dimension line. Removes line parts hidden by dimension text.

        Args:
            start: dimension line start
            end: dimension line end

        """
        dim_line = self.dimension_line
        arrows = self.arrows
        extension = self.dim_line_vec * dim_line.extension
        ticks = arrows.has_ticks
        if ticks or ARROWS.has_extension_line(arrows.arrow1_name):
            start = start - extension
        if ticks or ARROWS.has_extension_line(arrows.arrow2_name):
            end = end + extension

        attribs = dim_line.dxfattribs()

        if dim_line.suppress1 or dim_line.suppress2:
            # TODO: results not as expected, but good enough
            # center should take into account text location
            center = start.lerp(end)
            if not dim_line.suppress1:
                self.add_line(
                    start, center, dxfattribs=attribs, remove_hidden_lines=True
                )
            if not dim_line.suppress2:
                self.add_line(
                    center, end, dxfattribs=attribs, remove_hidden_lines=True
                )
        else:
            self.add_line(
                start, end, dxfattribs=attribs, remove_hidden_lines=True
            )

    def extension_line_points(
        self, start: Vec2, end: Vec2, text_above_extline=False
    ) -> tuple[Vec2, Vec2]:
        """Adjust start and end point of extension line by dimension variables
        DIMEXE, DIMEXO, DIMEXFIX, DIMEXLEN.

        Args:
            start: start point of extension line (measurement point)
            end: end point at dimension line
            text_above_extline: True if text is above and aligned with extension line

        Returns: adjusted start and end point

        """
        if start == end:
            direction = Vec2.from_deg_angle(self.ext_line_angle)
        else:
            direction = (end - start).normalize()
        if self.extension_lines.has_fixed_length:
            start = end - (direction * self.extension_lines.length_below)
        else:
            start = start + direction * self.extension_lines.offset
        extension = self.extension_lines.extension_above
        if text_above_extline:
            extension += self._total_text_width
        end = end + direction * extension
        return start, end

    def transform_ucs_to_wcs(self) -> None:
        """Transforms dimension definition points into WCS or if required into
        OCS.

        Can not be called in __init__(), because inherited classes may be need
        unmodified values.

        """

        def from_ucs(attr, func):
            point = self.dimension.get_dxf_attrib(attr)
            self.dimension.set_dxf_attrib(attr, func(point))

        ucs = self.geometry.ucs
        from_ucs("defpoint", ucs.to_wcs)
        from_ucs("defpoint2", ucs.to_wcs)
        from_ucs("defpoint3", ucs.to_wcs)
        from_ucs("text_midpoint", ucs.to_ocs)
        self.dimension.dxf.angle = ucs.to_ocs_angle_deg(
            self.dimension.dxf.angle
        )


CAN_SUPPRESS_ARROW1 = {
    ARROWS.dot,
    ARROWS.dot_small,
    ARROWS.dot_blank,
    ARROWS.origin_indicator,
    ARROWS.origin_indicator_2,
    ARROWS.dot_smallblank,
    ARROWS.none,
    ARROWS.oblique,
    ARROWS.box_filled,
    ARROWS.box,
    ARROWS.integral,
    ARROWS.architectural_tick,
}


def sort_projected_points(
    points: Iterable[UVec], angle: float = 0
) -> list[Vec2]:
    direction = Vec2.from_deg_angle(angle)
    projected_vectors = [(direction.project(Vec2(p)), p) for p in points]
    return [p for projection, p in sorted(projected_vectors)]


def multi_point_linear_dimension(
    layout: GenericLayoutType,
    base: UVec,
    points: Iterable[UVec],
    angle: float = 0,
    ucs: Optional[UCS] = None,
    avoid_double_rendering: bool = True,
    dimstyle: str = "EZDXF",
    override: Optional[dict] = None,
    dxfattribs=None,
    discard=False,
) -> None:
    """Creates multiple DIMENSION entities for each point pair in `points`.
    Measurement points will be sorted by appearance on the dimension line
    vector.

    Args:
        layout: target layout (model space, paper space or block)
        base: base point, any point on the dimension line vector will do
        points: iterable of measurement points
        angle: dimension line rotation in degrees (0=horizontal, 90=vertical)
        ucs: user defined coordinate system
        avoid_double_rendering: removes first extension line and arrow of
            following DIMENSION entity
        dimstyle: dimension style name
        override: dictionary of overridden dimension style attributes
        dxfattribs: DXF attributes for DIMENSION entities
        discard: discard rendering result for friendly CAD applications like
            BricsCAD to get a native and likely better rendering result.
            (does not work with AutoCAD)

    """

    def suppress_arrow1(dimstyle_override) -> bool:
        arrow_name1, arrow_name2 = dimstyle_override.get_arrow_names()
        if (arrow_name1 is None) or (arrow_name1 in CAN_SUPPRESS_ARROW1):
            return True
        else:
            return False

    points = sort_projected_points(points, angle)
    base = Vec2(base)
    override = override or {}
    override["dimtix"] = 1  # do not place measurement text outside
    override["dimtvp"] = 0  # do not place measurement text outside
    override["multi_point_mode"] = True
    # 1 .. move wide text up; 2 .. move wide text down; None .. ignore
    # moving text down, looks best combined with text fill bg: DIMTFILL = 1
    move_wide_text = 1
    _suppress_arrow1 = False
    first_run = True

    for p1, p2 in zip(points[:-1], points[1:]):
        _override = dict(override)
        _override["move_wide_text"] = move_wide_text
        if avoid_double_rendering and not first_run:
            _override["dimse1"] = 1
            _override["suppress_arrow1"] = _suppress_arrow1

        style = layout.add_linear_dim(
            Vec3(base),
            Vec3(p1),
            Vec3(p2),
            angle=angle,
            dimstyle=dimstyle,
            override=_override,
            dxfattribs=dxfattribs,
        )
        if first_run:
            _suppress_arrow1 = suppress_arrow1(style)

        renderer = cast(LinearDimension, style.render(ucs, discard=discard))
        if renderer.measurement.is_wide_text:
            # after wide text switch moving direction
            if move_wide_text == 1:
                move_wide_text = 2
            else:
                move_wide_text = 1
        else:  # reset to move text up
            move_wide_text = 1
        first_run = False
