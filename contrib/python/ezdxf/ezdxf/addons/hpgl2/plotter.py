#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Sequence, Iterator
import math

from .deps import (
    Vec2,
    Path,
    NULLVEC2,
    ConstructionCircle,
    Bezier4P,
)
from .properties import RGB, Properties, FillType
from .backend import Backend
from .polygon_buffer import PolygonBuffer
from .page import Page


class Plotter:
    """
    The :class:`Plotter` class represents a virtual plotter device.

    The HPGL/2 commands send by the :class:`Interpreter` are processed into simple
    polylines and filled polygons and send to low level :class:`Backend`.

    HPGL/2 uses a units system called "Plot Units":

    - 1 plot unit (plu) = 0.025mm
    - 40 plu = 1 mm
    - 1016 plu = 1 inch

    The Plotter device does not support font rendering and page rotation (RO).
    The scaling commands IP, RP, SC are supported.

    """
    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self._output_backend = backend
        self._polygon_buffer = PolygonBuffer()
        self.page = Page(1189, 841)
        self.properties = Properties()
        self.is_pen_down = False
        self.is_absolute_mode = True
        self.is_polygon_mode = False
        self.has_merge_control = False
        self._user_location = NULLVEC2
        self._pen_state_stack: list[bool] = []

    @property
    def user_location(self) -> Vec2:
        """Returns the current pen location as point in the user coordinate system."""
        return self._user_location

    @property
    def page_location(self) -> Vec2:
        """Returns the current pen location as page point in plotter units."""
        location = self.user_location
        return self.page.page_point(location.x, location.y)

    def setup_page(self, size_x: int, size_y: int):
        self.page = Page(size_x, size_y)

    def set_scaling_points(self, p1: Vec2, p2: Vec2) -> None:
        self.page.set_scaling_points(p1, p2)

    def set_scaling_points_relative_1(self, xp1: float, yp1: float) -> None:
        self.page.set_scaling_points_relative_1(xp1, yp1)

    def set_scaling_points_relative_2(
        self, xp1: float, yp1: float, xp2: float, yp2: float
    ) -> None:
        self.page.set_scaling_points_relative_2(xp1, yp1, xp2, yp2)

    def reset_scaling(self) -> None:
        self.page.reset_scaling()

    def set_point_factor(self, origin: Vec2, scale_x: float, scale_y: float) -> None:
        self.page.set_ucs(origin, scale_x, scale_y)

    def set_isotropic_scaling(
        self,
        x_min: float,
        x_max: float,
        y_min: float,
        y_max: float,
        left=0.5,
        bottom=0.5,
    ) -> None:
        self.page.set_isotropic_scaling(x_min, x_max, y_min, y_max, left, bottom)

    def set_anisotropic_scaling(
        self, x_min: float, x_max: float, y_min: float, y_max: float
    ) -> None:
        self.page.set_anisotropic_scaling(x_min, x_max, y_min, y_max)

    def set_merge_control(self, status: bool) -> None:
        self.has_merge_control = status

    def pen_up(self) -> None:
        self.is_pen_down = False

    def pen_down(self) -> None:
        self.is_pen_down = True

    def push_pen_state(self) -> None:
        self._pen_state_stack.append(self.is_pen_down)

    def pop_pen_state(self) -> None:
        if len(self._pen_state_stack):
            self.is_pen_down = self._pen_state_stack.pop()

    def move_to(self, location: Vec2) -> None:
        if self.is_absolute_mode:
            self.move_to_abs(location)
        else:
            self.move_to_rel(location)

    def move_to_abs(self, user_location: Vec2) -> None:
        self._user_location = user_location

    def move_to_rel(self, user_location: Vec2) -> None:
        self._user_location += user_location

    def set_absolute_mode(self) -> None:
        self.is_absolute_mode = True

    def set_relative_mode(self) -> None:
        self.is_absolute_mode = False

    def set_current_pen(self, index: int) -> None:
        self.properties.set_current_pen(index)

    def set_max_pen_count(self, index: int) -> None:
        self.properties.set_max_pen_count(index)

    def set_pen_width(self, index: int, width: float) -> None:
        self.properties.set_pen_width(index, width)

    def set_pen_color(self, index: int, color: RGB) -> None:
        self.properties.set_pen_color(index, color)

    def set_fill_type(self, fill_type: int, spacing: float, angle: float) -> None:
        if fill_type in (3, 4):  # adjust spacing between hatching lines
            spacing = max(self.page.scale_length(spacing))
        self.properties.set_fill_type(fill_type, spacing, angle)

    def enter_polygon_mode(self, status: int) -> None:
        self.is_polygon_mode = True
        self.backend = self._polygon_buffer
        if status == 0:
            self._polygon_buffer.reset(self.page_location)
        elif status == 1:
            self._polygon_buffer.close_path()

    def exit_polygon_mode(self) -> None:
        self.is_polygon_mode = False
        self._polygon_buffer.close_path()
        self.backend = self._output_backend

    def fill_polygon(self, fill_method: int) -> None:
        self.properties.set_fill_method(fill_method)
        self.plot_filled_polygon_buffer(self._polygon_buffer.get_paths())

    def edge_polygon(self) -> None:
        self.plot_outline_polygon_buffer(self._polygon_buffer.get_paths())

    def plot_polyline(self, points: Sequence[Vec2]):
        if not points:
            return
        if self.is_absolute_mode:
            self.plot_abs_polyline(points)
        else:
            self.plot_rel_polyline(points)

    def plot_abs_polyline(self, points: Sequence[Vec2]):
        # input coordinates are user coordinates
        if not points:
            return
        current_page_location = self.page_location
        self.move_to_abs(points[-1])  # user coordinates!
        if self.is_pen_down:
            # convert to page coordinates:
            points = self.page.page_points(points)
            # insert current page location as starting point:
            points.insert(0, current_page_location)
            # draw polyline in absolute page coordinates:
            self.backend.draw_polyline(self.properties, points)

    def plot_rel_polyline(self, points: Sequence[Vec2]):
        # input coordinates are user coordinates
        if not points:
            return
        # convert to absolute user coordinates:
        self.plot_abs_polyline(
            tuple(rel_to_abs_points_dynamic(self.user_location, points))
        )

    def plot_abs_circle(self, radius: float, chord_angle: float):
        # radius in user units
        if self.is_pen_down:
            center = self.user_location
            vertices = [
                center + Vec2.from_deg_angle(a, radius)
                for a in arc_angles(0, 360.0, chord_angle)
            ]
            # draw circle in absolute page coordinates:
            self.backend.draw_polyline(self.properties, vertices)

    def plot_abs_arc(self, center: Vec2, sweep_angle: float, chord_angle: float):
        start_point = self.user_location
        radius_vec = start_point - center
        radius = radius_vec.magnitude
        start_angle = radius_vec.angle_deg
        end_angle = start_angle + sweep_angle
        end_point = center + Vec2.from_deg_angle(end_angle, radius)

        self.move_to_abs(end_point)
        if self.is_pen_down:
            vertices = [
                center + Vec2.from_deg_angle(a, radius)
                for a in arc_angles(start_angle, sweep_angle, chord_angle)
            ]
            self.backend.draw_polyline(self.properties, vertices)

    def plot_rel_arc(self, center_rel: Vec2, sweep_angle: float, chord_angle: float):
        self.plot_abs_arc(center_rel + self.user_location, sweep_angle, chord_angle)

    def plot_abs_arc_three_points(self, inter: Vec2, end: Vec2, chord_angle: float):
        # input coordinates are user coordinates
        start = self.user_location
        circle = ConstructionCircle.from_3p(start, inter, end)
        center = circle.center
        start_angle = (start - center).angle_deg
        end_angle = (end - center).angle_deg
        inter_angle = (inter - center).angle_deg
        sweep_angle = sweeping_angle(start_angle, inter_angle, end_angle)
        self.plot_abs_arc(center, sweep_angle, chord_angle)

    def plot_rel_arc_three_points(self, inter: Vec2, end: Vec2, chord_angle: float):
        # input coordinates are user coordinates
        current = self.user_location
        self.plot_abs_arc_three_points(current + inter, current + end, chord_angle)

    def plot_abs_cubic_bezier(self, ctrl1: Vec2, ctrl2: Vec2, end: Vec2):
        # input coordinates are user coordinates
        current_page_location = self.page_location
        self.move_to_abs(end)  # user coordinates!
        if self.is_pen_down:
            # convert to page coordinates:
            ctrl1, ctrl2, end = self.page.page_points((ctrl1, ctrl2, end))
            # draw cubic bezier curve in absolute page coordinates:
            p = Path(current_page_location)
            p.curve4_to(end, ctrl1, ctrl2)
            self.backend.draw_paths(self.properties, [p], filled=False)

    def plot_rel_cubic_bezier(self, ctrl1: Vec2, ctrl2: Vec2, end: Vec2):
        # input coordinates are user coordinates
        ctrl1, ctrl2, end = rel_to_abs_points_static(
            self.user_location, (ctrl1, ctrl2, end)
        )
        self.plot_abs_cubic_bezier(ctrl1, ctrl2, end)

    def plot_filled_polygon_buffer(self, paths: Sequence[Path]):
        # input coordinates are page coordinates!
        self.backend.draw_paths(self.properties, paths, filled=True)

    def plot_outline_polygon_buffer(self, paths: Sequence[Path]):
        # input coordinates are page coordinates!
        self.backend.draw_paths(self.properties, paths, filled=False)


def rel_to_abs_points_dynamic(current: Vec2, points: Sequence[Vec2]) -> Iterator[Vec2]:
    """Returns the absolute location of increment points, each point is an increment
    of the previous point starting at the current pen location.
    """
    for point in points:
        current += point
        yield current


def rel_to_abs_points_static(current: Vec2, points: Sequence[Vec2]) -> Iterator[Vec2]:
    """Returns the absolute location of increment points, all points are relative
    to the current pen location.
    """
    for point in points:
        yield current + point


def arc_angles(start: float, sweep_angle: float, chord_angle: float) -> Iterator[float]:
    # clamp to 0.5 .. 180
    chord_angle = min(180.0, max(0.5, chord_angle))
    count = abs(round(sweep_angle / chord_angle))
    delta = sweep_angle / count
    for index in range(count + 1):
        yield start + delta * index


def sweeping_angle(start: float, intermediate: float, end: float) -> float:
    """Returns the sweeping angle from start angle to end angle passing the
    intermediate angle.
    """
    start = start % 360.0
    intermediate = intermediate % 360.0
    end = end % 360.0
    angle = end - start
    i_to_s = start - intermediate
    i_to_e = end - intermediate
    if math.isclose(abs(i_to_e) + abs(i_to_s), abs(angle)):
        return angle
    else:  # return complementary angle with opposite orientation
        if angle < 0:
            return 360.0 + angle
        else:
            return angle - 360.0
