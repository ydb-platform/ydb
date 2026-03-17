#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterator, Iterable
import string

from .deps import Vec2, NULLVEC2
from .properties import RGB
from .plotter import Plotter
from .tokenizer import Command, pe_decode


class Interpreter:
    """The :class:`Interpreter` is the frontend for the :class:`Plotter` class.
    The :meth:`run` methods interprets the low level HPGL commands from the
    :func:`hpgl2_commands` parser and sends the commands to the virtual plotter
    device, which sends his output to a low level :class:`Backend` class.

    Most CAD application send a very restricted subset of commands to plotters,
    mostly just polylines and filled polygons. Implementing the whole HPGL/2 command set
    is not worth the effort - unless reality proofs otherwise.

    Not implemented commands:

        - the whole character group - text is send as filled polygons or polylines
        - configuration group: IN, DF, RO, IW - the plotter is initialized by creating a
          new plotter and page rotation is handled by the add-on itself
        - polygon group: EA, ER, EW, FA, RR, WG, the rectangle and wedge commands
        - line and fill attributes group: LA, RF, SM, SV, TR, UL, WU, linetypes and
          hatch patterns are decomposed into simple lines by CAD applications

    Args:
        plotter: virtual :class:`Plotter` device

    """
    def __init__(self, plotter: Plotter) -> None:
        self.errors: list[str] = []
        self.not_implemented_commands: set[str] = set()
        self._disabled_commands: set[str] = set()
        self.plotter = plotter

    def add_error(self, error: str) -> None:
        self.errors.append(error)

    def run(self, commands: list[Command]) -> None:
        """Interprets the low level HPGL commands from the :func:`hpgl2_commands` parser
        and sends the commands to the virtual plotter device.
        """
        for name, args in commands:
            if name in self._disabled_commands:
                continue
            method = getattr(self, f"cmd_{name.lower()}", None)
            if method:
                method(args)
            elif name[0] in string.ascii_letters:
                self.not_implemented_commands.add(name)

    def disable_commands(self, commands: Iterable[str]) -> None:
        """Disable commands manually, like the scaling command ["SC", "IP", "IR"].
        This is a feature for experts, because disabling commands which changes the pen
        location may distort or destroy the plotter output.
        """
        self._disabled_commands.update(commands)

    # Configure pens, line types, fill types
    def cmd_ft(self, args: list[bytes]):
        """Set fill type."""
        fill_type = 1
        spacing = 0.0
        angle = 0.0
        values = tuple(to_floats(args))
        arg_count = len(values)
        if arg_count > 0:
            fill_type = int(values[0])
        if arg_count > 1:
            spacing = values[1]
        if arg_count > 2:
            angle = values[2]
        self.plotter.set_fill_type(fill_type, spacing, angle)

    def cmd_pc(self, args: list[bytes]):
        """Set pen color as RGB tuple."""
        values = list(to_ints(args))
        if len(values) == 4:
            index, r, g, b = values
            self.plotter.set_pen_color(index, RGB(r, g, b))
        else:
            self.add_error("invalid arguments for PC command")

    def cmd_pw(self, args: list[bytes]):
        """Set pen width."""
        arg_count = len(args)
        if arg_count:
            width = to_float(args[0], 0.35)
        else:
            self.add_error("invalid arguments for PW command")
            return
        index = -1
        if arg_count > 1:
            index = to_int(args[1], index)
        self.plotter.set_pen_width(index, width)

    def cmd_sp(self, args: list[bytes]):
        """Select pen."""
        if len(args):
            self.plotter.set_current_pen(to_int(args[0], 1))

    def cmd_np(self, args: list[bytes]):
        """Set number of pens."""
        if len(args):
            self.plotter.set_max_pen_count(to_int(args[0], 2))

    def cmd_ip(self, args: list[bytes]):
        """Set input points p1 and p2 absolute."""
        if len(args) == 0:
            self.plotter.reset_scaling()
            return

        points = to_points(to_floats(args))
        if len(points) > 1:
            self.plotter.set_scaling_points(points[0], points[1])
        else:
            self.add_error("invalid arguments for IP command")

    def cmd_ir(self, args: list[bytes]):
        """Set input points p1 and p2 in percentage of page size."""
        if len(args) == 0:
            self.plotter.reset_scaling()
            return

        values = list(to_floats(args))
        if len(values) == 2:
            xp1 = clamp(values[0], 0.0, 100.0)
            yp1 = clamp(values[1], 0.0, 100.0)
            self.plotter.set_scaling_points_relative_1(xp1 / 100.0, yp1 / 100.0)
        elif len(values) == 4:
            xp1 = clamp(values[0], 0.0, 100.0)
            yp1 = clamp(values[1], 0.0, 100.0)
            xp2 = clamp(values[2], 0.0, 100.0)
            yp2 = clamp(values[3], 0.0, 100.0)
            self.plotter.set_scaling_points_relative_2(
                xp1 / 100.0, yp1 / 100.0, xp2 / 100.0, yp2 / 100.0
            )
        else:
            self.add_error("invalid arguments for IP command")

    def cmd_sc(self, args: list[bytes]):
        if len(args) == 0:
            self.plotter.reset_scaling()
            return
        values = list(to_floats(args))
        if len(values) < 4:
            self.add_error("invalid arguments for SC command")
            return
        scaling_type = 0
        if len(values) > 4:
            scaling_type = int(values[4])
        if scaling_type == 1:  # isotropic
            left = 50.0
            if len(values) > 5:
                left = clamp(values[5], 0.0, 100.0)
            bottom = 50.0
            if len(values) > 6:
                bottom = clamp(values[6], 0.0, 100.0)
            self.plotter.set_isotropic_scaling(
                values[0],
                values[1],
                values[2],
                values[3],
                left,
                bottom,
            )
        elif scaling_type == 2:  # point factor
            self.plotter.set_point_factor(
                Vec2(values[0], values[2]), values[1], values[3]
            )
        else:  # anisotropic
            self.plotter.set_anisotropic_scaling(
                values[0], values[1], values[2], values[3]
            )

    def cmd_mc(self, args: list[bytes]):
        status = 0
        if len(args):
            status = to_int(args[0], status)
            self.plotter.set_merge_control(bool(status))

    def cmd_ps(self, args: list[bytes]):
        length = 1189  # A0
        height = 841
        count = len(args)
        if count:
            length = to_int(args[0], length)
            height = int(length * 1.5)
        if count > 1:
            height = to_int(args[1], height)
        self.plotter.setup_page(length, height)

    # pen movement:
    def cmd_pd(self, args: list[bytes]):
        """Lower pen down and plot lines."""
        self.plotter.pen_down()
        if len(args):
            self.plotter.plot_polyline(to_points(to_floats(args)))

    def cmd_pu(self, args: list[bytes]):
        """Lift pen up and move pen."""
        self.plotter.pen_up()
        if len(args):
            self.plotter.plot_polyline(to_points(to_floats(args)))

    def cmd_pa(self, args: list[bytes]):
        """Place pen absolute. Plots polylines if pen is down."""
        self.plotter.set_absolute_mode()
        if len(args):
            self.plotter.plot_polyline(to_points(to_floats(args)))

    def cmd_pr(self, args: list[bytes]):
        """Place pen relative.Plots polylines if pen is down."""
        self.plotter.set_relative_mode()
        if len(args):
            self.plotter.plot_polyline(to_points(to_floats(args)))

    # plot commands:
    def cmd_ci(self, args: list[bytes]):
        """Plot full circle."""
        arg_count = len(args)
        if not arg_count:
            self.add_error("invalid arguments for CI command")
            return
        self.plotter.push_pen_state()
        # implicit pen down!
        self.plotter.pen_down()
        radius = to_float(args[0], 1.0)
        chord_angle = 5.0
        if arg_count > 1:
            chord_angle = to_float(args[1], chord_angle)
        self.plotter.plot_abs_circle(radius, chord_angle)
        self.plotter.pop_pen_state()

    def cmd_aa(self, args: list[bytes]):
        """Plot arc absolute."""
        if len(args) < 3:
            self.add_error("invalid arguments for AR command")
            return
        self._arc_out(args, self.plotter.plot_abs_arc)

    def cmd_ar(self, args: list[bytes]):
        """Plot arc relative."""
        if len(args) < 3:
            self.add_error("invalid arguments for AR command")
            return
        self._arc_out(args, self.plotter.plot_rel_arc)

    @staticmethod
    def _arc_out(args: list[bytes], output_method):
        """Plot arc"""
        arg_count = len(args)
        if arg_count < 3:
            return
        x = to_float(args[0])
        y = to_float(args[1])
        sweep_angle = to_float(args[2])
        chord_angle = 5.0
        if arg_count > 3:
            chord_angle = to_float(args[3], chord_angle)
        output_method(Vec2(x, y), sweep_angle, chord_angle)

    def cmd_at(self, args: list[bytes]):
        """Plot arc absolute from three points."""
        if len(args) < 4:
            self.add_error("invalid arguments for AT command")
            return
        self._arc_3p_out(args, self.plotter.plot_abs_arc_three_points)

    def cmd_rt(self, args: list[bytes]):
        """Plot arc relative from three points."""
        if len(args) < 4:
            self.add_error("invalid arguments for RT command")
            return
        self._arc_3p_out(args, self.plotter.plot_rel_arc_three_points)

    @staticmethod
    def _arc_3p_out(args: list[bytes], output_method):
        """Plot arc from three points"""
        arg_count = len(args)
        if arg_count < 4:
            return
        points = to_points(to_floats(args))
        if len(points) < 2:
            return
        chord_angle = 5.0
        if arg_count > 4:
            chord_angle = to_float(args[4], chord_angle)
        try:
            output_method(points[0], points[1], chord_angle)
        except ZeroDivisionError:
            pass

    def cmd_bz(self, args: list[bytes]):
        """Plot cubic Bezier curves with absolute user coordinates."""
        self._bezier_out(args, self.plotter.plot_abs_cubic_bezier)

    def cmd_br(self, args: list[bytes]):
        """Plot cubic Bezier curves with relative user coordinates."""
        self._bezier_out(args, self.plotter.plot_rel_cubic_bezier)

    @staticmethod
    def _bezier_out(args: list[bytes], output_method):
        kind = 0
        ctrl1 = NULLVEC2
        ctrl2 = NULLVEC2
        for point in to_points(to_floats(args)):
            if kind == 0:
                ctrl1 = point
            elif kind == 1:
                ctrl2 = point
            elif kind == 2:
                end = point
                output_method(ctrl1, ctrl2, end)
            kind = (kind + 1) % 3

    def cmd_pe(self, args: list[bytes]):
        """Plot Polyline Encoded."""
        if len(args):
            data = args[0]
        else:
            self.add_error("invalid arguments for PE command")
            return

        plotter = self.plotter
        # The last pen up/down state remains after leaving the PE command.
        pen_down = True
        # Ignores and preserves the current absolute/relative mode of the plotter.
        absolute = False
        frac_bin_bits = 0
        base = 64
        index = 0
        length = len(data)
        point_queue: list[Vec2] = []

        while index < length:
            char = data[index]
            if char in b":<>=7":
                index += 1
                if char == 58:  # ":" - select pen
                    values, index = pe_decode(data, base=base, start=index)
                    plotter.set_current_pen(int(values[0]))
                    if len(values) > 1:
                        point_queue.extend(to_points(values[1:]))
                elif char == 60:  # "<" -  pen up and goto coordinates
                    pen_down = False
                elif char == 62:  # ">" - fractional data
                    values, index = pe_decode(data, base=base, start=index)
                    frac_bin_bits = int(values[0])
                    if len(values) > 1:
                        point_queue.extend(to_points(values[1:]))
                elif char == 61:  # "=" - next coordinates are absolute
                    absolute = True
                elif char == 55:  # "7" - 7-bit mode
                    base = 32
            else:
                values, index = pe_decode(
                    data, frac_bits=frac_bin_bits, base=base, start=index
                )
                point_queue.extend(to_points(values))

            if point_queue:
                plotter.pen_down()
                if absolute:
                    # next point is absolute: make relative
                    point_queue[0] = point_queue[0] - plotter.user_location
                if not pen_down:
                    target = point_queue.pop(0)
                    plotter.move_to_rel(target)
                    if not point_queue:  # last point in queue
                        plotter.pen_up()
                if point_queue:
                    plotter.plot_rel_polyline(point_queue)
                    point_queue.clear()
                pen_down = True
                absolute = False

    # polygon mode:
    def cmd_pm(self, args: list[bytes]) -> None:
        """Enter/Exit polygon mode."""
        status = 0
        if len(args):
            status = to_int(args[0], status)
        if status == 2:
            self.plotter.exit_polygon_mode()
        else:
            self.plotter.enter_polygon_mode(status)

    def cmd_fp(self, args: list[bytes]) -> None:
        """Plot filled polygon."""
        fill_method = 0
        if len(args):
            fill_method = one_of(to_int(args[0], fill_method), (0, 1))
        self.plotter.fill_polygon(fill_method)

    def cmd_ep(self, _) -> None:
        """Plot edged polygon."""
        self.plotter.edge_polygon()


def to_floats(args: Iterable[bytes]) -> Iterator[float]:
    for arg in args:
        try:
            yield float(arg)
        except ValueError:
            pass


def to_ints(args: Iterable[bytes]) -> Iterator[int]:
    for arg in args:
        try:
            yield int(arg)
        except ValueError:
            pass


def to_points(values: Iterable[float]) -> list[Vec2]:
    points: list[Vec2] = []
    append_point = False
    buffer: float = 0.0
    for value in values:
        if append_point:
            points.append(Vec2(buffer, value))
            append_point = False
        else:
            buffer = value
            append_point = True
    return points


def to_float(s: bytes, default=0.0) -> float:
    try:
        return float(s)
    except ValueError:
        return default


def to_int(s: bytes, default=0) -> int:
    try:
        return int(s)
    except ValueError:
        return default


def clamp(v, v_min, v_max):
    return max(min(v_max, v), v_min)


def one_of(value, choice):
    if value in choice:
        return value
    return choice[0]
