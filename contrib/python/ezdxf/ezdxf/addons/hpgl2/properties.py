#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
import dataclasses
import enum
import copy
from ezdxf.colors import RGB


class FillType(enum.IntEnum):
    """Fill type enumeration."""

    NONE = 0
    SOLID = 1
    HATCHING = 2
    CROSS_HATCHING = 3
    SHADING = 4


class FillMethod(enum.IntEnum):
    """Fill method enumeration."""

    EVEN_ODD = 0
    NON_ZERO_WINDING = 1


RGB_NONE = RGB(-1, -1, -1)
RGB_BLACK = RGB(0, 0, 0)
RGB_WHITE = RGB(255, 255, 255)
LIGHT_GREY = RGB(200, 200, 200)


@dataclasses.dataclass
class Pen:
    """Represents a pen table entry."""

    index: int
    width: float  # in mm
    color: RGB


class Properties:
    """Consolidated display properties."""

    DEFAULT_PEN = Pen(1, 0.35, RGB_NONE)

    def __init__(self) -> None:
        # hashed content
        self.pen_index: int = 1
        self.pen_color = RGB_NONE
        self.pen_width: float = 0.35  # in mm
        self.fill_type = FillType.SOLID
        self.fill_method = FillMethod.EVEN_ODD
        self.fill_hatch_line_angle: float = 0.0  # in degrees
        self.fill_hatch_line_spacing: float = 40.0  # in plotter units
        self.fill_shading_density: float = 100.0
        # not hashed content
        self.max_pen_count: int = 2
        self.pen_table: dict[int, Pen] = {}
        self.reset()

    def hash(self) -> int:
        return hash(
            (
                self.pen_index,
                self.pen_color,
                self.pen_width,
                self.fill_type,
                self.fill_method,
                self.fill_hatch_line_angle,
                self.fill_hatch_line_spacing,
                self.fill_shading_density,
            )
        )

    def copy(self) -> Properties:
        # the pen table is shared across all copies of Properties
        return copy.copy(self)

    def setup_default_pen_table(self):
        if len(self.pen_table):
            return
        pens = self.pen_table
        width = self.DEFAULT_PEN.width
        pens[0] = Pen(0, width, RGB(255, 255, 255))  # white
        pens[1] = Pen(1, width, RGB(0, 0, 0))  # black
        pens[2] = Pen(2, width, RGB(255, 0, 0))  # red
        pens[3] = Pen(3, width, RGB(0, 255, 0))  # green
        pens[4] = Pen(4, width, RGB(255, 255, 0))  # yellow
        pens[5] = Pen(5, width, RGB(0, 0, 255))  # blue
        pens[6] = Pen(6, width, RGB(255, 0, 255))  # magenta
        pens[7] = Pen(6, width, RGB(0, 255, 255))  # cyan

    def reset(self) -> None:
        self.max_pen_count = 2
        self.pen_index = self.DEFAULT_PEN.index
        self.pen_color = self.DEFAULT_PEN.color
        self.pen_width = self.DEFAULT_PEN.width
        self.pen_table = {}
        self.fill_type = FillType.SOLID
        self.fill_method = FillMethod.EVEN_ODD
        self.fill_hatch_line_angle = 0.0
        self.fill_hatch_line_spacing = 40.0
        self.fill_shading_density = 1.0
        self.setup_default_pen_table()

    def get_pen(self, index: int) -> Pen:
        return self.pen_table.get(index, self.DEFAULT_PEN)

    def set_max_pen_count(self, count: int) -> None:
        self.max_pen_count = count

    def set_current_pen(self, index: int) -> None:
        self.pen_index = index
        pen = self.get_pen(index)
        self.pen_width = pen.width
        self.pen_color = pen.color

    def set_pen_width(self, index: int, width: float) -> None:
        if index == -1:
            self.pen_width = width
        else:
            pen = self.pen_table.setdefault(
                index, Pen(index, width, self.DEFAULT_PEN.color)
            )
            pen.width = width

    def set_pen_color(self, index: int, rgb: RGB) -> None:
        if index == -1:
            self.pen_color = rgb
        else:
            pen = self.pen_table.setdefault(
                index, Pen(index, self.DEFAULT_PEN.width, rgb)
            )
            pen.color = rgb

    def set_fill_type(self, fill_type: int, spacing: float, angle: float):
        if fill_type == 3:
            self.fill_type = FillType.HATCHING
            self.fill_hatch_line_spacing = spacing
            self.fill_hatch_line_angle = angle
        elif fill_type == 4:
            self.fill_type = FillType.CROSS_HATCHING
            self.fill_hatch_line_spacing = spacing
            self.fill_hatch_line_angle = angle
        elif fill_type == 10:
            self.fill_type = FillType.SHADING
            self.fill_shading_density = spacing
        else:
            self.fill_type = FillType.SOLID

    def set_fill_method(self, fill_method: int) -> None:
        self.fill_method = FillMethod(fill_method)

    def resolve_pen_color(self) -> RGB:
        """Returns the final RGB pen color."""
        rgb = self.pen_color
        if rgb is RGB_NONE:
            pen = self.pen_table.get(self.pen_index, self.DEFAULT_PEN)
            rgb = pen.color
        if rgb is RGB_NONE:
            return RGB_BLACK
        return rgb

    def resolve_fill_color(self) -> RGB:
        """Returns the final RGB fill color."""
        ft = self.fill_type
        if ft == FillType.SOLID:
            return self.resolve_pen_color()
        elif ft == FillType.SHADING:
            grey = min(int(2.55 * (100.0 - self.fill_shading_density)), 255)
            return RGB(grey, grey, grey)
        elif ft == FillType.HATCHING or ft == FillType.CROSS_HATCHING:
            return LIGHT_GREY
        return RGB_WHITE
