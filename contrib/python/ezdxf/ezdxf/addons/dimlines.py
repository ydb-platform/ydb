# Copyright (c) 2010-2022, Manfred Moitzi
# License: MIT License
"""Dimension lines as composite entities build up by DXF primitives.

This add-on exist just for an easy transition from `dxfwrite` to `ezdxf`.

Classes
-------

- LinearDimension
- AngularDimension
- ArcDimension
- RadiusDimension
- DimStyle

This code was written long before I had any understanding of the DIMENSION
entity and therefore, the classes have completely different implementations and
styling features than the dimensions based on the DIMENSION entity .

.. warning::

    Try to not use these classes beside porting `dxfwrite` code to `ezdxf`
    and even for that case the usage of the regular DIMENSION entity is to
    prefer because this module will not get much maintenance and may be removed
    in the future.

"""
from __future__ import annotations
from typing import Any, TYPE_CHECKING, Iterable, Optional
from math import radians, degrees, pi
from abc import abstractmethod
from ezdxf.enums import TextEntityAlignment
from ezdxf.math import Vec3, Vec2, distance, lerp, ConstructionRay, UVec

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.eztypes import GenericLayoutType

DIMENSIONS_MIN_DISTANCE = 0.05
DIMENSIONS_FLOATINGPOINT = "."

ANGLE_DEG = 180.0 / pi
ANGLE_GRAD = 200.0 / pi
ANGLE_RAD = 1.0


class DimStyle(dict):
    """DimStyle parameter struct, a dumb object just to store values
    """

    default_values = [
        # tick block name, use setup to generate default blocks <dimblk> <dimblk1> <dimblk2>
        ("tick", "DIMTICK_ARCH"),
        # scale factor for ticks-block <dimtsz> <dimasz>
        ("tickfactor", 1.0),
        # tick2x means tick is drawn only for one side, insert tick a second
        # time rotated about 180 degree, but only one time at the dimension line
        # ends, this is useful for arrow-like ticks. hint: set dimlineext to 0. <none>
        ("tick2x", False),
        # dimension value scale factor, value = drawing-units * scale <dimlfac>
        ("scale", 100.0),
        # round dimension value to roundval fractional digits <dimdec>
        ("roundval", 0),
        # round dimension value to half units, round 0.4, 0.6 to 0.5 <dimrnd>
        ("roundhalf", False),
        # dimension value text color <dimclrt>
        ("textcolor", 7),
        # dimension value text height <dimtxt>
        ("height", 0.5),
        # dimension text prefix and suffix like 'x=' ... ' cm' <dimpost>
        ("prefix", ""),
        ("suffix", ""),
        # dimension value text style <dimtxsty>
        ("style", "OpenSansCondensed-Light"),
        # default layer for whole dimension object
        ("layer", "DIMENSIONS"),
        # dimension line color index (0 from layer) <dimclrd>
        ("dimlinecolor", 7),
        # dimension line extensions (in dimline direction, left and right) <dimdle>
        ("dimlineext", 0.3),
        # draw dimension value text `textabove` drawing-units above the
        # dimension line <dimgap>
        ("textabove", 0.2),
        # switch extension line False=off, True=on <dimse1> <dimse2>
        ("dimextline", True),
        # dimension extension line color index (0 from layer) <dimclre>
        ("dimextlinecolor", 5),
        # gap between measure target point and end of extension line <dimexo>
        ("dimextlinegap", 0.3),
    ]

    def __init__(self, name: str, **kwargs):
        super().__init__(DimStyle.default_values)
        # dimstyle name
        self["name"] = name
        self.update(kwargs)

    def __getattr__(self, attr: str) -> Any:
        return self[attr]

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value


class DimStyles:
    """
    DimStyle container

    """

    def __init__(self) -> None:
        self._styles: dict[str, DimStyle] = {}
        self.default = DimStyle("Default")

        self.new(
            "angle.deg",
            scale=ANGLE_DEG,
            suffix=str("°"),
            roundval=0,
            tick="DIMTICK_RADIUS",
            tick2x=True,
            dimlineext=0.0,
            dimextline=False,
        )
        self.new(
            "angle.grad",
            scale=ANGLE_GRAD,
            suffix="gon",
            roundval=0,
            tick="DIMTICK_RADIUS",
            tick2x=True,
            dimlineext=0.0,
            dimextline=False,
        )
        self.new(
            "angle.rad",
            scale=ANGLE_RAD,
            suffix="rad",
            roundval=3,
            tick="DIMTICK_RADIUS",
            tick2x=True,
            dimlineext=0.0,
            dimextline=False,
        )

    def get(self, name: str) -> DimStyle:
        """
        Get DimStyle() object by name.
        """
        return self._styles.get(name, self.default)

    def new(self, name: str, **kwargs) -> DimStyle:
        """
        Create a new dimstyle
        """
        style = DimStyle(name, **kwargs)
        self._styles[name] = style
        return style

    @staticmethod
    def setup(drawing: "Drawing"):
        """
        Insert necessary definitions into drawing:

            ticks: DIMTICK_ARCH, DIMTICK_DOT, DIMTICK_ARROW
        """
        # default pen assignment:
        # 1 : 1.40mm - red
        # 2 : 0.35mm - yellow
        # 3 : 0.70mm - green
        # 4 : 0.50mm - cyan
        # 5 : 0.13mm - blue
        # 6 : 1.00mm - magenta
        # 7 : 0.25mm - white/black
        # 8, 9 : 2.00mm
        # >=10 : 1.40mm

        dimcolor = {
            "color": dimstyles.default.dimextlinecolor,
            "layer": "BYBLOCK",
        }
        color4 = {"color": 4, "layer": "BYBLOCK"}
        color7 = {"color": 7, "layer": "BYBLOCK"}

        block = drawing.blocks.new("DIMTICK_ARCH")
        block.add_line(start=(0.0, +0.5), end=(0.0, -0.5), dxfattribs=dimcolor)
        block.add_line(start=(-0.2, -0.2), end=(0.2, +0.2), dxfattribs=color4)

        block = drawing.blocks.new("DIMTICK_DOT")
        block.add_line(start=(0.0, 0.5), end=(0.0, -0.5), dxfattribs=dimcolor)
        block.add_circle(center=(0, 0), radius=0.1, dxfattribs=color4)

        block = drawing.blocks.new("DIMTICK_ARROW")

        block.add_line(start=(0.0, 0.5), end=(0.0, -0.50), dxfattribs=dimcolor)
        block.add_solid([(0, 0), (0.3, 0.05), (0.3, -0.05)], dxfattribs=color7)

        block = drawing.blocks.new("DIMTICK_RADIUS")
        block.add_solid(
            [(0, 0), (0.3, 0.05), (0.25, 0.0), (0.3, -0.05)], dxfattribs=color7
        )


dimstyles = DimStyles()  # use this factory to create new dimstyles


class _DimensionBase:
    """
    Abstract base class for dimension lines.

    """

    def __init__(self, dimstyle: str, layer: str, roundval: int):
        self.dimstyle = dimstyles.get(dimstyle)
        self.layer = layer
        self.roundval = roundval

    def prop(self, property_name: str) -> Any:
        """
        Get dimension line properties by `property_name` with the possibility to override several properties.
        """
        if property_name == "layer":
            return self.layer if self.layer is not None else self.dimstyle.layer
        elif property_name == "roundval":
            return (
                self.roundval
                if self.roundval is not None
                else self.dimstyle.roundval
            )
        else:  # pass through self.dimstyle object DimStyle()
            return self.dimstyle[property_name]

    def format_dimtext(self, dimvalue: float) -> str:
        """
        Format the dimension text.
        """
        dimtextfmt = "%." + str(self.prop("roundval")) + "f"
        dimtext = dimtextfmt % dimvalue
        if DIMENSIONS_FLOATINGPOINT in dimtext:
            # remove successional zeros
            dimtext.rstrip("0")
            # remove floating point as last char
            dimtext.rstrip(DIMENSIONS_FLOATINGPOINT)
        return self.prop("prefix") + dimtext + self.prop("suffix")

    @abstractmethod
    def render(self, layout: "GenericLayoutType"):
        pass


class LinearDimension(_DimensionBase):
    """
    Simple straight dimension line with two or more measure points, build with basic DXF entities. This is NOT a dxf
    dimension entity. And This is a 2D element, so all z-values will be ignored!

    """

    def __init__(
        self,
        pos: UVec,
        measure_points: Iterable[UVec],
        angle: float = 0.0,
        dimstyle: str = "Default",
        layer: Optional[str] = None,
        roundval: Optional[int] = None,
    ):
        """
        LinearDimension Constructor.

        Args:
            pos: location as (x, y) tuple of dimension line, line goes through this point
            measure_points: list of points as (x, y) tuples to dimension (two or more)
            angle: angle (in degree) of dimension line
            dimstyle: dimstyle name, 'Default' - style is the default value
            layer: dimension line layer, override the default value of dimstyle
            roundval: count of decimal places

        """
        super().__init__(dimstyle, layer, roundval)  # type: ignore
        self.angle = angle
        self.measure_points = list(measure_points)
        self.text_override = [""] * self.section_count
        self.dimlinepos = Vec3(pos)
        self.layout = None

    def set_text(self, section: int, text: str) -> None:
        """
        Set and override the text of the dimension text for the given dimension line section.
        """
        self.text_override[section] = text

    def _setup(self) -> None:
        """
        Calc setup values and determines the point order of the dimension line points.
        """
        self.measure_points = [Vec3(point) for point in self.measure_points]
        dimlineray = ConstructionRay(self.dimlinepos, angle=radians(self.angle))
        self.dimline_points = [
            self._get_point_on_dimline(point, dimlineray)
            for point in self.measure_points
        ]
        self.point_order = self._indices_of_sorted_points(self.dimline_points)
        self._build_vectors()

    def _get_dimline_point(self, index: int) -> UVec:
        """
        Get point on the dimension line, index runs left to right.
        """
        return self.dimline_points[self.point_order[index]]

    def _get_section_points(self, section: int) -> tuple[Vec3, Vec3]:
        """
        Get start and end point on the dimension line of dimension section.
        """
        return self._get_dimline_point(section), self._get_dimline_point(
            section + 1
        )

    def _get_dimline_bounds(self) -> tuple[Vec3, Vec3]:
        """
        Get the first and the last point of dimension line.
        """
        return self._get_dimline_point(0), self._get_dimline_point(-1)

    @property
    def section_count(self) -> int:
        """count of dimline sections"""
        return len(self.measure_points) - 1

    @property
    def point_count(self) -> int:
        """count of dimline points"""
        return len(self.measure_points)

    def render(self, layout: "GenericLayoutType") -> None:
        """build dimension line object with basic dxf entities"""
        self._setup()
        self._draw_dimline(layout)
        if self.prop("dimextline"):
            self._draw_extension_lines(layout)
        self._draw_text(layout)
        self._draw_ticks(layout)

    @staticmethod
    def _indices_of_sorted_points(points: Iterable[UVec]) -> list[int]:
        """get indices of points, for points sorted by x, y values"""
        indexed_points = [(point, idx) for idx, point in enumerate(points)]
        indexed_points.sort()
        return [idx for point, idx in indexed_points]

    def _build_vectors(self) -> None:
        """build unit vectors, parallel and normal to dimension line"""
        point1, point2 = self._get_dimline_bounds()
        self.parallel_vector = (Vec3(point2) - Vec3(point1)).normalize()
        self.normal_vector = self.parallel_vector.orthogonal()

    @staticmethod
    def _get_point_on_dimline(point: UVec, dimray: ConstructionRay) -> Vec3:
        """get the measure target point projection on the dimension line"""
        return dimray.intersect(dimray.orthogonal(point))

    def _draw_dimline(self, layout: "GenericLayoutType") -> None:
        """build dimension line entity"""
        start_point, end_point = self._get_dimline_bounds()

        dimlineext = self.prop("dimlineext")
        if dimlineext > 0:
            start_point = start_point - (self.parallel_vector * dimlineext)
            end_point = end_point + (self.parallel_vector * dimlineext)

        attribs = {
            "color": self.prop("dimlinecolor"),
            "layer": self.prop("layer"),
        }
        layout.add_line(
            start=start_point,
            end=end_point,
            dxfattribs=attribs,
        )

    def _draw_extension_lines(self, layout: "GenericLayoutType") -> None:
        """build the extension lines entities"""
        dimextlinegap = self.prop("dimextlinegap")
        attribs = {
            "color": self.prop("dimlinecolor"),
            "layer": self.prop("layer"),
        }

        for dimline_point, target_point in zip(
            self.dimline_points, self.measure_points
        ):
            if distance(dimline_point, target_point) > max(
                dimextlinegap, DIMENSIONS_MIN_DISTANCE
            ):
                direction_vector = (target_point - dimline_point).normalize()
                target_point = target_point - (direction_vector * dimextlinegap)
                layout.add_line(
                    start=dimline_point,
                    end=target_point,
                    dxfattribs=attribs,
                )

    def _draw_text(self, layout: "GenericLayoutType") -> None:
        """build the dimension value text entity"""
        attribs = {
            "height": self.prop("height"),
            "color": self.prop("textcolor"),
            "layer": self.prop("layer"),
            "rotation": self.angle,
            "style": self.prop("style"),
        }
        for section in range(self.section_count):
            dimvalue_text = self._get_dimvalue_text(section)
            insert_point = self._get_text_insert_point(section)
            layout.add_text(
                text=dimvalue_text,
                dxfattribs=attribs,
            ).set_placement(
                insert_point, align=TextEntityAlignment.MIDDLE_CENTER
            )

    def _get_dimvalue_text(self, section: int) -> str:
        """get the dimension value as text, distance from point1 to point2"""
        override = self.text_override[section]
        if len(override):
            return override
        point1, point2 = self._get_section_points(section)

        dimvalue = distance(point1, point2) * self.prop("scale")
        return self.format_dimtext(dimvalue)

    def _get_text_insert_point(self, section: int) -> Vec3:
        """get the dimension value text insert point"""
        point1, point2 = self._get_section_points(section)
        dist = self.prop("height") / 2.0 + self.prop("textabove")
        return lerp(point1, point2) + (self.normal_vector * dist)

    def _draw_ticks(self, layout: "GenericLayoutType") -> None:
        """insert the dimension line ticks, (markers on the dimension line)"""
        attribs = {
            "xscale": self.prop("tickfactor"),
            "yscale": self.prop("tickfactor"),
            "layer": self.prop("layer"),
        }

        def add_tick(index: int, rotate: bool = False) -> None:
            """build the insert-entity for the tick block"""
            attribs["rotation"] = self.angle + (180.0 if rotate else 0.0)
            layout.add_blockref(
                insert=self._get_dimline_point(index),
                name=self.prop("tick"),
                dxfattribs=attribs,
            )

        if self.prop("tick2x"):
            for index in range(0, self.point_count - 1):
                add_tick(index, False)
            for index in range(1, self.point_count):
                add_tick(index, True)
        else:
            for index in range(self.point_count):
                add_tick(index, False)


class AngularDimension(_DimensionBase):
    """
    Draw an angle dimensioning line at dimline pos from start to end, dimension text is the angle build of the three
    points start-center-end.

    """

    DEG = ANGLE_DEG
    GRAD = ANGLE_GRAD
    RAD = ANGLE_RAD

    def __init__(
        self,
        pos: UVec,
        center: UVec,
        start: UVec,
        end: UVec,
        dimstyle: str = "angle.deg",
        layer: Optional[str] = None,
        roundval: Optional[int] = None,
    ):
        """
        AngularDimension constructor.

        Args:
            pos: location as (x, y) tuple of dimension line, line goes through this point
            center: center point as (x, y) tuple of angle
            start: line from center to start is the first side of the angle
            end: line from center to end is the second side of the angle
            dimstyle: dimstyle name, 'Default' - style is the default value
            layer: dimension line layer, override the default value of dimstyle
            roundval: count of decimal places

        """
        super().__init__(dimstyle, layer, roundval)  # type: ignore
        self.dimlinepos = Vec3(pos)
        self.center = Vec3(center)
        self.start = Vec3(start)
        self.end = Vec3(end)

    def _setup(self) -> None:
        """setup calculation values"""
        self.pos_radius = distance(self.center, self.dimlinepos)  # type: float
        self.radius = distance(self.center, self.start)  # type: float
        self.start_vector = (self.start - self.center).normalize()  # type: Vec3
        self.end_vector = (self.end - self.center).normalize()  # type: Vec3
        self.start_angle = self.start_vector.angle  # type: float
        self.end_angle = self.end_vector.angle  # type: float

    def render(self, layout: "GenericLayoutType") -> None:
        """build dimension line object with basic dxf entities"""

        self._setup()
        self._draw_dimension_line(layout)
        if self.prop("dimextline"):
            self._draw_extension_lines(layout)
        self._draw_dimension_text(layout)
        self._draw_ticks(layout)

    def _draw_dimension_line(self, layout: "GenericLayoutType") -> None:
        """draw the dimension line from start- to endangle."""
        layout.add_arc(
            radius=self.pos_radius,
            center=self.center,
            start_angle=degrees(self.start_angle),
            end_angle=degrees(self.end_angle),
            dxfattribs={
                "layer": self.prop("layer"),
                "color": self.prop("dimlinecolor"),
            },
        )

    def _draw_extension_lines(self, layout: "GenericLayoutType") -> None:
        """build the extension lines entities"""
        for vector in [self.start_vector, self.end_vector]:
            layout.add_line(
                start=self._get_extline_start(vector),
                end=self._get_extline_end(vector),
                dxfattribs={
                    "layer": self.prop("layer"),
                    "color": self.prop("dimextlinecolor"),
                },
            )

    def _get_extline_start(self, vector: Vec3) -> Vec3:
        return self.center + (vector * self.prop("dimextlinegap"))

    def _get_extline_end(self, vector: Vec3) -> Vec3:
        return self.center + (vector * self.pos_radius)

    def _draw_dimension_text(self, layout: "GenericLayoutType") -> None:
        attribs = {
            "height": self.prop("height"),
            "rotation": degrees(
                (self.start_angle + self.end_angle) / 2 - pi / 2.0
            ),
            "layer": self.prop("layer"),
            "style": self.prop("style"),
            "color": self.prop("textcolor"),
        }
        layout.add_text(
            text=self._get_dimtext(),
            dxfattribs=attribs,
        ).set_placement(
            self._get_text_insert_point(),
            align=TextEntityAlignment.MIDDLE_CENTER,
        )

    def _get_text_insert_point(self) -> Vec3:
        midvector = ((self.start_vector + self.end_vector) / 2.0).normalize()
        length = (
            self.pos_radius + self.prop("textabove") + self.prop("height") / 2.0
        )
        return self.center + (midvector * length)

    def _draw_ticks(self, layout: "GenericLayoutType") -> None:
        attribs = {
            "xscale": self.prop("tickfactor"),
            "yscale": self.prop("tickfactor"),
            "layer": self.prop("layer"),
        }
        for vector, mirror in [
            (self.start_vector, False),
            (self.end_vector, self.prop("tick2x")),
        ]:
            insert_point = self.center + (vector * self.pos_radius)
            rotation = vector.angle + pi / 2.0
            attribs["rotation"] = degrees(rotation + (pi if mirror else 0.0))
            layout.add_blockref(
                insert=insert_point,
                name=self.prop("tick"),
                dxfattribs=attribs,
            )

    def _get_dimtext(self) -> str:
        # set scale = ANGLE_DEG for degrees (circle = 360 deg)
        # set scale = ANGLE_GRAD for grad(circle = 400 grad)
        # set scale = ANGLE_RAD for rad(circle = 2*pi)
        angle = (self.end_angle - self.start_angle) * self.prop("scale")
        return self.format_dimtext(angle)


class ArcDimension(AngularDimension):
    """
    Arc is defined by start- and endpoint on arc and the center point, or by three points lying on the arc if acr3points
    is True. Measured length goes from start- to endpoint. The dimension line goes through the dimlinepos.

    """

    def __init__(
        self,
        pos: UVec,
        center: UVec,
        start: UVec,
        end: UVec,
        arc3points: bool = False,
        dimstyle: str = "Default",
        layer: Optional[str] = None,
        roundval: Optional[int] = None,
    ):
        """
        Args:
            pos: location as (x, y) tuple of dimension line, line goes through this point
            center: center point of arc
            start: start point of arc
            end: end point of arc
            arc3points: if **True** arc is defined by three points on the arc (center, start, end)
            dimstyle: dimstyle name, 'Default' - style is the default value
            layer: dimension line layer, override the default value of dimstyle
            roundval: count of decimal places

        """
        super().__init__(pos, center, start, end, dimstyle, layer, roundval)
        self.arc3points = arc3points

    def _setup(self) -> None:
        super()._setup()
        if self.arc3points:
            self.center = center_of_3points_arc(
                self.center, self.start, self.end
            )

    def _get_extline_start(self, vector: Vec3) -> Vec3:
        return self.center + (
            vector * (self.radius + self.prop("dimextlinegap"))
        )

    def _get_extline_end(self, vector: Vec3) -> Vec3:
        return self.center + (vector * self.pos_radius)

    def _get_dimtext(self) -> str:
        arc_length = (
            (self.end_angle - self.start_angle)
            * self.radius
            * self.prop("scale")
        )
        return self.format_dimtext(arc_length)


class RadialDimension(_DimensionBase):
    """
    Draw a radius dimension line from `target` in direction of `center` with length drawing units. RadiusDimension has
    a special tick!!
    """

    def __init__(
        self,
        center: UVec,
        target: UVec,
        length: float = 1.0,
        dimstyle: str = "Default",
        layer: Optional[str] = None,
        roundval: Optional[int] = None,
    ):
        """
        Args:
            center: center point of radius
            target: target point of radius
            length: length of radius arrow (drawing length)
            dimstyle: dimstyle name, 'Default' - style is the default value
            layer: dimension line layer, override the default value of dimstyle
            roundval: count of decimal places

        """
        super().__init__(dimstyle, layer, roundval)  # type: ignore
        self.center = Vec3(center)
        self.target = Vec3(target)
        self.length = float(length)

    def _setup(self) -> None:
        self.target_vector = (
            self.target - self.center
        ).normalize()  # type: Vec3
        self.radius = distance(self.center, self.target)  # type: float

    def render(self, layout: "GenericLayoutType") -> None:
        """build dimension line object with basic dxf entities"""
        self._setup()
        self._draw_dimension_line(layout)
        self._draw_dimension_text(layout)
        self._draw_ticks(layout)

    def _draw_dimension_line(self, layout: "GenericLayoutType") -> None:
        start_point = self.center + (
            self.target_vector * (self.radius - self.length)
        )
        layout.add_line(
            start=start_point,
            end=self.target,
            dxfattribs={
                "color": self.prop("dimlinecolor"),
                "layer": self.prop("layer"),
            },
        )

    def _draw_dimension_text(self, layout: "GenericLayoutType") -> None:
        layout.add_text(
            text=self._get_dimtext(),
            dxfattribs={
                "height": self.prop("height"),
                "rotation": self.target_vector.angle_deg,
                "layer": self.prop("layer"),
                "style": self.prop("style"),
                "color": self.prop("textcolor"),
            },
        ).set_placement(
            self._get_insert_point(), align=TextEntityAlignment.MIDDLE_RIGHT
        )

    def _get_insert_point(self) -> Vec3:
        return self.target - (
            self.target_vector * (self.length + self.prop("textabove"))
        )

    def _get_dimtext(self) -> str:
        return self.format_dimtext(self.radius * self.prop("scale"))

    def _draw_ticks(self, layout: "GenericLayoutType") -> None:
        layout.add_blockref(
            insert=self.target,
            name="DIMTICK_RADIUS",
            dxfattribs={
                "rotation": self.target_vector.angle_deg + 180,
                "xscale": self.prop("tickfactor"),
                "yscale": self.prop("tickfactor"),
                "layer": self.prop("layer"),
            },
        )


def center_of_3points_arc(point1: UVec, point2: UVec, point3: UVec) -> Vec2:
    """
    Calc center point of 3 point arc. ConstructionCircle is defined by 3 points
    on the circle: point1, point2 and point3.
    """
    ray1 = ConstructionRay(point1, point2)
    ray2 = ConstructionRay(point1, point3)
    midpoint1 = lerp(point1, point2)
    midpoint2 = lerp(point1, point3)
    center_ray1 = ray1.orthogonal(midpoint1)
    center_ray2 = ray2.orthogonal(midpoint2)
    return center_ray1.intersect(center_ray2)
