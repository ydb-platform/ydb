#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence, no_type_check

import copy
from xml.etree import ElementTree as ET
import numpy as np

from ezdxf.math import Vec2, BoundingBox2d, Matrix44
from ezdxf.path import Command


from .type_hints import Color
from .backend import BackendInterface, BkPath2d, BkPoints2d, ImageData
from .config import Configuration, LineweightPolicy
from .properties import BackendProperties
from . import layout, recorder

__all__ = ["SVGBackend"]


class SVGBackend(recorder.Recorder):
    """This is a native SVG rendering backend and does not require any external packages
    to render SVG images other than the core dependencies.  This backend support content
    cropping at page margins.
    """

    def __init__(self) -> None:
        super().__init__()
        self._init_flip_y = True
        self.transformation_matrix: Matrix44 | None = None

    def get_xml_root_element(
        self,
        page: layout.Page,
        *,
        settings: layout.Settings = layout.Settings(),
        render_box: BoundingBox2d | None = None,
    ) -> ET.Element:
        top_origin = True
        settings = copy.copy(settings)

        # This player changes the original recordings!
        player = self.player()
        if render_box is None:
            render_box = player.bbox()

        # the page origin (0, 0) is in the top-left corner.
        output_layout = layout.Layout(render_box, flip_y=self._init_flip_y)
        page = output_layout.get_final_page(page, settings)
        if page.width == 0 or page.height == 0:
            return ET.Element("svg")  # empty page

        self.transformation_matrix = output_layout.get_placement_matrix(
            page, settings=settings, top_origin=top_origin
        )
        # transform content to the output coordinates space:
        player.transform(self.transformation_matrix)
        if settings.crop_at_margins:
            p1, p2 = page.get_margin_rect(top_origin=top_origin)  # in mm
            # scale factor to map page coordinates to output space coordinates:
            output_scale = settings.page_output_scale_factor(page)
            max_sagitta = 0.1 * output_scale  # curve approximation 0.1 mm
            # crop content inplace by the margin rect:
            player.crop_rect(p1 * output_scale, p2 * output_scale, max_sagitta)

        self._init_flip_y = False
        backend = self.make_backend(page, settings)
        player.replay(backend)
        return backend.get_xml_root_element()

    def get_string(
        self,
        page: layout.Page,
        *,
        settings: layout.Settings = layout.Settings(),
        render_box: BoundingBox2d | None = None,
        xml_declaration=True,
    ) -> str:
        """Returns the XML data as unicode string.

        Args:
            page: page definition, see :class:`~ezdxf.addons.drawing.layout.Page`
            settings: layout settings, see :class:`~ezdxf.addons.drawing.layout.Settings`
            render_box: set explicit region to render, default is content bounding box
            xml_declaration: inserts the "<?xml version='1.0' encoding='utf-8'?>" string
                in front of the <svg> element

        """
        xml = self.get_xml_root_element(page, settings=settings, render_box=render_box)
        return ET.tostring(xml, encoding="unicode", xml_declaration=xml_declaration)

    @staticmethod
    def make_backend(page: layout.Page, settings: layout.Settings) -> SVGRenderBackend:
        """Override this method to use a customized render backend."""
        return SVGRenderBackend(page, settings)


def make_view_box(page: layout.Page, output_coordinate_space: float) -> tuple[int, int]:
    size = round(output_coordinate_space)
    if page.width > page.height:
        return size, round(size * (page.height / page.width))
    return round(size * (page.width / page.height)), size


def scale_page_to_view_box(page: layout.Page, output_coordinate_space: float) -> float:
    # The viewBox coordinates are integer values in the range of [0, output_coordinate_space]
    return min(
        output_coordinate_space / page.width,
        output_coordinate_space / page.height,
    )


class Styles:
    def __init__(self, xml: ET.Element) -> None:
        self._xml = xml
        self._class_names: dict[int, str] = dict()
        self._counter = 1

    def get_class(
        self,
        *,
        stroke: Color = "none",
        stroke_width: int | str = "none",
        stroke_opacity: float = 1.0,
        fill: Color = "none",
        fill_opacity: float = 1.0,
    ) -> str:
        style = (
            f"{{stroke: {stroke}; "
            f"stroke-width: {stroke_width}; "
            f"stroke-opacity: {stroke_opacity:.3f}; "
            f"fill: {fill}; "
            f"fill-opacity: {fill_opacity:.3f};}}"
        )
        key = hash(style)
        try:
            return self._class_names[key]
        except KeyError:
            pass
        name = f"C{self._counter:X}"
        self._counter += 1
        self._add_class(name, style)
        self._class_names[key] = name
        return name

    def _add_class(self, name, style_str: str) -> None:
        style = ET.Element("style")
        style.text = f".{name} {style_str}"
        self._xml.append(style)


CMD_M_ABS = "M {0.x:.0f} {0.y:.0f}"
CMD_M_REL = "m {0.x:.0f} {0.y:.0f}"
CMD_L_ABS = "L {0.x:.0f} {0.y:.0f}"
CMD_L_REL = "l {0.x:.0f} {0.y:.0f}"
CMD_C3_ABS = "Q {0.x:.0f} {0.y:.0f} {1.x:.0f} {1.y:.0f}"
CMD_C3_REL = "q {0.x:.0f} {0.y:.0f} {1.x:.0f} {1.y:.0f}"
CMD_C4_ABS = "C {0.x:.0f} {0.y:.0f} {1.x:.0f} {1.y:.0f} {2.x:.0f} {2.y:.0f}"
CMD_C4_REL = "c {0.x:.0f} {0.y:.0f} {1.x:.0f} {1.y:.0f} {2.x:.0f} {2.y:.0f}"
CMD_CONT = "{0.x:.0f} {0.y:.0f}"


class SVGRenderBackend(BackendInterface):
    """Creates the SVG output.

    This backend requires some preliminary work, record the frontend output via the
    Recorder backend to accomplish the following requirements:

    - Scale the content in y-axis by -1 to invert the y-axis (SVG).
    - Move content in the first quadrant of the coordinate system.
    - The viewBox is defined by the lower left corner in the origin (0, 0) and
      the upper right corner at (view_box_width, view_box_height)
    - The output coordinates are integer values, scale the content appropriately.
    - Replay the recorded output on this backend.

    """

    def __init__(self, page: layout.Page, settings: layout.Settings) -> None:
        self.settings = settings
        self._stroke_width_cache: dict[float, int] = dict()
        view_box_width, view_box_height = make_view_box(
            page, settings.output_coordinate_space
        )
        # StrokeWidthPolicy.absolute:
        # stroke-width in mm as resolved by the frontend
        self.stroke_width_scale: float = view_box_width / page.width_in_mm
        self.min_lineweight = 0.05  # in mm, set by configure()
        self.lineweight_scaling = 1.0  # set by configure()
        self.lineweight_policy = LineweightPolicy.ABSOLUTE  # set by configure()
        # fixed lineweight for all strokes in ABSOLUTE mode:
        # set Configuration.min_lineweight to the desired lineweight in 1/300 inch!
        # set Configuration.lineweight_scaling to 0

        # LineweightPolicy.RELATIVE:
        # max_stroke_width is determined as a certain percentage of settings.output_coordinate_space
        self.max_stroke_width: int = int(
            settings.output_coordinate_space * settings.max_stroke_width
        )
        # min_stroke_width is determined as a certain percentage of max_stroke_width
        self.min_stroke_width: int = int(
            self.max_stroke_width * settings.min_stroke_width
        )
        # LineweightPolicy.RELATIVE_FIXED:
        # all strokes have a fixed stroke-width as a certain percentage of max_stroke_width
        self.fixed_stroke_width: int = int(
            self.max_stroke_width * settings.fixed_stroke_width
        )
        self.root = ET.Element(
            "svg",
            xmlns="http://www.w3.org/2000/svg",
            width=f"{page.width_in_mm:g}mm",
            height=f"{page.height_in_mm:g}mm",
            viewBox=f"0 0 {view_box_width} {view_box_height}",
        )
        self.styles = Styles(ET.SubElement(self.root, "defs"))
        self.background = ET.SubElement(
            self.root,
            "rect",
            fill="white",
            x="0",
            y="0",
            width=str(view_box_width),
            height=str(view_box_height),
        )
        self.entities = ET.SubElement(self.root, "g")
        self.entities.set("stroke-linecap", "round")
        self.entities.set("stroke-linejoin", "round")
        self.entities.set("fill-rule", "evenodd")

    def get_xml_root_element(self) -> ET.Element:
        return self.root

    def add_strokes(self, d: str, properties: BackendProperties):
        if not d:
            return
        element = ET.SubElement(self.entities, "path", d=d)
        stroke_width = self.resolve_stroke_width(properties.lineweight)
        stroke_color, stroke_opacity = self.resolve_color(properties.color)
        cls = self.styles.get_class(
            stroke=stroke_color,
            stroke_width=stroke_width,
            stroke_opacity=stroke_opacity,
        )
        element.set("class", cls)

    def add_filling(self, d: str, properties: BackendProperties):
        if not d:
            return
        element = ET.SubElement(self.entities, "path", d=d)
        fill_color, fill_opacity = self.resolve_color(properties.color)
        cls = self.styles.get_class(fill=fill_color, fill_opacity=fill_opacity)
        element.set("class", cls)

    def resolve_color(self, color: Color) -> tuple[Color, float]:
        return color[:7], alpha_to_opacity(color[7:9])

    def resolve_stroke_width(self, width: float) -> int:
        try:
            return self._stroke_width_cache[width]
        except KeyError:
            pass
            stroke_width = self.fixed_stroke_width
        policy = self.lineweight_policy
        if policy == LineweightPolicy.ABSOLUTE:
            if self.lineweight_scaling:
                width = max(self.min_lineweight, width) * self.lineweight_scaling
            else:
                width = self.min_lineweight
            stroke_width = round(width * self.stroke_width_scale)
        elif policy == LineweightPolicy.RELATIVE:
            stroke_width = map_lineweight_to_stroke_width(
                width, self.min_stroke_width, self.max_stroke_width
            )
        self._stroke_width_cache[width] = stroke_width
        return stroke_width

    def set_background(self, color: Color) -> None:
        color_str = color[:7]
        opacity = alpha_to_opacity(color[7:9])
        self.background.set("fill", color_str)
        self.background.set("fill-opacity", str(opacity))

    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        self.add_strokes(self.make_polyline_str([pos, pos]), properties)

    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        self.add_strokes(self.make_polyline_str([start, end]), properties)

    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        lines = list(lines)
        if len(lines) == 0:
            return
        self.add_strokes(self.make_multi_line_str(lines), properties)

    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        self.add_strokes(self.make_path_str(path), properties)

    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        d = []
        for path in paths:
            if len(path):
                d.append(self.make_path_str(path, close=True))
        self.add_filling(" ".join(d), properties)

    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        self.add_filling(
            self.make_polyline_str(points.vertices(), close=True), properties
        )

    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        pass  # TODO: not implemented

    @staticmethod
    def make_polyline_str(points: Sequence[Vec2], close=False) -> str:
        if len(points) < 2:
            return ""
        current = points[0]
        # first move is absolute, consecutive lines are relative:
        d: list[str] = [CMD_M_ABS.format(current), "l"]
        for point in points[1:]:
            relative = point - current
            current = point
            d.append(CMD_CONT.format(relative))
        if close:
            d.append("Z")
        return " ".join(d)

    @staticmethod
    def make_multi_line_str(lines: Sequence[tuple[Vec2, Vec2]]) -> str:
        assert len(lines) > 0
        start, end = lines[0]
        d: list[str] = [CMD_M_ABS.format(start), CMD_L_REL.format(end - start)]
        current = end
        for start, end in lines[1:]:
            d.append(CMD_M_REL.format(start - current))
            current = start
            d.append(CMD_L_REL.format(end - current))
            current = end
        return " ".join(d)

    @staticmethod
    @no_type_check
    def make_path_str(path: BkPath2d, close=False) -> str:
        d: list[str] = [CMD_M_ABS.format(path.start)]
        if len(path) == 0:
            return ""

        current = path.start
        for cmd in path.commands():
            end = cmd.end
            if cmd.type == Command.MOVE_TO:
                d.append(CMD_M_REL.format(end - current))
            elif cmd.type == Command.LINE_TO:
                d.append(CMD_L_REL.format(end - current))
            elif cmd.type == Command.CURVE3_TO:
                d.append(CMD_C3_REL.format(cmd.ctrl - current, end - current))
            elif cmd.type == Command.CURVE4_TO:
                d.append(
                    CMD_C4_REL.format(
                        cmd.ctrl1 - current, cmd.ctrl2 - current, end - current
                    )
                )
            current = end
        if close:
            d.append("Z")

        return " ".join(d)

    def configure(self, config: Configuration) -> None:
        self.lineweight_policy = config.lineweight_policy
        if config.min_lineweight:
            # config.min_lineweight in 1/300 inch!
            min_lineweight_mm = config.min_lineweight * 25.4 / 300
            self.min_lineweight = max(0.05, min_lineweight_mm)
        self.lineweight_scaling = config.lineweight_scaling

    def clear(self) -> None:
        pass

    def finalize(self) -> None:
        pass

    def enter_entity(self, entity, properties) -> None:
        pass

    def exit_entity(self, entity) -> None:
        pass


def alpha_to_opacity(alpha: str) -> float:
    # stroke-opacity: 0.0 = transparent; 1.0 = opaque
    # alpha: "00" = transparent; "ff" = opaque
    if len(alpha):
        try:
            return int(alpha, 16) / 255
        except ValueError:
            pass
    return 1.0


def map_lineweight_to_stroke_width(
    lineweight: float,
    min_stroke_width: int,
    max_stroke_width: int,
    min_lineweight=0.05,  # defined by DXF
    max_lineweight=2.11,  # defined by DXF
) -> int:
    """Map the DXF lineweight in mm to stroke-width in viewBox coordinates."""
    lineweight = max(min(lineweight, max_lineweight), min_lineweight) - min_lineweight
    factor = (max_stroke_width - min_stroke_width) / (max_lineweight - min_lineweight)
    return min_stroke_width + round(lineweight * factor)
