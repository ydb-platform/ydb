#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, TYPE_CHECKING, no_type_check
from functools import lru_cache
import enum
import numpy as np

from ezdxf import colors
from ezdxf.lldxf.const import VALID_DXF_LINEWEIGHTS
from ezdxf.math import Vec2, BoundingBox2d, Matrix44
from ezdxf.path import to_splines_and_polylines, to_hatches
from ezdxf.layouts import BaseLayout

from .type_hints import Color
from .backend import BackendInterface, BkPath2d, BkPoints2d, ImageData
from .config import Configuration
from .properties import BackendProperties

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import Solid


class ColorMode(enum.Enum):
    """This enum is used to define the color output mode of the :class:`DXFBackend`.

    Attributes:
        ACI: the color is set as :ref:`ACI` and assigned by layer
        RGB: the color is set as RGB true color value

    """

    # Use color index as primary color
    ACI = enum.auto()

    # Use always the RGB value
    RGB = enum.auto()


DARK_COLOR_THRESHOLD = 0.2
RGB_BLACK = colors.RGB(0, 0, 0)
BYLAYER = 256


class DXFBackend(BackendInterface):
    """The :class:`DXFBackend` creates simple DXF files of POINT, LINE, LWPOLYLINE and
    HATCH entities. This backend does ot need any additional packages.

    Args:
        layout: a DXF :class:`~ezdxf.layouts.BaseLayout`
        color_mode: see :class:`ColorMode`

    """

    def __init__(
        self, layout: BaseLayout, color_mode: ColorMode = ColorMode.RGB
    ) -> None:
        assert layout.doc is not None, "valid DXF document required"
        super().__init__()
        self.layout = layout
        self.doc = layout.doc
        self.color_mode = color_mode
        self.bg_color = RGB_BLACK
        self.is_dark_bg = True
        self._layers: dict[int, str] = dict()
        self._dxfattribs: dict[int, dict] = dict()

    def set_background(self, color: Color) -> None:
        self.bg_color = colors.RGB.from_hex(color)
        self.is_dark_bg = self.bg_color.luminance < DARK_COLOR_THRESHOLD

    def get_layer_name(self, pen: int) -> str:
        try:
            return self._layers[pen]
        except KeyError:
            pass

        layer_name = f"PEN_{pen:03d}"
        self._layers[pen] = layer_name
        if not self.doc.layers.has_entry(layer_name):
            self.doc.layers.add(layer_name, color=pen)
        return layer_name

    def resolve_properties(self, properties: BackendProperties) -> dict:
        key = hash(properties)
        try:
            return self._dxfattribs[key]
        except KeyError:
            pass

        rgb = properties.rgb
        pen = properties.pen
        if pen < 1 or pen > 255:
            pen = 7
        aci = pen
        if self.color_mode == ColorMode.ACI:
            aci = BYLAYER
        attribs = {
            "color": aci,
            "layer": self.get_layer_name(pen),
            "lineweight": make_lineweight(properties.lineweight),
        }
        if self.color_mode == ColorMode.RGB:
            attribs["true_color"] = colors.rgb2int(rgb)

        alpha = properties.color[7:9]
        if alpha:
            try:
                f = int(alpha, 16) / 255
            except ValueError:
                pass
            else:
                attribs["transparency"] = colors.float2transparency(f)
        self._dxfattribs[key] = attribs
        return attribs

    def set_solid_fill(self, hatch, properties: BackendProperties) -> None:
        rgb: colors.RGB | None = None
        aci = BYLAYER
        if self.color_mode == ColorMode.RGB:
            rgb = properties.rgb
            aci = properties.pen
        hatch.set_solid_fill(color=aci, style=0, rgb=rgb)

    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        self.layout.add_point(pos, dxfattribs=self.resolve_properties(properties))

    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        self.layout.add_line(start, end, dxfattribs=self.resolve_properties(properties))

    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        lines = list(lines)
        if len(lines) == 0:
            return
        attribs = self.resolve_properties(properties)
        for start, end in lines:
            self.layout.add_line(start, end, dxfattribs=attribs)

    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        attribs = self.resolve_properties(properties)
        if path.has_curves:
            for entity in to_splines_and_polylines(path, dxfattribs=attribs):  # type: ignore
                self.layout.add_entity(entity)
        else:
            self.layout.add_lwpolyline(path.control_vertices(), dxfattribs=attribs)

    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        attribs = self.resolve_properties(properties)
        py_paths = [p.to_path() for p in paths]
        for hatch in to_hatches(py_paths, dxfattribs=attribs):
            self.layout.add_entity(hatch)
            self.set_solid_fill(hatch, properties)

    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        hatch = self.layout.add_hatch(dxfattribs=self.resolve_properties(properties))
        hatch.paths.add_polyline_path(points.vertices(), is_closed=True)
        self.set_solid_fill(hatch, properties)

    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        pass  # TODO: not implemented

    def configure(self, config: Configuration) -> None:
        pass

    def clear(self) -> None:
        pass

    def finalize(self) -> None:
        pass

    def enter_entity(self, entity, properties) -> None:
        pass

    def exit_entity(self, entity) -> None:
        pass


def alpha_to_transparency(alpha: int) -> float:
    return colors.float2transparency(alpha / 255)


@lru_cache(maxsize=None)
def make_lineweight(width: float) -> int:
    width_int = int(width * 100)
    for lw in VALID_DXF_LINEWEIGHTS:
        if width_int <= lw:
            return lw
    return VALID_DXF_LINEWEIGHTS[-1]


@no_type_check
def update_extents(doc: Drawing, bbox: BoundingBox2d) -> None:
    doc.header["$EXTMIN"] = (bbox.extmin.x, bbox.extmin.y, 0)
    doc.header["$EXTMAX"] = (bbox.extmax.x, bbox.extmax.y, 0)


def setup_paperspace(doc: Drawing, bbox: BoundingBox2d):
    psp_size = bbox.size / 40.0  # plu to mm
    psp_center = psp_size * 0.5
    psp = doc.paperspace()
    psp.page_setup(size=(psp_size.x, psp_size.y), margins=(0, 0, 0, 0), units="mm")
    psp.add_viewport(
        center=psp_center,
        size=(psp_size.x, psp_size.y),
        view_center_point=bbox.center,
        view_height=bbox.size.y,
        status=2,
    )


def add_background(msp: BaseLayout, bbox: BoundingBox2d, color: colors.RGB) -> Solid:
    v = bbox.rect_vertices()
    bg = msp.add_solid(
        [v[0], v[1], v[3], v[2]], dxfattribs={"true_color": colors.rgb2int(color)}
    )
    return bg
