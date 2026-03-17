#  Copyright (c) 2021-2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Optional
from typing_extensions import Protocol
import copy
import math

from ezdxf import colors
from ezdxf.entities import MText
from ezdxf.lldxf import const
from ezdxf.math import Matrix44, Vec3, AnyVec
from ezdxf.render.abstract_mtext_renderer import AbstractMTextRenderer
from ezdxf.fonts import fonts
from ezdxf.tools import text_layout as tl
from ezdxf.tools.text import MTextContext
from .properties import Properties, RenderContext, rgb_to_hex
from .type_hints import Color

__all__ = ["complex_mtext_renderer"]


def corner_vertices(
    left: float,
    bottom: float,
    right: float,
    top: float,
    m: Matrix44 = None,
) -> Iterable[Vec3]:
    corners = [  # closed polygon: fist vertex  == last vertex
        (left, top),
        (right, top),
        (right, bottom),
        (left, bottom),
        (left, top),
    ]
    if m is None:
        return Vec3.generate(corners)
    else:
        return m.transform_vertices(corners)


class DrawInterface(Protocol):
    def draw_line(self, start: AnyVec, end: AnyVec, properties: Properties) -> None:
        ...

    def draw_filled_polygon(
        self, points: Iterable[AnyVec], properties: Properties
    ) -> None:
        ...

    def draw_text(
        self,
        text: str,
        transform: Matrix44,
        properties: Properties,
        cap_height: float,
    ) -> None:
        ...


class FrameRenderer(tl.ContentRenderer):
    def __init__(self, properties: Properties, backend: DrawInterface):
        self.properties = properties
        self.backend = backend

    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Matrix44 = None,
    ) -> None:
        self._render_outline(list(corner_vertices(left, bottom, right, top, m)))

    def _render_outline(self, vertices: list[Vec3]) -> None:
        backend = self.backend
        properties = self.properties
        prev = vertices.pop(0)
        for vertex in vertices:
            backend.draw_line(prev, vertex, properties)
            prev = vertex

    def line(
        self, x1: float, y1: float, x2: float, y2: float, m: Matrix44 = None
    ) -> None:
        points = [(x1, y1), (x2, y2)]
        if m is not None:
            p1, p2 = m.transform_vertices(points)
        else:
            p1, p2 = Vec3.generate(points)
        self.backend.draw_line(p1, p2, self.properties)


class ColumnBackgroundRenderer(FrameRenderer):
    def __init__(
        self,
        properties: Properties,
        backend: DrawInterface,
        bg_properties: Optional[Properties] = None,
        offset: float = 0,
        text_frame: bool = False,
    ):
        super().__init__(properties, backend)
        self.bg_properties = bg_properties
        self.offset = offset  # background border offset
        self.has_text_frame = text_frame

    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Matrix44 = None,
    ) -> None:
        # Important: this is not a clipping box, it is possible to
        # render anything outside of the given borders!
        offset = self.offset
        vertices = list(
            corner_vertices(
                left - offset, bottom - offset, right + offset, top + offset, m
            )
        )
        if self.bg_properties is not None:
            self.backend.draw_filled_polygon(vertices, self.bg_properties)
        if self.has_text_frame:
            self._render_outline(vertices)


class TextRenderer(FrameRenderer):
    """Text content renderer."""

    def __init__(
        self,
        text: str,
        cap_height: float,
        width_factor: float,
        oblique: float,  # angle in degrees
        properties: Properties,
        backend: DrawInterface,
    ):
        super().__init__(properties, backend)
        self.text = text
        self.cap_height = cap_height
        self.width_factor = width_factor
        self.oblique = oblique  # angle in degrees

    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Matrix44 = None,
    ):
        """Create/render the text content"""
        sx = 1.0
        tx = 0.0
        if not math.isclose(self.width_factor, 1.0, rel_tol=1e-6):
            sx = self.width_factor
        if abs(self.oblique) > 1e-3:  # degrees
            tx = math.tan(math.radians(self.oblique))
        # fmt: off
        t = Matrix44((
            sx, 0.0, 0.0, 0.0,
            tx, 1.0, 0.0, 0.0,
            0.0, 0.0, 1.0, 0.0,
            left, bottom, 0.0, 1.0
        ))
        # fmt: on
        if m is not None:
            t *= m
        self.backend.draw_text(self.text, t, self.properties, self.cap_height)


def complex_mtext_renderer(
    ctx: RenderContext,
    backend: DrawInterface,
    mtext: MText,
    properties: Properties,
) -> None:
    cmr = ComplexMTextRenderer(ctx, backend, properties)
    align = tl.LayoutAlignment(mtext.dxf.attachment_point)
    layout_engine = cmr.layout_engine(mtext)
    layout_engine.place(align=align)
    layout_engine.render(mtext.ucs().matrix)


class ComplexMTextRenderer(AbstractMTextRenderer):
    def __init__(
        self,
        ctx: RenderContext,
        backend: DrawInterface,
        properties: Properties,
    ):
        super().__init__()
        self._render_ctx = ctx
        self._backend = backend
        self._properties = properties

    # Implementation of required AbstractMTextRenderer methods:

    def word(self, text: str, ctx: MTextContext) -> tl.ContentCell:
        return tl.Text(
            width=self.get_font(ctx).text_width(text),
            height=ctx.cap_height,
            valign=tl.CellAlignment(ctx.align),
            stroke=self.get_stroke(ctx),
            renderer=TextRenderer(
                text,
                ctx.cap_height,
                ctx.width_factor,
                ctx.oblique,
                self.new_text_properties(self._properties, ctx),
                self._backend,
            ),
        )

    def fraction(self, data: tuple[str, str, str], ctx: MTextContext) -> tl.ContentCell:
        upr, lwr, type_ = data
        if type_:
            return tl.Fraction(
                top=self.word(upr, ctx),
                bottom=self.word(lwr, ctx),
                stacking=self.get_stacking(type_),
                # renders just the divider line:
                renderer=FrameRenderer(self._properties, self._backend),
            )
        else:
            return self.word(upr, ctx)

    def get_font_face(self, mtext: MText) -> fonts.FontFace:
        return self._properties.font  # type: ignore

    def make_bg_renderer(self, mtext: MText) -> tl.ContentRenderer:
        dxf = mtext.dxf
        bg_fill = dxf.get("bg_fill", 0)

        bg_aci = None
        bg_true_color = None
        bg_properties: Optional[Properties] = None
        has_text_frame = False
        offset = 0
        if bg_fill:
            # The fill scale is a multiple of the initial char height and
            # a scale of 1, fits exact the outer border
            # of the column -> offset = 0
            offset = dxf.char_height * (dxf.get("box_fill_scale", 1.5) - 1)
            if bg_fill & const.MTEXT_BG_COLOR:
                if dxf.hasattr("bg_fill_color"):
                    bg_aci = dxf.bg_fill_color

                if dxf.hasattr("bg_fill_true_color"):
                    bg_aci = None
                    bg_true_color = dxf.bg_fill_true_color

                if (bg_fill & 3) == 3:  # canvas color = bit 0 and 1 set
                    # can not detect canvas color from DXF document!
                    # do not draw any background:
                    bg_aci = None
                    bg_true_color = None

            if bg_fill & const.MTEXT_TEXT_FRAME:
                has_text_frame = True
            bg_properties = self.new_bg_properties(bg_aci, bg_true_color)

        return ColumnBackgroundRenderer(
            self._properties,
            self._backend,
            bg_properties,
            offset=offset,
            text_frame=has_text_frame,
        )

    # Implementation details of ComplexMTextRenderer:

    @property
    def backend(self) -> DrawInterface:
        return self._backend

    def resolve_aci_color(self, aci: int) -> Color:
        return self._render_ctx.resolve_aci_color(aci, self._properties.layer)

    def new_text_properties(
        self, properties: Properties, ctx: MTextContext
    ) -> Properties:
        new_properties = copy.copy(properties)
        if ctx.rgb is None:
            new_properties.color = self.resolve_aci_color(ctx.aci)
        else:
            new_properties.color = rgb_to_hex(ctx.rgb)
        new_properties.font = ctx.font_face
        return new_properties

    def new_bg_properties(
        self, aci: Optional[int], true_color: Optional[int]
    ) -> Properties:
        new_properties = copy.copy(self._properties)
        new_properties.color = (  # canvas background color
            self._render_ctx.current_layout_properties.background_color
        )
        if true_color is None:
            if aci is not None:
                new_properties.color = self.resolve_aci_color(aci)
            # else canvas background color
        else:
            new_properties.color = rgb_to_hex(colors.int2rgb(true_color))
        return new_properties
