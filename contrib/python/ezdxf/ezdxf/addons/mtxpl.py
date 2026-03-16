#  Copyright (c) 2021-2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import cast, Any, Optional, TYPE_CHECKING
import math
import ezdxf
from ezdxf.entities import MText, DXFGraphic, Textstyle
from ezdxf.enums import TextEntityAlignment

# from ezdxf.layouts import BaseLayout
from ezdxf.document import Drawing
from ezdxf.math import Matrix44
from ezdxf.fonts import fonts
from ezdxf.tools import text_layout as tl
from ezdxf.tools.text import MTextContext
from ezdxf.render.abstract_mtext_renderer import AbstractMTextRenderer

if TYPE_CHECKING:
    from ezdxf.eztypes import GenericLayoutType

__all__ = ["MTextExplode"]


class FrameRenderer(tl.ContentRenderer):
    def __init__(self, attribs: dict, layout: GenericLayoutType):
        self.line_attribs = attribs
        self.layout = layout

    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Matrix44 = None,
    ) -> None:
        pline = self.layout.add_lwpolyline(
            [(left, top), (right, top), (right, bottom), (left, bottom)],
            close=True,
            dxfattribs=self.line_attribs,
        )
        if m:
            pline.transform(m)

    def line(
        self, x1: float, y1: float, x2: float, y2: float, m: Matrix44 = None
    ) -> None:
        line = self.layout.add_line((x1, y1), (x2, y2), dxfattribs=self.line_attribs)
        if m:
            line.transform(m)


class ColumnBackgroundRenderer(FrameRenderer):
    def __init__(
        self,
        attribs: dict,
        layout: GenericLayoutType,
        bg_aci: Optional[int] = None,
        bg_true_color: Optional[int] = None,
        offset: float = 0,
        text_frame: bool = False,
    ):
        super().__init__(attribs, layout)
        self.solid_attribs = None
        if bg_aci is not None:
            self.solid_attribs = dict(attribs)
            self.solid_attribs["color"] = bg_aci
        elif bg_true_color is not None:
            self.solid_attribs = dict(attribs)
            self.solid_attribs["true_color"] = bg_true_color
        self.offset = offset  # background border offset
        self.has_text_frame = text_frame

    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Optional[Matrix44] = None,
    ) -> None:
        # Important: this is not a clipping box, it is possible to
        # render anything outside of the given borders!
        offset = self.offset
        left -= offset
        right += offset
        top += offset
        bottom -= offset
        if self.solid_attribs is not None:
            solid = self.layout.add_solid(
                # SOLID! swap last two vertices:
                [(left, top), (right, top), (left, bottom), (right, bottom)],
                dxfattribs=self.solid_attribs,
            )
            if m:
                solid.transform(m)
        if self.has_text_frame:
            super().render(left, bottom, right, top, m)


class TextRenderer(FrameRenderer):
    """Text content renderer."""

    def __init__(
        self,
        text: str,
        text_attribs: dict,
        line_attribs: dict,
        layout: GenericLayoutType,
    ):
        super().__init__(line_attribs, layout)
        self.text = text
        self.text_attribs = text_attribs

    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Optional[Matrix44] = None,
    ):
        """Create/render the text content"""
        text = self.layout.add_text(self.text, dxfattribs=self.text_attribs)
        text.set_placement((left, bottom), align=TextEntityAlignment.LEFT)
        if m:
            text.transform(m)


# todo: replace by fonts.get_entity_font_face()
def get_font_face(entity: DXFGraphic, doc=None) -> fonts.FontFace:
    """Returns the :class:`~ezdxf.tools.fonts.FontFace` defined by the
    associated text style. Returns the default font face if the `entity` does
    not have or support the DXF attribute "style".

    Pass a DXF document as argument `doc` to resolve text styles for virtual
    entities which are not assigned to a DXF document. The argument `doc`
    always overrides the DXF document to which the `entity` is assigned to.

    """
    if entity.doc and doc is None:
        doc = entity.doc
    assert doc is not None, "valid DXF document required"

    style_name = ""
    # This works also for entities which do not support "style",
    # where style_name = entity.dxf.get("style") would fail.
    if entity.dxf.is_supported("style"):
        style_name = entity.dxf.style

    font_face = fonts.FontFace()
    if style_name and doc is not None:
        style = cast(Textstyle, doc.styles.get(style_name))
        family, italic, bold = style.get_extended_font_data()
        if family:
            text_style = "Italic" if italic else "Regular"
            text_weight = 700 if bold else 400
            font_face = fonts.FontFace(
                family=family, style=text_style, weight=text_weight
            )
        else:
            ttf = style.dxf.font
            if ttf:
                font_face = fonts.get_font_face(ttf)
    return font_face


def get_color_attribs(ctx: MTextContext) -> dict:
    attribs = {"color": ctx.aci}
    if ctx.rgb is not None:
        attribs["true_color"] = ezdxf.rgb2int(ctx.rgb)
    return attribs


def make_bg_renderer(mtext: MText, layout: GenericLayoutType):
    attribs = get_base_attribs(mtext)
    dxf = mtext.dxf
    bg_fill = dxf.get("bg_fill", 0)

    bg_aci = None
    bg_true_color = None
    has_text_frame = False
    offset = 0
    if bg_fill:
        # The fill scale is a multiple of the initial char height and
        # a scale of 1, fits exact the outer border
        # of the column -> offset = 0
        offset = dxf.char_height * (dxf.get("box_fill_scale", 1.5) - 1)
        if bg_fill & ezdxf.const.MTEXT_BG_COLOR:
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

        if bg_fill & ezdxf.const.MTEXT_TEXT_FRAME:
            has_text_frame = True

    return ColumnBackgroundRenderer(
        attribs,
        layout,
        bg_aci=bg_aci,
        bg_true_color=bg_true_color,
        offset=offset,
        text_frame=has_text_frame,
    )


def get_base_attribs(mtext: MText) -> dict:
    dxf = mtext.dxf
    attribs = {
        "layer": dxf.layer,
        "color": dxf.color,
    }
    return attribs


class MTextExplode(AbstractMTextRenderer):
    """The :class:`MTextExplode` class is a tool to disassemble MTEXT entities
    into single line TEXT entities and additional LINE entities if required to
    emulate strokes.

    The `layout` argument defines the target layout  for "exploded" parts of the
    MTEXT entity. Use argument `doc` if the target layout has no DXF document assigned
    like virtual layouts.  The `spacing_factor` argument is an advanced tuning parameter
    to scale the size of space chars.

    """

    def __init__(
        self,
        layout: GenericLayoutType,
        doc: Optional[Drawing] = None,
        spacing_factor: float = 1.0,
    ):
        super().__init__()
        self.layout: GenericLayoutType = layout
        self._doc = doc
        # scale the width of spaces by this factor:
        self._spacing_factor = float(spacing_factor)
        self._required_text_styles: dict[str, fonts.FontFace] = {}
        self.current_base_attribs: dict[str, Any] = dict()

    # Implementation of required AbstractMTextRenderer methods and overrides:

    def layout_engine(self, mtext: MText) -> tl.Layout:
        self.current_base_attribs = get_base_attribs(mtext)
        return super().layout_engine(mtext)

    def word(self, text: str, ctx: MTextContext) -> tl.ContentCell:
        line_attribs = dict(self.current_base_attribs or {})
        line_attribs.update(get_color_attribs(ctx))
        text_attribs = dict(line_attribs)
        text_attribs.update(self.get_text_attribs(ctx))
        return tl.Text(
            width=self.get_font(ctx).text_width(text),
            height=ctx.cap_height,
            valign=tl.CellAlignment(ctx.align),
            stroke=self.get_stroke(ctx),
            renderer=TextRenderer(text, text_attribs, line_attribs, self.layout),
        )

    def fraction(self, data: tuple, ctx: MTextContext) -> tl.ContentCell:
        upr, lwr, type_ = data
        if type_:
            return tl.Fraction(
                top=self.word(upr, ctx),
                bottom=self.word(lwr, ctx),
                stacking=self.get_stacking(type_),
                # renders just the divider line:
                renderer=FrameRenderer(self.current_base_attribs, self.layout),
            )
        else:
            return self.word(upr, ctx)

    def get_font_face(self, mtext: MText) -> fonts.FontFace:
        return get_font_face(mtext)

    def make_bg_renderer(self, mtext: MText) -> tl.ContentRenderer:
        return make_bg_renderer(mtext, self.layout)

    # Implementation details of MTextExplode:

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalize()

    def mtext_exploded_text_style(self, font_face: fonts.FontFace) -> str:
        style = 0
        if font_face.is_bold:
            style += 1
        if font_face.is_italic:
            style += 2
        style_str = str(style) if style > 0 else ""
        # BricsCAD naming convention for exploded MTEXT styles:
        text_style = f"MtXpl_{font_face.family}" + style_str
        self._required_text_styles[text_style] = font_face
        return text_style

    def get_font(self, ctx: MTextContext) -> fonts.AbstractFont:
        ttf = fonts.find_font_file_name(ctx.font_face)
        key = (ttf, ctx.cap_height, ctx.width_factor)
        font = self._font_cache.get(key)
        if font is None:
            font = fonts.make_font(ttf, ctx.cap_height, ctx.width_factor)
            self._font_cache[key] = font
        return font

    def get_text_attribs(self, ctx: MTextContext) -> dict:
        attribs = {
            "height": ctx.cap_height,
            "style": self.mtext_exploded_text_style(ctx.font_face),
        }
        if not math.isclose(ctx.width_factor, 1.0):
            attribs["width"] = ctx.width_factor
        if abs(ctx.oblique) > 1e-6:
            attribs["oblique"] = ctx.oblique
        return attribs

    def explode(self, mtext: MText, destroy=True):
        """Explode `mtext` and destroy the source entity if argument `destroy`
        is ``True``.
        """
        align = tl.LayoutAlignment(mtext.dxf.attachment_point)
        layout_engine = self.layout_engine(mtext)
        layout_engine.place(align=align)
        layout_engine.render(mtext.ucs().matrix)
        if destroy:
            mtext.destroy()

    def finalize(self):
        """Create required text styles. This method is called automatically if
        the class is used as context manager. This method does not work with virtual
        layouts if no document was assigned at initialization!
        """

        doc = self._doc
        if doc is None:
            doc = self.layout.doc
        if doc is None:
            raise ezdxf.DXFValueError(
                "DXF document required, finalize() does not work with virtual layouts "
                "if no document was assigned at initialization."
            )
        text_styles = doc.styles
        for style in self.make_required_style_table_entries():
            try:
                text_styles.add_entry(style)
            except ezdxf.DXFTableEntryError:
                pass

    def make_required_style_table_entries(self) -> list[Textstyle]:
        def ttf_path(font_face: fonts.FontFace) -> str:
            ttf = font_face.filename
            if not ttf:
                ttf = fonts.find_font_file_name(font_face)
            else:
                # remapping SHX replacement fonts to SHX fonts,
                # like "txt_____.ttf" to "TXT.SHX":
                shx = fonts.map_ttf_to_shx(ttf)
                if shx:
                    ttf = shx
            return ttf

        text_styles: list[Textstyle] = []
        for name, font_face in self._required_text_styles.items():
            ttf = ttf_path(font_face)
            style = Textstyle.new(dxfattribs={
                "name": name,
                "font": ttf,
            })
            if not ttf.endswith(".SHX"):
                style.set_extended_font_data(
                    font_face.family,
                    italic=font_face.is_italic,
                    bold=font_face.is_bold,
                )
            text_styles.append(style)
        return text_styles
