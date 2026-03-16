# Copyright (c) 2021-2023, Manfred Moitzi
# License: MIT License

# This is the abstract link between the text layout engine implemented in
# ezdxf.tools.text_layout and a concrete MTEXT renderer implementation like
# MTextExplode or ComplexMTextRenderer.
from __future__ import annotations
from typing import Sequence, Optional
import abc
from ezdxf.lldxf import const
from ezdxf import colors
from ezdxf.entities.mtext import MText, MTextColumns
from ezdxf.enums import (
    MTextParagraphAlignment,
)
from ezdxf.fonts import fonts
from ezdxf.tools import text_layout as tl
from ezdxf.tools.text import (
    MTextParser,
    MTextContext,
    TokenType,
    ParagraphProperties,
    estimate_mtext_extents,
)

__all__ = ["AbstractMTextRenderer"]

ALIGN = {
    MTextParagraphAlignment.LEFT: tl.ParagraphAlignment.LEFT,
    MTextParagraphAlignment.RIGHT: tl.ParagraphAlignment.RIGHT,
    MTextParagraphAlignment.CENTER: tl.ParagraphAlignment.CENTER,
    MTextParagraphAlignment.JUSTIFIED: tl.ParagraphAlignment.JUSTIFIED,
    MTextParagraphAlignment.DISTRIBUTED: tl.ParagraphAlignment.JUSTIFIED,
    MTextParagraphAlignment.DEFAULT: tl.ParagraphAlignment.LEFT,
}

ATTACHMENT_POINT_TO_ALIGN = {
    const.MTEXT_TOP_LEFT: tl.ParagraphAlignment.LEFT,
    const.MTEXT_MIDDLE_LEFT: tl.ParagraphAlignment.LEFT,
    const.MTEXT_BOTTOM_LEFT: tl.ParagraphAlignment.LEFT,
    const.MTEXT_TOP_CENTER: tl.ParagraphAlignment.CENTER,
    const.MTEXT_MIDDLE_CENTER: tl.ParagraphAlignment.CENTER,
    const.MTEXT_BOTTOM_CENTER: tl.ParagraphAlignment.CENTER,
    const.MTEXT_TOP_RIGHT: tl.ParagraphAlignment.RIGHT,
    const.MTEXT_MIDDLE_RIGHT: tl.ParagraphAlignment.RIGHT,
    const.MTEXT_BOTTOM_RIGHT: tl.ParagraphAlignment.RIGHT,
}

STACKING = {
    "^": tl.Stacking.OVER,
    "/": tl.Stacking.LINE,
    "#": tl.Stacking.SLANTED,
}


def make_default_tab_stops(cap_height: float, width: float) -> list[tl.TabStop]:
    tab_stops = []
    step = 4.0 * cap_height
    pos = step
    while pos < width:
        tab_stops.append(tl.TabStop(pos, tl.TabStopType.LEFT))
        pos += step
    return tab_stops


def append_default_tab_stops(
    tab_stops: list[tl.TabStop], default_stops: Sequence[tl.TabStop]
) -> None:
    last_pos = 0.0
    if tab_stops:
        last_pos = tab_stops[-1].pos
    tab_stops.extend(stop for stop in default_stops if stop.pos > last_pos)


def make_tab_stops(
    cap_height: float,
    width: float,
    tab_stops: Sequence,
    default_stops: Sequence[tl.TabStop],
) -> list[tl.TabStop]:
    _tab_stops = []
    for stop in tab_stops:
        if isinstance(stop, str):
            value = float(stop[1:])
            if stop[0] == "c":
                kind = tl.TabStopType.CENTER
            else:
                kind = tl.TabStopType.RIGHT
        else:
            kind = tl.TabStopType.LEFT
            value = float(stop)
        pos = value * cap_height
        if pos < width:
            _tab_stops.append(tl.TabStop(pos, kind))

    append_default_tab_stops(_tab_stops, default_stops)
    return _tab_stops


def get_stroke(ctx: MTextContext) -> int:
    stroke = 0
    if ctx.underline:
        stroke += tl.Stroke.UNDERLINE
    if ctx.strike_through:
        stroke += tl.Stroke.STRIKE_THROUGH
    if ctx.overline:
        stroke += tl.Stroke.OVERLINE
    if ctx.continue_stroke:
        stroke += tl.Stroke.CONTINUE
    return stroke


def new_paragraph(
    cells: list,
    ctx: MTextContext,
    cap_height: float,
    line_spacing: float = 1,
    width: float = 0,
    default_stops: Optional[Sequence[tl.TabStop]] = None,
):
    if cells:
        p = ctx.paragraph
        align = ALIGN.get(p.align, tl.ParagraphAlignment.LEFT)
        left = p.left * cap_height
        right = p.right * cap_height
        first = left + p.indent * cap_height  # relative to left
        _default_stops: Sequence[tl.TabStop] = default_stops or []
        tab_stops = _default_stops
        if p.tab_stops:
            tab_stops = make_tab_stops(cap_height, width, p.tab_stops, _default_stops)
        paragraph = tl.Paragraph(
            align=align,
            indent=(first, left, right),
            line_spacing=line_spacing,
            tab_stops=tab_stops,
        )
        paragraph.append_content(cells)
    else:
        paragraph = tl.EmptyParagraph(  # type: ignore
            cap_height=ctx.cap_height, line_spacing=line_spacing
        )
    return paragraph


def super_glue():
    return tl.NonBreakingSpace(width=0, min_width=0, max_width=0)


def defined_width(mtext: MText) -> float:
    width = mtext.dxf.get("width", 0.0)
    if width < 1e-6:
        width, height = estimate_mtext_extents(mtext)
    return width


def column_heights(columns: MTextColumns) -> list[Optional[float]]:
    heights: list[Optional[float]]
    if columns.heights:  # dynamic manual
        heights = list(columns.heights)
        # last height has to be auto height = None
        heights[-1] = None
        return heights
    # static, dynamic auto
    defined_height = abs(columns.defined_height)
    if defined_height < 1e-6:
        return [None]
    return [defined_height] * columns.count


class AbstractMTextRenderer(abc.ABC):
    def __init__(self) -> None:
        self._font_cache: dict[tuple[str, float, float], fonts.AbstractFont] = {}

    @abc.abstractmethod
    def word(self, test: str, ctx: MTextContext) -> tl.ContentCell:
        ...

    @abc.abstractmethod
    def fraction(self, data: tuple[str, str, str], ctx: MTextContext) -> tl.ContentCell:
        ...

    @abc.abstractmethod
    def get_font_face(self, mtext: MText) -> fonts.FontFace:
        ...

    @abc.abstractmethod
    def make_bg_renderer(self, mtext: MText) -> tl.ContentRenderer:
        ...

    def make_mtext_context(self, mtext: MText) -> MTextContext:
        ctx = MTextContext()
        ctx.paragraph = ParagraphProperties(
            align=ATTACHMENT_POINT_TO_ALIGN.get(  # type: ignore
                mtext.dxf.attachment_point, tl.ParagraphAlignment.LEFT
            )
        )
        ctx.font_face = self.get_font_face(mtext)
        ctx.cap_height = mtext.dxf.char_height
        ctx.aci = mtext.dxf.color
        rgb = mtext.rgb
        if rgb is not None:
            ctx.rgb = colors.RGB(*rgb)
        return ctx

    def get_font(self, ctx: MTextContext) -> fonts.AbstractFont:
        ttf = fonts.find_font_file_name(ctx.font_face)  # 1st call is very slow
        key = (ttf, ctx.cap_height, ctx.width_factor)
        font = self._font_cache.get(key)
        if font is None:
            font = fonts.make_font(ttf, ctx.cap_height, ctx.width_factor)
            self._font_cache[key] = font
        return font

    def get_stroke(self, ctx: MTextContext) -> int:
        return get_stroke(ctx)

    def get_stacking(self, type_: str) -> tl.Stacking:
        return STACKING.get(type_, tl.Stacking.LINE)

    def space_width(self, ctx: MTextContext) -> float:
        return self.get_font(ctx).space_width()

    def space(self, ctx: MTextContext):
        return tl.Space(width=self.space_width(ctx))

    def tabulator(self, ctx: MTextContext):
        return tl.Tabulator(width=self.space_width(ctx))

    def non_breaking_space(self, ctx: MTextContext):
        return tl.NonBreakingSpace(width=self.space_width(ctx))

    def layout_engine(self, mtext: MText) -> tl.Layout:
        initial_cap_height = mtext.dxf.char_height
        line_spacing = mtext.dxf.line_spacing_factor

        def append_paragraph():
            paragraph = new_paragraph(
                cells,
                ctx,
                initial_cap_height,
                line_spacing,
                width,
                default_stops,
            )
            layout.append_paragraphs([paragraph])
            cells.clear()

        bg_renderer = self.make_bg_renderer(mtext)
        width = defined_width(mtext)
        default_stops = make_default_tab_stops(initial_cap_height, width)
        layout = tl.Layout(width=width)
        if mtext.has_columns:
            columns = mtext.columns
            assert columns is not None
            for height in column_heights(columns):
                layout.append_column(
                    width=columns.width,
                    height=height,
                    gutter=columns.gutter_width,
                    renderer=bg_renderer,
                )
        else:
            # column with auto height and default width
            layout.append_column(renderer=bg_renderer)

        content = mtext.all_columns_raw_content()
        ctx = self.make_mtext_context(mtext)
        cells: list[tl.Cell] = []
        for token in MTextParser(content, ctx):
            ctx = token.ctx
            if token.type == TokenType.NEW_PARAGRAPH:
                append_paragraph()
            elif token.type == TokenType.NEW_COLUMN:
                append_paragraph()
                layout.next_column()
            elif token.type == TokenType.SPACE:
                cells.append(self.space(ctx))
            elif token.type == TokenType.NBSP:
                cells.append(self.non_breaking_space(ctx))
            elif token.type == TokenType.TABULATOR:
                cells.append(self.tabulator(ctx))
            elif token.type == TokenType.WORD:
                if cells and isinstance(cells[-1], (tl.Text, tl.Fraction)):
                    # Create an unbreakable connection between those two parts.
                    cells.append(super_glue())
                cells.append(self.word(token.data, ctx))
            elif token.type == TokenType.STACK:
                if cells and isinstance(cells[-1], (tl.Text, tl.Fraction)):
                    # Create an unbreakable connection between those two parts.
                    cells.append(super_glue())
                cells.append(self.fraction(token.data, ctx))

        if cells:
            append_paragraph()

        return layout
