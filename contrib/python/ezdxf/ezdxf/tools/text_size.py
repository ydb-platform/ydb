# Copyright (c) 2021-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, Optional
from dataclasses import dataclass

from ezdxf.math import Matrix44, Vec2
from ezdxf.entities import Text, MText, get_font_name
from ezdxf.fonts import fonts
from ezdxf.tools import text_layout as tl
from ezdxf.tools.text import MTextContext
from ezdxf.render.abstract_mtext_renderer import AbstractMTextRenderer
from ezdxf.tools.text import estimate_mtext_extents

__all__ = [
    "text_size",
    "mtext_size",
    "TextSize",
    "MTextSize",
    "WordSizeDetector",
    # estimate_mtext_extents() belongs also to the topic of this module, users
    # may look here first
    "estimate_mtext_extents",
]


@dataclass(frozen=True)
class TextSize:
    width: float
    # The text entity has a fixed font:
    cap_height: float  # height of "X" without descender
    total_height: float  # including the descender


@dataclass(frozen=True)
class MTextSize:
    total_width: float
    total_height: float
    column_width: float
    gutter_width: float
    column_heights: Sequence[float]

    # Storing additional font metrics like "cap_height" makes no sense, because
    # the font metrics can be variable by using inline codes to vary the text
    # height or the width factor or even changing the used font at all.
    @property
    def column_count(self) -> int:
        return len(self.column_heights)


def text_size(text: Text) -> TextSize:
    """Returns the measured text width, the font cap-height and the font
    total-height for a :class:`~ezdxf.entities.Text` entity.
    This function uses the optional `Matplotlib` package if available to measure
    the final rendering width and font-height for the :class:`Text` entity as
    close as possible. This function does not measure the real char height!
    Without access to the `Matplotlib` package the
    :class:`~ezdxf.tools.fonts.MonospaceFont` is used and the measurements are
    very inaccurate.

    See the :mod:`~ezdxf.addons.text2path` add-on for more tools to work
    with the text path objects created by the `Matplotlib` package.

    """
    width_factor: float = text.dxf.get_default("width")
    text_width: float = 0.0
    cap_height: float = text.dxf.get_default("height")
    font: fonts.AbstractFont = fonts.MonospaceFont(cap_height, width_factor)
    if text.doc is not None:
        font_name = get_font_name(text)
        font = fonts.make_font(font_name, cap_height, width_factor)

    total_height = font.measurements.total_height
    content = text.plain_text()
    if content:
        text_width = font.text_width(content)
    return TextSize(text_width, cap_height, total_height)


def mtext_size(
    mtext: MText, tool: Optional[MTextSizeDetector] = None
) -> MTextSize:
    """Returns the total-width, -height and columns information for a
    :class:`~ezdxf.entities.MText` entity.

    This function uses the optional `Matplotlib` package if available to do
    font measurements and the internal text layout engine to determine the final
    rendering size for the :class:`MText` entity as close as possible.
    Without access to the `Matplotlib` package the :class:`~ezdxf.tools.fonts.MonospaceFont`
    is used and the measurements are very inaccurate.

    Attention: The required full layout calculation is slow!

    The first call to this function with `Matplotlib` support is very slow,
    because `Matplotlib` lookup all available fonts on the system. To speedup
    the calculation and accepting inaccurate results you can disable the
    `Matplotlib` support manually::

        ezdxf.option.use_matplotlib = False

    """
    tool = tool or MTextSizeDetector()
    column_heights: list[float] = [0.0]
    gutter_width = 0.0
    column_width = 0.0
    if mtext.text:
        columns: list[tl.Column] = list(tool.measure(mtext))
        if len(columns):
            first_column = columns[0]
            # same values for all columns
            column_width = first_column.total_width
            gutter_width = first_column.gutter
            column_heights = [column.total_height for column in columns]

    count = len(column_heights)
    return MTextSize(
        total_width=column_width * count + gutter_width * (count - 1),
        total_height=max(column_heights),
        column_width=column_width,
        gutter_width=gutter_width,
        column_heights=tuple(column_heights),
    )


class MTextSizeDetector(AbstractMTextRenderer):
    def __init__(self):
        super().__init__()
        self.do_nothing = tl.DoNothingRenderer()
        self.renderer = self.do_nothing

    def reset(self):
        pass

    def word(self, text: str, ctx: MTextContext) -> tl.ContentCell:
        return tl.Text(
            # The first call to get_font() is very slow!
            width=self.get_font(ctx).text_width(text),
            height=ctx.cap_height,
            valign=tl.CellAlignment(ctx.align),
            renderer=self.renderer,
        )

    def fraction(self, data: tuple, ctx: MTextContext) -> tl.ContentCell:
        upr, lwr, type_ = data
        if type_:
            return tl.Fraction(
                top=self.word(upr, ctx),
                bottom=self.word(lwr, ctx),
                stacking=self.get_stacking(type_),
                renderer=self.renderer,
            )
        else:
            return self.word(upr, ctx)

    def get_font_face(self, mtext: MText) -> fonts.FontFace:
        return fonts.get_entity_font_face(mtext)

    def make_bg_renderer(self, mtext: MText) -> tl.ContentRenderer:
        return self.do_nothing

    def measure(self, mtext: MText) -> tl.Layout:
        self.reset()
        layout = self.layout_engine(mtext)
        layout.place()
        return layout


class WordSizeCollector(tl.DoNothingRenderer):
    """Collects word sizes as tuples of the lower left corner and the upper
    right corner as Vec2 objects, ignores lines.
    """

    def __init__(self) -> None:
        self.word_boxes: list[tuple[Vec2, Vec2]] = []

    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Optional[Matrix44] = None,
    ) -> None:
        self.word_boxes.append((Vec2(left, bottom), Vec2(right, top)))


class WordSizeDetector(MTextSizeDetector):
    def reset(self):
        self.renderer = WordSizeCollector()

    def measure(self, mtext: MText) -> tl.Layout:
        layout = super().measure(mtext)
        layout.render()
        return layout

    def word_boxes(self) -> list[tuple[Vec2, Vec2]]:
        return self.renderer.word_boxes
