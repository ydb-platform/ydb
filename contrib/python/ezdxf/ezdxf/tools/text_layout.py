# Copyright (c) 2021-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, Iterable, Optional, Tuple, NamedTuple
from typing_extensions import TypeAlias
import abc
import itertools
import enum

from ezdxf.math import Matrix44, BoundingBox2d
from ezdxf.tools.text import leading

"""

Text Layout Engine
==================

The main goal of this text layout engine is to layout words as boxes in 
columns, paragraphs, and (bullet) lists. 

The starting point is a layout engine for MTEXT, which can be used for 
different purposes like the drawing add-on or exploding MTEXT into DXF 
primitives. But the engine is not bound to the MTEXT entity, the MTEXT 
entity just defines the basic requirements.

This engine works on given (text) boxes as input and does not render the glyphs
by itself nor does it have any knowledge about the glyphs, therefore individual 
kerning between letters is not supported in anyway. As consequence the 
"distributed" paragraph alignment of MTEXT can not be supported.

Each input box can have an individual rendering object attached, derived from 
the :class:`ContentRenderer` class, which requires two methods:

1. method :meth:`render` to render the box content like the text or the 
   container background

2. method :meth:`line` to render simple straight lines like under- and over 
   stroke lines or fraction dividers.

Soft hyphens or auto word wrapping is not supported.

Text direction is determined by the client by the given arrangement of the 
input cells, but the vertical flow is always organized in lines from top to 
bottom.

The main work done by the layout engine is the placing of the given content 
cells. The layout engine does not change the size of content cells and only 
adjusts the width of glue cells e.g. "justified" paragraphs.

Switching fonts or changing text size and -color has to be done by the client 
at the process of dividing the input text into text- and glue cells and 
assigning them appropriate rendering functions.

The only text styling provided by the layout engine are strokes above, through 
or below one or more words, which have to span across glue cells.

Content organization
--------------------

The content is divided into containers (layout, column, paragraphs, ...) and
simple boxes for the actual content as cells like words and glue cells like 
spaces or tabs.

The basic content object is a text cell, which represents a single word. 
Fractions of the MTEXT entity are supported by fraction cells. Content cells
have to be separated by mandatory glue cells. 
Non breaking spaces have to be fed into the layout engine as special glue 
element, because it is also a simple space, which should be adjustable in the 
"justified" paragraph alignment. 

Containers
----------

All containers support margins.

1. Layout

    Contains only columns. The size of the layout is determined by the 
    columns inside of the layout. Each column can have a different width.
    
2. Column
    
    Contains only paragraphs. A Column has a fixed width, the height can be
    fixed (MTEXT) or flexible.
    
3. Paragraph

    A paragraph has a fixed width and the height is always flexible.
    A paragraph can contain anything except the high level containers
    Layout and Column.
    
    3.1 FlowText, supports left, right, center and justified alignments;
        indentation for the left side, the right side and the first line; 
        line spacing; no nested paragraphs or bullet lists;
        The final content is distributed as lines (HCellGroup).
        
    3.2 BulletList, the "bullet" can be any text cell, the flow text of each
        list is an paragraph with left aligned text ...

Simple Boxes
------------

Do not support margins.

1. Glue cells

    The height of glue cells is always 0.

    1.1 Space, flexible width but has a minimum width, possible line break
    1.2 Non breaking space, like a space but prevents line break between 
        adjacent text cells
    1.3 Tabulator, the current implementation treats tabulators like spaces. 

2. Content cells

    2.1 Text cell - the height of a text cell is the cap height (height of 
        letter "X"), ascenders and descenders are ignored. 
        This is not a clipping box, the associated render object can still draw 
        outside of the box borders, this box is only used to determine the final 
        layout location.
        
    2.2 Fraction cell ... (MTEXT!)

3. AbstractLine
    
    A line contains only simple boxes and has a fixed width. 
    The height is determined by the tallest box of the group. 
    The content cells (words) are connected/separated by mandatory glue cells.

"""

LOREM_IPSUM = """Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed 
diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed 
diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet 
clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.
"""


class LayoutError(Exception):
    pass


class Stacking(enum.IntEnum):
    OVER = 0
    LINE = 1
    SLANTED = 2


class LayoutAlignment(enum.IntEnum):
    TOP_LEFT = 1
    TOP_CENTER = 2
    TOP_RIGHT = 3
    MIDDLE_LEFT = 4
    MIDDLE_CENTER = 5
    MIDDLE_RIGHT = 6
    BOTTOM_LEFT = 7
    BOTTOM_CENTER = 8
    BOTTOM_RIGHT = 9


class CellAlignment(enum.IntEnum):
    BOTTOM = 0
    CENTER = 1
    TOP = 2


class ParagraphAlignment(enum.IntEnum):
    LEFT = 1
    RIGHT = 2
    CENTER = 3
    JUSTIFIED = 4


class TabStopType(enum.IntEnum):
    LEFT = 0
    RIGHT = 1
    CENTER = 2


class TabStop(NamedTuple):
    pos: float = 0.0
    kind: TabStopType = TabStopType.LEFT


def lorem_ipsum(count=100):
    return itertools.islice(itertools.cycle(LOREM_IPSUM.split()), count)


class ContentRenderer(abc.ABC):
    @abc.abstractmethod
    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Matrix44 = None,
    ) -> None:
        """Render content into the given borders (lower left and upper right
        corners).

        Args:
            left: x coordinate of the left border
            bottom: y coordinate of the bottom border
            right: x coordinate of the right border
            top: y coordinate of the top border
            m: transformation Matrix44

        """

    @abc.abstractmethod
    def line(
        self, x1: float, y1: float, x2: float, y2: float, m: Matrix44 = None
    ) -> None:
        """Draw a line from (x1, y1) to (x2, y2)."""


class DoNothingRenderer(ContentRenderer):
    def render(
        self,
        left: float,
        bottom: float,
        right: float,
        top: float,
        m: Matrix44 = None,
    ) -> None:
        pass

    def line(
        self, x1: float, y1: float, x2: float, y2: float, m: Matrix44 = None
    ) -> None:
        pass


Tuple4f: TypeAlias = Tuple[float, float, float, float]
Tuple2f: TypeAlias = Tuple[float, float]


def resolve_margins(margins: Optional[Sequence[float]]) -> Tuple4f:
    """Returns the box margins in CSS like order: top, right, bottom, left."""
    if margins is None:
        return 0, 0, 0, 0
    count = len(margins)
    if count == 4:  # CSS: top, right, bottom, left
        return margins[0], margins[1], margins[2], margins[3]
    if count == 3:  # CSS: top, right, bottom, left=right
        return margins[0], margins[1], margins[2], margins[1]
    if count == 2:  # CSS: top, right, bottom=top, left=right
        return margins[0], margins[1], margins[0], margins[1]
    if count == 1:  # CSS: top, right=top, bottom=top, left=top
        return margins[0], margins[0], margins[0], margins[0]
    return 0, 0, 0, 0


def insert_location(align: LayoutAlignment, width: float, height: float) -> Tuple2f:
    """Returns the left top corner adjusted to the given alignment."""
    left: float = 0.0
    top: float = 0.0
    center = width / 2.0
    middle = height / 2.0
    if align == LayoutAlignment.TOP_LEFT:
        pass
    elif align == LayoutAlignment.TOP_CENTER:
        left, top = (-center, 0)
    elif align == LayoutAlignment.TOP_RIGHT:
        left, top = (-width, 0)
    elif align == LayoutAlignment.MIDDLE_LEFT:
        left, top = (0, middle)
    elif align == LayoutAlignment.MIDDLE_CENTER:
        left, top = (-center, middle)
    elif align == LayoutAlignment.MIDDLE_RIGHT:
        left, top = (-width, middle)
    elif align == LayoutAlignment.BOTTOM_LEFT:
        left, top = (0, height)
    elif align == LayoutAlignment.BOTTOM_CENTER:
        left, top = (-center, height)
    elif align == LayoutAlignment.BOTTOM_RIGHT:
        left, top = (-width, height)
    return left, top


class Box(abc.ABC):
    @property
    @abc.abstractmethod
    def total_width(self) -> float: ...

    @property
    @abc.abstractmethod
    def total_height(self) -> float: ...

    @abc.abstractmethod
    def place(self, x: float, y: float):
        """(x, y) is the top/left corner"""

    @abc.abstractmethod
    def final_location(self) -> tuple[float, float]:
        """Returns the final location as the top/left corner"""

    @abc.abstractmethod
    def render(self, m: Matrix44 = None) -> None:
        """Render content at the final location."""

    def bbox(self) -> BoundingBox2d:
        """Returns the 2D bounding box of the container. If the cell is not placed the 
        top/left corner is (0, 0).

        """
        try:
            x, y = self.final_location()
        except (LayoutError, TypeError):
            x, y = 0, 0
        return BoundingBox2d([(x, y), (x + self.total_width, y - self.total_height)])


class Cell(Box):  # ABC
    is_visible = False

    def place(self, x: float, y: float):
        # Base cells do not render anything, therefore placing the content is
        # not necessary
        pass

    def final_location(self) -> tuple[float, float]:
        # Base cells do not render anything, therefore final location is not
        # important
        return 0, 0

    def render(self, m: Matrix44 = None) -> None:
        pass


class Glue(Cell):  # ABC
    EMPTY: tuple = tuple()

    def __init__(
        self,
        width: float,
        min_width: Optional[float] = None,
        max_width: Optional[float] = None,
    ):
        self._width: float = float(width)
        self._min_width = float(min_width) if min_width else self._width
        self._max_width: Optional[float] = max_width

    def resize(self, width: float):
        max_width = self._max_width
        if max_width is not None:
            width = min(max_width, width)
        self._width = max(width, self._min_width)

    @property
    def can_shrink(self):
        return self._min_width < self._width

    @property
    def can_grow(self):
        return self._max_width is None or self._width < self._max_width

    @property
    def total_width(self) -> float:
        return self._width

    @property
    def total_height(self) -> float:
        return 0

    def to_space(self) -> Space:
        return Space(self._width, self._min_width, self._max_width)


class Space(Glue):
    pass


class NonBreakingSpace(Glue):
    pass


class Tabulator(Glue):
    pass


class ContentCell(Cell):  # ABC
    """Represents visible content like text or fractions.

    Supported vertical alignments (IntEnum):

        === =================
        int CellAlignment
        === =================
        0   BOTTOM
        1   CENTER
        2   TOP
        === =================

    """

    is_visible = True

    def __init__(
        self,
        width: float,
        height: float,
        valign: CellAlignment = CellAlignment.BOTTOM,
        renderer: Optional[ContentRenderer] = None,
    ):
        self._final_x: Optional[float] = None
        self._final_y: Optional[float] = None
        self._width = float(width)
        self._height = float(height)
        self.valign = CellAlignment(valign)  # public attribute read/write
        self.renderer = renderer

    def set_final_location(self, x: float, y: float):
        self._final_x = x
        self._final_y = y

    def final_location(self):
        return self._final_x, self._final_y

    @property
    def total_width(self) -> float:
        return self._width

    @property
    def total_height(self) -> float:
        return self._height

    def place(self, x: float, y: float):
        """(x, y) is the top/left corner"""
        self._final_x = x
        self._final_y = y


class Stroke:
    # no enum because bit values can be combined: UNDERLINE + OVERLINE
    NO_STROKE = 0
    UNDERLINE = 1
    STRIKE_THROUGH = 2
    OVERLINE = 4
    CONTINUE = 8  # continue stroke to following text cell


class Text(ContentCell):
    """Represents visible text content.

    Supported strokes as bit values (flags), can be combined:

        === =================
        int Stroke
        === =================
        0   NO_STROKE
        1   UNDERLINE
        2   STRIKE THROUGH
        4   OVERLINE
        8   CONTINUE
        === =================

    The CONTINUE flag extends the stroke of the current text cell across the
    glue cells to the following text cell.

    """

    def __init__(
        self,
        width: float,
        height: float,
        valign: CellAlignment = CellAlignment.BOTTOM,
        stroke: int = Stroke.NO_STROKE,
        renderer: Optional[ContentRenderer] = None,
    ):
        super().__init__(width, height, valign, renderer)
        self.stroke = int(stroke)  # public attribute read/write

    def render(self, m: Optional[Matrix44] = None) -> None:
        left, top = self.final_location()
        height = self.total_height
        bottom = top - height
        right = left + self.total_width
        self.renderer.render(  # type: ignore
            left=left, bottom=bottom, right=right, top=top, m=m
        )

    def render_stroke(
        self,
        extend_left: float = 0,
        extend_right: float = 0,
        m: Matrix44 = None,
    ) -> None:
        left, top = self.final_location()
        left -= extend_left
        height = self.total_height
        bottom = top - height
        right = left + self.total_width + extend_right
        renderer = self.renderer
        assert renderer is not None

        # render underline, strike through, overline
        spacing = height / 5  # ???
        if self.stroke & Stroke.UNDERLINE:
            y = bottom - spacing
            renderer.line(left, y, right, y, m)
        if self.stroke & Stroke.STRIKE_THROUGH:
            y = (top + bottom) / 2
            renderer.line(left, y, right, y, m)
        if self.stroke & Stroke.OVERLINE:
            y = top + spacing
            renderer.line(left, y, right, y, m)


def render_cells(cells: Iterable[Cell], m: Matrix44 = None) -> None:
    for cell in cells:
        if cell.is_visible:
            cell.render(m)


def render_text_strokes(cells: list[Cell], m: Matrix44 = None) -> None:
    """Render text cell strokes across glue cells."""

    # Should be called for container with horizontal arranged text cells
    # like HCellGroup to create underline, overline and strike through
    # features.
    # Can not render strokes across line breaks!
    def stroke_extension():
        extend = 0
        i = index + 1
        count = len(cells)
        while i < count:
            cell = cells[i]
            # extend stroke only across adjacent glue cells:
            if isinstance(cell, Glue):
                extend += cell.total_width
            else:
                break
            i += 1
        return extend

    for index, cell in enumerate(cells):
        if isinstance(cell, Text) and cell.stroke:
            extend = stroke_extension() if cell.stroke & Stroke.CONTINUE else 0
            cell.render_stroke(extend_right=extend, m=m)


class Fraction(ContentCell):
    """Represents visible fractions.

    Supported stacking A/B (IntEnum):

        === =========== =========
        int Stacking    Description
        === =========== =========
        0   OVER        A over B, without horizontal line
        1   LINE        A over B, horizontal line between
        2   SLANTED     A slanted line B
        === =========== =========

    """

    HEIGHT_SCALE = 1.2

    def __init__(
        self,
        top: ContentCell,
        bottom: ContentCell,
        stacking: Stacking = Stacking.OVER,
        valign: CellAlignment = CellAlignment.BOTTOM,
        renderer: Optional[ContentRenderer] = None,
    ):
        super().__init__(0, 0, valign, renderer)
        self._stacking = stacking
        self._top_content = top
        self._bottom_content = bottom
        self._update_size()

    def _update_size(self):
        top = self._top_content
        bottom = self._bottom_content
        if self._stacking == Stacking.SLANTED:
            self._height = top.total_height + bottom.total_height
            self._width = top.total_width + bottom.total_width
        else:
            self._height = self.HEIGHT_SCALE * (top.total_height + bottom.total_height)
            self._width = max(top.total_width, bottom.total_width)

    def place(self, x: float, y: float):
        """(x, y) is the top/left corner"""
        self._final_x = x
        self._final_y = y
        width = self.total_width
        height = self.total_height
        top_content = self._top_content
        bottom_content = self._bottom_content
        if top_content is None or bottom_content is None:
            raise LayoutError("no content set")

        if self._stacking == Stacking.SLANTED:
            top_content.place(x, y)  # left/top
            x += width - bottom_content.total_width
            y -= height - bottom_content.total_height
            bottom_content.place(x, y)  # right/bottom
        else:
            center = x + width / 2
            x = center - top_content.total_width / 2
            top_content.place(x, y)  # center/top
            x = center - bottom_content.total_width / 2
            y -= height - bottom_content.total_height
            bottom_content.place(x, y)  # center/bottom

    def render(self, m: Matrix44 = None) -> None:
        self._top_content.render(m)
        self._bottom_content.render(m)
        if self._stacking != Stacking.OVER:
            self._render_line(m)

    def _render_line(self, m: Matrix44) -> None:
        x, y = self.final_location()
        tw = self.total_width
        th = self.total_height
        if self._stacking == Stacking.LINE:
            x1 = x
            x2 = x + tw
            y1 = y2 = y - th / 2
        else:  # SLANTED
            delta = min(tw, th) / 2
            cx = x + self._top_content.total_width
            cy = y - self._top_content.total_height
            x1 = cx - delta
            y1 = cy - delta
            x2 = cx + delta
            y2 = cy + delta
        self.renderer.line(x1, y1, x2, y2, m)  # type: ignore


_content = (Text, Fraction)
_glue = (Space, NonBreakingSpace, Tabulator)
_no_break = (Text, NonBreakingSpace)


def normalize_cells(cells: Iterable[Cell]) -> list[Cell]:
    def replace_pending_nbsp_by_spaces():
        index = len(content) - 1
        while index >= 0:
            cell = content[index]
            if isinstance(cell, NonBreakingSpace):
                content[index] = cell.to_space()
                index -= 1
            else:
                return

    def is_useless_nbsp():
        try:
            peek = cells[index + 1]
        except IndexError:
            return True
        if not isinstance(prev, _no_break) or not isinstance(peek, _no_break):
            return True
        return False

    content = []
    cells = list(cells)
    prev = None
    for index, cell in enumerate(cells):
        if isinstance(cell, _content):
            if isinstance(prev, _content):
                raise LayoutError("no glue between content cells")
        elif isinstance(cell, NonBreakingSpace) and is_useless_nbsp():
            cell = cell.to_space()
            replace_pending_nbsp_by_spaces()

        prev = cell
        content.append(cell)

    # remove pending glue:
    while content and isinstance(content[-1], _glue):
        content.pop()

    return content


class Container(Box):
    def __init__(
        self,
        width: Optional[float],
        height: Optional[float] = None,
        margins: Optional[Sequence[float]] = None,
        renderer: Optional[ContentRenderer] = None,
    ):
        self._final_x: Optional[float] = None
        self._final_y: Optional[float] = None

        # _content_width is None for: defined by content
        self._content_width: Optional[float] = width

        # _content_height is None for: defined by content
        self._content_height: Optional[float] = height

        # margins are always defined
        self._margins: Tuple4f = resolve_margins(margins)

        # content renderer is optional:
        self.renderer: Optional[ContentRenderer] = renderer

    def place(self, x: float, y: float):
        self._final_x = x
        self._final_y = y
        self.place_content()

    def final_location(self):
        if not self.is_placed():
            raise LayoutError("Container is not placed.")
        return self._final_x, self._final_y

    def is_placed(self) -> bool:
        return self._final_x is not None and self._final_y is not None

    @abc.abstractmethod
    def __iter__(self) -> Box:
        pass

    @property
    def top_margin(self) -> float:
        return self._margins[0]

    @property
    def right_margin(self) -> float:
        return self._margins[1]

    @property
    def bottom_margin(self) -> float:
        return self._margins[2]

    @property
    def left_margin(self) -> float:
        return self._margins[3]

    @property
    def content_width(self) -> float:
        if self._content_width is None:
            return 0
        else:
            return self._content_width

    @property
    def total_width(self) -> float:
        return self.content_width + self.right_margin + self.left_margin

    @property
    def content_height(self) -> float:
        if self._content_height is None:
            return 0
        else:
            return self._content_height

    @property
    def has_flex_height(self):
        return self._content_height is None

    @property
    def total_height(self) -> float:
        return self.content_height + self.top_margin + self.bottom_margin

    def render(self, m: Matrix44 = None) -> None:
        """Render container content.

        (x, y) is the top/left corner
        """
        if not self.is_placed():
            raise LayoutError("Layout has to be placed before rendering")
        if self.renderer:
            self.render_background(m)
        self.render_content(m)

    @abc.abstractmethod
    def place_content(self):
        """Place container content at the final location."""
        pass

    def render_content(self, m: Matrix44 = None) -> None:
        """Render content at the final location."""
        for entity in self:  # type: ignore
            entity.render(m)

    def render_background(self, m: Matrix44) -> None:
        """Render background at the final location."""
        # Render content background inclusive margins!
        # (x, y) is the top/left corner
        x, y = self.final_location()
        if self.renderer:
            self.renderer.render(
                left=x,
                bottom=y - self.total_height,
                top=y,
                right=x + self.total_width,
                m=m,
            )


class EmptyParagraph(Cell):
    """Spacer between two paragraphs, represents empty lines like in
    "line1\n\nline2".
    """

    def __init__(self, cap_height: float, line_spacing: float = 1):
        self._height: float = cap_height
        self._width: float = 0
        self._last_line_spacing = leading(cap_height, line_spacing) - cap_height

    @property
    def total_width(self) -> float:
        return self._width

    @property
    def total_height(self) -> float:
        return self._height

    def set_total_width(self, width: float):
        self._width = width

    def distribute_content(self, height: Optional[float] = None):
        pass

    @property
    def distance_to_next_paragraph(self) -> float:
        return self._last_line_spacing


class Paragraph(Container):
    def __init__(
        self,
        width: Optional[float] = None,  # defined by parent container
        align: ParagraphAlignment = ParagraphAlignment.LEFT,
        indent: tuple[float, float, float] = (0, 0, 0),
        line_spacing: float = 1,
        margins: Optional[Sequence[float]] = None,
        tab_stops: Optional[Sequence[TabStop]] = None,
        renderer: Optional[ContentRenderer] = None,
    ):
        super().__init__(width, None, margins, renderer)
        self._align = align
        first, left, right = indent
        self._indent_first = first
        self._indent_left = left
        self._indent_right = right
        self._line_spacing = line_spacing
        self._tab_stops = tab_stops or []

        # contains the raw and not distributed content:
        self._cells: list[Cell] = []

        # contains the final distributed content:
        self._lines: list[AbstractLine] = []

        # space to next paragraph
        self._last_line_spacing = 0.0

    def __iter__(self):
        return iter(self._lines)

    @property
    def distance_to_next_paragraph(self):
        return self._last_line_spacing

    def set_total_width(self, width: float):
        self._content_width = width - self.left_margin - self.right_margin
        if self._content_width < 1e-6:
            raise LayoutError("invalid width, no usable space left")

    def append_content(self, content: Iterable[Cell]):
        self._cells.extend(content)

    def line_width(self, first: bool) -> float:
        indent = self._indent_right
        indent += self._indent_first if first else self._indent_left
        return self.content_width - indent

    def place_content(self):
        x, y = self.final_location()
        x += self.left_margin
        y -= self.top_margin
        first = True
        lines = self._lines
        for line in lines:
            x_final = self._left_border(x, first)
            line.place(x_final, y)
            y -= leading(line.total_height, self._line_spacing)
            first = False

    def _left_border(self, x: float, first: bool) -> float:
        """Apply indentation and paragraph alignment"""
        left_indent = self._indent_first if first else self._indent_left
        return x + left_indent

    def _calculate_content_height(self) -> float:
        """Returns the actual content height determined by the distributed
        lines.
        """
        lines = self._lines
        line_spacing = self._line_spacing
        height = 0.0
        if len(lines):
            last_line = lines[-1]
            height = sum(
                leading(line.total_height, line_spacing) for line in lines[:-1]
            )
            # do not add line spacing after last line!
            last_line_height = last_line.total_height
            self._last_line_spacing = (
                leading(last_line_height, line_spacing) - last_line_height
            )
            height += last_line_height
        return height

    def distribute_content(self, height: Optional[float] = None) -> Optional[Paragraph]:
        """Distribute the raw content into lines. Returns the cells which do
        not fit as a new paragraph.

        Args:
            height: available total height (margins + content), ``None`` for
                unrestricted paragraph height

        """

        def new_line(width: float) -> AbstractLine:
            if align in (ParagraphAlignment.LEFT, ParagraphAlignment.JUSTIFIED):
                indent = self._indent_first if first else self._indent_left
                tab_stops = shift_tab_stops(self._tab_stops, -indent, width)
                return (
                    LeftLine(width, tab_stops)
                    if align == ParagraphAlignment.LEFT
                    else JustifiedLine(width, tab_stops)
                )
            elif align == ParagraphAlignment.RIGHT:
                return RightLine(width)
            elif align == ParagraphAlignment.CENTER:
                return CenterLine(width)
            else:
                raise LayoutError(align)

        cells: list[Cell] = normalize_cells(self._cells)
        cells = group_non_breakable_cells(cells)
        # Delete raw content:
        self._cells.clear()

        align: ParagraphAlignment = self._align
        index: int = 0  # index of current cell
        count: int = len(cells)
        first: bool = True  # is current line the first line?

        # current paragraph height:
        paragraph_height: float = self.top_margin + self.bottom_margin

        # localize enums for core loop optimization:
        # CPython 3.9 access is around 3x faster, no difference for PyPy 3.7!
        FAIL, SUCCESS, FORCED = iter(AppendType)
        while index < count:
            # store index of first unprocessed cell to restore index,
            # if not enough space in line
            undo = index
            line = new_line(self.line_width(first))
            has_tab_support = line.has_tab_support
            while index < count:
                # core loop of paragraph processing and the whole layout engine:
                cell = cells[index]
                if isinstance(cell, Tabulator) and has_tab_support:
                    append_state = line.append_with_tab(
                        # a tabulator cell has always a following cell,
                        # see normalize_cells()!
                        cells[index + 1],
                        cell,
                    )
                    if append_state == SUCCESS:
                        index += 1  # consume tabulator
                    elif not line.has_content:
                        # The tabulator and the following word do no fit into a
                        # line -> ignore the tabulator
                        index += 1
                else:
                    append_state = line.append(cell)
                # state check order by probability:
                if append_state == SUCCESS:
                    index += 1  # consume current cell
                elif append_state == FAIL:
                    break
                elif append_state == FORCED:
                    index += 1  # consume current cell
                    break

            if line.has_content:
                line.remove_line_breaking_space()
                # remove line breaking space:
                if index < count and isinstance(cells[index], Space):
                    index += 1

                line_height = line.total_height
                if (
                    height is not None  # restricted paragraph height
                    and paragraph_height + line_height > height
                ):
                    # Not enough space for the new line:
                    index = undo
                    break
                else:
                    first = False
                    self._lines.append(line)
                    paragraph_height += leading(line_height, self._line_spacing)
        not_all_cells_processed = index < count
        if align == ParagraphAlignment.JUSTIFIED:
            # distribute justified text across the line width,
            # except for the VERY last line:
            end = len(self._lines) if not_all_cells_processed else -1
            for line in self._lines[:end]:
                assert isinstance(line, JustifiedLine)
                line.distribute()

        # Update content height:
        self._content_height = self._calculate_content_height()

        # If not all cells could be processed, put them into a new paragraph
        # and return it to the caller.
        if not_all_cells_processed:
            return self._new_paragraph(cells[index:], first)
        else:
            return None

    def _new_paragraph(self, cells: list[Cell], first: bool) -> Paragraph:
        # First line of the paragraph included?
        indent_first = self._indent_first if first else self._indent_left
        indent = (indent_first, self._indent_left, self._indent_right)
        paragraph = Paragraph(
            self._content_width,
            self._align,
            indent,
            self._line_spacing,
            self._margins,
            self._tab_stops,
            self.renderer,
        )
        paragraph.append_content(cells)
        return paragraph


class Column(Container):
    def __init__(
        self,
        width: float,
        height: Optional[float] = None,
        gutter: float = 0,
        margins: Optional[Sequence[float]] = None,
        renderer: Optional[ContentRenderer] = None,
    ):
        super().__init__(width, height, margins, renderer)
        # spacing between columns
        self._gutter = gutter
        self._paragraphs: list[Paragraph] = []

    def clone_empty(self) -> Column:
        return self.__class__(
            width=self.content_width,
            height=self.content_height,
            gutter=self.gutter,
            margins=(
                self.top_margin,
                self.right_margin,
                self.bottom_margin,
                self.left_margin,
            ),
            renderer=self.renderer,
        )

    def __iter__(self):
        return iter(self._paragraphs)

    def __len__(self):
        return len(self._paragraphs)

    @property
    def content_height(self) -> float:
        """Returns the current content height for flexible columns and the
        max. content height otherwise.
        """
        max_height = self.max_content_height
        if max_height is None:
            return self.used_content_height()
        else:
            return max_height

    def used_content_height(self) -> float:
        paragraphs = self._paragraphs
        height = 0.0
        if paragraphs:
            height = sum(
                p.total_height + p.distance_to_next_paragraph for p in paragraphs[:-1]
            )
            height += paragraphs[-1].total_height
        return height

    @property
    def gutter(self):
        return self._gutter

    @property
    def max_content_height(self) -> Optional[float]:
        return self._content_height

    @property
    def has_free_space(self) -> bool:
        if self.max_content_height is None:  # flexible height column
            return True
        return self.used_content_height() < self.max_content_height

    def place_content(self):
        x, y = self.final_location()
        x += self.left_margin
        y -= self.top_margin
        for p in self._paragraphs:
            p.place(x, y)
            y -= p.total_height + p.distance_to_next_paragraph

    def append_paragraphs(self, paragraphs: Iterable[Paragraph]) -> list[Paragraph]:
        remainder: list[Paragraph] = []
        for paragraph in paragraphs:
            if remainder:
                remainder.append(paragraph)
                continue
            paragraph.set_total_width(self.content_width)
            if self.has_flex_height:
                height = None
            else:
                height = self.max_content_height - self.used_content_height()  # type: ignore
            rest = paragraph.distribute_content(height)
            self._paragraphs.append(paragraph)
            if rest is not None:
                remainder.append(rest)
        return remainder


class Layout(Container):
    def __init__(
        self,
        width: float,
        height: Optional[float] = None,
        margins: Optional[Sequence[float]] = None,
        renderer: Optional[ContentRenderer] = None,
    ):
        super().__init__(width, height, margins, renderer)
        self._reference_column_width = width
        self._current_column = 0
        self._columns: list[Column] = []

    def __iter__(self):
        return iter(self._columns)

    def __len__(self):
        return len(self._columns)

    @property
    def current_column_index(self):
        return self._current_column

    @property
    def content_width(self):
        width = self._content_width
        if self._columns:
            width = self._calculate_content_width()
        return width

    def _calculate_content_width(self) -> float:
        width = sum(c.total_width + c.gutter for c in self._columns[:-1])
        if self._columns:
            width += self._columns[-1].total_width
        return width

    @property
    def content_height(self):
        height = self._content_height
        if self._columns:
            height = self._calculate_content_height()
        elif height is None:
            height = 0
        return height

    def _calculate_content_height(self) -> float:
        return max(c.total_height for c in self._columns)

    def place(
        self,
        x: float = 0,
        y: float = 0,
        align: LayoutAlignment = LayoutAlignment.TOP_LEFT,
    ):
        """Place layout and all sub-entities at the final location, relative
        to the insertion point (x, y) by the alignment defined by the argument
        `align` (IntEnum).

        === ================
        int LayoutAlignment
        === ================
        1   TOP_LEFT
        2   TOP_CENTER
        3   TOP_RIGHT
        4   MIDDLE_LEFT
        5   MIDDLE_CENTER
        6   MIDDLE_RIGHT
        7   BOTTOM_LEFT
        8   BOTTOM_CENTER
        9   BOTTOM_RIGHT
        === ================

        It is possible to add content after calling :meth:`place`, but
        :meth:`place` has to be called again before calling :meth:`render`.

        It is recommended to place the layout at origin (0, 0) and use a
        transformation matrix to move the layout to the final location in
        the target DXF layout.

        """

        width = self.total_width
        height = self.total_height
        left, top = insert_location(align, width, height)
        super().place(x + left, y + top)

    def place_content(self):
        """Place content at the final location."""
        x, y = self.final_location()
        x = x + self.left_margin
        y = y - self.top_margin
        for column in self:
            column.place(x, y)
            x += column.total_width + column.gutter

    def append_column(
        self,
        width: Optional[float] = None,
        height: Optional[float] = None,
        gutter: float = 0,
        margins: Optional[Sequence[float]] = None,
        renderer: Optional[ContentRenderer] = None,
    ) -> Column:
        """Append a new column to the layout."""
        if not width:
            width = self._reference_column_width
        column = Column(
            width, height, gutter=gutter, margins=margins, renderer=renderer
        )
        self._columns.append(column)
        return column

    def append_paragraphs(self, paragraphs: Iterable[Paragraph]):
        remainder = list(paragraphs)
        # 1. fill existing columns:
        columns = self._columns
        while self._current_column < len(columns):
            column = columns[self._current_column]
            remainder = column.append_paragraphs(remainder)
            if len(remainder) == 0:
                return
            self._current_column += 1

        # 2. create additional columns
        while remainder:
            column = self._new_column()
            self._current_column = len(self._columns) - 1
            remainder = column.append_paragraphs(remainder)
            if self._current_column > 100:
                raise LayoutError("Internal error - not enough space!?")

    def _new_column(self) -> Column:
        if len(self._columns) == 0:
            raise LayoutError("no column exist")
        empty = self._columns[-1].clone_empty()
        self._columns.append(empty)
        return empty

    def next_column(self) -> None:
        self._current_column += 1
        if self._current_column >= len(self._columns):
            self._new_column()


def linear_placing(cells: Sequence[Cell], x: float, y: float):
    for cell in cells:
        cell.place(x, y)
        x += cell.total_width


class RigidConnection(ContentCell):
    def __init__(
        self, cells: Optional[Iterable[Cell]] = None, valign=CellAlignment.BOTTOM
    ):
        super().__init__(0, 0, valign=valign)
        self._cells: list[Cell] = list(cells) if cells else []

    def __iter__(self):
        return iter(self._cells)

    @property
    def total_width(self) -> float:
        return sum(cell.total_width for cell in self._cells)

    @property
    def total_height(self) -> float:
        return max(cell.total_height for cell in self._cells)

    def render(self, m: Optional[Matrix44] = None) -> None:
        render_cells(self._cells, m)
        render_text_strokes(self._cells, m)

    def place(self, x: float, y: float):
        super().place(x, y)
        linear_placing(self._cells, x, y)

    def growable_glue(self) -> Iterable[Glue]:
        return (
            cell for cell in self._cells if isinstance(cell, Glue) and cell.can_grow
        )


def group_non_breakable_cells(cells: list[Cell]) -> list[Cell]:
    def append_rigid_content(s: int, e: int):
        _rigid_content = cells[s:e]
        if len(_rigid_content) > 1:
            new_cells.append(RigidConnection(_rigid_content))
        else:
            new_cells.append(_rigid_content[0])

    index = 0
    count = len(cells)
    new_cells = []
    while index < count:
        cell = cells[index]
        if isinstance(cell, _no_break):
            start = index
            index += 1
            while index < count:
                if not isinstance(cells[index], _no_break):
                    append_rigid_content(start, index)
                    break
                index += 1
            if index == count:
                append_rigid_content(start, index)
            else:
                continue
        else:
            new_cells.append(cell)
        index += 1
    return new_cells


def vertical_cell_shift(cell: Cell, group_height: float) -> float:
    dy = 0.0
    if isinstance(cell, ContentCell) and cell.valign != CellAlignment.TOP:
        dy = cell.total_height - group_height
        if cell.valign == CellAlignment.CENTER:
            dy /= 2.0
    return dy


class LineCell(NamedTuple):
    cell: Cell
    offset: float
    locked: bool


class AppendType(enum.IntEnum):
    FAIL = 0
    SUCCESS = 1
    FORCED = 2


class AbstractLine(ContentCell):  # ABC
    has_tab_support = False

    def __init__(self, width: float):
        super().__init__(width=width, height=0, valign=CellAlignment.BOTTOM)
        self._cells: list[LineCell] = []
        self._current_offset: float = 0.0

    def __iter__(self):
        return self.flatten()

    @abc.abstractmethod
    def append(self, cell: Cell) -> AppendType:
        """Append cell to the line content and report SUCCESS or FAIL."""
        pass

    @abc.abstractmethod
    def append_with_tab(self, cell: Cell, tab: Tabulator) -> AppendType:
        """Append cell with preceding tabulator cell to the line content
        and report SUCCESS or FAIL.
        """
        pass

    @property
    def has_content(self):
        return bool(self._cells)

    def place(self, x: float, y: float):
        super().place(x, y)
        group_height = self.total_height
        for line_cell in self._cells:
            cell = line_cell.cell
            cx = x + line_cell.offset
            cy = y + vertical_cell_shift(cell, group_height)
            cell.place(cx, cy)

    @property
    def line_width(self) -> float:
        return self._width

    @property
    def total_width(self) -> float:
        width: float = 0.0
        if len(self._cells):
            last_cell = self._cells[-1]
            width = last_cell.offset + last_cell.cell.total_width
        return width

    @property
    def total_height(self) -> float:
        if len(self._cells):
            return max(c.cell.total_height for c in self._cells)
        return 0.0

    def cells(self) -> Iterable[Cell]:
        """Yield line content including RigidConnections."""
        return [c.cell for c in self._cells]

    def flatten(self) -> Iterable[Cell]:
        """Yield line content with resolved RigidConnections."""
        for cell in self.cells():
            if isinstance(cell, RigidConnection):
                yield from cell
            else:
                yield cell

    def render(self, m: Matrix44 = None) -> None:
        cells = list(self.cells())
        render_cells(cells, m)
        render_text_strokes(cells, m)

    def remove_line_breaking_space(self) -> bool:
        """Remove the last space in the line. Returns True if such a space was
        removed.
        """
        _cells = self._cells
        if _cells and isinstance(_cells[-1].cell, Space):
            _cells.pop()
            return True
        return False


class LeftLine(AbstractLine):
    has_tab_support = True

    def __init__(self, width: float, tab_stops: Optional[Sequence[TabStop]] = None):
        super().__init__(width=width)
        self._tab_stops = tab_stops or []  # tab stops relative to line start

    def _append_line_cell(
        self, cell: Cell, offset: float, locked: bool = False
    ) -> None:
        self._cells.append(LineCell(cell, offset, locked))

    def append(self, cell: Cell) -> AppendType:
        width = cell.total_width
        if self._current_offset + width <= self.line_width:
            self._append_line_cell(cell, self._current_offset)
            self._current_offset += width
            return AppendType.SUCCESS
        if len(self._cells) == 0:
            # single cell is too wide for a line,
            # forced rendering with oversize
            self._append_line_cell(cell, 0)
            return AppendType.FORCED
        return AppendType.FAIL

    def append_with_tab(self, cell: Cell, tab: Tabulator) -> AppendType:
        width = cell.total_width
        pos = self._current_offset
        # does content fit into line:
        if pos + width > self.line_width:
            return AppendType.FAIL

        # next possible tab stop location:
        left_pos = pos
        center_pos = pos + width / 2
        right_pos = pos + width
        tab_stop = self._next_tab_stop(left_pos, center_pos, right_pos)
        if tab_stop is None:  # no tab stop found
            return self._append_unlocked_tab(tab, cell)
        else:
            if tab_stop.kind == TabStopType.LEFT:
                return self._append_left(cell, tab_stop.pos)
            elif tab_stop.kind == TabStopType.CENTER:
                return self._append_center(cell, tab_stop.pos)
            else:
                return self._append_right(cell, tab_stop.pos)

    def _append_unlocked_tab(self, tab, cell) -> AppendType:
        pos0 = self._current_offset
        space = tab.to_space()  # replace tabulator by space
        pos1 = pos0 + space.total_width
        width = cell.total_width
        if pos1 + width <= self.line_width:
            # The following cell is not locked and requires a space in front of it.
            self._append_line_cell(space, pos0)
            self._append_line_cell(cell, pos1)
            self._current_offset = pos1 + width
            return AppendType.SUCCESS
        return AppendType.FAIL

    def _append_left(self, cell, pos) -> AppendType:
        width = cell.total_width
        if pos + width <= self.line_width:
            self._append_line_cell(cell, pos, True)
            self._current_offset = pos + width
            return AppendType.SUCCESS
        return AppendType.FAIL

    def _append_center(self, cell, pos) -> AppendType:
        width2 = cell.total_width / 2
        if self._current_offset + width2 > pos:
            return self.append(cell)
        elif pos + width2 <= self.line_width:
            self._append_line_cell(cell, pos - width2, True)
            self._current_offset = pos + width2
            return AppendType.SUCCESS
        return AppendType.FAIL

    def _append_right(self, cell, pos) -> AppendType:
        width = cell.total_width
        end_of_cell_pos = self._current_offset + width
        if end_of_cell_pos > self.line_width:
            return AppendType.FAIL
        if end_of_cell_pos > pos:
            return self.append(cell)
        self._append_line_cell(cell, pos - width, True)
        self._current_offset = pos
        return AppendType.SUCCESS

    def _next_tab_stop(self, left, center, right):
        for tab in self._tab_stops:
            if tab.kind == TabStopType.LEFT and tab.pos > left:
                return tab
            elif tab.kind == TabStopType.CENTER and tab.pos > center:
                return tab
            elif tab.kind == TabStopType.RIGHT and tab.pos > right:
                return tab
        return None


def content_width(cells: Iterable[Cell]) -> float:
    return sum(cell.total_width for cell in cells)


def growable_cells(cells: Iterable[Cell]) -> list[Glue]:
    growable = []
    for cell in cells:
        if isinstance(cell, Glue) and cell.can_grow:
            growable.append(cell)
        elif isinstance(cell, RigidConnection):
            growable.extend(cell.growable_glue())
    return growable


def update_offsets(cells: list[LineCell], index: int) -> None:
    count = len(cells)
    if count == 0 or index > count:
        return

    last_cell = cells[index - 1]
    offset = last_cell.offset + last_cell.cell.total_width
    while index < count:
        cell = cells[index].cell
        cells[index] = LineCell(cell, offset, False)
        offset += cell.total_width
        index += 1


class JustifiedLine(LeftLine):
    def distribute(self):
        cells = self._cells
        last_locked_cell = self._last_locked_cell()
        if last_locked_cell == len(cells):
            return

        available_space = self._available_space(last_locked_cell)
        cells = [c.cell for c in cells[last_locked_cell + 1 :]]
        modified = False
        while True:
            growable = growable_cells(cells)
            if len(growable) == 0:
                break

            space_to_distribute = available_space - content_width(cells)
            if space_to_distribute <= 1e-9:
                break

            delta = space_to_distribute / len(growable)
            for cell in growable:
                cell.resize(cell.total_width + delta)
            modified = True

        if modified:
            update_offsets(self._cells, last_locked_cell + 1)

    def _end_offset(self, index):
        cell = self._cells[index]
        return cell.offset + cell.cell.total_width

    def _available_space(self, index):
        return self.line_width - self._end_offset(index)

    def _last_locked_cell(self):
        cells = self._cells
        index = len(cells) - 1
        while index > 0:
            if cells[index].locked:
                return index
            index -= 1
        return 0


class NoTabLine(AbstractLine):
    """Base class for lines without tab stop support!"""

    has_tab_support = False

    def append(self, cell: Cell) -> AppendType:
        if isinstance(cell, Tabulator):
            cell = cell.to_space()
        width = cell.total_width
        if self._current_offset + width < self.line_width:
            self._cells.append(LineCell(cell, self._current_offset, False))
            self._current_offset += width
            return AppendType.SUCCESS
        if len(self._cells) == 0:
            # single cell is too wide for a line,
            # forced rendering with oversize
            self._cells.append(LineCell(cell, 0, False))
            return AppendType.FORCED
        return AppendType.FAIL

    def append_with_tab(self, cell: Cell, tab: Tabulator) -> AppendType:
        """No tabulator support!"""
        raise NotImplementedError()

    def place(self, x: float, y: float):
        # shift the line cell:
        super().place(x + self.start_offset(), y)

    @abc.abstractmethod
    def start_offset(self) -> float:
        pass


class CenterLine(NoTabLine):
    """Right aligned lines do not support tab stops!"""

    def start_offset(self) -> float:
        real_width = sum(c.cell.total_width for c in self._cells)
        return (self.line_width - real_width) / 2


class RightLine(NoTabLine):
    """Right aligned lines do not support tab stops!"""

    def start_offset(self) -> float:
        real_width = sum(c.cell.total_width for c in self._cells)
        return self.line_width - real_width


def shift_tab_stops(
    tab_stops: Iterable[TabStop], offset: float, right_border: float
) -> list[TabStop]:
    return [
        tab_stop
        for tab_stop in (TabStop(pos + offset, kind) for pos, kind in tab_stops)
        if 0 < tab_stop.pos <= right_border
    ]
