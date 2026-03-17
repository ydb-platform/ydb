# Copyright (c) 2010-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
)
from copy import deepcopy

from ezdxf.lldxf import const
from ezdxf.enums import (
    MTextEntityAlignment,
    MAP_MTEXT_ALIGN_TO_FLAGS,
)
from ezdxf.addons import MTextSurrogate
from ezdxf.math import UVec, Vec2

if TYPE_CHECKING:
    from ezdxf.layouts import BlockLayout
    from ezdxf.eztypes import GenericLayoutType

DEFAULT_TABLE_BG_LAYER = "TABLEBACKGROUND"
DEFAULT_TABLE_FG_LAYER = "TABLECONTENT"
DEFAULT_TABLE_GRID_LAYER = "TABLEGRID"
DEFAULT_TEXT_STYLE = "STANDARD"
DEFAULT_CELL_HEIGHT = 1.0
DEFAULT_CELL_WIDTH = 2.5
DEFAULT_CELL_CHAR_HEIGHT = 0.7
DEFAULT_CELL_LINE_SPACING = 1.5
DEFAULT_CELL_X_SCALE = 1.0
DEFAULT_CELL_Y_SCALE = 1.0
DEFAULT_CELL_TEXTCOLOR = const.BYLAYER
DEFAULT_CELL_BG_COLOR = None
DEFAULT_CELL_X_MARGIN = 0.1
DEFAULT_CELL_Y_MARGIN = 0.1
DEFAULT_BORDER_COLOR = 5
DEFAULT_BORDER_LINETYPE = "BYLAYER"
DEFAULT_BORDER_STATUS = True
DEFAULT_BORDER_PRIORITY = 50


T = TypeVar("T", bound="Cell")


class TablePainter:
    """The TablePainter class renders tables build from DXF primitives.

    The TablePainter instance contains all the data cells.

    Args:
        insert: insert location as or :class:`~ezdxf.math.UVec`
        nrows: row count
        ncols: column count
        cell_width: default cell width in drawing units
        cell_height: default cell height in drawing units
        default_grid: draw a grid of solid lines if ``True``, otherwise
            draw only explicit defined borders, the default grid has a
            priority of 50.

    """

    def __init__(
        self,
        insert: UVec,
        nrows: int,
        ncols: int,
        cell_width=DEFAULT_CELL_WIDTH,
        cell_height=DEFAULT_CELL_HEIGHT,
        default_grid=True,
    ):
        self.insert = Vec2(insert)
        self.nrows: int = nrows
        self.ncols: int = ncols
        self.row_heights: list[float] = [cell_height] * nrows
        self.col_widths: list[float] = [cell_width] * ncols
        self.bg_layer_name: str = DEFAULT_TABLE_BG_LAYER
        self.fg_layer_name: str = DEFAULT_TABLE_FG_LAYER
        self.grid_layer_name: str = DEFAULT_TABLE_GRID_LAYER
        self.styles: dict[str, CellStyle] = {"default": CellStyle()}
        if not default_grid:
            default_style = self.get_cell_style("default")
            default_style.set_border_status(False, False, False, False)

        self._cells: dict[tuple[int, int], Cell] = {}  # data cells
        self.frames: list[Frame] = []  # border frame objects
        self.empty_cell = Cell(self)  # represents all empty cells

    def set_col_width(self, index: int, value: float):
        """Set column width in drawing units of the given column index.

        Args:
            index: zero based column index
            value: new column width in drawing units

        """
        self.col_widths[index] = float(value)

    def set_row_height(self, index: int, value: float):
        """Set row height in drawing units of the given row index.

        Args:
            index: zero based row index
            value: new row height in drawing units
        """
        self.row_heights[index] = float(value)

    def text_cell(
        self,
        row: int,
        col: int,
        text: str,
        span: tuple[int, int] = (1, 1),
        style="default",
    ) -> TextCell:
        """Factory method to create a new text cell at location (row, col),
        with `text` as content, the `text` can be a line breaks ``'\\n'``.
        The final cell can spread over several cells defined by the argument
        `span`.

        """
        cell = TextCell(self, text, style=style, span=span)
        return self.set_cell(row, col, cell)

    def block_cell(
        self,
        row: int,
        col: int,
        blockdef: BlockLayout,
        span: tuple[int, int] = (1, 1),
        attribs=None,
        style="default",
    ) -> BlockCell:
        """Factory method to Create a new block cell at position (row, col).

        Content is a block reference inserted by an INSERT entity,
        attributes will be added if the block definition contains ATTDEF.
        Assignments are defined by attribs-key to attdef-tag association.

        Example: attribs = {'num': 1} if an ATTDEF with tag=='num' in
        the block definition exists, an attrib with text=str(1) will be
        created and added to the insert entity.

        The cell spans over 'span' cells and has the cell style with the
        name 'style'.
        """
        if attribs is None:
            attribs = {}
        cell = BlockCell(
            self, blockdef, style=style, attribs=attribs, span=span
        )
        return self.set_cell(row, col, cell)

    @property
    def table_width(self) -> float:
        """Returns the total table width."""
        return sum(self.col_widths)

    @property
    def table_height(self) -> float:
        """Returns the total table height."""
        return sum(self.row_heights)

    def set_cell(self, row: int, col: int, cell: T) -> T:
        """Insert a cell at position (row, col)."""
        row, col = self.validate_index(row, col)
        self._cells[row, col] = cell
        return cell

    def get_cell(self, row: int, col: int) -> Cell:
        """Get cell at location (row, col)."""
        row, col = self.validate_index(row, col)
        try:
            return self._cells[row, col]
        except KeyError:
            return self.empty_cell  # empty cell with default style

    def validate_index(self, row: int, col: int) -> tuple[int, int]:
        row = int(row)
        col = int(col)
        if row < 0 or row >= self.nrows or col < 0 or col >= self.ncols:
            raise IndexError("cell index out of range")
        return row, col

    def frame(
        self,
        row: int,
        col: int,
        width: int = 1,
        height: int = 1,
        style="default",
    ) -> Frame:
        """Creates a frame around the give cell area, starting at (row, col) and
        covering `width` columns and `height` rows. The `style` argument is the
        name of a :class:`CellStyle`.
        """
        frame = Frame(self, pos=(row, col), span=(height, width), style=style)
        self.frames.append(frame)
        return frame

    def new_cell_style(self, name: str, **kwargs) -> CellStyle:
        """Factory method to create a new :class:`CellStyle` object, overwrites
        an already existing cell style.

        Args:
            name: style name as string
            kwargs: see attributes of class :class:`CellStyle`

        """
        assert (
            isinstance(name, str) and name != ""
        ), "name has to be a non-empty string"
        style: CellStyle = deepcopy(self.get_cell_style("default"))
        style.update(kwargs)
        self.styles[name] = style
        return style

    @staticmethod
    def new_border_style(
        color: int = const.BYLAYER,
        status=True,
        priority: int = 100,
        linetype: str = "BYLAYER",
        lineweight: int = const.LINEWEIGHT_BYLAYER,
    ) -> BorderStyle:
        """Factory method to create a new border style.

        Args:
            status: ``True`` for visible, ``False`` for invisible
            color: :ref:`ACI`
            linetype: linetype name, default is "BYLAYER"
            lineweight: lineweight as int, default is by layer
            priority: drawing priority, higher priorities cover lower priorities

        """
        border_style = BorderStyle()
        border_style.color = color
        border_style.linetype = linetype
        border_style.lineweight = lineweight
        border_style.status = status
        border_style.priority = priority
        return border_style

    def get_cell_style(self, name: str) -> CellStyle:
        """Get cell style by name."""
        return self.styles[name]

    def iter_visible_cells(
        self, visibility_map: VisibilityMap
    ) -> Iterator[tuple[int, int, Cell]]:
        """Iterate over all visible cells"""
        return (
            (row, col, self.get_cell(row, col)) for row, col in visibility_map
        )

    def render(self, layout: GenericLayoutType, insert: Optional[UVec] = None):
        """Render table to layout."""
        insert_backup = self.insert
        if insert is not None:
            self.insert = Vec2(insert)
        visibility_map = VisibilityMap(self)
        grid = Grid(self)
        for row, col, cell in self.iter_visible_cells(visibility_map):
            grid.render_cell_background(layout, row, col, cell)
            grid.render_cell_content(layout, row, col, cell)
        grid.render_lines(layout, visibility_map)
        self.insert = insert_backup


class VisibilityMap:
    """Stores the visibility of the table cells."""

    def __init__(self, table: TablePainter):
        """Create the visibility map for table."""
        self.table: TablePainter = table
        self._hidden_cells: set[tuple[int, int]] = set()
        self._create_visibility_map()

    def _create_visibility_map(self):
        """Set visibility for all existing cells."""
        for row, col in iter(self):
            cell = self.table.get_cell(row, col)
            self._set_span_visibility(row, col, cell.span)

    def _set_span_visibility(self, row: int, col: int, span: tuple[int, int]):
        """Set the visibility of the given cell.

        The cell itself is visible, all other cells in the span-range
        (tuple: width, height) are invisible, they are covered by the
        main cell (row, col).
        """

        if span != (1, 1):
            nrows, ncols = span
            for rowx in range(nrows):
                for colx in range(ncols):
                    # switch all cells in span range to invisible
                    self.hide(row + rowx, col + colx)
        # switch content cell visible
        self.show(row, col)

    def show(self, row: int, col: int):
        """Show cell (row, col)."""
        try:
            self._hidden_cells.remove((row, col))
        except KeyError:
            pass

    def hide(self, row: int, col: int) -> None:
        """Hide cell (row, col)."""
        self._hidden_cells.add((row, col))

    def iter_all_cells(self) -> Iterator[tuple[int, int]]:
        """Iterate over all cell indices, yields (row, col) tuples."""
        for row in range(self.table.nrows):
            for col in range(self.table.ncols):
                yield row, col

    def is_visible_cell(self, row: int, col: int) -> bool:
        """True if cell (row, col)  is visible, else False."""
        return (row, col) not in self._hidden_cells

    def __iter__(self) -> Iterator[tuple[int, int]]:
        """Iterate over all visible cells."""
        return (
            (row, col)
            for (row, col) in self.iter_all_cells()
            if self.is_visible_cell(row, col)
        )


class CellStyle:
    """Cell style object.

    .. important::

        Always instantiate new styles by the factory method:
        :meth:`TablePainter.new_cell_style`

    """

    def __init__(self, data: Optional[dict[str, Any]] = None):
        # text style is ignored by block cells
        self.text_style = "STANDARD"
        # text height in drawing units, ignored by block cells
        self.char_height = DEFAULT_CELL_CHAR_HEIGHT
        # line spacing in percent = char_height * line_spacing, ignored by block cells
        self.line_spacing = DEFAULT_CELL_LINE_SPACING
        # text stretching factor (width factor) or block reference x-scaling factor
        self.scale_x = DEFAULT_CELL_X_SCALE
        # block reference y-axis scaling factor, ignored by text cells
        self.scale_y = DEFAULT_CELL_Y_SCALE
        # dxf color index, ignored by block cells
        self.text_color = DEFAULT_CELL_TEXTCOLOR
        # text or block rotation in degrees
        self.rotation = 0.0
        # Letters are stacked top-to-bottom, but not rotated
        self.stacked = False
        # text and block alignment, see ezdxf.enums.MTextEntityAlignment
        self.align = MTextEntityAlignment.TOP_CENTER
        # left and right cell margin in drawing units
        self.margin_x = DEFAULT_CELL_X_MARGIN
        # top and bottom cell margin in drawing units
        self.margin_y = DEFAULT_CELL_Y_MARGIN
        # background color, dxf color index, ignored by block cells
        self.bg_color = DEFAULT_CELL_BG_COLOR
        # left border style
        self.left = BorderStyle()
        # top border style
        self.top = BorderStyle()
        # right border style
        self.right = BorderStyle()
        # bottom border style
        self.bottom = BorderStyle()
        if data:
            self.update(data)

    def __getitem__(self, k: str) -> Any:
        return self.__dict__[k]

    def __setitem__(self, k: str, v: Any):
        if k in self.__dict__:
            self.__dict__.__setitem__(k, v)
        else:
            raise KeyError(f"invalid attribute name: {k}")

    def update(self, data: dict[str, Any]):
        for k, v in data.items():
            self.__setitem__(k, v)
        assert isinstance(
            self.align, MTextEntityAlignment
        ), "enum ezdxf.enums.MTextEntityAlignment for text alignments required"

    def set_border_status(self, left=True, right=True, top=True, bottom=True):
        """Set status of all cell borders at once."""
        self.left.status = left
        self.right.status = right
        self.top.status = top
        self.bottom.status = bottom

    def set_border_style(
        self, style: BorderStyle, left=True, right=True, top=True, bottom=True
    ):
        """Set border styles of all cell borders at once."""
        for border, status in (
            ("left", left),
            ("right", right),
            ("top", top),
            ("bottom", bottom),
        ):
            if status:
                self[border] = style

    @staticmethod
    def get_default_border_style() -> BorderStyle:
        return BorderStyle()

    def get_text_align_flags(self) -> tuple[int, int]:
        return MAP_MTEXT_ALIGN_TO_FLAGS[self.align]


class BorderStyle:
    """Border style class.

    .. important::

        Always instantiate new border styles by the factory method:
        :meth:`TablePainter.new_border_style`

    """

    def __init__(
        self,
        status: bool = DEFAULT_BORDER_STATUS,
        color: int = DEFAULT_BORDER_COLOR,
        linetype: str = DEFAULT_BORDER_LINETYPE,
        lineweight=const.LINEWEIGHT_BYLAYER,
        priority: int = DEFAULT_BORDER_PRIORITY,
    ):
        # border status, True for visible, False for hidden
        self.status = status
        # ACI
        self.color = color
        # linetype name, BYLAYER if None
        self.linetype = linetype
        # lineweight
        self.lineweight = lineweight
        # drawing priority, higher values cover lower values
        self.priority = priority


class Grid:
    """Grid contains the graphical representation of the table."""

    def __init__(self, table: TablePainter):
        self.table: TablePainter = table
        # contains the x-axis coords of the grid lines between the data columns.
        self.col_pos: list[float] = self._calc_col_pos()
        # contains the y-axis coords of the grid lines between the data rows.
        self.row_pos: list[float] = self._calc_row_pos()

        # _x_borders contains the horizontal border elements, list of border styles
        # get index with _border_index(row, col), which means the border element
        # above row, col, and row-indices are [0 .. nrows+1], nrows+1 for the
        # grid line below the last row; list contains only the border style with
        # the highest priority.
        self._x_borders: list[BorderStyle] = []  # created in _init_borders

        # _y_borders: same as _x_borders but for the vertical borders,
        # col-indices are [0 .. ncols+1], ncols+1 for the last grid line right
        # of the last column
        self._y_borders: list[BorderStyle] = []  # created in _init_borders
        # border style to delete borders inside of merged cells
        self.no_border = BorderStyle(
            status=False, priority=999, linetype="BYLAYER", color=0
        )

    def _init_borders(self, x_border: BorderStyle, y_border: BorderStyle):
        """Init the _hborders with  <hborder> and _vborders with <vborder>."""
        # <border_count> has more elements than necessary, but it unifies the
        # index calculation for _vborders and _hborders.
        # exact values are:
        # x_border_count = ncols * (nrows+1), hindex = ncols * <row> + <col>
        # y_border_count = nrows * (ncols+1), vindex = (ncols+1) * <row> + <col>
        border_count: int = (self.table.nrows + 1) * (self.table.ncols + 1)
        self._x_borders = [x_border] * border_count
        self._y_borders = [y_border] * border_count

    def _border_index(self, row: int, col: int) -> int:
        """Calculate linear index for border arrays _x_borders and _y_borders."""
        return row * (self.table.ncols + 1) + col

    def set_x_border(self, row: int, col: int, border_style: BorderStyle):
        """Set <border_style> for the horizontal border element above
        <row>, <col>.
        """
        return self._set_border_style(self._x_borders, row, col, border_style)

    def set_y_border(self, row: int, col: int, border_style: BorderStyle):
        """Set <border_style> for the vertical border element left of
        <row>, <col>.
        """
        return self._set_border_style(self._y_borders, row, col, border_style)

    def _set_border_style(
        self,
        borders: list[BorderStyle],
        row: int,
        col: int,
        border_style: BorderStyle,
    ):
        """Set <border_style> for <row>, <col> in <borders>."""
        border_index = self._border_index(row, col)
        actual_borderstyle = borders[border_index]
        if border_style.priority >= actual_borderstyle.priority:
            borders[border_index] = border_style

    def get_x_border(self, row: int, col: int) -> BorderStyle:
        """Get the horizontal border element above <row>, <col>.
        Last grid line (below <nrows>) is the element above of <nrows+1>.
        """
        return self._get_border(self._x_borders, row, col)

    def get_y_border(self, row: int, col: int) -> BorderStyle:
        """Get the vertical border element left of <row>, <col>.
        Last grid line (right of <ncols>) is the element left of <ncols+1>.
        """
        return self._get_border(self._y_borders, row, col)

    def _get_border(
        self, borders: list[BorderStyle], row: int, col: int
    ) -> BorderStyle:
        """Get border element at <row>, <col> from <borders>."""
        return borders[self._border_index(row, col)]

    def _calc_col_pos(self) -> list[float]:
        """Calculate the x-axis coords of the grid lines between the columns."""
        col_pos: list[float] = []
        start_x: float = self.table.insert.x
        sum_fields(start_x, self.table.col_widths, col_pos.append)
        return col_pos

    def _calc_row_pos(self) -> list[float]:
        """Calculate the y-axis coords of the grid lines between the rows."""
        row_pos: list[float] = []
        start_y: float = self.table.insert.y
        sum_fields(start_y, self.table.row_heights, row_pos.append, -1.0)
        return row_pos

    def cell_coords(
        self, row: int, col: int, span: tuple[int, int]
    ) -> tuple[float, float, float, float]:
        """Get the coordinates of the cell <row>,<col> as absolute drawing units.

        :return: a tuple (left, right, top, bottom)
        """
        top = self.row_pos[row]
        bottom = self.row_pos[row + span[0]]
        left = self.col_pos[col]
        right = self.col_pos[col + span[1]]
        return left, right, top, bottom

    def render_cell_background(
        self, layout: GenericLayoutType, row: int, col: int, cell: Cell
    ):
        """Render the cell background for (row, col) as SOLID entity."""
        style = cell.style
        if style.bg_color is None:
            return
        # get cell coords in absolute drawing units
        left, right, top, bottom = self.cell_coords(row, col, cell.span)
        layout.add_solid(
            points=((left, top), (left, bottom), (right, top), (right, bottom)),
            dxfattribs={
                "color": style.bg_color,
                "layer": self.table.bg_layer_name,
            },
        )

    def render_cell_content(
        self, layout: GenericLayoutType, row: int, col: int, cell: Cell
    ):
        """Render the cell content for <row>,<col> into layout object."""
        # get cell coords in absolute drawing units
        coords = self.cell_coords(row, col, cell.span)
        cell.render(layout, coords, self.table.fg_layer_name)

    def render_lines(self, layout: GenericLayoutType, vm: VisibilityMap):
        """Render all grid lines into layout object."""
        # Init borders with default_style top- and left border.
        default_style = self.table.get_cell_style("default")
        x_border = default_style.top
        y_border = default_style.left
        self._init_borders(x_border, y_border)
        self._set_frames(self.table.frames)
        self._set_borders(self.table.iter_visible_cells(vm))
        self._render_borders(layout, self.table)

    def _set_borders(self, visible_cells: Iterable[tuple[int, int, Cell]]):
        """Set borders of the visible cells."""
        for row, col, cell in visible_cells:
            bottom_row = row + cell.span[0]
            right_col = col + cell.span[1]
            self._set_rect_borders(row, bottom_row, col, right_col, cell.style)
            self._set_inner_borders(
                row, bottom_row, col, right_col, self.no_border
            )

    def _set_inner_borders(
        self,
        top_row: int,
        bottom_row: int,
        left_col: int,
        right_col: int,
        border_style: BorderStyle,
    ):
        """Set `border_style` to the inner borders of the rectangle (top_row,
        bottom_row, ...)
        """
        if bottom_row - top_row > 1:
            for col in range(left_col, right_col):
                for row in range(top_row + 1, bottom_row):
                    self.set_x_border(row, col, border_style)
        if right_col - left_col > 1:
            for row in range(top_row, bottom_row):
                for col in range(left_col + 1, right_col):
                    self.set_y_border(row, col, border_style)

    def _set_rect_borders(
        self,
        top_row: int,
        bottom_row: int,
        left_col: int,
        right_col: int,
        style: CellStyle,
    ):
        """Set border `style` to the rectangle (top_row, bottom_row, ...)

        The values describing the grid lines between the cells, see doc-strings
        for methods set_x_border() and set_y_border() and see comments for
        self._x_borders and self._y_borders.
        """
        for col in range(left_col, right_col):
            self.set_x_border(top_row, col, style.top)
            self.set_x_border(bottom_row, col, style.bottom)
        for row in range(top_row, bottom_row):
            self.set_y_border(row, left_col, style.left)
            self.set_y_border(row, right_col, style.right)

    def _set_frames(self, frames: Iterable[Frame]):
        """Set borders for all defined frames."""
        for frame in frames:
            top_row = frame.pos[0]
            left_col = frame.pos[1]
            bottom_row = top_row + frame.span[0]
            right_col = left_col + frame.span[1]
            self._set_rect_borders(
                top_row, bottom_row, left_col, right_col, frame.style
            )

    def _render_borders(self, layout: GenericLayoutType, table: TablePainter):
        """Render the grid lines as LINE entities into layout object."""

        def render_line(start: Vec2, end: Vec2, style: BorderStyle):
            """Render the LINE entity into layout object."""
            if style.status:
                layout.add_line(
                    start=start,
                    end=end,
                    dxfattribs={
                        "layer": layer,
                        "color": style.color,
                        "linetype": style.linetype,
                        "lineweight": style.lineweight,
                    },
                )

        def render_x_borders():
            """Draw the horizontal grid lines."""
            for row in range(table.nrows + 1):
                y = self.row_pos[row]
                for col in range(table.ncols):
                    left = self.col_pos[col]
                    right = self.col_pos[col + 1]
                    style = self.get_x_border(row, col)
                    render_line(Vec2(left, y), Vec2(right, y), style)

        def render_y_borders():
            """Draw the vertical grid lines."""
            for col in range(table.ncols + 1):
                x = self.col_pos[col]
                for row in range(table.nrows):
                    top = self.row_pos[row]
                    bottom = self.row_pos[row + 1]
                    style = self.get_y_border(row, col)
                    render_line(Vec2(x, top), Vec2(x, bottom), style)

        layer = table.grid_layer_name
        render_x_borders()
        render_y_borders()


class Frame:
    """Represent a rectangle cell area enclosed by borderlines.

    Args:
         table: the assigned data table
         pos: tuple (row, col), border goes left and top of pos
         span: count of cells that Frame covers, border goes right and below of this cells
         style: style name as string
    """

    def __init__(
        self,
        table: TablePainter,
        pos: tuple[int, int] = (0, 0),
        span: tuple[int, int] = (1, 1),
        style="default",
    ):
        self.table = table
        self.pos = pos
        self.span = span
        self.stylename = style

    @property
    def style(self) -> CellStyle:
        return self.table.get_cell_style(self.stylename)


class Cell:
    """Base class for table cells.

    Args:
        table: assigned data table
        style: style name as string
        span: tuple(spanrows, spancols), count of cells that cell covers

    A cell doesn't know its own position in the data table, because a cell can
    be used multiple times in the same or in different tables, therefore the
    cell itself can not determine if the cell-range reaches beyond the table
    borders.
    """

    def __init__(
        self,
        table: TablePainter,
        style="default",
        span: tuple[int, int] = (1, 1),
    ):
        self.table = table
        self.stylename = style
        # span values has to be >= 1
        self.span = span

    @property
    def span(self) -> tuple[int, int]:
        """Get/set table span parameters."""
        return self._span

    @span.setter
    def span(self, value: tuple[int, int]):
        """Ensures that span values are >= 1 in each direction."""
        self._span = (max(1, value[0]), max(1, value[1]))

    @property
    def style(self) -> CellStyle:
        """Returns the associated :class:`CellStyle`."""
        return self.table.get_cell_style(self.stylename)

    def render(
        self, layout: GenericLayoutType, coords: Sequence[float], layer: str
    ):
        """Renders the cell content into the given `layout`."""
        pass

    def get_workspace_coords(self, coords: Sequence[float]) -> Sequence[float]:
        """Reduces the cell-coords about the margin_x and the margin_y values."""
        margin_x = self.style.margin_x
        margin_y = self.style.margin_y
        return (
            coords[0] + margin_x,  # left
            coords[1] - margin_x,  # right
            coords[2] - margin_y,  # top
            coords[3] + margin_y,  # bottom
        )


CustomCell = Cell


class TextCell(Cell):
    """Implements a cell type containing a multi-line text. Uses the
    :class:`~ezdxf.addons.MTextSurrogate` add-on to render the multi-line
    text, therefore the content of these cells is compatible to DXF R12.

    Args:
        table: assigned data table
        text: multi line text, lines separated by the new line character ``"\\n"``
        style: cell style name as string
        span: tuple(rows, cols) area of cells to cover

    """

    def __init__(
        self,
        table: TablePainter,
        text: str,
        style="default",
        span: tuple[int, int] = (1, 1),
    ):
        super(TextCell, self).__init__(table, style, span)
        self.text = text

    def render(
        self, layout: GenericLayoutType, coords: Sequence[float], layer: str
    ):
        """Text cell.

        Args:
            layout: target layout
            coords: tuple of border-coordinates: left, right, top, bottom
            layer: target layer name as string

        """
        if not len(self.text):
            return

        left, right, top, bottom = self.get_workspace_coords(coords)
        style = self.style
        h_align, v_align = style.get_text_align_flags()
        rotated = self.style.rotation
        text = self.text
        if style.stacked:
            rotated = 0.0
            text = "\n".join((char for char in self.text.replace("\n", " ")))
        xpos = (left, float(left + right) / 2.0, right)[h_align]
        ypos = (bottom, float(bottom + top) / 2.0, top)[v_align - 1]
        mtext = MTextSurrogate(
            text,
            (xpos, ypos),
            line_spacing=self.style.line_spacing,
            style=self.style.text_style,
            char_height=self.style.char_height,
            rotation=rotated,
            width_factor=self.style.scale_x,
            align=style.align,
            color=self.style.text_color,
            layer=layer,
        )
        mtext.render(layout)


class BlockCell(Cell):
    """Implements a cell type containing a block reference.

    Args:
        table: table object
        blockdef: :class:`ezdxf.layouts.BlockLayout` instance
        attribs: BLOCK attributes as (tag, value) dictionary
        style: cell style name as string
        span: tuple(rows, cols) area of cells to cover

    """

    def __init__(
        self,
        table: TablePainter,
        blockdef: BlockLayout,
        style="default",
        attribs=None,
        span: tuple[int, int] = (1, 1),
    ):
        if attribs is None:
            attribs = {}
        super(BlockCell, self).__init__(table, style, span)
        self.block_name = blockdef.name  # dxf block name!
        self.attribs = attribs

    def render(
        self, layout: GenericLayoutType, coords: Sequence[float], layer: str
    ):
        """Create the cell content as INSERT-entity with trailing ATTRIB-Entities.

        Args:
            layout: target layout
            coords: tuple of border-coordinates : left, right, top, bottom
            layer: target layer name as string

        """
        left, right, top, bottom = self.get_workspace_coords(coords)
        style = self.style
        h_align, v_align = style.get_text_align_flags()
        xpos = (left, float(left + right) / 2.0, right)[h_align]
        ypos = (bottom, float(bottom + top) / 2.0, top)[v_align - 1]
        layout.add_auto_blockref(
            name=self.block_name,
            insert=(xpos, ypos),
            values=self.attribs,
            dxfattribs={
                "xscale": style.scale_x,
                "yscale": style.scale_y,
                "rotation": style.rotation,
                "layer": layer,
            },
        )


def sum_fields(
    start_value: float,
    fields: list[float],
    append: Callable[[float], None],
    sign: float = 1.0,
):
    """Adds step-by-step the fields-values, starting with <start_value>,
    and appends the resulting values to another object with the
    append-method.
    """
    position = start_value
    append(position)
    for element in fields:
        position += element * sign
        append(position)
