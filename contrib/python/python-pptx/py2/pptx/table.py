# encoding: utf-8

"""Table-related objects such as Table and Cell."""

from pptx.compat import is_integer
from pptx.dml.fill import FillFormat
from pptx.oxml.table import TcRange
from pptx.shapes import Subshape
from pptx.text.text import TextFrame
from pptx.util import lazyproperty


class Table(object):
    """A DrawingML table object.

    Not intended to be constructed directly, use
    :meth:`.Slide.shapes.add_table` to add a table to a slide.
    """

    def __init__(self, tbl, graphic_frame):
        super(Table, self).__init__()
        self._tbl = tbl
        self._graphic_frame = graphic_frame

    def cell(self, row_idx, col_idx):
        """Return cell at *row_idx*, *col_idx*.

        Return value is an instance of |_Cell|. *row_idx* and *col_idx* are
        zero-based, e.g. cell(0, 0) is the top, left cell in the table.
        """
        return _Cell(self._tbl.tc(row_idx, col_idx), self)

    @lazyproperty
    def columns(self):
        """|_ColumnCollection| instance for this table.

        Provides access to |_Column| objects representing the table's columns. |_Column|
        objects are accessed using list notation, e.g. ``col = tbl.columns[0]``.
        """
        return _ColumnCollection(self._tbl, self)

    @property
    def first_col(self):
        """
        Read/write boolean property which, when true, indicates the first
        column should be formatted differently, as for a side-heading column
        at the far left of the table.
        """
        return self._tbl.firstCol

    @first_col.setter
    def first_col(self, value):
        self._tbl.firstCol = value

    @property
    def first_row(self):
        """
        Read/write boolean property which, when true, indicates the first
        row should be formatted differently, e.g. for column headings.
        """
        return self._tbl.firstRow

    @first_row.setter
    def first_row(self, value):
        self._tbl.firstRow = value

    @property
    def horz_banding(self):
        """
        Read/write boolean property which, when true, indicates the rows of
        the table should appear with alternating shading.
        """
        return self._tbl.bandRow

    @horz_banding.setter
    def horz_banding(self, value):
        self._tbl.bandRow = value

    def iter_cells(self):
        """Generate _Cell object for each cell in this table.

        Each grid cell is generated in left-to-right, top-to-bottom order.
        """
        return (_Cell(tc, self) for tc in self._tbl.iter_tcs())

    @property
    def last_col(self):
        """
        Read/write boolean property which, when true, indicates the last
        column should be formatted differently, as for a row totals column at
        the far right of the table.
        """
        return self._tbl.lastCol

    @last_col.setter
    def last_col(self, value):
        self._tbl.lastCol = value

    @property
    def last_row(self):
        """
        Read/write boolean property which, when true, indicates the last
        row should be formatted differently, as for a totals row at the
        bottom of the table.
        """
        return self._tbl.lastRow

    @last_row.setter
    def last_row(self, value):
        self._tbl.lastRow = value

    def notify_height_changed(self):
        """
        Called by a row when its height changes, triggering the graphic frame
        to recalculate its total height (as the sum of the row heights).
        """
        new_table_height = sum([row.height for row in self.rows])
        self._graphic_frame.height = new_table_height

    def notify_width_changed(self):
        """
        Called by a column when its width changes, triggering the graphic
        frame to recalculate its total width (as the sum of the column
        widths).
        """
        new_table_width = sum([col.width for col in self.columns])
        self._graphic_frame.width = new_table_width

    @property
    def part(self):
        """
        The package part containing this table.
        """
        return self._graphic_frame.part

    @lazyproperty
    def rows(self):
        """|_RowCollection| instance for this table.

        Provides access to |_Row| objects representing the table's rows. |_Row| objects
        are accessed using list notation, e.g. ``col = tbl.rows[0]``.
        """
        return _RowCollection(self._tbl, self)

    @property
    def vert_banding(self):
        """
        Read/write boolean property which, when true, indicates the columns
        of the table should appear with alternating shading.
        """
        return self._tbl.bandCol

    @vert_banding.setter
    def vert_banding(self, value):
        self._tbl.bandCol = value


class _Cell(Subshape):
    """Table cell"""

    def __init__(self, tc, parent):
        super(_Cell, self).__init__(parent)
        self._tc = tc

    def __eq__(self, other):
        """|True| if this object proxies the same element as *other*.

        Equality for proxy objects is defined as referring to the same XML
        element, whether or not they are the same proxy object instance.
        """
        if not isinstance(other, type(self)):
            return False
        return self._tc is other._tc

    def __ne__(self, other):
        if not isinstance(other, type(self)):
            return True
        return self._tc is not other._tc

    @lazyproperty
    def fill(self):
        """
        |FillFormat| instance for this cell, providing access to fill
        properties such as foreground color.
        """
        tcPr = self._tc.get_or_add_tcPr()
        return FillFormat.from_fill_parent(tcPr)

    @property
    def is_merge_origin(self):
        """True if this cell is the top-left grid cell in a merged cell."""
        return self._tc.is_merge_origin

    @property
    def is_spanned(self):
        """True if this cell is spanned by a merge-origin cell.

        A merge-origin cell "spans" the other grid cells in its merge range,
        consuming their area and "shadowing" the spanned grid cells.

        Note this value is |False| for a merge-origin cell. A merge-origin
        cell spans other grid cells, but is not itself a spanned cell.
        """
        return self._tc.is_spanned

    @property
    def margin_left(self):
        """
        Read/write integer value of left margin of cell as a |Length| value
        object. If assigned |None|, the default value is used, 0.1 inches for
        left and right margins and 0.05 inches for top and bottom.
        """
        return self._tc.marL

    @margin_left.setter
    def margin_left(self, margin_left):
        self._validate_margin_value(margin_left)
        self._tc.marL = margin_left

    @property
    def margin_right(self):
        """
        Right margin of cell.
        """
        return self._tc.marR

    @margin_right.setter
    def margin_right(self, margin_right):
        self._validate_margin_value(margin_right)
        self._tc.marR = margin_right

    @property
    def margin_top(self):
        """
        Top margin of cell.
        """
        return self._tc.marT

    @margin_top.setter
    def margin_top(self, margin_top):
        self._validate_margin_value(margin_top)
        self._tc.marT = margin_top

    @property
    def margin_bottom(self):
        """
        Bottom margin of cell.
        """
        return self._tc.marB

    @margin_bottom.setter
    def margin_bottom(self, margin_bottom):
        self._validate_margin_value(margin_bottom)
        self._tc.marB = margin_bottom

    def merge(self, other_cell):
        """Create merged cell from this cell to *other_cell*.

        This cell and *other_cell* specify opposite corners of the merged
        cell range. Either diagonal of the cell region may be specified in
        either order, e.g. self=bottom-right, other_cell=top-left, etc.

        Raises |ValueError| if the specified range already contains merged
        cells anywhere within its extents or if *other_cell* is not in the
        same table as *self*.
        """
        tc_range = TcRange(self._tc, other_cell._tc)

        if not tc_range.in_same_table:
            raise ValueError("other_cell from different table")
        if tc_range.contains_merged_cell:
            raise ValueError("range contains one or more merged cells")

        tc_range.move_content_to_origin()

        row_count, col_count = tc_range.dimensions

        for tc in tc_range.iter_top_row_tcs():
            tc.rowSpan = row_count
        for tc in tc_range.iter_left_col_tcs():
            tc.gridSpan = col_count
        for tc in tc_range.iter_except_left_col_tcs():
            tc.hMerge = True
        for tc in tc_range.iter_except_top_row_tcs():
            tc.vMerge = True

    @property
    def span_height(self):
        """int count of rows spanned by this cell.

        The value of this property may be misleading (often 1) on cells where
        `.is_merge_origin` is not |True|, since only a merge-origin cell
        contains complete span information. This property is only intended
        for use on cells known to be a merge origin by testing
        `.is_merge_origin`.
        """
        return self._tc.rowSpan

    @property
    def span_width(self):
        """int count of columns spanned by this cell.

        The value of this property may be misleading (often 1) on cells where
        `.is_merge_origin` is not |True|, since only a merge-origin cell
        contains complete span information. This property is only intended
        for use on cells known to be a merge origin by testing
        `.is_merge_origin`.
        """
        return self._tc.gridSpan

    def split(self):
        """Remove merge from this (merge-origin) cell.

        The merged cell represented by this object will be "unmerged",
        yielding a separate unmerged cell for each grid cell previously
        spanned by this merge.

        Raises |ValueError| when this cell is not a merge-origin cell. Test
        with `.is_merge_origin` before calling.
        """
        if not self.is_merge_origin:
            raise ValueError(
                "not a merge-origin cell; only a merge-origin cell can be sp" "lit"
            )

        tc_range = TcRange.from_merge_origin(self._tc)

        for tc in tc_range.iter_tcs():
            tc.rowSpan = tc.gridSpan = 1
            tc.hMerge = tc.vMerge = False

    @property
    def text(self):
        """Unicode (str in Python 3) representation of cell contents.

        The returned string will contain a newline character (``"\\n"``) separating each
        paragraph and a vertical-tab (``"\\v"``) character for each line break (soft
        carriage return) in the cell's text.

        Assignment to *text* replaces all text currently contained in the cell. A
        newline character (``"\\n"``) in the assigned text causes a new paragraph to be
        started. A vertical-tab (``"\\v"``) character in the assigned text causes
        a line-break (soft carriage-return) to be inserted. (The vertical-tab character
        appears in clipboard text copied from PowerPoint as its encoding of
        line-breaks.)

        Either bytes (Python 2 str) or unicode (Python 3 str) can be assigned. Bytes can
        be 7-bit ASCII or UTF-8 encoded 8-bit bytes. Bytes values are converted to
        unicode assuming UTF-8 encoding (which correctly decodes ASCII).
        """
        return self.text_frame.text

    @text.setter
    def text(self, text):
        self.text_frame.text = text

    @property
    def text_frame(self):
        """
        |TextFrame| instance containing the text that appears in the cell.
        """
        txBody = self._tc.get_or_add_txBody()
        return TextFrame(txBody, self)

    @property
    def vertical_anchor(self):
        """Vertical alignment of this cell.

        This value is a member of the :ref:`MsoVerticalAnchor` enumeration or
        |None|. A value of |None| indicates the cell has no explicitly
        applied vertical anchor setting and its effective value is inherited
        from its style-hierarchy ancestors.

        Assigning |None| to this property causes any explicitly applied
        vertical anchor setting to be cleared and inheritance of its
        effective value to be restored.
        """
        return self._tc.anchor

    @vertical_anchor.setter
    def vertical_anchor(self, mso_anchor_idx):
        self._tc.anchor = mso_anchor_idx

    @staticmethod
    def _validate_margin_value(margin_value):
        """
        Raise ValueError if *margin_value* is not a positive integer value or
        |None|.
        """
        if not is_integer(margin_value) and margin_value is not None:
            tmpl = "margin value must be integer or None, got '%s'"
            raise TypeError(tmpl % margin_value)


class _Column(Subshape):
    """Table column"""

    def __init__(self, gridCol, parent):
        super(_Column, self).__init__(parent)
        self._gridCol = gridCol

    @property
    def width(self):
        """
        Width of column in EMU.
        """
        return self._gridCol.w

    @width.setter
    def width(self, width):
        self._gridCol.w = width
        self._parent.notify_width_changed()


class _Row(Subshape):
    """Table row"""

    def __init__(self, tr, parent):
        super(_Row, self).__init__(parent)
        self._tr = tr

    @property
    def cells(self):
        """
        Read-only reference to collection of cells in row. An individual cell
        is referenced using list notation, e.g. ``cell = row.cells[0]``.
        """
        return _CellCollection(self._tr, self)

    @property
    def height(self):
        """
        Height of row in EMU.
        """
        return self._tr.h

    @height.setter
    def height(self, height):
        self._tr.h = height
        self._parent.notify_height_changed()


class _CellCollection(Subshape):
    """Horizontal sequence of row cells"""

    def __init__(self, tr, parent):
        super(_CellCollection, self).__init__(parent)
        self._tr = tr

    def __getitem__(self, idx):
        """Provides indexed access, (e.g. 'cells[0]')."""
        if idx < 0 or idx >= len(self._tr.tc_lst):
            msg = "cell index [%d] out of range" % idx
            raise IndexError(msg)
        return _Cell(self._tr.tc_lst[idx], self)

    def __iter__(self):
        """Provides iterability."""
        return (_Cell(tc, self) for tc in self._tr.tc_lst)

    def __len__(self):
        """Supports len() function (e.g. 'len(cells) == 1')."""
        return len(self._tr.tc_lst)


class _ColumnCollection(Subshape):
    """Sequence of table columns."""

    def __init__(self, tbl, parent):
        super(_ColumnCollection, self).__init__(parent)
        self._tbl = tbl

    def __getitem__(self, idx):
        """
        Provides indexed access, (e.g. 'columns[0]').
        """
        if idx < 0 or idx >= len(self._tbl.tblGrid.gridCol_lst):
            msg = "column index [%d] out of range" % idx
            raise IndexError(msg)
        return _Column(self._tbl.tblGrid.gridCol_lst[idx], self)

    def __len__(self):
        """
        Supports len() function (e.g. 'len(columns) == 1').
        """
        return len(self._tbl.tblGrid.gridCol_lst)

    def notify_width_changed(self):
        """
        Called by a column when its width changes. Pass along to parent.
        """
        self._parent.notify_width_changed()


class _RowCollection(Subshape):
    """Sequence of table rows"""

    def __init__(self, tbl, parent):
        super(_RowCollection, self).__init__(parent)
        self._tbl = tbl

    def __getitem__(self, idx):
        """
        Provides indexed access, (e.g. 'rows[0]').
        """
        if idx < 0 or idx >= len(self):
            msg = "row index [%d] out of range" % idx
            raise IndexError(msg)
        return _Row(self._tbl.tr_lst[idx], self)

    def __len__(self):
        """
        Supports len() function (e.g. 'len(rows) == 1').
        """
        return len(self._tbl.tr_lst)

    def notify_height_changed(self):
        """
        Called by a row when its height changes. Pass along to parent.
        """
        self._parent.notify_height_changed()
