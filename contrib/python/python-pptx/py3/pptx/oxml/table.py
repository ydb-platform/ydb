"""Custom element classes for table-related XML elements"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterator, cast

from pptx.enum.text import MSO_VERTICAL_ANCHOR
from pptx.oxml import parse_xml
from pptx.oxml.dml.fill import CT_GradientFillProperties
from pptx.oxml.ns import nsdecls
from pptx.oxml.simpletypes import ST_Coordinate, ST_Coordinate32, XsdBoolean, XsdInt
from pptx.oxml.text import CT_TextBody
from pptx.oxml.xmlchemy import (
    BaseOxmlElement,
    Choice,
    OneAndOnlyOne,
    OptionalAttribute,
    RequiredAttribute,
    ZeroOrMore,
    ZeroOrOne,
    ZeroOrOneChoice,
)
from pptx.util import Emu, lazyproperty

if TYPE_CHECKING:
    from pptx.util import Length


class CT_Table(BaseOxmlElement):
    """`a:tbl` custom element class"""

    get_or_add_tblPr: Callable[[], CT_TableProperties]
    tr_lst: list[CT_TableRow]
    _add_tr: Callable[..., CT_TableRow]

    _tag_seq = ("a:tblPr", "a:tblGrid", "a:tr")
    tblPr: CT_TableProperties | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:tblPr", successors=_tag_seq[1:]
    )
    tblGrid: CT_TableGrid = OneAndOnlyOne("a:tblGrid")  # pyright: ignore[reportAssignmentType]
    tr = ZeroOrMore("a:tr", successors=_tag_seq[3:])
    del _tag_seq

    def add_tr(self, height: Length) -> CT_TableRow:
        """Return a newly created `a:tr` child element having its `h` attribute set to `height`."""
        return self._add_tr(h=height)

    @property
    def bandCol(self) -> bool:
        return self._get_boolean_property("bandCol")

    @bandCol.setter
    def bandCol(self, value: bool):
        self._set_boolean_property("bandCol", value)

    @property
    def bandRow(self) -> bool:
        return self._get_boolean_property("bandRow")

    @bandRow.setter
    def bandRow(self, value: bool):
        self._set_boolean_property("bandRow", value)

    @property
    def firstCol(self) -> bool:
        return self._get_boolean_property("firstCol")

    @firstCol.setter
    def firstCol(self, value: bool):
        self._set_boolean_property("firstCol", value)

    @property
    def firstRow(self) -> bool:
        return self._get_boolean_property("firstRow")

    @firstRow.setter
    def firstRow(self, value: bool):
        self._set_boolean_property("firstRow", value)

    def iter_tcs(self) -> Iterator[CT_TableCell]:
        """Generate each `a:tc` element in this tbl.

        `a:tc` elements are generated left-to-right, top-to-bottom.
        """
        return (tc for tr in self.tr_lst for tc in tr.tc_lst)

    @property
    def lastCol(self) -> bool:
        return self._get_boolean_property("lastCol")

    @lastCol.setter
    def lastCol(self, value: bool):
        self._set_boolean_property("lastCol", value)

    @property
    def lastRow(self) -> bool:
        return self._get_boolean_property("lastRow")

    @lastRow.setter
    def lastRow(self, value: bool):
        self._set_boolean_property("lastRow", value)

    @classmethod
    def new_tbl(
        cls, rows: int, cols: int, width: int, height: int, tableStyleId: str | None = None
    ) -> CT_Table:
        """Return a new `p:tbl` element tree."""
        # working hypothesis is this is the default table style GUID
        if tableStyleId is None:
            tableStyleId = "{5C22544A-7EE6-4342-B048-85BDC9FD1C3A}"

        xml = cls._tbl_tmpl() % (tableStyleId)
        tbl = cast(CT_Table, parse_xml(xml))

        # add specified number of rows and columns
        rowheight = height // rows
        colwidth = width // cols

        for col in range(cols):
            # adjust width of last col to absorb any div error
            if col == cols - 1:
                colwidth = width - ((cols - 1) * colwidth)
            tbl.tblGrid.add_gridCol(width=Emu(colwidth))

        for row in range(rows):
            # adjust height of last row to absorb any div error
            if row == rows - 1:
                rowheight = height - ((rows - 1) * rowheight)
            tr = tbl.add_tr(height=Emu(rowheight))
            for col in range(cols):
                tr.add_tc()

        return tbl

    def tc(self, row_idx: int, col_idx: int) -> CT_TableCell:
        """Return `a:tc` element at `row_idx`, `col_idx`."""
        return self.tr_lst[row_idx].tc_lst[col_idx]

    def _get_boolean_property(self, propname: str) -> bool:
        """Generalized getter for the boolean properties on the `a:tblPr` child element.

        Defaults to False if `propname` attribute is missing or `a:tblPr` element itself is not
        present.
        """
        tblPr = self.tblPr
        if tblPr is None:
            return False
        propval = getattr(tblPr, propname)
        return {True: True, False: False, None: False}[propval]

    def _set_boolean_property(self, propname: str, value: bool) -> None:
        """Generalized setter for boolean properties on the `a:tblPr` child element.

        Sets `propname` attribute appropriately based on `value`. If `value` is True, the
        attribute is set to "1"; a tblPr child element is added if necessary. If `value` is False,
        the `propname` attribute is removed if present, allowing its default value of False to be
        its effective value.
        """
        if value not in (True, False):
            raise ValueError("assigned value must be either True or False, got %s" % value)
        tblPr = self.get_or_add_tblPr()
        setattr(tblPr, propname, value)

    @classmethod
    def _tbl_tmpl(cls):
        return (
            "<a:tbl %s>\n"
            '  <a:tblPr firstRow="1" bandRow="1">\n'
            "    <a:tableStyleId>%s</a:tableStyleId>\n"
            "  </a:tblPr>\n"
            "  <a:tblGrid/>\n"
            "</a:tbl>" % (nsdecls("a"), "%s")
        )


class CT_TableCell(BaseOxmlElement):
    """`a:tc` custom element class"""

    get_or_add_tcPr: Callable[[], CT_TableCellProperties]
    get_or_add_txBody: Callable[[], CT_TextBody]

    _tag_seq = ("a:txBody", "a:tcPr", "a:extLst")
    txBody: CT_TextBody | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:txBody", successors=_tag_seq[1:]
    )
    tcPr: CT_TableCellProperties | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:tcPr", successors=_tag_seq[2:]
    )
    del _tag_seq

    gridSpan: int = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "gridSpan", XsdInt, default=1
    )
    rowSpan: int = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "rowSpan", XsdInt, default=1
    )
    hMerge: bool = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "hMerge", XsdBoolean, default=False
    )
    vMerge: bool = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "vMerge", XsdBoolean, default=False
    )

    @property
    def anchor(self) -> MSO_VERTICAL_ANCHOR | None:
        """String held in `anchor` attribute of `a:tcPr` child element of this `a:tc` element."""
        if self.tcPr is None:
            return None
        return self.tcPr.anchor

    @anchor.setter
    def anchor(self, anchor_enum_idx: MSO_VERTICAL_ANCHOR | None):
        """Set value of anchor attribute on `a:tcPr` child element."""
        if anchor_enum_idx is None and self.tcPr is None:
            return
        tcPr = self.get_or_add_tcPr()
        tcPr.anchor = anchor_enum_idx

    def append_ps_from(self, spanned_tc: CT_TableCell):
        """Append `a:p` elements taken from `spanned_tc`.

        Any non-empty paragraph elements in `spanned_tc` are removed and appended to the
        text-frame of this cell. If `spanned_tc` is left with no content after this process, a
        single empty `a:p` element is added to ensure the cell is compliant with the spec.
        """
        source_txBody = spanned_tc.get_or_add_txBody()
        target_txBody = self.get_or_add_txBody()

        # ---if source is empty, there's nothing to do---
        if source_txBody.is_empty:
            return

        # ---a single empty paragraph in target is overwritten---
        if target_txBody.is_empty:
            target_txBody.clear_content()

        for p in source_txBody.p_lst:
            target_txBody.append(p)

        # ---neither source nor target can be left without ps---
        source_txBody.unclear_content()
        target_txBody.unclear_content()

    @property
    def col_idx(self) -> int:
        """Offset of this cell's column in its table."""
        # ---tc elements come before any others in `a:tr` element---
        return cast(CT_TableRow, self.getparent()).index(self)

    @property
    def is_merge_origin(self) -> bool:
        """True if cell is top-left in merged cell range."""
        if self.gridSpan > 1 and not self.vMerge:
            return True
        return self.rowSpan > 1 and not self.hMerge

    @property
    def is_spanned(self) -> bool:
        """True if cell is in merged cell range but not merge origin cell."""
        return self.hMerge or self.vMerge

    @property
    def marT(self) -> Length:
        """Top margin for this cell.

        This value is stored in the `marT` attribute of the `a:tcPr` child element of this `a:tc`.

        Read/write. If the attribute is not present, the default value `45720` (0.05 inches) is
        returned for top and bottom; `91440` (0.10 inches) is the default for left and right.
        Assigning |None| to any `marX` property clears that attribute from the element,
        effectively setting it to the default value.
        """
        return self._get_marX("marT", Emu(45720))

    @marT.setter
    def marT(self, value: Length | None):
        self._set_marX("marT", value)

    @property
    def marR(self) -> Length:
        """Right margin value represented in `marR` attribute."""
        return self._get_marX("marR", Emu(91440))

    @marR.setter
    def marR(self, value: Length | None):
        self._set_marX("marR", value)

    @property
    def marB(self) -> Length:
        """Bottom margin value represented in `marB` attribute."""
        return self._get_marX("marB", Emu(45720))

    @marB.setter
    def marB(self, value: Length | None):
        self._set_marX("marB", value)

    @property
    def marL(self) -> Length:
        """Left margin value represented in `marL` attribute."""
        return self._get_marX("marL", Emu(91440))

    @marL.setter
    def marL(self, value: Length | None):
        self._set_marX("marL", value)

    @classmethod
    def new(cls) -> CT_TableCell:
        """Return a new `a:tc` element subtree."""
        return cast(
            CT_TableCell,
            parse_xml(
                f"<a:tc {nsdecls('a')}>\n"
                f"  <a:txBody>\n"
                f"    <a:bodyPr/>\n"
                f"    <a:lstStyle/>\n"
                f"    <a:p/>\n"
                f"  </a:txBody>\n"
                f"  <a:tcPr/>\n"
                f"</a:tc>"
            ),
        )

    @property
    def row_idx(self) -> int:
        """Offset of this cell's row in its table."""
        return cast(CT_TableRow, self.getparent()).row_idx

    @property
    def tbl(self) -> CT_Table:
        """Table element this cell belongs to."""
        return cast(CT_Table, self.xpath("ancestor::a:tbl")[0])

    @property
    def text(self) -> str:  # pyright: ignore[reportIncompatibleMethodOverride]
        """str text contained in cell"""
        # ---note this shadows lxml _Element.text---
        txBody = self.txBody
        if txBody is None:
            return ""
        return "\n".join([p.text for p in txBody.p_lst])

    def _get_marX(self, attr_name: str, default: Length) -> Length:
        """Generalized method to get margin values."""
        if self.tcPr is None:
            return Emu(default)
        return Emu(int(self.tcPr.get(attr_name, default)))

    def _new_txBody(self) -> CT_TextBody:
        return CT_TextBody.new_a_txBody()

    def _set_marX(self, marX: str, value: Length | None) -> None:
        """Set value of marX attribute on `a:tcPr` child element.

        If `marX` is |None|, the marX attribute is removed. `marX` is a string, one of `('marL',
        'marR', 'marT', 'marB')`.
        """
        if value is None and self.tcPr is None:
            return
        tcPr = self.get_or_add_tcPr()
        setattr(tcPr, marX, value)


class CT_TableCellProperties(BaseOxmlElement):
    """`a:tcPr` custom element class"""

    eg_fillProperties = ZeroOrOneChoice(
        (
            Choice("a:noFill"),
            Choice("a:solidFill"),
            Choice("a:gradFill"),
            Choice("a:blipFill"),
            Choice("a:pattFill"),
            Choice("a:grpFill"),
        ),
        successors=("a:headers", "a:extLst"),
    )
    anchor: MSO_VERTICAL_ANCHOR | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "anchor", MSO_VERTICAL_ANCHOR
    )
    marL: Length | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "marL", ST_Coordinate32
    )
    marR: Length | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "marR", ST_Coordinate32
    )
    marT: Length | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "marT", ST_Coordinate32
    )
    marB: Length | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "marB", ST_Coordinate32
    )

    def _new_gradFill(self):
        return CT_GradientFillProperties.new_gradFill()


class CT_TableCol(BaseOxmlElement):
    """`a:gridCol` custom element class."""

    w: Length = RequiredAttribute("w", ST_Coordinate)  # pyright: ignore[reportAssignmentType]


class CT_TableGrid(BaseOxmlElement):
    """`a:tblGrid` custom element class."""

    gridCol_lst: list[CT_TableCol]
    _add_gridCol: Callable[..., CT_TableCol]

    gridCol = ZeroOrMore("a:gridCol")

    def add_gridCol(self, width: Length) -> CT_TableCol:
        """A newly appended `a:gridCol` child element having its `w` attribute set to `width`."""
        return self._add_gridCol(w=width)


class CT_TableProperties(BaseOxmlElement):
    """`a:tblPr` custom element class."""

    bandRow = OptionalAttribute("bandRow", XsdBoolean, default=False)
    bandCol = OptionalAttribute("bandCol", XsdBoolean, default=False)
    firstRow = OptionalAttribute("firstRow", XsdBoolean, default=False)
    firstCol = OptionalAttribute("firstCol", XsdBoolean, default=False)
    lastRow = OptionalAttribute("lastRow", XsdBoolean, default=False)
    lastCol = OptionalAttribute("lastCol", XsdBoolean, default=False)


class CT_TableRow(BaseOxmlElement):
    """`a:tr` custom element class."""

    tc_lst: list[CT_TableCell]
    _add_tc: Callable[[], CT_TableCell]

    tc = ZeroOrMore("a:tc", successors=("a:extLst",))
    h: Length = RequiredAttribute("h", ST_Coordinate)  # pyright: ignore[reportAssignmentType]

    def add_tc(self) -> CT_TableCell:
        """A newly added minimal valid `a:tc` child element."""
        return self._add_tc()

    @property
    def row_idx(self) -> int:
        """Offset of this row in its table."""
        return cast(CT_Table, self.getparent()).tr_lst.index(self)

    def _new_tc(self):
        return CT_TableCell.new()


class TcRange(object):
    """A 2D block of `a:tc` cell elements in a table.

    This object assumes the structure of the underlying table does not change during its lifetime.
    Structural changes in this context would be insertion or removal of rows or columns.

    The client is expected to create, use, and then abandon an instance in the context of a single
    user operation that is known to have no structural side-effects of this type.
    """

    def __init__(self, tc: CT_TableCell, other_tc: CT_TableCell):
        self._tc = tc
        self._other_tc = other_tc

    @classmethod
    def from_merge_origin(cls, tc: CT_TableCell):
        """Return instance created from merge-origin tc element."""
        other_tc = tc.tbl.tc(
            tc.row_idx + tc.rowSpan - 1,  # ---other_row_idx
            tc.col_idx + tc.gridSpan - 1,  # ---other_col_idx
        )
        return cls(tc, other_tc)

    @lazyproperty
    def contains_merged_cell(self) -> bool:
        """True if one or more cells in range are part of a merged cell."""
        for tc in self.iter_tcs():
            if tc.gridSpan > 1:
                return True
            if tc.rowSpan > 1:
                return True
            if tc.hMerge:
                return True
            if tc.vMerge:
                return True
        return False

    @lazyproperty
    def dimensions(self) -> tuple[int, int]:
        """(row_count, col_count) pair describing size of range."""
        _, _, width, height = self._extents
        return height, width

    @lazyproperty
    def in_same_table(self):
        """True if both cells provided to constructor are in same table."""
        if self._tc.tbl is self._other_tc.tbl:
            return True
        return False

    def iter_except_left_col_tcs(self):
        """Generate each `a:tc` element not in leftmost column of range."""
        for tr in self._tbl.tr_lst[self._top : self._bottom]:
            for tc in tr.tc_lst[self._left + 1 : self._right]:
                yield tc

    def iter_except_top_row_tcs(self):
        """Generate each `a:tc` element in non-first rows of range."""
        for tr in self._tbl.tr_lst[self._top + 1 : self._bottom]:
            for tc in tr.tc_lst[self._left : self._right]:
                yield tc

    def iter_left_col_tcs(self):
        """Generate each `a:tc` element in leftmost column of range."""
        col_idx = self._left
        for tr in self._tbl.tr_lst[self._top : self._bottom]:
            yield tr.tc_lst[col_idx]

    def iter_tcs(self):
        """Generate each `a:tc` element in this range.

        Cell elements are generated left-to-right, top-to-bottom.
        """
        return (
            tc
            for tr in self._tbl.tr_lst[self._top : self._bottom]
            for tc in tr.tc_lst[self._left : self._right]
        )

    def iter_top_row_tcs(self):
        """Generate each `a:tc` element in topmost row of range."""
        tr = self._tbl.tr_lst[self._top]
        for tc in tr.tc_lst[self._left : self._right]:
            yield tc

    def move_content_to_origin(self):
        """Move all paragraphs in range to origin cell."""
        tcs = list(self.iter_tcs())
        origin_tc = tcs[0]
        for spanned_tc in tcs[1:]:
            origin_tc.append_ps_from(spanned_tc)

    @lazyproperty
    def _bottom(self):
        """Index of row following last row of range"""
        _, top, _, height = self._extents
        return top + height

    @lazyproperty
    def _extents(self) -> tuple[int, int, int, int]:
        """A (left, top, width, height) tuple describing range extents.

        Note this is normalized to accommodate the various orderings of the corner cells provided
        on construction, which may be in any of four configurations such as (top-left,
        bottom-right), (bottom-left, top-right), etc.
        """

        def start_and_size(idx: int, other_idx: int) -> tuple[int, int]:
            """Return beginning and length of range based on two indexes."""
            return min(idx, other_idx), abs(idx - other_idx) + 1

        tc, other_tc = self._tc, self._other_tc

        left, width = start_and_size(tc.col_idx, other_tc.col_idx)
        top, height = start_and_size(tc.row_idx, other_tc.row_idx)

        return left, top, width, height

    @lazyproperty
    def _left(self):
        """Index of leftmost column in range."""
        left, _, _, _ = self._extents
        return left

    @lazyproperty
    def _right(self):
        """Index of column following the last column in range."""
        left, _, width, _ = self._extents
        return left + width

    @lazyproperty
    def _tbl(self):
        """`a:tbl` element containing this cell range."""
        return self._tc.tbl

    @lazyproperty
    def _top(self):
        """Index of topmost row in range."""
        _, top, _, _ = self._extents
        return top
