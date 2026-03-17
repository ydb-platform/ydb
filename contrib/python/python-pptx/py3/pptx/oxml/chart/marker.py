"""Series-related oxml objects."""

from __future__ import annotations

from pptx.enum.chart import XL_MARKER_STYLE
from pptx.oxml.simpletypes import ST_MarkerSize
from pptx.oxml.xmlchemy import BaseOxmlElement, RequiredAttribute, ZeroOrOne


class CT_Marker(BaseOxmlElement):
    """
    `c:marker` custom element class, containing visual properties for a data
    point marker on line-type charts.
    """

    _tag_seq = ("c:symbol", "c:size", "c:spPr", "c:extLst")
    symbol = ZeroOrOne("c:symbol", successors=_tag_seq[1:])
    size = ZeroOrOne("c:size", successors=_tag_seq[2:])
    spPr = ZeroOrOne("c:spPr", successors=_tag_seq[3:])
    del _tag_seq

    @property
    def size_val(self):
        """
        Return the value of `./c:size/@val`, specifying the size of this
        marker in points. Returns |None| if no `c:size` element is present or
        its val attribute is not present.
        """
        size = self.size
        if size is None:
            return None
        return size.val

    @property
    def symbol_val(self):
        """
        Return the value of `./c:symbol/@val`, specifying the shape of this
        marker. Returns |None| if no `c:symbol` element is present.
        """
        symbol = self.symbol
        if symbol is None:
            return None
        return symbol.val


class CT_MarkerSize(BaseOxmlElement):
    """
    `c:size` custom element class, specifying the size (in points) of a data
    point marker for a line, XY, or radar chart.
    """

    val = RequiredAttribute("val", ST_MarkerSize)


class CT_MarkerStyle(BaseOxmlElement):
    """
    `c:symbol` custom element class, specifying the shape of a data point
    marker for a line, XY, or radar chart.
    """

    val = RequiredAttribute("val", XL_MARKER_STYLE)
