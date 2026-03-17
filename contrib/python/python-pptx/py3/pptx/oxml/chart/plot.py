"""Plot-related oxml objects."""

from __future__ import annotations

from pptx.oxml.chart.datalabel import CT_DLbls
from pptx.oxml.simpletypes import (
    ST_BarDir,
    ST_BubbleScale,
    ST_GapAmount,
    ST_Grouping,
    ST_Overlap,
)
from pptx.oxml.xmlchemy import (
    BaseOxmlElement,
    OneAndOnlyOne,
    OptionalAttribute,
    ZeroOrMore,
    ZeroOrOne,
)


class BaseChartElement(BaseOxmlElement):
    """
    Base class for barChart, lineChart, and other plot elements.
    """

    @property
    def cat(self):
        """
        Return the `c:cat` element of the first series in this xChart, or
        |None| if not present.
        """
        cats = self.xpath("./c:ser[1]/c:cat")
        return cats[0] if cats else None

    @property
    def cat_pt_count(self):
        """
        Return the value of the `c:ptCount` descendent of this xChart
        element. Its parent can be one of three element types. This value
        represents the true number of (leaf) categories, although they might
        not all have a corresponding `c:pt` sibling; a category with no label
        does not get a `c:pt` element. Returns 0 if there is no `c:ptCount`
        descendent.
        """
        cat_ptCounts = self.xpath("./c:ser//c:cat//c:ptCount")
        if not cat_ptCounts:
            return 0
        return cat_ptCounts[0].val

    @property
    def cat_pts(self):
        """
        Return a sequence representing the `c:pt` elements under the `c:cat`
        element of the first series in this xChart element. A category having
        no value will have no corresponding `c:pt` element; |None| will
        appear in that position in such cases. Items appear in `idx` order.
        Only those in the first ``<c:lvl>`` element are included in the case
        of multi-level categories.
        """
        cat_pts = self.xpath("./c:ser[1]/c:cat//c:lvl[1]/c:pt")
        if not cat_pts:
            cat_pts = self.xpath("./c:ser[1]/c:cat//c:pt")

        cat_pt_dict = dict((pt.idx, pt) for pt in cat_pts)

        return [cat_pt_dict.get(idx, None) for idx in range(self.cat_pt_count)]

    @property
    def grouping_val(self):
        """
        Return the value of the ``./c:grouping{val=?}`` attribute, taking
        defaults into account when items are not present.
        """
        grouping = self.grouping
        if grouping is None:
            return ST_Grouping.STANDARD
        val = grouping.val
        if val is None:
            return ST_Grouping.STANDARD
        return val

    def iter_sers(self):
        """
        Generate each ``<c:ser>`` child element in this xChart in
        c:order/@val sequence (not document or c:idx order).
        """

        def ser_order(ser):
            return ser.order.val

        return (ser for ser in sorted(self.xpath("./c:ser"), key=ser_order))

    @property
    def sers(self):
        """
        Sequence of ``<c:ser>`` child elements in this xChart in c:order/@val
        sequence (not document or c:idx order).
        """
        return tuple(self.iter_sers())

    def _new_dLbls(self):
        return CT_DLbls.new_dLbls()


class CT_Area3DChart(BaseChartElement):
    """
    ``<c:area3DChart>`` element.
    """

    grouping = ZeroOrOne(
        "c:grouping",
        successors=(
            "c:varyColors",
            "c:ser",
            "c:dLbls",
            "c:dropLines",
            "c:gapDepth",
            "c:axId",
        ),
    )


class CT_AreaChart(BaseChartElement):
    """
    ``<c:areaChart>`` element.
    """

    _tag_seq = (
        "c:grouping",
        "c:varyColors",
        "c:ser",
        "c:dLbls",
        "c:dropLines",
        "c:axId",
        "c:extLst",
    )
    grouping = ZeroOrOne("c:grouping", successors=_tag_seq[1:])
    varyColors = ZeroOrOne("c:varyColors", successors=_tag_seq[2:])
    ser = ZeroOrMore("c:ser", successors=_tag_seq[3:])
    dLbls = ZeroOrOne("c:dLbls", successors=_tag_seq[4:])
    del _tag_seq


class CT_BarChart(BaseChartElement):
    """
    ``<c:barChart>`` element.
    """

    _tag_seq = (
        "c:barDir",
        "c:grouping",
        "c:varyColors",
        "c:ser",
        "c:dLbls",
        "c:gapWidth",
        "c:overlap",
        "c:serLines",
        "c:axId",
        "c:extLst",
    )
    barDir = OneAndOnlyOne("c:barDir")
    grouping = ZeroOrOne("c:grouping", successors=_tag_seq[2:])
    varyColors = ZeroOrOne("c:varyColors", successors=_tag_seq[3:])
    ser = ZeroOrMore("c:ser", successors=_tag_seq[4:])
    dLbls = ZeroOrOne("c:dLbls", successors=_tag_seq[5:])
    gapWidth = ZeroOrOne("c:gapWidth", successors=_tag_seq[6:])
    overlap = ZeroOrOne("c:overlap", successors=_tag_seq[7:])
    del _tag_seq

    @property
    def grouping_val(self):
        """
        Return the value of the ``./c:grouping{val=?}`` attribute, taking
        defaults into account when items are not present.
        """
        grouping = self.grouping
        if grouping is None:
            return ST_Grouping.CLUSTERED
        val = grouping.val
        if val is None:
            return ST_Grouping.CLUSTERED
        return val


class CT_BarDir(BaseOxmlElement):
    """
    ``<c:barDir>`` child of a barChart element, specifying the orientation of
    the bars, 'bar' if they are horizontal and 'col' if they are vertical.
    """

    val = OptionalAttribute("val", ST_BarDir, default=ST_BarDir.COL)


class CT_BubbleChart(BaseChartElement):
    """
    ``<c:bubbleChart>`` custom element class
    """

    _tag_seq = (
        "c:varyColors",
        "c:ser",
        "c:dLbls",
        "c:axId",
        "c:bubble3D",
        "c:bubbleScale",
        "c:showNegBubbles",
        "c:sizeRepresents",
        "c:axId",
        "c:extLst",
    )
    ser = ZeroOrMore("c:ser", successors=_tag_seq[2:])
    dLbls = ZeroOrOne("c:dLbls", successors=_tag_seq[3:])
    bubble3D = ZeroOrOne("c:bubble3D", successors=_tag_seq[5:])
    bubbleScale = ZeroOrOne("c:bubbleScale", successors=_tag_seq[6:])
    del _tag_seq


class CT_BubbleScale(BaseChartElement):
    """
    ``<c:bubbleScale>`` custom element class
    """

    val = OptionalAttribute("val", ST_BubbleScale, default=100)


class CT_DoughnutChart(BaseChartElement):
    """
    ``<c:doughnutChart>`` element.
    """

    _tag_seq = (
        "c:varyColors",
        "c:ser",
        "c:dLbls",
        "c:firstSliceAng",
        "c:holeSize",
        "c:extLst",
    )
    varyColors = ZeroOrOne("c:varyColors", successors=_tag_seq[1:])
    ser = ZeroOrMore("c:ser", successors=_tag_seq[2:])
    dLbls = ZeroOrOne("c:dLbls", successors=_tag_seq[3:])
    del _tag_seq


class CT_GapAmount(BaseOxmlElement):
    """
    ``<c:gapWidth>`` child of ``<c:barChart>`` element, also used for other
    purposes like error bars.
    """

    val = OptionalAttribute("val", ST_GapAmount, default=150)


class CT_Grouping(BaseOxmlElement):
    """
    ``<c:grouping>`` child of an xChart element, specifying a value like
    'clustered' or 'stacked'. Also used for variants with the same tag name
    like CT_BarGrouping.
    """

    val = OptionalAttribute("val", ST_Grouping)


class CT_LineChart(BaseChartElement):
    """
    ``<c:lineChart>`` custom element class
    """

    _tag_seq = (
        "c:grouping",
        "c:varyColors",
        "c:ser",
        "c:dLbls",
        "c:dropLines",
        "c:hiLowLines",
        "c:upDownBars",
        "c:marker",
        "c:smooth",
        "c:axId",
        "c:extLst",
    )
    grouping = ZeroOrOne("c:grouping", successors=(_tag_seq[1:]))
    varyColors = ZeroOrOne("c:varyColors", successors=_tag_seq[2:])
    ser = ZeroOrMore("c:ser", successors=_tag_seq[3:])
    dLbls = ZeroOrOne("c:dLbls", successors=(_tag_seq[4:]))
    del _tag_seq


class CT_Overlap(BaseOxmlElement):
    """
    ``<c:overlap>`` element specifying bar overlap as an integer percentage
    of bar width, in range -100 to 100.
    """

    val = OptionalAttribute("val", ST_Overlap, default=0)


class CT_PieChart(BaseChartElement):
    """
    ``<c:pieChart>`` custom element class
    """

    _tag_seq = ("c:varyColors", "c:ser", "c:dLbls", "c:firstSliceAng", "c:extLst")
    varyColors = ZeroOrOne("c:varyColors", successors=_tag_seq[1:])
    ser = ZeroOrMore("c:ser", successors=_tag_seq[2:])
    dLbls = ZeroOrOne("c:dLbls", successors=_tag_seq[3:])
    del _tag_seq


class CT_RadarChart(BaseChartElement):
    """
    ``<c:radarChart>`` custom element class
    """

    _tag_seq = (
        "c:radarStyle",
        "c:varyColors",
        "c:ser",
        "c:dLbls",
        "c:axId",
        "c:extLst",
    )
    varyColors = ZeroOrOne("c:varyColors", successors=_tag_seq[2:])
    ser = ZeroOrMore("c:ser", successors=_tag_seq[3:])
    dLbls = ZeroOrOne("c:dLbls", successors=(_tag_seq[4:]))
    del _tag_seq


class CT_ScatterChart(BaseChartElement):
    """
    ``<c:scatterChart>`` custom element class
    """

    _tag_seq = (
        "c:scatterStyle",
        "c:varyColors",
        "c:ser",
        "c:dLbls",
        "c:axId",
        "c:extLst",
    )
    varyColors = ZeroOrOne("c:varyColors", successors=_tag_seq[2:])
    ser = ZeroOrMore("c:ser", successors=_tag_seq[3:])
    del _tag_seq
