"""Custom element classes for top-level chart-related XML elements."""

from __future__ import annotations

from typing import cast

from pptx.oxml import parse_xml
from pptx.oxml.chart.shared import CT_Title
from pptx.oxml.ns import nsdecls, qn
from pptx.oxml.simpletypes import ST_Style, XsdString
from pptx.oxml.text import CT_TextBody
from pptx.oxml.xmlchemy import (
    BaseOxmlElement,
    OneAndOnlyOne,
    RequiredAttribute,
    ZeroOrMore,
    ZeroOrOne,
)


class CT_Chart(BaseOxmlElement):
    """`c:chart` custom element class."""

    _tag_seq = (
        "c:title",
        "c:autoTitleDeleted",
        "c:pivotFmts",
        "c:view3D",
        "c:floor",
        "c:sideWall",
        "c:backWall",
        "c:plotArea",
        "c:legend",
        "c:plotVisOnly",
        "c:dispBlanksAs",
        "c:showDLblsOverMax",
        "c:extLst",
    )
    title = ZeroOrOne("c:title", successors=_tag_seq[1:])
    autoTitleDeleted = ZeroOrOne("c:autoTitleDeleted", successors=_tag_seq[2:])
    plotArea = OneAndOnlyOne("c:plotArea")
    legend = ZeroOrOne("c:legend", successors=_tag_seq[9:])
    rId: str = RequiredAttribute("r:id", XsdString)  # pyright: ignore[reportAssignmentType]

    @property
    def has_legend(self):
        """
        True if this chart has a legend defined, False otherwise.
        """
        legend = self.legend
        if legend is None:
            return False
        return True

    @has_legend.setter
    def has_legend(self, bool_value):
        """
        Add, remove, or leave alone the ``<c:legend>`` child element depending
        on current state and *bool_value*. If *bool_value* is |True| and no
        ``<c:legend>`` element is present, a new default element is added.
        When |False|, any existing legend element is removed.
        """
        if bool(bool_value) is False:
            self._remove_legend()
        else:
            if self.legend is None:
                self._add_legend()

    @staticmethod
    def new_chart(rId: str) -> CT_Chart:
        """Return a new `c:chart` element."""
        return cast(CT_Chart, parse_xml(f'<c:chart {nsdecls("c")} {nsdecls("r")} r:id="{rId}"/>'))

    def _new_title(self):
        return CT_Title.new_title()


class CT_ChartSpace(BaseOxmlElement):
    """`c:chartSpace` root element of a chart part."""

    _tag_seq = (
        "c:date1904",
        "c:lang",
        "c:roundedCorners",
        "c:style",
        "c:clrMapOvr",
        "c:pivotSource",
        "c:protection",
        "c:chart",
        "c:spPr",
        "c:txPr",
        "c:externalData",
        "c:printSettings",
        "c:userShapes",
        "c:extLst",
    )
    date1904 = ZeroOrOne("c:date1904", successors=_tag_seq[1:])
    style = ZeroOrOne("c:style", successors=_tag_seq[4:])
    chart = OneAndOnlyOne("c:chart")
    txPr = ZeroOrOne("c:txPr", successors=_tag_seq[10:])
    externalData = ZeroOrOne("c:externalData", successors=_tag_seq[11:])
    del _tag_seq

    @property
    def catAx_lst(self):
        return self.chart.plotArea.catAx_lst

    @property
    def date_1904(self):
        """
        Return |True| if the `c:date1904` child element resolves truthy,
        |False| otherwise. This value indicates whether date number values
        are based on the 1900 or 1904 epoch.
        """
        date1904 = self.date1904
        if date1904 is None:
            return False
        return date1904.val

    @property
    def dateAx_lst(self):
        return self.xpath("c:chart/c:plotArea/c:dateAx")

    def get_or_add_title(self):
        """Return the `c:title` grandchild, newly created if not present."""
        return self.chart.get_or_add_title()

    @property
    def plotArea(self):
        """
        Return the required `c:chartSpace/c:chart/c:plotArea` grandchild
        element.
        """
        return self.chart.plotArea

    @property
    def valAx_lst(self):
        return self.chart.plotArea.valAx_lst

    @property
    def xlsx_part_rId(self):
        """
        The string in the required ``r:id`` attribute of the
        `<c:externalData>` child, or |None| if no externalData element is
        present.
        """
        externalData = self.externalData
        if externalData is None:
            return None
        return externalData.rId

    def _add_externalData(self):
        """
        Always add a ``<c:autoUpdate val="0"/>`` child so auto-updating
        behavior is off by default.
        """
        externalData = self._new_externalData()
        externalData._add_autoUpdate(val=False)
        self._insert_externalData(externalData)
        return externalData

    def _new_txPr(self):
        return CT_TextBody.new_txPr()


class CT_ExternalData(BaseOxmlElement):
    """
    `<c:externalData>` element, defining link to embedded Excel package part
    containing the chart data.
    """

    autoUpdate = ZeroOrOne("c:autoUpdate")
    rId = RequiredAttribute("r:id", XsdString)


class CT_PlotArea(BaseOxmlElement):
    """
    ``<c:plotArea>`` element.
    """

    catAx = ZeroOrMore("c:catAx")
    valAx = ZeroOrMore("c:valAx")

    def iter_sers(self):
        """
        Generate each of the `c:ser` elements in this chart, ordered first by
        the document order of the containing xChart element, then by their
        ordering within the xChart element (not necessarily document order).
        """
        for xChart in self.iter_xCharts():
            for ser in xChart.iter_sers():
                yield ser

    def iter_xCharts(self):
        """
        Generate each xChart child element in document.
        """
        plot_tags = (
            qn("c:area3DChart"),
            qn("c:areaChart"),
            qn("c:bar3DChart"),
            qn("c:barChart"),
            qn("c:bubbleChart"),
            qn("c:doughnutChart"),
            qn("c:line3DChart"),
            qn("c:lineChart"),
            qn("c:ofPieChart"),
            qn("c:pie3DChart"),
            qn("c:pieChart"),
            qn("c:radarChart"),
            qn("c:scatterChart"),
            qn("c:stockChart"),
            qn("c:surface3DChart"),
            qn("c:surfaceChart"),
        )

        for child in self.iterchildren():
            if child.tag not in plot_tags:
                continue
            yield child

    @property
    def last_ser(self):
        """
        Return the last `<c:ser>` element in the last xChart element, based
        on series order (not necessarily the same element as document order).
        """
        last_xChart = self.xCharts[-1]
        sers = last_xChart.sers
        if not sers:
            return None
        return sers[-1]

    @property
    def next_idx(self):
        """
        Return the next available `c:ser/c:idx` value within the scope of
        this chart, the maximum idx value found on existing series,
        incremented by one.
        """
        idx_vals = [s.idx.val for s in self.sers]
        if not idx_vals:
            return 0
        return max(idx_vals) + 1

    @property
    def next_order(self):
        """
        Return the next available `c:ser/c:order` value within the scope of
        this chart, the maximum order value found on existing series,
        incremented by one.
        """
        order_vals = [s.order.val for s in self.sers]
        if not order_vals:
            return 0
        return max(order_vals) + 1

    @property
    def sers(self):
        """
        Return a sequence containing all the `c:ser` elements in this chart,
        ordered first by the document order of the containing xChart element,
        then by their ordering within the xChart element (not necessarily
        document order).
        """
        return tuple(self.iter_sers())

    @property
    def xCharts(self):
        """
        Return a sequence containing all the `c:{x}Chart` elements in this
        chart, in document order.
        """
        return tuple(self.iter_xCharts())


class CT_Style(BaseOxmlElement):
    """
    ``<c:style>`` element; defines the chart style.
    """

    val = RequiredAttribute("val", ST_Style)
