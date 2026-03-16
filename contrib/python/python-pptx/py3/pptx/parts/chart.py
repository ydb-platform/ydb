"""Chart part objects, including Chart and Charts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pptx.chart.chart import Chart
from pptx.opc.constants import CONTENT_TYPE as CT
from pptx.opc.constants import RELATIONSHIP_TYPE as RT
from pptx.opc.package import XmlPart
from pptx.parts.embeddedpackage import EmbeddedXlsxPart
from pptx.util import lazyproperty

if TYPE_CHECKING:
    from pptx.chart.data import ChartData
    from pptx.enum.chart import XL_CHART_TYPE
    from pptx.package import Package


class ChartPart(XmlPart):
    """A chart part.

    Corresponds to parts having partnames matching ppt/charts/chart[1-9][0-9]*.xml
    """

    partname_template = "/ppt/charts/chart%d.xml"

    @classmethod
    def new(cls, chart_type: XL_CHART_TYPE, chart_data: ChartData, package: Package):
        """Return new |ChartPart| instance added to `package`.

        Returned chart-part contains a chart of `chart_type` depicting `chart_data`.
        """
        chart_part = cls.load(
            package.next_partname(cls.partname_template),
            CT.DML_CHART,
            package,
            chart_data.xml_bytes(chart_type),
        )
        chart_part.chart_workbook.update_from_xlsx_blob(chart_data.xlsx_blob)
        return chart_part

    @lazyproperty
    def chart(self):
        """|Chart| object representing the chart in this part."""
        return Chart(self._element, self)

    @lazyproperty
    def chart_workbook(self):
        """
        The |ChartWorkbook| object providing access to the external chart
        data in a linked or embedded Excel workbook.
        """
        return ChartWorkbook(self._element, self)


class ChartWorkbook(object):
    """Provides access to external chart data in a linked or embedded Excel workbook."""

    def __init__(self, chartSpace, chart_part):
        super(ChartWorkbook, self).__init__()
        self._chartSpace = chartSpace
        self._chart_part = chart_part

    def update_from_xlsx_blob(self, xlsx_blob):
        """
        Replace the Excel spreadsheet in the related |EmbeddedXlsxPart| with
        the Excel binary in *xlsx_blob*, adding a new |EmbeddedXlsxPart| if
        there isn't one.
        """
        xlsx_part = self.xlsx_part
        if xlsx_part is None:
            self.xlsx_part = EmbeddedXlsxPart.new(xlsx_blob, self._chart_part.package)
            return
        xlsx_part.blob = xlsx_blob

    @property
    def xlsx_part(self):
        """Optional |EmbeddedXlsxPart| object containing data for this chart.

        This related part has its rId at `c:chartSpace/c:externalData/@rId`. This value
        is |None| if there is no `<c:externalData>` element.
        """
        xlsx_part_rId = self._chartSpace.xlsx_part_rId
        return None if xlsx_part_rId is None else self._chart_part.related_part(xlsx_part_rId)

    @xlsx_part.setter
    def xlsx_part(self, xlsx_part):
        """
        Set the related |EmbeddedXlsxPart| to *xlsx_part*. Assume one does
        not already exist.
        """
        rId = self._chart_part.relate_to(xlsx_part, RT.PACKAGE)
        externalData = self._chartSpace.get_or_add_externalData()
        externalData.rId = rId
