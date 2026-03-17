from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.charts.axes import WorkbookChartAxes
from office365.onedrive.workbooks.charts.data_labels import WorkbookChartDataLabels
from office365.onedrive.workbooks.charts.legend.legend import WorkbookChartLegend
from office365.onedrive.workbooks.charts.series.series import WorkbookChartSeries
from office365.onedrive.workbooks.charts.title import WorkbookChartTitle
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookChart(Entity):
    """Represents a chart object in a workbook."""

    def image(self, width=None, height=None):
        """Renders the chart as a base64-encoded image by scaling the chart to fit the specified dimensions.

        :param int width: Specifies the width of the rendered image in pixels.
        :param int height: Specifies the height of the rendered image in pixels.
        """
        return_type = ClientResult(self.context)
        params = {"width": width, "height": height}
        qry = FunctionQuery(self, "image", params, return_type)
        self.context.add_query(qry)
        return return_type

    def set_data(self, source_data, series_by):
        """
        Updates the data source of a chart
        :param dict source_data:
        :param str series_by:
        """
        payload = {"sourceData": source_data, "seriesBy": series_by}
        qry = ServiceOperationQuery(self, "setData", None, payload)
        self.context.add_query(qry)
        return self

    def set_position(self, start_cell, end_cell):
        """Positions the chart relative to cells on the worksheet.
        :param str start_cell: The start cell. It is where the chart is moved to. The start cell is the top-left or
             top-right cell, depending on the user's right-to-left display settings.
        :param str end_cell: The end cell. If specified, the chart's width and height is set to fully cover up
             this cell/range.
        """
        payload = {"startCell": start_cell, "endCell": end_cell}
        qry = ServiceOperationQuery(self, "setPosition", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def axes(self):
        """Represents chart axes."""
        return self.properties.get(
            "protection",
            WorkbookChartAxes(self.context, ResourcePath("axes", self.resource_path)),
        )

    @property
    def data_labels(self):
        """Represents the data labels on the chart."""
        return self.properties.get(
            "dataLabels",
            WorkbookChartDataLabels(
                self.context, ResourcePath("dataLabels", self.resource_path)
            ),
        )

    @property
    def legend(self):
        """Represents the legend on the chart."""
        return self.properties.get(
            "legend",
            WorkbookChartLegend(
                self.context, ResourcePath("legend", self.resource_path)
            ),
        )

    @property
    def series(self):
        """Represents chart series."""
        return self.properties.get(
            "series",
            EntityCollection(
                self.context,
                WorkbookChartSeries,
                ResourcePath("series", self.resource_path),
            ),
        )

    @property
    def title(self):
        """Represents the title on the chart."""
        return self.properties.get(
            "title",
            WorkbookChartTitle(self.context, ResourcePath("title", self.resource_path)),
        )

    @property
    def worksheet(self):
        """The worksheet containing the current chart."""
        from office365.onedrive.workbooks.worksheets.worksheet import WorkbookWorksheet

        return self.properties.get(
            "worksheet",
            WorkbookWorksheet(
                self.context, ResourcePath("worksheet", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "dataLabels": self.data_labels,
            }
            default_value = property_mapping.get(name, None)
        return super(WorkbookChart, self).get_property(name, default_value)
