from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.charts.point import WorkbookChartPoint
from office365.onedrive.workbooks.charts.series.format import WorkbookChartSeriesFormat
from office365.runtime.paths.resource_path import ResourcePath


class WorkbookChartSeries(Entity):
    """Represents a series in a chart."""

    @property
    def format(self):
        """The formatting of a chart series, which includes fill and line formatting."""
        return self.properties.get(
            "format",
            WorkbookChartSeriesFormat(
                self.context, ResourcePath("format", self.resource_path)
            ),
        )

    @property
    def points(self):
        """A collection of all points in the series."""
        return self.properties.get(
            "points",
            EntityCollection(
                self.context,
                WorkbookChartPoint,
                ResourcePath("points", self.resource_path),
            ),
        )
