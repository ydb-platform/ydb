from office365.entity import Entity
from office365.onedrive.workbooks.charts.title_format import WorkbookChartTitleFormat
from office365.runtime.paths.resource_path import ResourcePath


class WorkbookChartTitle(Entity):
    """Represents a chart title object of a chart."""

    @property
    def format(self):
        """The formatting of a chart title, which includes fill and font formatting."""
        return self.properties.get(
            "format",
            WorkbookChartTitleFormat(
                self.context, ResourcePath("format", self.resource_path)
            ),
        )
