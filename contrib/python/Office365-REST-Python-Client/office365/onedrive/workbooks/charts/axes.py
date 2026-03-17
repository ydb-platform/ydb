from office365.entity import Entity
from office365.onedrive.workbooks.charts.axis import WorkbookChartAxis
from office365.runtime.paths.resource_path import ResourcePath


class WorkbookChartAxes(Entity):
    """Represents the chart axes."""

    @property
    def category_axis(self):
        """Represents the category axis in a chart."""
        return self.properties.get(
            "categoryAxis",
            WorkbookChartAxis(
                self.context, ResourcePath("categoryAxis", self.resource_path)
            ),
        )

    @property
    def series_axis(self):
        """Represents the series axis of a 3-dimensional chart."""
        return self.properties.get(
            "seriesAxis",
            WorkbookChartAxis(
                self.context, ResourcePath("seriesAxis", self.resource_path)
            ),
        )

    @property
    def value_axis(self):
        """Represents the value axis in an axis."""
        return self.properties.get(
            "valueAxis",
            WorkbookChartAxis(
                self.context, ResourcePath("valueAxis", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "categoryAxis": self.category_axis,
                "seriesAxis": self.series_axis,
                "valueAxis": self.value_axis,
            }
            default_value = property_mapping.get(name, None)
        return super(WorkbookChartAxes, self).get_property(name, default_value)
