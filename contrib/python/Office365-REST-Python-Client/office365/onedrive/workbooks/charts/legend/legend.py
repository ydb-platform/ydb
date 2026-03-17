from typing import Optional

from office365.entity import Entity


class WorkbookChartLegend(Entity):
    """Represents the legend in a chart."""

    @property
    def overlay(self):
        # type: () -> Optional[bool]
        """Indicates whether the chart legend should overlap with the main body of the chart."""
        return self.properties.get("overlay", None)

    @property
    def position(self):
        # type: () -> Optional[str]
        """Represents the position of the legend on the chart.
        The possible values are: Top, Bottom, Left, Right, Corner, Custom."""
        return self.properties.get("position", None)

    @property
    def visible(self):
        # type: () -> Optional[bool]
        """Indicates whether the chart legend is visible."""
        return self.properties.get("visible", None)
