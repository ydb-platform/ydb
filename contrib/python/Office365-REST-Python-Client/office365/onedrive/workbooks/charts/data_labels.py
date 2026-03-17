from typing import Optional

from office365.entity import Entity


class WorkbookChartDataLabels(Entity):
    """Represents a collection of all the data labels on a chart point."""

    @property
    def position(self):
        # type: () -> Optional[str]
        """DataLabelPosition value that represents the position of the data label."""
        return self.properties.get("position", None)

    @property
    def separator(self):
        # type: () -> Optional[str]
        """String representing the separator used for the data labels on a chart."""
        return self.properties.get("separator", None)
