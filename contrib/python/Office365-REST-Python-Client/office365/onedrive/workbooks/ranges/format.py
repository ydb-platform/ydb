from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.ranges.border import WorkbookRangeBorder
from office365.onedrive.workbooks.ranges.fill import WorkbookRangeFill
from office365.onedrive.workbooks.ranges.font import WorkbookRangeFont
from office365.onedrive.workbooks.ranges.format_protection import (
    WorkbookFormatProtection,
)
from office365.runtime.paths.resource_path import ResourcePath


class WorkbookRangeFormat(Entity):
    """A format object encapsulating the range's font, fill, borders, alignment, and other properties."""

    @property
    def column_width(self):
        # type: () -> Optional[float]
        """Gets or sets the width of all columns within the range. If the column widths aren't uniform,
        null will be returned."""
        return self.properties.get("columnWidth", None)

    @property
    def horizontal_alignment(self):
        # type: () -> Optional[str]
        """Represents the horizontal alignment for the specified object"""
        return self.properties.get("horizontalAlignment", None)

    @property
    def row_height(self):
        # type: () -> Optional[float]
        """Gets or sets the height of all rows in the range. If the row heights aren't uniform null will be returned."""
        return self.properties.get("rowHeight", None)

    @property
    def vertical_alignment(self):
        # type: () -> Optional[str]
        """Represents the vertical alignment for the specified object.
        Possible values are: Top, Center, Bottom, Justify, Distributed."""
        return self.properties.get("verticalAlignment", None)

    @property
    def wrap_text(self):
        # type: () -> Optional[bool]
        """Indicates if Excel wraps the text in the object.
        A null value indicates that the entire range doesn't have uniform wrap setting
        """
        return self.properties.get("wrapText", None)

    @property
    def borders(self):
        # type: () -> EntityCollection[WorkbookRangeBorder]
        """Collection of border objects that apply to the overall range selected."""
        return self.properties.get(
            "borders",
            EntityCollection(
                self.context,
                WorkbookRangeBorder,
                ResourcePath("borders", self.resource_path),
            ),
        )

    @property
    def fill(self):
        """Returns the fill object defined on the overall range"""
        return self.properties.get(
            "fill",
            WorkbookRangeFill(self.context, ResourcePath("fill", self.resource_path)),
        )

    @property
    def font(self):
        """Returns the font object defined on the overall range selected"""
        return self.properties.get(
            "font",
            WorkbookRangeFont(self.context, ResourcePath("font", self.resource_path)),
        )

    @property
    def protection(self):
        """Returns the format protection object for a range"""
        return self.properties.get(
            "protection",
            WorkbookFormatProtection(
                self.context, ResourcePath("protection", self.resource_path)
            ),
        )
