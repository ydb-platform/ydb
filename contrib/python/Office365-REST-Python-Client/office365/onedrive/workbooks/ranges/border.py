from typing import Optional

from office365.entity import Entity


class WorkbookRangeBorder(Entity):
    """Represents the border of an object."""

    @property
    def color(self):
        # type: () -> Optional[str]
        """The HTML color code that represents the color of the border line. Can either be of the form #RRGGBB,
        for example "FFA500", or a named HTML color, for example orange."""
        return self.properties.get("color", None)

    @property
    def side_index(self):
        # type: () -> Optional[str]
        """Indicates the specific side of the border.
        The possible values are: EdgeTop, EdgeBottom, EdgeLeft, EdgeRight, InsideVertical, InsideHorizontal,
        DiagonalDown, DiagonalUp. Read-only."""
        return self.properties.get("sideIndex", None)

    @property
    def style(self):
        # type: () -> Optional[str]
        """Indicates the line style for the border. The possible values are: None, Continuous, Dash, DashDot,
        DashDotDot, Dot, Double, SlantDashDot."""
        return self.properties.get("style", None)

    @property
    def weight(self):
        # type: () -> Optional[str]
        """The weight of the border around a range. The possible values are: Hairline, Thin, Medium, Thick."""
        return self.properties.get("weight", None)
