from typing import Optional

from office365.entity import Entity


class WorkbookRangeFont(Entity):
    """This object represents the font attributes (font name, font size, color, etc.) for an object."""

    @property
    def bold(self):
        # type: () -> Optional[bool]
        """Inidicates whether the font is bold."""
        return self.properties.get("bold", None)

    @property
    def color(self):
        # type: () -> Optional[str]
        """The HTML color code representation of the text color. For example, #FF0000 represents the color red."""
        return self.properties.get("color", None)

    @property
    def italic(self):
        # type: () -> Optional[bool]
        """Inidicates whether the font is italic."""
        return self.properties.get("italic", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """The font name. For example, "Calibri"."""
        return self.properties.get("name", None)

    @property
    def size(self):
        # type: () -> Optional[float]
        """The font size."""
        return self.properties.get("size", None)

    @property
    def underline(self):
        # type: () -> Optional[bool]
        """The type of underlining applied to the font. The possible values are:
        None, Single, Double, SingleAccountant, DoubleAccountant."""
        return self.properties.get("underline", None)
