from typing import Optional

from office365.sharepoint.fields.field import Field


class FieldNumber(Field):
    """Specifies a field that contains number values. To set properties, call the Update method
    (section 3.2.5.53.2.1.5)."""

    @property
    def display_format(self):
        # type: () -> Optional[int]
        """Gets the number of decimal places to be used when displaying the field."""
        return self.properties.get("DisplayFormat", None)

    @property
    def comma_separator(self):
        # type: () -> Optional[str]
        """Gets the separator used to format the value of the field."""
        return self.properties.get("CommaSeparator", None)

    @comma_separator.setter
    def comma_separator(self, value):
        # type: (str) -> None
        """Sets the separator used to format the value of the field."""
        self.set_property("CommaSeparator", value)

    @property
    def show_as_percentage(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that specifies whether to render the field as a percentage."""
        return self.properties.get("ShowAsPercentage", None)

    @show_as_percentage.setter
    def show_as_percentage(self, value):
        # type: (bool) -> None
        """Sets a Boolean value that specifies whether to render the field as a percentage."""
        self.set_property("ShowAsPercentage", value)
