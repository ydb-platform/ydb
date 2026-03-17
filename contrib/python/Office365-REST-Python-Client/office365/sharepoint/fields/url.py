from typing import Optional

from office365.sharepoint.fields.field import Field


class FieldUrl(Field):
    """Specifies a fields that contains a URL."""

    @property
    def display_format(self):
        # type: () -> Optional[int]
        """Gets the number of decimal places to be used when displaying the field."""
        return self.properties.get("DisplayFormat", None)
