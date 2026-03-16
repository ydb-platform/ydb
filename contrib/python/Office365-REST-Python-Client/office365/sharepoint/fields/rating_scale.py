from typing import Optional

from office365.sharepoint.fields.multi_choice import FieldMultiChoice


class FieldRatingScale(FieldMultiChoice):
    """Specifies a field (2) that contains rating scale values for a survey list.
    To set properties, call the Update method (section 3.2.5.54.2.1.5)."""

    @property
    def grid_start_number(self):
        # type: () -> Optional[int]
        """Gets the start number for the rating scale."""
        return self.properties.get("GridStartNumber", None)

    @grid_start_number.setter
    def grid_start_number(self, value):
        # type: (int) -> None
        """Gets the start number for the rating scale."""
        self.set_property("GridStartNumber", value)

    @property
    def range_count(self):
        # type: () -> Optional[int]
        """Specifies the number of options in the rating scale."""
        return self.properties.get("RangeCount", None)
