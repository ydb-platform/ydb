from typing import Optional

from office365.runtime.types.collections import StringCollection
from office365.sharepoint.fields.field import Field


class FieldMultiChoice(Field):
    """Specifies a field that contains one or more values from a set of specified values."""

    @property
    def fill_in_choice(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the field can accept values other than those specified in
        Microsoft.Sharepoint.Client.FieldMultiChoice.Choices, as specified in section 3.2.5.51.1.1.2.
        """
        return self.properties.get("FillInChoice", None)

    @property
    def mappings(self):
        # type: () -> Optional[str]
        """Specifies the internal values corresponding to Choices."""
        return self.properties.get("Mappings", None)

    @property
    def choices(self):
        """Specifies values that are available for selection in the field"""
        return self.properties.get("Choices", StringCollection())
