from typing import Optional

from office365.sharepoint.entity import Entity


class FieldLink(Entity):
    """Specifies a reference to a field or field definition for a content type."""

    @property
    def id(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the GUID of the FieldLink."""
        return self.properties.get("Id", None)

    @property
    def field_internal_name(self):
        # type: () -> Optional[str]
        """Gets a value that specifies field internal name"""
        return self.properties.get("FieldInternalName", None)

    @property
    def read_only(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("ReadOnly", None)

    @property
    def hidden(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether the field is displayed in forms that can be edited."""
        return self.properties.get("Hidden", None)

    @property
    def required(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether the field (2) requires a value."""
        return self.properties.get("Required", None)

    @property
    def show_in_display_form(self):
        # type: () -> Optional[bool]
        """A Boolean value that indicates whether this field is shown in the display form."""
        return self.properties.get("ShowInDisplayForm", None)
