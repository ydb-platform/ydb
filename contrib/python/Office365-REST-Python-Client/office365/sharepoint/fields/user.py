from typing import Optional

from office365.sharepoint.fields.lookup import FieldLookup


class FieldUser(FieldLookup):
    """Specifies a field that contains a user."""

    @property
    def presence(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether presence is enabled on the field."""
        return self.properties.get("Presence", None)

    @presence.setter
    def presence(self, val):
        # type: (bool) -> None
        """Sets a value that specifies whether presence is enabled on the field."""
        self.properties.get("Presence", val)

    @property
    def allow_display(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether to display the name of the user in a survey list."""
        return self.properties.get("AllowDisplay", None)

    @allow_display.setter
    def allow_display(self, val):
        # type: (bool) -> None
        """Sets a value that specifies whether to display the name of the user in a survey list."""
        self.properties.get("AllowDisplay", val)

    @property
    def selection_group(self):
        # type: () -> Optional[int]
        """
        Gets a value that specifies the identifier of the SharePoint group whose members can be selected
        as values of the field.
        """
        return self.properties.get("SelectionGroup", None)

    @selection_group.setter
    def selection_group(self, val):
        # type: (int) -> None
        """Sets a value that specifies the identifier of the SharePoint group whose members can be selected as
        values of the field.
        """
        self.properties.get("SelectionGroup", val)
