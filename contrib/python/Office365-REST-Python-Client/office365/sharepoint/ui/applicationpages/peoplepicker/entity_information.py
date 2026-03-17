from typing import Optional

from office365.runtime.client_object import ClientObject
from office365.sharepoint.ui.applicationpages.peoplepicker.entity_information_request import (
    PickerEntityInformationRequest,
)


class PickerEntityInformation(ClientObject):
    """Represents additional information about the principal."""

    @property
    def total_member_count(self):
        # type: () -> Optional[int]
        """
        The count of members in a group. Valid when the principal is a SharePoint group or a security group.
        """
        return self.properties.get("TotalMemberCount", None)

    @property
    def entity(self):
        """The principal for which information is being requested."""
        return self.properties.get("Entity", PickerEntityInformationRequest())

    @property
    def entity_type_name(self):
        return "SP.UI.ApplicationPages.PickerEntityInformation"
