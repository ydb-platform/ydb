from typing import Optional

from office365.sharepoint.entity import Entity
from office365.sharepoint.lists.creatable_item_info import CreatableItemInfoCollection


class CreatablesInfo(Entity):
    """
    Returns an object that describes what this list can create, and a collection of links to visit in order to create
    those things. If it can't create certain things, it contains an error message describing why.

    The consumer MUST append the encoded URL of the current page to the links returned here.
    (This page the link goes to needs it as a query parameter to function correctly.) The consumer SHOULD also consider
    appending &IsDlg=1 to the link, to remove the UI from the linked page, if desired.
    """

    @property
    def can_create_folders(self):
        # type: () -> Optional[bool]
        """
        Indicates if the user is able to create folders in the current list. The user MUST have the appropriate
        permissions and the list MUST allow folder creation.
        """
        return self.properties.get("CanCreateFolders", None)

    @property
    def can_create_items(self):
        # type: () -> Optional[bool]
        """
        Indicates whether this list can create items (such as documents (Word/Excel/PowerPoint))
        using Microsoft Office Online.
        """
        return self.properties.get("CanCreateItems", None)

    @property
    def can_upload_files(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the user is able to upload files to this list.
        """
        return self.properties.get("CanUploadFiles", None)

    @property
    def creatables_collection(self):
        """
        Represents a collection of CreatableItemInfo (section 3.2.5.283) objects describing what can be created,
        one CreatableItemInfo for each creatable type.
        """
        return self.properties.get(
            "CreatablesCollection", CreatableItemInfoCollection()
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"CreatablesCollection": self.creatables_collection}
            default_value = property_mapping.get(name, None)
        return super(CreatablesInfo, self).get_property(name, default_value)
