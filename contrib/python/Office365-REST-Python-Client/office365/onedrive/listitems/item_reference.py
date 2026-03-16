from office365.onedrive.sharepoint_ids import SharePointIds
from office365.runtime.client_value import ClientValue


class ItemReference(ClientValue):
    """The ItemReference resource provides information necessary to address a DriveItem via the API."""

    def __init__(
        self,
        _id=None,
        name=None,
        path=None,
        drive_id=None,
        drive_type=None,
        site_id=None,
        sharepoint_ids=SharePointIds(),
        share_id=None,
    ):
        """
        :param str _id: Unique identifier of the driveItem in the drive or a listItem in a list. Read-only.
        :param str name: The name of the item being referenced. Read-only.
        :param str path: Path that can be used to navigate to the item. Read-only.
        :param str drive_id: Unique identifier of the drive instance that contains the driveItem.
            Only returned if the item is located in a drive. Read-only.
        :param str drive_type: Identifies the type of drive. See drive resource for values.
        :param str site_id: For OneDrive for Business and SharePoint, this property represents the ID of the site
            that contains the parent document library of the driveItem resource. The value is the same as the id
            property of that site resource. It is an opaque string that consists of three identifiers of the site.
            For OneDrive, this property is not populated.
        :param SharePointIds sharepoint_ids: Returns identifiers useful for SharePoint REST compatibility
        :param str share_id: A unique identifier for a shared resource that can be accessed via the Shares API.
        """
        super(ItemReference, self).__init__()
        self.id = _id
        self.name = name
        self.path = path
        self.driveId = drive_id
        self.driveType = drive_type
        self.siteId = site_id
        self.sharepointIds = sharepoint_ids
        self.shareId = share_id
