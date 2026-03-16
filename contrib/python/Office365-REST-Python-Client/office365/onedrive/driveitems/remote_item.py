from office365.directory.permissions.identity_set import IdentitySet
from office365.onedrive.driveitems.image import Image
from office365.onedrive.files.file import File
from office365.onedrive.files.system_info import FileSystemInfo
from office365.onedrive.folders.folder import Folder
from office365.runtime.client_value import ClientValue


class RemoteItem(ClientValue):
    """
    The remoteItem resource indicates that a driveItem references an item that exists in another drive.
    This resource provides the unique IDs of the source drive and target item.

    DriveItems with a non-null remoteItem facet are resources that are shared, added to the user's OneDrive,
    or on items returned from heterogeneous collections of items (like search results).
    """

    def __init__(
        self,
        _id=None,
        created_by=IdentitySet(),
        created_datetime=None,
        file=File(),
        file_system_info=FileSystemInfo(),
        folder=Folder(),
        image=Image(),
    ):
        """
        :param str _id: Unique identifier for the remote item in its drive. Read-only.
        :param IdentitySet created_by: Identity of the user, device, and application which created the item. Read-only.
        :param datetime.datetime created_datetime: Date and time of item creation. Read-only.
        :param File file: Indicates that the remote item is a file. Read-only.
        :param FileSystemInfo file_system_info: Information about the remote item from the local file system. Read-only.
        :param Folder folder: Indicates that the remote item is a folder. Read-only.
        :param Image image: Image metadata, if the item is an image. Read-only.
        """
        self.id = _id
        self.createdBy = created_by
        self.createdDateTime = created_datetime
        self.file = file
        self.fileSystemInfo = file_system_info
        self.folder = folder
        self.image = image
