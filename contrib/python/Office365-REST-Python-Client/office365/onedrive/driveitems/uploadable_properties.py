from office365.onedrive.files.system_info import FileSystemInfo
from office365.runtime.client_value import ClientValue


class DriveItemUploadableProperties(ClientValue):
    """The driveItemUploadableProperties resource represents an item being uploaded when creating an upload session."""

    def __init__(
        self,
        file_system_info=FileSystemInfo(),
        name=None,
        description=None,
        file_size=None,
    ):
        """
        :param FileSystemInfo file_system_info: File system information on client.
        :param str name: The name of the item (filename and extension).
        :param str description: Provides a user-visible description of the item. Read-write. Only on OneDrive Personal.
        :param int file_size: 	Provides an expected file size to perform a quota check prior to upload.
             Only on OneDrive Personal.
        """
        super(DriveItemUploadableProperties, self).__init__()
        self.fileSystemInfo = file_system_info
        self.name = name
        self.description = description
        self._fileSize = file_size

    @property
    def file_size(self):
        """Provides an expected file size to perform a quota check prior to upload. Only on OneDrive Personal."""
        return self._fileSize
