from office365.runtime.client_value import ClientValue


class FileSystemInfo(ClientValue):
    """The FileSystemInfo resource contains properties that are reported by the device's local file system for the
    local version of an item."""

    def __init__(
        self,
        created_datetime=None,
        last_accessed_datetime=None,
        last_modified_datetime=None,
    ):
        """
        :param datetime.datetime created_datetime: The UTC date and time the file was created on a client.
        :param datetime.datetime last_accessed_datetime: The UTC date and time the file was last accessed.
            Available for the recent file list only.
        :param datetime.datetime last_modified_datetime: The UTC date and time the file was last modified on a client.
        """
        super(FileSystemInfo, self).__init__()
        self.createdDateTime = created_datetime
        self.lastAccessedDateTime = last_accessed_datetime
        self.lastModifiedDateTime = last_modified_datetime
