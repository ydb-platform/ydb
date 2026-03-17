from office365.runtime.client_value import ClientValue


class File(ClientValue):
    """
    The File resource groups file-related data items into a single structure.

    If a DriveItem has a non-null file facet, the item represents a file.
    In addition to other properties, files have a content relationship which contains the byte stream of the file.
    """

    def __init__(self, mime_type=None, hashes=None):
        """
        :param str mime_type: The MIME type for the file. This is determined by logic on the server and might not be
            the value provided when the file was uploaded. Read-only.
        :param str hashes: Hashes of the file's binary content, if available. Read-only.
        """
        super(File, self).__init__()
        self.hashes = hashes
        self.mimeType = mime_type
        self.processingMetadata = None
