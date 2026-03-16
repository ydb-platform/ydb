from office365.runtime.client_value import ClientValue


class DocumentSetContent(ClientValue):
    """Represents the default content of document set in SharePoint."""

    def __init__(self, content_type=None, file_name=None, folder_name=None):
        """
        :param ContentTypeInfo content_type: Content type information of the file.
        :param str file_name: Name of the file in resource folder that should be added as a default content or a
            template in the document set.
        :param str folder_name: Folder name in which the file will be placed when a new document set is created in the
            library.
        """
        self.contentType = content_type
        self.fileName = file_name
        self.folderName = folder_name
