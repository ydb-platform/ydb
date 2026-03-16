from office365.runtime.client_value import ClientValue


class FileCreationInformation(ClientValue):
    """Represents properties that can be set when creating a file by using the FileCollection.Add method."""

    def __init__(self, url=None, overwrite=False, content=None):
        """
        :param str url: Specifies the URL of the file to be added. It MUST NOT be NULL. It MUST be a URL of relative
            or absolute form. Its length MUST be equal to or greater than 1.
        :param bool overwrite: Specifies whether to overwrite an existing file with the same name and in the same
            location as the one being added.
        :param str or bytes content: Specifies the binary content of the file to be added.
        """
        super(FileCreationInformation, self).__init__()
        self.Url = url
        self.Overwrite = overwrite
        self.Content = content
        self.XorHash = None

    def to_json(self, json_format=None):
        return {"overwrite": self.Overwrite, "url": self.Url}

    @property
    def entity_type_name(self):
        return None
