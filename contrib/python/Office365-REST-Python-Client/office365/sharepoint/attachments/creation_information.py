from office365.runtime.client_value import ClientValue


class AttachmentCreationInformation(ClientValue):
    def __init__(self, filename=None, content=None):
        """
        Represents properties that can be set when creating a file by using the AttachmentFiles.Add method.

        :param str filename: Specifies the file name of the list item attachment.
        :param str or bytes content: The contents of the file as a stream.
        """
        super(AttachmentCreationInformation, self).__init__()
        self._filename = filename
        self._content = content

    @property
    def content(self):
        """Gets the binary content of the file."""
        return self._content

    @content.setter
    def content(self, value):
        """Sets the binary content of the file."""
        self._content = value

    @property
    def filename(self):
        """The URL of the file."""
        return self._filename

    @filename.setter
    def filename(self, value):
        self._filename = value
