from office365.runtime.client_value import ClientValue


class ImageFieldValue(ClientValue):
    def __init__(self, server_relative_url=None):
        """
        :param str server_relative_url:
        """
        self.serverRelativeUrl = server_relative_url
        self.type = ("thumbnail",)
        self.fileName = None
        self.nativeFile = {}
        self.fieldName = "Image"
        self.serverUrl = None
        self.fieldId = None
        self.id = None

    @property
    def entity_type_name(self):
        return None
