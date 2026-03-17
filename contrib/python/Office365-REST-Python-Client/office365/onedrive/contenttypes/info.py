from office365.runtime.client_value import ClientValue


class ContentTypeInfo(ClientValue):
    def __init__(self, _id=None, name=None):
        """
        The contentTypeInfo resource indicates the SharePoint content type of an item.

        :param str _id: The id of the content type.
        :param str name: The name of the content type.
        """
        self.id = _id
        self.name = name
