from office365.runtime.client_value import ClientValue


class DocumentSetVersionItem(ClientValue):
    """
    Represents an item that is a part of a captured documentSetVersion.
    """

    def __init__(self, item_id=None, title=None, version_id=None):
        """
        :param str item_id: The unique identifier for the item.
        :param str title: The title of the item.
        :param str version_id: The version ID of the item.
        """
        self.itemId = item_id
        self.title = title
        self.versionId = version_id
