from office365.runtime.client_value import ClientValue


class ItemBody(ClientValue):
    """Represents properties of the body of an item, such as a message, event or group post."""

    def __init__(self, content=None, content_type="Text"):
        """
        :param str content: The content of the item.
        :param str content_type: The type of the content. Possible values are text and html.
        """
        super(ItemBody, self).__init__()
        self.content = content
        self.contentType = content_type

    @staticmethod
    def text(content):
        # type: (str) -> "ItemBody"
        return ItemBody(content)

    @staticmethod
    def html(content):
        # type: (str) -> "ItemBody"
        return ItemBody(content, "html")

    def __repr__(self):
        return self.content
