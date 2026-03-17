from office365.runtime.client_value import ClientValue


class Thumbnail(ClientValue):
    """
    The thumbnail resource type represents a thumbnail for an image, video, document,
        or any item that has a bitmap representation.
    """

    def __init__(
        self, content=None, height=None, source_item_id=None, url=None, width=None
    ):
        """
        :param str content: The content stream for the thumbnail.
        :param int height: The height of the thumbnail, in pixels.
        :param str source_item_id: The unique identifier of the item that provided the thumbnail. This is only
            available when a folder thumbnail is requested.
        :param str url: The URL used to fetch the thumbnail content.
        :param int width: The width of the thumbnail, in pixels.
        """
        self.content = content
        self.height = height
        self.sourceItemId = source_item_id
        self.url = url
        self.width = width
