from office365.runtime.client_value import ClientValue


class Image(ClientValue):
    """
    The Image resource groups image-related properties into a single structure. If a DriveItem has a
    non-null image facet, the item represents a bitmap image.

    Note: If the service is unable to determine the width and height of the image, the Image resource may be empty.
    """

    def __init__(self, height=None, width=None):
        """
        :param int height:
        :param int width:
        """
        self.height = height
        self.width = width
