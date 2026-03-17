from office365.runtime.client_value import ClientValue


class ThumbnailColumn(ClientValue):
    """This column stores thumbnail values."""

    def __init__(self, is_picture=None):
        """
        :param bool is_picture:
        """
        self.isPicture = is_picture
