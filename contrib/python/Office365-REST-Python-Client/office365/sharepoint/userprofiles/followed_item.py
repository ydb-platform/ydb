from office365.runtime.client_value import ClientValue


class FollowedItem(ClientValue):
    """The FollowItem method is reserved for server-to-server use only. The server sets the specified item to be
    followed by the current user. This method cannot be called from the client."""

    def __init__(self, file_type=None):
        """
        :param int file_type:
        """
        self.FileType = file_type
