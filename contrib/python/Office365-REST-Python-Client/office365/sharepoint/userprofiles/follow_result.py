from office365.runtime.client_value import ClientValue
from office365.sharepoint.userprofiles.followed_item import FollowedItem


class FollowResult(ClientValue):
    """The FollowResult class returns information about a request to follow an item."""

    def __init__(self, item=FollowedItem(), result_type=None):
        """
        :param FollowedItem item: The Item property contains the item being followed.
        :param int result_type: The ResultType property provides information about the attempt to follow an item.
            For details on the FollowResultType type, see section 3.1.5.54.
        """
        self.Item = item
        self.ResultType = result_type
