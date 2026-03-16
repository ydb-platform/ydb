from office365.runtime.client_value import ClientValue


class AccessRequestSettings(ClientValue):
    """
    This class returns the access request settings. It’s an optional property that can be retrieved in
    Microsoft.SharePoint.Client.Sharing.SecurableObjectExtensions.GetSharingInformation() call on a list item.
    """

    def __init__(
        self,
        has_pending_access_requests=None,
        pending_access_requests_link=None,
        requires_access_approval=None,
    ):
        """
        :param bool has_pending_access_requests: Boolean indicating whether there are pending access requests
            for the list item.
        :param str pending_access_requests_link: The full URL to the access requests page for the list item,
             or an empty string if the link is not available.
        :param bool requires_access_approval: Boolean indicating whether the current user’s access on the list item
             requires approval from admin for sharing to others.
        """
        self.hasPendingAccessRequests = has_pending_access_requests
        self.pendingAccessRequestsLink = pending_access_requests_link
        self.requiresAccessApproval = requires_access_approval

    @property
    def entity_type_name(self):
        return "SP.Sharing.AccessRequestSettings"
