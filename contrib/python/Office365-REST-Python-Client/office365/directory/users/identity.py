from office365.runtime.client_value import ClientValue


class UserIdentity(ClientValue):
    """
    In the context of an Azure AD audit log, this represents the user information that initiated or
    was affected by an audit activity.
    """

    def __init__(self, display_name=None, ip_address=None, user_principal_name=None):
        """
        :param str display_name: he identity's display name. Note that this may not always be available or up-to-date.
        :param str ip_address: Indicates the client IP address used by user performing the activity (audit log only).
        :param str user_principal_name: The userPrincipalName attribute of the user.
        """
        super(UserIdentity, self).__init__()
        self.displayName = display_name
        self.ipAddress = ip_address
        self.userPrincipalName = user_principal_name
