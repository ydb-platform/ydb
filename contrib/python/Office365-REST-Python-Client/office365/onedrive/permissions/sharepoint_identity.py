from office365.directory.permissions.identity import Identity


class SharePointIdentity(Identity):
    """This resource extends from the identity resource to provide the ability to expose SharePoint-specific
    information; for example, loginName or SharePoint IDs."""

    def __init__(self, login_name=None):
        """
        :param str login_name: The sign in name of the SharePoint identity.
        """
        super(SharePointIdentity, self).__init__()
        self.loginName = login_name
