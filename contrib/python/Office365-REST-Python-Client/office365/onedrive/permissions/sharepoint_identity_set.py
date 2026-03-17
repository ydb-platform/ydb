from office365.directory.permissions.identity import Identity
from office365.directory.permissions.identity_set import IdentitySet
from office365.onedrive.permissions.sharepoint_identity import SharePointIdentity


class SharePointIdentitySet(IdentitySet):
    """
    Represents a keyed collection of sharePointIdentity resources. This resource extends from the identitySet resource
    to provide the ability to expose SharePoint-specific information to the user.

    This resource is used to represent a set of identities associated with various events for an item,
    such as created by or last modified by.
    """

    def __init__(
        self,
        group=Identity(),
        site_group=SharePointIdentity(),
        site_user=SharePointIdentity(),
    ):
        """
        :param Identity group: The group associated with this action.
        :param SharePointIdentity site_group: The SharePoint group associated with this action
        :param SharePointIdentity site_user: The SharePoint user associated with this action
        """
        super(SharePointIdentitySet, self).__init__()
        self.group = group
        self.siteGroup = site_group
        self.siteUser = site_user
