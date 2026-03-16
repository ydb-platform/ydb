from office365.directory.permissions.identity_set import IdentitySet
from office365.runtime.client_value import ClientValue


class Shared(ClientValue):
    def __init__(
        self,
        owner=IdentitySet(),
        scope=None,
        shared_by=IdentitySet(),
        shared_datetime=None,
    ):
        """
        The Shared resource indicates a DriveItem has been shared with others. The resource includes information
        about how the item is shared.

        :param office365.directory.identities.identity_set.IdentitySet owner: The identity of the owner of the shared
           item.
        :param str scope: Indicates the scope of how the item is shared: anonymous, organization, or users. Read-only.
           item.
        :param office365.directory.identities.identity_set.IdentitySet shared_by: The identity of the user who shared
            the item
        :param datetime shared_datetime: The UTC date and time when the item was shared. Read-only.
        """
        super(Shared, self).__init__()
        self.owner = owner
        self.scope = scope
        self.sharedBy = shared_by
        self.sharedDateTime = shared_datetime
