from office365.directory.permissions.identity_set import IdentitySet
from office365.runtime.client_value import ClientValue


class SharingInvitation(ClientValue):
    """The SharingInvitation resource groups invitation-related data items into a single structure."""

    def __init__(
        self,
        email=None,
        invited_by=IdentitySet(),
        redeemed_by=None,
        signin_required=None,
    ):
        """
        :param str email: The email address provided for the recipient of the sharing invitation. Read-only.
        :param IdentitySet invited_by: Provides information about who sent the invitation that created this permission,
            if that information is available. Read-only.
        :param str redeemed_by:
        :param bool signin_required: If true the recipient of the invitation needs to sign in in order
            to access the shared item. Read-only.
        """
        super(SharingInvitation, self).__init__()
        self.email = email
        self.invitedBy = invited_by
        self.redeemedBy = redeemed_by
        self.signInRequired = signin_required
