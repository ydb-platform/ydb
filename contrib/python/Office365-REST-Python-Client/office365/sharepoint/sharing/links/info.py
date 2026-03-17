from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.sharing.invitation.link import LinkInvitation
from office365.sharepoint.sharing.principal import Principal


class SharingLinkInfo(ClientValue):
    def __init__(
        self,
        allows_anonymous_access=None,
        application_id=None,
        created=None,
        created_by=Principal(),
        password_protected=None,
        invitations=None,
        redeemed_users=None,
        last_modified_by=Principal(),
        password_last_modified_by=Principal(),
        track_link_users=None,
        share_token_string=None,
        url=None,
    ):
        """
        Specifies the information about the tokenized sharing link.

        :param bool allows_anonymous_access: Indicates whether the tokenized sharing link allows anonymous access.
        :param str application_id:
        :param str created: The UTC date/time string with complete representation for calendar date and time of day
             which represents the time and date of creation of the tokenized sharing link.
        :param Principal created_by: Indicates the principal who created the tokenized sharing link, or null if the
             created by value is not recorded.
        :param bool password_protected:
        :param list[LinkInvitation] invitations: This value contains the current membership list for principals
             that have been Invited to the tokenized sharing link.
        :param list[LinkInvitation] redeemed_users:
        :param Principal last_modified_by: Indicates the principal who last modified the tokenized sharing link.
             This value MUST be null if the last modified by value is not recorded.
        :param Principal password_last_modified_by:
        :param bool track_link_users:
        :param str share_token_string:
        """
        super(SharingLinkInfo, self).__init__()
        self.AllowsAnonymousAccess = allows_anonymous_access
        self.ApplicationId = application_id
        self.Created = created
        self.CreatedBy = created_by
        self.PasswordProtected = password_protected
        self.Invitations = ClientValueCollection(LinkInvitation, invitations)
        self.RedeemedUsers = ClientValueCollection(LinkInvitation, redeemed_users)
        self.LastModifiedBy = last_modified_by
        self.PasswordLastModifiedBy = password_last_modified_by
        self.TrackLinkUsers = track_link_users
        self.ShareTokenString = share_token_string
        self.Url = url

    def __str__(self):
        return self.Url or ""

    def __repr__(self):
        return self.Url or self.entity_type_name

    @property
    def entity_type_name(self):
        return "SP.Sharing.SharingLinkInfo"
