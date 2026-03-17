from office365.runtime.client_value import ClientValue


class SharedWithMeDocumentUser(ClientValue):
    """Represents a user of a document that is shared with the current user."""

    def __init__(self, _id=None, login_name=None, sip_address=None, title=None):
        """
        :param str _id: Identifier
        :param str login_name: Specifies the login name of the user.
        :param str sip_address: Specifies the sip address of the user.
        :param str title: Specifies the title of the user.
        """
        self.Id = _id
        self.LoginName = login_name
        self.SipAddress = sip_address
        self.Title = title

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.SharedWithMeDocumentUser"
