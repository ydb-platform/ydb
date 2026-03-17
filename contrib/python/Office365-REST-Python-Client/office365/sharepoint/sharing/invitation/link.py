from office365.runtime.client_value import ClientValue
from office365.sharepoint.sharing.principal import Principal


class LinkInvitation(ClientValue):
    """This class is used to identify the specific invitees for a tokenized sharing link,
    along with who invited them and when."""

    def __init__(self, invited_by=Principal(), invited_on=None, invitee=Principal()):
        """
        :param Principal invited_by: Indicates the principal who invited the invitee to the tokenized sharing link.
        :param str invited_on: String representation of nullable DateTime value indicating when the invitee was
             invited to the tokenized sharing link.
        :param Principal invitee: Indicates a principal who is invited to the tokenized sharing link.
        """
        super(LinkInvitation, self).__init__()
        self.invitedBy = invited_by
        self.invitedOn = invited_on
        self.invitee = invitee

    @property
    def entity_type_name(self):
        return "SP.Sharing.LinkInvitation"
