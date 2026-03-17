from office365.runtime.client_value import ClientValue


class SPInvitationCreationResult(ClientValue):
    """Specifies a result of adding an invitation."""

    def __init__(self):
        super(SPInvitationCreationResult, self).__init__()
        """
        """
        self.Email = None
        self.InvitationLink = None
        self.Succeeded = None

    @property
    def entity_type_name(self):
        return "SP.SPInvitationCreationResult"
