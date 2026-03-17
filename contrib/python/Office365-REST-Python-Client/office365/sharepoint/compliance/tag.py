from office365.runtime.client_value import ClientValue


class ComplianceTag(ClientValue):
    def __init__(
        self, accept_messages_only_from_senders_or_members=None, access_type=None
    ):
        """
        :param bool accept_messages_only_from_senders_or_members:
        :param str access_type:
        """
        self.AcceptMessagesOnlyFromSendersOrMembers = (
            accept_messages_only_from_senders_or_members
        )
        self.AccessType = access_type

    @property
    def entity_type_name(self):
        return "SP.CompliancePolicy.ComplianceTag"
