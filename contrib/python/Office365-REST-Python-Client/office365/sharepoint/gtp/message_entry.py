from office365.runtime.client_value import ClientValue


class MessageEntry(ClientValue):
    """"""

    def __init__(self, content=None):
        """
        :param str content:
        """
        self.content = content

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Internal.MessageEntry"
