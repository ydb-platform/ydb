from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.gtp.base_request_options import BaseGptRequestOptions
from office365.sharepoint.gtp.message_entry import MessageEntry


class ChatGptRequestOptions(BaseGptRequestOptions):
    """"""

    def __init__(self, messages=None):
        """
        :param list[MessageEntry] messages:
        """
        self.Messages = ClientValueCollection(MessageEntry, messages)
