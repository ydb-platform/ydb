from typing import List

from office365.delta_collection import DeltaCollection
from office365.outlook.mail.item_body import ItemBody
from office365.outlook.mail.messages.message import Message
from office365.outlook.mail.recipient import Recipient
from office365.runtime.client_value_collection import ClientValueCollection


class MessageCollection(DeltaCollection[Message]):
    def __init__(self, context, resource_path=None):
        super(MessageCollection, self).__init__(context, Message, resource_path)

    def add(self, subject=None, body=None, to_recipients=None, **kwargs):
        # type: (str, str|ItemBody, List[str], ...) -> Message
        """
        Create a draft of a new message in either JSON or MIME format.

        :param str subject: The subject of the message.
        :param str or ItemBody body: The body of the message. It can be in HTML or text format
        :param list[str] to_recipients:
        """
        if to_recipients is not None:
            kwargs["toRecipients"] = ClientValueCollection(
                Recipient, [Recipient.from_email(email) for email in to_recipients]
            )
        if body is not None:
            kwargs["body"] = body if isinstance(body, ItemBody) else ItemBody(body)
        if subject is not None:
            kwargs["subject"] = subject

        return super(MessageCollection, self).add(**kwargs)
