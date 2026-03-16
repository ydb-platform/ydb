from office365.outlook.mail.recipient import Recipient
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class InvitedUserMessageInfo(ClientValue):
    """The invitedUserMessageInfo object allows you to configure the invitation message."""

    def __init__(
        self, cc_recipients=None, customized_message_body=None, message_language=None
    ):
        """
        :param list[Recipient] cc_recipients: Additional recipients the invitation message should be sent to.
             Currently only 1 additional recipient is supported.
        :param str customized_message_body: Customized message body you want to send if you don't want the default
            message.
        :param str message_language: The language you want to send the default message in. If the customizedMessageBody
             is specified, this property is ignored, and the message is sent using the customizedMessageBody.
             The language format should be in ISO 639. The default is en-US.
        """
        self.ccRecipients = ClientValueCollection(Recipient, cc_recipients)
        self.customizedMessageBody = customized_message_body
        self.messageLanguage = message_language
