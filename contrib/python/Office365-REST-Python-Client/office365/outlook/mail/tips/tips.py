from office365.outlook.calendar.email_address import EmailAddress
from office365.outlook.mail.automatic_replies_mailtips import AutomaticRepliesMailTips
from office365.outlook.mail.tips.error import MailTipsError
from office365.runtime.client_value import ClientValue


class MailTips(ClientValue):
    """Informative messages about a recipient, that are displayed to users while they're composing a message.
    For example, an out-of-office message as an automatic reply for a message recipient.
    """

    def __init__(
        self,
        automatic_replies=AutomaticRepliesMailTips(),
        custom_mail_tip=None,
        delivery_restricted=None,
        email_address=EmailAddress(),
        error=MailTipsError(),
        external_member_count=None,
        is_moderated=None,
    ):
        """
        :param AutomaticRepliesMailTips automatic_replies: Mail tips for automatic reply if it has been set up by
            the recipient.
        :param str custom_mail_tip: A custom mail tip that can be set on the recipient's mailbox.
        :param bool delivery_restricted: Whether the recipient is delivery restricted.
        :param EmailAddress email_address: Email address for the recipient.
        :param MailTipsError error: Error raised when an error occurs.
        :param int external_member_count: The number of external members if the recipient is a distribution list.
        :param bool is_moderated: Whether sending messages to the recipient requires approval. For example,
            if the recipient is a large distribution list and a moderator has been set up to approve messages sent
            to that distribution list, or if sending messages to a recipient requires approval
            of the recipient's manager.
        """
        self.automaticReplies = automatic_replies
        self.customMailTip = custom_mail_tip
        self.deliveryRestricted = delivery_restricted
        self.emailAddress = email_address
        self.error = error
        self.externalMemberCount = external_member_count
        self.isModerated = is_moderated
