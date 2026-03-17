from office365.outlook.mail.recipient import Recipient
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import StringCollection


class MessageRulePredicates(ClientValue):
    """Represents the set of conditions and exceptions that are available for a rule."""

    def __init__(
        self,
        body_contains=None,
        body_or_subject_contains=None,
        categories=None,
        from_addresses=None,
        has_attachments=None,
        header_contains=None,
        importance=None,
        is_approval_request=None,
    ):
        """
        :param list[str] body_contains: Represents the strings that should appear in the body of an incoming message
            in order for the condition or exception to apply.
        :param list[str] body_or_subject_contains: Represents the strings that should appear in the body or subject
             of an incoming message in order for the condition or exception to apply.
        :param list[str] categories: Represents the categories that an incoming message should be labeled with in
             order for the condition or exception to apply.
        :param list[Recipient] from_addresses: 	Represents the specific sender email addresses of an incoming message
             in order for the condition or exception to apply.
        :param bool has_attachments: Indicates whether an incoming message must have attachments in order for the
             condition or exception to apply.
        :param list[str] header_contains:
        :param bool is_approval_request:
        """
        self.bodyContains = StringCollection(body_contains)
        self.bodyOrSubjectContains = StringCollection(body_or_subject_contains)
        self.categories = StringCollection(categories)
        self.fromAddresses = ClientValueCollection(Recipient, from_addresses)
        self.hasAttachments = has_attachments
        self.headerContains = StringCollection(header_contains)
        self.importance = importance
        self.isApprovalRequest = is_approval_request
