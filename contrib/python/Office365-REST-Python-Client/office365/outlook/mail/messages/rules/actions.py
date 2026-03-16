from office365.outlook.mail.recipient import Recipient
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import StringCollection


class MessageRuleActions(ClientValue):
    """Represents the set of actions that are available to a rule."""

    def __init__(
        self,
        assign_categories=None,
        copy_to_folder=None,
        delete=None,
        forward_as_attachment_to=None,
        forward_to=None,
        mark_as_read=None,
        mark_importance=None,
        move_to_folder=None,
        permanent_delete=None,
        redirect_to=None,
        stop_processing_rules=None,
    ):
        """
        :param list[str] assign_categories: A list of categories to be assigned to a message.
        :param str copy_to_folder: The ID of a folder that a message is to be copied to.
        :param bool delete: Indicates whether a message should be moved to the Deleted Items folder.
        :param list[Recipient] forward_as_attachment_to: The email addresses of the recipients to which a message
            should be forwarded as an attachment.
        :param list[Recipient] forward_to: The email addresses of the recipients to which a message should be forwarded.
        :param bool mark_as_read: Indicates whether a message should be marked as read.
        :param str mark_importance: Sets the importance of the message, which can be: low, normal, high.
        :param str move_to_folder: The ID of the folder that a message will be moved to.
        :param bool permanent_delete: Indicates whether a message should be permanently deleted and not saved to the
            Deleted Items folder.
        :param list[Recipient] redirect_to: The email addresses to which a message should be redirected.
        :param bool stop_processing_rules: Indicates whether subsequent rules should be evaluated.
        """
        self.assignCategories = StringCollection(assign_categories)
        self.copyToFolder = copy_to_folder
        self.delete = delete
        self.forwardAsAttachmentTo = ClientValueCollection(
            Recipient, forward_as_attachment_to
        )
        self.forwardTo = ClientValueCollection(Recipient, forward_to)
        self.markAsRead = mark_as_read
        self.markImportance = mark_importance
        self.moveToFolder = move_to_folder
        self.permanentDelete = permanent_delete
        self.redirectTo = ClientValueCollection(Recipient, redirect_to)
        self.stopProcessingRules = stop_processing_rules
