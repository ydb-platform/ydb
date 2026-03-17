from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.mail.post import Post
from office365.outlook.mail.recipient import Recipient
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class ConversationThread(Entity):
    """A conversationThread is a collection of posts."""

    def reply(self, post):
        """Reply to a thread in a group conversation and add a new post to it. You can specify the parent conversation
        in the request, or, you can specify just the thread without the parent conversation.

        :param Post post: A comment to include. Can be an empty string.
        """
        payload = {"post": post}
        qry = ServiceOperationQuery(self, "reply", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def cc_recipients(self):
        """The Cc: recipients for the thread."""
        return self.properties.get("ccRecipients", ClientValueCollection(Recipient))

    @property
    def has_attachments(self):
        """Indicates whether any of the posts within this thread has at least one attachment."""
        return self.properties.get("hasAttachments", None)

    @property
    def to_recipients(self):
        """The To: recipients for the thread."""
        return self.properties.get("toRecipients", ClientValueCollection(Recipient))

    @property
    def posts(self):
        """"""
        return self.properties.get(
            "posts",
            EntityCollection(
                self.context, Post, ResourcePath("posts", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "ccRecipients": self.cc_recipients,
                "toRecipients": self.to_recipients,
            }
            default_value = property_mapping.get(name, None)
        return super(ConversationThread, self).get_property(name, default_value)
