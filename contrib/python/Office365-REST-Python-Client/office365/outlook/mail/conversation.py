import datetime
from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.mail.conversation_thread import ConversationThread
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.types.collections import StringCollection


class Conversation(Entity):
    """
    A conversation is a collection of threads, and a thread contains posts to that thread.
    All threads and posts in a conversation share the same subject.
    """

    @property
    def has_attachments(self):
        # type: () -> Optional[bool]
        """
        Indicates whether any of the posts within this Conversation has at least one attachment.
        Supports $filter (eq, ne) and $search.
        """
        return self.properties.get("hasAttachments", None)

    @property
    def last_delivered_datetime(self):
        # type: () -> Optional[datetime.datetime]
        """The Timestamp type represents date and time information using ISO 8601 format and is always in UTC time."""
        return self.properties.get("lastDeliveredDateTime", datetime.datetime.min)

    @property
    def preview(self):
        # type: () -> Optional[str]
        """
        A short summary from the body of the latest post in this conversation.
        Supports $filter (eq, ne, le, ge).
        """
        return self.properties.get("preview", None)

    @property
    def topic(self):
        # type: () -> Optional[str]
        """
        The topic of the conversation. This property can be set when the conversation is created, but it cannot be
        updated.
        """
        return self.properties.get("topic", None)

    @property
    def unique_senders(self):
        """All the users that sent a message to this Conversation."""
        return self.properties.get("uniqueSenders", StringCollection())

    @property
    def threads(self):
        # type: () -> EntityCollection[ConversationThread]
        """A collection of all the conversation threads in the conversation."""
        return self.properties.get(
            "threads",
            EntityCollection(
                self.context,
                ConversationThread,
                ResourcePath("threads", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"lastDeliveredDateTime": self.last_delivered_datetime}
            default_value = property_mapping.get(name, None)
        return super(Conversation, self).get_property(name, default_value)
