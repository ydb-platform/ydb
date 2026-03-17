from office365.entity import Entity
from office365.runtime.types.collections import StringCollection


class ConversationMember(Entity):
    """Represents a user in a team, a channel, or a chat. See also aadUserConversationMember."""

    @property
    def display_name(self):
        """The display name of the user."""
        return self.properties.get("displayName", None)

    @property
    def roles(self):
        """The roles for that user."""
        return self.properties.get("roles", StringCollection())
