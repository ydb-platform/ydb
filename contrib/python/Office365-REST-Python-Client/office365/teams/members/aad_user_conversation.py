from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.members.conversation import ConversationMember


class AadUserConversationMember(ConversationMember):
    """Represents an Azure Active Directory user in a team, a channel, or a chat."""

    @property
    def user_id(self):
        # type: () -> Optional[str]
        """The guid of the user."""
        return self.properties.get("userId", None)

    @property
    def user(self):
        from office365.directory.users.user import User

        return self.properties.get(
            "user", User(self.context, ResourcePath("user", self.resource_path))
        )

    def to_json(self, json_format=None):
        return {
            "roles": self.roles,
            "user@odata.bind": "https://graph.microsoft.com/v1.0/users/{0}".format(
                self.user_id
            ),
        }
