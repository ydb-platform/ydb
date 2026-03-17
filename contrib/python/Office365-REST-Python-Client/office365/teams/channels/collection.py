from office365.entity_collection import EntityCollection
from office365.runtime.queries.function import FunctionQuery
from office365.teams.channels.channel import Channel
from office365.teams.chats.messages.message import ChatMessage


class ChannelCollection(EntityCollection[Channel]):
    """Team's channel collection"""

    def __init__(self, context, resource_path=None):
        super(ChannelCollection, self).__init__(context, Channel, resource_path)

    def add(self, display_name, description=None, membership_type=None, **kwargs):
        """Create a new channel in a Microsoft Team, as specified in the request body.

        :param str description: Optional textual description for the channel.
        :param str display_name: Channel name as it will appear to the user in Microsoft Teams.
        :param str membership_type: The type of the channel.
        """
        return super(ChannelCollection, self).add(
            displayName=display_name,
            description=description,
            membershipType=membership_type,
            **kwargs
        )

    def get_all_messages(self):
        """
        Retrieve messages across all channels in a team, including text, audio, and video conversations.
        """
        return_type = EntityCollection(self.context, ChatMessage, self.resource_path)
        qry = FunctionQuery(self, "getAllMessages", None, return_type)
        self.context.add_query(qry)
        return return_type
