from office365.entity_collection import EntityCollection
from office365.teams.chats.chat import Chat


class ChatCollection(EntityCollection[Chat]):
    """Team's collection"""

    def __init__(self, context, resource_path=None):
        super(ChatCollection, self).__init__(context, Chat, resource_path)

    def add(self, chat_type):
        """
        Create a new chat object.

        Note: Only one one-on-one chat can exist between two members. If a one-on-one chat already exists,
        this operation will return the existing chat and not create a new one.

        :param str chat_type: Specifies the type of chat. Possible values are: group and oneOnOne.
        """

        return super(ChatCollection, self).add(chatType=chat_type)
