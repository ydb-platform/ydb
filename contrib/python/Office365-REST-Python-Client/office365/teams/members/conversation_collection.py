from office365.entity_collection import EntityCollection
from office365.teams.members.conversation import ConversationMember


class ConversationMemberCollection(EntityCollection[ConversationMember]):
    def __init__(self, context, resource_path=None):
        super(ConversationMemberCollection, self).__init__(
            context, ConversationMember, resource_path
        )

    def add(self, user, roles, visible_history_start_datetime=None):
        """
        Add a conversationMember.

        :param str or office365.directory.users.user.User user: The conversation members that should be added.
            Every user who will participate in the chat, including the user who initiates the create request,
            must be specified in this list.
            Each member must be assigned a role of owner or guest. Guest tenant members must be assigned the guest role
        :param list[str] roles: The roles for that user.
        :param datetime.datetime visible_history_start_datetime:
        """
        return_type = super(ConversationMemberCollection, self).add(roles=roles)
        from office365.directory.users.user import User

        if isinstance(user, User):

            def _user_loaded():
                return_type.set_property("userId", user.id)

            user.ensure_property("id", _user_loaded)
        else:
            return_type.set_property("userId", user)
        return return_type
