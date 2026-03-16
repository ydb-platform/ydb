from office365.runtime.client_value import ClientValue


class TeamMessagingSettings(ClientValue):
    """Settings to configure messaging and mentions in the team."""

    def __init__(
        self,
        allow_user_edit_messages=True,
        allow_user_delete_messages=True,
        allow_owner_delete_messages=True,
        allow_team_mentions=True,
        allow_channel_mentions=True,
    ):
        """
        :param bool allow_user_edit_messages: If set to true, users can edit their messages.
        :param bool allow_user_delete_messages: If set to true, users can delete their messages.
        :param bool allow_owner_delete_messages: If set to true, owners can delete their messages.
        :param bool allow_team_mentions: If set to true, owners can delete their messages.
        :param bool allow_channel_mentions: If set to true, owners can delete their messages.
        """
        super(TeamMessagingSettings, self).__init__()
        self.allowUserEditMessages = allow_user_edit_messages
        self.allowUserDeleteMessages = allow_user_delete_messages
        self.allowOwnerDeleteMessages = allow_owner_delete_messages
        self.allowTeamMentions = allow_team_mentions
        self.allowChannelMentions = allow_channel_mentions
