from office365.runtime.client_value import ClientValue


class TeamGuestSettings(ClientValue):
    """Settings to configure whether guests can create, update, or delete channels in the team."""

    def __init__(self, allow_create_update_channels=True, allow_delete_channels=True):
        """
        :param bool allow_create_update_channels:
        :param bool allow_delete_channels:
        """
        super(TeamGuestSettings, self).__init__()
        self.allowCreateUpdateChannels = allow_create_update_channels
        self.allowDeleteChannels = allow_delete_channels
