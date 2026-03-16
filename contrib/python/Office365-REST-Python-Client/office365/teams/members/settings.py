from office365.runtime.client_value import ClientValue


class TeamMemberSettings(ClientValue):
    """Settings to configure whether members can perform certain actions, for example, create channels and add bots,
    in the team."""

    def __init__(self, allow_create_update_channels=None):
        super(TeamMemberSettings, self).__init__()
        self.allowCreateUpdateChannels = allow_create_update_channels
        self.allowDeleteChannels = True
        self.allowAddRemoveApps = True
        self.allowCreateUpdateRemoveTabs = True
        self.allowCreateUpdateRemoveConnectors = True
