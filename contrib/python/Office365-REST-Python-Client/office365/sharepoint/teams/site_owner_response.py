from office365.runtime.client_value import ClientValue


class GetTeamChannelSiteOwnerResponse(ClientValue):
    def __init__(self, owner=None, secondary_contact=None):
        self.Owner = owner
        self.SecondaryContact = secondary_contact

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.GetTeamChannelSiteOwnerResponse"
