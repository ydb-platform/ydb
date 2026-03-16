from office365.runtime.client_value import ClientValue


class CommunicationEntity(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Campaigns.CampaignCommunicationEntity"
