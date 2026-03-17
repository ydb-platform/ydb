from office365.runtime.client_value import ClientValue
from office365.sharepoint.publishing.pages.fields_data import SitePageFieldsData


class CampaignPublicationMailDraftData(ClientValue):
    """ """

    @property
    def entity_type_name(self):
        return "SP.Publishing.CampaignPublicationMailDraftData"


class CampaignPublicationFieldsData(SitePageFieldsData):
    """ """

    @property
    def entity_type_name(self):
        return "SP.Publishing.CampaignPublicationFieldsData"
