from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.marketplace.corporatecuratedgallery.addins.instance_info import (
    SPAddinInstanceInfo,
)


class SPAvailableAddinsResponse(ClientValue):
    def __init__(self, addins=None):
        self.addins = ClientValueCollection(SPAddinInstanceInfo, addins)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Marketplace.CorporateCuratedGallery.SPAvailableAddinsResponse"
