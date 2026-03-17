from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.portal.linkedsites.contract import LinkedSiteContract


class LinkedSitesListContract(ClientValue):
    def __init__(self, linked_sites=ClientValueCollection(LinkedSiteContract)):
        super().__init__()
        self.LinkedSites = linked_sites

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.LinkedSitesListContract"
