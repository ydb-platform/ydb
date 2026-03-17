from office365.runtime.client_value import ClientValue


class SiteCreationData(ClientValue):
    def __init__(self, count=None, site_creation_source_guid=None):
        self.Count = count
        self.SiteCreationSourceGuid = site_creation_source_guid

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SiteCreationData"
