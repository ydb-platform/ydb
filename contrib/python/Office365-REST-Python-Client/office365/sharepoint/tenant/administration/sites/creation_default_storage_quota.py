from office365.runtime.client_value import ClientValue


class SiteCreationDefaultStorageQuota(ClientValue):
    def __init__(self, IsReadOnly=None, Value=None):
        # type: (bool, int) -> None
        self.IsReadOnly = IsReadOnly
        self.Value = Value

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SiteCreationDefaultStorageQuota"
