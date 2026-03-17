from office365.runtime.client_value import ClientValue


class TenantCdnUrl(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.TenantCdn.TenantCdnUrl"
