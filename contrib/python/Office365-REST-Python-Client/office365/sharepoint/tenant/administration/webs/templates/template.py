from office365.runtime.client_value import ClientValue


class SPOTenantWebTemplate(ClientValue):
    def __init__(self):
        super(SPOTenantWebTemplate, self).__init__()

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SPOTenantWebTemplate"
