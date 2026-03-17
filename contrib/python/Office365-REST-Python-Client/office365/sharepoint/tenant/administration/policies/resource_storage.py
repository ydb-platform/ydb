from office365.runtime.client_value import ClientValue


class PolicyResourceStorage(ClientValue):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Administration.TenantAdmin.PolicyResourceStorage"
