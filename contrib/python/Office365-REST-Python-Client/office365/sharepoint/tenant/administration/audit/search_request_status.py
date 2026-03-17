from office365.runtime.client_value import ClientValue


class AuditSearchRequestStatus(ClientValue):
    @property
    def entity_type_name(self):
        return (
            "Microsoft.SharePoint.Administration.TenantAdmin.AuditSearchRequestStatus"
        )
