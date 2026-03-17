from office365.runtime.client_value import ClientValue


class CollaborationInsightsOverview(ClientValue):
    """"""

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.SharePoint.Administration.TenantAdmin.CollaborationInsightsOverview"
