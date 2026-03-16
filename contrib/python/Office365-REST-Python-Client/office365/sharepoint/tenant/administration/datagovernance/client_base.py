from office365.sharepoint.entity import Entity


class SPDataGovernanceRestApiClientBase(Entity):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SPDataGovernanceRestApiClientBase"
