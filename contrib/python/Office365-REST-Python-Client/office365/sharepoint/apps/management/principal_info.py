from office365.sharepoint.entity import Entity


class SPAppPrincipalInfo(Entity):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.AppManagement.SPAppPrincipalInfo"
