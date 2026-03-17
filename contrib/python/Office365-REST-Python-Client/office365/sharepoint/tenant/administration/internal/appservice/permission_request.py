from office365.sharepoint.entity import Entity


class SPOWebAppServicePrincipalPermissionRequest(Entity):
    """ """

    @property
    def client_component_item_unique_id(self):
        return self.properties.get("ClientComponentItemUniqueId", False)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.Internal.SPOWebAppServicePrincipalPermissionRequest"
