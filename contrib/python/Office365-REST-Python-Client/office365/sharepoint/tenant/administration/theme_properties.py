from office365.sharepoint.entity import Entity


class ThemeProperties(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantManagement.ThemeProperties"
