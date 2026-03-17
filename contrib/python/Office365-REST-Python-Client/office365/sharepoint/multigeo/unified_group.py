from office365.sharepoint.entity import Entity


class UnifiedGroup(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MultiGeo.Service.UnifiedGroup"
