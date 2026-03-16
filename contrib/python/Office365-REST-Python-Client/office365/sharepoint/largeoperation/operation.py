from office365.sharepoint.entity import Entity


class SPLargeOperation(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.LargeOperation.SPLargeOperation"
