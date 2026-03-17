from office365.sharepoint.entity import Entity


class MigrationCredential(Entity):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MigrationCenter.Service.MigrationCredential"
