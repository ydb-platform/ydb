from office365.sharepoint.entity import Entity


class MigrationCenterTeams(Entity):
    """"""

    @property
    def entity_type_name(self):
        return (
            "Microsoft.Online.SharePoint.MigrationCenter.Service.MigrationCenterTeams"
        )
