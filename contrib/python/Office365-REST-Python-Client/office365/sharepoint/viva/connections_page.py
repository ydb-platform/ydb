from office365.sharepoint.entity import Entity


class VivaConnectionsPage(Entity):

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.EmployeeEngagement.VivaConnectionsPage"
