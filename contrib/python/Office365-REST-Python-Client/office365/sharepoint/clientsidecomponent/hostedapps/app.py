from office365.sharepoint.entity import Entity


class HostedApp(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.ClientSideComponent.HostedApp"
