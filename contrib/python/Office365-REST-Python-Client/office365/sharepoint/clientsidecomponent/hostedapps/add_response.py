from office365.sharepoint.entity import Entity


class HostedAppAddResponse(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.ClientSideComponent.HostedAppAddResponse"
