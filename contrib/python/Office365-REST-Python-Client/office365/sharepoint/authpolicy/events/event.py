from office365.sharepoint.entity import Entity


class SPAuthEvent(Entity):

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.AuthPolicy.Events.SPAuthEvent"
