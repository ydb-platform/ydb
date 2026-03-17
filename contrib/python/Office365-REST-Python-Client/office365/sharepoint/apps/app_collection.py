from office365.sharepoint.entity import Entity


class AppCollection(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.AppServices.AppCollection"
