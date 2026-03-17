from office365.sharepoint.entity import Entity


class FavoriteLists(Entity):
    @property
    def entity_type_name(self):
        return "SP.FavoriteLists"
