from office365.sharepoint.entity import Entity


class StructuralNavigationCacheWrapper(Entity):
    @property
    def entity_type_name(self):
        return "SP.Publishing.Navigation.StructuralNavigationCacheWrapper"
