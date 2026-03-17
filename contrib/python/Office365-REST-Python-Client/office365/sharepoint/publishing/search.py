from office365.sharepoint.entity import Entity


class Search(Entity):
    @property
    def entity_type_name(self):
        return "SP.Publishing.Search"
