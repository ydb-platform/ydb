from office365.sharepoint.entity import Entity


class PersonMagazine(Entity):
    @property
    def entity_type_name(self):
        return "SP.Publishing.PersonMagazine"
