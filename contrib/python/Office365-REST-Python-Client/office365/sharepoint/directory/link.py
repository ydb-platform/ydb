from office365.sharepoint.entity import Entity


class Link(Entity):
    @property
    def entity_type_name(self):
        return "SP.Directory.Link"
