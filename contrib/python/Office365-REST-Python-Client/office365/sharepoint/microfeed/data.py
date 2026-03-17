from office365.sharepoint.entity import Entity


class MicrofeedData(Entity):
    @property
    def entity_type_name(self):
        return "SP.Microfeed.MicrofeedData"
