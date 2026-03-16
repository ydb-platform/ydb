from office365.sharepoint.entity import Entity


class ClickManager(Entity):
    @property
    def entity_type_name(self):
        return "SP.PageInstrumentation.ClickManager"
