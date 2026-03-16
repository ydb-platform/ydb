from office365.runtime.client_value import ClientValue


class ContentTypeEntityData(ClientValue):
    def __init__(
        self, name=None, description=None, group=None, parent_content_type_id=None
    ):
        self.Name = name
        self.Description = description
        self.Group = group
        self.ParentContentTypeId = parent_content_type_id

    @property
    def entity_type_name(self):
        return "SP.ContentTypeEntityData"
