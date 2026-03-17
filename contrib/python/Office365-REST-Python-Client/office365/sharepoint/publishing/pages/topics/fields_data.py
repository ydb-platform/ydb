from office365.sharepoint.publishing.pages.fields_data import SitePageFieldsData


class TopicPageFieldsData(SitePageFieldsData):
    def __init__(self, entity_id=None, entity_relations=None):
        super(TopicPageFieldsData, self).__init__()
        self.EntityId = entity_id
        self.EntityRelations = entity_relations

    @property
    def entity_type_name(self):
        return "SP.Publishing.TopicPageFieldsData"
