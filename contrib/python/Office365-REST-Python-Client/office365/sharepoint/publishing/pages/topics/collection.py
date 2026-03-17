from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.publishing.pages.topics.topic import TopicSitePage


class TopicPageCollection(EntityCollection[TopicSitePage]):
    def __init__(self, context, resource_path=None):
        super(TopicPageCollection, self).__init__(context, TopicSitePage, resource_path)
