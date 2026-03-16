from office365.sharepoint.publishing.pages.page import SitePage


class TopicSitePage(SitePage):
    @property
    def entity_id(self):
        return self.properties.get("EntityId", None)

    @property
    def entity_type(self):
        return self.properties.get("EntityType", None)
