from office365.sharepoint.publishing.pages.page import SitePage


class SitePage3D(SitePage):
    @property
    def space_content(self):
        return self.properties.get("SpaceContent", None)

    @property
    def entity_type_name(self):
        return "SP.Publishing.SitePage3D"
