from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.navigation.node_collection import NavigationNodeCollection


class Navigation(Entity):
    """Represents navigation operations at the site collection level."""

    @property
    def use_shared(self):
        """Gets a value that specifies whether the site inherits navigation."""
        return self.properties.get("UseShared", None)

    @use_shared.setter
    def use_shared(self, value):
        """Sets a value that specifies whether the site inherits navigation."""
        self.set_property("UseShared", value)

    @property
    def quick_launch(self):
        """Gets a value that collects navigation nodes corresponding to links in the Quick Launch area of the site."""
        return self.properties.get(
            "QuickLaunch",
            NavigationNodeCollection(
                self.context, ResourcePath("QuickLaunch", self.resource_path)
            ),
        )

    @property
    def top_navigation_bar(self):
        """Gets a value that collects navigation nodes corresponding to links in the top navigation bar of the site."""
        return self.properties.get(
            "TopNavigationBar",
            NavigationNodeCollection(
                self.context, ResourcePath("TopNavigationBar", self.resource_path)
            ),
        )

    def set_property(self, name, value, persist_changes=True):
        super(Navigation, self).set_property(name, value, persist_changes)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "TopNavigationBar": self.top_navigation_bar,
                "QuickLaunch": self.quick_launch,
            }
            default_value = property_mapping.get(name, None)
        return super(Navigation, self).get_property(name, default_value)
