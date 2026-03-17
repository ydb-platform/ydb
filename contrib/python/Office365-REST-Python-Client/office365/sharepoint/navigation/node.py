from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.translation.user_resource import UserResource


class NavigationNode(Entity):
    """
    Represents the URL to a specific navigation node and provides access to properties and methods for
    manipulating the ordering of the navigation node in a navigation node collection.
    """

    def __str__(self):
        return self.title or self.entity_type_name

    def __repr__(self):
        return self.url or self.entity_type_name

    @property
    def children(self):
        # type: () -> 'NavigationNodeCollection'
        """Gets the collection of child nodes of the navigation node."""
        from office365.sharepoint.navigation.node_collection import (
            NavigationNodeCollection,
        )

        return self.properties.get(
            "Children",
            NavigationNodeCollection(
                self.context, ResourcePath("Children", self.resource_path)
            ),
        )

    @property
    def title(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the anchor text for the navigation node link."""
        return self.properties.get("Title", None)

    @title.setter
    def title(self, value):
        """Sets a value that specifies the anchor text for the navigation node link."""
        self.set_property("Title", value)

    @property
    def url(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the URL stored with the navigation node."""
        return self.properties.get("Url", None)

    @url.setter
    def url(self, value):
        # type: (str) -> None
        """Sets a value that specifies the URL stored with the navigation node."""
        self.set_property("Url", value)

    @property
    def is_visible(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies the anchor text for the navigation node link."""
        return self.properties.get("isVisible", None)

    @property
    def is_external(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("isExternal", None)

    @property
    def parent_collection(self):
        # type: () -> 'NavigationNodeCollection'
        return self._parent_collection

    @property
    def title_resource(self):
        """Represents the title of this node."""
        return self.properties.get(
            "TitleResource",
            UserResource(
                self.context, ResourcePath("TitleResource", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"TitleResource": self.title_resource}
            default_value = property_mapping.get(name, None)
        return super(NavigationNode, self).get_property(name, default_value)
