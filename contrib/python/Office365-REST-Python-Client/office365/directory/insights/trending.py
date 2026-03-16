from datetime import datetime
from typing import Optional

from office365.directory.insights.resource_reference import ResourceReference
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class Trending(Entity):
    """
    Rich relationship connecting a user to documents that are trending around the user (are relevant to the user).
    OneDrive files, and files stored on SharePoint team sites can trend around the user.
    """

    @property
    def last_modified_datetime(self):
        # type: () -> Optional[datetime]
        """Gets date and time the item was last modified."""
        return self.properties.get("lastModifiedDateTime", datetime.min)

    @property
    def resource_reference(self):
        # type: () -> ResourceReference
        """Reference properties of the trending document, such as the url and type of the document."""
        return self.properties.get("resourceReference", ResourceReference())

    @property
    def resource(self):
        """Used for navigating to the trending document."""
        return self.properties.get(
            "resource",
            Entity(self.context, ResourcePath("resource", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "lastModifiedDateTime": self.last_modified_datetime,
                "resourceReference": self.resource_reference,
            }
            default_value = property_mapping.get(name, None)
        return super(Trending, self).get_property(name, default_value)
