from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.intune.servicecommunications.health.health import ServiceHealth
from office365.intune.servicecommunications.issues.issue import ServiceHealthIssue
from office365.intune.servicecommunications.messages.update import ServiceUpdateMessage
from office365.runtime.paths.resource_path import ResourcePath


class ServiceAnnouncement(Entity):
    """A top-level container for service communications resources."""

    @property
    def health_overviews(self):
        """Get the serviceHealth resources from the healthOverviews navigation property."""
        return self.properties.get(
            "healthOverviews",
            EntityCollection(
                self.context,
                ServiceHealth,
                ResourcePath("healthOverviews", self.resource_path),
            ),
        )

    @property
    def issues(self):
        """Get the serviceHealthIssue resources from the issues navigation property."""
        return self.properties.get(
            "issues",
            EntityCollection(
                self.context,
                ServiceHealthIssue,
                ResourcePath("issues", self.resource_path),
            ),
        )

    @property
    def messages(self):
        """Get the serviceUpdateMessage resources from the messages navigation property."""
        return self.properties.get(
            "messages",
            EntityCollection(
                self.context,
                ServiceUpdateMessage,
                ResourcePath("messages", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"healthOverviews": self.health_overviews}
            default_value = property_mapping.get(name, None)
        return super(ServiceAnnouncement, self).get_property(name, default_value)
