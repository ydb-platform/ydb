from office365.admin.sharepoint_settings import SharepointSettings
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class Sharepoint(Entity):
    """A container for administrative resources to manage tenant-level settings for SharePoint and OneDrive."""

    @property
    def settings(self):
        # type: () -> SharepointSettings
        """Represents the tenant-level settings for SharePoint and OneDrive."""
        return self.properties.get(
            "settings",
            SharepointSettings(
                self.context, ResourcePath("settings", self.resource_path)
            ),
        )
