from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class ItemActivity(Entity):
    """
    The itemActivity resource provides information about activities that took place on an item or within a container.
    Currently only available on SharePoint and OneDrive for Business.
    """

    @property
    def actor(self):
        """Identity of who performed the action."""
        return self.properties.get("actor", IdentitySet())

    @property
    def drive_item(self):
        """Exposes the driveItem that was the target of this activity."""
        from office365.onedrive.driveitems.driveItem import DriveItem

        return self.properties.get(
            "driveItem",
            DriveItem(self.context, ResourcePath("driveItem", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "driveItem": self.drive_item,
            }
            default_value = property_mapping.get(name, None)
        return super(ItemActivity, self).get_property(name, default_value)
