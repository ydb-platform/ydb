from office365.directory.permissions.identity_set import IdentitySet
from office365.entity_collection import EntityCollection
from office365.onedrive.base_item import BaseItem
from office365.onedrive.driveitems.driveItem import DriveItem
from office365.onedrive.internal.paths.root import RootPath
from office365.onedrive.listitems.list_item import ListItem
from office365.onedrive.lists.list import List
from office365.onedrive.permissions.permission import Permission
from office365.onedrive.sites.site import Site
from office365.runtime.paths.resource_path import ResourcePath


class SharedDriveItem(BaseItem):
    """The sharedDriveItem resource is returned when using the Shares API to access a shared driveItem."""

    @property
    def items(self):
        # type: () -> EntityCollection[DriveItem]
        """All driveItems contained in the sharing root. This collection cannot be enumerated."""
        return self.properties.get(
            "items",
            EntityCollection(
                self.context, DriveItem, ResourcePath("items", self.resource_path)
            ),
        )

    @property
    def list_item(self):
        # type: () -> ListItem
        """Used to access the underlying listItem"""
        return self.properties.get(
            "listItem",
            ListItem(self.context, ResourcePath("listItem", self.resource_path)),
        )

    @property
    def list(self):
        # type: () -> List
        """Used to access the underlying list"""
        return self.properties.get(
            "list", List(self.context, ResourcePath("list", self.resource_path))
        )

    @property
    def drive_item(self):
        # type: () -> DriveItem
        """Used to access the underlying driveItem"""
        return self.properties.get(
            "driveItem",
            DriveItem(self.context, ResourcePath("driveItem", self.resource_path)),
        )

    @property
    def owner(self):
        """Information about the owner of the shared item being referenced."""
        return self.properties.get("owner", IdentitySet())

    @property
    def root(self):
        """Used to access the underlying driveItem.
        Deprecated -- use driveItem instead.
        """
        return self.properties.get(
            "root",
            DriveItem(
                self.context, RootPath(self.resource_path, self.items.resource_path)
            ),
        )

    @property
    def site(self):
        """Used to access the underlying site"""
        return self.properties.get(
            "site", Site(self.context, ResourcePath("site", self.resource_path))
        )

    @property
    def permission(self):
        """Used to access the permission representing the underlying sharing link"""
        return self.properties.get(
            "permission",
            Permission(self.context, ResourcePath("permission", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "driveItem": self.drive_item,
                "listItem": self.list_item,
            }
            default_value = property_mapping.get(name, None)
        super(SharedDriveItem, self).get_property(name, default_value)
