from typing import Optional

from office365.directory.permissions.identity_set import IdentitySet
from office365.entity_collection import EntityCollection
from office365.onedrive.base_item import BaseItem
from office365.onedrive.driveitems.conflict_behavior import ConflictBehavior
from office365.onedrive.driveitems.driveItem import DriveItem
from office365.onedrive.driveitems.system_facet import SystemFacet
from office365.onedrive.drives.quota import Quota
from office365.onedrive.internal.paths.root import RootPath
from office365.onedrive.lists.list import List
from office365.onedrive.sharepoint_ids import SharePointIds
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.runtime.queries.function import FunctionQuery


class Drive(BaseItem):
    """The drive resource is the top level object representing a user's OneDrive or a document library in
    SharePoint."""

    def create_bundle(self, name, children=None):
        """
        Add a new bundle to the user's drive.

        :param str name: Bundle name
        :param list children: the list of file facets if creating a files or a folder facets if creating a folder
            or a remoteItem facets if adding a shared folders
        """
        return_type = DriveItem(self.context)
        self.bundles.add_child(return_type)
        payload = {
            "name": name,
            "@microsoft.graph.conflictBehavior": ConflictBehavior.Rename,
            "bundle": {},
            "children": [{"id": item_id} for item_id in children],
        }
        qry = CreateEntityQuery(self.bundles, payload, return_type)
        self.context.add_query(qry)
        return return_type

    def search(self, query_text):
        """Search the hierarchy of items for items matching a query.

        :type query_text: str
        """
        return_type = EntityCollection(
            self.context, DriveItem, self.items.resource_path
        )
        qry = FunctionQuery(self, "search", {"q": query_text}, return_type)
        self.context.add_query(qry)
        return return_type

    def recent(self):
        """
        List a set of items that have been recently used by the signed in user.
        This collection includes items that are in the user's drive as well as items
        they have access to from other drives.
        """
        return_type = EntityCollection(
            self.context, DriveItem, self.items.resource_path
        )
        qry = FunctionQuery(self, "recent", None, return_type)
        self.context.add_query(qry)
        return return_type

    def shared_with_me(self):
        """Retrieve a collection of DriveItem resources that have been shared with the owner of the Drive."""
        return_type = EntityCollection(
            self.context, DriveItem, self.items.resource_path
        )
        qry = FunctionQuery(self, "sharedWithMe", None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def drive_type(self):
        # type: () -> Optional[str]
        """
        Describes the type of drive represented by this resource. OneDrive personal drives will return personal.
        OneDrive for Business will return business. SharePoint document libraries will return documentLibrary.
        """
        return self.properties.get("driveType", None)

    @property
    def sharepoint_ids(self):
        """Returns identifiers useful for SharePoint REST compatibility."""
        return self.properties.get("sharepointIds", SharePointIds())

    @property
    def system(self):
        """Optional. The user account that owns the drive. Read-only."""
        return self.properties.get("system", SystemFacet())

    @property
    def owner(self):
        """If present, indicates that this is a system-managed drive. Read-only."""
        return self.properties.get("owner", IdentitySet())

    @property
    def root(self):
        # type: () -> DriveItem
        """The root folder of the drive."""
        return self.properties.get(
            "root",
            DriveItem(
                self.context, RootPath(self.resource_path, self.items.resource_path)
            ),
        )

    @property
    def list(self):
        # type: () -> List
        """For drives in SharePoint, the underlying document library list."""
        return self.properties.get(
            "list", List(self.context, ResourcePath("list", self.resource_path))
        )

    @property
    def bundles(self):
        # type: () -> EntityCollection[DriveItem]
        """Bundle metadata, if the item is a bundle."""
        return self.properties.get(
            "bundles",
            EntityCollection(
                self.context, DriveItem, ResourcePath("bundles", self.resource_path)
            ),
        )

    @property
    def items(self):
        # type: () -> EntityCollection[DriveItem]
        """All items contained in the drive."""
        return self.properties.get(
            "items",
            EntityCollection(
                self.context, DriveItem, ResourcePath("items", self.resource_path)
            ),
        )

    @property
    def following(self):
        # type: () -> EntityCollection[DriveItem]
        """The list of items the user is following. Only in OneDrive for Business."""
        return self.properties.get(
            "following",
            EntityCollection(
                self.context, DriveItem, ResourcePath("following", self.resource_path)
            ),
        )

    @property
    def quota(self):
        """Optional. Information about the drive's storage space quota. Read-only."""
        return self.properties.get("quota", Quota())

    @property
    def special(self):
        # type: () -> EntityCollection[DriveItem]
        """Collection of auth folders available in OneDrive. Read-only. Nullable."""
        return self.properties.get(
            "special",
            EntityCollection(
                self.context, DriveItem, ResourcePath("special", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        return super(Drive, self).get_property(name, default_value)
