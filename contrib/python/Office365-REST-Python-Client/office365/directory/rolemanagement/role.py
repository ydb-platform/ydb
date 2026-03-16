from typing import Optional

from office365.directory.object import DirectoryObject
from office365.directory.permissions.scoped_role_membership import ScopedRoleMembership
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class DirectoryRole(DirectoryObject):
    """Represents an Azure AD directory role. Azure AD directory roles are also known as administrator roles"""

    def __repr__(self):
        return self.id or self.entity_type_name

    def __str__(self):
        return "Name: {0}".format(self.display_name)

    @property
    def description(self):
        # type: () -> Optional[str]
        """The description for the directory role."""
        return self.properties.get("Description", None)

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The display name for the directory role."""
        return self.properties.get("displayName", None)

    @property
    def members(self):
        """Users that are members of this directory role."""
        from office365.directory.object_collection import DirectoryObjectCollection

        return self.properties.get(
            "members",
            DirectoryObjectCollection(
                self.context, ResourcePath("members", self.resource_path)
            ),
        )

    @property
    def scoped_members(self):
        """Members of this directory role that are scoped to administrative units."""

        return self.properties.get(
            "scopedMembers",
            EntityCollection(
                self.context,
                ScopedRoleMembership,
                ResourcePath("scopedMembers", self.resource_path),
            ),
        )
