from typing import Optional

from office365.directory.object import DirectoryObject


class DirectoryRoleTemplate(DirectoryObject):
    """Represents a directory role template. A directory role template specifies the property values of a directory
    role (directoryRole)."""

    def __repr__(self):
        return self.display_name

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The display name to set for the directory role"""
        return self.properties.get("displayName", None)

    @property
    def description(self):
        # type: () -> Optional[str]
        """The display name to set for the directory role"""
        return self.properties.get("description", None)
