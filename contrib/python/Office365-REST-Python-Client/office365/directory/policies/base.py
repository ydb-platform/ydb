from typing import Optional

from office365.directory.object import DirectoryObject


class PolicyBase(DirectoryObject):
    """Represents an abstract base type for policy types to inherit from"""

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """Display name for this policy"""
        return self.properties.get("displayName", None)
