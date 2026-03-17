from office365.directory.identitygovernance.privilegedaccess.group import (
    PrivilegedAccessGroup,
)
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class PrivilegedAccessRoot(Entity):
    """Represents the entry point for resources related to Privileged Identity Management (PIM)."""

    @property
    def group(self):
        """A list of pending user consent requests."""
        return self.properties.get(
            "group",
            PrivilegedAccessGroup(
                self.context, ResourcePath("group", self.resource_path)
            ),
        )
