from typing import Optional

from office365.entity import Entity


class LongRunningOperation(Entity):
    """The status of a long-running operation."""

    @property
    def resource_location(self):
        # type: () -> Optional[str]
        """URI of the resource that the operation is performed on."""
        return self.properties.get("resourceLocation", None)

    @property
    def status_detail(self):
        # type: () -> Optional[str]
        """Details about the status of the operation."""
        return self.properties.get("statusDetail", None)
