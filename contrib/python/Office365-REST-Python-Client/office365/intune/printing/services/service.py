from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.intune.printing.services.endpoint import PrintServiceEndpoint
from office365.runtime.paths.resource_path import ResourcePath


class PrintService(Entity):
    """Represents an Azure AD tenant-specific description of a print service instance.
    Services exist for each component of the printing infrastructure (discovery, notifications, registration, and IPP)
    and have one or more endpoints."""

    def endpoints(self):
        """Endpoints that can be used to access the service."""
        return self.properties.get(
            "endpoints",
            EntityCollection(
                self.context,
                PrintServiceEndpoint,
                ResourcePath("endpoints", self.resource_path),
            ),
        )
