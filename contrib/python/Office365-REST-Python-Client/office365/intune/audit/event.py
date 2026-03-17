from typing import Optional

from office365.entity import Entity
from office365.intune.audit.actor import AuditActor
from office365.intune.audit.resource import AuditResource
from office365.runtime.client_value_collection import ClientValueCollection


class AuditEvent(Entity):
    """A class containing the properties for Audit Event."""

    @property
    def activity(self):
        # type: () -> Optional[str]
        """Friendly name of the activity."""
        return self.properties.get("activity", None)

    @property
    def actor(self):
        """AAD user and application that are associated with the audit event."""
        return self.properties.get("actor", AuditActor())

    @property
    def resources(self):
        """Resources being modified"""
        return self.properties.get("resources", ClientValueCollection(AuditResource))
