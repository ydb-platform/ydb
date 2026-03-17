from datetime import datetime
from typing import Optional

from office365.directory.audit.provisioning.service_principal import (
    ProvisioningServicePrincipal,
)
from office365.entity import Entity


class ProvisioningObjectSummary(Entity):
    """Represents an action performed by the Azure AD Provisioning service and its associated properties."""

    @property
    def activity_datetime(self):
        """Represents date and time information using ISO 8601 format and is always in UTC time."""
        return self.properties.get("activityDateTime", datetime.min)

    @property
    def change_id(self):
        # type: () -> Optional[str]
        """Unique ID of this change in this cycle. Supports $filter (eq, contains)."""
        return self.properties.get("changeId", None)

    @property
    def cycle_id(self):
        # type: () -> Optional[str]
        """Unique ID per job iteration. Supports $filter (eq, contains)."""
        return self.properties.get("cycleId", None)

    @property
    def duration_in_milliseconds(self):
        # type: () -> Optional[int]
        """Indicates how long this provisioning action took to finish. Measured in milliseconds."""
        return self.properties.get("durationInMilliseconds", None)

    @property
    def service_principal(self):
        """Represents the service principal used for provisioning."""
        return self.properties.get("servicePrincipal", ProvisioningServicePrincipal())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "activityDateTime": self.activity_datetime,
                "servicePrincipal": self.service_principal,
            }
            default_value = property_mapping.get(name, None)
        return super(ProvisioningObjectSummary, self).get_property(name, default_value)
