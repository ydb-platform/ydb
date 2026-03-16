from datetime import datetime

from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity


class ThreatAssessmentRequest(Entity):
    """An abstract resource type used to represent a threat assessment request item."""

    @property
    def created_by(self):
        """The threat assessment request creator."""
        return self.properties.get("createdBy", IdentitySet())

    @property
    def created_datetime(self):
        """
        The Timestamp type represents date and time information using ISO 8601 format and is always in UTC time.
        For example, midnight UTC on Jan 1, 2014 is 2014-01-01T00:00:00Z
        """
        return self.properties.get("createdDateTime", datetime.min)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdDateTime": self.created_datetime,
                "createdBy": self.created_by,
            }
            default_value = property_mapping.get(name, None)
        return super(ThreatAssessmentRequest, self).get_property(name, default_value)
