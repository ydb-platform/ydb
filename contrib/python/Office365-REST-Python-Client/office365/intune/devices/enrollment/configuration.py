import datetime
from typing import Optional

from office365.entity import Entity


class DeviceEnrollmentConfiguration(Entity):
    """The Base Class of Device Enrollment Configuration"""

    def __str__(self):
        return self.display_name

    @property
    def created_datetime(self):
        """
        Created date time in UTC of the device enrollment configuration
        """
        return self.properties.get("createdDateTime", datetime.datetime.min)

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The display name of the device enrollment configuration"""
        return self.properties.get("displayName", None)

    def get_property(self, name, default_value=None):
        return super(DeviceEnrollmentConfiguration, self).get_property(
            name, default_value
        )
