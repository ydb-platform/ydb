from datetime import datetime
from typing import Optional

from office365.entity import Entity


class DeviceLocalCredentialInfo(Entity):
    """Represents local administrator credential information for all device objects in Azure Active Directory
    that are enabled with Local Admin Password Solution (LAPS)."""

    @property
    def device_name(self):
        # type: () -> Optional[str]
        """
        Display name of the device that the local credentials are associated with.
        """
        return self.properties.get("deviceName", None)

    @property
    def last_backup_datetime(self):
        """When the local administrator account credential was backed up to Azure Active Directory."""
        return self.properties.get("lastBackupDateTime", datetime.min)

    @property
    def refresh_datetime(self):
        """When the local administrator account credential will be refreshed and backed up to Azure Active Directory."""
        return self.properties.get("refreshDateTime", datetime.min)
