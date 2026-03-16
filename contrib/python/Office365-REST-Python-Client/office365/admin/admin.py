from office365.admin.microsoft365_apps import AdminMicrosoft365Apps
from office365.admin.people_settings import PeopleAdminSettings
from office365.admin.report_settings import AdminReportSettings
from office365.admin.sharepoint import Sharepoint
from office365.entity import Entity
from office365.intune.servicecommunications.announcement import ServiceAnnouncement
from office365.runtime.paths.resource_path import ResourcePath


class Admin(Entity):
    """Entity that acts as a container for administrator functionality."""

    @property
    def microsoft365_apps(self):
        """A container for the Microsoft 365 apps admin functionality."""
        return self.properties.get(
            "microsoft365Apps",
            AdminMicrosoft365Apps(
                self.context, ResourcePath("microsoft365Apps", self.resource_path)
            ),
        )

    @property
    def sharepoint(self):
        """A container for administrative resources to manage tenant-level settings for SharePoint and OneDrive."""
        return self.properties.get(
            "sharepoint",
            Sharepoint(self.context, ResourcePath("sharepoint", self.resource_path)),
        )

    @property
    def service_announcement(self):
        """A container for service communications resources. Read-only."""
        return self.properties.get(
            "serviceAnnouncement",
            ServiceAnnouncement(
                self.context, ResourcePath("serviceAnnouncement", self.resource_path)
            ),
        )

    @property
    def report_settings(self):
        """A container for administrative resources to manage reports."""
        return self.properties.get(
            "reportSettings",
            AdminReportSettings(
                self.context, ResourcePath("reportSettings", self.resource_path)
            ),
        )

    @property
    def people(self):
        """Represents a setting to control people-related admin settings in the tenant."""
        return self.properties.get(
            "people",
            PeopleAdminSettings(
                self.context, ResourcePath("people", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "microsoft365Apps": self.microsoft365_apps,
                "serviceAnnouncement": self.service_announcement,
                "reportSettings": self.report_settings,
            }
            default_value = property_mapping.get(name, None)
        return super(Admin, self).get_property(name, default_value)
