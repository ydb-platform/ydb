from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.intune.devices.managed_app_registration import ManagedAppRegistration
from office365.runtime.paths.resource_path import ResourcePath


class DeviceAppManagement(Entity):
    """Singleton entity that acts as a container for all device and app management functionality."""

    @property
    def managed_app_registrations(self):
        """"""
        return self.properties.get(
            "managedAppRegistrations",
            EntityCollection(
                self.context,
                ManagedAppRegistration,
                ResourcePath("managedAppRegistrations", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "managedAppRegistrations": self.managed_app_registrations,
            }
            default_value = property_mapping.get(name, None)
        return super(DeviceAppManagement, self).get_property(name, default_value)
