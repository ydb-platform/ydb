from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.intune.devices.category import DeviceCategory
from office365.intune.devices.compliance.policy_state import DeviceCompliancePolicyState
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class ManagedDevice(Entity):
    """Devices that are managed or pre-enrolled through Intune"""

    def locate_device(self):
        """Locate a device"""
        qry = ServiceOperationQuery(self, "locateDevice")
        self.context.add_query(qry)
        return self

    @property
    def activation_lock_bypass_code(self):
        # type: () -> Optional[str]
        """
        The code that allows the Activation Lock on managed device to be bypassed. Default,
        is Null (Non-Default property) for this property when returned as part of managedDevice entity in LIST call.
        To retrieve actual values GET call needs to be made, with device id and included in select parameter.
        Supports: $select. $Search is not supported. Read-only. This property is read-only.
        """
        return self.properties.get("activationLockBypassCode", None)

    @property
    def device_category(self):
        """Device category"""
        return self.properties.get(
            "deviceCategory",
            DeviceCategory(
                self.context, ResourcePath("deviceCategory", self.resource_path)
            ),
        )

    @property
    def manufacturer(self):
        # type: () -> Optional[str]
        """Manufacturer of the device."""
        return self.properties.get("manufacturer", None)

    @property
    def operating_system(self):
        # type: () -> Optional[str]
        """Manufacturer of the device."""
        return self.properties.get("operatingSystem", None)

    @property
    def device_compliance_policy_states(self):
        # type: () -> EntityCollection[DeviceCompliancePolicyState]
        """Device compliance policy states for this device"""
        return self.properties.get(
            "deviceCompliancePolicyStates",
            EntityCollection(
                self.context,
                DeviceCompliancePolicyState,
                ResourcePath("deviceCompliancePolicyStates", self.resource_path),
            ),
        )

    @property
    def users(self):
        """The primary users associated with the managed device."""
        from office365.directory.users.collection import UserCollection

        return self.properties.get(
            "users",
            UserCollection(self.context, ResourcePath("users", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "deviceCategory": self.device_category,
                "deviceCompliancePolicyStates": self.device_compliance_policy_states,
            }
            default_value = property_mapping.get(name, None)
        return super(ManagedDevice, self).get_property(name, default_value)
