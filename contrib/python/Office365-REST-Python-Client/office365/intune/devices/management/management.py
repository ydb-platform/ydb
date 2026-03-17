from office365.directory.rolemanagement.role_permission import RolePermission
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.intune.audit.event_collection import AuditEventCollection
from office365.intune.brand import IntuneBrand
from office365.intune.devices.category import DeviceCategory
from office365.intune.devices.enrollment.configuration import (
    DeviceEnrollmentConfiguration,
)
from office365.intune.devices.managed import ManagedDevice
from office365.intune.devices.management.reports.reports import DeviceManagementReports
from office365.intune.devices.management.terms_and_conditions import TermsAndConditions
from office365.intune.devices.management.virtual_endpoint import VirtualEndpoint
from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery


class DeviceManagement(Entity):
    """
    The deviceManagement resource represents a tenant's collection device identities that have been pre-staged in
    Intune, and the enrollment profiles that may be assigned to device identities that support pre-enrollment
    configuration.
    """

    def get_effective_permissions(self, scope=None):
        """Retrieves the effective permissions of the currently authenticated user"""
        return_type = ClientResult(self.context, ClientValueCollection(RolePermission))
        # params = {"scope": scope}
        qry = FunctionQuery(self, "getEffectivePermissions", None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def audit_events(self):
        """"""
        return self.properties.get(
            "auditEvents",
            AuditEventCollection(
                self.context, ResourcePath("auditEvents", self.resource_path)
            ),
        )

    @property
    def virtual_endpoint(self):
        """"""
        return self.properties.get(
            "virtualEndpoint",
            VirtualEndpoint(
                self.context, ResourcePath("virtualEndpoint", self.resource_path)
            ),
        )

    @property
    def terms_and_conditions(self):
        """"""
        return self.properties.get(
            "termsAndConditions",
            EntityCollection(
                self.context,
                TermsAndConditions,
                ResourcePath("termsAndConditions", self.resource_path),
            ),
        )

    @property
    def device_categories(self):
        """"""
        return self.properties.get(
            "deviceCategories",
            EntityCollection(
                self.context,
                DeviceCategory,
                ResourcePath("deviceCategories", self.resource_path),
            ),
        )

    @property
    def device_enrollment_configurations(self):
        """"""
        return self.properties.get(
            "deviceEnrollmentConfigurations",
            EntityCollection(
                self.context,
                DeviceEnrollmentConfiguration,
                ResourcePath("deviceEnrollmentConfigurations", self.resource_path),
            ),
        )

    @property
    def intune_brand(self):
        return self.properties.get("intuneBrand", IntuneBrand())

    @property
    def managed_devices(self):
        """"""
        return self.properties.get(
            "managedDevices",
            EntityCollection(
                self.context,
                ManagedDevice,
                ResourcePath("managedDevices", self.resource_path),
            ),
        )

    @property
    def reports(self):
        """"""
        return self.properties.get(
            "reports",
            DeviceManagementReports(
                self.context, ResourcePath("reports", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "auditEvents": self.audit_events,
                "deviceCategories": self.device_categories,
                "deviceEnrollmentConfigurations": self.device_enrollment_configurations,
                "intuneBrand": self.intune_brand,
                "managedDevices": self.managed_devices,
                "termsAndConditions": self.terms_and_conditions,
                "virtualEndpoint": self.virtual_endpoint,
            }
            default_value = property_mapping.get(name, None)
        return super(DeviceManagement, self).get_property(name, default_value)
