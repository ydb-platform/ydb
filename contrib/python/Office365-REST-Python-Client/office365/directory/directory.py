from office365.directory.administrative_unit import AdministrativeUnit
from office365.directory.custom_security_attribute_definition import (
    CustomSecurityAttributeDefinition,
)
from office365.directory.device_local_credential_info import DeviceLocalCredentialInfo
from office365.directory.object_collection import DirectoryObjectCollection
from office365.directory.subscriptions.company import CompanySubscription
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class Directory(Entity):
    """Represents a deleted item in the directory. When an item is deleted, it is added to the deleted items
    "container". Deleted items will remain available to restore for up to 30 days. After 30 days, the items are
    permanently deleted."""

    @property
    def device_local_credentials(self):
        """Conceptual container for user and group directory objects."""
        return self.properties.get(
            "deviceLocalCredentials",
            EntityCollection(
                self.context,
                DeviceLocalCredentialInfo,
                ResourcePath("deviceLocalCredentials", self.resource_path),
            ),
        )

    @property
    def administrative_units(self):
        """Conceptual container for user and group directory objects."""
        return self.properties.get(
            "administrativeUnits",
            EntityCollection(
                self.context,
                AdministrativeUnit,
                ResourcePath("administrativeUnits", self.resource_path),
            ),
        )

    @property
    def custom_security_attribute_definitions(self):
        """Conceptual container for customSecurityAttributeDefinition objects and their properties."""
        return self.properties.get(
            "customSecurityAttributeDefinitions",
            EntityCollection(
                self.context,
                CustomSecurityAttributeDefinition,
                ResourcePath("customSecurityAttributeDefinitions", self.resource_path),
            ),
        )

    @property
    def subscriptions(self):
        """List of commercial subscriptions that an organization acquired."""
        return self.properties.get(
            "subscriptions",
            EntityCollection(
                self.context,
                CompanySubscription,
                ResourcePath("subscriptions", self.resource_path),
            ),
        )

    def deleted_items(self, entity_type=None):
        """Recently deleted items. Read-only. Nullable."""
        if entity_type:
            return DirectoryObjectCollection(
                self.context,
                ResourcePath(
                    entity_type, ResourcePath("deletedItems", self.resource_path)
                ),
            )
        else:
            return self.properties.get(
                "deletedItems",
                DirectoryObjectCollection(
                    self.context, ResourcePath("deletedItems", self.resource_path)
                ),
            )

    @property
    def deleted_groups(self):
        """Recently deleted groups"""
        return self.deleted_items("microsoft.graph.group")

    @property
    def deleted_users(self):
        """Recently deleted users"""
        return self.deleted_items("microsoft.graph.user")

    @property
    def deleted_applications(self):
        """Recently deleted applications"""
        return self.deleted_items("microsoft.graph.application")

    @property
    def deleted_service_principals(self):
        """Recently deleted service Principals"""
        return self.deleted_items("microsoft.graph.servicePrincipal")
