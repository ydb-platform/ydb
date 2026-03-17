from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.permissions.roles.definitions.collection import (
    RoleDefinitionCollection,
)
from office365.sharepoint.principal.principal import Principal


class RoleAssignment(Entity):
    """An association between a principal or a site group and a role definition."""

    @property
    def principal_id(self):
        """Specifies the identifier of the user or group corresponding to the role assignment."""
        return self.properties.get("PrincipalId", None)

    @property
    def member(self):
        """Specifies the user or group corresponding to the role assignment."""
        return self.properties.get(
            "Member",
            Principal(self.context, ResourcePath("Member", self.resource_path)),
        )

    @property
    def role_definition_bindings(self):
        """Specifies a collection of role definitions for this role assignment."""
        return self.properties.get(
            "RoleDefinitionBindings",
            RoleDefinitionCollection(
                self.context, ResourcePath("RoleDefinitionBindings", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "RoleDefinitionBindings": self.role_definition_bindings,
            }
            default_value = property_mapping.get(name, None)
        return super(RoleAssignment, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(RoleAssignment, self).set_property(name, value, persist_changes)
        if self._resource_path is None:
            if name == "PrincipalId":
                self._resource_path = self.parent_collection.get_by_principal_id(
                    value
                ).resource_path
        return self
