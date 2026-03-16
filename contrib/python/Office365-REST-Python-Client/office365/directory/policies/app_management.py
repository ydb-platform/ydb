from office365.directory.object_collection import DirectoryObjectCollection
from office365.directory.policies.base import PolicyBase
from office365.runtime.paths.resource_path import ResourcePath


class AppManagementPolicy(PolicyBase):
    """
    Restrictions on app management operations for specific applications and service principals.
    If this resource is not configured for an application or service principal, the restrictions default
    to the settings in the tenantAppManagementPolicy object.
    """

    @property
    def applies_to(self):
        """Collection of applications and service principals to which the policy is applied."""
        return self.properties.get(
            "appliesTo",
            DirectoryObjectCollection(
                self.context, ResourcePath("appliesTo", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "appliesTo": self.applies_to,
            }
            default_value = property_mapping.get(name, None)
        return super(AppManagementPolicy, self).get_property(name, default_value)
