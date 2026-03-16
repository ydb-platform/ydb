from office365.directory.object_collection import DirectoryObjectCollection
from office365.directory.policies.base import PolicyBase
from office365.runtime.paths.resource_path import ResourcePath


class StsPolicy(PolicyBase):
    """Represents an abstract base type for policy types that control Microsoft identity platform behavior."""

    @property
    def applies_to(self):
        """"""
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
        return super(StsPolicy, self).get_property(name, default_value)
