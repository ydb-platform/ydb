from office365.directory.policies.conditional_access import ConditionalAccessPolicy
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class ConditionalAccessRoot(Entity):
    """The conditionalAccessRoot resource is the entry point for the Conditional Access (CA) object model.
    It doesn't contain any usable properties."""

    @property
    def authentication_strength(self):
        """The entry point for the Conditional Access (CA) object model."""
        return self.properties.get(
            "authenticationStrength",
            ConditionalAccessRoot(
                self.context, ResourcePath("authenticationStrength", self.resource_path)
            ),
        )

    @property
    def policies(self):
        """Returns a collection of the specified Conditional Access (CA) policies."""
        return self.properties.get(
            "policies",
            EntityCollection(
                self.context,
                ConditionalAccessPolicy,
                ResourcePath("policies", self.resource_path),
            ),
        )
