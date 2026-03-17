from office365.entity import Entity
from office365.entity_collection import EntityCollection


class IdentityUserFlowAttributeAssignment(Entity):
    """Update the properties of a identityUserFlowAttributeAssignment object."""

    pass


class IdentityUserFlowAttributeAssignmentCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(IdentityUserFlowAttributeAssignmentCollection, self).__init__(
            context, IdentityUserFlowAttributeAssignment, resource_path
        )
