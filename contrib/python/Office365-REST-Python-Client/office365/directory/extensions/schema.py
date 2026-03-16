from office365.entity import Entity
from office365.runtime.types.collections import StringCollection


class SchemaExtension(Entity):
    """Schema extensions allow you to define a schema to extend and add strongly-typed custom data to a resource type.
    The custom data appears as a complex type on the extended resource."""

    @property
    def target_types(self):
        """
        Set of Microsoft Graph types (that can support extensions) that the schema extension can be applied to.
        Select from administrativeUnit, contact, device, event, group, message, organization, post, todoTask,
        todoTaskList, or user.
        """
        return self.properties.get("targetTypes", StringCollection())
