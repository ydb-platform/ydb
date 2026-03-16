from typing import Optional

from office365.directory.extensions.extension import Extension
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.mail.item_body import ItemBody
from office365.runtime.paths.resource_path import ResourcePath
from office365.todo.attachments.base import AttachmentBase
from office365.todo.checklist_item import ChecklistItem
from office365.todo.linked_resource import LinkedResource


class TodoTask(Entity):
    """A todoTask represents a task, such as a piece of work or personal item, that can be tracked and completed."""

    def __str__(self):
        return self.title or self.entity_type_name

    @property
    def body(self):
        """The task body that typically contains information about the task."""
        return self.properties.get("body", ItemBody())

    @property
    def title(self):
        # type: () -> Optional[str]
        """A brief description of the task."""
        return self.properties.get("title", None)

    @property
    def attachments(self):
        # type: () -> EntityCollection[AttachmentBase]
        """A collection of file attachments for the task."""
        return self.properties.get(
            "attachments",
            EntityCollection(
                self.context,
                AttachmentBase,
                ResourcePath("attachments", self.resource_path),
            ),
        )

    @property
    def extensions(self):
        # type: () -> EntityCollection[Extension]
        """The collection of open extensions defined for the task."""
        return self.properties.get(
            "extensions",
            EntityCollection(
                self.context, Extension, ResourcePath("extensions", self.resource_path)
            ),
        )

    @property
    def checklist_items(self):
        # type: () -> EntityCollection[ChecklistItem]
        """A collection of checklistItems linked to a task."""
        return self.properties.get(
            "checklistItems",
            EntityCollection(
                self.context,
                ChecklistItem,
                ResourcePath("checklistItems", self.resource_path),
            ),
        )

    @property
    def linked_resources(self):
        # type: () -> EntityCollection[LinkedResource]
        """A collection of resources linked to the task."""
        return self.properties.get(
            "linkedResources",
            EntityCollection(
                self.context,
                LinkedResource,
                ResourcePath("linkedResources", self.resource_path),
            ),
        )

    @property
    def entity_type_name(self):
        return None

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "checklistItems": self.checklist_items,
                "linked_resources": self.linked_resources,
            }
            default_value = property_mapping.get(name, None)
        return super(TodoTask, self).get_property(name, default_value)
