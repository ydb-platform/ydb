from typing import Optional

from office365.directory.extensions.extension import Extension
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.todo.tasks.task import TodoTask


class TodoTaskList(Entity):
    """A list in Microsoft To Do that contains one or more todoTask resources."""

    def __str__(self):
        return self.display_name or self.entity_type_name

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The name of the task list."""
        return self.properties.get("displayName", None)

    @property
    def extensions(self):
        # type: () -> EntityCollection[Extension]
        """The collection of open extensions defined for the task list."""
        return self.properties.get(
            "extensions",
            EntityCollection(
                self.context, Extension, ResourcePath("extensions", self.resource_path)
            ),
        )

    @property
    def tasks(self):
        # type: () -> EntityCollection[TodoTask]
        """The tasks in this task list."""
        return self.properties.get(
            "tasks",
            EntityCollection(
                self.context, TodoTask, ResourcePath("tasks", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        return None
