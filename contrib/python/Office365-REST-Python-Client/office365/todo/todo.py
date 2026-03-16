from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.todo.tasks.list_collection import TodoTaskListCollection


class Todo(Entity):
    """Represents the To Do services available to a user."""

    @property
    def lists(self):
        # type: () -> TodoTaskListCollection
        """The task lists in the users mailbox."""
        return self.properties.get(
            "lists",
            TodoTaskListCollection(
                self.context, ResourcePath("lists", self.resource_path)
            ),
        )
