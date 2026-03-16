from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.changes.change import Change


class ChangeList(Change):
    """
    Specifies a change on a list.

    The RelativeTime and RootFolderUrl properties are not included in the default scalar property set for this type.
    """

    def __repr__(self):
        return "List: {0}".format(self.list_id)

    @property
    def base_template(self):
        # type: () -> Optional[int]
        """An SP.ListTemplateType object that returns the list template type of the list."""
        return self.properties.get("BaseTemplate", None)

    @property
    def editor(self):
        # type: () -> Optional[str]
        """A string that returns the name of the user who modified the list."""
        return self.properties.get("Editor", None)

    @property
    def hidden(self):
        # type: () -> Optional[bool]
        """Returns a Boolean value that indicates whether a list is a hidden list."""
        return self.properties.get("Hidden", None)

    @property
    def list_id(self):
        # type: () -> Optional[str]
        """Identifies the changed list"""
        return self.properties.get("ListId", None)

    @property
    def creator(self):
        """An SP.User object that represents information about the user who created the list."""
        from office365.sharepoint.principal.users.user import User

        return self.properties.get(
            "Creator", User(self.context, ResourcePath("Creator", self.resource_path))
        )
