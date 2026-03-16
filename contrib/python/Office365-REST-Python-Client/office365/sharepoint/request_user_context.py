from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class RequestUserContext(Entity):
    """The class that represents the user context for the present request. Typically found under /_api/me"""

    @property
    def current(self):
        """Gets the SP.RequestUserContext for the current request."""
        return self.properties.get(
            "Current",
            RequestUserContext(
                self.context, ResourcePath("Current", self.resource_path)
            ),
        )

    @property
    def user(self):
        """The SP.User object for the current request."""
        from office365.sharepoint.principal.users.user import User

        return self.properties.get(
            "User", User(self.context, ResourcePath("User", self.resource_path))
        )
