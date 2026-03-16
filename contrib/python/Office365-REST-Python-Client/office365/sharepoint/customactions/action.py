from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.permissions.base_permissions import BasePermissions
from office365.sharepoint.translation.user_resource import UserResource


class UserCustomAction(Entity):
    """Specifies a custom action."""

    def get_property(self, name, default_value=None):
        if name == "DescriptionResource":
            default_value = self.description_resource
        elif name == "TitleResource":
            default_value = self.title_resource
        return super(UserCustomAction, self).get_property(name, default_value)

    @property
    def rights(self):
        """Specifies the permissions needed for the custom action."""
        return self.properties.get("Rights", BasePermissions())

    @property
    def description_resource(self):
        """Gets the SP.UserResource object that corresponds to the Description for this object."""
        return self.properties.get(
            "DescriptionResource",
            UserResource(
                self.context, ResourcePath("DescriptionResource", self.resource_path)
            ),
        )

    @property
    def title_resource(self):
        """Returns the UserResource object that corresponds to the Title for this object."""
        return self.properties.get(
            "TitleResource",
            UserResource(
                self.context, ResourcePath("TitleResource", self.resource_path)
            ),
        )
