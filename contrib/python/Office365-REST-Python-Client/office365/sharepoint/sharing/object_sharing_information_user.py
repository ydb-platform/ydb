from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.principal.principal import Principal
from office365.sharepoint.principal.users.user import User


class ObjectSharingInformationUser(Entity):
    """Contains information about a principal with whom a securable object is shared. It can be a user or a group."""

    @property
    def email(self):
        # type: () -> Optional[str]
        """Specifies the email address for the user."""
        return self.properties.get("Email", None)

    @property
    def sip_address(self):
        # type: () -> Optional[str]
        """Specifies the SIP address of the user."""
        return self.properties.get("SipAddress", None)

    @property
    def login_name(self):
        # type: () -> Optional[str]
        """Specifies the login name for the principal."""
        return self.properties.get("LoginName", None)

    def principal(self):
        """The principal with whom a securable object is shared. It is either a user or a group."""
        return self.properties.get(
            "Principal",
            Principal(self.context, ResourcePath("Principal", self.resource_path)),
        )

    def user(self):
        """Specifies the user with whom a securable object is shared."""
        return self.properties.get(
            "User", User(self.context, ResourcePath("User", self.resource_path))
        )
