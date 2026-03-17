from datetime import datetime
from typing import Optional

from office365.directory.authentication.methods.method import AuthenticationMethod


class PasswordAuthenticationMethod(AuthenticationMethod):
    """A representation of a user's password. For security, the password itself will never be returned in the object,
    but action can be taken to reset a password."""

    @property
    def created_datetime(self):
        # type: () -> datetime
        """The date and time when this password was last updated. This property is currently not populated."""
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def password(self):
        # type: () -> Optional[str]
        """For security, the password is always returned as null from a LIST or GET operation."""
        return self.properties.get("password", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"createdDateTime": self.created_datetime}
            default_value = property_mapping.get(name, None)
        return super(PasswordAuthenticationMethod, self).get_property(
            name, default_value
        )
