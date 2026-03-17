from typing import Optional

from office365.directory.authentication.methods.method import AuthenticationMethod


class EmailAuthenticationMethod(AuthenticationMethod):
    """
    Represents an email address registered to a user. Email as an authentication method is available only for
    self-service password reset (SSPR). Users may only have one email authentication method.
    """

    @property
    def email_address(self):
        # type: () -> Optional[str]
        """The email address registered to this user."""
        return self.properties.get("emailAddress", None)
