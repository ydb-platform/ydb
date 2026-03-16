from typing import Optional

from office365.directory.authentication.methods.method import AuthenticationMethod


class Fido2AuthenticationMethod(AuthenticationMethod):
    """Representation of a FIDO2 security key registered to a user. FIDO2 is a sign-in authentication method."""

    @property
    def model(self):
        # type: () -> Optional[str]
        """The manufacturer-assigned model of the FIDO2 security key."""
        return self.properties.get("model", None)
