from office365.directory.authentication.methods.method import AuthenticationMethod
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class Authentication(Entity):
    """
    Exposes relationships that represent the authentication methods supported by Azure AD and that can configured
    for users.
    """

    @property
    def email_methods(self):
        """The email address registered to a user for authentication."""
        from office365.directory.authentication.methods.email import (
            EmailAuthenticationMethod,
        )

        return self.properties.get(
            "emailMethods",
            EntityCollection(
                self.context,
                EmailAuthenticationMethod,
                ResourcePath("emailMethods", self.resource_path),
            ),
        )

    @property
    def fido2_methods(self):
        """Represents the FIDO2 security keys registered to a user for authentication."""
        from office365.directory.authentication.methods.fido import (
            Fido2AuthenticationMethod,
        )

        return self.properties.get(
            "fido2Methods",
            EntityCollection(
                self.context,
                Fido2AuthenticationMethod,
                ResourcePath("fido2Methods", self.resource_path),
            ),
        )

    @property
    def microsoft_authenticator_methods(self):
        from office365.directory.authentication.methods.microsoft_authenticator import (
            MicrosoftAuthenticatorAuthenticationMethod,
        )

        return self.properties.get(
            "microsoftAuthenticatorMethods",
            EntityCollection(
                self.context,
                MicrosoftAuthenticatorAuthenticationMethod,
                ResourcePath("microsoftAuthenticatorMethods", self.resource_path),
            ),
        )

    @property
    def phone_methods(self):
        """The phone numbers registered to a user for authentication."""
        from office365.directory.authentication.methods.phone import (
            PhoneAuthenticationMethod,
        )

        return self.properties.get(
            "phoneMethods",
            EntityCollection(
                self.context,
                PhoneAuthenticationMethod,
                ResourcePath("phoneMethods", self.resource_path),
            ),
        )

    @property
    def password_methods(self):
        """Represents the password that's registered to a user for authentication. For security, the password itself
        will never be returned in the object, but action can be taken to reset a password.
        """
        from office365.directory.authentication.methods.password import (
            PasswordAuthenticationMethod,
        )

        return self.properties.get(
            "passwordMethods",
            EntityCollection(
                self.context,
                PasswordAuthenticationMethod,
                ResourcePath("passwordMethods", self.resource_path),
            ),
        )

    @property
    def methods(self):
        # type: () -> EntityCollection[AuthenticationMethod]
        """Represents all authentication methods registered to a user."""
        return self.properties.get(
            "methods",
            EntityCollection(
                self.context,
                AuthenticationMethod,
                ResourcePath("methods", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "emailMethods": self.email_methods,
                "fido2Methods": self.fido2_methods,
                "microsoftAuthenticatorMethods": self.microsoft_authenticator_methods,
                "passwordMethods": self.password_methods,
                "phoneMethods": self.phone_methods,
            }
            default_value = property_mapping.get(name, None)
        return super(Authentication, self).get_property(name, default_value)
