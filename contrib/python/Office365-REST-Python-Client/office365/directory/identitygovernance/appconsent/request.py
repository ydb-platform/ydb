from typing import Optional

from office365.directory.identitygovernance.appconsent.request_scope import (
    AppConsentRequestScope,
)
from office365.directory.identitygovernance.userconsent.request_collection import (
    UserConsentRequestCollection,
)
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath


class AppConsentRequest(Entity):
    """
    Represents the request that a user creates when they request the tenant admin for consent to access an app or
    to grant permissions to an app. The details include the app that the user wants access to be granted to on their
    behalf and the permissions that the user is requesting.

    The user can create a consent request when an app or a permission requires admin authorization and only when
    the admin consent workflow is enabled.
    """

    def __str__(self):
        return self.app_display_name or self.entity_type_name

    def __repr__(self):
        return self.app_id or self.entity_type_name

    @property
    def app_display_name(self):
        # type: () -> Optional[str]
        """Display name of the application object on which this extension property is defined. Read-only"""
        return self.properties.get("appDisplayName", None)

    @property
    def app_id(self):
        # type: () -> Optional[str]
        """The identifier of the application"""
        return self.properties.get("appId", None)

    @property
    def pending_scopes(self):
        # type: () -> ClientValueCollection[AppConsentRequestScope]
        """A list of pending scopes waiting for approval. Required."""
        return self.properties.get(
            "pendingScopes", ClientValueCollection(AppConsentRequestScope)
        )

    @property
    def user_consent_requests(self):
        """A list of pending user consent requests."""
        return self.properties.get(
            "userConsentRequests",
            UserConsentRequestCollection(
                self.context, ResourcePath("userConsentRequests", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "pendingScopes": self.pending_scopes,
                "userConsentRequests": self.user_consent_requests,
            }
            default_value = property_mapping.get(name, None)
        return super(AppConsentRequest, self).get_property(name, default_value)
