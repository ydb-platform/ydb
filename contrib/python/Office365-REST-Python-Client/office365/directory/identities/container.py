from office365.directory.authentication.conditions.event_listener import (
    AuthenticationEventListener,
)
from office365.directory.identities.api_connector import IdentityApiConnector
from office365.directory.identities.conditional_access_root import ConditionalAccessRoot
from office365.directory.identities.providers.base_collection import (
    IdentityProviderBaseCollection,
)
from office365.directory.identities.userflows.attribute import IdentityUserFlowAttribute
from office365.directory.identities.userflows.b2x.user_flow import B2XIdentityUserFlow
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class IdentityContainer(Entity):
    """
    Represents the entry point to different features in External Identities for
    both Azure Active Directory (Azure AD) and Azure AD B2C tenants.
    """

    @property
    def api_connectors(self):
        # type: () -> EntityCollection[IdentityApiConnector]
        """Represents entry point for API connectors."""
        return self.properties.get(
            "apiConnectors",
            EntityCollection(
                self.context,
                IdentityApiConnector,
                ResourcePath("apiConnectors", self.resource_path),
            ),
        )

    @property
    def authentication_event_listeners(self):
        # type: () -> EntityCollection[AuthenticationEventListener]
        """Get the collection of authenticationListener resources supported by the onSignupStart event."""
        return self.properties.get(
            "authenticationEventListeners",
            EntityCollection(
                self.context,
                AuthenticationEventListener,
                ResourcePath("authenticationEventListeners", self.resource_path),
            ),
        )

    @property
    def conditional_access(self):
        """The entry point for the Conditional Access (CA) object model."""
        return self.properties.get(
            "conditionalAccess",
            ConditionalAccessRoot(
                self.context, ResourcePath("conditionalAccess", self.resource_path)
            ),
        )

    @property
    def identity_providers(self):
        """Represents entry point for identity provider base."""
        return self.properties.get(
            "identityProviders",
            IdentityProviderBaseCollection(
                self.context, ResourcePath("identityProviders", self.resource_path)
            ),
        )

    @property
    def b2x_user_flows(self):
        """Represents entry point for B2X/self-service sign-up identity userflows."""
        return self.properties.get(
            "b2xUserFlows",
            EntityCollection(
                self.context,
                B2XIdentityUserFlow,
                ResourcePath("b2xUserFlows", self.resource_path),
            ),
        )

    @property
    def user_flow_attributes(self):
        """Represents entry point for identity userflow attributes."""
        return self.properties.get(
            "userFlowAttributes",
            EntityCollection(
                self.context,
                IdentityUserFlowAttribute,
                ResourcePath("userFlowAttributes", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "apiConnectors": self.api_connectors,
                "authenticationEventListeners": self.authentication_event_listeners,
                "b2xUserFlows": self.b2x_user_flows,
                "conditionalAccess": self.conditional_access,
                "identityProviders": self.identity_providers,
                "userFlowAttributes": self.user_flow_attributes,
            }
            default_value = property_mapping.get(name, None)
        return super(IdentityContainer, self).get_property(name, default_value)
