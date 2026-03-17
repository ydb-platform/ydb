from office365.directory.policies.admin_consent_request import AdminConsentRequestPolicy
from office365.directory.policies.app_management import AppManagementPolicy
from office365.directory.policies.authentication_flows import AuthenticationFlowsPolicy
from office365.directory.policies.authentication_methods import (
    AuthenticationMethodsPolicy,
)
from office365.directory.policies.authentication_strength import (
    AuthenticationStrengthPolicy,
)
from office365.directory.policies.authorization import AuthorizationPolicy
from office365.directory.policies.conditional_access import ConditionalAccessPolicy
from office365.directory.policies.cross_tenant_access import CrossTenantAccessPolicy
from office365.directory.policies.device_registration import DeviceRegistrationPolicy
from office365.directory.policies.feature_rollout import FeatureRolloutPolicy
from office365.directory.policies.permission_grant import PermissionGrantPolicy
from office365.directory.policies.tenant_app_management import TenantAppManagementPolicy
from office365.directory.policies.unified_role_management import (
    UnifiedRoleManagementPolicy,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class PolicyRoot(Entity):
    """Resource type exposing navigation properties for the policies singleton."""

    @property
    def admin_consent_request_policy(self):
        """
        The policy by which consent requests are created and managed for the entire tenant.
        """
        return self.properties.get(
            "adminConsentRequestPolicy",
            AdminConsentRequestPolicy(
                self.context,
                ResourcePath("adminConsentRequestPolicy", self.resource_path),
            ),
        )

    @property
    def authentication_methods_policy(self):
        """
        The authentication methods and the users that are allowed to use them to sign in and perform multi-factor
        authentication (MFA) in Azure Active Directory (Azure AD).
        """
        return self.properties.get(
            "authenticationMethodsPolicy",
            AuthenticationMethodsPolicy(
                self.context,
                ResourcePath("authenticationMethodsPolicy", self.resource_path),
            ),
        )

    @property
    def authentication_strength_policies(self):
        # type: () -> EntityCollection[AuthenticationStrengthPolicy]
        """
        The authentication method combinations that are to be used in scenarios defined by Azure AD Conditional Access.
        """
        return self.properties.get(
            "authenticationStrengthPolicies",
            EntityCollection(
                self.context,
                AuthenticationStrengthPolicy,
                ResourcePath("authenticationStrengthPolicies", self.resource_path),
            ),
        )

    @property
    def authentication_flows_policy(self):
        """The policy configuration of the self-service sign-up experience of external users."""
        return self.properties.get(
            "authenticationFlowsPolicy",
            AuthenticationFlowsPolicy(
                self.context,
                ResourcePath("authenticationFlowsPolicy", self.resource_path),
            ),
        )

    @property
    def authorization_policy(self):
        """The policy that controls Azure AD authorization settings."""
        return self.properties.get(
            "authorizationPolicy",
            AuthorizationPolicy(
                self.context, ResourcePath("authorizationPolicy", self.resource_path)
            ),
        )

    @property
    def app_management_policies(self):
        # type: () -> EntityCollection[AppManagementPolicy]
        """The policies that enforce app management restrictions for specific applications and service principals,
        overriding the defaultAppManagementPolicy."""
        return self.properties.get(
            "appManagementPolicies",
            EntityCollection(
                self.context,
                AppManagementPolicy,
                ResourcePath("appManagementPolicies", self.resource_path),
            ),
        )

    @property
    def conditional_access_policies(self):
        # type: () -> EntityCollection[ConditionalAccessPolicy]
        """The custom rules that define an access scenario"""
        return self.properties.get(
            "conditionalAccessPolicies",
            EntityCollection(
                self.context,
                ConditionalAccessPolicy,
                ResourcePath("conditionalAccessPolicies", self.resource_path),
            ),
        )

    @property
    def cross_tenant_access_policy(self):
        """
        The custom rules that define an access scenario when interacting with external Azure AD tenants.
        """
        return self.properties.get(
            "crossTenantAccessPolicy",
            CrossTenantAccessPolicy(
                self.context,
                ResourcePath("crossTenantAccessPolicy", self.resource_path),
            ),
        )

    @property
    def device_registration_policy(self):
        """ """
        return self.properties.get(
            "deviceRegistrationPolicy",
            DeviceRegistrationPolicy(
                self.context,
                ResourcePath("deviceRegistrationPolicy", self.resource_path),
            ),
        )

    @property
    def default_app_management_policy(self):
        """
        The tenant-wide policy that enforces app management restrictions for all applications and service principals.
        """
        return self.properties.get(
            "defaultAppManagementPolicy",
            TenantAppManagementPolicy(
                self.context,
                ResourcePath("defaultAppManagementPolicy", self.resource_path),
            ),
        )

    @property
    def feature_rollout_policies(self):
        # type: () -> EntityCollection[FeatureRolloutPolicy]
        """The feature rollout policy associated with a directory object."""
        return self.properties.get(
            "featureRolloutPolicies",
            EntityCollection(
                self.context,
                FeatureRolloutPolicy,
                ResourcePath("featureRolloutPolicies", self.resource_path),
            ),
        )

    @property
    def permission_grant_policies(self):
        # type: () -> EntityCollection[PermissionGrantPolicy]
        """
        The policy that specifies the conditions under which consent can be granted.
        """
        return self.properties.get(
            "permissionGrantPolicies",
            EntityCollection(
                self.context,
                PermissionGrantPolicy,
                ResourcePath("permissionGrantPolicies", self.resource_path),
            ),
        )

    @property
    def role_management_policies(self):
        # type: () -> EntityCollection[UnifiedRoleManagementPolicy]
        """Specifies the various policies associated with scopes and roles."""
        return self.properties.get(
            "roleManagementPolicies",
            EntityCollection(
                self.context,
                UnifiedRoleManagementPolicy,
                ResourcePath("roleManagementPolicies", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "adminConsentRequestPolicy": self.admin_consent_request_policy,
                "authenticationStrengthPolicies": self.authentication_strength_policies,
                "authenticationFlowsPolicy": self.authentication_flows_policy,
                "appManagementPolicies": self.app_management_policies,
                "authenticationMethodsPolicy": self.authentication_methods_policy,
                "authorizationPolicy": self.authorization_policy,
                "conditional_access_policies": self.conditional_access_policies,
                "crossTenantAccessPolicy": self.cross_tenant_access_policy,
                "defaultAppManagementPolicy": self.default_app_management_policy,
                "deviceRegistrationPolicy": self.device_registration_policy,
                "featureRolloutPolicies": self.feature_rollout_policies,
                "permissionGrantPolicies": self.permission_grant_policies,
                "roleManagementPolicies": self.role_management_policies,
            }
            default_value = property_mapping.get(name, None)
        return super(PolicyRoot, self).get_property(name, default_value)
