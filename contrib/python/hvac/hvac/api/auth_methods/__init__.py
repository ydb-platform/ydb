"""Collection of classes for various Vault auth methods."""

from hvac.api.auth_methods.approle import AppRole
from hvac.api.auth_methods.azure import Azure
from hvac.api.auth_methods.gcp import Gcp
from hvac.api.auth_methods.github import Github
from hvac.api.auth_methods.jwt import JWT
from hvac.api.auth_methods.kubernetes import Kubernetes
from hvac.api.auth_methods.ldap import Ldap
from hvac.api.auth_methods.userpass import Userpass
from hvac.api.auth_methods.legacy_mfa import LegacyMfa
from hvac.api.auth_methods.oidc import OIDC
from hvac.api.auth_methods.okta import Okta
from hvac.api.auth_methods.radius import Radius
from hvac.api.auth_methods.token import Token
from hvac.api.auth_methods.aws import Aws
from hvac.api.auth_methods.cert import Cert
from hvac.api.vault_api_category import VaultApiCategory

__all__ = (
    "AuthMethods",
    "AppRole",
    "Azure",
    "Gcp",
    "Github",
    "JWT",
    "Kubernetes",
    "Ldap",
    "Userpass",
    "LegacyMfa",
    "OIDC",
    "Okta",
    "Radius",
    "Token",
    "Aws",
    "Cert",
)


class AuthMethods(VaultApiCategory):
    """Auth Methods."""

    implemented_classes = [
        AppRole,
        Azure,
        Github,
        Gcp,
        JWT,
        Kubernetes,
        Ldap,
        Userpass,
        LegacyMfa,
        OIDC,
        Okta,
        Radius,
        Token,
        Aws,
        Cert,
    ]
    unimplemented_classes = [
        "AppId",
        "AliCloud",
        "Mfa",
    ]
