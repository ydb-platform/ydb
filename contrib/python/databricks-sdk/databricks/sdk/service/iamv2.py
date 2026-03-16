# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service._internal import _enum, _from_dict, _repeated_enum

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class Group:
    """The details of a Group resource."""

    account_id: Optional[str] = None
    """The parent account ID for group in Databricks."""

    external_id: Optional[str] = None
    """ExternalId of the group in the customer's IdP."""

    group_name: Optional[str] = None
    """Display name of the group."""

    internal_id: Optional[int] = None
    """Internal group ID of the group in Databricks."""

    def as_dict(self) -> dict:
        """Serializes the Group into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.internal_id is not None:
            body["internal_id"] = self.internal_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Group into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.internal_id is not None:
            body["internal_id"] = self.internal_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Group:
        """Deserializes the Group from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            external_id=d.get("external_id", None),
            group_name=d.get("group_name", None),
            internal_id=d.get("internal_id", None),
        )


class PrincipalType(Enum):
    """The type of the principal (user/sp/group)."""

    GROUP = "GROUP"
    SERVICE_PRINCIPAL = "SERVICE_PRINCIPAL"
    USER = "USER"


@dataclass
class ResolveGroupResponse:
    group: Optional[Group] = None
    """The group that was resolved."""

    def as_dict(self) -> dict:
        """Serializes the ResolveGroupResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group:
            body["group"] = self.group.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolveGroupResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group:
            body["group"] = self.group
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolveGroupResponse:
        """Deserializes the ResolveGroupResponse from a dictionary."""
        return cls(group=_from_dict(d, "group", Group))


@dataclass
class ResolveServicePrincipalResponse:
    service_principal: Optional[ServicePrincipal] = None
    """The service principal that was resolved."""

    def as_dict(self) -> dict:
        """Serializes the ResolveServicePrincipalResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.service_principal:
            body["service_principal"] = self.service_principal.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolveServicePrincipalResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.service_principal:
            body["service_principal"] = self.service_principal
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolveServicePrincipalResponse:
        """Deserializes the ResolveServicePrincipalResponse from a dictionary."""
        return cls(service_principal=_from_dict(d, "service_principal", ServicePrincipal))


@dataclass
class ResolveUserResponse:
    user: Optional[User] = None
    """The user that was resolved."""

    def as_dict(self) -> dict:
        """Serializes the ResolveUserResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.user:
            body["user"] = self.user.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolveUserResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.user:
            body["user"] = self.user
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolveUserResponse:
        """Deserializes the ResolveUserResponse from a dictionary."""
        return cls(user=_from_dict(d, "user", User))


@dataclass
class ServicePrincipal:
    """The details of a ServicePrincipal resource."""

    account_id: Optional[str] = None
    """The parent account ID for the service principal in Databricks."""

    account_sp_status: Optional[State] = None
    """The activity status of a service principal in a Databricks account."""

    application_id: Optional[str] = None
    """Application ID of the service principal."""

    display_name: Optional[str] = None
    """Display name of the service principal."""

    external_id: Optional[str] = None
    """ExternalId of the service principal in the customer's IdP."""

    internal_id: Optional[int] = None
    """Internal service principal ID of the service principal in Databricks."""

    def as_dict(self) -> dict:
        """Serializes the ServicePrincipal into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.account_sp_status is not None:
            body["account_sp_status"] = self.account_sp_status.value
        if self.application_id is not None:
            body["application_id"] = self.application_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.internal_id is not None:
            body["internal_id"] = self.internal_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ServicePrincipal into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.account_sp_status is not None:
            body["account_sp_status"] = self.account_sp_status
        if self.application_id is not None:
            body["application_id"] = self.application_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.internal_id is not None:
            body["internal_id"] = self.internal_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ServicePrincipal:
        """Deserializes the ServicePrincipal from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            account_sp_status=_enum(d, "account_sp_status", State),
            application_id=d.get("application_id", None),
            display_name=d.get("display_name", None),
            external_id=d.get("external_id", None),
            internal_id=d.get("internal_id", None),
        )


class State(Enum):
    """The activity status of a user or service principal in a Databricks account or workspace."""

    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


@dataclass
class User:
    """The details of a User resource."""

    account_id: Optional[str] = None
    """The accountId parent of the user in Databricks."""

    account_user_status: Optional[State] = None
    """The activity status of a user in a Databricks account."""

    external_id: Optional[str] = None
    """ExternalId of the user in the customer's IdP."""

    internal_id: Optional[int] = None
    """Internal userId of the user in Databricks."""

    name: Optional[UserName] = None

    username: Optional[str] = None
    """Username/email of the user."""

    def as_dict(self) -> dict:
        """Serializes the User into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.account_user_status is not None:
            body["account_user_status"] = self.account_user_status.value
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.internal_id is not None:
            body["internal_id"] = self.internal_id
        if self.name:
            body["name"] = self.name.as_dict()
        if self.username is not None:
            body["username"] = self.username
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the User into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.account_user_status is not None:
            body["account_user_status"] = self.account_user_status
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.internal_id is not None:
            body["internal_id"] = self.internal_id
        if self.name:
            body["name"] = self.name
        if self.username is not None:
            body["username"] = self.username
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> User:
        """Deserializes the User from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            account_user_status=_enum(d, "account_user_status", State),
            external_id=d.get("external_id", None),
            internal_id=d.get("internal_id", None),
            name=_from_dict(d, "name", UserName),
            username=d.get("username", None),
        )


@dataclass
class UserName:
    family_name: Optional[str] = None

    given_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the UserName into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.family_name is not None:
            body["family_name"] = self.family_name
        if self.given_name is not None:
            body["given_name"] = self.given_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UserName into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.family_name is not None:
            body["family_name"] = self.family_name
        if self.given_name is not None:
            body["given_name"] = self.given_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UserName:
        """Deserializes the UserName from a dictionary."""
        return cls(family_name=d.get("family_name", None), given_name=d.get("given_name", None))


@dataclass
class WorkspaceAccessDetail:
    """The details of a principal's access to a workspace."""

    access_type: Optional[WorkspaceAccessDetailAccessType] = None

    account_id: Optional[str] = None
    """The account ID parent of the workspace where the principal has access."""

    permissions: Optional[List[WorkspacePermission]] = None
    """The permissions granted to the principal in the workspace."""

    principal_id: Optional[int] = None
    """The internal ID of the principal (user/sp/group) in Databricks."""

    principal_type: Optional[PrincipalType] = None

    status: Optional[State] = None
    """The activity status of the principal in the workspace. Not applicable for groups at the moment."""

    workspace_id: Optional[int] = None
    """The workspace ID where the principal has access."""

    def as_dict(self) -> dict:
        """Serializes the WorkspaceAccessDetail into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_type is not None:
            body["access_type"] = self.access_type.value
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.permissions:
            body["permissions"] = [v.value for v in self.permissions]
        if self.principal_id is not None:
            body["principal_id"] = self.principal_id
        if self.principal_type is not None:
            body["principal_type"] = self.principal_type.value
        if self.status is not None:
            body["status"] = self.status.value
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceAccessDetail into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_type is not None:
            body["access_type"] = self.access_type
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.permissions:
            body["permissions"] = self.permissions
        if self.principal_id is not None:
            body["principal_id"] = self.principal_id
        if self.principal_type is not None:
            body["principal_type"] = self.principal_type
        if self.status is not None:
            body["status"] = self.status
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceAccessDetail:
        """Deserializes the WorkspaceAccessDetail from a dictionary."""
        return cls(
            access_type=_enum(d, "access_type", WorkspaceAccessDetailAccessType),
            account_id=d.get("account_id", None),
            permissions=_repeated_enum(d, "permissions", WorkspacePermission),
            principal_id=d.get("principal_id", None),
            principal_type=_enum(d, "principal_type", PrincipalType),
            status=_enum(d, "status", State),
            workspace_id=d.get("workspace_id", None),
        )


class WorkspaceAccessDetailAccessType(Enum):
    """The type of access the principal has to the workspace."""

    DIRECT = "DIRECT"
    INDIRECT = "INDIRECT"


class WorkspaceAccessDetailView(Enum):
    """Controls what fields are returned in the GetWorkspaceAccessDetail response."""

    BASIC = "BASIC"
    FULL = "FULL"


class WorkspacePermission(Enum):
    """The type of permission a principal has to a workspace (admin/user)."""

    ADMIN_PERMISSION = "ADMIN_PERMISSION"
    USER_PERMISSION = "USER_PERMISSION"


class AccountIamV2API:
    """These APIs are used to manage identities and the workspace access of these identities in <Databricks>."""

    def __init__(self, api_client):
        self._api = api_client

    def get_workspace_access_detail(
        self, workspace_id: int, principal_id: int, *, view: Optional[WorkspaceAccessDetailView] = None
    ) -> WorkspaceAccessDetail:
        """Returns the access details for a principal in a workspace. Allows for checking access details for any
        provisioned principal (user, service principal, or group) in a workspace. * Provisioned principal here
        refers to one that has been synced into Databricks from the customer's IdP or added explicitly to
        Databricks via SCIM/UI. Allows for passing in a "view" parameter to control what fields are returned
        (BASIC by default or FULL).

        :param workspace_id: int
          Required. The workspace ID for which the access details are being requested.
        :param principal_id: int
          Required. The internal ID of the principal (user/sp/group) for which the access details are being
          requested.
        :param view: :class:`WorkspaceAccessDetailView` (optional)
          Controls what fields are returned.

        :returns: :class:`WorkspaceAccessDetail`
        """

        query = {}
        if view is not None:
            query["view"] = view.value
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/identity/accounts/{self._api.account_id}/workspaces/{workspace_id}/workspaceAccessDetails/{principal_id}",
            query=query,
            headers=headers,
        )
        return WorkspaceAccessDetail.from_dict(res)

    def resolve_group(self, external_id: str) -> ResolveGroupResponse:
        """Resolves a group with the given external ID from the customer's IdP. If the group does not exist, it
        will be created in the account. If the customer is not onboarded onto Automatic Identity Management
        (AIM), this will return an error.

        :param external_id: str
          Required. The external ID of the group in the customer's IdP.

        :returns: :class:`ResolveGroupResponse`
        """

        body = {}
        if external_id is not None:
            body["external_id"] = external_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST",
            f"/api/2.0/identity/accounts/{self._api.account_id}/groups/resolveByExternalId",
            body=body,
            headers=headers,
        )
        return ResolveGroupResponse.from_dict(res)

    def resolve_service_principal(self, external_id: str) -> ResolveServicePrincipalResponse:
        """Resolves an SP with the given external ID from the customer's IdP. If the SP does not exist, it will
        be created. If the customer is not onboarded onto Automatic Identity Management (AIM), this will
        return an error.

        :param external_id: str
          Required. The external ID of the service principal in the customer's IdP.

        :returns: :class:`ResolveServicePrincipalResponse`
        """

        body = {}
        if external_id is not None:
            body["external_id"] = external_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST",
            f"/api/2.0/identity/accounts/{self._api.account_id}/servicePrincipals/resolveByExternalId",
            body=body,
            headers=headers,
        )
        return ResolveServicePrincipalResponse.from_dict(res)

    def resolve_user(self, external_id: str) -> ResolveUserResponse:
        """Resolves a user with the given external ID from the customer's IdP. If the user does not exist, it
        will be created. If the customer is not onboarded onto Automatic Identity Management (AIM), this will
        return an error.

        :param external_id: str
          Required. The external ID of the user in the customer's IdP.

        :returns: :class:`ResolveUserResponse`
        """

        body = {}
        if external_id is not None:
            body["external_id"] = external_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST",
            f"/api/2.0/identity/accounts/{self._api.account_id}/users/resolveByExternalId",
            body=body,
            headers=headers,
        )
        return ResolveUserResponse.from_dict(res)


class WorkspaceIamV2API:
    """These APIs are used to manage identities and the workspace access of these identities in <Databricks>."""

    def __init__(self, api_client):
        self._api = api_client

    def get_workspace_access_detail_local(
        self, principal_id: int, *, view: Optional[WorkspaceAccessDetailView] = None
    ) -> WorkspaceAccessDetail:
        """Returns the access details for a principal in the current workspace. Allows for checking access
        details for any provisioned principal (user, service principal, or group) in the current workspace. *
        Provisioned principal here refers to one that has been synced into Databricks from the customer's IdP
        or added explicitly to Databricks via SCIM/UI. Allows for passing in a "view" parameter to control
        what fields are returned (BASIC by default or FULL).

        :param principal_id: int
          Required. The internal ID of the principal (user/sp/group) for which the access details are being
          requested.
        :param view: :class:`WorkspaceAccessDetailView` (optional)
          Controls what fields are returned.

        :returns: :class:`WorkspaceAccessDetail`
        """

        query = {}
        if view is not None:
            query["view"] = view.value
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/identity/workspaceAccessDetails/{principal_id}", query=query, headers=headers
        )
        return WorkspaceAccessDetail.from_dict(res)

    def resolve_group_proxy(self, external_id: str) -> ResolveGroupResponse:
        """Resolves a group with the given external ID from the customer's IdP. If the group does not exist, it
        will be created in the account. If the customer is not onboarded onto Automatic Identity Management
        (AIM), this will return an error.

        :param external_id: str
          Required. The external ID of the group in the customer's IdP.

        :returns: :class:`ResolveGroupResponse`
        """

        body = {}
        if external_id is not None:
            body["external_id"] = external_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/identity/groups/resolveByExternalId", body=body, headers=headers)
        return ResolveGroupResponse.from_dict(res)

    def resolve_service_principal_proxy(self, external_id: str) -> ResolveServicePrincipalResponse:
        """Resolves an SP with the given external ID from the customer's IdP. If the SP does not exist, it will
        be created. If the customer is not onboarded onto Automatic Identity Management (AIM), this will
        return an error.

        :param external_id: str
          Required. The external ID of the service principal in the customer's IdP.

        :returns: :class:`ResolveServicePrincipalResponse`
        """

        body = {}
        if external_id is not None:
            body["external_id"] = external_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", "/api/2.0/identity/servicePrincipals/resolveByExternalId", body=body, headers=headers
        )
        return ResolveServicePrincipalResponse.from_dict(res)

    def resolve_user_proxy(self, external_id: str) -> ResolveUserResponse:
        """Resolves a user with the given external ID from the customer's IdP. If the user does not exist, it
        will be created. If the customer is not onboarded onto Automatic Identity Management (AIM), this will
        return an error.

        :param external_id: str
          Required. The external ID of the user in the customer's IdP.

        :returns: :class:`ResolveUserResponse`
        """

        body = {}
        if external_id is not None:
            body["external_id"] = external_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/identity/users/resolveByExternalId", body=body, headers=headers)
        return ResolveUserResponse.from_dict(res)
