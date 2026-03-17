# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service._internal import _enum, _from_dict, _repeated_dict

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AclItem:
    """An item representing an ACL rule applied to the given principal (user or group) on the
    associated scope point."""

    principal: str
    """The principal in which the permission is applied."""

    permission: AclPermission
    """The permission level applied to the principal."""

    def as_dict(self) -> dict:
        """Serializes the AclItem into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        if self.principal is not None:
            body["principal"] = self.principal
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AclItem into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        if self.principal is not None:
            body["principal"] = self.principal
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AclItem:
        """Deserializes the AclItem from a dictionary."""
        return cls(permission=_enum(d, "permission", AclPermission), principal=d.get("principal", None))


class AclPermission(Enum):
    """The ACL permission levels for Secret ACLs applied to secret scopes."""

    MANAGE = "MANAGE"
    READ = "READ"
    WRITE = "WRITE"


@dataclass
class AzureKeyVaultSecretScopeMetadata:
    """The metadata of the Azure KeyVault for a secret scope of type `AZURE_KEYVAULT`"""

    resource_id: str
    """The resource id of the azure KeyVault that user wants to associate the scope with."""

    dns_name: str
    """The DNS of the KeyVault"""

    def as_dict(self) -> dict:
        """Serializes the AzureKeyVaultSecretScopeMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dns_name is not None:
            body["dns_name"] = self.dns_name
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureKeyVaultSecretScopeMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dns_name is not None:
            body["dns_name"] = self.dns_name
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureKeyVaultSecretScopeMetadata:
        """Deserializes the AzureKeyVaultSecretScopeMetadata from a dictionary."""
        return cls(dns_name=d.get("dns_name", None), resource_id=d.get("resource_id", None))


@dataclass
class CreateCredentialsResponse:
    credential_id: int
    """ID of the credential object in the workspace."""

    git_provider: str
    """The Git provider associated with the credential."""

    git_email: Optional[str] = None
    """The authenticating email associated with your Git provider user account. Used for authentication
    with the remote repository and also sets the author & committer identity for commits. Required
    for most Git providers except AWS CodeCommit. Learn more at
    https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider"""

    git_username: Optional[str] = None
    """The username provided with your Git provider account and associated with the credential. For
    most Git providers it is only used to set the Git committer & author names for commits, however
    it may be required for authentication depending on your Git provider / token requirements.
    Required for AWS CodeCommit."""

    is_default_for_provider: Optional[bool] = None
    """if the credential is the default for the given provider"""

    name: Optional[str] = None
    """the name of the git credential, used for identification and ease of lookup"""

    def as_dict(self) -> dict:
        """Serializes the CreateCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.git_email is not None:
            body["git_email"] = self.git_email
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_username is not None:
            body["git_username"] = self.git_username
        if self.is_default_for_provider is not None:
            body["is_default_for_provider"] = self.is_default_for_provider
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.git_email is not None:
            body["git_email"] = self.git_email
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_username is not None:
            body["git_username"] = self.git_username
        if self.is_default_for_provider is not None:
            body["is_default_for_provider"] = self.is_default_for_provider
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateCredentialsResponse:
        """Deserializes the CreateCredentialsResponse from a dictionary."""
        return cls(
            credential_id=d.get("credential_id", None),
            git_email=d.get("git_email", None),
            git_provider=d.get("git_provider", None),
            git_username=d.get("git_username", None),
            is_default_for_provider=d.get("is_default_for_provider", None),
            name=d.get("name", None),
        )


@dataclass
class CreateRepoResponse:
    branch: Optional[str] = None
    """Branch that the Git folder (repo) is checked out to."""

    head_commit_id: Optional[str] = None
    """SHA-1 hash representing the commit ID of the current HEAD of the Git folder (repo)."""

    id: Optional[int] = None
    """ID of the Git folder (repo) object in the workspace."""

    path: Optional[str] = None
    """Path of the Git folder (repo) in the workspace."""

    provider: Optional[str] = None
    """Git provider of the linked Git repository."""

    sparse_checkout: Optional[SparseCheckout] = None
    """Sparse checkout settings for the Git folder (repo)."""

    url: Optional[str] = None
    """URL of the linked Git repository."""

    def as_dict(self) -> dict:
        """Serializes the CreateRepoResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.head_commit_id is not None:
            body["head_commit_id"] = self.head_commit_id
        if self.id is not None:
            body["id"] = self.id
        if self.path is not None:
            body["path"] = self.path
        if self.provider is not None:
            body["provider"] = self.provider
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout.as_dict()
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateRepoResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.head_commit_id is not None:
            body["head_commit_id"] = self.head_commit_id
        if self.id is not None:
            body["id"] = self.id
        if self.path is not None:
            body["path"] = self.path
        if self.provider is not None:
            body["provider"] = self.provider
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateRepoResponse:
        """Deserializes the CreateRepoResponse from a dictionary."""
        return cls(
            branch=d.get("branch", None),
            head_commit_id=d.get("head_commit_id", None),
            id=d.get("id", None),
            path=d.get("path", None),
            provider=d.get("provider", None),
            sparse_checkout=_from_dict(d, "sparse_checkout", SparseCheckout),
            url=d.get("url", None),
        )


@dataclass
class CredentialInfo:
    credential_id: int
    """ID of the credential object in the workspace."""

    git_email: Optional[str] = None
    """The authenticating email associated with your Git provider user account. Used for authentication
    with the remote repository and also sets the author & committer identity for commits. Required
    for most Git providers except AWS CodeCommit. Learn more at
    https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider"""

    git_provider: Optional[str] = None
    """The Git provider associated with the credential."""

    git_username: Optional[str] = None
    """The username provided with your Git provider account and associated with the credential. For
    most Git providers it is only used to set the Git committer & author names for commits, however
    it may be required for authentication depending on your Git provider / token requirements.
    Required for AWS CodeCommit."""

    is_default_for_provider: Optional[bool] = None
    """if the credential is the default for the given provider"""

    name: Optional[str] = None
    """the name of the git credential, used for identification and ease of lookup"""

    def as_dict(self) -> dict:
        """Serializes the CredentialInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.git_email is not None:
            body["git_email"] = self.git_email
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_username is not None:
            body["git_username"] = self.git_username
        if self.is_default_for_provider is not None:
            body["is_default_for_provider"] = self.is_default_for_provider
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CredentialInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.git_email is not None:
            body["git_email"] = self.git_email
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_username is not None:
            body["git_username"] = self.git_username
        if self.is_default_for_provider is not None:
            body["is_default_for_provider"] = self.is_default_for_provider
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CredentialInfo:
        """Deserializes the CredentialInfo from a dictionary."""
        return cls(
            credential_id=d.get("credential_id", None),
            git_email=d.get("git_email", None),
            git_provider=d.get("git_provider", None),
            git_username=d.get("git_username", None),
            is_default_for_provider=d.get("is_default_for_provider", None),
            name=d.get("name", None),
        )


@dataclass
class DeleteCredentialsResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteCredentialsResponse:
        """Deserializes the DeleteCredentialsResponse from a dictionary."""
        return cls()


@dataclass
class DeleteRepoResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteRepoResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteRepoResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteRepoResponse:
        """Deserializes the DeleteRepoResponse from a dictionary."""
        return cls()


@dataclass
class DeleteResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteResponse:
        """Deserializes the DeleteResponse from a dictionary."""
        return cls()


@dataclass
class DeleteSecretResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteSecretResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteSecretResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteSecretResponse:
        """Deserializes the DeleteSecretResponse from a dictionary."""
        return cls()


class ExportFormat(Enum):
    """The format for workspace import and export."""

    AUTO = "AUTO"
    DBC = "DBC"
    HTML = "HTML"
    JUPYTER = "JUPYTER"
    RAW = "RAW"
    R_MARKDOWN = "R_MARKDOWN"
    SOURCE = "SOURCE"


@dataclass
class ExportResponse:
    """The request field `direct_download` determines whether a JSON response or binary contents are
    returned by this endpoint."""

    content: Optional[str] = None
    """The base64-encoded content. If the limit (10MB) is exceeded, exception with error code
    **MAX_NOTEBOOK_SIZE_EXCEEDED** is thrown."""

    file_type: Optional[str] = None
    """The file type of the exported file."""

    def as_dict(self) -> dict:
        """Serializes the ExportResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.content is not None:
            body["content"] = self.content
        if self.file_type is not None:
            body["file_type"] = self.file_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExportResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.content is not None:
            body["content"] = self.content
        if self.file_type is not None:
            body["file_type"] = self.file_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExportResponse:
        """Deserializes the ExportResponse from a dictionary."""
        return cls(content=d.get("content", None), file_type=d.get("file_type", None))


@dataclass
class GetCredentialsResponse:
    credential_id: int
    """ID of the credential object in the workspace."""

    git_email: Optional[str] = None
    """The authenticating email associated with your Git provider user account. Used for authentication
    with the remote repository and also sets the author & committer identity for commits. Required
    for most Git providers except AWS CodeCommit. Learn more at
    https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider"""

    git_provider: Optional[str] = None
    """The Git provider associated with the credential."""

    git_username: Optional[str] = None
    """The username provided with your Git provider account and associated with the credential. For
    most Git providers it is only used to set the Git committer & author names for commits, however
    it may be required for authentication depending on your Git provider / token requirements.
    Required for AWS CodeCommit."""

    is_default_for_provider: Optional[bool] = None
    """if the credential is the default for the given provider"""

    name: Optional[str] = None
    """the name of the git credential, used for identification and ease of lookup"""

    def as_dict(self) -> dict:
        """Serializes the GetCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.git_email is not None:
            body["git_email"] = self.git_email
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_username is not None:
            body["git_username"] = self.git_username
        if self.is_default_for_provider is not None:
            body["is_default_for_provider"] = self.is_default_for_provider
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.git_email is not None:
            body["git_email"] = self.git_email
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_username is not None:
            body["git_username"] = self.git_username
        if self.is_default_for_provider is not None:
            body["is_default_for_provider"] = self.is_default_for_provider
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetCredentialsResponse:
        """Deserializes the GetCredentialsResponse from a dictionary."""
        return cls(
            credential_id=d.get("credential_id", None),
            git_email=d.get("git_email", None),
            git_provider=d.get("git_provider", None),
            git_username=d.get("git_username", None),
            is_default_for_provider=d.get("is_default_for_provider", None),
            name=d.get("name", None),
        )


@dataclass
class GetRepoPermissionLevelsResponse:
    permission_levels: Optional[List[RepoPermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetRepoPermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetRepoPermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetRepoPermissionLevelsResponse:
        """Deserializes the GetRepoPermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", RepoPermissionsDescription))


@dataclass
class GetRepoResponse:
    branch: Optional[str] = None
    """Branch that the local version of the repo is checked out to."""

    head_commit_id: Optional[str] = None
    """SHA-1 hash representing the commit ID of the current HEAD of the repo."""

    id: Optional[int] = None
    """ID of the Git folder (repo) object in the workspace."""

    path: Optional[str] = None
    """Path of the Git folder (repo) in the workspace."""

    provider: Optional[str] = None
    """Git provider of the linked Git repository."""

    sparse_checkout: Optional[SparseCheckout] = None
    """Sparse checkout settings for the Git folder (repo)."""

    url: Optional[str] = None
    """URL of the linked Git repository."""

    def as_dict(self) -> dict:
        """Serializes the GetRepoResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.head_commit_id is not None:
            body["head_commit_id"] = self.head_commit_id
        if self.id is not None:
            body["id"] = self.id
        if self.path is not None:
            body["path"] = self.path
        if self.provider is not None:
            body["provider"] = self.provider
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout.as_dict()
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetRepoResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.head_commit_id is not None:
            body["head_commit_id"] = self.head_commit_id
        if self.id is not None:
            body["id"] = self.id
        if self.path is not None:
            body["path"] = self.path
        if self.provider is not None:
            body["provider"] = self.provider
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetRepoResponse:
        """Deserializes the GetRepoResponse from a dictionary."""
        return cls(
            branch=d.get("branch", None),
            head_commit_id=d.get("head_commit_id", None),
            id=d.get("id", None),
            path=d.get("path", None),
            provider=d.get("provider", None),
            sparse_checkout=_from_dict(d, "sparse_checkout", SparseCheckout),
            url=d.get("url", None),
        )


@dataclass
class GetSecretResponse:
    key: Optional[str] = None
    """A unique name to identify the secret."""

    value: Optional[str] = None
    """The value of the secret in its byte representation."""

    def as_dict(self) -> dict:
        """Serializes the GetSecretResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetSecretResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetSecretResponse:
        """Deserializes the GetSecretResponse from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class GetWorkspaceObjectPermissionLevelsResponse:
    permission_levels: Optional[List[WorkspaceObjectPermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetWorkspaceObjectPermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetWorkspaceObjectPermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetWorkspaceObjectPermissionLevelsResponse:
        """Deserializes the GetWorkspaceObjectPermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", WorkspaceObjectPermissionsDescription))


class ImportFormat(Enum):
    """The format for workspace import and export."""

    AUTO = "AUTO"
    DBC = "DBC"
    HTML = "HTML"
    JUPYTER = "JUPYTER"
    RAW = "RAW"
    R_MARKDOWN = "R_MARKDOWN"
    SOURCE = "SOURCE"


@dataclass
class ImportResponse:
    def as_dict(self) -> dict:
        """Serializes the ImportResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ImportResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ImportResponse:
        """Deserializes the ImportResponse from a dictionary."""
        return cls()


class Language(Enum):
    """The language of notebook."""

    PYTHON = "PYTHON"
    R = "R"
    SCALA = "SCALA"
    SQL = "SQL"


@dataclass
class ListAclsResponse:
    items: Optional[List[AclItem]] = None
    """The associated ACLs rule applied to principals in the given scope."""

    def as_dict(self) -> dict:
        """Serializes the ListAclsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.items:
            body["items"] = [v.as_dict() for v in self.items]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAclsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.items:
            body["items"] = self.items
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAclsResponse:
        """Deserializes the ListAclsResponse from a dictionary."""
        return cls(items=_repeated_dict(d, "items", AclItem))


@dataclass
class ListCredentialsResponse:
    credentials: Optional[List[CredentialInfo]] = None
    """List of credentials."""

    def as_dict(self) -> dict:
        """Serializes the ListCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credentials:
            body["credentials"] = [v.as_dict() for v in self.credentials]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credentials:
            body["credentials"] = self.credentials
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListCredentialsResponse:
        """Deserializes the ListCredentialsResponse from a dictionary."""
        return cls(credentials=_repeated_dict(d, "credentials", CredentialInfo))


@dataclass
class ListReposResponse:
    next_page_token: Optional[str] = None
    """Token that can be specified as a query parameter to the `GET /repos` endpoint to retrieve the
    next page of results."""

    repos: Optional[List[RepoInfo]] = None
    """List of Git folders (repos)."""

    def as_dict(self) -> dict:
        """Serializes the ListReposResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.repos:
            body["repos"] = [v.as_dict() for v in self.repos]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListReposResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.repos:
            body["repos"] = self.repos
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListReposResponse:
        """Deserializes the ListReposResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), repos=_repeated_dict(d, "repos", RepoInfo))


@dataclass
class ListResponse:
    objects: Optional[List[ObjectInfo]] = None
    """List of objects."""

    def as_dict(self) -> dict:
        """Serializes the ListResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.objects:
            body["objects"] = [v.as_dict() for v in self.objects]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.objects:
            body["objects"] = self.objects
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListResponse:
        """Deserializes the ListResponse from a dictionary."""
        return cls(objects=_repeated_dict(d, "objects", ObjectInfo))


@dataclass
class ListScopesResponse:
    scopes: Optional[List[SecretScope]] = None
    """The available secret scopes."""

    def as_dict(self) -> dict:
        """Serializes the ListScopesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.scopes:
            body["scopes"] = [v.as_dict() for v in self.scopes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListScopesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.scopes:
            body["scopes"] = self.scopes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListScopesResponse:
        """Deserializes the ListScopesResponse from a dictionary."""
        return cls(scopes=_repeated_dict(d, "scopes", SecretScope))


@dataclass
class ListSecretsResponse:
    secrets: Optional[List[SecretMetadata]] = None
    """Metadata information of all secrets contained within the given scope."""

    def as_dict(self) -> dict:
        """Serializes the ListSecretsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.secrets:
            body["secrets"] = [v.as_dict() for v in self.secrets]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSecretsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.secrets:
            body["secrets"] = self.secrets
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSecretsResponse:
        """Deserializes the ListSecretsResponse from a dictionary."""
        return cls(secrets=_repeated_dict(d, "secrets", SecretMetadata))


@dataclass
class MkdirsResponse:
    def as_dict(self) -> dict:
        """Serializes the MkdirsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MkdirsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MkdirsResponse:
        """Deserializes the MkdirsResponse from a dictionary."""
        return cls()


@dataclass
class ObjectInfo:
    """The information of the object in workspace. It will be returned by ``list`` and ``get-status``."""

    created_at: Optional[int] = None
    """Only applicable to files. The creation UTC timestamp."""

    language: Optional[Language] = None
    """The language of the object. This value is set only if the object type is ``NOTEBOOK``."""

    modified_at: Optional[int] = None
    """Only applicable to files, the last modified UTC timestamp."""

    object_id: Optional[int] = None
    """Unique identifier for the object."""

    object_type: Optional[ObjectType] = None
    """The type of the object in workspace.
    
    - `NOTEBOOK`: document that contains runnable code, visualizations, and explanatory text. -
    `DIRECTORY`: directory - `LIBRARY`: library - `FILE`: file - `REPO`: repository - `DASHBOARD`:
    Lakeview dashboard"""

    path: Optional[str] = None
    """The absolute path of the object."""

    resource_id: Optional[str] = None
    """A unique identifier for the object that is consistent across all Databricks APIs."""

    size: Optional[int] = None
    """Only applicable to files. The file size in bytes can be returned."""

    def as_dict(self) -> dict:
        """Serializes the ObjectInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.language is not None:
            body["language"] = self.language.value
        if self.modified_at is not None:
            body["modified_at"] = self.modified_at
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type.value
        if self.path is not None:
            body["path"] = self.path
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.size is not None:
            body["size"] = self.size
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ObjectInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.language is not None:
            body["language"] = self.language
        if self.modified_at is not None:
            body["modified_at"] = self.modified_at
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        if self.path is not None:
            body["path"] = self.path
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.size is not None:
            body["size"] = self.size
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ObjectInfo:
        """Deserializes the ObjectInfo from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            language=_enum(d, "language", Language),
            modified_at=d.get("modified_at", None),
            object_id=d.get("object_id", None),
            object_type=_enum(d, "object_type", ObjectType),
            path=d.get("path", None),
            resource_id=d.get("resource_id", None),
            size=d.get("size", None),
        )


class ObjectType(Enum):
    """The type of the object in workspace."""

    DASHBOARD = "DASHBOARD"
    DIRECTORY = "DIRECTORY"
    FILE = "FILE"
    LIBRARY = "LIBRARY"
    NOTEBOOK = "NOTEBOOK"
    REPO = "REPO"


@dataclass
class RepoAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[RepoPermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the RepoAccessControlRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoAccessControlRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoAccessControlRequest:
        """Deserializes the RepoAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", RepoPermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class RepoAccessControlResponse:
    all_permissions: Optional[List[RepoPermission]] = None
    """All permissions."""

    display_name: Optional[str] = None
    """Display name of the user or service principal."""

    group_name: Optional[str] = None
    """name of the group"""

    service_principal_name: Optional[str] = None
    """Name of the service principal."""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the RepoAccessControlResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = [v.as_dict() for v in self.all_permissions]
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoAccessControlResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = self.all_permissions
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoAccessControlResponse:
        """Deserializes the RepoAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", RepoPermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class RepoInfo:
    """Git folder (repo) information."""

    branch: Optional[str] = None
    """Name of the current git branch of the git folder (repo)."""

    head_commit_id: Optional[str] = None
    """Current git commit id of the git folder (repo)."""

    id: Optional[int] = None
    """Id of the git folder (repo) in the Workspace."""

    path: Optional[str] = None
    """Root path of the git folder (repo) in the Workspace."""

    provider: Optional[str] = None
    """Git provider of the remote git repository, e.g. `gitHub`."""

    sparse_checkout: Optional[SparseCheckout] = None
    """Sparse checkout config for the git folder (repo)."""

    url: Optional[str] = None
    """URL of the remote git repository."""

    def as_dict(self) -> dict:
        """Serializes the RepoInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.head_commit_id is not None:
            body["head_commit_id"] = self.head_commit_id
        if self.id is not None:
            body["id"] = self.id
        if self.path is not None:
            body["path"] = self.path
        if self.provider is not None:
            body["provider"] = self.provider
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout.as_dict()
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.head_commit_id is not None:
            body["head_commit_id"] = self.head_commit_id
        if self.id is not None:
            body["id"] = self.id
        if self.path is not None:
            body["path"] = self.path
        if self.provider is not None:
            body["provider"] = self.provider
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoInfo:
        """Deserializes the RepoInfo from a dictionary."""
        return cls(
            branch=d.get("branch", None),
            head_commit_id=d.get("head_commit_id", None),
            id=d.get("id", None),
            path=d.get("path", None),
            provider=d.get("provider", None),
            sparse_checkout=_from_dict(d, "sparse_checkout", SparseCheckout),
            url=d.get("url", None),
        )


@dataclass
class RepoPermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[RepoPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the RepoPermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoPermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoPermission:
        """Deserializes the RepoPermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", RepoPermissionLevel),
        )


class RepoPermissionLevel(Enum):
    """Permission level"""

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_READ = "CAN_READ"
    CAN_RUN = "CAN_RUN"


@dataclass
class RepoPermissions:
    access_control_list: Optional[List[RepoAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the RepoPermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoPermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoPermissions:
        """Deserializes the RepoPermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", RepoAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class RepoPermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[RepoPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the RepoPermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoPermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoPermissionsDescription:
        """Deserializes the RepoPermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None), permission_level=_enum(d, "permission_level", RepoPermissionLevel)
        )


class ScopeBackendType(Enum):
    """The types of secret scope backends in the Secret Manager. Azure KeyVault backed secret scopes
    will be supported in a later release."""

    AZURE_KEYVAULT = "AZURE_KEYVAULT"
    DATABRICKS = "DATABRICKS"


@dataclass
class SecretMetadata:
    """The metadata about a secret. Returned when listing secrets. Does not contain the actual secret
    value."""

    key: Optional[str] = None
    """A unique name to identify the secret."""

    last_updated_timestamp: Optional[int] = None
    """The last updated timestamp (in milliseconds) for the secret."""

    def as_dict(self) -> dict:
        """Serializes the SecretMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SecretMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SecretMetadata:
        """Deserializes the SecretMetadata from a dictionary."""
        return cls(key=d.get("key", None), last_updated_timestamp=d.get("last_updated_timestamp", None))


@dataclass
class SecretScope:
    """An organizational resource for storing secrets. Secret scopes can be different types
    (Databricks-managed, Azure KeyVault backed, etc), and ACLs can be applied to control permissions
    for all secrets within a scope."""

    backend_type: Optional[ScopeBackendType] = None
    """The type of secret scope backend."""

    keyvault_metadata: Optional[AzureKeyVaultSecretScopeMetadata] = None
    """The metadata for the secret scope if the type is ``AZURE_KEYVAULT``"""

    name: Optional[str] = None
    """A unique name to identify the secret scope."""

    def as_dict(self) -> dict:
        """Serializes the SecretScope into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.backend_type is not None:
            body["backend_type"] = self.backend_type.value
        if self.keyvault_metadata:
            body["keyvault_metadata"] = self.keyvault_metadata.as_dict()
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SecretScope into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.backend_type is not None:
            body["backend_type"] = self.backend_type
        if self.keyvault_metadata:
            body["keyvault_metadata"] = self.keyvault_metadata
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SecretScope:
        """Deserializes the SecretScope from a dictionary."""
        return cls(
            backend_type=_enum(d, "backend_type", ScopeBackendType),
            keyvault_metadata=_from_dict(d, "keyvault_metadata", AzureKeyVaultSecretScopeMetadata),
            name=d.get("name", None),
        )


@dataclass
class SparseCheckout:
    """Sparse checkout configuration, it contains options like cone patterns."""

    patterns: Optional[List[str]] = None
    """List of sparse checkout cone patterns, see [cone mode handling] for details.
    
    [cone mode handling]: https://git-scm.com/docs/git-sparse-checkout#_internalscone_mode_handling"""

    def as_dict(self) -> dict:
        """Serializes the SparseCheckout into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.patterns:
            body["patterns"] = [v for v in self.patterns]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SparseCheckout into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.patterns:
            body["patterns"] = self.patterns
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SparseCheckout:
        """Deserializes the SparseCheckout from a dictionary."""
        return cls(patterns=d.get("patterns", None))


@dataclass
class SparseCheckoutUpdate:
    """Sparse checkout configuration, it contains options like cone patterns."""

    patterns: Optional[List[str]] = None
    """List of sparse checkout cone patterns, see [cone mode handling] for details.
    
    [cone mode handling]: https://git-scm.com/docs/git-sparse-checkout#_internalscone_mode_handling"""

    def as_dict(self) -> dict:
        """Serializes the SparseCheckoutUpdate into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.patterns:
            body["patterns"] = [v for v in self.patterns]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SparseCheckoutUpdate into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.patterns:
            body["patterns"] = self.patterns
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SparseCheckoutUpdate:
        """Deserializes the SparseCheckoutUpdate from a dictionary."""
        return cls(patterns=d.get("patterns", None))


@dataclass
class UpdateCredentialsResponse:
    def as_dict(self) -> dict:
        """Serializes the UpdateCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateCredentialsResponse:
        """Deserializes the UpdateCredentialsResponse from a dictionary."""
        return cls()


@dataclass
class UpdateRepoResponse:
    def as_dict(self) -> dict:
        """Serializes the UpdateRepoResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateRepoResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateRepoResponse:
        """Deserializes the UpdateRepoResponse from a dictionary."""
        return cls()


@dataclass
class WorkspaceObjectAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[WorkspaceObjectPermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the WorkspaceObjectAccessControlRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceObjectAccessControlRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceObjectAccessControlRequest:
        """Deserializes the WorkspaceObjectAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", WorkspaceObjectPermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class WorkspaceObjectAccessControlResponse:
    all_permissions: Optional[List[WorkspaceObjectPermission]] = None
    """All permissions."""

    display_name: Optional[str] = None
    """Display name of the user or service principal."""

    group_name: Optional[str] = None
    """name of the group"""

    service_principal_name: Optional[str] = None
    """Name of the service principal."""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the WorkspaceObjectAccessControlResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = [v.as_dict() for v in self.all_permissions]
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceObjectAccessControlResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = self.all_permissions
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceObjectAccessControlResponse:
        """Deserializes the WorkspaceObjectAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", WorkspaceObjectPermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class WorkspaceObjectPermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[WorkspaceObjectPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the WorkspaceObjectPermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceObjectPermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceObjectPermission:
        """Deserializes the WorkspaceObjectPermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", WorkspaceObjectPermissionLevel),
        )


class WorkspaceObjectPermissionLevel(Enum):
    """Permission level"""

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_READ = "CAN_READ"
    CAN_RUN = "CAN_RUN"


@dataclass
class WorkspaceObjectPermissions:
    access_control_list: Optional[List[WorkspaceObjectAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the WorkspaceObjectPermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceObjectPermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceObjectPermissions:
        """Deserializes the WorkspaceObjectPermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", WorkspaceObjectAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class WorkspaceObjectPermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[WorkspaceObjectPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the WorkspaceObjectPermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceObjectPermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceObjectPermissionsDescription:
        """Deserializes the WorkspaceObjectPermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None),
            permission_level=_enum(d, "permission_level", WorkspaceObjectPermissionLevel),
        )


class GitCredentialsAPI:
    """Registers personal access token for Databricks to do operations on behalf of the user.

    See [more info].

    [more info]: https://docs.databricks.com/repos/get-access-tokens-from-git-provider.html"""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        git_provider: str,
        *,
        git_email: Optional[str] = None,
        git_username: Optional[str] = None,
        is_default_for_provider: Optional[bool] = None,
        name: Optional[str] = None,
        personal_access_token: Optional[str] = None,
        principal_id: Optional[int] = None,
    ) -> CreateCredentialsResponse:
        """Creates a Git credential entry for the user. Use the PATCH endpoint to update existing credentials, or
        the DELETE endpoint to delete existing credentials.

        :param git_provider: str
          Git provider. This field is case-insensitive. The available Git providers are `gitHub`,
          `bitbucketCloud`, `gitLab`, `azureDevOpsServices`, `gitHubEnterprise`, `bitbucketServer`,
          `gitLabEnterpriseEdition` and `awsCodeCommit`.
        :param git_email: str (optional)
          The authenticating email associated with your Git provider user account. Used for authentication
          with the remote repository and also sets the author & committer identity for commits. Required for
          most Git providers except AWS CodeCommit. Learn more at
          https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider
        :param git_username: str (optional)
          The username provided with your Git provider account and associated with the credential. For most
          Git providers it is only used to set the Git committer & author names for commits, however it may be
          required for authentication depending on your Git provider / token requirements. Required for AWS
          CodeCommit.
        :param is_default_for_provider: bool (optional)
          if the credential is the default for the given provider
        :param name: str (optional)
          the name of the git credential, used for identification and ease of lookup
        :param personal_access_token: str (optional)
          The personal access token used to authenticate to the corresponding Git provider. For certain
          providers, support may exist for other types of scoped access tokens. [Learn more].

          [Learn more]: https://docs.databricks.com/repos/get-access-tokens-from-git-provider.html
        :param principal_id: int (optional)
          The ID of the service principal whose credentials will be modified. Only service principal managers
          can perform this action.

        :returns: :class:`CreateCredentialsResponse`
        """

        body = {}
        if git_email is not None:
            body["git_email"] = git_email
        if git_provider is not None:
            body["git_provider"] = git_provider
        if git_username is not None:
            body["git_username"] = git_username
        if is_default_for_provider is not None:
            body["is_default_for_provider"] = is_default_for_provider
        if name is not None:
            body["name"] = name
        if personal_access_token is not None:
            body["personal_access_token"] = personal_access_token
        if principal_id is not None:
            body["principal_id"] = principal_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/git-credentials", body=body, headers=headers)
        return CreateCredentialsResponse.from_dict(res)

    def delete(self, credential_id: int, *, principal_id: Optional[int] = None):
        """Deletes the specified Git credential.

        :param credential_id: int
          The ID for the corresponding credential to access.
        :param principal_id: int (optional)
          The ID of the service principal whose credentials will be modified. Only service principal managers
          can perform this action.


        """

        query = {}
        if principal_id is not None:
            query["principal_id"] = principal_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/git-credentials/{credential_id}", query=query, headers=headers)

    def get(self, credential_id: int, *, principal_id: Optional[int] = None) -> GetCredentialsResponse:
        """Gets the Git credential with the specified credential ID.

        :param credential_id: int
          The ID for the corresponding credential to access.
        :param principal_id: int (optional)
          The ID of the service principal whose credentials will be modified. Only service principal managers
          can perform this action.

        :returns: :class:`GetCredentialsResponse`
        """

        query = {}
        if principal_id is not None:
            query["principal_id"] = principal_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/git-credentials/{credential_id}", query=query, headers=headers)
        return GetCredentialsResponse.from_dict(res)

    def list(self, *, principal_id: Optional[int] = None) -> Iterator[CredentialInfo]:
        """Lists the calling user's Git credentials.

        :param principal_id: int (optional)
          The ID of the service principal whose credentials will be listed. Only service principal managers
          can perform this action.

        :returns: Iterator over :class:`CredentialInfo`
        """

        query = {}
        if principal_id is not None:
            query["principal_id"] = principal_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/git-credentials", query=query, headers=headers)
        parsed = ListCredentialsResponse.from_dict(json).credentials
        return parsed if parsed is not None else []

    def update(
        self,
        credential_id: int,
        git_provider: str,
        *,
        git_email: Optional[str] = None,
        git_username: Optional[str] = None,
        is_default_for_provider: Optional[bool] = None,
        name: Optional[str] = None,
        personal_access_token: Optional[str] = None,
        principal_id: Optional[int] = None,
    ):
        """Updates the specified Git credential.

        :param credential_id: int
          The ID for the corresponding credential to access.
        :param git_provider: str
          Git provider. This field is case-insensitive. The available Git providers are `gitHub`,
          `bitbucketCloud`, `gitLab`, `azureDevOpsServices`, `gitHubEnterprise`, `bitbucketServer`,
          `gitLabEnterpriseEdition` and `awsCodeCommit`.
        :param git_email: str (optional)
          The authenticating email associated with your Git provider user account. Used for authentication
          with the remote repository and also sets the author & committer identity for commits. Required for
          most Git providers except AWS CodeCommit. Learn more at
          https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider
        :param git_username: str (optional)
          The username provided with your Git provider account and associated with the credential. For most
          Git providers it is only used to set the Git committer & author names for commits, however it may be
          required for authentication depending on your Git provider / token requirements. Required for AWS
          CodeCommit.
        :param is_default_for_provider: bool (optional)
          if the credential is the default for the given provider
        :param name: str (optional)
          the name of the git credential, used for identification and ease of lookup
        :param personal_access_token: str (optional)
          The personal access token used to authenticate to the corresponding Git provider. For certain
          providers, support may exist for other types of scoped access tokens. [Learn more].

          [Learn more]: https://docs.databricks.com/repos/get-access-tokens-from-git-provider.html
        :param principal_id: int (optional)
          The ID of the service principal whose credentials will be modified. Only service principal managers
          can perform this action.


        """

        body = {}
        if git_email is not None:
            body["git_email"] = git_email
        if git_provider is not None:
            body["git_provider"] = git_provider
        if git_username is not None:
            body["git_username"] = git_username
        if is_default_for_provider is not None:
            body["is_default_for_provider"] = is_default_for_provider
        if name is not None:
            body["name"] = name
        if personal_access_token is not None:
            body["personal_access_token"] = personal_access_token
        if principal_id is not None:
            body["principal_id"] = principal_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PATCH", f"/api/2.0/git-credentials/{credential_id}", body=body, headers=headers)


class ReposAPI:
    """The Repos API allows users to manage their git repos. Users can use the API to access all repos that they
    have manage permissions on.

    Databricks Repos is a visual Git client in Databricks. It supports common Git operations such a cloning a
    repository, committing and pushing, pulling, branch management, and visual comparison of diffs when
    committing.

    Within Repos you can develop code in notebooks or other files and follow data science and engineering code
    development best practices using Git for version control, collaboration, and CI/CD."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, url: str, provider: str, *, path: Optional[str] = None, sparse_checkout: Optional[SparseCheckout] = None
    ) -> CreateRepoResponse:
        """Creates a repo in the workspace and links it to the remote Git repo specified. Note that repos created
        programmatically must be linked to a remote Git repo, unlike repos created in the browser.

        :param url: str
          URL of the Git repository to be linked.
        :param provider: str
          Git provider. This field is case-insensitive. The available Git providers are `gitHub`,
          `bitbucketCloud`, `gitLab`, `azureDevOpsServices`, `gitHubEnterprise`, `bitbucketServer`,
          `gitLabEnterpriseEdition` and `awsCodeCommit`.
        :param path: str (optional)
          Desired path for the repo in the workspace. Almost any path in the workspace can be chosen. If repo
          is created in `/Repos`, path must be in the format `/Repos/{folder}/{repo-name}`.
        :param sparse_checkout: :class:`SparseCheckout` (optional)
          If specified, the repo will be created with sparse checkout enabled. You cannot enable/disable
          sparse checkout after the repo is created.

        :returns: :class:`CreateRepoResponse`
        """

        body = {}
        if path is not None:
            body["path"] = path
        if provider is not None:
            body["provider"] = provider
        if sparse_checkout is not None:
            body["sparse_checkout"] = sparse_checkout.as_dict()
        if url is not None:
            body["url"] = url
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/repos", body=body, headers=headers)
        return CreateRepoResponse.from_dict(res)

    def delete(self, repo_id: int):
        """Deletes the specified repo.

        :param repo_id: int
          The ID for the corresponding repo to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/repos/{repo_id}", headers=headers)

    def get(self, repo_id: int) -> GetRepoResponse:
        """Returns the repo with the given repo ID.

        :param repo_id: int
          ID of the Git folder (repo) object in the workspace.

        :returns: :class:`GetRepoResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/repos/{repo_id}", headers=headers)
        return GetRepoResponse.from_dict(res)

    def get_permission_levels(self, repo_id: str) -> GetRepoPermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param repo_id: str
          The repo for which to get or manage permissions.

        :returns: :class:`GetRepoPermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/repos/{repo_id}/permissionLevels", headers=headers)
        return GetRepoPermissionLevelsResponse.from_dict(res)

    def get_permissions(self, repo_id: str) -> RepoPermissions:
        """Gets the permissions of a repo. Repos can inherit permissions from their root object.

        :param repo_id: str
          The repo for which to get or manage permissions.

        :returns: :class:`RepoPermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/repos/{repo_id}", headers=headers)
        return RepoPermissions.from_dict(res)

    def list(self, *, next_page_token: Optional[str] = None, path_prefix: Optional[str] = None) -> Iterator[RepoInfo]:
        """Returns repos that the calling user has Manage permissions on. Use `next_page_token` to iterate
        through additional pages.

        :param next_page_token: str (optional)
          Token used to get the next page of results. If not specified, returns the first page of results as
          well as a next page token if there are more results.
        :param path_prefix: str (optional)
          Filters repos that have paths starting with the given path prefix. If not provided or when provided
          an effectively empty prefix (`/` or `/Workspace`) Git folders (repos) from `/Workspace/Repos` will
          be served.

        :returns: Iterator over :class:`RepoInfo`
        """

        query = {}
        if next_page_token is not None:
            query["next_page_token"] = next_page_token
        if path_prefix is not None:
            query["path_prefix"] = path_prefix
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/repos", query=query, headers=headers)
            if "repos" in json:
                for v in json["repos"]:
                    yield RepoInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["next_page_token"] = json["next_page_token"]

    def set_permissions(
        self, repo_id: str, *, access_control_list: Optional[List[RepoAccessControlRequest]] = None
    ) -> RepoPermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param repo_id: str
          The repo for which to get or manage permissions.
        :param access_control_list: List[:class:`RepoAccessControlRequest`] (optional)

        :returns: :class:`RepoPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/permissions/repos/{repo_id}", body=body, headers=headers)
        return RepoPermissions.from_dict(res)

    def update(
        self,
        repo_id: int,
        *,
        branch: Optional[str] = None,
        sparse_checkout: Optional[SparseCheckoutUpdate] = None,
        tag: Optional[str] = None,
    ):
        """Updates the repo to a different branch or tag, or updates the repo to the latest commit on the same
        branch.

        :param repo_id: int
          ID of the Git folder (repo) object in the workspace.
        :param branch: str (optional)
          Branch that the local version of the repo is checked out to.
        :param sparse_checkout: :class:`SparseCheckoutUpdate` (optional)
          If specified, update the sparse checkout settings. The update will fail if sparse checkout is not
          enabled for the repo.
        :param tag: str (optional)
          Tag that the local version of the repo is checked out to. Updating the repo to a tag puts the repo
          in a detached HEAD state. Before committing new changes, you must update the repo to a branch
          instead of the detached HEAD.


        """

        body = {}
        if branch is not None:
            body["branch"] = branch
        if sparse_checkout is not None:
            body["sparse_checkout"] = sparse_checkout.as_dict()
        if tag is not None:
            body["tag"] = tag
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PATCH", f"/api/2.0/repos/{repo_id}", body=body, headers=headers)

    def update_permissions(
        self, repo_id: str, *, access_control_list: Optional[List[RepoAccessControlRequest]] = None
    ) -> RepoPermissions:
        """Updates the permissions on a repo. Repos can inherit permissions from their root object.

        :param repo_id: str
          The repo for which to get or manage permissions.
        :param access_control_list: List[:class:`RepoAccessControlRequest`] (optional)

        :returns: :class:`RepoPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/permissions/repos/{repo_id}", body=body, headers=headers)
        return RepoPermissions.from_dict(res)


class SecretsAPI:
    """The Secrets API allows you to manage secrets, secret scopes, and access permissions.

    Sometimes accessing data requires that you authenticate to external data sources through JDBC. Instead of
    directly entering your credentials into a notebook, use Databricks secrets to store your credentials and
    reference them in notebooks and jobs.

    Administrators, secret creators, and users granted permission can read Databricks secrets. While
    Databricks makes an effort to redact secret values that might be displayed in notebooks, it is not
    possible to prevent such users from reading secrets."""

    def __init__(self, api_client):
        self._api = api_client

    def create_scope(
        self,
        scope: str,
        *,
        backend_azure_keyvault: Optional[AzureKeyVaultSecretScopeMetadata] = None,
        initial_manage_principal: Optional[str] = None,
        scope_backend_type: Optional[ScopeBackendType] = None,
    ):
        """Creates a new secret scope.

        The scope name must consist of alphanumeric characters, dashes, underscores, and periods, and may not
        exceed 128 characters.

        Example request:

        .. code::

        { "scope": "my-simple-databricks-scope", "initial_manage_principal": "users" "scope_backend_type":
        "databricks|azure_keyvault", # below is only required if scope type is azure_keyvault
        "backend_azure_keyvault": { "resource_id":
        "/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/resourceGroups/xxxx/providers/Microsoft.KeyVault/vaults/xxxx",
        "tenant_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", "dns_name": "https://xxxx.vault.azure.net/", } }

        If ``initial_manage_principal`` is specified, the initial ACL applied to the scope is applied to the
        supplied principal (user or group) with ``MANAGE`` permissions. The only supported principal for this
        option is the group ``users``, which contains all users in the workspace. If
        ``initial_manage_principal`` is not specified, the initial ACL with ``MANAGE`` permission applied to
        the scope is assigned to the API request issuer's user identity.

        If ``scope_backend_type`` is ``azure_keyvault``, a secret scope is created with secrets from a given
        Azure KeyVault. The caller must provide the keyvault_resource_id and the tenant_id for the key vault.
        If ``scope_backend_type`` is ``databricks`` or is unspecified, an empty secret scope is created and
        stored in Databricks's own storage.

        Throws ``RESOURCE_ALREADY_EXISTS`` if a scope with the given name already exists. Throws
        ``RESOURCE_LIMIT_EXCEEDED`` if maximum number of scopes in the workspace is exceeded. Throws
        ``INVALID_PARAMETER_VALUE`` if the scope name is invalid. Throws ``BAD_REQUEST`` if request violated
        constraints. Throws ``CUSTOMER_UNAUTHORIZED`` if normal user attempts to create a scope with name
        reserved for databricks internal usage. Throws ``UNAUTHENTICATED`` if unable to verify user access
        permission on Azure KeyVault

        :param scope: str
          Scope name requested by the user. Scope names are unique.
        :param backend_azure_keyvault: :class:`AzureKeyVaultSecretScopeMetadata` (optional)
          The metadata for the secret scope if the type is ``AZURE_KEYVAULT``
        :param initial_manage_principal: str (optional)
          The principal that is initially granted ``MANAGE`` permission to the created scope.
        :param scope_backend_type: :class:`ScopeBackendType` (optional)
          The backend type the scope will be created with. If not specified, will default to ``DATABRICKS``


        """

        body = {}
        if backend_azure_keyvault is not None:
            body["backend_azure_keyvault"] = backend_azure_keyvault.as_dict()
        if initial_manage_principal is not None:
            body["initial_manage_principal"] = initial_manage_principal
        if scope is not None:
            body["scope"] = scope
        if scope_backend_type is not None:
            body["scope_backend_type"] = scope_backend_type.value
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/secrets/scopes/create", body=body, headers=headers)

    def delete_acl(self, scope: str, principal: str):
        """Deletes the given ACL on the given scope.

        Users must have the ``MANAGE`` permission to invoke this API.

        Example request:

        .. code::

        { "scope": "my-secret-scope", "principal": "data-scientists" }

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope, principal, or ACL exists. Throws
        ``PERMISSION_DENIED`` if the user does not have permission to make this API call. Throws
        ``INVALID_PARAMETER_VALUE`` if the permission or principal is invalid.

        :param scope: str
          The name of the scope to remove permissions from.
        :param principal: str
          The principal to remove an existing ACL from.


        """

        body = {}
        if principal is not None:
            body["principal"] = principal
        if scope is not None:
            body["scope"] = scope
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/secrets/acls/delete", body=body, headers=headers)

    def delete_scope(self, scope: str):
        """Deletes a secret scope.

        Example request:

        .. code::

        { "scope": "my-secret-scope" }

        Throws ``RESOURCE_DOES_NOT_EXIST`` if the scope does not exist. Throws ``PERMISSION_DENIED`` if the
        user does not have permission to make this API call. Throws ``BAD_REQUEST`` if system user attempts to
        delete internal secret scope.

        :param scope: str
          Name of the scope to delete.


        """

        body = {}
        if scope is not None:
            body["scope"] = scope
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/secrets/scopes/delete", body=body, headers=headers)

    def delete_secret(self, scope: str, key: str):
        """Deletes the secret stored in this secret scope. You must have ``WRITE`` or ``MANAGE`` permission on
        the Secret Scope.

        Example request:

        .. code::

        { "scope": "my-secret-scope", "key": "my-secret-key" }

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope or secret exists. Throws
        ``PERMISSION_DENIED`` if the user does not have permission to make this API call. Throws
        ``BAD_REQUEST`` if system user attempts to delete an internal secret, or request is made against Azure
        KeyVault backed scope.

        :param scope: str
          The name of the scope that contains the secret to delete.
        :param key: str
          Name of the secret to delete.


        """

        body = {}
        if key is not None:
            body["key"] = key
        if scope is not None:
            body["scope"] = scope
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/secrets/delete", body=body, headers=headers)

    def get_acl(self, scope: str, principal: str) -> AclItem:
        """Describes the details about the given ACL, such as the group and permission.

        Users must have the ``MANAGE`` permission to invoke this API.

        Example response:

        .. code::

        { "principal": "data-scientists", "permission": "READ" }

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope exists. Throws ``PERMISSION_DENIED`` if the
        user does not have permission to make this API call. Throws ``INVALID_PARAMETER_VALUE`` if the
        permission or principal is invalid.

        :param scope: str
          The name of the scope to fetch ACL information from.
        :param principal: str
          The principal to fetch ACL information for.

        :returns: :class:`AclItem`
        """

        query = {}
        if principal is not None:
            query["principal"] = principal
        if scope is not None:
            query["scope"] = scope
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/secrets/acls/get", query=query, headers=headers)
        return AclItem.from_dict(res)

    def get_secret(self, scope: str, key: str) -> GetSecretResponse:
        """Gets a secret for a given key and scope. This API can only be called from the DBUtils interface. Users
        need the READ permission to make this call.

        Example response:

        .. code::

        { "key": "my-string-key", "value": <bytes of the secret value> }

        Note that the secret value returned is in bytes. The interpretation of the bytes is determined by the
        caller in DBUtils and the type the data is decoded into.

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret or secret scope exists. Throws
        ``PERMISSION_DENIED`` if the user does not have permission to make this API call.

        Note: This is explicitly an undocumented API. It also doesn't need to be supported for the /preview
        prefix, because it's not a customer-facing API (i.e. only used for DBUtils SecretUtils to fetch
        secrets).

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope or secret exists. Throws ``BAD_REQUEST`` if
        normal user calls get secret outside of a notebook. AKV specific errors: Throws
        ``INVALID_PARAMETER_VALUE`` if secret name is not alphanumeric or too long. Throws
        ``PERMISSION_DENIED`` if secret manager cannot access AKV with 403 error Throws ``MALFORMED_REQUEST``
        if secret manager cannot access AKV with any other 4xx error

        :param scope: str
          The name of the scope that contains the secret.
        :param key: str
          Name of the secret to fetch value information.

        :returns: :class:`GetSecretResponse`
        """

        query = {}
        if key is not None:
            query["key"] = key
        if scope is not None:
            query["scope"] = scope
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/secrets/get", query=query, headers=headers)
        return GetSecretResponse.from_dict(res)

    def list_acls(self, scope: str) -> Iterator[AclItem]:
        """Lists the ACLs set on the given scope.

        Users must have the ``MANAGE`` permission to invoke this API.

        Example response:

        .. code::

        { "acls": [{ "principal": "admins", "permission": "MANAGE" },{ "principal": "data-scientists",
        "permission": "READ" }] }

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope exists. Throws ``PERMISSION_DENIED`` if the
        user does not have permission to make this API call.

        :param scope: str
          The name of the scope to fetch ACL information from.

        :returns: Iterator over :class:`AclItem`
        """

        query = {}
        if scope is not None:
            query["scope"] = scope
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/secrets/acls/list", query=query, headers=headers)
        parsed = ListAclsResponse.from_dict(json).items
        return parsed if parsed is not None else []

    def list_scopes(self) -> Iterator[SecretScope]:
        """Lists all secret scopes available in the workspace.

        Example response:

        .. code::

        { "scopes": [{ "name": "my-databricks-scope", "backend_type": "DATABRICKS" },{ "name": "mount-points",
        "backend_type": "DATABRICKS" }] }

        Throws ``PERMISSION_DENIED`` if the user does not have permission to make this API call.


        :returns: Iterator over :class:`SecretScope`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/secrets/scopes/list", headers=headers)
        parsed = ListScopesResponse.from_dict(json).scopes
        return parsed if parsed is not None else []

    def list_secrets(self, scope: str) -> Iterator[SecretMetadata]:
        """Lists the secret keys that are stored at this scope. This is a metadata-only operation; secret data
        cannot be retrieved using this API. Users need the READ permission to make this call.

        Example response:

        .. code::

        { "secrets": [ { "key": "my-string-key"", "last_updated_timestamp": "1520467595000" }, { "key":
        "my-byte-key", "last_updated_timestamp": "1520467595000" }, ] }

        The lastUpdatedTimestamp returned is in milliseconds since epoch.

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope exists. Throws ``PERMISSION_DENIED`` if the
        user does not have permission to make this API call.

        :param scope: str
          The name of the scope to list secrets within.

        :returns: Iterator over :class:`SecretMetadata`
        """

        query = {}
        if scope is not None:
            query["scope"] = scope
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/secrets/list", query=query, headers=headers)
        parsed = ListSecretsResponse.from_dict(json).secrets
        return parsed if parsed is not None else []

    def put_acl(self, scope: str, principal: str, permission: AclPermission):
        """Creates or overwrites the ACL associated with the given principal (user or group) on the specified
        scope point. In general, a user or group will use the most powerful permission available to them, and
        permissions are ordered as follows:

        * ``MANAGE`` - Allowed to change ACLs, and read and write to this secret scope. * ``WRITE`` - Allowed
        to read and write to this secret scope. * ``READ`` - Allowed to read this secret scope and list what
        secrets are available.

        Note that in general, secret values can only be read from within a command on a cluster (for example,
        through a notebook). There is no API to read the actual secret value material outside of a cluster.
        However, the user's permission will be applied based on who is executing the command, and they must
        have at least READ permission.

        Users must have the ``MANAGE`` permission to invoke this API.

        Example request:

        .. code::

        { "scope": "my-secret-scope", "principal": "data-scientists", "permission": "READ" }

        The principal is a user or group name corresponding to an existing Databricks principal to be granted
        or revoked access.

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope exists. Throws ``RESOURCE_ALREADY_EXISTS``
        if a permission for the principal already exists. Throws ``INVALID_PARAMETER_VALUE`` if the permission
        or principal is invalid. Throws ``PERMISSION_DENIED`` if the user does not have permission to make
        this API call.

        :param scope: str
          The name of the scope to apply permissions to.
        :param principal: str
          The principal in which the permission is applied.
        :param permission: :class:`AclPermission`
          The permission level applied to the principal.


        """

        body = {}
        if permission is not None:
            body["permission"] = permission.value
        if principal is not None:
            body["principal"] = principal
        if scope is not None:
            body["scope"] = scope
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/secrets/acls/put", body=body, headers=headers)

    def put_secret(
        self, scope: str, key: str, *, bytes_value: Optional[str] = None, string_value: Optional[str] = None
    ):
        """Inserts a secret under the provided scope with the given name. If a secret already exists with the
        same name, this command overwrites the existing secret's value. The server encrypts the secret using
        the secret scope's encryption settings before storing it. You must have ``WRITE`` or ``MANAGE``
        permission on the secret scope.

        The secret key must consist of alphanumeric characters, dashes, underscores, and periods, and cannot
        exceed 128 characters. The maximum allowed secret value size is 128 KB. The maximum number of secrets
        in a given scope is 1000.

        Example request:

        .. code::

        { "scope": "my-databricks-scope", "key": "my-string-key", "string_value": "foobar" }

        The input fields "string_value" or "bytes_value" specify the type of the secret, which will determine
        the value returned when the secret value is requested. Exactly one must be specified.

        Throws ``RESOURCE_DOES_NOT_EXIST`` if no such secret scope exists. Throws ``RESOURCE_LIMIT_EXCEEDED``
        if maximum number of secrets in scope is exceeded. Throws ``INVALID_PARAMETER_VALUE`` if the request
        parameters are invalid. Throws ``PERMISSION_DENIED`` if the user does not have permission to make this
        API call. Throws ``MALFORMED_REQUEST`` if request is incorrectly formatted or conflicting. Throws
        ``BAD_REQUEST`` if request is made against Azure KeyVault backed scope.

        :param scope: str
          The name of the scope to which the secret will be associated with.
        :param key: str
          A unique name to identify the secret.
        :param bytes_value: str (optional)
          If specified, value will be stored as bytes.
        :param string_value: str (optional)
          If specified, note that the value will be stored in UTF-8 (MB4) form.


        """

        body = {}
        if bytes_value is not None:
            body["bytes_value"] = bytes_value
        if key is not None:
            body["key"] = key
        if scope is not None:
            body["scope"] = scope
        if string_value is not None:
            body["string_value"] = string_value
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/secrets/put", body=body, headers=headers)


class WorkspaceAPI:
    """The Workspace API allows you to list, import, export, and delete notebooks and folders.

    A notebook is a web-based interface to a document that contains runnable code, visualizations, and
    explanatory text."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, path: str, *, recursive: Optional[bool] = None):
        """Deletes an object or a directory (and optionally recursively deletes all objects in the directory). *
        If `path` does not exist, this call returns an error `RESOURCE_DOES_NOT_EXIST`. * If `path` is a
        non-empty directory and `recursive` is set to `false`, this call returns an error
        `DIRECTORY_NOT_EMPTY`.

        Object deletion cannot be undone and deleting a directory recursively is not atomic.

        :param path: str
          The absolute path of the notebook or directory.
        :param recursive: bool (optional)
          The flag that specifies whether to delete the object recursively. It is `false` by default. Please
          note this deleting directory is not atomic. If it fails in the middle, some of objects under this
          directory may be deleted and cannot be undone.


        """

        body = {}
        if path is not None:
            body["path"] = path
        if recursive is not None:
            body["recursive"] = recursive
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/workspace/delete", body=body, headers=headers)

    def export(self, path: str, *, format: Optional[ExportFormat] = None) -> ExportResponse:
        """Exports an object or the contents of an entire directory.

        If `path` does not exist, this call returns an error `RESOURCE_DOES_NOT_EXIST`.

        If the exported data would exceed size limit, this call returns `MAX_NOTEBOOK_SIZE_EXCEEDED`.
        Currently, this API does not support exporting a library.

        :param path: str
          The absolute path of the object or directory. Exporting a directory is only supported for the `DBC`,
          `SOURCE`, and `AUTO` format.
        :param format: :class:`ExportFormat` (optional)
          This specifies the format of the exported file. By default, this is `SOURCE`.

          The value is case sensitive.

          - `SOURCE`: The notebook is exported as source code. Directory exports will not include non-notebook
          entries. - `HTML`: The notebook is exported as an HTML file. - `JUPYTER`: The notebook is exported
          as a Jupyter/IPython Notebook file. - `DBC`: The notebook is exported in Databricks archive format.
          Directory exports will not include non-notebook entries. - `R_MARKDOWN`: The notebook is exported to
          R Markdown format. - `AUTO`: The object or directory is exported depending on the objects type.
          Directory exports will include notebooks and workspace files.

        :returns: :class:`ExportResponse`
        """

        query = {}
        if format is not None:
            query["format"] = format.value
        if path is not None:
            query["path"] = path
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/workspace/export", query=query, headers=headers)
        return ExportResponse.from_dict(res)

    def get_permission_levels(
        self, workspace_object_type: str, workspace_object_id: str
    ) -> GetWorkspaceObjectPermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param workspace_object_type: str
          The workspace object type for which to get or manage permissions. Could be one of the following:
          alerts, alertsv2, dashboards, dbsql-dashboards, directories, experiments, files, genie, notebooks,
          queries
        :param workspace_object_id: str
          The workspace object for which to get or manage permissions.

        :returns: :class:`GetWorkspaceObjectPermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/permissions/{workspace_object_type}/{workspace_object_id}/permissionLevels",
            headers=headers,
        )
        return GetWorkspaceObjectPermissionLevelsResponse.from_dict(res)

    def get_permissions(self, workspace_object_type: str, workspace_object_id: str) -> WorkspaceObjectPermissions:
        """Gets the permissions of a workspace object. Workspace objects can inherit permissions from their
        parent objects or root object.

        :param workspace_object_type: str
          The workspace object type for which to get or manage permissions. Could be one of the following:
          alerts, alertsv2, dashboards, dbsql-dashboards, directories, experiments, files, genie, notebooks,
          queries
        :param workspace_object_id: str
          The workspace object for which to get or manage permissions.

        :returns: :class:`WorkspaceObjectPermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/permissions/{workspace_object_type}/{workspace_object_id}", headers=headers
        )
        return WorkspaceObjectPermissions.from_dict(res)

    def get_status(self, path: str) -> ObjectInfo:
        """Gets the status of an object or a directory. If `path` does not exist, this call returns an error
        `RESOURCE_DOES_NOT_EXIST`.

        :param path: str
          The absolute path of the notebook or directory.

        :returns: :class:`ObjectInfo`
        """

        query = {}
        if path is not None:
            query["path"] = path
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/workspace/get-status", query=query, headers=headers)
        return ObjectInfo.from_dict(res)

    def import_(
        self,
        path: str,
        *,
        content: Optional[str] = None,
        format: Optional[ImportFormat] = None,
        language: Optional[Language] = None,
        overwrite: Optional[bool] = None,
    ):
        """Imports a workspace object (for example, a notebook or file) or the contents of an entire directory.
        If `path` already exists and `overwrite` is set to `false`, this call returns an error
        `RESOURCE_ALREADY_EXISTS`. To import a directory, you can use either the `DBC` format or the `SOURCE`
        format with the `language` field unset. To import a single file as `SOURCE`, you must set the
        `language` field. Zip files within directories are not supported.

        :param path: str
          The absolute path of the object or directory. Importing a directory is only supported for the `DBC`
          and `SOURCE` formats.
        :param content: str (optional)
          The base64-encoded content. This has a limit of 10 MB.

          If the limit (10MB) is exceeded, exception with error code **MAX_NOTEBOOK_SIZE_EXCEEDED** is thrown.
          This parameter might be absent, and instead a posted file is used.
        :param format: :class:`ImportFormat` (optional)
          This specifies the format of the file to be imported.

          The value is case sensitive.

          - `AUTO`: The item is imported depending on an analysis of the item's extension and the header
          content provided in the request. If the item is imported as a notebook, then the item's extension is
          automatically removed. - `SOURCE`: The notebook or directory is imported as source code. - `HTML`:
          The notebook is imported as an HTML file. - `JUPYTER`: The notebook is imported as a Jupyter/IPython
          Notebook file. - `DBC`: The notebook is imported in Databricks archive format. Required for
          directories. - `R_MARKDOWN`: The notebook is imported from R Markdown format.
        :param language: :class:`Language` (optional)
          The language of the object. This value is set only if the object type is `NOTEBOOK`.
        :param overwrite: bool (optional)
          The flag that specifies whether to overwrite existing object. It is `false` by default. For `DBC`
          format, `overwrite` is not supported since it may contain a directory.


        """

        body = {}
        if content is not None:
            body["content"] = content
        if format is not None:
            body["format"] = format.value
        if language is not None:
            body["language"] = language.value
        if overwrite is not None:
            body["overwrite"] = overwrite
        if path is not None:
            body["path"] = path
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/workspace/import", body=body, headers=headers)

    def list(self, path: str, *, notebooks_modified_after: Optional[int] = None) -> Iterator[ObjectInfo]:
        """Lists the contents of a directory, or the object if it is not a directory. If the input path does not
        exist, this call returns an error `RESOURCE_DOES_NOT_EXIST`.

        :param path: str
          The absolute path of the notebook or directory.
        :param notebooks_modified_after: int (optional)
          UTC timestamp in milliseconds

        :returns: Iterator over :class:`ObjectInfo`
        """

        query = {}
        if notebooks_modified_after is not None:
            query["notebooks_modified_after"] = notebooks_modified_after
        if path is not None:
            query["path"] = path
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/workspace/list", query=query, headers=headers)
        parsed = ListResponse.from_dict(json).objects
        return parsed if parsed is not None else []

    def mkdirs(self, path: str):
        """Creates the specified directory (and necessary parent directories if they do not exist). If there is
        an object (not a directory) at any prefix of the input path, this call returns an error
        `RESOURCE_ALREADY_EXISTS`.

        Note that if this operation fails it may have succeeded in creating some of the necessary parent
        directories.

        :param path: str
          The absolute path of the directory. If the parent directories do not exist, it will also create
          them. If the directory already exists, this command will do nothing and succeed.


        """

        body = {}
        if path is not None:
            body["path"] = path
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/workspace/mkdirs", body=body, headers=headers)

    def set_permissions(
        self,
        workspace_object_type: str,
        workspace_object_id: str,
        *,
        access_control_list: Optional[List[WorkspaceObjectAccessControlRequest]] = None,
    ) -> WorkspaceObjectPermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their parent objects or root
        object.

        :param workspace_object_type: str
          The workspace object type for which to get or manage permissions. Could be one of the following:
          alerts, alertsv2, dashboards, dbsql-dashboards, directories, experiments, files, genie, notebooks,
          queries
        :param workspace_object_id: str
          The workspace object for which to get or manage permissions.
        :param access_control_list: List[:class:`WorkspaceObjectAccessControlRequest`] (optional)

        :returns: :class:`WorkspaceObjectPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PUT", f"/api/2.0/permissions/{workspace_object_type}/{workspace_object_id}", body=body, headers=headers
        )
        return WorkspaceObjectPermissions.from_dict(res)

    def update_permissions(
        self,
        workspace_object_type: str,
        workspace_object_id: str,
        *,
        access_control_list: Optional[List[WorkspaceObjectAccessControlRequest]] = None,
    ) -> WorkspaceObjectPermissions:
        """Updates the permissions on a workspace object. Workspace objects can inherit permissions from their
        parent objects or root object.

        :param workspace_object_type: str
          The workspace object type for which to get or manage permissions. Could be one of the following:
          alerts, alertsv2, dashboards, dbsql-dashboards, directories, experiments, files, genie, notebooks,
          queries
        :param workspace_object_id: str
          The workspace object for which to get or manage permissions.
        :param access_control_list: List[:class:`WorkspaceObjectAccessControlRequest`] (optional)

        :returns: :class:`WorkspaceObjectPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.0/permissions/{workspace_object_type}/{workspace_object_id}", body=body, headers=headers
        )
        return WorkspaceObjectPermissions.from_dict(res)
