# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional

from google.protobuf.timestamp_pb2 import Timestamp

from databricks.sdk.client_types import HostType
from databricks.sdk.common import lro
from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.retries import RetryError, poll
from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict, _timestamp)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class App:
    name: str
    """The name of the app. The name must contain only lowercase alphanumeric characters and hyphens.
    It must be unique within the workspace."""

    active_deployment: Optional[AppDeployment] = None
    """The active deployment of the app. A deployment is considered active when it has been deployed to
    the app compute."""

    app_status: Optional[ApplicationStatus] = None

    budget_policy_id: Optional[str] = None

    compute_size: Optional[ComputeSize] = None

    compute_status: Optional[ComputeStatus] = None

    create_time: Optional[str] = None
    """The creation time of the app. Formatted timestamp in ISO 6801."""

    creator: Optional[str] = None
    """The email of the user that created the app."""

    default_source_code_path: Optional[str] = None
    """The default workspace file system path of the source code from which app deployment are created.
    This field tracks the workspace source code path of the last active deployment."""

    description: Optional[str] = None
    """The description of the app."""

    effective_budget_policy_id: Optional[str] = None

    effective_usage_policy_id: Optional[str] = None

    effective_user_api_scopes: Optional[List[str]] = None
    """The effective api scopes granted to the user access token."""

    git_repository: Optional[GitRepository] = None
    """Git repository configuration for app deployments. When specified, deployments can reference code
    from this repository by providing only the git reference (branch, tag, or commit)."""

    id: Optional[str] = None
    """The unique identifier of the app."""

    oauth2_app_client_id: Optional[str] = None

    oauth2_app_integration_id: Optional[str] = None

    pending_deployment: Optional[AppDeployment] = None
    """The pending deployment of the app. A deployment is considered pending when it is being prepared
    for deployment to the app compute."""

    resources: Optional[List[AppResource]] = None
    """Resources for the app."""

    service_principal_client_id: Optional[str] = None

    service_principal_id: Optional[int] = None

    service_principal_name: Optional[str] = None

    space: Optional[str] = None
    """Name of the space this app belongs to."""

    update_time: Optional[str] = None
    """The update time of the app. Formatted timestamp in ISO 6801."""

    updater: Optional[str] = None
    """The email of the user that last updated the app."""

    url: Optional[str] = None
    """The URL of the app once it is deployed."""

    usage_policy_id: Optional[str] = None

    user_api_scopes: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the App into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.active_deployment:
            body["active_deployment"] = self.active_deployment.as_dict()
        if self.app_status:
            body["app_status"] = self.app_status.as_dict()
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.compute_size is not None:
            body["compute_size"] = self.compute_size.value
        if self.compute_status:
            body["compute_status"] = self.compute_status.as_dict()
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.default_source_code_path is not None:
            body["default_source_code_path"] = self.default_source_code_path
        if self.description is not None:
            body["description"] = self.description
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.effective_user_api_scopes:
            body["effective_user_api_scopes"] = [v for v in self.effective_user_api_scopes]
        if self.git_repository:
            body["git_repository"] = self.git_repository.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.oauth2_app_client_id is not None:
            body["oauth2_app_client_id"] = self.oauth2_app_client_id
        if self.oauth2_app_integration_id is not None:
            body["oauth2_app_integration_id"] = self.oauth2_app_integration_id
        if self.pending_deployment:
            body["pending_deployment"] = self.pending_deployment.as_dict()
        if self.resources:
            body["resources"] = [v.as_dict() for v in self.resources]
        if self.service_principal_client_id is not None:
            body["service_principal_client_id"] = self.service_principal_client_id
        if self.service_principal_id is not None:
            body["service_principal_id"] = self.service_principal_id
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.space is not None:
            body["space"] = self.space
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.updater is not None:
            body["updater"] = self.updater
        if self.url is not None:
            body["url"] = self.url
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = [v for v in self.user_api_scopes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the App into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.active_deployment:
            body["active_deployment"] = self.active_deployment
        if self.app_status:
            body["app_status"] = self.app_status
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.compute_size is not None:
            body["compute_size"] = self.compute_size
        if self.compute_status:
            body["compute_status"] = self.compute_status
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.default_source_code_path is not None:
            body["default_source_code_path"] = self.default_source_code_path
        if self.description is not None:
            body["description"] = self.description
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.effective_user_api_scopes:
            body["effective_user_api_scopes"] = self.effective_user_api_scopes
        if self.git_repository:
            body["git_repository"] = self.git_repository
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.oauth2_app_client_id is not None:
            body["oauth2_app_client_id"] = self.oauth2_app_client_id
        if self.oauth2_app_integration_id is not None:
            body["oauth2_app_integration_id"] = self.oauth2_app_integration_id
        if self.pending_deployment:
            body["pending_deployment"] = self.pending_deployment
        if self.resources:
            body["resources"] = self.resources
        if self.service_principal_client_id is not None:
            body["service_principal_client_id"] = self.service_principal_client_id
        if self.service_principal_id is not None:
            body["service_principal_id"] = self.service_principal_id
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.space is not None:
            body["space"] = self.space
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.updater is not None:
            body["updater"] = self.updater
        if self.url is not None:
            body["url"] = self.url
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = self.user_api_scopes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> App:
        """Deserializes the App from a dictionary."""
        return cls(
            active_deployment=_from_dict(d, "active_deployment", AppDeployment),
            app_status=_from_dict(d, "app_status", ApplicationStatus),
            budget_policy_id=d.get("budget_policy_id", None),
            compute_size=_enum(d, "compute_size", ComputeSize),
            compute_status=_from_dict(d, "compute_status", ComputeStatus),
            create_time=d.get("create_time", None),
            creator=d.get("creator", None),
            default_source_code_path=d.get("default_source_code_path", None),
            description=d.get("description", None),
            effective_budget_policy_id=d.get("effective_budget_policy_id", None),
            effective_usage_policy_id=d.get("effective_usage_policy_id", None),
            effective_user_api_scopes=d.get("effective_user_api_scopes", None),
            git_repository=_from_dict(d, "git_repository", GitRepository),
            id=d.get("id", None),
            name=d.get("name", None),
            oauth2_app_client_id=d.get("oauth2_app_client_id", None),
            oauth2_app_integration_id=d.get("oauth2_app_integration_id", None),
            pending_deployment=_from_dict(d, "pending_deployment", AppDeployment),
            resources=_repeated_dict(d, "resources", AppResource),
            service_principal_client_id=d.get("service_principal_client_id", None),
            service_principal_id=d.get("service_principal_id", None),
            service_principal_name=d.get("service_principal_name", None),
            space=d.get("space", None),
            update_time=d.get("update_time", None),
            updater=d.get("updater", None),
            url=d.get("url", None),
            usage_policy_id=d.get("usage_policy_id", None),
            user_api_scopes=d.get("user_api_scopes", None),
        )


@dataclass
class AppAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[AppPermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the AppAccessControlRequest into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the AppAccessControlRequest into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> AppAccessControlRequest:
        """Deserializes the AppAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", AppPermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class AppAccessControlResponse:
    all_permissions: Optional[List[AppPermission]] = None
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
        """Serializes the AppAccessControlResponse into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the AppAccessControlResponse into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> AppAccessControlResponse:
        """Deserializes the AppAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", AppPermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class AppDeployment:
    command: Optional[List[str]] = None
    """The command with which to run the app. This will override the command specified in the app.yaml
    file."""

    create_time: Optional[str] = None
    """The creation time of the deployment. Formatted timestamp in ISO 6801."""

    creator: Optional[str] = None
    """The email of the user creates the deployment."""

    deployment_artifacts: Optional[AppDeploymentArtifacts] = None
    """The deployment artifacts for an app."""

    deployment_id: Optional[str] = None
    """The unique id of the deployment."""

    env_vars: Optional[List[EnvVar]] = None
    """The environment variables to set in the app runtime environment. This will override the
    environment variables specified in the app.yaml file."""

    git_source: Optional[GitSource] = None
    """Git repository to use as the source for the app deployment."""

    mode: Optional[AppDeploymentMode] = None
    """The mode of which the deployment will manage the source code."""

    source_code_path: Optional[str] = None
    """The workspace file system path of the source code used to create the app deployment. This is
    different from `deployment_artifacts.source_code_path`, which is the path used by the deployed
    app. The former refers to the original source code location of the app in the workspace during
    deployment creation, whereas the latter provides a system generated stable snapshotted source
    code path used by the deployment."""

    status: Optional[AppDeploymentStatus] = None
    """Status and status message of the deployment"""

    update_time: Optional[str] = None
    """The update time of the deployment. Formatted timestamp in ISO 6801."""

    def as_dict(self) -> dict:
        """Serializes the AppDeployment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.command:
            body["command"] = [v for v in self.command]
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.deployment_artifacts:
            body["deployment_artifacts"] = self.deployment_artifacts.as_dict()
        if self.deployment_id is not None:
            body["deployment_id"] = self.deployment_id
        if self.env_vars:
            body["env_vars"] = [v.as_dict() for v in self.env_vars]
        if self.git_source:
            body["git_source"] = self.git_source.as_dict()
        if self.mode is not None:
            body["mode"] = self.mode.value
        if self.source_code_path is not None:
            body["source_code_path"] = self.source_code_path
        if self.status:
            body["status"] = self.status.as_dict()
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppDeployment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.command:
            body["command"] = self.command
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.deployment_artifacts:
            body["deployment_artifacts"] = self.deployment_artifacts
        if self.deployment_id is not None:
            body["deployment_id"] = self.deployment_id
        if self.env_vars:
            body["env_vars"] = self.env_vars
        if self.git_source:
            body["git_source"] = self.git_source
        if self.mode is not None:
            body["mode"] = self.mode
        if self.source_code_path is not None:
            body["source_code_path"] = self.source_code_path
        if self.status:
            body["status"] = self.status
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppDeployment:
        """Deserializes the AppDeployment from a dictionary."""
        return cls(
            command=d.get("command", None),
            create_time=d.get("create_time", None),
            creator=d.get("creator", None),
            deployment_artifacts=_from_dict(d, "deployment_artifacts", AppDeploymentArtifacts),
            deployment_id=d.get("deployment_id", None),
            env_vars=_repeated_dict(d, "env_vars", EnvVar),
            git_source=_from_dict(d, "git_source", GitSource),
            mode=_enum(d, "mode", AppDeploymentMode),
            source_code_path=d.get("source_code_path", None),
            status=_from_dict(d, "status", AppDeploymentStatus),
            update_time=d.get("update_time", None),
        )


@dataclass
class AppDeploymentArtifacts:
    source_code_path: Optional[str] = None
    """The snapshotted workspace file system path of the source code loaded by the deployed app."""

    def as_dict(self) -> dict:
        """Serializes the AppDeploymentArtifacts into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.source_code_path is not None:
            body["source_code_path"] = self.source_code_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppDeploymentArtifacts into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.source_code_path is not None:
            body["source_code_path"] = self.source_code_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppDeploymentArtifacts:
        """Deserializes the AppDeploymentArtifacts from a dictionary."""
        return cls(source_code_path=d.get("source_code_path", None))


class AppDeploymentMode(Enum):

    AUTO_SYNC = "AUTO_SYNC"
    SNAPSHOT = "SNAPSHOT"


class AppDeploymentState(Enum):

    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"


@dataclass
class AppDeploymentStatus:
    message: Optional[str] = None
    """Message corresponding with the deployment state."""

    state: Optional[AppDeploymentState] = None
    """State of the deployment."""

    def as_dict(self) -> dict:
        """Serializes the AppDeploymentStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppDeploymentStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppDeploymentStatus:
        """Deserializes the AppDeploymentStatus from a dictionary."""
        return cls(message=d.get("message", None), state=_enum(d, "state", AppDeploymentState))


@dataclass
class AppManifest:
    """App manifest definition"""

    version: int
    """The manifest schema version, for now only 1 is allowed"""

    name: str
    """Name of the app defined by manifest author / publisher"""

    description: Optional[str] = None
    """Description of the app defined by manifest author / publisher"""

    resource_specs: Optional[List[AppManifestAppResourceSpec]] = None

    def as_dict(self) -> dict:
        """Serializes the AppManifest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.name is not None:
            body["name"] = self.name
        if self.resource_specs:
            body["resource_specs"] = [v.as_dict() for v in self.resource_specs]
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.name is not None:
            body["name"] = self.name
        if self.resource_specs:
            body["resource_specs"] = self.resource_specs
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifest:
        """Deserializes the AppManifest from a dictionary."""
        return cls(
            description=d.get("description", None),
            name=d.get("name", None),
            resource_specs=_repeated_dict(d, "resource_specs", AppManifestAppResourceSpec),
            version=d.get("version", None),
        )


@dataclass
class AppManifestAppResourceExperimentSpec:
    permission: AppManifestAppResourceExperimentSpecExperimentPermission

    def as_dict(self) -> dict:
        """Serializes the AppManifestAppResourceExperimentSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifestAppResourceExperimentSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifestAppResourceExperimentSpec:
        """Deserializes the AppManifestAppResourceExperimentSpec from a dictionary."""
        return cls(permission=_enum(d, "permission", AppManifestAppResourceExperimentSpecExperimentPermission))


class AppManifestAppResourceExperimentSpecExperimentPermission(Enum):

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_READ = "CAN_READ"


@dataclass
class AppManifestAppResourceJobSpec:
    permission: AppManifestAppResourceJobSpecJobPermission
    """Permissions to grant on the Job. Supported permissions are: "CAN_MANAGE", "IS_OWNER",
    "CAN_MANAGE_RUN", "CAN_VIEW"."""

    def as_dict(self) -> dict:
        """Serializes the AppManifestAppResourceJobSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifestAppResourceJobSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifestAppResourceJobSpec:
        """Deserializes the AppManifestAppResourceJobSpec from a dictionary."""
        return cls(permission=_enum(d, "permission", AppManifestAppResourceJobSpecJobPermission))


class AppManifestAppResourceJobSpecJobPermission(Enum):

    CAN_MANAGE = "CAN_MANAGE"
    CAN_MANAGE_RUN = "CAN_MANAGE_RUN"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"


@dataclass
class AppManifestAppResourceSecretSpec:
    permission: AppManifestAppResourceSecretSpecSecretPermission
    """Permission to grant on the secret scope. For secrets, only one permission is allowed. Permission
    must be one of: "READ", "WRITE", "MANAGE"."""

    def as_dict(self) -> dict:
        """Serializes the AppManifestAppResourceSecretSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifestAppResourceSecretSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifestAppResourceSecretSpec:
        """Deserializes the AppManifestAppResourceSecretSpec from a dictionary."""
        return cls(permission=_enum(d, "permission", AppManifestAppResourceSecretSpecSecretPermission))


class AppManifestAppResourceSecretSpecSecretPermission(Enum):
    """Permission to grant on the secret scope. Supported permissions are: "READ", "WRITE", "MANAGE"."""

    MANAGE = "MANAGE"
    READ = "READ"
    WRITE = "WRITE"


@dataclass
class AppManifestAppResourceServingEndpointSpec:
    permission: AppManifestAppResourceServingEndpointSpecServingEndpointPermission
    """Permission to grant on the serving endpoint. Supported permissions are: "CAN_MANAGE",
    "CAN_QUERY", "CAN_VIEW"."""

    def as_dict(self) -> dict:
        """Serializes the AppManifestAppResourceServingEndpointSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifestAppResourceServingEndpointSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifestAppResourceServingEndpointSpec:
        """Deserializes the AppManifestAppResourceServingEndpointSpec from a dictionary."""
        return cls(
            permission=_enum(d, "permission", AppManifestAppResourceServingEndpointSpecServingEndpointPermission)
        )


class AppManifestAppResourceServingEndpointSpecServingEndpointPermission(Enum):

    CAN_MANAGE = "CAN_MANAGE"
    CAN_QUERY = "CAN_QUERY"
    CAN_VIEW = "CAN_VIEW"


@dataclass
class AppManifestAppResourceSpec:
    """AppResource related fields are copied from app.proto but excludes resource identifiers (e.g.
    name, id, key, scope, etc.)"""

    name: str
    """Name of the App Resource."""

    description: Optional[str] = None
    """Description of the App Resource."""

    experiment_spec: Optional[AppManifestAppResourceExperimentSpec] = None

    job_spec: Optional[AppManifestAppResourceJobSpec] = None

    secret_spec: Optional[AppManifestAppResourceSecretSpec] = None

    serving_endpoint_spec: Optional[AppManifestAppResourceServingEndpointSpec] = None

    sql_warehouse_spec: Optional[AppManifestAppResourceSqlWarehouseSpec] = None

    uc_securable_spec: Optional[AppManifestAppResourceUcSecurableSpec] = None

    def as_dict(self) -> dict:
        """Serializes the AppManifestAppResourceSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.experiment_spec:
            body["experiment_spec"] = self.experiment_spec.as_dict()
        if self.job_spec:
            body["job_spec"] = self.job_spec.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.secret_spec:
            body["secret_spec"] = self.secret_spec.as_dict()
        if self.serving_endpoint_spec:
            body["serving_endpoint_spec"] = self.serving_endpoint_spec.as_dict()
        if self.sql_warehouse_spec:
            body["sql_warehouse_spec"] = self.sql_warehouse_spec.as_dict()
        if self.uc_securable_spec:
            body["uc_securable_spec"] = self.uc_securable_spec.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifestAppResourceSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.experiment_spec:
            body["experiment_spec"] = self.experiment_spec
        if self.job_spec:
            body["job_spec"] = self.job_spec
        if self.name is not None:
            body["name"] = self.name
        if self.secret_spec:
            body["secret_spec"] = self.secret_spec
        if self.serving_endpoint_spec:
            body["serving_endpoint_spec"] = self.serving_endpoint_spec
        if self.sql_warehouse_spec:
            body["sql_warehouse_spec"] = self.sql_warehouse_spec
        if self.uc_securable_spec:
            body["uc_securable_spec"] = self.uc_securable_spec
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifestAppResourceSpec:
        """Deserializes the AppManifestAppResourceSpec from a dictionary."""
        return cls(
            description=d.get("description", None),
            experiment_spec=_from_dict(d, "experiment_spec", AppManifestAppResourceExperimentSpec),
            job_spec=_from_dict(d, "job_spec", AppManifestAppResourceJobSpec),
            name=d.get("name", None),
            secret_spec=_from_dict(d, "secret_spec", AppManifestAppResourceSecretSpec),
            serving_endpoint_spec=_from_dict(d, "serving_endpoint_spec", AppManifestAppResourceServingEndpointSpec),
            sql_warehouse_spec=_from_dict(d, "sql_warehouse_spec", AppManifestAppResourceSqlWarehouseSpec),
            uc_securable_spec=_from_dict(d, "uc_securable_spec", AppManifestAppResourceUcSecurableSpec),
        )


@dataclass
class AppManifestAppResourceSqlWarehouseSpec:
    permission: AppManifestAppResourceSqlWarehouseSpecSqlWarehousePermission
    """Permission to grant on the SQL warehouse. Supported permissions are: "CAN_MANAGE", "CAN_USE",
    "IS_OWNER"."""

    def as_dict(self) -> dict:
        """Serializes the AppManifestAppResourceSqlWarehouseSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifestAppResourceSqlWarehouseSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifestAppResourceSqlWarehouseSpec:
        """Deserializes the AppManifestAppResourceSqlWarehouseSpec from a dictionary."""
        return cls(permission=_enum(d, "permission", AppManifestAppResourceSqlWarehouseSpecSqlWarehousePermission))


class AppManifestAppResourceSqlWarehouseSpecSqlWarehousePermission(Enum):

    CAN_MANAGE = "CAN_MANAGE"
    CAN_USE = "CAN_USE"
    IS_OWNER = "IS_OWNER"


@dataclass
class AppManifestAppResourceUcSecurableSpec:
    securable_type: AppManifestAppResourceUcSecurableSpecUcSecurableType

    permission: AppManifestAppResourceUcSecurableSpecUcSecurablePermission

    def as_dict(self) -> dict:
        """Serializes the AppManifestAppResourceUcSecurableSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppManifestAppResourceUcSecurableSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppManifestAppResourceUcSecurableSpec:
        """Deserializes the AppManifestAppResourceUcSecurableSpec from a dictionary."""
        return cls(
            permission=_enum(d, "permission", AppManifestAppResourceUcSecurableSpecUcSecurablePermission),
            securable_type=_enum(d, "securable_type", AppManifestAppResourceUcSecurableSpecUcSecurableType),
        )


class AppManifestAppResourceUcSecurableSpecUcSecurablePermission(Enum):

    EXECUTE = "EXECUTE"
    MANAGE = "MANAGE"
    READ_VOLUME = "READ_VOLUME"
    SELECT = "SELECT"
    USE_CONNECTION = "USE_CONNECTION"
    WRITE_VOLUME = "WRITE_VOLUME"


class AppManifestAppResourceUcSecurableSpecUcSecurableType(Enum):

    CONNECTION = "CONNECTION"
    FUNCTION = "FUNCTION"
    TABLE = "TABLE"
    VOLUME = "VOLUME"


@dataclass
class AppPermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[AppPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the AppPermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppPermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppPermission:
        """Deserializes the AppPermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", AppPermissionLevel),
        )


class AppPermissionLevel(Enum):
    """Permission level"""

    CAN_MANAGE = "CAN_MANAGE"
    CAN_USE = "CAN_USE"


@dataclass
class AppPermissions:
    access_control_list: Optional[List[AppAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the AppPermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppPermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppPermissions:
        """Deserializes the AppPermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", AppAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class AppPermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[AppPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the AppPermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppPermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppPermissionsDescription:
        """Deserializes the AppPermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None), permission_level=_enum(d, "permission_level", AppPermissionLevel)
        )


@dataclass
class AppResource:
    name: str
    """Name of the App Resource."""

    app: Optional[AppResourceApp] = None

    database: Optional[AppResourceDatabase] = None

    description: Optional[str] = None
    """Description of the App Resource."""

    experiment: Optional[AppResourceExperiment] = None

    genie_space: Optional[AppResourceGenieSpace] = None

    job: Optional[AppResourceJob] = None

    secret: Optional[AppResourceSecret] = None

    serving_endpoint: Optional[AppResourceServingEndpoint] = None

    sql_warehouse: Optional[AppResourceSqlWarehouse] = None

    uc_securable: Optional[AppResourceUcSecurable] = None

    def as_dict(self) -> dict:
        """Serializes the AppResource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.app:
            body["app"] = self.app.as_dict()
        if self.database:
            body["database"] = self.database.as_dict()
        if self.description is not None:
            body["description"] = self.description
        if self.experiment:
            body["experiment"] = self.experiment.as_dict()
        if self.genie_space:
            body["genie_space"] = self.genie_space.as_dict()
        if self.job:
            body["job"] = self.job.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.secret:
            body["secret"] = self.secret.as_dict()
        if self.serving_endpoint:
            body["serving_endpoint"] = self.serving_endpoint.as_dict()
        if self.sql_warehouse:
            body["sql_warehouse"] = self.sql_warehouse.as_dict()
        if self.uc_securable:
            body["uc_securable"] = self.uc_securable.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.app:
            body["app"] = self.app
        if self.database:
            body["database"] = self.database
        if self.description is not None:
            body["description"] = self.description
        if self.experiment:
            body["experiment"] = self.experiment
        if self.genie_space:
            body["genie_space"] = self.genie_space
        if self.job:
            body["job"] = self.job
        if self.name is not None:
            body["name"] = self.name
        if self.secret:
            body["secret"] = self.secret
        if self.serving_endpoint:
            body["serving_endpoint"] = self.serving_endpoint
        if self.sql_warehouse:
            body["sql_warehouse"] = self.sql_warehouse
        if self.uc_securable:
            body["uc_securable"] = self.uc_securable
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResource:
        """Deserializes the AppResource from a dictionary."""
        return cls(
            app=_from_dict(d, "app", AppResourceApp),
            database=_from_dict(d, "database", AppResourceDatabase),
            description=d.get("description", None),
            experiment=_from_dict(d, "experiment", AppResourceExperiment),
            genie_space=_from_dict(d, "genie_space", AppResourceGenieSpace),
            job=_from_dict(d, "job", AppResourceJob),
            name=d.get("name", None),
            secret=_from_dict(d, "secret", AppResourceSecret),
            serving_endpoint=_from_dict(d, "serving_endpoint", AppResourceServingEndpoint),
            sql_warehouse=_from_dict(d, "sql_warehouse", AppResourceSqlWarehouse),
            uc_securable=_from_dict(d, "uc_securable", AppResourceUcSecurable),
        )


@dataclass
class AppResourceApp:
    def as_dict(self) -> dict:
        """Serializes the AppResourceApp into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceApp into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceApp:
        """Deserializes the AppResourceApp from a dictionary."""
        return cls()


@dataclass
class AppResourceDatabase:
    instance_name: str

    database_name: str

    permission: AppResourceDatabaseDatabasePermission

    def as_dict(self) -> dict:
        """Serializes the AppResourceDatabase into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.database_name is not None:
            body["database_name"] = self.database_name
        if self.instance_name is not None:
            body["instance_name"] = self.instance_name
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceDatabase into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.database_name is not None:
            body["database_name"] = self.database_name
        if self.instance_name is not None:
            body["instance_name"] = self.instance_name
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceDatabase:
        """Deserializes the AppResourceDatabase from a dictionary."""
        return cls(
            database_name=d.get("database_name", None),
            instance_name=d.get("instance_name", None),
            permission=_enum(d, "permission", AppResourceDatabaseDatabasePermission),
        )


class AppResourceDatabaseDatabasePermission(Enum):

    CAN_CONNECT_AND_CREATE = "CAN_CONNECT_AND_CREATE"


@dataclass
class AppResourceExperiment:
    experiment_id: str

    permission: AppResourceExperimentExperimentPermission

    def as_dict(self) -> dict:
        """Serializes the AppResourceExperiment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceExperiment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceExperiment:
        """Deserializes the AppResourceExperiment from a dictionary."""
        return cls(
            experiment_id=d.get("experiment_id", None),
            permission=_enum(d, "permission", AppResourceExperimentExperimentPermission),
        )


class AppResourceExperimentExperimentPermission(Enum):

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_READ = "CAN_READ"


@dataclass
class AppResourceGenieSpace:
    name: str

    space_id: str

    permission: AppResourceGenieSpaceGenieSpacePermission

    def as_dict(self) -> dict:
        """Serializes the AppResourceGenieSpace into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.permission is not None:
            body["permission"] = self.permission.value
        if self.space_id is not None:
            body["space_id"] = self.space_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceGenieSpace into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.permission is not None:
            body["permission"] = self.permission
        if self.space_id is not None:
            body["space_id"] = self.space_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceGenieSpace:
        """Deserializes the AppResourceGenieSpace from a dictionary."""
        return cls(
            name=d.get("name", None),
            permission=_enum(d, "permission", AppResourceGenieSpaceGenieSpacePermission),
            space_id=d.get("space_id", None),
        )


class AppResourceGenieSpaceGenieSpacePermission(Enum):

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_RUN = "CAN_RUN"
    CAN_VIEW = "CAN_VIEW"


@dataclass
class AppResourceJob:
    id: str
    """Id of the job to grant permission on."""

    permission: AppResourceJobJobPermission
    """Permissions to grant on the Job. Supported permissions are: "CAN_MANAGE", "IS_OWNER",
    "CAN_MANAGE_RUN", "CAN_VIEW"."""

    def as_dict(self) -> dict:
        """Serializes the AppResourceJob into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceJob into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceJob:
        """Deserializes the AppResourceJob from a dictionary."""
        return cls(id=d.get("id", None), permission=_enum(d, "permission", AppResourceJobJobPermission))


class AppResourceJobJobPermission(Enum):

    CAN_MANAGE = "CAN_MANAGE"
    CAN_MANAGE_RUN = "CAN_MANAGE_RUN"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"


@dataclass
class AppResourceSecret:
    scope: str
    """Scope of the secret to grant permission on."""

    key: str
    """Key of the secret to grant permission on."""

    permission: AppResourceSecretSecretPermission
    """Permission to grant on the secret scope. For secrets, only one permission is allowed. Permission
    must be one of: "READ", "WRITE", "MANAGE"."""

    def as_dict(self) -> dict:
        """Serializes the AppResourceSecret into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.permission is not None:
            body["permission"] = self.permission.value
        if self.scope is not None:
            body["scope"] = self.scope
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceSecret into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.permission is not None:
            body["permission"] = self.permission
        if self.scope is not None:
            body["scope"] = self.scope
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceSecret:
        """Deserializes the AppResourceSecret from a dictionary."""
        return cls(
            key=d.get("key", None),
            permission=_enum(d, "permission", AppResourceSecretSecretPermission),
            scope=d.get("scope", None),
        )


class AppResourceSecretSecretPermission(Enum):
    """Permission to grant on the secret scope. Supported permissions are: "READ", "WRITE", "MANAGE"."""

    MANAGE = "MANAGE"
    READ = "READ"
    WRITE = "WRITE"


@dataclass
class AppResourceServingEndpoint:
    name: str
    """Name of the serving endpoint to grant permission on."""

    permission: AppResourceServingEndpointServingEndpointPermission
    """Permission to grant on the serving endpoint. Supported permissions are: "CAN_MANAGE",
    "CAN_QUERY", "CAN_VIEW"."""

    def as_dict(self) -> dict:
        """Serializes the AppResourceServingEndpoint into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceServingEndpoint into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceServingEndpoint:
        """Deserializes the AppResourceServingEndpoint from a dictionary."""
        return cls(
            name=d.get("name", None),
            permission=_enum(d, "permission", AppResourceServingEndpointServingEndpointPermission),
        )


class AppResourceServingEndpointServingEndpointPermission(Enum):

    CAN_MANAGE = "CAN_MANAGE"
    CAN_QUERY = "CAN_QUERY"
    CAN_VIEW = "CAN_VIEW"


@dataclass
class AppResourceSqlWarehouse:
    id: str
    """Id of the SQL warehouse to grant permission on."""

    permission: AppResourceSqlWarehouseSqlWarehousePermission
    """Permission to grant on the SQL warehouse. Supported permissions are: "CAN_MANAGE", "CAN_USE",
    "IS_OWNER"."""

    def as_dict(self) -> dict:
        """Serializes the AppResourceSqlWarehouse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.permission is not None:
            body["permission"] = self.permission.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceSqlWarehouse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.permission is not None:
            body["permission"] = self.permission
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceSqlWarehouse:
        """Deserializes the AppResourceSqlWarehouse from a dictionary."""
        return cls(
            id=d.get("id", None), permission=_enum(d, "permission", AppResourceSqlWarehouseSqlWarehousePermission)
        )


class AppResourceSqlWarehouseSqlWarehousePermission(Enum):

    CAN_MANAGE = "CAN_MANAGE"
    CAN_USE = "CAN_USE"
    IS_OWNER = "IS_OWNER"


@dataclass
class AppResourceUcSecurable:
    securable_full_name: str

    securable_type: AppResourceUcSecurableUcSecurableType

    permission: AppResourceUcSecurableUcSecurablePermission

    securable_kind: Optional[str] = None
    """The securable kind from Unity Catalog. See
    https://docs.databricks.com/api/workspace/tables/get#securable_kind_manifest-securable_kind."""

    def as_dict(self) -> dict:
        """Serializes the AppResourceUcSecurable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission.value
        if self.securable_full_name is not None:
            body["securable_full_name"] = self.securable_full_name
        if self.securable_kind is not None:
            body["securable_kind"] = self.securable_kind
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppResourceUcSecurable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission is not None:
            body["permission"] = self.permission
        if self.securable_full_name is not None:
            body["securable_full_name"] = self.securable_full_name
        if self.securable_kind is not None:
            body["securable_kind"] = self.securable_kind
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppResourceUcSecurable:
        """Deserializes the AppResourceUcSecurable from a dictionary."""
        return cls(
            permission=_enum(d, "permission", AppResourceUcSecurableUcSecurablePermission),
            securable_full_name=d.get("securable_full_name", None),
            securable_kind=d.get("securable_kind", None),
            securable_type=_enum(d, "securable_type", AppResourceUcSecurableUcSecurableType),
        )


class AppResourceUcSecurableUcSecurablePermission(Enum):

    EXECUTE = "EXECUTE"
    MODIFY = "MODIFY"
    READ_VOLUME = "READ_VOLUME"
    SELECT = "SELECT"
    USE_CONNECTION = "USE_CONNECTION"
    WRITE_VOLUME = "WRITE_VOLUME"


class AppResourceUcSecurableUcSecurableType(Enum):

    CONNECTION = "CONNECTION"
    FUNCTION = "FUNCTION"
    TABLE = "TABLE"
    VOLUME = "VOLUME"


@dataclass
class AppUpdate:
    budget_policy_id: Optional[str] = None

    compute_size: Optional[ComputeSize] = None

    description: Optional[str] = None

    git_repository: Optional[GitRepository] = None

    resources: Optional[List[AppResource]] = None

    status: Optional[AppUpdateUpdateStatus] = None

    usage_policy_id: Optional[str] = None

    user_api_scopes: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the AppUpdate into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.compute_size is not None:
            body["compute_size"] = self.compute_size.value
        if self.description is not None:
            body["description"] = self.description
        if self.git_repository:
            body["git_repository"] = self.git_repository.as_dict()
        if self.resources:
            body["resources"] = [v.as_dict() for v in self.resources]
        if self.status:
            body["status"] = self.status.as_dict()
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = [v for v in self.user_api_scopes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppUpdate into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.compute_size is not None:
            body["compute_size"] = self.compute_size
        if self.description is not None:
            body["description"] = self.description
        if self.git_repository:
            body["git_repository"] = self.git_repository
        if self.resources:
            body["resources"] = self.resources
        if self.status:
            body["status"] = self.status
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = self.user_api_scopes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppUpdate:
        """Deserializes the AppUpdate from a dictionary."""
        return cls(
            budget_policy_id=d.get("budget_policy_id", None),
            compute_size=_enum(d, "compute_size", ComputeSize),
            description=d.get("description", None),
            git_repository=_from_dict(d, "git_repository", GitRepository),
            resources=_repeated_dict(d, "resources", AppResource),
            status=_from_dict(d, "status", AppUpdateUpdateStatus),
            usage_policy_id=d.get("usage_policy_id", None),
            user_api_scopes=d.get("user_api_scopes", None),
        )


@dataclass
class AppUpdateUpdateStatus:
    message: Optional[str] = None

    state: Optional[AppUpdateUpdateStatusUpdateState] = None

    def as_dict(self) -> dict:
        """Serializes the AppUpdateUpdateStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AppUpdateUpdateStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AppUpdateUpdateStatus:
        """Deserializes the AppUpdateUpdateStatus from a dictionary."""
        return cls(message=d.get("message", None), state=_enum(d, "state", AppUpdateUpdateStatusUpdateState))


class AppUpdateUpdateStatusUpdateState(Enum):

    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"
    NOT_UPDATED = "NOT_UPDATED"
    SUCCEEDED = "SUCCEEDED"


class ApplicationState(Enum):

    CRASHED = "CRASHED"
    DEPLOYING = "DEPLOYING"
    RUNNING = "RUNNING"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass
class ApplicationStatus:
    message: Optional[str] = None
    """Application status message"""

    state: Optional[ApplicationState] = None
    """State of the application."""

    def as_dict(self) -> dict:
        """Serializes the ApplicationStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ApplicationStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ApplicationStatus:
        """Deserializes the ApplicationStatus from a dictionary."""
        return cls(message=d.get("message", None), state=_enum(d, "state", ApplicationState))


class ComputeSize(Enum):

    LARGE = "LARGE"
    MEDIUM = "MEDIUM"


class ComputeState(Enum):

    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    ERROR = "ERROR"
    STARTING = "STARTING"
    STOPPED = "STOPPED"
    STOPPING = "STOPPING"
    UPDATING = "UPDATING"


@dataclass
class ComputeStatus:
    active_instances: Optional[int] = None
    """The number of compute instances currently serving requests for this application. An instance is
    considered active if it is reachable and ready to handle requests."""

    message: Optional[str] = None
    """Compute status message"""

    state: Optional[ComputeState] = None
    """State of the app compute."""

    def as_dict(self) -> dict:
        """Serializes the ComputeStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.active_instances is not None:
            body["active_instances"] = self.active_instances
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ComputeStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.active_instances is not None:
            body["active_instances"] = self.active_instances
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ComputeStatus:
        """Deserializes the ComputeStatus from a dictionary."""
        return cls(
            active_instances=d.get("active_instances", None),
            message=d.get("message", None),
            state=_enum(d, "state", ComputeState),
        )


@dataclass
class CustomTemplate:
    name: str
    """The name of the template. It must contain only alphanumeric characters, hyphens, underscores,
    and whitespaces. It must be unique within the workspace."""

    git_repo: str
    """The Git repository URL that the template resides in."""

    path: str
    """The path to the template within the Git repository."""

    manifest: AppManifest
    """The manifest of the template. It defines fields and default values when installing the template."""

    git_provider: str
    """The Git provider of the template."""

    creator: Optional[str] = None

    description: Optional[str] = None
    """The description of the template."""

    def as_dict(self) -> dict:
        """Serializes the CustomTemplate into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creator is not None:
            body["creator"] = self.creator
        if self.description is not None:
            body["description"] = self.description
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_repo is not None:
            body["git_repo"] = self.git_repo
        if self.manifest:
            body["manifest"] = self.manifest.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.path is not None:
            body["path"] = self.path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CustomTemplate into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creator is not None:
            body["creator"] = self.creator
        if self.description is not None:
            body["description"] = self.description
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_repo is not None:
            body["git_repo"] = self.git_repo
        if self.manifest:
            body["manifest"] = self.manifest
        if self.name is not None:
            body["name"] = self.name
        if self.path is not None:
            body["path"] = self.path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CustomTemplate:
        """Deserializes the CustomTemplate from a dictionary."""
        return cls(
            creator=d.get("creator", None),
            description=d.get("description", None),
            git_provider=d.get("git_provider", None),
            git_repo=d.get("git_repo", None),
            manifest=_from_dict(d, "manifest", AppManifest),
            name=d.get("name", None),
            path=d.get("path", None),
        )


@dataclass
class DatabricksServiceExceptionWithDetailsProto:
    """Databricks Error that is returned by all Databricks APIs."""

    details: Optional[List[dict]] = None

    error_code: Optional[ErrorCode] = None

    message: Optional[str] = None

    stack_trace: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the DatabricksServiceExceptionWithDetailsProto into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.details:
            body["details"] = [v for v in self.details]
        if self.error_code is not None:
            body["error_code"] = self.error_code.value
        if self.message is not None:
            body["message"] = self.message
        if self.stack_trace is not None:
            body["stack_trace"] = self.stack_trace
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabricksServiceExceptionWithDetailsProto into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.details:
            body["details"] = self.details
        if self.error_code is not None:
            body["error_code"] = self.error_code
        if self.message is not None:
            body["message"] = self.message
        if self.stack_trace is not None:
            body["stack_trace"] = self.stack_trace
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabricksServiceExceptionWithDetailsProto:
        """Deserializes the DatabricksServiceExceptionWithDetailsProto from a dictionary."""
        return cls(
            details=d.get("details", None),
            error_code=_enum(d, "error_code", ErrorCode),
            message=d.get("message", None),
            stack_trace=d.get("stack_trace", None),
        )


@dataclass
class EnvVar:
    name: Optional[str] = None
    """The name of the environment variable."""

    value: Optional[str] = None
    """The value for the environment variable."""

    value_from: Optional[str] = None
    """The name of an external Databricks resource that contains the value, such as a secret or a
    database table."""

    def as_dict(self) -> dict:
        """Serializes the EnvVar into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.value is not None:
            body["value"] = self.value
        if self.value_from is not None:
            body["value_from"] = self.value_from
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnvVar into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.value is not None:
            body["value"] = self.value
        if self.value_from is not None:
            body["value_from"] = self.value_from
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnvVar:
        """Deserializes the EnvVar from a dictionary."""
        return cls(name=d.get("name", None), value=d.get("value", None), value_from=d.get("value_from", None))


class ErrorCode(Enum):
    """Error codes returned by Databricks APIs to indicate specific failure conditions."""

    ABORTED = "ABORTED"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    BAD_REQUEST = "BAD_REQUEST"
    CANCELLED = "CANCELLED"
    CATALOG_ALREADY_EXISTS = "CATALOG_ALREADY_EXISTS"
    CATALOG_DOES_NOT_EXIST = "CATALOG_DOES_NOT_EXIST"
    CATALOG_NOT_EMPTY = "CATALOG_NOT_EMPTY"
    COULD_NOT_ACQUIRE_LOCK = "COULD_NOT_ACQUIRE_LOCK"
    CUSTOMER_UNAUTHORIZED = "CUSTOMER_UNAUTHORIZED"
    DAC_ALREADY_EXISTS = "DAC_ALREADY_EXISTS"
    DAC_DOES_NOT_EXIST = "DAC_DOES_NOT_EXIST"
    DATA_LOSS = "DATA_LOSS"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    DEPLOYMENT_TIMEOUT = "DEPLOYMENT_TIMEOUT"
    DIRECTORY_NOT_EMPTY = "DIRECTORY_NOT_EMPTY"
    DIRECTORY_PROTECTED = "DIRECTORY_PROTECTED"
    DRY_RUN_FAILED = "DRY_RUN_FAILED"
    ENDPOINT_NOT_FOUND = "ENDPOINT_NOT_FOUND"
    EXTERNAL_LOCATION_ALREADY_EXISTS = "EXTERNAL_LOCATION_ALREADY_EXISTS"
    EXTERNAL_LOCATION_DOES_NOT_EXIST = "EXTERNAL_LOCATION_DOES_NOT_EXIST"
    FEATURE_DISABLED = "FEATURE_DISABLED"
    GIT_CONFLICT = "GIT_CONFLICT"
    GIT_REMOTE_ERROR = "GIT_REMOTE_ERROR"
    GIT_SENSITIVE_TOKEN_DETECTED = "GIT_SENSITIVE_TOKEN_DETECTED"
    GIT_UNKNOWN_REF = "GIT_UNKNOWN_REF"
    GIT_URL_NOT_ON_ALLOW_LIST = "GIT_URL_NOT_ON_ALLOW_LIST"
    INSECURE_PARTNER_RESPONSE = "INSECURE_PARTNER_RESPONSE"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    INVALID_PARAMETER_VALUE = "INVALID_PARAMETER_VALUE"
    INVALID_STATE = "INVALID_STATE"
    INVALID_STATE_TRANSITION = "INVALID_STATE_TRANSITION"
    IO_ERROR = "IO_ERROR"
    IPYNB_FILE_IN_REPO = "IPYNB_FILE_IN_REPO"
    MALFORMED_PARTNER_RESPONSE = "MALFORMED_PARTNER_RESPONSE"
    MALFORMED_REQUEST = "MALFORMED_REQUEST"
    MANAGED_RESOURCE_GROUP_DOES_NOT_EXIST = "MANAGED_RESOURCE_GROUP_DOES_NOT_EXIST"
    MAX_BLOCK_SIZE_EXCEEDED = "MAX_BLOCK_SIZE_EXCEEDED"
    MAX_CHILD_NODE_SIZE_EXCEEDED = "MAX_CHILD_NODE_SIZE_EXCEEDED"
    MAX_LIST_SIZE_EXCEEDED = "MAX_LIST_SIZE_EXCEEDED"
    MAX_NOTEBOOK_SIZE_EXCEEDED = "MAX_NOTEBOOK_SIZE_EXCEEDED"
    MAX_READ_SIZE_EXCEEDED = "MAX_READ_SIZE_EXCEEDED"
    METASTORE_ALREADY_EXISTS = "METASTORE_ALREADY_EXISTS"
    METASTORE_DOES_NOT_EXIST = "METASTORE_DOES_NOT_EXIST"
    METASTORE_NOT_EMPTY = "METASTORE_NOT_EMPTY"
    NOT_FOUND = "NOT_FOUND"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    PARTIAL_DELETE = "PARTIAL_DELETE"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    PERMISSION_NOT_PROPAGATED = "PERMISSION_NOT_PROPAGATED"
    PRINCIPAL_DOES_NOT_EXIST = "PRINCIPAL_DOES_NOT_EXIST"
    PROJECTS_OPERATION_TIMEOUT = "PROJECTS_OPERATION_TIMEOUT"
    PROVIDER_ALREADY_EXISTS = "PROVIDER_ALREADY_EXISTS"
    PROVIDER_DOES_NOT_EXIST = "PROVIDER_DOES_NOT_EXIST"
    PROVIDER_SHARE_NOT_ACCESSIBLE = "PROVIDER_SHARE_NOT_ACCESSIBLE"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    RECIPIENT_ALREADY_EXISTS = "RECIPIENT_ALREADY_EXISTS"
    RECIPIENT_DOES_NOT_EXIST = "RECIPIENT_DOES_NOT_EXIST"
    REQUEST_LIMIT_EXCEEDED = "REQUEST_LIMIT_EXCEEDED"
    RESOURCE_ALREADY_EXISTS = "RESOURCE_ALREADY_EXISTS"
    RESOURCE_CONFLICT = "RESOURCE_CONFLICT"
    RESOURCE_DOES_NOT_EXIST = "RESOURCE_DOES_NOT_EXIST"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    RESOURCE_LIMIT_EXCEEDED = "RESOURCE_LIMIT_EXCEEDED"
    SCHEMA_ALREADY_EXISTS = "SCHEMA_ALREADY_EXISTS"
    SCHEMA_DOES_NOT_EXIST = "SCHEMA_DOES_NOT_EXIST"
    SCHEMA_NOT_EMPTY = "SCHEMA_NOT_EMPTY"
    SEARCH_QUERY_TOO_LONG = "SEARCH_QUERY_TOO_LONG"
    SEARCH_QUERY_TOO_SHORT = "SEARCH_QUERY_TOO_SHORT"
    SERVICE_UNDER_MAINTENANCE = "SERVICE_UNDER_MAINTENANCE"
    SHARE_ALREADY_EXISTS = "SHARE_ALREADY_EXISTS"
    SHARE_DOES_NOT_EXIST = "SHARE_DOES_NOT_EXIST"
    STORAGE_CREDENTIAL_ALREADY_EXISTS = "STORAGE_CREDENTIAL_ALREADY_EXISTS"
    STORAGE_CREDENTIAL_DOES_NOT_EXIST = "STORAGE_CREDENTIAL_DOES_NOT_EXIST"
    TABLE_ALREADY_EXISTS = "TABLE_ALREADY_EXISTS"
    TABLE_DOES_NOT_EXIST = "TABLE_DOES_NOT_EXIST"
    TEMPORARILY_UNAVAILABLE = "TEMPORARILY_UNAVAILABLE"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    UNAVAILABLE = "UNAVAILABLE"
    UNKNOWN = "UNKNOWN"
    UNPARSEABLE_HTTP_ERROR = "UNPARSEABLE_HTTP_ERROR"
    WORKSPACE_TEMPORARILY_UNAVAILABLE = "WORKSPACE_TEMPORARILY_UNAVAILABLE"


@dataclass
class GetAppPermissionLevelsResponse:
    permission_levels: Optional[List[AppPermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetAppPermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetAppPermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetAppPermissionLevelsResponse:
        """Deserializes the GetAppPermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", AppPermissionsDescription))


@dataclass
class GitRepository:
    """Git repository configuration specifying the location of the repository."""

    url: str
    """URL of the Git repository."""

    provider: str
    """Git provider. Case insensitive. Supported values: gitHub, gitHubEnterprise, bitbucketCloud,
    bitbucketServer, azureDevOpsServices, gitLab, gitLabEnterpriseEdition, awsCodeCommit."""

    def as_dict(self) -> dict:
        """Serializes the GitRepository into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.provider is not None:
            body["provider"] = self.provider
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GitRepository into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.provider is not None:
            body["provider"] = self.provider
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GitRepository:
        """Deserializes the GitRepository from a dictionary."""
        return cls(provider=d.get("provider", None), url=d.get("url", None))


@dataclass
class GitSource:
    """Complete git source specification including repository location and reference."""

    branch: Optional[str] = None
    """Git branch to checkout."""

    commit: Optional[str] = None
    """Git commit SHA to checkout."""

    git_repository: Optional[GitRepository] = None
    """Git repository configuration. Populated from the app's git_repository configuration."""

    resolved_commit: Optional[str] = None
    """The resolved commit SHA that was actually used for the deployment. This is populated by the
    system after resolving the reference (branch, tag, or commit). If commit is specified directly,
    this will match commit. If a branch or tag is specified, this contains the commit SHA that the
    branch or tag pointed to at deployment time."""

    source_code_path: Optional[str] = None
    """Relative path to the app source code within the Git repository. If not specified, the root of
    the repository is used."""

    tag: Optional[str] = None
    """Git tag to checkout."""

    def as_dict(self) -> dict:
        """Serializes the GitSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.commit is not None:
            body["commit"] = self.commit
        if self.git_repository:
            body["git_repository"] = self.git_repository.as_dict()
        if self.resolved_commit is not None:
            body["resolved_commit"] = self.resolved_commit
        if self.source_code_path is not None:
            body["source_code_path"] = self.source_code_path
        if self.tag is not None:
            body["tag"] = self.tag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GitSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.branch is not None:
            body["branch"] = self.branch
        if self.commit is not None:
            body["commit"] = self.commit
        if self.git_repository:
            body["git_repository"] = self.git_repository
        if self.resolved_commit is not None:
            body["resolved_commit"] = self.resolved_commit
        if self.source_code_path is not None:
            body["source_code_path"] = self.source_code_path
        if self.tag is not None:
            body["tag"] = self.tag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GitSource:
        """Deserializes the GitSource from a dictionary."""
        return cls(
            branch=d.get("branch", None),
            commit=d.get("commit", None),
            git_repository=_from_dict(d, "git_repository", GitRepository),
            resolved_commit=d.get("resolved_commit", None),
            source_code_path=d.get("source_code_path", None),
            tag=d.get("tag", None),
        )


@dataclass
class ListAppDeploymentsResponse:
    app_deployments: Optional[List[AppDeployment]] = None
    """Deployment history of the app."""

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of apps."""

    def as_dict(self) -> dict:
        """Serializes the ListAppDeploymentsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.app_deployments:
            body["app_deployments"] = [v.as_dict() for v in self.app_deployments]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAppDeploymentsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.app_deployments:
            body["app_deployments"] = self.app_deployments
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAppDeploymentsResponse:
        """Deserializes the ListAppDeploymentsResponse from a dictionary."""
        return cls(
            app_deployments=_repeated_dict(d, "app_deployments", AppDeployment),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListAppsResponse:
    apps: Optional[List[App]] = None

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of apps."""

    def as_dict(self) -> dict:
        """Serializes the ListAppsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.apps:
            body["apps"] = [v.as_dict() for v in self.apps]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAppsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.apps:
            body["apps"] = self.apps
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAppsResponse:
        """Deserializes the ListAppsResponse from a dictionary."""
        return cls(apps=_repeated_dict(d, "apps", App), next_page_token=d.get("next_page_token", None))


@dataclass
class ListCustomTemplatesResponse:
    next_page_token: Optional[str] = None
    """Pagination token to request the next page of custom templates."""

    templates: Optional[List[CustomTemplate]] = None

    def as_dict(self) -> dict:
        """Serializes the ListCustomTemplatesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.templates:
            body["templates"] = [v.as_dict() for v in self.templates]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListCustomTemplatesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.templates:
            body["templates"] = self.templates
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListCustomTemplatesResponse:
        """Deserializes the ListCustomTemplatesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), templates=_repeated_dict(d, "templates", CustomTemplate)
        )


@dataclass
class ListSpacesResponse:
    next_page_token: Optional[str] = None
    """Pagination token to request the next page of app spaces."""

    spaces: Optional[List[Space]] = None

    def as_dict(self) -> dict:
        """Serializes the ListSpacesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.spaces:
            body["spaces"] = [v.as_dict() for v in self.spaces]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSpacesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.spaces:
            body["spaces"] = self.spaces
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSpacesResponse:
        """Deserializes the ListSpacesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), spaces=_repeated_dict(d, "spaces", Space))


@dataclass
class Operation:
    """This resource represents a long-running operation that is the result of a network API call."""

    done: Optional[bool] = None
    """If the value is `false`, it means the operation is still in progress. If `true`, the operation
    is completed, and either `error` or `response` is available."""

    error: Optional[DatabricksServiceExceptionWithDetailsProto] = None
    """The error result of the operation in case of failure or cancellation."""

    metadata: Optional[dict] = None
    """Service-specific metadata associated with the operation. It typically contains progress
    information and common metadata such as create time. Some services might not provide such
    metadata."""

    name: Optional[str] = None
    """The server-assigned name, which is only unique within the same service that originally returns
    it. If you use the default HTTP mapping, the `name` should be a resource name ending with
    `operations/{unique_id}`."""

    response: Optional[dict] = None
    """The normal, successful response of the operation."""

    def as_dict(self) -> dict:
        """Serializes the Operation into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.done is not None:
            body["done"] = self.done
        if self.error:
            body["error"] = self.error.as_dict()
        if self.metadata:
            body["metadata"] = self.metadata
        if self.name is not None:
            body["name"] = self.name
        if self.response:
            body["response"] = self.response
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Operation into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.done is not None:
            body["done"] = self.done
        if self.error:
            body["error"] = self.error
        if self.metadata:
            body["metadata"] = self.metadata
        if self.name is not None:
            body["name"] = self.name
        if self.response:
            body["response"] = self.response
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Operation:
        """Deserializes the Operation from a dictionary."""
        return cls(
            done=d.get("done", None),
            error=_from_dict(d, "error", DatabricksServiceExceptionWithDetailsProto),
            metadata=d.get("metadata", None),
            name=d.get("name", None),
            response=d.get("response", None),
        )


@dataclass
class Space:
    name: str
    """The name of the app space. The name must contain only lowercase alphanumeric characters and
    hyphens. It must be unique within the workspace."""

    create_time: Optional[Timestamp] = None
    """The creation time of the app space. Formatted timestamp in ISO 6801."""

    creator: Optional[str] = None
    """The email of the user that created the app space."""

    description: Optional[str] = None
    """The description of the app space."""

    effective_usage_policy_id: Optional[str] = None
    """The effective usage policy ID used by apps in the space."""

    effective_user_api_scopes: Optional[List[str]] = None
    """The effective api scopes granted to the user access token."""

    id: Optional[str] = None
    """The unique identifier of the app space."""

    oauth2_app_client_id: Optional[str] = None
    """The OAuth2 app client ID for the app space."""

    oauth2_app_integration_id: Optional[str] = None
    """The OAuth2 app integration ID for the app space."""

    resources: Optional[List[AppResource]] = None
    """Resources for the app space. Resources configured at the space level are available to all apps
    in the space."""

    service_principal_client_id: Optional[str] = None
    """The service principal client ID for the app space."""

    service_principal_id: Optional[int] = None
    """The service principal ID for the app space."""

    service_principal_name: Optional[str] = None
    """The service principal name for the app space."""

    status: Optional[SpaceStatus] = None
    """The status of the app space."""

    update_time: Optional[Timestamp] = None
    """The update time of the app space. Formatted timestamp in ISO 6801."""

    updater: Optional[str] = None
    """The email of the user that last updated the app space."""

    usage_policy_id: Optional[str] = None
    """The usage policy ID for managing cost at the space level."""

    user_api_scopes: Optional[List[str]] = None
    """OAuth scopes for apps in the space."""

    def as_dict(self) -> dict:
        """Serializes the Space into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time.ToJsonString()
        if self.creator is not None:
            body["creator"] = self.creator
        if self.description is not None:
            body["description"] = self.description
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.effective_user_api_scopes:
            body["effective_user_api_scopes"] = [v for v in self.effective_user_api_scopes]
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.oauth2_app_client_id is not None:
            body["oauth2_app_client_id"] = self.oauth2_app_client_id
        if self.oauth2_app_integration_id is not None:
            body["oauth2_app_integration_id"] = self.oauth2_app_integration_id
        if self.resources:
            body["resources"] = [v.as_dict() for v in self.resources]
        if self.service_principal_client_id is not None:
            body["service_principal_client_id"] = self.service_principal_client_id
        if self.service_principal_id is not None:
            body["service_principal_id"] = self.service_principal_id
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.status:
            body["status"] = self.status.as_dict()
        if self.update_time is not None:
            body["update_time"] = self.update_time.ToJsonString()
        if self.updater is not None:
            body["updater"] = self.updater
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = [v for v in self.user_api_scopes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Space into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.description is not None:
            body["description"] = self.description
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.effective_user_api_scopes:
            body["effective_user_api_scopes"] = self.effective_user_api_scopes
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.oauth2_app_client_id is not None:
            body["oauth2_app_client_id"] = self.oauth2_app_client_id
        if self.oauth2_app_integration_id is not None:
            body["oauth2_app_integration_id"] = self.oauth2_app_integration_id
        if self.resources:
            body["resources"] = self.resources
        if self.service_principal_client_id is not None:
            body["service_principal_client_id"] = self.service_principal_client_id
        if self.service_principal_id is not None:
            body["service_principal_id"] = self.service_principal_id
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.status:
            body["status"] = self.status
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.updater is not None:
            body["updater"] = self.updater
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = self.user_api_scopes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Space:
        """Deserializes the Space from a dictionary."""
        return cls(
            create_time=_timestamp(d, "create_time"),
            creator=d.get("creator", None),
            description=d.get("description", None),
            effective_usage_policy_id=d.get("effective_usage_policy_id", None),
            effective_user_api_scopes=d.get("effective_user_api_scopes", None),
            id=d.get("id", None),
            name=d.get("name", None),
            oauth2_app_client_id=d.get("oauth2_app_client_id", None),
            oauth2_app_integration_id=d.get("oauth2_app_integration_id", None),
            resources=_repeated_dict(d, "resources", AppResource),
            service_principal_client_id=d.get("service_principal_client_id", None),
            service_principal_id=d.get("service_principal_id", None),
            service_principal_name=d.get("service_principal_name", None),
            status=_from_dict(d, "status", SpaceStatus),
            update_time=_timestamp(d, "update_time"),
            updater=d.get("updater", None),
            usage_policy_id=d.get("usage_policy_id", None),
            user_api_scopes=d.get("user_api_scopes", None),
        )


@dataclass
class SpaceStatus:
    message: Optional[str] = None
    """Message providing context about the current state."""

    state: Optional[SpaceStatusSpaceState] = None
    """The state of the app space."""

    def as_dict(self) -> dict:
        """Serializes the SpaceStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SpaceStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SpaceStatus:
        """Deserializes the SpaceStatus from a dictionary."""
        return cls(message=d.get("message", None), state=_enum(d, "state", SpaceStatusSpaceState))


class SpaceStatusSpaceState(Enum):

    SPACE_ACTIVE = "SPACE_ACTIVE"
    SPACE_CREATING = "SPACE_CREATING"
    SPACE_DELETED = "SPACE_DELETED"
    SPACE_DELETING = "SPACE_DELETING"
    SPACE_ERROR = "SPACE_ERROR"
    SPACE_UPDATING = "SPACE_UPDATING"


@dataclass
class SpaceUpdate:
    """Tracks app space update information."""

    description: Optional[str] = None

    resources: Optional[List[AppResource]] = None

    status: Optional[SpaceUpdateStatus] = None

    usage_policy_id: Optional[str] = None

    user_api_scopes: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the SpaceUpdate into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.resources:
            body["resources"] = [v.as_dict() for v in self.resources]
        if self.status:
            body["status"] = self.status.as_dict()
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = [v for v in self.user_api_scopes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SpaceUpdate into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.resources:
            body["resources"] = self.resources
        if self.status:
            body["status"] = self.status
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.user_api_scopes:
            body["user_api_scopes"] = self.user_api_scopes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SpaceUpdate:
        """Deserializes the SpaceUpdate from a dictionary."""
        return cls(
            description=d.get("description", None),
            resources=_repeated_dict(d, "resources", AppResource),
            status=_from_dict(d, "status", SpaceUpdateStatus),
            usage_policy_id=d.get("usage_policy_id", None),
            user_api_scopes=d.get("user_api_scopes", None),
        )


class SpaceUpdateState(Enum):

    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"
    NOT_UPDATED = "NOT_UPDATED"
    SUCCEEDED = "SUCCEEDED"


@dataclass
class SpaceUpdateStatus:
    """Status of an app space update operation"""

    message: Optional[str] = None

    state: Optional[SpaceUpdateState] = None

    def as_dict(self) -> dict:
        """Serializes the SpaceUpdateStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SpaceUpdateStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SpaceUpdateStatus:
        """Deserializes the SpaceUpdateStatus from a dictionary."""
        return cls(message=d.get("message", None), state=_enum(d, "state", SpaceUpdateState))


class AppsAPI:
    """Apps run directly on a customer's Databricks instance, integrate with their data, use and extend
    Databricks services, and enable users to interact through single sign-on."""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_app_active(
        self, name: str, timeout=timedelta(minutes=20), callback: Optional[Callable[[App], None]] = None
    ) -> App:
        deadline = time.time() + timeout.total_seconds()
        target_states = (ComputeState.ACTIVE,)
        failure_states = (
            ComputeState.ERROR,
            ComputeState.STOPPED,
        )
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get(name=name)
            status = poll.compute_status.state
            status_message = f"current status: {status}"
            if poll.compute_status:
                status_message = poll.compute_status.message
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach ACTIVE, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"name={name}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def wait_get_update_app_succeeded(
        self, app_name: str, timeout=timedelta(minutes=20), callback: Optional[Callable[[AppUpdate], None]] = None
    ) -> AppUpdate:
        deadline = time.time() + timeout.total_seconds()
        target_states = (AppUpdateUpdateStatusUpdateState.SUCCEEDED,)
        failure_states = (AppUpdateUpdateStatusUpdateState.FAILED,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get_update(app_name=app_name)
            status = poll.status.state
            status_message = f"current status: {status}"
            if poll.status:
                status_message = poll.status.message
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach SUCCEEDED, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"app_name={app_name}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def wait_get_deployment_app_succeeded(
        self,
        app_name: str,
        deployment_id: str,
        timeout=timedelta(minutes=20),
        callback: Optional[Callable[[AppDeployment], None]] = None,
    ) -> AppDeployment:
        deadline = time.time() + timeout.total_seconds()
        target_states = (AppDeploymentState.SUCCEEDED,)
        failure_states = (AppDeploymentState.FAILED,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get_deployment(app_name=app_name, deployment_id=deployment_id)
            status = poll.status.state
            status_message = f"current status: {status}"
            if poll.status:
                status_message = poll.status.message
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach SUCCEEDED, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"app_name={app_name}, deployment_id={deployment_id}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def wait_get_app_stopped(
        self, name: str, timeout=timedelta(minutes=20), callback: Optional[Callable[[App], None]] = None
    ) -> App:
        deadline = time.time() + timeout.total_seconds()
        target_states = (ComputeState.STOPPED,)
        failure_states = (ComputeState.ERROR,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get(name=name)
            status = poll.compute_status.state
            status_message = f"current status: {status}"
            if poll.compute_status:
                status_message = poll.compute_status.message
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach STOPPED, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"name={name}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def create(self, app: App, *, no_compute: Optional[bool] = None) -> Wait[App]:
        """Creates a new app.

        :param app: :class:`App`
        :param no_compute: bool (optional)
          If true, the app will not be started after creation.

        :returns:
          Long-running operation waiter for :class:`App`.
          See :method:wait_get_app_active for more details.
        """

        body = app.as_dict()
        query = {}
        if no_compute is not None:
            query["no_compute"] = no_compute
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.0/apps", query=query, body=body, headers=headers)
        return Wait(self.wait_get_app_active, response=App.from_dict(op_response), name=op_response["name"])

    def create_and_wait(self, app: App, *, no_compute: Optional[bool] = None, timeout=timedelta(minutes=20)) -> App:
        return self.create(app=app, no_compute=no_compute).result(timeout=timeout)

    def create_space(self, space: Space) -> CreateSpaceOperation:
        """Creates a new app space.

        :param space: :class:`Space`

        :returns: :class:`Operation`
        """

        body = space.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/app-spaces", body=body, headers=headers)
        operation = Operation.from_dict(res)
        return CreateSpaceOperation(self, operation)

    def create_update(self, app_name: str, update_mask: str, *, app: Optional[App] = None) -> Wait[AppUpdate]:
        """Creates an app update and starts the update process. The update process is asynchronous and the status
        of the update can be checked with the GetAppUpdate method.

        :param app_name: str
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.
        :param app: :class:`App` (optional)

        :returns:
          Long-running operation waiter for :class:`AppUpdate`.
          See :method:wait_get_update_app_succeeded for more details.
        """

        body = {}
        if app is not None:
            body["app"] = app.as_dict()
        if update_mask is not None:
            body["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/apps/{app_name}/update", body=body, headers=headers)
        return Wait(self.wait_get_update_app_succeeded, response=AppUpdate.from_dict(op_response), app_name=app_name)

    def create_update_and_wait(
        self, app_name: str, update_mask: str, *, app: Optional[App] = None, timeout=timedelta(minutes=20)
    ) -> AppUpdate:
        return self.create_update(app=app, app_name=app_name, update_mask=update_mask).result(timeout=timeout)

    def delete(self, name: str) -> App:
        """Deletes an app.

        :param name: str
          The name of the app.

        :returns: :class:`App`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.0/apps/{name}", headers=headers)
        return App.from_dict(res)

    def delete_space(self, name: str) -> DeleteSpaceOperation:
        """Deletes an app space.

        :param name: str
          The name of the app space.

        :returns: :class:`Operation`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.0/app-spaces/{name}", headers=headers)
        operation = Operation.from_dict(res)
        return DeleteSpaceOperation(self, operation)

    def deploy(self, app_name: str, app_deployment: AppDeployment) -> Wait[AppDeployment]:
        """Creates an app deployment for the app with the supplied name.

        :param app_name: str
          The name of the app.
        :param app_deployment: :class:`AppDeployment`
          The app deployment configuration.

        :returns:
          Long-running operation waiter for :class:`AppDeployment`.
          See :method:wait_get_deployment_app_succeeded for more details.
        """

        body = app_deployment.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/apps/{app_name}/deployments", body=body, headers=headers)
        return Wait(
            self.wait_get_deployment_app_succeeded,
            response=AppDeployment.from_dict(op_response),
            app_name=app_name,
            deployment_id=op_response["deployment_id"],
        )

    def deploy_and_wait(
        self, app_name: str, app_deployment: AppDeployment, timeout=timedelta(minutes=20)
    ) -> AppDeployment:
        return self.deploy(app_deployment=app_deployment, app_name=app_name).result(timeout=timeout)

    def get(self, name: str) -> App:
        """Retrieves information for the app with the supplied name.

        :param name: str
          The name of the app.

        :returns: :class:`App`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/apps/{name}", headers=headers)
        return App.from_dict(res)

    def get_deployment(self, app_name: str, deployment_id: str) -> AppDeployment:
        """Retrieves information for the app deployment with the supplied name and deployment id.

        :param app_name: str
          The name of the app.
        :param deployment_id: str
          The unique id of the deployment.

        :returns: :class:`AppDeployment`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/apps/{app_name}/deployments/{deployment_id}", headers=headers)
        return AppDeployment.from_dict(res)

    def get_permission_levels(self, app_name: str) -> GetAppPermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param app_name: str
          The app for which to get or manage permissions.

        :returns: :class:`GetAppPermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/apps/{app_name}/permissionLevels", headers=headers)
        return GetAppPermissionLevelsResponse.from_dict(res)

    def get_permissions(self, app_name: str) -> AppPermissions:
        """Gets the permissions of an app. Apps can inherit permissions from their root object.

        :param app_name: str
          The app for which to get or manage permissions.

        :returns: :class:`AppPermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/apps/{app_name}", headers=headers)
        return AppPermissions.from_dict(res)

    def get_space(self, name: str) -> Space:
        """Retrieves information for the app space with the supplied name.

        :param name: str
          The name of the app space.

        :returns: :class:`Space`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/app-spaces/{name}", headers=headers)
        return Space.from_dict(res)

    def get_space_operation(self, name: str) -> Operation:
        """Gets the status of an app space update operation.

        :param name: str
          The name of the operation resource.

        :returns: :class:`Operation`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/app-spaces/{name}/operation", headers=headers)
        return Operation.from_dict(res)

    def get_update(self, app_name: str) -> AppUpdate:
        """Gets the status of an app update.

        :param app_name: str
          The name of the app.

        :returns: :class:`AppUpdate`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/apps/{app_name}/update", headers=headers)
        return AppUpdate.from_dict(res)

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None, space: Optional[str] = None
    ) -> Iterator[App]:
        """Lists all apps in the workspace.

        :param page_size: int (optional)
          Upper bound for items returned.
        :param page_token: str (optional)
          Pagination token to go to the next page of apps. Requests first page if absent.
        :param space: str (optional)
          Filter apps by app space name. When specified, only apps belonging to this space are returned.

        :returns: Iterator over :class:`App`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        if space is not None:
            query["space"] = space
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/apps", query=query, headers=headers)
            if "apps" in json:
                for v in json["apps"]:
                    yield App.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_deployments(
        self, app_name: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[AppDeployment]:
        """Lists all app deployments for the app with the supplied name.

        :param app_name: str
          The name of the app.
        :param page_size: int (optional)
          Upper bound for items returned.
        :param page_token: str (optional)
          Pagination token to go to the next page of apps. Requests first page if absent.

        :returns: Iterator over :class:`AppDeployment`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", f"/api/2.0/apps/{app_name}/deployments", query=query, headers=headers)
            if "app_deployments" in json:
                for v in json["app_deployments"]:
                    yield AppDeployment.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_spaces(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[Space]:
        """Lists all app spaces in the workspace.

        :param page_size: int (optional)
          Upper bound for items returned.
        :param page_token: str (optional)
          Pagination token to go to the next page of app spaces. Requests first page if absent.

        :returns: Iterator over :class:`Space`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/app-spaces", query=query, headers=headers)
            if "spaces" in json:
                for v in json["spaces"]:
                    yield Space.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def set_permissions(
        self, app_name: str, *, access_control_list: Optional[List[AppAccessControlRequest]] = None
    ) -> AppPermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param app_name: str
          The app for which to get or manage permissions.
        :param access_control_list: List[:class:`AppAccessControlRequest`] (optional)

        :returns: :class:`AppPermissions`
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

        res = self._api.do("PUT", f"/api/2.0/permissions/apps/{app_name}", body=body, headers=headers)
        return AppPermissions.from_dict(res)

    def start(self, name: str) -> Wait[App]:
        """Start the last active deployment of the app in the workspace.

        :param name: str
          The name of the app.

        :returns:
          Long-running operation waiter for :class:`App`.
          See :method:wait_get_app_active for more details.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/apps/{name}/start", headers=headers)
        return Wait(self.wait_get_app_active, response=App.from_dict(op_response), name=op_response["name"])

    def start_and_wait(self, name: str, timeout=timedelta(minutes=20)) -> App:
        return self.start(name=name).result(timeout=timeout)

    def stop(self, name: str) -> Wait[App]:
        """Stops the active deployment of the app in the workspace.

        :param name: str
          The name of the app.

        :returns:
          Long-running operation waiter for :class:`App`.
          See :method:wait_get_app_stopped for more details.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/apps/{name}/stop", headers=headers)
        return Wait(self.wait_get_app_stopped, response=App.from_dict(op_response), name=op_response["name"])

    def stop_and_wait(self, name: str, timeout=timedelta(minutes=20)) -> App:
        return self.stop(name=name).result(timeout=timeout)

    def update(self, name: str, app: App) -> App:
        """Updates the app with the supplied name.

        :param name: str
          The name of the app. The name must contain only lowercase alphanumeric characters and hyphens. It
          must be unique within the workspace.
        :param app: :class:`App`

        :returns: :class:`App`
        """

        body = app.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/apps/{name}", body=body, headers=headers)
        return App.from_dict(res)

    def update_permissions(
        self, app_name: str, *, access_control_list: Optional[List[AppAccessControlRequest]] = None
    ) -> AppPermissions:
        """Updates the permissions on an app. Apps can inherit permissions from their root object.

        :param app_name: str
          The app for which to get or manage permissions.
        :param access_control_list: List[:class:`AppAccessControlRequest`] (optional)

        :returns: :class:`AppPermissions`
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

        res = self._api.do("PATCH", f"/api/2.0/permissions/apps/{app_name}", body=body, headers=headers)
        return AppPermissions.from_dict(res)

    def update_space(self, name: str, space: Space, update_mask: FieldMask) -> UpdateSpaceOperation:
        """Updates an app space. The update process is asynchronous and the status of the update can be checked
        with the GetSpaceOperation method.

        :param name: str
          The name of the app space. The name must contain only lowercase alphanumeric characters and hyphens.
          It must be unique within the workspace.
        :param space: :class:`Space`
        :param update_mask: FieldMask
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`Operation`
        """

        body = space.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask.ToJsonString()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/app-spaces/{name}", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return UpdateSpaceOperation(self, operation)


class CreateSpaceOperation:
    """Long-running operation for create_space"""

    def __init__(self, impl: AppsAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Space:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Space`
        """

        def poll_operation():
            operation = self._impl.get_space_operation(name=self._operation.name)

            # Update local operation state
            self._operation = operation

            if not operation.done:
                return None, RetryError.continues("operation still in progress")

            if operation.error:
                error_msg = operation.error.message if operation.error.message else "unknown error"
                if operation.error.error_code:
                    error_msg = f"[{operation.error.error_code}] {error_msg}"
                return None, RetryError.halt(Exception(f"operation failed: {error_msg}"))

            # Operation completed successfully, unmarshal response.
            if operation.response is None:
                return None, RetryError.halt(Exception("operation completed but no response available"))

            space = Space.from_dict(operation.response)

            return space, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> Space:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`Space` or None
        """
        if self._operation.metadata is None:
            return None

        return Space.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_space_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class DeleteSpaceOperation:
    """Long-running operation for delete_space"""

    def __init__(self, impl: AppsAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None):
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Any /* MISSING TYPE */`
        """

        def poll_operation():
            operation = self._impl.get_space_operation(name=self._operation.name)

            # Update local operation state
            self._operation = operation

            if not operation.done:
                return None, RetryError.continues("operation still in progress")

            if operation.error:
                error_msg = operation.error.message if operation.error.message else "unknown error"
                if operation.error.error_code:
                    error_msg = f"[{operation.error.error_code}] {error_msg}"
                return None, RetryError.halt(Exception(f"operation failed: {error_msg}"))

            # Operation completed successfully, unmarshal response.
            if operation.response is None:
                return None, RetryError.halt(Exception("operation completed but no response available"))

            return {}, None

        poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> Space:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`Space` or None
        """
        if self._operation.metadata is None:
            return None

        return Space.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_space_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class UpdateSpaceOperation:
    """Long-running operation for update_space"""

    def __init__(self, impl: AppsAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Space:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Space`
        """

        def poll_operation():
            operation = self._impl.get_space_operation(name=self._operation.name)

            # Update local operation state
            self._operation = operation

            if not operation.done:
                return None, RetryError.continues("operation still in progress")

            if operation.error:
                error_msg = operation.error.message if operation.error.message else "unknown error"
                if operation.error.error_code:
                    error_msg = f"[{operation.error.error_code}] {error_msg}"
                return None, RetryError.halt(Exception(f"operation failed: {error_msg}"))

            # Operation completed successfully, unmarshal response.
            if operation.response is None:
                return None, RetryError.halt(Exception("operation completed but no response available"))

            space = Space.from_dict(operation.response)

            return space, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> SpaceUpdate:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`SpaceUpdate` or None
        """
        if self._operation.metadata is None:
            return None

        return SpaceUpdate.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_space_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class AppsSettingsAPI:
    """Apps Settings manage the settings for the Apps service on a customer's Databricks instance."""

    def __init__(self, api_client):
        self._api = api_client

    def create_custom_template(self, template: CustomTemplate) -> CustomTemplate:
        """Creates a custom template.

        :param template: :class:`CustomTemplate`

        :returns: :class:`CustomTemplate`
        """

        body = template.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/apps-settings/templates", body=body, headers=headers)
        return CustomTemplate.from_dict(res)

    def delete_custom_template(self, name: str) -> CustomTemplate:
        """Deletes the custom template with the specified name.

        :param name: str
          The name of the custom template.

        :returns: :class:`CustomTemplate`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.0/apps-settings/templates/{name}", headers=headers)
        return CustomTemplate.from_dict(res)

    def get_custom_template(self, name: str) -> CustomTemplate:
        """Gets the custom template with the specified name.

        :param name: str
          The name of the custom template.

        :returns: :class:`CustomTemplate`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/apps-settings/templates/{name}", headers=headers)
        return CustomTemplate.from_dict(res)

    def list_custom_templates(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[CustomTemplate]:
        """Lists all custom templates in the workspace.

        :param page_size: int (optional)
          Upper bound for items returned.
        :param page_token: str (optional)
          Pagination token to go to the next page of custom templates. Requests first page if absent.

        :returns: Iterator over :class:`CustomTemplate`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/apps-settings/templates", query=query, headers=headers)
            if "templates" in json:
                for v in json["templates"]:
                    yield CustomTemplate.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_custom_template(self, name: str, template: CustomTemplate) -> CustomTemplate:
        """Updates the custom template with the specified name. Note that the template name cannot be updated.

        :param name: str
          The name of the template. It must contain only alphanumeric characters, hyphens, underscores, and
          whitespaces. It must be unique within the workspace.
        :param template: :class:`CustomTemplate`

        :returns: :class:`CustomTemplate`
        """

        body = template.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/apps-settings/templates/{name}", body=body, headers=headers)
        return CustomTemplate.from_dict(res)
