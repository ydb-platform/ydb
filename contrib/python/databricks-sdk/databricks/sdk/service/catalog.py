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
from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict, _repeated_enum,
                                              _timestamp)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AccessRequestDestinations:
    securable: Securable
    """The securable for which the access request destinations are being modified or read."""

    are_any_destinations_hidden: Optional[bool] = None
    """Indicates whether any destinations are hidden from the caller due to a lack of permissions. This
    value is true if the caller does not have permission to see all destinations."""

    destination_source_securable: Optional[Securable] = None
    """The source securable from which the destinations are inherited. Either the same value as
    securable (if destination is set directly on the securable) or the nearest parent securable with
    destinations set."""

    destinations: Optional[List[NotificationDestination]] = None
    """The access request destinations for the securable."""

    full_name: Optional[str] = None
    """The full name of the securable. Redundant with the name in the securable object, but necessary
    for Terraform integration"""

    securable_type: Optional[str] = None
    """The type of the securable. Redundant with the type in the securable object, but necessary for
    Terraform integration"""

    def as_dict(self) -> dict:
        """Serializes the AccessRequestDestinations into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.are_any_destinations_hidden is not None:
            body["are_any_destinations_hidden"] = self.are_any_destinations_hidden
        if self.destination_source_securable:
            body["destination_source_securable"] = self.destination_source_securable.as_dict()
        if self.destinations:
            body["destinations"] = [v.as_dict() for v in self.destinations]
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.securable:
            body["securable"] = self.securable.as_dict()
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccessRequestDestinations into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.are_any_destinations_hidden is not None:
            body["are_any_destinations_hidden"] = self.are_any_destinations_hidden
        if self.destination_source_securable:
            body["destination_source_securable"] = self.destination_source_securable
        if self.destinations:
            body["destinations"] = self.destinations
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.securable:
            body["securable"] = self.securable
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccessRequestDestinations:
        """Deserializes the AccessRequestDestinations from a dictionary."""
        return cls(
            are_any_destinations_hidden=d.get("are_any_destinations_hidden", None),
            destination_source_securable=_from_dict(d, "destination_source_securable", Securable),
            destinations=_repeated_dict(d, "destinations", NotificationDestination),
            full_name=d.get("full_name", None),
            securable=_from_dict(d, "securable", Securable),
            securable_type=d.get("securable_type", None),
        )


@dataclass
class AccountsCreateMetastoreAssignmentResponse:
    """The metastore assignment was successfully created."""

    def as_dict(self) -> dict:
        """Serializes the AccountsCreateMetastoreAssignmentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsCreateMetastoreAssignmentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsCreateMetastoreAssignmentResponse:
        """Deserializes the AccountsCreateMetastoreAssignmentResponse from a dictionary."""
        return cls()


@dataclass
class AccountsCreateMetastoreResponse:
    metastore_info: Optional[MetastoreInfo] = None

    def as_dict(self) -> dict:
        """Serializes the AccountsCreateMetastoreResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metastore_info:
            body["metastore_info"] = self.metastore_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsCreateMetastoreResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metastore_info:
            body["metastore_info"] = self.metastore_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsCreateMetastoreResponse:
        """Deserializes the AccountsCreateMetastoreResponse from a dictionary."""
        return cls(metastore_info=_from_dict(d, "metastore_info", MetastoreInfo))


@dataclass
class AccountsCreateStorageCredentialInfo:
    credential_info: Optional[StorageCredentialInfo] = None

    def as_dict(self) -> dict:
        """Serializes the AccountsCreateStorageCredentialInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_info:
            body["credential_info"] = self.credential_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsCreateStorageCredentialInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_info:
            body["credential_info"] = self.credential_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsCreateStorageCredentialInfo:
        """Deserializes the AccountsCreateStorageCredentialInfo from a dictionary."""
        return cls(credential_info=_from_dict(d, "credential_info", StorageCredentialInfo))


@dataclass
class AccountsDeleteMetastoreAssignmentResponse:
    """The metastore assignment was successfully deleted."""

    def as_dict(self) -> dict:
        """Serializes the AccountsDeleteMetastoreAssignmentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsDeleteMetastoreAssignmentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsDeleteMetastoreAssignmentResponse:
        """Deserializes the AccountsDeleteMetastoreAssignmentResponse from a dictionary."""
        return cls()


@dataclass
class AccountsDeleteMetastoreResponse:
    """The metastore was successfully deleted."""

    def as_dict(self) -> dict:
        """Serializes the AccountsDeleteMetastoreResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsDeleteMetastoreResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsDeleteMetastoreResponse:
        """Deserializes the AccountsDeleteMetastoreResponse from a dictionary."""
        return cls()


@dataclass
class AccountsDeleteStorageCredentialResponse:
    """The storage credential was successfully deleted."""

    def as_dict(self) -> dict:
        """Serializes the AccountsDeleteStorageCredentialResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsDeleteStorageCredentialResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsDeleteStorageCredentialResponse:
        """Deserializes the AccountsDeleteStorageCredentialResponse from a dictionary."""
        return cls()


@dataclass
class AccountsGetMetastoreResponse:
    """The metastore was successfully returned."""

    metastore_info: Optional[MetastoreInfo] = None

    def as_dict(self) -> dict:
        """Serializes the AccountsGetMetastoreResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metastore_info:
            body["metastore_info"] = self.metastore_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsGetMetastoreResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metastore_info:
            body["metastore_info"] = self.metastore_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsGetMetastoreResponse:
        """Deserializes the AccountsGetMetastoreResponse from a dictionary."""
        return cls(metastore_info=_from_dict(d, "metastore_info", MetastoreInfo))


@dataclass
class AccountsListMetastoresResponse:
    """Metastores were returned successfully."""

    metastores: Optional[List[MetastoreInfo]] = None
    """An array of metastore information objects."""

    def as_dict(self) -> dict:
        """Serializes the AccountsListMetastoresResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metastores:
            body["metastores"] = [v.as_dict() for v in self.metastores]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsListMetastoresResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metastores:
            body["metastores"] = self.metastores
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsListMetastoresResponse:
        """Deserializes the AccountsListMetastoresResponse from a dictionary."""
        return cls(metastores=_repeated_dict(d, "metastores", MetastoreInfo))


@dataclass
class AccountsMetastoreAssignment:
    """The workspace metastore assignment was successfully returned."""

    metastore_assignment: Optional[MetastoreAssignment] = None

    def as_dict(self) -> dict:
        """Serializes the AccountsMetastoreAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metastore_assignment:
            body["metastore_assignment"] = self.metastore_assignment.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsMetastoreAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metastore_assignment:
            body["metastore_assignment"] = self.metastore_assignment
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsMetastoreAssignment:
        """Deserializes the AccountsMetastoreAssignment from a dictionary."""
        return cls(metastore_assignment=_from_dict(d, "metastore_assignment", MetastoreAssignment))


@dataclass
class AccountsStorageCredentialInfo:
    """The storage credential was successfully retrieved."""

    credential_info: Optional[StorageCredentialInfo] = None

    def as_dict(self) -> dict:
        """Serializes the AccountsStorageCredentialInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_info:
            body["credential_info"] = self.credential_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsStorageCredentialInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_info:
            body["credential_info"] = self.credential_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsStorageCredentialInfo:
        """Deserializes the AccountsStorageCredentialInfo from a dictionary."""
        return cls(credential_info=_from_dict(d, "credential_info", StorageCredentialInfo))


@dataclass
class AccountsUpdateMetastoreAssignmentResponse:
    """The metastore assignment was successfully updated."""

    def as_dict(self) -> dict:
        """Serializes the AccountsUpdateMetastoreAssignmentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsUpdateMetastoreAssignmentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsUpdateMetastoreAssignmentResponse:
        """Deserializes the AccountsUpdateMetastoreAssignmentResponse from a dictionary."""
        return cls()


@dataclass
class AccountsUpdateMetastoreResponse:
    """The metastore update request succeeded."""

    metastore_info: Optional[MetastoreInfo] = None

    def as_dict(self) -> dict:
        """Serializes the AccountsUpdateMetastoreResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metastore_info:
            body["metastore_info"] = self.metastore_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsUpdateMetastoreResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metastore_info:
            body["metastore_info"] = self.metastore_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsUpdateMetastoreResponse:
        """Deserializes the AccountsUpdateMetastoreResponse from a dictionary."""
        return cls(metastore_info=_from_dict(d, "metastore_info", MetastoreInfo))


@dataclass
class AccountsUpdateStorageCredentialResponse:
    """The storage credential was successfully updated."""

    credential_info: Optional[StorageCredentialInfo] = None

    def as_dict(self) -> dict:
        """Serializes the AccountsUpdateStorageCredentialResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_info:
            body["credential_info"] = self.credential_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountsUpdateStorageCredentialResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_info:
            body["credential_info"] = self.credential_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountsUpdateStorageCredentialResponse:
        """Deserializes the AccountsUpdateStorageCredentialResponse from a dictionary."""
        return cls(credential_info=_from_dict(d, "credential_info", StorageCredentialInfo))


@dataclass
class ArtifactAllowlistInfo:
    artifact_matchers: Optional[List[ArtifactMatcher]] = None
    """A list of allowed artifact match patterns."""

    created_at: Optional[int] = None
    """Time at which this artifact allowlist was set, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of the user who set the artifact allowlist."""

    metastore_id: Optional[str] = None
    """Unique identifier of parent metastore."""

    def as_dict(self) -> dict:
        """Serializes the ArtifactAllowlistInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.artifact_matchers:
            body["artifact_matchers"] = [v.as_dict() for v in self.artifact_matchers]
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ArtifactAllowlistInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.artifact_matchers:
            body["artifact_matchers"] = self.artifact_matchers
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ArtifactAllowlistInfo:
        """Deserializes the ArtifactAllowlistInfo from a dictionary."""
        return cls(
            artifact_matchers=_repeated_dict(d, "artifact_matchers", ArtifactMatcher),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            metastore_id=d.get("metastore_id", None),
        )


@dataclass
class ArtifactMatcher:
    artifact: str
    """The artifact path or maven coordinate"""

    match_type: MatchType
    """The pattern matching type of the artifact"""

    def as_dict(self) -> dict:
        """Serializes the ArtifactMatcher into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.artifact is not None:
            body["artifact"] = self.artifact
        if self.match_type is not None:
            body["match_type"] = self.match_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ArtifactMatcher into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.artifact is not None:
            body["artifact"] = self.artifact
        if self.match_type is not None:
            body["match_type"] = self.match_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ArtifactMatcher:
        """Deserializes the ArtifactMatcher from a dictionary."""
        return cls(artifact=d.get("artifact", None), match_type=_enum(d, "match_type", MatchType))


class ArtifactType(Enum):
    """The artifact type"""

    INIT_SCRIPT = "INIT_SCRIPT"
    LIBRARY_JAR = "LIBRARY_JAR"
    LIBRARY_MAVEN = "LIBRARY_MAVEN"


@dataclass
class AssignResponse:
    def as_dict(self) -> dict:
        """Serializes the AssignResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AssignResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AssignResponse:
        """Deserializes the AssignResponse from a dictionary."""
        return cls()


@dataclass
class AwsCredentials:
    """AWS temporary credentials for API authentication. Read more at
    https://docs.aws.amazon.com/STS/latest/APIReference/API_Credentials.html."""

    access_key_id: Optional[str] = None
    """The access key ID that identifies the temporary credentials."""

    access_point: Optional[str] = None
    """The Amazon Resource Name (ARN) of the S3 access point for temporary credentials related the
    external location."""

    secret_access_key: Optional[str] = None
    """The secret access key that can be used to sign AWS API requests."""

    session_token: Optional[str] = None
    """The token that users must pass to AWS API to use the temporary credentials."""

    def as_dict(self) -> dict:
        """Serializes the AwsCredentials into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_key_id is not None:
            body["access_key_id"] = self.access_key_id
        if self.access_point is not None:
            body["access_point"] = self.access_point
        if self.secret_access_key is not None:
            body["secret_access_key"] = self.secret_access_key
        if self.session_token is not None:
            body["session_token"] = self.session_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AwsCredentials into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_key_id is not None:
            body["access_key_id"] = self.access_key_id
        if self.access_point is not None:
            body["access_point"] = self.access_point
        if self.secret_access_key is not None:
            body["secret_access_key"] = self.secret_access_key
        if self.session_token is not None:
            body["session_token"] = self.session_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AwsCredentials:
        """Deserializes the AwsCredentials from a dictionary."""
        return cls(
            access_key_id=d.get("access_key_id", None),
            access_point=d.get("access_point", None),
            secret_access_key=d.get("secret_access_key", None),
            session_token=d.get("session_token", None),
        )


@dataclass
class AwsIamRole:
    """The AWS IAM role configuration"""

    external_id: Optional[str] = None
    """The external ID used in role assumption to prevent the confused deputy problem."""

    role_arn: Optional[str] = None
    """The Amazon Resource Name (ARN) of the AWS IAM role used to vend temporary credentials."""

    unity_catalog_iam_arn: Optional[str] = None
    """The Amazon Resource Name (ARN) of the AWS IAM user managed by Databricks. This is the identity
    that is going to assume the AWS IAM role."""

    def as_dict(self) -> dict:
        """Serializes the AwsIamRole into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        if self.unity_catalog_iam_arn is not None:
            body["unity_catalog_iam_arn"] = self.unity_catalog_iam_arn
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AwsIamRole into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        if self.unity_catalog_iam_arn is not None:
            body["unity_catalog_iam_arn"] = self.unity_catalog_iam_arn
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AwsIamRole:
        """Deserializes the AwsIamRole from a dictionary."""
        return cls(
            external_id=d.get("external_id", None),
            role_arn=d.get("role_arn", None),
            unity_catalog_iam_arn=d.get("unity_catalog_iam_arn", None),
        )


@dataclass
class AwsIamRoleRequest:
    """The AWS IAM role configuration"""

    role_arn: str
    """The Amazon Resource Name (ARN) of the AWS IAM role used to vend temporary credentials."""

    def as_dict(self) -> dict:
        """Serializes the AwsIamRoleRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AwsIamRoleRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AwsIamRoleRequest:
        """Deserializes the AwsIamRoleRequest from a dictionary."""
        return cls(role_arn=d.get("role_arn", None))


@dataclass
class AwsIamRoleResponse:
    """The AWS IAM role configuration"""

    role_arn: str
    """The Amazon Resource Name (ARN) of the AWS IAM role used to vend temporary credentials."""

    external_id: Optional[str] = None
    """The external ID used in role assumption to prevent the confused deputy problem."""

    unity_catalog_iam_arn: Optional[str] = None
    """The Amazon Resource Name (ARN) of the AWS IAM user managed by Databricks. This is the identity
    that is going to assume the AWS IAM role."""

    def as_dict(self) -> dict:
        """Serializes the AwsIamRoleResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        if self.unity_catalog_iam_arn is not None:
            body["unity_catalog_iam_arn"] = self.unity_catalog_iam_arn
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AwsIamRoleResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_id is not None:
            body["external_id"] = self.external_id
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        if self.unity_catalog_iam_arn is not None:
            body["unity_catalog_iam_arn"] = self.unity_catalog_iam_arn
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AwsIamRoleResponse:
        """Deserializes the AwsIamRoleResponse from a dictionary."""
        return cls(
            external_id=d.get("external_id", None),
            role_arn=d.get("role_arn", None),
            unity_catalog_iam_arn=d.get("unity_catalog_iam_arn", None),
        )


@dataclass
class AwsSqsQueue:
    managed_resource_id: Optional[str] = None
    """Unique identifier included in the name of file events managed cloud resources."""

    queue_url: Optional[str] = None
    """The AQS queue url in the format https://sqs.{region}.amazonaws.com/{account id}/{queue name}.
    Only required for provided_sqs."""

    def as_dict(self) -> dict:
        """Serializes the AwsSqsQueue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.managed_resource_id is not None:
            body["managed_resource_id"] = self.managed_resource_id
        if self.queue_url is not None:
            body["queue_url"] = self.queue_url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AwsSqsQueue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.managed_resource_id is not None:
            body["managed_resource_id"] = self.managed_resource_id
        if self.queue_url is not None:
            body["queue_url"] = self.queue_url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AwsSqsQueue:
        """Deserializes the AwsSqsQueue from a dictionary."""
        return cls(managed_resource_id=d.get("managed_resource_id", None), queue_url=d.get("queue_url", None))


@dataclass
class AzureActiveDirectoryToken:
    """Azure Active Directory token, essentially the Oauth token for Azure Service Principal or Managed
    Identity. Read more at
    https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token"""

    aad_token: Optional[str] = None
    """Opaque token that contains claims that you can use in Azure Active Directory to access cloud
    services."""

    def as_dict(self) -> dict:
        """Serializes the AzureActiveDirectoryToken into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aad_token is not None:
            body["aad_token"] = self.aad_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureActiveDirectoryToken into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aad_token is not None:
            body["aad_token"] = self.aad_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureActiveDirectoryToken:
        """Deserializes the AzureActiveDirectoryToken from a dictionary."""
        return cls(aad_token=d.get("aad_token", None))


@dataclass
class AzureManagedIdentity:
    """The Azure managed identity configuration."""

    access_connector_id: str
    """The Azure resource ID of the Azure Databricks Access Connector. Use the format
    `/subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.Databricks/accessConnectors/{connector-name}`."""

    credential_id: Optional[str] = None
    """The Databricks internal ID that represents this managed identity."""

    managed_identity_id: Optional[str] = None
    """The Azure resource ID of the managed identity. Use the format,
    `/subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{identity-name}`
    This is only available for user-assgined identities. For system-assigned identities, the
    access_connector_id is used to identify the identity. If this field is not provided, then we
    assume the AzureManagedIdentity is using the system-assigned identity."""

    def as_dict(self) -> dict:
        """Serializes the AzureManagedIdentity into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_connector_id is not None:
            body["access_connector_id"] = self.access_connector_id
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.managed_identity_id is not None:
            body["managed_identity_id"] = self.managed_identity_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureManagedIdentity into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_connector_id is not None:
            body["access_connector_id"] = self.access_connector_id
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.managed_identity_id is not None:
            body["managed_identity_id"] = self.managed_identity_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureManagedIdentity:
        """Deserializes the AzureManagedIdentity from a dictionary."""
        return cls(
            access_connector_id=d.get("access_connector_id", None),
            credential_id=d.get("credential_id", None),
            managed_identity_id=d.get("managed_identity_id", None),
        )


@dataclass
class AzureManagedIdentityRequest:
    """The Azure managed identity configuration."""

    access_connector_id: str
    """The Azure resource ID of the Azure Databricks Access Connector. Use the format
    `/subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.Databricks/accessConnectors/{connector-name}`."""

    managed_identity_id: Optional[str] = None
    """The Azure resource ID of the managed identity. Use the format,
    `/subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{identity-name}`
    This is only available for user-assgined identities. For system-assigned identities, the
    access_connector_id is used to identify the identity. If this field is not provided, then we
    assume the AzureManagedIdentity is using the system-assigned identity."""

    def as_dict(self) -> dict:
        """Serializes the AzureManagedIdentityRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_connector_id is not None:
            body["access_connector_id"] = self.access_connector_id
        if self.managed_identity_id is not None:
            body["managed_identity_id"] = self.managed_identity_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureManagedIdentityRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_connector_id is not None:
            body["access_connector_id"] = self.access_connector_id
        if self.managed_identity_id is not None:
            body["managed_identity_id"] = self.managed_identity_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureManagedIdentityRequest:
        """Deserializes the AzureManagedIdentityRequest from a dictionary."""
        return cls(
            access_connector_id=d.get("access_connector_id", None),
            managed_identity_id=d.get("managed_identity_id", None),
        )


@dataclass
class AzureManagedIdentityResponse:
    """The Azure managed identity configuration."""

    access_connector_id: str
    """The Azure resource ID of the Azure Databricks Access Connector. Use the format
    `/subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.Databricks/accessConnectors/{connector-name}`."""

    credential_id: Optional[str] = None
    """The Databricks internal ID that represents this managed identity."""

    managed_identity_id: Optional[str] = None
    """The Azure resource ID of the managed identity. Use the format,
    `/subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{identity-name}`
    This is only available for user-assgined identities. For system-assigned identities, the
    access_connector_id is used to identify the identity. If this field is not provided, then we
    assume the AzureManagedIdentity is using the system-assigned identity."""

    def as_dict(self) -> dict:
        """Serializes the AzureManagedIdentityResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_connector_id is not None:
            body["access_connector_id"] = self.access_connector_id
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.managed_identity_id is not None:
            body["managed_identity_id"] = self.managed_identity_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureManagedIdentityResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_connector_id is not None:
            body["access_connector_id"] = self.access_connector_id
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.managed_identity_id is not None:
            body["managed_identity_id"] = self.managed_identity_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureManagedIdentityResponse:
        """Deserializes the AzureManagedIdentityResponse from a dictionary."""
        return cls(
            access_connector_id=d.get("access_connector_id", None),
            credential_id=d.get("credential_id", None),
            managed_identity_id=d.get("managed_identity_id", None),
        )


@dataclass
class AzureQueueStorage:
    managed_resource_id: Optional[str] = None
    """Unique identifier included in the name of file events managed cloud resources."""

    queue_url: Optional[str] = None
    """The AQS queue url in the format https://{storage account}.queue.core.windows.net/{queue name}
    Only required for provided_aqs."""

    resource_group: Optional[str] = None
    """Optional resource group for the queue, event grid subscription, and external location storage
    account. Only required for locations with a service principal storage credential"""

    subscription_id: Optional[str] = None
    """Optional subscription id for the queue, event grid subscription, and external location storage
    account. Required for locations with a service principal storage credential"""

    def as_dict(self) -> dict:
        """Serializes the AzureQueueStorage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.managed_resource_id is not None:
            body["managed_resource_id"] = self.managed_resource_id
        if self.queue_url is not None:
            body["queue_url"] = self.queue_url
        if self.resource_group is not None:
            body["resource_group"] = self.resource_group
        if self.subscription_id is not None:
            body["subscription_id"] = self.subscription_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureQueueStorage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.managed_resource_id is not None:
            body["managed_resource_id"] = self.managed_resource_id
        if self.queue_url is not None:
            body["queue_url"] = self.queue_url
        if self.resource_group is not None:
            body["resource_group"] = self.resource_group
        if self.subscription_id is not None:
            body["subscription_id"] = self.subscription_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureQueueStorage:
        """Deserializes the AzureQueueStorage from a dictionary."""
        return cls(
            managed_resource_id=d.get("managed_resource_id", None),
            queue_url=d.get("queue_url", None),
            resource_group=d.get("resource_group", None),
            subscription_id=d.get("subscription_id", None),
        )


@dataclass
class AzureServicePrincipal:
    """The Azure service principal configuration. Only applicable when purpose is **STORAGE**."""

    directory_id: str
    """The directory ID corresponding to the Azure Active Directory (AAD) tenant of the application."""

    application_id: str
    """The application ID of the application registration within the referenced AAD tenant."""

    client_secret: str
    """The client secret generated for the above app ID in AAD."""

    def as_dict(self) -> dict:
        """Serializes the AzureServicePrincipal into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.application_id is not None:
            body["application_id"] = self.application_id
        if self.client_secret is not None:
            body["client_secret"] = self.client_secret
        if self.directory_id is not None:
            body["directory_id"] = self.directory_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureServicePrincipal into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.application_id is not None:
            body["application_id"] = self.application_id
        if self.client_secret is not None:
            body["client_secret"] = self.client_secret
        if self.directory_id is not None:
            body["directory_id"] = self.directory_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureServicePrincipal:
        """Deserializes the AzureServicePrincipal from a dictionary."""
        return cls(
            application_id=d.get("application_id", None),
            client_secret=d.get("client_secret", None),
            directory_id=d.get("directory_id", None),
        )


@dataclass
class AzureUserDelegationSas:
    """Azure temporary credentials for API authentication. Read more at
    https://docs.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas"""

    sas_token: Optional[str] = None
    """The signed URI (SAS Token) used to access blob services for a given path"""

    def as_dict(self) -> dict:
        """Serializes the AzureUserDelegationSas into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.sas_token is not None:
            body["sas_token"] = self.sas_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureUserDelegationSas into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.sas_token is not None:
            body["sas_token"] = self.sas_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureUserDelegationSas:
        """Deserializes the AzureUserDelegationSas from a dictionary."""
        return cls(sas_token=d.get("sas_token", None))


@dataclass
class BatchCreateAccessRequestsResponse:
    responses: Optional[List[CreateAccessRequestResponse]] = None
    """The access request destinations for each securable object the principal requested."""

    def as_dict(self) -> dict:
        """Serializes the BatchCreateAccessRequestsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.responses:
            body["responses"] = [v.as_dict() for v in self.responses]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BatchCreateAccessRequestsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.responses:
            body["responses"] = self.responses
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BatchCreateAccessRequestsResponse:
        """Deserializes the BatchCreateAccessRequestsResponse from a dictionary."""
        return cls(responses=_repeated_dict(d, "responses", CreateAccessRequestResponse))


@dataclass
class CancelRefreshResponse:
    def as_dict(self) -> dict:
        """Serializes the CancelRefreshResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CancelRefreshResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CancelRefreshResponse:
        """Deserializes the CancelRefreshResponse from a dictionary."""
        return cls()


@dataclass
class CatalogInfo:
    browse_only: Optional[bool] = None
    """Indicates whether the principal is limited to retrieving metadata for the associated object
    through the BROWSE privilege when include_browse is enabled in the request."""

    catalog_type: Optional[CatalogType] = None

    comment: Optional[str] = None
    """User-provided free-form text description."""

    connection_name: Optional[str] = None
    """The name of the connection to an external data source."""

    created_at: Optional[int] = None
    """Time at which this catalog was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of catalog creator."""

    effective_predictive_optimization_flag: Optional[EffectivePredictiveOptimizationFlag] = None

    enable_predictive_optimization: Optional[EnablePredictiveOptimization] = None
    """Whether predictive optimization should be enabled for this object and objects under it."""

    full_name: Optional[str] = None
    """The full name of the catalog. Corresponds with the name field."""

    isolation_mode: Optional[CatalogIsolationMode] = None
    """Whether the current securable is accessible from all workspaces or a specific set of workspaces."""

    metastore_id: Optional[str] = None
    """Unique identifier of parent metastore."""

    name: Optional[str] = None
    """Name of catalog."""

    options: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    owner: Optional[str] = None
    """Username of current owner of catalog."""

    properties: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    provider_name: Optional[str] = None
    """The name of delta sharing provider.
    
    A Delta Sharing catalog is a catalog that is based on a Delta share on a remote sharing server."""

    provisioning_info: Optional[ProvisioningInfo] = None

    securable_type: Optional[SecurableType] = None

    share_name: Optional[str] = None
    """The name of the share under the share provider."""

    storage_location: Optional[str] = None
    """Storage Location URL (full path) for managed tables within catalog."""

    storage_root: Optional[str] = None
    """Storage root URL for managed tables within catalog."""

    updated_at: Optional[int] = None
    """Time at which this catalog was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified catalog."""

    def as_dict(self) -> dict:
        """Serializes the CatalogInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_type is not None:
            body["catalog_type"] = self.catalog_type.value
        if self.comment is not None:
            body["comment"] = self.comment
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.effective_predictive_optimization_flag:
            body["effective_predictive_optimization_flag"] = self.effective_predictive_optimization_flag.as_dict()
        if self.enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = self.enable_predictive_optimization.value
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode.value
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.provider_name is not None:
            body["provider_name"] = self.provider_name
        if self.provisioning_info:
            body["provisioning_info"] = self.provisioning_info.as_dict()
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type.value
        if self.share_name is not None:
            body["share_name"] = self.share_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CatalogInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_type is not None:
            body["catalog_type"] = self.catalog_type
        if self.comment is not None:
            body["comment"] = self.comment
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.effective_predictive_optimization_flag:
            body["effective_predictive_optimization_flag"] = self.effective_predictive_optimization_flag
        if self.enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = self.enable_predictive_optimization
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.provider_name is not None:
            body["provider_name"] = self.provider_name
        if self.provisioning_info:
            body["provisioning_info"] = self.provisioning_info
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        if self.share_name is not None:
            body["share_name"] = self.share_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CatalogInfo:
        """Deserializes the CatalogInfo from a dictionary."""
        return cls(
            browse_only=d.get("browse_only", None),
            catalog_type=_enum(d, "catalog_type", CatalogType),
            comment=d.get("comment", None),
            connection_name=d.get("connection_name", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            effective_predictive_optimization_flag=_from_dict(
                d, "effective_predictive_optimization_flag", EffectivePredictiveOptimizationFlag
            ),
            enable_predictive_optimization=_enum(d, "enable_predictive_optimization", EnablePredictiveOptimization),
            full_name=d.get("full_name", None),
            isolation_mode=_enum(d, "isolation_mode", CatalogIsolationMode),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            options=d.get("options", None),
            owner=d.get("owner", None),
            properties=d.get("properties", None),
            provider_name=d.get("provider_name", None),
            provisioning_info=_from_dict(d, "provisioning_info", ProvisioningInfo),
            securable_type=_enum(d, "securable_type", SecurableType),
            share_name=d.get("share_name", None),
            storage_location=d.get("storage_location", None),
            storage_root=d.get("storage_root", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


class CatalogIsolationMode(Enum):

    ISOLATED = "ISOLATED"
    OPEN = "OPEN"


class CatalogType(Enum):
    """The type of the catalog."""

    DELTASHARING_CATALOG = "DELTASHARING_CATALOG"
    FOREIGN_CATALOG = "FOREIGN_CATALOG"
    INTERNAL_CATALOG = "INTERNAL_CATALOG"
    MANAGED_CATALOG = "MANAGED_CATALOG"
    MANAGED_ONLINE_CATALOG = "MANAGED_ONLINE_CATALOG"
    SYSTEM_CATALOG = "SYSTEM_CATALOG"


@dataclass
class CloudflareApiToken:
    """The Cloudflare API token configuration. Read more at
    https://developers.cloudflare.com/r2/api/s3/tokens/"""

    access_key_id: str
    """The access key ID associated with the API token."""

    secret_access_key: str
    """The secret access token generated for the above access key ID."""

    account_id: Optional[str] = None
    """The ID of the account associated with the API token."""

    def as_dict(self) -> dict:
        """Serializes the CloudflareApiToken into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_key_id is not None:
            body["access_key_id"] = self.access_key_id
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.secret_access_key is not None:
            body["secret_access_key"] = self.secret_access_key
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CloudflareApiToken into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_key_id is not None:
            body["access_key_id"] = self.access_key_id
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.secret_access_key is not None:
            body["secret_access_key"] = self.secret_access_key
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CloudflareApiToken:
        """Deserializes the CloudflareApiToken from a dictionary."""
        return cls(
            access_key_id=d.get("access_key_id", None),
            account_id=d.get("account_id", None),
            secret_access_key=d.get("secret_access_key", None),
        )


@dataclass
class ColumnInfo:
    comment: Optional[str] = None
    """User-provided free-form text description."""

    mask: Optional[ColumnMask] = None

    name: Optional[str] = None
    """Name of Column."""

    nullable: Optional[bool] = None
    """Whether field may be Null (default: true)."""

    partition_index: Optional[int] = None
    """Partition index for column."""

    position: Optional[int] = None
    """Ordinal position of column (starting at position 0)."""

    type_interval_type: Optional[str] = None
    """Format of IntervalType."""

    type_json: Optional[str] = None
    """Full data type specification, JSON-serialized."""

    type_name: Optional[ColumnTypeName] = None

    type_precision: Optional[int] = None
    """Digits of precision; required for DecimalTypes."""

    type_scale: Optional[int] = None
    """Digits to right of decimal; Required for DecimalTypes."""

    type_text: Optional[str] = None
    """Full data type specification as SQL/catalogString text."""

    def as_dict(self) -> dict:
        """Serializes the ColumnInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.mask:
            body["mask"] = self.mask.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.nullable is not None:
            body["nullable"] = self.nullable
        if self.partition_index is not None:
            body["partition_index"] = self.partition_index
        if self.position is not None:
            body["position"] = self.position
        if self.type_interval_type is not None:
            body["type_interval_type"] = self.type_interval_type
        if self.type_json is not None:
            body["type_json"] = self.type_json
        if self.type_name is not None:
            body["type_name"] = self.type_name.value
        if self.type_precision is not None:
            body["type_precision"] = self.type_precision
        if self.type_scale is not None:
            body["type_scale"] = self.type_scale
        if self.type_text is not None:
            body["type_text"] = self.type_text
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ColumnInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.mask:
            body["mask"] = self.mask
        if self.name is not None:
            body["name"] = self.name
        if self.nullable is not None:
            body["nullable"] = self.nullable
        if self.partition_index is not None:
            body["partition_index"] = self.partition_index
        if self.position is not None:
            body["position"] = self.position
        if self.type_interval_type is not None:
            body["type_interval_type"] = self.type_interval_type
        if self.type_json is not None:
            body["type_json"] = self.type_json
        if self.type_name is not None:
            body["type_name"] = self.type_name
        if self.type_precision is not None:
            body["type_precision"] = self.type_precision
        if self.type_scale is not None:
            body["type_scale"] = self.type_scale
        if self.type_text is not None:
            body["type_text"] = self.type_text
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ColumnInfo:
        """Deserializes the ColumnInfo from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            mask=_from_dict(d, "mask", ColumnMask),
            name=d.get("name", None),
            nullable=d.get("nullable", None),
            partition_index=d.get("partition_index", None),
            position=d.get("position", None),
            type_interval_type=d.get("type_interval_type", None),
            type_json=d.get("type_json", None),
            type_name=_enum(d, "type_name", ColumnTypeName),
            type_precision=d.get("type_precision", None),
            type_scale=d.get("type_scale", None),
            type_text=d.get("type_text", None),
        )


@dataclass
class ColumnMask:
    function_name: Optional[str] = None
    """The full name of the column mask SQL UDF."""

    using_arguments: Optional[List[PolicyFunctionArgument]] = None
    """The list of additional table columns or literals to be passed as additional arguments to a
    column mask function. This is the replacement of the deprecated using_column_names field and
    carries information about the types (alias or constant) of the arguments to the mask function."""

    using_column_names: Optional[List[str]] = None
    """The list of additional table columns to be passed as input to the column mask function. The
    first arg of the mask function should be of the type of the column being masked and the types of
    the rest of the args should match the types of columns in 'using_column_names'."""

    def as_dict(self) -> dict:
        """Serializes the ColumnMask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.using_arguments:
            body["using_arguments"] = [v.as_dict() for v in self.using_arguments]
        if self.using_column_names:
            body["using_column_names"] = [v for v in self.using_column_names]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ColumnMask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.using_arguments:
            body["using_arguments"] = self.using_arguments
        if self.using_column_names:
            body["using_column_names"] = self.using_column_names
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ColumnMask:
        """Deserializes the ColumnMask from a dictionary."""
        return cls(
            function_name=d.get("function_name", None),
            using_arguments=_repeated_dict(d, "using_arguments", PolicyFunctionArgument),
            using_column_names=d.get("using_column_names", None),
        )


@dataclass
class ColumnMaskOptions:
    function_name: str
    """The fully qualified name of the column mask function. The function is called on each row of the
    target table. The function's first argument and its return type should match the type of the
    masked column. Required on create and update."""

    on_column: str
    """The alias of the column to be masked. The alias must refer to one of matched columns. The values
    of the column is passed to the column mask function as the first argument. Required on create
    and update."""

    using: Optional[List[FunctionArgument]] = None
    """Optional list of column aliases or constant literals to be passed as additional arguments to the
    column mask function. The type of each column should match the positional argument of the column
    mask function."""

    def as_dict(self) -> dict:
        """Serializes the ColumnMaskOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.on_column is not None:
            body["on_column"] = self.on_column
        if self.using:
            body["using"] = [v.as_dict() for v in self.using]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ColumnMaskOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.on_column is not None:
            body["on_column"] = self.on_column
        if self.using:
            body["using"] = self.using
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ColumnMaskOptions:
        """Deserializes the ColumnMaskOptions from a dictionary."""
        return cls(
            function_name=d.get("function_name", None),
            on_column=d.get("on_column", None),
            using=_repeated_dict(d, "using", FunctionArgument),
        )


@dataclass
class ColumnRelationship:
    source: Optional[str] = None

    target: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ColumnRelationship into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.source is not None:
            body["source"] = self.source
        if self.target is not None:
            body["target"] = self.target
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ColumnRelationship into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.source is not None:
            body["source"] = self.source
        if self.target is not None:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ColumnRelationship:
        """Deserializes the ColumnRelationship from a dictionary."""
        return cls(source=d.get("source", None), target=d.get("target", None))


class ColumnTypeName(Enum):

    ARRAY = "ARRAY"
    BINARY = "BINARY"
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    CHAR = "CHAR"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    DOUBLE = "DOUBLE"
    FLOAT = "FLOAT"
    GEOGRAPHY = "GEOGRAPHY"
    GEOMETRY = "GEOMETRY"
    INT = "INT"
    INTERVAL = "INTERVAL"
    LONG = "LONG"
    MAP = "MAP"
    NULL = "NULL"
    SHORT = "SHORT"
    STRING = "STRING"
    STRUCT = "STRUCT"
    TABLE_TYPE = "TABLE_TYPE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    USER_DEFINED_TYPE = "USER_DEFINED_TYPE"
    VARIANT = "VARIANT"


@dataclass
class ConnectionDependency:
    """A connection that is dependent on a SQL object."""

    connection_name: Optional[str] = None
    """Full name of the dependent connection, in the form of __connection_name__."""

    def as_dict(self) -> dict:
        """Serializes the ConnectionDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ConnectionDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ConnectionDependency:
        """Deserializes the ConnectionDependency from a dictionary."""
        return cls(connection_name=d.get("connection_name", None))


@dataclass
class ConnectionInfo:
    """Next ID: 24"""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    connection_id: Optional[str] = None
    """Unique identifier of the Connection."""

    connection_type: Optional[ConnectionType] = None
    """The type of connection."""

    created_at: Optional[int] = None
    """Time at which this connection was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of connection creator."""

    credential_type: Optional[CredentialType] = None
    """The type of credential."""

    full_name: Optional[str] = None
    """Full name of connection."""

    metastore_id: Optional[str] = None
    """Unique identifier of parent metastore."""

    name: Optional[str] = None
    """Name of the connection."""

    options: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    owner: Optional[str] = None
    """Username of current owner of the connection."""

    properties: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    provisioning_info: Optional[ProvisioningInfo] = None

    read_only: Optional[bool] = None
    """If the connection is read only."""

    securable_type: Optional[SecurableType] = None

    updated_at: Optional[int] = None
    """Time at which this connection was updated, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified connection."""

    url: Optional[str] = None
    """URL of the remote data source, extracted from options."""

    def as_dict(self) -> dict:
        """Serializes the ConnectionInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.connection_id is not None:
            body["connection_id"] = self.connection_id
        if self.connection_type is not None:
            body["connection_type"] = self.connection_type.value
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.credential_type is not None:
            body["credential_type"] = self.credential_type.value
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.provisioning_info:
            body["provisioning_info"] = self.provisioning_info.as_dict()
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type.value
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ConnectionInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.connection_id is not None:
            body["connection_id"] = self.connection_id
        if self.connection_type is not None:
            body["connection_type"] = self.connection_type
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.credential_type is not None:
            body["credential_type"] = self.credential_type
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.provisioning_info:
            body["provisioning_info"] = self.provisioning_info
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ConnectionInfo:
        """Deserializes the ConnectionInfo from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            connection_id=d.get("connection_id", None),
            connection_type=_enum(d, "connection_type", ConnectionType),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            credential_type=_enum(d, "credential_type", CredentialType),
            full_name=d.get("full_name", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            options=d.get("options", None),
            owner=d.get("owner", None),
            properties=d.get("properties", None),
            provisioning_info=_from_dict(d, "provisioning_info", ProvisioningInfo),
            read_only=d.get("read_only", None),
            securable_type=_enum(d, "securable_type", SecurableType),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            url=d.get("url", None),
        )


class ConnectionType(Enum):
    """Next Id: 72"""

    BIGQUERY = "BIGQUERY"
    DATABRICKS = "DATABRICKS"
    GA4_RAW_DATA = "GA4_RAW_DATA"
    GLUE = "GLUE"
    HIVE_METASTORE = "HIVE_METASTORE"
    HTTP = "HTTP"
    MYSQL = "MYSQL"
    ORACLE = "ORACLE"
    POSTGRESQL = "POSTGRESQL"
    POWER_BI = "POWER_BI"
    REDSHIFT = "REDSHIFT"
    SALESFORCE = "SALESFORCE"
    SALESFORCE_DATA_CLOUD = "SALESFORCE_DATA_CLOUD"
    SERVICENOW = "SERVICENOW"
    SNOWFLAKE = "SNOWFLAKE"
    SQLDW = "SQLDW"
    SQLSERVER = "SQLSERVER"
    TERADATA = "TERADATA"
    UNKNOWN_CONNECTION_TYPE = "UNKNOWN_CONNECTION_TYPE"
    WORKDAY_RAAS = "WORKDAY_RAAS"


@dataclass
class ContinuousUpdateStatus:
    """Detailed status of an online table. Shown if the online table is in the ONLINE_CONTINUOUS_UPDATE
    or the ONLINE_UPDATING_PIPELINE_RESOURCES state."""

    initial_pipeline_sync_progress: Optional[PipelineProgress] = None
    """Progress of the initial data synchronization."""

    last_processed_commit_version: Optional[int] = None
    """The last source table Delta version that was synced to the online table. Note that this Delta
    version may not be completely synced to the online table yet."""

    timestamp: Optional[str] = None
    """The timestamp of the last time any data was synchronized from the source table to the online
    table."""

    def as_dict(self) -> dict:
        """Serializes the ContinuousUpdateStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress.as_dict()
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ContinuousUpdateStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ContinuousUpdateStatus:
        """Deserializes the ContinuousUpdateStatus from a dictionary."""
        return cls(
            initial_pipeline_sync_progress=_from_dict(d, "initial_pipeline_sync_progress", PipelineProgress),
            last_processed_commit_version=d.get("last_processed_commit_version", None),
            timestamp=d.get("timestamp", None),
        )


@dataclass
class CreateAccessRequest:
    behalf_of: Optional[Principal] = None
    """Optional. The principal this request is for. Empty `behalf_of` defaults to the requester's
    identity.
    
    Principals must be unique across the API call."""

    comment: Optional[str] = None
    """Optional. Comment associated with the request.
    
    At most 200 characters, can only contain lowercase/uppercase letters (a-z, A-Z), numbers (0-9),
    punctuation, and spaces."""

    securable_permissions: Optional[List[SecurablePermissions]] = None
    """List of securables and their corresponding requested UC privileges.
    
    At most 30 securables can be requested for a principal per batched call. Each securable can only
    be requested once per principal."""

    def as_dict(self) -> dict:
        """Serializes the CreateAccessRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.behalf_of:
            body["behalf_of"] = self.behalf_of.as_dict()
        if self.comment is not None:
            body["comment"] = self.comment
        if self.securable_permissions:
            body["securable_permissions"] = [v.as_dict() for v in self.securable_permissions]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateAccessRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.behalf_of:
            body["behalf_of"] = self.behalf_of
        if self.comment is not None:
            body["comment"] = self.comment
        if self.securable_permissions:
            body["securable_permissions"] = self.securable_permissions
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateAccessRequest:
        """Deserializes the CreateAccessRequest from a dictionary."""
        return cls(
            behalf_of=_from_dict(d, "behalf_of", Principal),
            comment=d.get("comment", None),
            securable_permissions=_repeated_dict(d, "securable_permissions", SecurablePermissions),
        )


@dataclass
class CreateAccessRequestResponse:
    behalf_of: Optional[Principal] = None
    """The principal the request was made on behalf of."""

    request_destinations: Optional[List[AccessRequestDestinations]] = None
    """The access request destinations for all the securables the principal requested."""

    def as_dict(self) -> dict:
        """Serializes the CreateAccessRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.behalf_of:
            body["behalf_of"] = self.behalf_of.as_dict()
        if self.request_destinations:
            body["request_destinations"] = [v.as_dict() for v in self.request_destinations]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateAccessRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.behalf_of:
            body["behalf_of"] = self.behalf_of
        if self.request_destinations:
            body["request_destinations"] = self.request_destinations
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateAccessRequestResponse:
        """Deserializes the CreateAccessRequestResponse from a dictionary."""
        return cls(
            behalf_of=_from_dict(d, "behalf_of", Principal),
            request_destinations=_repeated_dict(d, "request_destinations", AccessRequestDestinations),
        )


@dataclass
class CreateAccountsMetastore:
    name: str
    """The user-specified name of the metastore."""

    external_access_enabled: Optional[bool] = None
    """Whether to allow non-DBR clients to directly access entities under the metastore."""

    region: Optional[str] = None
    """Cloud region which the metastore serves (e.g., `us-west-2`, `westus`)."""

    storage_root: Optional[str] = None
    """The storage root URL for metastore"""

    def as_dict(self) -> dict:
        """Serializes the CreateAccountsMetastore into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.name is not None:
            body["name"] = self.name
        if self.region is not None:
            body["region"] = self.region
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateAccountsMetastore into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.name is not None:
            body["name"] = self.name
        if self.region is not None:
            body["region"] = self.region
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateAccountsMetastore:
        """Deserializes the CreateAccountsMetastore from a dictionary."""
        return cls(
            external_access_enabled=d.get("external_access_enabled", None),
            name=d.get("name", None),
            region=d.get("region", None),
            storage_root=d.get("storage_root", None),
        )


@dataclass
class CreateAccountsStorageCredential:
    name: str
    """The credential name. The name must be unique among storage and service credentials within the
    metastore."""

    aws_iam_role: Optional[AwsIamRoleRequest] = None
    """The AWS IAM role configuration."""

    azure_managed_identity: Optional[AzureManagedIdentityRequest] = None
    """The Azure managed identity configuration."""

    azure_service_principal: Optional[AzureServicePrincipal] = None
    """The Azure service principal configuration."""

    cloudflare_api_token: Optional[CloudflareApiToken] = None
    """The Cloudflare API token configuration."""

    comment: Optional[str] = None
    """Comment associated with the credential."""

    databricks_gcp_service_account: Optional[DatabricksGcpServiceAccountRequest] = None
    """The Databricks managed GCP service account configuration."""

    read_only: Optional[bool] = None
    """Whether the credential is usable only for read operations. Only applicable when purpose is
    **STORAGE**."""

    def as_dict(self) -> dict:
        """Serializes the CreateAccountsStorageCredential into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role.as_dict()
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity.as_dict()
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal.as_dict()
        if self.cloudflare_api_token:
            body["cloudflare_api_token"] = self.cloudflare_api_token.as_dict()
        if self.comment is not None:
            body["comment"] = self.comment
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.read_only is not None:
            body["read_only"] = self.read_only
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateAccountsStorageCredential into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal
        if self.cloudflare_api_token:
            body["cloudflare_api_token"] = self.cloudflare_api_token
        if self.comment is not None:
            body["comment"] = self.comment
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account
        if self.name is not None:
            body["name"] = self.name
        if self.read_only is not None:
            body["read_only"] = self.read_only
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateAccountsStorageCredential:
        """Deserializes the CreateAccountsStorageCredential from a dictionary."""
        return cls(
            aws_iam_role=_from_dict(d, "aws_iam_role", AwsIamRoleRequest),
            azure_managed_identity=_from_dict(d, "azure_managed_identity", AzureManagedIdentityRequest),
            azure_service_principal=_from_dict(d, "azure_service_principal", AzureServicePrincipal),
            cloudflare_api_token=_from_dict(d, "cloudflare_api_token", CloudflareApiToken),
            comment=d.get("comment", None),
            databricks_gcp_service_account=_from_dict(
                d, "databricks_gcp_service_account", DatabricksGcpServiceAccountRequest
            ),
            name=d.get("name", None),
            read_only=d.get("read_only", None),
        )


@dataclass
class CreateFunction:
    name: str
    """Name of function, relative to parent schema."""

    catalog_name: str
    """Name of parent Catalog."""

    schema_name: str
    """Name of parent Schema relative to its parent Catalog."""

    input_params: FunctionParameterInfos
    """Function input parameters."""

    data_type: ColumnTypeName
    """Scalar function return data type."""

    full_data_type: str
    """Pretty printed function data type."""

    routine_body: CreateFunctionRoutineBody
    """Function language. When **EXTERNAL** is used, the language of the routine function should be
    specified in the **external_language** field, and the **return_params** of the function cannot
    be used (as **TABLE** return type is not supported), and the **sql_data_access** field must be
    **NO_SQL**."""

    routine_definition: str
    """Function body."""

    parameter_style: CreateFunctionParameterStyle
    """Function parameter style. **S** is the value for SQL."""

    is_deterministic: bool
    """Whether the function is deterministic."""

    sql_data_access: CreateFunctionSqlDataAccess
    """Function SQL data access."""

    is_null_call: bool
    """Function null call."""

    security_type: CreateFunctionSecurityType
    """Function security type."""

    specific_name: str
    """Specific name of the function; Reserved for future use."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    external_language: Optional[str] = None
    """External function language."""

    external_name: Optional[str] = None
    """External function name."""

    properties: Optional[str] = None
    """JSON-serialized key-value pair map, encoded (escaped) as a string."""

    return_params: Optional[FunctionParameterInfos] = None
    """Table function return parameters."""

    routine_dependencies: Optional[DependencyList] = None
    """function dependencies."""

    sql_path: Optional[str] = None
    """List of schemes whose objects can be referenced without qualification."""

    def as_dict(self) -> dict:
        """Serializes the CreateFunction into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.data_type is not None:
            body["data_type"] = self.data_type.value
        if self.external_language is not None:
            body["external_language"] = self.external_language
        if self.external_name is not None:
            body["external_name"] = self.external_name
        if self.full_data_type is not None:
            body["full_data_type"] = self.full_data_type
        if self.input_params:
            body["input_params"] = self.input_params.as_dict()
        if self.is_deterministic is not None:
            body["is_deterministic"] = self.is_deterministic
        if self.is_null_call is not None:
            body["is_null_call"] = self.is_null_call
        if self.name is not None:
            body["name"] = self.name
        if self.parameter_style is not None:
            body["parameter_style"] = self.parameter_style.value
        if self.properties is not None:
            body["properties"] = self.properties
        if self.return_params:
            body["return_params"] = self.return_params.as_dict()
        if self.routine_body is not None:
            body["routine_body"] = self.routine_body.value
        if self.routine_definition is not None:
            body["routine_definition"] = self.routine_definition
        if self.routine_dependencies:
            body["routine_dependencies"] = self.routine_dependencies.as_dict()
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.security_type is not None:
            body["security_type"] = self.security_type.value
        if self.specific_name is not None:
            body["specific_name"] = self.specific_name
        if self.sql_data_access is not None:
            body["sql_data_access"] = self.sql_data_access.value
        if self.sql_path is not None:
            body["sql_path"] = self.sql_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateFunction into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.data_type is not None:
            body["data_type"] = self.data_type
        if self.external_language is not None:
            body["external_language"] = self.external_language
        if self.external_name is not None:
            body["external_name"] = self.external_name
        if self.full_data_type is not None:
            body["full_data_type"] = self.full_data_type
        if self.input_params:
            body["input_params"] = self.input_params
        if self.is_deterministic is not None:
            body["is_deterministic"] = self.is_deterministic
        if self.is_null_call is not None:
            body["is_null_call"] = self.is_null_call
        if self.name is not None:
            body["name"] = self.name
        if self.parameter_style is not None:
            body["parameter_style"] = self.parameter_style
        if self.properties is not None:
            body["properties"] = self.properties
        if self.return_params:
            body["return_params"] = self.return_params
        if self.routine_body is not None:
            body["routine_body"] = self.routine_body
        if self.routine_definition is not None:
            body["routine_definition"] = self.routine_definition
        if self.routine_dependencies:
            body["routine_dependencies"] = self.routine_dependencies
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.security_type is not None:
            body["security_type"] = self.security_type
        if self.specific_name is not None:
            body["specific_name"] = self.specific_name
        if self.sql_data_access is not None:
            body["sql_data_access"] = self.sql_data_access
        if self.sql_path is not None:
            body["sql_path"] = self.sql_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateFunction:
        """Deserializes the CreateFunction from a dictionary."""
        return cls(
            catalog_name=d.get("catalog_name", None),
            comment=d.get("comment", None),
            data_type=_enum(d, "data_type", ColumnTypeName),
            external_language=d.get("external_language", None),
            external_name=d.get("external_name", None),
            full_data_type=d.get("full_data_type", None),
            input_params=_from_dict(d, "input_params", FunctionParameterInfos),
            is_deterministic=d.get("is_deterministic", None),
            is_null_call=d.get("is_null_call", None),
            name=d.get("name", None),
            parameter_style=_enum(d, "parameter_style", CreateFunctionParameterStyle),
            properties=d.get("properties", None),
            return_params=_from_dict(d, "return_params", FunctionParameterInfos),
            routine_body=_enum(d, "routine_body", CreateFunctionRoutineBody),
            routine_definition=d.get("routine_definition", None),
            routine_dependencies=_from_dict(d, "routine_dependencies", DependencyList),
            schema_name=d.get("schema_name", None),
            security_type=_enum(d, "security_type", CreateFunctionSecurityType),
            specific_name=d.get("specific_name", None),
            sql_data_access=_enum(d, "sql_data_access", CreateFunctionSqlDataAccess),
            sql_path=d.get("sql_path", None),
        )


class CreateFunctionParameterStyle(Enum):

    S = "S"


class CreateFunctionRoutineBody(Enum):

    EXTERNAL = "EXTERNAL"
    SQL = "SQL"


class CreateFunctionSecurityType(Enum):

    DEFINER = "DEFINER"


class CreateFunctionSqlDataAccess(Enum):

    CONTAINS_SQL = "CONTAINS_SQL"
    NO_SQL = "NO_SQL"
    READS_SQL_DATA = "READS_SQL_DATA"


@dataclass
class CreateMetastoreAssignment:
    workspace_id: int
    """A workspace ID."""

    metastore_id: str
    """The unique ID of the metastore."""

    default_catalog_name: str
    """The name of the default catalog in the metastore. This field is deprecated. Please use "Default
    Namespace API" to configure the default catalog for a Databricks workspace."""

    def as_dict(self) -> dict:
        """Serializes the CreateMetastoreAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default_catalog_name is not None:
            body["default_catalog_name"] = self.default_catalog_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateMetastoreAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default_catalog_name is not None:
            body["default_catalog_name"] = self.default_catalog_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateMetastoreAssignment:
        """Deserializes the CreateMetastoreAssignment from a dictionary."""
        return cls(
            default_catalog_name=d.get("default_catalog_name", None),
            metastore_id=d.get("metastore_id", None),
            workspace_id=d.get("workspace_id", None),
        )


@dataclass
class CreateRequestExternalLineage:
    source: ExternalLineageObject
    """Source object of the external lineage relationship."""

    target: ExternalLineageObject
    """Target object of the external lineage relationship."""

    columns: Optional[List[ColumnRelationship]] = None
    """List of column relationships between source and target objects."""

    id: Optional[str] = None
    """Unique identifier of the external lineage relationship."""

    properties: Optional[Dict[str, str]] = None
    """Key-value properties associated with the external lineage relationship."""

    def as_dict(self) -> dict:
        """Serializes the CreateRequestExternalLineage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source.as_dict()
        if self.target:
            body["target"] = self.target.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateRequestExternalLineage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.columns:
            body["columns"] = self.columns
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source
        if self.target:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateRequestExternalLineage:
        """Deserializes the CreateRequestExternalLineage from a dictionary."""
        return cls(
            columns=_repeated_dict(d, "columns", ColumnRelationship),
            id=d.get("id", None),
            properties=d.get("properties", None),
            source=_from_dict(d, "source", ExternalLineageObject),
            target=_from_dict(d, "target", ExternalLineageObject),
        )


@dataclass
class CredentialDependency:
    """A credential that is dependent on a SQL object."""

    credential_name: Optional[str] = None
    """Full name of the dependent credential, in the form of __credential_name__."""

    def as_dict(self) -> dict:
        """Serializes the CredentialDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_name is not None:
            body["credential_name"] = self.credential_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CredentialDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_name is not None:
            body["credential_name"] = self.credential_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CredentialDependency:
        """Deserializes the CredentialDependency from a dictionary."""
        return cls(credential_name=d.get("credential_name", None))


@dataclass
class CredentialInfo:
    aws_iam_role: Optional[AwsIamRole] = None
    """The AWS IAM role configuration."""

    azure_managed_identity: Optional[AzureManagedIdentity] = None
    """The Azure managed identity configuration."""

    azure_service_principal: Optional[AzureServicePrincipal] = None
    """The Azure service principal configuration."""

    comment: Optional[str] = None
    """Comment associated with the credential."""

    created_at: Optional[int] = None
    """Time at which this credential was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of credential creator."""

    databricks_gcp_service_account: Optional[DatabricksGcpServiceAccount] = None
    """The Databricks managed GCP service account configuration."""

    full_name: Optional[str] = None
    """The full name of the credential."""

    id: Optional[str] = None
    """The unique identifier of the credential."""

    isolation_mode: Optional[IsolationMode] = None
    """Whether the current securable is accessible from all workspaces or a specific set of workspaces."""

    metastore_id: Optional[str] = None
    """Unique identifier of the parent metastore."""

    name: Optional[str] = None
    """The credential name. The name must be unique among storage and service credentials within the
    metastore."""

    owner: Optional[str] = None
    """Username of current owner of credential."""

    purpose: Optional[CredentialPurpose] = None
    """Indicates the purpose of the credential."""

    read_only: Optional[bool] = None
    """Whether the credential is usable only for read operations. Only applicable when purpose is
    **STORAGE**."""

    updated_at: Optional[int] = None
    """Time at which this credential was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified the credential."""

    used_for_managed_storage: Optional[bool] = None
    """Whether this credential is the current metastore's root storage credential. Only applicable when
    purpose is **STORAGE**."""

    def as_dict(self) -> dict:
        """Serializes the CredentialInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role.as_dict()
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity.as_dict()
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal.as_dict()
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account.as_dict()
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.id is not None:
            body["id"] = self.id
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode.value
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.purpose is not None:
            body["purpose"] = self.purpose.value
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.used_for_managed_storage is not None:
            body["used_for_managed_storage"] = self.used_for_managed_storage
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CredentialInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.id is not None:
            body["id"] = self.id
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.purpose is not None:
            body["purpose"] = self.purpose
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.used_for_managed_storage is not None:
            body["used_for_managed_storage"] = self.used_for_managed_storage
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CredentialInfo:
        """Deserializes the CredentialInfo from a dictionary."""
        return cls(
            aws_iam_role=_from_dict(d, "aws_iam_role", AwsIamRole),
            azure_managed_identity=_from_dict(d, "azure_managed_identity", AzureManagedIdentity),
            azure_service_principal=_from_dict(d, "azure_service_principal", AzureServicePrincipal),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            databricks_gcp_service_account=_from_dict(d, "databricks_gcp_service_account", DatabricksGcpServiceAccount),
            full_name=d.get("full_name", None),
            id=d.get("id", None),
            isolation_mode=_enum(d, "isolation_mode", IsolationMode),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            purpose=_enum(d, "purpose", CredentialPurpose),
            read_only=d.get("read_only", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            used_for_managed_storage=d.get("used_for_managed_storage", None),
        )


class CredentialPurpose(Enum):

    SERVICE = "SERVICE"
    STORAGE = "STORAGE"


class CredentialType(Enum):
    """Next Id: 16"""

    ANY_STATIC_CREDENTIAL = "ANY_STATIC_CREDENTIAL"
    BEARER_TOKEN = "BEARER_TOKEN"
    EDGEGRID_AKAMAI = "EDGEGRID_AKAMAI"
    OAUTH_ACCESS_TOKEN = "OAUTH_ACCESS_TOKEN"
    OAUTH_M2M = "OAUTH_M2M"
    OAUTH_MTLS = "OAUTH_MTLS"
    OAUTH_REFRESH_TOKEN = "OAUTH_REFRESH_TOKEN"
    OAUTH_RESOURCE_OWNER_PASSWORD = "OAUTH_RESOURCE_OWNER_PASSWORD"
    OAUTH_U2M = "OAUTH_U2M"
    OAUTH_U2M_MAPPING = "OAUTH_U2M_MAPPING"
    OIDC_TOKEN = "OIDC_TOKEN"
    PEM_PRIVATE_KEY = "PEM_PRIVATE_KEY"
    SERVICE_CREDENTIAL = "SERVICE_CREDENTIAL"
    SSWS_TOKEN = "SSWS_TOKEN"
    UNKNOWN_CREDENTIAL_TYPE = "UNKNOWN_CREDENTIAL_TYPE"
    USERNAME_PASSWORD = "USERNAME_PASSWORD"


@dataclass
class CredentialValidationResult:
    message: Optional[str] = None
    """Error message would exist when the result does not equal to **PASS**."""

    result: Optional[ValidateCredentialResult] = None
    """The results of the tested operation."""

    def as_dict(self) -> dict:
        """Serializes the CredentialValidationResult into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.result is not None:
            body["result"] = self.result.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CredentialValidationResult into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.result is not None:
            body["result"] = self.result
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CredentialValidationResult:
        """Deserializes the CredentialValidationResult from a dictionary."""
        return cls(message=d.get("message", None), result=_enum(d, "result", ValidateCredentialResult))


class DataSourceFormat(Enum):
    """Data source format"""

    AVRO = "AVRO"
    BIGQUERY_FORMAT = "BIGQUERY_FORMAT"
    CSV = "CSV"
    DATABRICKS_FORMAT = "DATABRICKS_FORMAT"
    DATABRICKS_ROW_STORE_FORMAT = "DATABRICKS_ROW_STORE_FORMAT"
    DELTA = "DELTA"
    DELTASHARING = "DELTASHARING"
    DELTA_UNIFORM_HUDI = "DELTA_UNIFORM_HUDI"
    DELTA_UNIFORM_ICEBERG = "DELTA_UNIFORM_ICEBERG"
    HIVE = "HIVE"
    ICEBERG = "ICEBERG"
    JSON = "JSON"
    MONGODB_FORMAT = "MONGODB_FORMAT"
    MYSQL_FORMAT = "MYSQL_FORMAT"
    NETSUITE_FORMAT = "NETSUITE_FORMAT"
    ORACLE_FORMAT = "ORACLE_FORMAT"
    ORC = "ORC"
    PARQUET = "PARQUET"
    POSTGRESQL_FORMAT = "POSTGRESQL_FORMAT"
    REDSHIFT_FORMAT = "REDSHIFT_FORMAT"
    SALESFORCE_DATA_CLOUD_FORMAT = "SALESFORCE_DATA_CLOUD_FORMAT"
    SALESFORCE_FORMAT = "SALESFORCE_FORMAT"
    SNOWFLAKE_FORMAT = "SNOWFLAKE_FORMAT"
    SQLDW_FORMAT = "SQLDW_FORMAT"
    SQLSERVER_FORMAT = "SQLSERVER_FORMAT"
    TERADATA_FORMAT = "TERADATA_FORMAT"
    TEXT = "TEXT"
    UNITY_CATALOG = "UNITY_CATALOG"
    VECTOR_INDEX_FORMAT = "VECTOR_INDEX_FORMAT"
    WORKDAY_RAAS_FORMAT = "WORKDAY_RAAS_FORMAT"


@dataclass
class DatabricksGcpServiceAccount:
    """GCP long-lived credential. Databricks-created Google Cloud Storage service account."""

    credential_id: Optional[str] = None
    """The Databricks internal ID that represents this managed identity."""

    email: Optional[str] = None
    """The email of the service account."""

    private_key_id: Optional[str] = None
    """The ID that represents the private key for this Service Account"""

    def as_dict(self) -> dict:
        """Serializes the DatabricksGcpServiceAccount into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.email is not None:
            body["email"] = self.email
        if self.private_key_id is not None:
            body["private_key_id"] = self.private_key_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabricksGcpServiceAccount into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.email is not None:
            body["email"] = self.email
        if self.private_key_id is not None:
            body["private_key_id"] = self.private_key_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabricksGcpServiceAccount:
        """Deserializes the DatabricksGcpServiceAccount from a dictionary."""
        return cls(
            credential_id=d.get("credential_id", None),
            email=d.get("email", None),
            private_key_id=d.get("private_key_id", None),
        )


@dataclass
class DatabricksGcpServiceAccountRequest:
    """GCP long-lived credential. Databricks-created Google Cloud Storage service account."""

    def as_dict(self) -> dict:
        """Serializes the DatabricksGcpServiceAccountRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabricksGcpServiceAccountRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabricksGcpServiceAccountRequest:
        """Deserializes the DatabricksGcpServiceAccountRequest from a dictionary."""
        return cls()


@dataclass
class DatabricksGcpServiceAccountResponse:
    """GCP long-lived credential. Databricks-created Google Cloud Storage service account."""

    credential_id: Optional[str] = None
    """The Databricks internal ID that represents this managed identity."""

    email: Optional[str] = None
    """The email of the service account."""

    def as_dict(self) -> dict:
        """Serializes the DatabricksGcpServiceAccountResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.email is not None:
            body["email"] = self.email
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabricksGcpServiceAccountResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.email is not None:
            body["email"] = self.email
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabricksGcpServiceAccountResponse:
        """Deserializes the DatabricksGcpServiceAccountResponse from a dictionary."""
        return cls(credential_id=d.get("credential_id", None), email=d.get("email", None))


@dataclass
class DeleteCredentialResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteCredentialResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteCredentialResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteCredentialResponse:
        """Deserializes the DeleteCredentialResponse from a dictionary."""
        return cls()


@dataclass
class DeleteMonitorResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteMonitorResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteMonitorResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteMonitorResponse:
        """Deserializes the DeleteMonitorResponse from a dictionary."""
        return cls()


@dataclass
class DeletePolicyResponse:
    def as_dict(self) -> dict:
        """Serializes the DeletePolicyResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeletePolicyResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeletePolicyResponse:
        """Deserializes the DeletePolicyResponse from a dictionary."""
        return cls()


@dataclass
class DeleteRequestExternalLineage:
    source: ExternalLineageObject
    """Source object of the external lineage relationship."""

    target: ExternalLineageObject
    """Target object of the external lineage relationship."""

    id: Optional[str] = None
    """Unique identifier of the external lineage relationship."""

    def as_dict(self) -> dict:
        """Serializes the DeleteRequestExternalLineage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.source:
            body["source"] = self.source.as_dict()
        if self.target:
            body["target"] = self.target.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteRequestExternalLineage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.source:
            body["source"] = self.source
        if self.target:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteRequestExternalLineage:
        """Deserializes the DeleteRequestExternalLineage from a dictionary."""
        return cls(
            id=d.get("id", None),
            source=_from_dict(d, "source", ExternalLineageObject),
            target=_from_dict(d, "target", ExternalLineageObject),
        )


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
class DeleteTableConstraintResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteTableConstraintResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteTableConstraintResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteTableConstraintResponse:
        """Deserializes the DeleteTableConstraintResponse from a dictionary."""
        return cls()


@dataclass
class DeltaRuntimePropertiesKvPairs:
    """Properties pertaining to the current state of the delta table as given by the commit server.
    This does not contain **delta.*** (input) properties in __TableInfo.properties__."""

    delta_runtime_properties: Dict[str, str]
    """A map of key-value properties attached to the securable."""

    def as_dict(self) -> dict:
        """Serializes the DeltaRuntimePropertiesKvPairs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.delta_runtime_properties:
            body["delta_runtime_properties"] = self.delta_runtime_properties
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaRuntimePropertiesKvPairs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.delta_runtime_properties:
            body["delta_runtime_properties"] = self.delta_runtime_properties
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaRuntimePropertiesKvPairs:
        """Deserializes the DeltaRuntimePropertiesKvPairs from a dictionary."""
        return cls(delta_runtime_properties=d.get("delta_runtime_properties", None))


class DeltaSharingScopeEnum(Enum):

    INTERNAL = "INTERNAL"
    INTERNAL_AND_EXTERNAL = "INTERNAL_AND_EXTERNAL"


@dataclass
class Dependency:
    """A dependency of a SQL object. One of the following fields must be defined: __table__,
    __function__, __connection__, or __credential__."""

    connection: Optional[ConnectionDependency] = None

    credential: Optional[CredentialDependency] = None

    function: Optional[FunctionDependency] = None

    table: Optional[TableDependency] = None

    def as_dict(self) -> dict:
        """Serializes the Dependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection:
            body["connection"] = self.connection.as_dict()
        if self.credential:
            body["credential"] = self.credential.as_dict()
        if self.function:
            body["function"] = self.function.as_dict()
        if self.table:
            body["table"] = self.table.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Dependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection:
            body["connection"] = self.connection
        if self.credential:
            body["credential"] = self.credential
        if self.function:
            body["function"] = self.function
        if self.table:
            body["table"] = self.table
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Dependency:
        """Deserializes the Dependency from a dictionary."""
        return cls(
            connection=_from_dict(d, "connection", ConnectionDependency),
            credential=_from_dict(d, "credential", CredentialDependency),
            function=_from_dict(d, "function", FunctionDependency),
            table=_from_dict(d, "table", TableDependency),
        )


@dataclass
class DependencyList:
    """A list of dependencies."""

    dependencies: Optional[List[Dependency]] = None
    """Array of dependencies."""

    def as_dict(self) -> dict:
        """Serializes the DependencyList into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dependencies:
            body["dependencies"] = [v.as_dict() for v in self.dependencies]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DependencyList into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dependencies:
            body["dependencies"] = self.dependencies
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DependencyList:
        """Deserializes the DependencyList from a dictionary."""
        return cls(dependencies=_repeated_dict(d, "dependencies", Dependency))


class DestinationType(Enum):

    EMAIL = "EMAIL"
    GENERIC_WEBHOOK = "GENERIC_WEBHOOK"
    MICROSOFT_TEAMS = "MICROSOFT_TEAMS"
    SLACK = "SLACK"
    URL = "URL"


@dataclass
class DisableResponse:
    def as_dict(self) -> dict:
        """Serializes the DisableResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DisableResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DisableResponse:
        """Deserializes the DisableResponse from a dictionary."""
        return cls()


@dataclass
class EffectivePermissionsList:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    privilege_assignments: Optional[List[EffectivePrivilegeAssignment]] = None
    """The privileges conveyed to each principal (either directly or via inheritance)"""

    def as_dict(self) -> dict:
        """Serializes the EffectivePermissionsList into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.privilege_assignments:
            body["privilege_assignments"] = [v.as_dict() for v in self.privilege_assignments]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EffectivePermissionsList into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.privilege_assignments:
            body["privilege_assignments"] = self.privilege_assignments
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EffectivePermissionsList:
        """Deserializes the EffectivePermissionsList from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            privilege_assignments=_repeated_dict(d, "privilege_assignments", EffectivePrivilegeAssignment),
        )


@dataclass
class EffectivePredictiveOptimizationFlag:
    value: EnablePredictiveOptimization
    """Whether predictive optimization should be enabled for this object and objects under it."""

    inherited_from_name: Optional[str] = None
    """The name of the object from which the flag was inherited. If there was no inheritance, this
    field is left blank."""

    inherited_from_type: Optional[EffectivePredictiveOptimizationFlagInheritedFromType] = None
    """The type of the object from which the flag was inherited. If there was no inheritance, this
    field is left blank."""

    def as_dict(self) -> dict:
        """Serializes the EffectivePredictiveOptimizationFlag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited_from_name is not None:
            body["inherited_from_name"] = self.inherited_from_name
        if self.inherited_from_type is not None:
            body["inherited_from_type"] = self.inherited_from_type.value
        if self.value is not None:
            body["value"] = self.value.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EffectivePredictiveOptimizationFlag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited_from_name is not None:
            body["inherited_from_name"] = self.inherited_from_name
        if self.inherited_from_type is not None:
            body["inherited_from_type"] = self.inherited_from_type
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EffectivePredictiveOptimizationFlag:
        """Deserializes the EffectivePredictiveOptimizationFlag from a dictionary."""
        return cls(
            inherited_from_name=d.get("inherited_from_name", None),
            inherited_from_type=_enum(d, "inherited_from_type", EffectivePredictiveOptimizationFlagInheritedFromType),
            value=_enum(d, "value", EnablePredictiveOptimization),
        )


class EffectivePredictiveOptimizationFlagInheritedFromType(Enum):

    CATALOG = "CATALOG"
    SCHEMA = "SCHEMA"


@dataclass
class EffectivePrivilege:
    inherited_from_name: Optional[str] = None
    """The full name of the object that conveys this privilege via inheritance. This field is omitted
    when privilege is not inherited (it's assigned to the securable itself)."""

    inherited_from_type: Optional[SecurableType] = None
    """The type of the object that conveys this privilege via inheritance. This field is omitted when
    privilege is not inherited (it's assigned to the securable itself)."""

    privilege: Optional[Privilege] = None
    """The privilege assigned to the principal."""

    def as_dict(self) -> dict:
        """Serializes the EffectivePrivilege into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited_from_name is not None:
            body["inherited_from_name"] = self.inherited_from_name
        if self.inherited_from_type is not None:
            body["inherited_from_type"] = self.inherited_from_type.value
        if self.privilege is not None:
            body["privilege"] = self.privilege.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EffectivePrivilege into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited_from_name is not None:
            body["inherited_from_name"] = self.inherited_from_name
        if self.inherited_from_type is not None:
            body["inherited_from_type"] = self.inherited_from_type
        if self.privilege is not None:
            body["privilege"] = self.privilege
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EffectivePrivilege:
        """Deserializes the EffectivePrivilege from a dictionary."""
        return cls(
            inherited_from_name=d.get("inherited_from_name", None),
            inherited_from_type=_enum(d, "inherited_from_type", SecurableType),
            privilege=_enum(d, "privilege", Privilege),
        )


@dataclass
class EffectivePrivilegeAssignment:
    principal: Optional[str] = None
    """The principal (user email address or group name)."""

    privileges: Optional[List[EffectivePrivilege]] = None
    """The privileges conveyed to the principal (either directly or via inheritance)."""

    def as_dict(self) -> dict:
        """Serializes the EffectivePrivilegeAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.principal is not None:
            body["principal"] = self.principal
        if self.privileges:
            body["privileges"] = [v.as_dict() for v in self.privileges]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EffectivePrivilegeAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.principal is not None:
            body["principal"] = self.principal
        if self.privileges:
            body["privileges"] = self.privileges
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EffectivePrivilegeAssignment:
        """Deserializes the EffectivePrivilegeAssignment from a dictionary."""
        return cls(principal=d.get("principal", None), privileges=_repeated_dict(d, "privileges", EffectivePrivilege))


class EnablePredictiveOptimization(Enum):

    DISABLE = "DISABLE"
    ENABLE = "ENABLE"
    INHERIT = "INHERIT"


@dataclass
class EnableResponse:
    def as_dict(self) -> dict:
        """Serializes the EnableResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnableResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnableResponse:
        """Deserializes the EnableResponse from a dictionary."""
        return cls()


@dataclass
class EncryptionDetails:
    """Encryption options that apply to clients connecting to cloud storage."""

    sse_encryption_details: Optional[SseEncryptionDetails] = None
    """Server-Side Encryption properties for clients communicating with AWS s3."""

    def as_dict(self) -> dict:
        """Serializes the EncryptionDetails into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.sse_encryption_details:
            body["sse_encryption_details"] = self.sse_encryption_details.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EncryptionDetails into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.sse_encryption_details:
            body["sse_encryption_details"] = self.sse_encryption_details
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EncryptionDetails:
        """Deserializes the EncryptionDetails from a dictionary."""
        return cls(sse_encryption_details=_from_dict(d, "sse_encryption_details", SseEncryptionDetails))


@dataclass
class EntityTagAssignment:
    """Represents a tag assignment to an entity"""

    entity_name: str
    """The fully qualified name of the entity to which the tag is assigned"""

    tag_key: str
    """The key of the tag"""

    entity_type: str
    """The type of the entity to which the tag is assigned. Allowed values are: catalogs, schemas,
    tables, columns, volumes."""

    source_type: Optional[TagAssignmentSourceType] = None
    """The source type of the tag assignment, e.g., user-assigned or system-assigned"""

    tag_value: Optional[str] = None
    """The value of the tag"""

    update_time: Optional[Timestamp] = None
    """The timestamp when the tag assignment was last updated"""

    updated_by: Optional[str] = None
    """The user or principal who updated the tag assignment"""

    def as_dict(self) -> dict:
        """Serializes the EntityTagAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.entity_name is not None:
            body["entity_name"] = self.entity_name
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.source_type is not None:
            body["source_type"] = self.source_type.value
        if self.tag_key is not None:
            body["tag_key"] = self.tag_key
        if self.tag_value is not None:
            body["tag_value"] = self.tag_value
        if self.update_time is not None:
            body["update_time"] = self.update_time.ToJsonString()
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EntityTagAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.entity_name is not None:
            body["entity_name"] = self.entity_name
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.source_type is not None:
            body["source_type"] = self.source_type
        if self.tag_key is not None:
            body["tag_key"] = self.tag_key
        if self.tag_value is not None:
            body["tag_value"] = self.tag_value
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EntityTagAssignment:
        """Deserializes the EntityTagAssignment from a dictionary."""
        return cls(
            entity_name=d.get("entity_name", None),
            entity_type=d.get("entity_type", None),
            source_type=_enum(d, "source_type", TagAssignmentSourceType),
            tag_key=d.get("tag_key", None),
            tag_value=d.get("tag_value", None),
            update_time=_timestamp(d, "update_time"),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class ExternalLineageExternalMetadata:
    name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageExternalMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageExternalMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageExternalMetadata:
        """Deserializes the ExternalLineageExternalMetadata from a dictionary."""
        return cls(name=d.get("name", None))


@dataclass
class ExternalLineageExternalMetadataInfo:
    """Represents the external metadata object in the lineage event."""

    entity_type: Optional[str] = None
    """Type of entity represented by the external metadata object."""

    event_time: Optional[str] = None
    """Timestamp of the lineage event."""

    name: Optional[str] = None
    """Name of the external metadata object."""

    system_type: Optional[SystemType] = None
    """Type of external system."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageExternalMetadataInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.name is not None:
            body["name"] = self.name
        if self.system_type is not None:
            body["system_type"] = self.system_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageExternalMetadataInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.name is not None:
            body["name"] = self.name
        if self.system_type is not None:
            body["system_type"] = self.system_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageExternalMetadataInfo:
        """Deserializes the ExternalLineageExternalMetadataInfo from a dictionary."""
        return cls(
            entity_type=d.get("entity_type", None),
            event_time=d.get("event_time", None),
            name=d.get("name", None),
            system_type=_enum(d, "system_type", SystemType),
        )


@dataclass
class ExternalLineageFileInfo:
    """Represents the path information in the lineage event."""

    event_time: Optional[str] = None
    """Timestamp of the lineage event."""

    path: Optional[str] = None
    """URL of the path."""

    securable_name: Optional[str] = None
    """The full name of the securable on the path."""

    securable_type: Optional[str] = None
    """The securable type of the securable on the path."""

    storage_location: Optional[str] = None
    """The storage location associated with securable on the path."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageFileInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.path is not None:
            body["path"] = self.path
        if self.securable_name is not None:
            body["securable_name"] = self.securable_name
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageFileInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.path is not None:
            body["path"] = self.path
        if self.securable_name is not None:
            body["securable_name"] = self.securable_name
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageFileInfo:
        """Deserializes the ExternalLineageFileInfo from a dictionary."""
        return cls(
            event_time=d.get("event_time", None),
            path=d.get("path", None),
            securable_name=d.get("securable_name", None),
            securable_type=d.get("securable_type", None),
            storage_location=d.get("storage_location", None),
        )


@dataclass
class ExternalLineageInfo:
    """Lineage response containing lineage information of a data asset."""

    external_lineage_info: Optional[ExternalLineageRelationshipInfo] = None
    """Information about the edge metadata of the external lineage relationship."""

    external_metadata_info: Optional[ExternalLineageExternalMetadataInfo] = None
    """Information about external metadata involved in the lineage relationship."""

    file_info: Optional[ExternalLineageFileInfo] = None
    """Information about the file involved in the lineage relationship."""

    model_info: Optional[ExternalLineageModelVersionInfo] = None
    """Information about the model version involved in the lineage relationship."""

    table_info: Optional[ExternalLineageTableInfo] = None
    """Information about the table involved in the lineage relationship."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_lineage_info:
            body["external_lineage_info"] = self.external_lineage_info.as_dict()
        if self.external_metadata_info:
            body["external_metadata_info"] = self.external_metadata_info.as_dict()
        if self.file_info:
            body["file_info"] = self.file_info.as_dict()
        if self.model_info:
            body["model_info"] = self.model_info.as_dict()
        if self.table_info:
            body["table_info"] = self.table_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_lineage_info:
            body["external_lineage_info"] = self.external_lineage_info
        if self.external_metadata_info:
            body["external_metadata_info"] = self.external_metadata_info
        if self.file_info:
            body["file_info"] = self.file_info
        if self.model_info:
            body["model_info"] = self.model_info
        if self.table_info:
            body["table_info"] = self.table_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageInfo:
        """Deserializes the ExternalLineageInfo from a dictionary."""
        return cls(
            external_lineage_info=_from_dict(d, "external_lineage_info", ExternalLineageRelationshipInfo),
            external_metadata_info=_from_dict(d, "external_metadata_info", ExternalLineageExternalMetadataInfo),
            file_info=_from_dict(d, "file_info", ExternalLineageFileInfo),
            model_info=_from_dict(d, "model_info", ExternalLineageModelVersionInfo),
            table_info=_from_dict(d, "table_info", ExternalLineageTableInfo),
        )


@dataclass
class ExternalLineageModelVersion:
    name: Optional[str] = None

    version: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageModelVersion into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageModelVersion into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageModelVersion:
        """Deserializes the ExternalLineageModelVersion from a dictionary."""
        return cls(name=d.get("name", None), version=d.get("version", None))


@dataclass
class ExternalLineageModelVersionInfo:
    """Represents the model version information in the lineage event."""

    event_time: Optional[str] = None
    """Timestamp of the lineage event."""

    model_name: Optional[str] = None
    """Name of the model."""

    version: Optional[int] = None
    """Version number of the model."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageModelVersionInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageModelVersionInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageModelVersionInfo:
        """Deserializes the ExternalLineageModelVersionInfo from a dictionary."""
        return cls(
            event_time=d.get("event_time", None), model_name=d.get("model_name", None), version=d.get("version", None)
        )


@dataclass
class ExternalLineageObject:
    external_metadata: Optional[ExternalLineageExternalMetadata] = None

    model_version: Optional[ExternalLineageModelVersion] = None

    path: Optional[ExternalLineagePath] = None

    table: Optional[ExternalLineageTable] = None

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageObject into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_metadata:
            body["external_metadata"] = self.external_metadata.as_dict()
        if self.model_version:
            body["model_version"] = self.model_version.as_dict()
        if self.path:
            body["path"] = self.path.as_dict()
        if self.table:
            body["table"] = self.table.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageObject into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_metadata:
            body["external_metadata"] = self.external_metadata
        if self.model_version:
            body["model_version"] = self.model_version
        if self.path:
            body["path"] = self.path
        if self.table:
            body["table"] = self.table
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageObject:
        """Deserializes the ExternalLineageObject from a dictionary."""
        return cls(
            external_metadata=_from_dict(d, "external_metadata", ExternalLineageExternalMetadata),
            model_version=_from_dict(d, "model_version", ExternalLineageModelVersion),
            path=_from_dict(d, "path", ExternalLineagePath),
            table=_from_dict(d, "table", ExternalLineageTable),
        )


@dataclass
class ExternalLineagePath:
    url: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ExternalLineagePath into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineagePath into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineagePath:
        """Deserializes the ExternalLineagePath from a dictionary."""
        return cls(url=d.get("url", None))


@dataclass
class ExternalLineageRelationship:
    source: ExternalLineageObject
    """Source object of the external lineage relationship."""

    target: ExternalLineageObject
    """Target object of the external lineage relationship."""

    columns: Optional[List[ColumnRelationship]] = None
    """List of column relationships between source and target objects."""

    id: Optional[str] = None
    """Unique identifier of the external lineage relationship."""

    properties: Optional[Dict[str, str]] = None
    """Key-value properties associated with the external lineage relationship."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageRelationship into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source.as_dict()
        if self.target:
            body["target"] = self.target.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageRelationship into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.columns:
            body["columns"] = self.columns
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source
        if self.target:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageRelationship:
        """Deserializes the ExternalLineageRelationship from a dictionary."""
        return cls(
            columns=_repeated_dict(d, "columns", ColumnRelationship),
            id=d.get("id", None),
            properties=d.get("properties", None),
            source=_from_dict(d, "source", ExternalLineageObject),
            target=_from_dict(d, "target", ExternalLineageObject),
        )


@dataclass
class ExternalLineageRelationshipInfo:
    source: ExternalLineageObject
    """Source object of the external lineage relationship."""

    target: ExternalLineageObject
    """Target object of the external lineage relationship."""

    columns: Optional[List[ColumnRelationship]] = None
    """List of column relationships between source and target objects."""

    id: Optional[str] = None
    """Unique identifier of the external lineage relationship."""

    properties: Optional[Dict[str, str]] = None
    """Key-value properties associated with the external lineage relationship."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageRelationshipInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source.as_dict()
        if self.target:
            body["target"] = self.target.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageRelationshipInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.columns:
            body["columns"] = self.columns
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source
        if self.target:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageRelationshipInfo:
        """Deserializes the ExternalLineageRelationshipInfo from a dictionary."""
        return cls(
            columns=_repeated_dict(d, "columns", ColumnRelationship),
            id=d.get("id", None),
            properties=d.get("properties", None),
            source=_from_dict(d, "source", ExternalLineageObject),
            target=_from_dict(d, "target", ExternalLineageObject),
        )


@dataclass
class ExternalLineageTable:
    name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageTable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageTable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageTable:
        """Deserializes the ExternalLineageTable from a dictionary."""
        return cls(name=d.get("name", None))


@dataclass
class ExternalLineageTableInfo:
    """Represents the table information in the lineage event."""

    catalog_name: Optional[str] = None
    """Name of Catalog."""

    event_time: Optional[str] = None
    """Timestamp of the lineage event."""

    name: Optional[str] = None
    """Name of Table."""

    schema_name: Optional[str] = None
    """Name of Schema."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLineageTableInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.name is not None:
            body["name"] = self.name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLineageTableInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.event_time is not None:
            body["event_time"] = self.event_time
        if self.name is not None:
            body["name"] = self.name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLineageTableInfo:
        """Deserializes the ExternalLineageTableInfo from a dictionary."""
        return cls(
            catalog_name=d.get("catalog_name", None),
            event_time=d.get("event_time", None),
            name=d.get("name", None),
            schema_name=d.get("schema_name", None),
        )


@dataclass
class ExternalLocationInfo:
    browse_only: Optional[bool] = None
    """Indicates whether the principal is limited to retrieving metadata for the associated object
    through the BROWSE privilege when include_browse is enabled in the request."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this external location was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of external location creator."""

    credential_id: Optional[str] = None
    """Unique ID of the location's storage credential."""

    credential_name: Optional[str] = None
    """Name of the storage credential used with this location."""

    effective_enable_file_events: Optional[bool] = None
    """The effective value of `enable_file_events` after applying server-side defaults."""

    enable_file_events: Optional[bool] = None
    """Whether to enable file events on this external location. Default to `true`. Set to `false` to
    disable file events. The actual applied value may differ due to server-side defaults; check
    `effective_enable_file_events` for the effective state."""

    encryption_details: Optional[EncryptionDetails] = None

    fallback: Optional[bool] = None
    """Indicates whether fallback mode is enabled for this external location. When fallback mode is
    enabled, the access to the location falls back to cluster credentials if UC credentials are not
    sufficient."""

    file_event_queue: Optional[FileEventQueue] = None
    """File event queue settings. If `enable_file_events` is not `false`, must be defined and have
    exactly one of the documented properties."""

    isolation_mode: Optional[IsolationMode] = None

    metastore_id: Optional[str] = None
    """Unique identifier of metastore hosting the external location."""

    name: Optional[str] = None
    """Name of the external location."""

    owner: Optional[str] = None
    """The owner of the external location."""

    read_only: Optional[bool] = None
    """Indicates whether the external location is read-only."""

    updated_at: Optional[int] = None
    """Time at which external location this was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified the external location."""

    url: Optional[str] = None
    """Path URL of the external location."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLocationInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.credential_name is not None:
            body["credential_name"] = self.credential_name
        if self.effective_enable_file_events is not None:
            body["effective_enable_file_events"] = self.effective_enable_file_events
        if self.enable_file_events is not None:
            body["enable_file_events"] = self.enable_file_events
        if self.encryption_details:
            body["encryption_details"] = self.encryption_details.as_dict()
        if self.fallback is not None:
            body["fallback"] = self.fallback
        if self.file_event_queue:
            body["file_event_queue"] = self.file_event_queue.as_dict()
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode.value
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLocationInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        if self.credential_name is not None:
            body["credential_name"] = self.credential_name
        if self.effective_enable_file_events is not None:
            body["effective_enable_file_events"] = self.effective_enable_file_events
        if self.enable_file_events is not None:
            body["enable_file_events"] = self.enable_file_events
        if self.encryption_details:
            body["encryption_details"] = self.encryption_details
        if self.fallback is not None:
            body["fallback"] = self.fallback
        if self.file_event_queue:
            body["file_event_queue"] = self.file_event_queue
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLocationInfo:
        """Deserializes the ExternalLocationInfo from a dictionary."""
        return cls(
            browse_only=d.get("browse_only", None),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            credential_id=d.get("credential_id", None),
            credential_name=d.get("credential_name", None),
            effective_enable_file_events=d.get("effective_enable_file_events", None),
            enable_file_events=d.get("enable_file_events", None),
            encryption_details=_from_dict(d, "encryption_details", EncryptionDetails),
            fallback=d.get("fallback", None),
            file_event_queue=_from_dict(d, "file_event_queue", FileEventQueue),
            isolation_mode=_enum(d, "isolation_mode", IsolationMode),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            read_only=d.get("read_only", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            url=d.get("url", None),
        )


@dataclass
class ExternalMetadata:
    name: str
    """Name of the external metadata object."""

    system_type: SystemType
    """Type of external system."""

    entity_type: str
    """Type of entity within the external system."""

    columns: Optional[List[str]] = None
    """List of columns associated with the external metadata object."""

    create_time: Optional[str] = None
    """Time at which this external metadata object was created."""

    created_by: Optional[str] = None
    """Username of external metadata object creator."""

    description: Optional[str] = None
    """User-provided free-form text description."""

    id: Optional[str] = None
    """Unique identifier of the external metadata object."""

    metastore_id: Optional[str] = None
    """Unique identifier of parent metastore."""

    owner: Optional[str] = None
    """Owner of the external metadata object."""

    properties: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the external metadata object."""

    update_time: Optional[str] = None
    """Time at which this external metadata object was last modified."""

    updated_by: Optional[str] = None
    """Username of user who last modified external metadata object."""

    url: Optional[str] = None
    """URL associated with the external metadata object."""

    def as_dict(self) -> dict:
        """Serializes the ExternalMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.columns:
            body["columns"] = [v for v in self.columns]
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.description is not None:
            body["description"] = self.description
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.id is not None:
            body["id"] = self.id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.system_type is not None:
            body["system_type"] = self.system_type.value
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.columns:
            body["columns"] = self.columns
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.description is not None:
            body["description"] = self.description
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.id is not None:
            body["id"] = self.id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.system_type is not None:
            body["system_type"] = self.system_type
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalMetadata:
        """Deserializes the ExternalMetadata from a dictionary."""
        return cls(
            columns=d.get("columns", None),
            create_time=d.get("create_time", None),
            created_by=d.get("created_by", None),
            description=d.get("description", None),
            entity_type=d.get("entity_type", None),
            id=d.get("id", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            properties=d.get("properties", None),
            system_type=_enum(d, "system_type", SystemType),
            update_time=d.get("update_time", None),
            updated_by=d.get("updated_by", None),
            url=d.get("url", None),
        )


@dataclass
class FailedStatus:
    """Detailed status of an online table. Shown if the online table is in the OFFLINE_FAILED or the
    ONLINE_PIPELINE_FAILED state."""

    last_processed_commit_version: Optional[int] = None
    """The last source table Delta version that was synced to the online table. Note that this Delta
    version may only be partially synced to the online table. Only populated if the table is still
    online and available for serving."""

    timestamp: Optional[str] = None
    """The timestamp of the last time any data was synchronized from the source table to the online
    table. Only populated if the table is still online and available for serving."""

    def as_dict(self) -> dict:
        """Serializes the FailedStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FailedStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FailedStatus:
        """Deserializes the FailedStatus from a dictionary."""
        return cls(
            last_processed_commit_version=d.get("last_processed_commit_version", None),
            timestamp=d.get("timestamp", None),
        )


@dataclass
class FileEventQueue:
    managed_aqs: Optional[AzureQueueStorage] = None

    managed_pubsub: Optional[GcpPubsub] = None

    managed_sqs: Optional[AwsSqsQueue] = None

    provided_aqs: Optional[AzureQueueStorage] = None

    provided_pubsub: Optional[GcpPubsub] = None

    provided_sqs: Optional[AwsSqsQueue] = None

    def as_dict(self) -> dict:
        """Serializes the FileEventQueue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.managed_aqs:
            body["managed_aqs"] = self.managed_aqs.as_dict()
        if self.managed_pubsub:
            body["managed_pubsub"] = self.managed_pubsub.as_dict()
        if self.managed_sqs:
            body["managed_sqs"] = self.managed_sqs.as_dict()
        if self.provided_aqs:
            body["provided_aqs"] = self.provided_aqs.as_dict()
        if self.provided_pubsub:
            body["provided_pubsub"] = self.provided_pubsub.as_dict()
        if self.provided_sqs:
            body["provided_sqs"] = self.provided_sqs.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileEventQueue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.managed_aqs:
            body["managed_aqs"] = self.managed_aqs
        if self.managed_pubsub:
            body["managed_pubsub"] = self.managed_pubsub
        if self.managed_sqs:
            body["managed_sqs"] = self.managed_sqs
        if self.provided_aqs:
            body["provided_aqs"] = self.provided_aqs
        if self.provided_pubsub:
            body["provided_pubsub"] = self.provided_pubsub
        if self.provided_sqs:
            body["provided_sqs"] = self.provided_sqs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileEventQueue:
        """Deserializes the FileEventQueue from a dictionary."""
        return cls(
            managed_aqs=_from_dict(d, "managed_aqs", AzureQueueStorage),
            managed_pubsub=_from_dict(d, "managed_pubsub", GcpPubsub),
            managed_sqs=_from_dict(d, "managed_sqs", AwsSqsQueue),
            provided_aqs=_from_dict(d, "provided_aqs", AzureQueueStorage),
            provided_pubsub=_from_dict(d, "provided_pubsub", GcpPubsub),
            provided_sqs=_from_dict(d, "provided_sqs", AwsSqsQueue),
        )


@dataclass
class ForeignKeyConstraint:
    name: str
    """The name of the constraint."""

    child_columns: List[str]
    """Column names for this constraint."""

    parent_table: str
    """The full name of the parent constraint."""

    parent_columns: List[str]
    """Column names for this constraint."""

    rely: Optional[bool] = None
    """True if the constraint is RELY, false or unset if NORELY."""

    def as_dict(self) -> dict:
        """Serializes the ForeignKeyConstraint into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.child_columns:
            body["child_columns"] = [v for v in self.child_columns]
        if self.name is not None:
            body["name"] = self.name
        if self.parent_columns:
            body["parent_columns"] = [v for v in self.parent_columns]
        if self.parent_table is not None:
            body["parent_table"] = self.parent_table
        if self.rely is not None:
            body["rely"] = self.rely
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ForeignKeyConstraint into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.child_columns:
            body["child_columns"] = self.child_columns
        if self.name is not None:
            body["name"] = self.name
        if self.parent_columns:
            body["parent_columns"] = self.parent_columns
        if self.parent_table is not None:
            body["parent_table"] = self.parent_table
        if self.rely is not None:
            body["rely"] = self.rely
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ForeignKeyConstraint:
        """Deserializes the ForeignKeyConstraint from a dictionary."""
        return cls(
            child_columns=d.get("child_columns", None),
            name=d.get("name", None),
            parent_columns=d.get("parent_columns", None),
            parent_table=d.get("parent_table", None),
            rely=d.get("rely", None),
        )


@dataclass
class FunctionArgument:
    alias: Optional[str] = None
    """The alias of a matched column."""

    constant: Optional[str] = None
    """A constant literal."""

    def as_dict(self) -> dict:
        """Serializes the FunctionArgument into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alias is not None:
            body["alias"] = self.alias
        if self.constant is not None:
            body["constant"] = self.constant
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FunctionArgument into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alias is not None:
            body["alias"] = self.alias
        if self.constant is not None:
            body["constant"] = self.constant
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FunctionArgument:
        """Deserializes the FunctionArgument from a dictionary."""
        return cls(alias=d.get("alias", None), constant=d.get("constant", None))


@dataclass
class FunctionDependency:
    """A function that is dependent on a SQL object."""

    function_full_name: str
    """Full name of the dependent function, in the form of
    __catalog_name__.__schema_name__.__function_name__."""

    def as_dict(self) -> dict:
        """Serializes the FunctionDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.function_full_name is not None:
            body["function_full_name"] = self.function_full_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FunctionDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.function_full_name is not None:
            body["function_full_name"] = self.function_full_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FunctionDependency:
        """Deserializes the FunctionDependency from a dictionary."""
        return cls(function_full_name=d.get("function_full_name", None))


@dataclass
class FunctionInfo:
    browse_only: Optional[bool] = None
    """Indicates whether the principal is limited to retrieving metadata for the associated object
    through the BROWSE privilege when include_browse is enabled in the request."""

    catalog_name: Optional[str] = None
    """Name of parent Catalog."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this function was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of function creator."""

    data_type: Optional[ColumnTypeName] = None
    """Scalar function return data type."""

    external_language: Optional[str] = None
    """External function language."""

    external_name: Optional[str] = None
    """External function name."""

    full_data_type: Optional[str] = None
    """Pretty printed function data type."""

    full_name: Optional[str] = None
    """Full name of Function, in form of **catalog_name**.**schema_name**.**function_name**"""

    function_id: Optional[str] = None
    """Id of Function, relative to parent schema."""

    input_params: Optional[FunctionParameterInfos] = None
    """Function input parameters."""

    is_deterministic: Optional[bool] = None
    """Whether the function is deterministic."""

    is_null_call: Optional[bool] = None
    """Function null call."""

    metastore_id: Optional[str] = None
    """Unique identifier of parent metastore."""

    name: Optional[str] = None
    """Name of function, relative to parent schema."""

    owner: Optional[str] = None
    """Username of current owner of the function."""

    parameter_style: Optional[FunctionInfoParameterStyle] = None
    """Function parameter style. **S** is the value for SQL."""

    properties: Optional[str] = None
    """JSON-serialized key-value pair map, encoded (escaped) as a string."""

    return_params: Optional[FunctionParameterInfos] = None
    """Table function return parameters."""

    routine_body: Optional[FunctionInfoRoutineBody] = None
    """Function language. When **EXTERNAL** is used, the language of the routine function should be
    specified in the **external_language** field, and the **return_params** of the function cannot
    be used (as **TABLE** return type is not supported), and the **sql_data_access** field must be
    **NO_SQL**."""

    routine_definition: Optional[str] = None
    """Function body."""

    routine_dependencies: Optional[DependencyList] = None
    """function dependencies."""

    schema_name: Optional[str] = None
    """Name of parent Schema relative to its parent Catalog."""

    security_type: Optional[FunctionInfoSecurityType] = None
    """Function security type."""

    specific_name: Optional[str] = None
    """Specific name of the function; Reserved for future use."""

    sql_data_access: Optional[FunctionInfoSqlDataAccess] = None
    """Function SQL data access."""

    sql_path: Optional[str] = None
    """List of schemes whose objects can be referenced without qualification."""

    updated_at: Optional[int] = None
    """Time at which this function was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified the function."""

    def as_dict(self) -> dict:
        """Serializes the FunctionInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_type is not None:
            body["data_type"] = self.data_type.value
        if self.external_language is not None:
            body["external_language"] = self.external_language
        if self.external_name is not None:
            body["external_name"] = self.external_name
        if self.full_data_type is not None:
            body["full_data_type"] = self.full_data_type
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.function_id is not None:
            body["function_id"] = self.function_id
        if self.input_params:
            body["input_params"] = self.input_params.as_dict()
        if self.is_deterministic is not None:
            body["is_deterministic"] = self.is_deterministic
        if self.is_null_call is not None:
            body["is_null_call"] = self.is_null_call
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.parameter_style is not None:
            body["parameter_style"] = self.parameter_style.value
        if self.properties is not None:
            body["properties"] = self.properties
        if self.return_params:
            body["return_params"] = self.return_params.as_dict()
        if self.routine_body is not None:
            body["routine_body"] = self.routine_body.value
        if self.routine_definition is not None:
            body["routine_definition"] = self.routine_definition
        if self.routine_dependencies:
            body["routine_dependencies"] = self.routine_dependencies.as_dict()
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.security_type is not None:
            body["security_type"] = self.security_type.value
        if self.specific_name is not None:
            body["specific_name"] = self.specific_name
        if self.sql_data_access is not None:
            body["sql_data_access"] = self.sql_data_access.value
        if self.sql_path is not None:
            body["sql_path"] = self.sql_path
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FunctionInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_type is not None:
            body["data_type"] = self.data_type
        if self.external_language is not None:
            body["external_language"] = self.external_language
        if self.external_name is not None:
            body["external_name"] = self.external_name
        if self.full_data_type is not None:
            body["full_data_type"] = self.full_data_type
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.function_id is not None:
            body["function_id"] = self.function_id
        if self.input_params:
            body["input_params"] = self.input_params
        if self.is_deterministic is not None:
            body["is_deterministic"] = self.is_deterministic
        if self.is_null_call is not None:
            body["is_null_call"] = self.is_null_call
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.parameter_style is not None:
            body["parameter_style"] = self.parameter_style
        if self.properties is not None:
            body["properties"] = self.properties
        if self.return_params:
            body["return_params"] = self.return_params
        if self.routine_body is not None:
            body["routine_body"] = self.routine_body
        if self.routine_definition is not None:
            body["routine_definition"] = self.routine_definition
        if self.routine_dependencies:
            body["routine_dependencies"] = self.routine_dependencies
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.security_type is not None:
            body["security_type"] = self.security_type
        if self.specific_name is not None:
            body["specific_name"] = self.specific_name
        if self.sql_data_access is not None:
            body["sql_data_access"] = self.sql_data_access
        if self.sql_path is not None:
            body["sql_path"] = self.sql_path
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FunctionInfo:
        """Deserializes the FunctionInfo from a dictionary."""
        return cls(
            browse_only=d.get("browse_only", None),
            catalog_name=d.get("catalog_name", None),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            data_type=_enum(d, "data_type", ColumnTypeName),
            external_language=d.get("external_language", None),
            external_name=d.get("external_name", None),
            full_data_type=d.get("full_data_type", None),
            full_name=d.get("full_name", None),
            function_id=d.get("function_id", None),
            input_params=_from_dict(d, "input_params", FunctionParameterInfos),
            is_deterministic=d.get("is_deterministic", None),
            is_null_call=d.get("is_null_call", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            parameter_style=_enum(d, "parameter_style", FunctionInfoParameterStyle),
            properties=d.get("properties", None),
            return_params=_from_dict(d, "return_params", FunctionParameterInfos),
            routine_body=_enum(d, "routine_body", FunctionInfoRoutineBody),
            routine_definition=d.get("routine_definition", None),
            routine_dependencies=_from_dict(d, "routine_dependencies", DependencyList),
            schema_name=d.get("schema_name", None),
            security_type=_enum(d, "security_type", FunctionInfoSecurityType),
            specific_name=d.get("specific_name", None),
            sql_data_access=_enum(d, "sql_data_access", FunctionInfoSqlDataAccess),
            sql_path=d.get("sql_path", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


class FunctionInfoParameterStyle(Enum):

    S = "S"


class FunctionInfoRoutineBody(Enum):

    EXTERNAL = "EXTERNAL"
    SQL = "SQL"


class FunctionInfoSecurityType(Enum):

    DEFINER = "DEFINER"


class FunctionInfoSqlDataAccess(Enum):

    CONTAINS_SQL = "CONTAINS_SQL"
    NO_SQL = "NO_SQL"
    READS_SQL_DATA = "READS_SQL_DATA"


@dataclass
class FunctionParameterInfo:
    name: str
    """Name of Parameter."""

    type_text: str
    """Full data type spec, SQL/catalogString text."""

    type_name: ColumnTypeName
    """Name of type (INT, STRUCT, MAP, etc.)"""

    position: int
    """Ordinal position of column (starting at position 0)."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    parameter_default: Optional[str] = None
    """Default value of the parameter."""

    parameter_mode: Optional[FunctionParameterMode] = None
    """Function parameter mode."""

    parameter_type: Optional[FunctionParameterType] = None
    """Function parameter type."""

    type_interval_type: Optional[str] = None
    """Format of IntervalType."""

    type_json: Optional[str] = None
    """Full data type spec, JSON-serialized."""

    type_precision: Optional[int] = None
    """Digits of precision; required on Create for DecimalTypes."""

    type_scale: Optional[int] = None
    """Digits to right of decimal; Required on Create for DecimalTypes."""

    def as_dict(self) -> dict:
        """Serializes the FunctionParameterInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.name is not None:
            body["name"] = self.name
        if self.parameter_default is not None:
            body["parameter_default"] = self.parameter_default
        if self.parameter_mode is not None:
            body["parameter_mode"] = self.parameter_mode.value
        if self.parameter_type is not None:
            body["parameter_type"] = self.parameter_type.value
        if self.position is not None:
            body["position"] = self.position
        if self.type_interval_type is not None:
            body["type_interval_type"] = self.type_interval_type
        if self.type_json is not None:
            body["type_json"] = self.type_json
        if self.type_name is not None:
            body["type_name"] = self.type_name.value
        if self.type_precision is not None:
            body["type_precision"] = self.type_precision
        if self.type_scale is not None:
            body["type_scale"] = self.type_scale
        if self.type_text is not None:
            body["type_text"] = self.type_text
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FunctionParameterInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.name is not None:
            body["name"] = self.name
        if self.parameter_default is not None:
            body["parameter_default"] = self.parameter_default
        if self.parameter_mode is not None:
            body["parameter_mode"] = self.parameter_mode
        if self.parameter_type is not None:
            body["parameter_type"] = self.parameter_type
        if self.position is not None:
            body["position"] = self.position
        if self.type_interval_type is not None:
            body["type_interval_type"] = self.type_interval_type
        if self.type_json is not None:
            body["type_json"] = self.type_json
        if self.type_name is not None:
            body["type_name"] = self.type_name
        if self.type_precision is not None:
            body["type_precision"] = self.type_precision
        if self.type_scale is not None:
            body["type_scale"] = self.type_scale
        if self.type_text is not None:
            body["type_text"] = self.type_text
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FunctionParameterInfo:
        """Deserializes the FunctionParameterInfo from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            name=d.get("name", None),
            parameter_default=d.get("parameter_default", None),
            parameter_mode=_enum(d, "parameter_mode", FunctionParameterMode),
            parameter_type=_enum(d, "parameter_type", FunctionParameterType),
            position=d.get("position", None),
            type_interval_type=d.get("type_interval_type", None),
            type_json=d.get("type_json", None),
            type_name=_enum(d, "type_name", ColumnTypeName),
            type_precision=d.get("type_precision", None),
            type_scale=d.get("type_scale", None),
            type_text=d.get("type_text", None),
        )


@dataclass
class FunctionParameterInfos:
    parameters: Optional[List[FunctionParameterInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the FunctionParameterInfos into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FunctionParameterInfos into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FunctionParameterInfos:
        """Deserializes the FunctionParameterInfos from a dictionary."""
        return cls(parameters=_repeated_dict(d, "parameters", FunctionParameterInfo))


class FunctionParameterMode(Enum):

    IN = "IN"


class FunctionParameterType(Enum):

    COLUMN = "COLUMN"
    PARAM = "PARAM"


@dataclass
class GcpOauthToken:
    """GCP temporary credentials for API authentication. Read more at
    https://developers.google.com/identity/protocols/oauth2/service-account"""

    oauth_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the GcpOauthToken into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.oauth_token is not None:
            body["oauth_token"] = self.oauth_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpOauthToken into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.oauth_token is not None:
            body["oauth_token"] = self.oauth_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpOauthToken:
        """Deserializes the GcpOauthToken from a dictionary."""
        return cls(oauth_token=d.get("oauth_token", None))


@dataclass
class GcpPubsub:
    managed_resource_id: Optional[str] = None
    """Unique identifier included in the name of file events managed cloud resources."""

    subscription_name: Optional[str] = None
    """The Pub/Sub subscription name in the format projects/{project}/subscriptions/{subscription
    name}. Only required for provided_pubsub."""

    def as_dict(self) -> dict:
        """Serializes the GcpPubsub into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.managed_resource_id is not None:
            body["managed_resource_id"] = self.managed_resource_id
        if self.subscription_name is not None:
            body["subscription_name"] = self.subscription_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpPubsub into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.managed_resource_id is not None:
            body["managed_resource_id"] = self.managed_resource_id
        if self.subscription_name is not None:
            body["subscription_name"] = self.subscription_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpPubsub:
        """Deserializes the GcpPubsub from a dictionary."""
        return cls(
            managed_resource_id=d.get("managed_resource_id", None), subscription_name=d.get("subscription_name", None)
        )


@dataclass
class GenerateTemporaryPathCredentialResponse:
    aws_temp_credentials: Optional[AwsCredentials] = None

    azure_aad: Optional[AzureActiveDirectoryToken] = None

    azure_user_delegation_sas: Optional[AzureUserDelegationSas] = None

    expiration_time: Optional[int] = None
    """Server time when the credential will expire, in epoch milliseconds. The API client is advised to
    cache the credential given this expiration time."""

    gcp_oauth_token: Optional[GcpOauthToken] = None

    r2_temp_credentials: Optional[R2Credentials] = None

    url: Optional[str] = None
    """The URL of the storage path accessible by the temporary credential."""

    def as_dict(self) -> dict:
        """Serializes the GenerateTemporaryPathCredentialResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_temp_credentials:
            body["aws_temp_credentials"] = self.aws_temp_credentials.as_dict()
        if self.azure_aad:
            body["azure_aad"] = self.azure_aad.as_dict()
        if self.azure_user_delegation_sas:
            body["azure_user_delegation_sas"] = self.azure_user_delegation_sas.as_dict()
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.gcp_oauth_token:
            body["gcp_oauth_token"] = self.gcp_oauth_token.as_dict()
        if self.r2_temp_credentials:
            body["r2_temp_credentials"] = self.r2_temp_credentials.as_dict()
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenerateTemporaryPathCredentialResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_temp_credentials:
            body["aws_temp_credentials"] = self.aws_temp_credentials
        if self.azure_aad:
            body["azure_aad"] = self.azure_aad
        if self.azure_user_delegation_sas:
            body["azure_user_delegation_sas"] = self.azure_user_delegation_sas
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.gcp_oauth_token:
            body["gcp_oauth_token"] = self.gcp_oauth_token
        if self.r2_temp_credentials:
            body["r2_temp_credentials"] = self.r2_temp_credentials
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenerateTemporaryPathCredentialResponse:
        """Deserializes the GenerateTemporaryPathCredentialResponse from a dictionary."""
        return cls(
            aws_temp_credentials=_from_dict(d, "aws_temp_credentials", AwsCredentials),
            azure_aad=_from_dict(d, "azure_aad", AzureActiveDirectoryToken),
            azure_user_delegation_sas=_from_dict(d, "azure_user_delegation_sas", AzureUserDelegationSas),
            expiration_time=d.get("expiration_time", None),
            gcp_oauth_token=_from_dict(d, "gcp_oauth_token", GcpOauthToken),
            r2_temp_credentials=_from_dict(d, "r2_temp_credentials", R2Credentials),
            url=d.get("url", None),
        )


@dataclass
class GenerateTemporaryServiceCredentialAzureOptions:
    """The Azure cloud options to customize the requested temporary credential"""

    resources: Optional[List[str]] = None
    """The resources to which the temporary Azure credential should apply. These resources are the
    scopes that are passed to the token provider (see
    https://learn.microsoft.com/python/api/azure-core/azure.core.credentials.tokencredential?view=azure-python)"""

    def as_dict(self) -> dict:
        """Serializes the GenerateTemporaryServiceCredentialAzureOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.resources:
            body["resources"] = [v for v in self.resources]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenerateTemporaryServiceCredentialAzureOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.resources:
            body["resources"] = self.resources
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenerateTemporaryServiceCredentialAzureOptions:
        """Deserializes the GenerateTemporaryServiceCredentialAzureOptions from a dictionary."""
        return cls(resources=d.get("resources", None))


@dataclass
class GenerateTemporaryServiceCredentialGcpOptions:
    """The GCP cloud options to customize the requested temporary credential"""

    scopes: Optional[List[str]] = None
    """The scopes to which the temporary GCP credential should apply. These resources are the scopes
    that are passed to the token provider (see
    https://google-auth.readthedocs.io/en/latest/reference/google.auth.html#google.auth.credentials.Credentials)"""

    def as_dict(self) -> dict:
        """Serializes the GenerateTemporaryServiceCredentialGcpOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.scopes:
            body["scopes"] = [v for v in self.scopes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenerateTemporaryServiceCredentialGcpOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.scopes:
            body["scopes"] = self.scopes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenerateTemporaryServiceCredentialGcpOptions:
        """Deserializes the GenerateTemporaryServiceCredentialGcpOptions from a dictionary."""
        return cls(scopes=d.get("scopes", None))


@dataclass
class GenerateTemporaryTableCredentialResponse:
    aws_temp_credentials: Optional[AwsCredentials] = None

    azure_aad: Optional[AzureActiveDirectoryToken] = None

    azure_user_delegation_sas: Optional[AzureUserDelegationSas] = None

    expiration_time: Optional[int] = None
    """Server time when the credential will expire, in epoch milliseconds. The API client is advised to
    cache the credential given this expiration time."""

    gcp_oauth_token: Optional[GcpOauthToken] = None

    r2_temp_credentials: Optional[R2Credentials] = None

    url: Optional[str] = None
    """The URL of the storage path accessible by the temporary credential."""

    def as_dict(self) -> dict:
        """Serializes the GenerateTemporaryTableCredentialResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_temp_credentials:
            body["aws_temp_credentials"] = self.aws_temp_credentials.as_dict()
        if self.azure_aad:
            body["azure_aad"] = self.azure_aad.as_dict()
        if self.azure_user_delegation_sas:
            body["azure_user_delegation_sas"] = self.azure_user_delegation_sas.as_dict()
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.gcp_oauth_token:
            body["gcp_oauth_token"] = self.gcp_oauth_token.as_dict()
        if self.r2_temp_credentials:
            body["r2_temp_credentials"] = self.r2_temp_credentials.as_dict()
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenerateTemporaryTableCredentialResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_temp_credentials:
            body["aws_temp_credentials"] = self.aws_temp_credentials
        if self.azure_aad:
            body["azure_aad"] = self.azure_aad
        if self.azure_user_delegation_sas:
            body["azure_user_delegation_sas"] = self.azure_user_delegation_sas
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.gcp_oauth_token:
            body["gcp_oauth_token"] = self.gcp_oauth_token
        if self.r2_temp_credentials:
            body["r2_temp_credentials"] = self.r2_temp_credentials
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenerateTemporaryTableCredentialResponse:
        """Deserializes the GenerateTemporaryTableCredentialResponse from a dictionary."""
        return cls(
            aws_temp_credentials=_from_dict(d, "aws_temp_credentials", AwsCredentials),
            azure_aad=_from_dict(d, "azure_aad", AzureActiveDirectoryToken),
            azure_user_delegation_sas=_from_dict(d, "azure_user_delegation_sas", AzureUserDelegationSas),
            expiration_time=d.get("expiration_time", None),
            gcp_oauth_token=_from_dict(d, "gcp_oauth_token", GcpOauthToken),
            r2_temp_credentials=_from_dict(d, "r2_temp_credentials", R2Credentials),
            url=d.get("url", None),
        )


@dataclass
class GetCatalogWorkspaceBindingsResponse:
    workspaces: Optional[List[int]] = None
    """A list of workspace IDs"""

    def as_dict(self) -> dict:
        """Serializes the GetCatalogWorkspaceBindingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.workspaces:
            body["workspaces"] = [v for v in self.workspaces]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetCatalogWorkspaceBindingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.workspaces:
            body["workspaces"] = self.workspaces
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetCatalogWorkspaceBindingsResponse:
        """Deserializes the GetCatalogWorkspaceBindingsResponse from a dictionary."""
        return cls(workspaces=d.get("workspaces", None))


@dataclass
class GetMetastoreSummaryResponse:
    cloud: Optional[str] = None
    """Cloud vendor of the metastore home shard (e.g., `aws`, `azure`, `gcp`)."""

    created_at: Optional[int] = None
    """Time at which this metastore was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of metastore creator."""

    default_data_access_config_id: Optional[str] = None
    """Unique identifier of the metastore's (Default) Data Access Configuration."""

    delta_sharing_organization_name: Optional[str] = None
    """The organization name of a Delta Sharing entity, to be used in Databricks-to-Databricks Delta
    Sharing as the official name."""

    delta_sharing_recipient_token_lifetime_in_seconds: Optional[int] = None
    """The lifetime of delta sharing recipient token in seconds."""

    delta_sharing_scope: Optional[DeltaSharingScopeEnum] = None
    """The scope of Delta Sharing enabled for the metastore."""

    external_access_enabled: Optional[bool] = None
    """Whether to allow non-DBR clients to directly access entities under the metastore."""

    global_metastore_id: Optional[str] = None
    """Globally unique metastore ID across clouds and regions, of the form `cloud:region:metastore_id`."""

    metastore_id: Optional[str] = None
    """Unique identifier of metastore."""

    name: Optional[str] = None
    """The user-specified name of the metastore."""

    owner: Optional[str] = None
    """The owner of the metastore."""

    privilege_model_version: Optional[str] = None
    """Privilege model version of the metastore, of the form `major.minor` (e.g., `1.0`)."""

    region: Optional[str] = None
    """Cloud region which the metastore serves (e.g., `us-west-2`, `westus`)."""

    storage_root: Optional[str] = None
    """The storage root URL for metastore"""

    storage_root_credential_id: Optional[str] = None
    """UUID of storage credential to access the metastore storage_root."""

    storage_root_credential_name: Optional[str] = None
    """Name of the storage credential to access the metastore storage_root."""

    updated_at: Optional[int] = None
    """Time at which the metastore was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified the metastore."""

    def as_dict(self) -> dict:
        """Serializes the GetMetastoreSummaryResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.default_data_access_config_id is not None:
            body["default_data_access_config_id"] = self.default_data_access_config_id
        if self.delta_sharing_organization_name is not None:
            body["delta_sharing_organization_name"] = self.delta_sharing_organization_name
        if self.delta_sharing_recipient_token_lifetime_in_seconds is not None:
            body["delta_sharing_recipient_token_lifetime_in_seconds"] = (
                self.delta_sharing_recipient_token_lifetime_in_seconds
            )
        if self.delta_sharing_scope is not None:
            body["delta_sharing_scope"] = self.delta_sharing_scope.value
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.global_metastore_id is not None:
            body["global_metastore_id"] = self.global_metastore_id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.privilege_model_version is not None:
            body["privilege_model_version"] = self.privilege_model_version
        if self.region is not None:
            body["region"] = self.region
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.storage_root_credential_id is not None:
            body["storage_root_credential_id"] = self.storage_root_credential_id
        if self.storage_root_credential_name is not None:
            body["storage_root_credential_name"] = self.storage_root_credential_name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetMetastoreSummaryResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.default_data_access_config_id is not None:
            body["default_data_access_config_id"] = self.default_data_access_config_id
        if self.delta_sharing_organization_name is not None:
            body["delta_sharing_organization_name"] = self.delta_sharing_organization_name
        if self.delta_sharing_recipient_token_lifetime_in_seconds is not None:
            body["delta_sharing_recipient_token_lifetime_in_seconds"] = (
                self.delta_sharing_recipient_token_lifetime_in_seconds
            )
        if self.delta_sharing_scope is not None:
            body["delta_sharing_scope"] = self.delta_sharing_scope
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.global_metastore_id is not None:
            body["global_metastore_id"] = self.global_metastore_id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.privilege_model_version is not None:
            body["privilege_model_version"] = self.privilege_model_version
        if self.region is not None:
            body["region"] = self.region
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.storage_root_credential_id is not None:
            body["storage_root_credential_id"] = self.storage_root_credential_id
        if self.storage_root_credential_name is not None:
            body["storage_root_credential_name"] = self.storage_root_credential_name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetMetastoreSummaryResponse:
        """Deserializes the GetMetastoreSummaryResponse from a dictionary."""
        return cls(
            cloud=d.get("cloud", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            default_data_access_config_id=d.get("default_data_access_config_id", None),
            delta_sharing_organization_name=d.get("delta_sharing_organization_name", None),
            delta_sharing_recipient_token_lifetime_in_seconds=d.get(
                "delta_sharing_recipient_token_lifetime_in_seconds", None
            ),
            delta_sharing_scope=_enum(d, "delta_sharing_scope", DeltaSharingScopeEnum),
            external_access_enabled=d.get("external_access_enabled", None),
            global_metastore_id=d.get("global_metastore_id", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            privilege_model_version=d.get("privilege_model_version", None),
            region=d.get("region", None),
            storage_root=d.get("storage_root", None),
            storage_root_credential_id=d.get("storage_root_credential_id", None),
            storage_root_credential_name=d.get("storage_root_credential_name", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class GetPermissionsResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    privilege_assignments: Optional[List[PrivilegeAssignment]] = None
    """The privileges assigned to each principal"""

    def as_dict(self) -> dict:
        """Serializes the GetPermissionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.privilege_assignments:
            body["privilege_assignments"] = [v.as_dict() for v in self.privilege_assignments]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetPermissionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.privilege_assignments:
            body["privilege_assignments"] = self.privilege_assignments
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetPermissionsResponse:
        """Deserializes the GetPermissionsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            privilege_assignments=_repeated_dict(d, "privilege_assignments", PrivilegeAssignment),
        )


@dataclass
class GetQuotaResponse:
    quota_info: Optional[QuotaInfo] = None
    """The returned QuotaInfo."""

    def as_dict(self) -> dict:
        """Serializes the GetQuotaResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.quota_info:
            body["quota_info"] = self.quota_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetQuotaResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.quota_info:
            body["quota_info"] = self.quota_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetQuotaResponse:
        """Deserializes the GetQuotaResponse from a dictionary."""
        return cls(quota_info=_from_dict(d, "quota_info", QuotaInfo))


@dataclass
class GetWorkspaceBindingsResponse:
    bindings: Optional[List[WorkspaceBinding]] = None
    """List of workspace bindings"""

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the GetWorkspaceBindingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bindings:
            body["bindings"] = [v.as_dict() for v in self.bindings]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetWorkspaceBindingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bindings:
            body["bindings"] = self.bindings
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetWorkspaceBindingsResponse:
        """Deserializes the GetWorkspaceBindingsResponse from a dictionary."""
        return cls(
            bindings=_repeated_dict(d, "bindings", WorkspaceBinding), next_page_token=d.get("next_page_token", None)
        )


class IsolationMode(Enum):

    ISOLATION_MODE_ISOLATED = "ISOLATION_MODE_ISOLATED"
    ISOLATION_MODE_OPEN = "ISOLATION_MODE_OPEN"


class LineageDirection(Enum):

    DOWNSTREAM = "DOWNSTREAM"
    UPSTREAM = "UPSTREAM"


@dataclass
class ListAccountMetastoreAssignmentsResponse:
    """The metastore assignments were successfully returned."""

    workspace_ids: Optional[List[int]] = None

    def as_dict(self) -> dict:
        """Serializes the ListAccountMetastoreAssignmentsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.workspace_ids:
            body["workspace_ids"] = [v for v in self.workspace_ids]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAccountMetastoreAssignmentsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.workspace_ids:
            body["workspace_ids"] = self.workspace_ids
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAccountMetastoreAssignmentsResponse:
        """Deserializes the ListAccountMetastoreAssignmentsResponse from a dictionary."""
        return cls(workspace_ids=d.get("workspace_ids", None))


@dataclass
class ListAccountStorageCredentialsResponse:
    """The metastore storage credentials were successfully returned."""

    storage_credentials: Optional[List[StorageCredentialInfo]] = None
    """An array of metastore storage credentials."""

    def as_dict(self) -> dict:
        """Serializes the ListAccountStorageCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.storage_credentials:
            body["storage_credentials"] = [v.as_dict() for v in self.storage_credentials]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAccountStorageCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.storage_credentials:
            body["storage_credentials"] = self.storage_credentials
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAccountStorageCredentialsResponse:
        """Deserializes the ListAccountStorageCredentialsResponse from a dictionary."""
        return cls(storage_credentials=_repeated_dict(d, "storage_credentials", StorageCredentialInfo))


@dataclass
class ListCatalogsResponse:
    catalogs: Optional[List[CatalogInfo]] = None
    """An array of catalog information objects."""

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the ListCatalogsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalogs:
            body["catalogs"] = [v.as_dict() for v in self.catalogs]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListCatalogsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalogs:
            body["catalogs"] = self.catalogs
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListCatalogsResponse:
        """Deserializes the ListCatalogsResponse from a dictionary."""
        return cls(catalogs=_repeated_dict(d, "catalogs", CatalogInfo), next_page_token=d.get("next_page_token", None))


@dataclass
class ListConnectionsResponse:
    connections: Optional[List[ConnectionInfo]] = None
    """An array of connection information objects."""

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the ListConnectionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connections:
            body["connections"] = [v.as_dict() for v in self.connections]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListConnectionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connections:
            body["connections"] = self.connections
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListConnectionsResponse:
        """Deserializes the ListConnectionsResponse from a dictionary."""
        return cls(
            connections=_repeated_dict(d, "connections", ConnectionInfo), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListCredentialsResponse:
    credentials: Optional[List[CredentialInfo]] = None

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the ListCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credentials:
            body["credentials"] = [v.as_dict() for v in self.credentials]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credentials:
            body["credentials"] = self.credentials
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListCredentialsResponse:
        """Deserializes the ListCredentialsResponse from a dictionary."""
        return cls(
            credentials=_repeated_dict(d, "credentials", CredentialInfo), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListEntityTagAssignmentsResponse:
    next_page_token: Optional[str] = None
    """Optional. Pagination token for retrieving the next page of results"""

    tag_assignments: Optional[List[EntityTagAssignment]] = None
    """The list of tag assignments"""

    def as_dict(self) -> dict:
        """Serializes the ListEntityTagAssignmentsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tag_assignments:
            body["tag_assignments"] = [v.as_dict() for v in self.tag_assignments]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListEntityTagAssignmentsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tag_assignments:
            body["tag_assignments"] = self.tag_assignments
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListEntityTagAssignmentsResponse:
        """Deserializes the ListEntityTagAssignmentsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            tag_assignments=_repeated_dict(d, "tag_assignments", EntityTagAssignment),
        )


@dataclass
class ListExternalLineageRelationshipsResponse:
    external_lineage_relationships: Optional[List[ExternalLineageInfo]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListExternalLineageRelationshipsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_lineage_relationships:
            body["external_lineage_relationships"] = [v.as_dict() for v in self.external_lineage_relationships]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListExternalLineageRelationshipsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_lineage_relationships:
            body["external_lineage_relationships"] = self.external_lineage_relationships
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListExternalLineageRelationshipsResponse:
        """Deserializes the ListExternalLineageRelationshipsResponse from a dictionary."""
        return cls(
            external_lineage_relationships=_repeated_dict(d, "external_lineage_relationships", ExternalLineageInfo),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListExternalLocationsResponse:
    external_locations: Optional[List[ExternalLocationInfo]] = None
    """An array of external locations."""

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the ListExternalLocationsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_locations:
            body["external_locations"] = [v.as_dict() for v in self.external_locations]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListExternalLocationsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_locations:
            body["external_locations"] = self.external_locations
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListExternalLocationsResponse:
        """Deserializes the ListExternalLocationsResponse from a dictionary."""
        return cls(
            external_locations=_repeated_dict(d, "external_locations", ExternalLocationInfo),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListExternalMetadataResponse:
    external_metadata: Optional[List[ExternalMetadata]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListExternalMetadataResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.external_metadata:
            body["external_metadata"] = [v.as_dict() for v in self.external_metadata]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListExternalMetadataResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.external_metadata:
            body["external_metadata"] = self.external_metadata
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListExternalMetadataResponse:
        """Deserializes the ListExternalMetadataResponse from a dictionary."""
        return cls(
            external_metadata=_repeated_dict(d, "external_metadata", ExternalMetadata),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListFunctionsResponse:
    functions: Optional[List[FunctionInfo]] = None
    """An array of function information objects."""

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the ListFunctionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.functions:
            body["functions"] = [v.as_dict() for v in self.functions]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListFunctionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.functions:
            body["functions"] = self.functions
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListFunctionsResponse:
        """Deserializes the ListFunctionsResponse from a dictionary."""
        return cls(
            functions=_repeated_dict(d, "functions", FunctionInfo), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListMetastoresResponse:
    metastores: Optional[List[MetastoreInfo]] = None
    """An array of metastore information objects."""

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the ListMetastoresResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metastores:
            body["metastores"] = [v.as_dict() for v in self.metastores]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListMetastoresResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metastores:
            body["metastores"] = self.metastores
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListMetastoresResponse:
        """Deserializes the ListMetastoresResponse from a dictionary."""
        return cls(
            metastores=_repeated_dict(d, "metastores", MetastoreInfo), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListModelVersionsResponse:
    model_versions: Optional[List[ModelVersionInfo]] = None

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    def as_dict(self) -> dict:
        """Serializes the ListModelVersionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_versions:
            body["model_versions"] = [v.as_dict() for v in self.model_versions]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListModelVersionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_versions:
            body["model_versions"] = self.model_versions
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListModelVersionsResponse:
        """Deserializes the ListModelVersionsResponse from a dictionary."""
        return cls(
            model_versions=_repeated_dict(d, "model_versions", ModelVersionInfo),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListPoliciesResponse:
    next_page_token: Optional[str] = None
    """Optional opaque token for continuing pagination. `page_token` should be set to this value for
    the next request to retrieve the next page of results."""

    policies: Optional[List[PolicyInfo]] = None
    """The list of retrieved policies."""

    def as_dict(self) -> dict:
        """Serializes the ListPoliciesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.policies:
            body["policies"] = [v.as_dict() for v in self.policies]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListPoliciesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.policies:
            body["policies"] = self.policies
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListPoliciesResponse:
        """Deserializes the ListPoliciesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), policies=_repeated_dict(d, "policies", PolicyInfo))


@dataclass
class ListQuotasResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request."""

    quotas: Optional[List[QuotaInfo]] = None
    """An array of returned QuotaInfos."""

    def as_dict(self) -> dict:
        """Serializes the ListQuotasResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.quotas:
            body["quotas"] = [v.as_dict() for v in self.quotas]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListQuotasResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.quotas:
            body["quotas"] = self.quotas
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListQuotasResponse:
        """Deserializes the ListQuotasResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), quotas=_repeated_dict(d, "quotas", QuotaInfo))


@dataclass
class ListRegisteredModelsResponse:
    next_page_token: Optional[str] = None
    """Opaque token for pagination. Omitted if there are no more results. page_token should be set to
    this value for fetching the next page."""

    registered_models: Optional[List[RegisteredModelInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ListRegisteredModelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.registered_models:
            body["registered_models"] = [v.as_dict() for v in self.registered_models]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListRegisteredModelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.registered_models:
            body["registered_models"] = self.registered_models
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListRegisteredModelsResponse:
        """Deserializes the ListRegisteredModelsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            registered_models=_repeated_dict(d, "registered_models", RegisteredModelInfo),
        )


@dataclass
class ListSchemasResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    schemas: Optional[List[SchemaInfo]] = None
    """An array of schema information objects."""

    def as_dict(self) -> dict:
        """Serializes the ListSchemasResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.schemas:
            body["schemas"] = [v.as_dict() for v in self.schemas]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSchemasResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.schemas:
            body["schemas"] = self.schemas
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSchemasResponse:
        """Deserializes the ListSchemasResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), schemas=_repeated_dict(d, "schemas", SchemaInfo))


@dataclass
class ListStorageCredentialsResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    storage_credentials: Optional[List[StorageCredentialInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ListStorageCredentialsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.storage_credentials:
            body["storage_credentials"] = [v.as_dict() for v in self.storage_credentials]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListStorageCredentialsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.storage_credentials:
            body["storage_credentials"] = self.storage_credentials
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListStorageCredentialsResponse:
        """Deserializes the ListStorageCredentialsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            storage_credentials=_repeated_dict(d, "storage_credentials", StorageCredentialInfo),
        )


@dataclass
class ListSystemSchemasResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    schemas: Optional[List[SystemSchemaInfo]] = None
    """An array of system schema information objects."""

    def as_dict(self) -> dict:
        """Serializes the ListSystemSchemasResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.schemas:
            body["schemas"] = [v.as_dict() for v in self.schemas]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSystemSchemasResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.schemas:
            body["schemas"] = self.schemas
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSystemSchemasResponse:
        """Deserializes the ListSystemSchemasResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), schemas=_repeated_dict(d, "schemas", SystemSchemaInfo)
        )


@dataclass
class ListTableSummariesResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    tables: Optional[List[TableSummary]] = None
    """List of table summaries."""

    def as_dict(self) -> dict:
        """Serializes the ListTableSummariesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tables:
            body["tables"] = [v.as_dict() for v in self.tables]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListTableSummariesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tables:
            body["tables"] = self.tables
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListTableSummariesResponse:
        """Deserializes the ListTableSummariesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), tables=_repeated_dict(d, "tables", TableSummary))


@dataclass
class ListTablesResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    tables: Optional[List[TableInfo]] = None
    """An array of table information objects."""

    def as_dict(self) -> dict:
        """Serializes the ListTablesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tables:
            body["tables"] = [v.as_dict() for v in self.tables]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListTablesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tables:
            body["tables"] = self.tables
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListTablesResponse:
        """Deserializes the ListTablesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), tables=_repeated_dict(d, "tables", TableInfo))


@dataclass
class ListVolumesResponseContent:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request to retrieve the next page of
    results."""

    volumes: Optional[List[VolumeInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ListVolumesResponseContent into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.volumes:
            body["volumes"] = [v.as_dict() for v in self.volumes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListVolumesResponseContent into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.volumes:
            body["volumes"] = self.volumes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListVolumesResponseContent:
        """Deserializes the ListVolumesResponseContent from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), volumes=_repeated_dict(d, "volumes", VolumeInfo))


@dataclass
class MatchColumn:
    alias: Optional[str] = None
    """Optional alias of the matched column."""

    condition: Optional[str] = None
    """The condition expression used to match a table column."""

    def as_dict(self) -> dict:
        """Serializes the MatchColumn into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alias is not None:
            body["alias"] = self.alias
        if self.condition is not None:
            body["condition"] = self.condition
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MatchColumn into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alias is not None:
            body["alias"] = self.alias
        if self.condition is not None:
            body["condition"] = self.condition
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MatchColumn:
        """Deserializes the MatchColumn from a dictionary."""
        return cls(alias=d.get("alias", None), condition=d.get("condition", None))


class MatchType(Enum):
    """The artifact pattern matching type"""

    PREFIX_MATCH = "PREFIX_MATCH"


@dataclass
class MetastoreAssignment:
    workspace_id: int
    """The unique ID of the Databricks workspace."""

    metastore_id: str
    """The unique ID of the metastore."""

    default_catalog_name: Optional[str] = None
    """The name of the default catalog in the metastore. This field is deprecated. Please use "Default
    Namespace API" to configure the default catalog for a Databricks workspace."""

    def as_dict(self) -> dict:
        """Serializes the MetastoreAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default_catalog_name is not None:
            body["default_catalog_name"] = self.default_catalog_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MetastoreAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default_catalog_name is not None:
            body["default_catalog_name"] = self.default_catalog_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MetastoreAssignment:
        """Deserializes the MetastoreAssignment from a dictionary."""
        return cls(
            default_catalog_name=d.get("default_catalog_name", None),
            metastore_id=d.get("metastore_id", None),
            workspace_id=d.get("workspace_id", None),
        )


@dataclass
class MetastoreInfo:
    cloud: Optional[str] = None
    """Cloud vendor of the metastore home shard (e.g., `aws`, `azure`, `gcp`)."""

    created_at: Optional[int] = None
    """Time at which this metastore was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of metastore creator."""

    default_data_access_config_id: Optional[str] = None
    """Unique identifier of the metastore's (Default) Data Access Configuration."""

    delta_sharing_organization_name: Optional[str] = None
    """The organization name of a Delta Sharing entity, to be used in Databricks-to-Databricks Delta
    Sharing as the official name."""

    delta_sharing_recipient_token_lifetime_in_seconds: Optional[int] = None
    """The lifetime of delta sharing recipient token in seconds."""

    delta_sharing_scope: Optional[DeltaSharingScopeEnum] = None
    """The scope of Delta Sharing enabled for the metastore."""

    external_access_enabled: Optional[bool] = None
    """Whether to allow non-DBR clients to directly access entities under the metastore."""

    global_metastore_id: Optional[str] = None
    """Globally unique metastore ID across clouds and regions, of the form `cloud:region:metastore_id`."""

    metastore_id: Optional[str] = None
    """Unique identifier of metastore."""

    name: Optional[str] = None
    """The user-specified name of the metastore."""

    owner: Optional[str] = None
    """The owner of the metastore."""

    privilege_model_version: Optional[str] = None
    """Privilege model version of the metastore, of the form `major.minor` (e.g., `1.0`)."""

    region: Optional[str] = None
    """Cloud region which the metastore serves (e.g., `us-west-2`, `westus`)."""

    storage_root: Optional[str] = None
    """The storage root URL for metastore"""

    storage_root_credential_id: Optional[str] = None
    """UUID of storage credential to access the metastore storage_root."""

    storage_root_credential_name: Optional[str] = None
    """Name of the storage credential to access the metastore storage_root."""

    updated_at: Optional[int] = None
    """Time at which the metastore was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified the metastore."""

    def as_dict(self) -> dict:
        """Serializes the MetastoreInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.default_data_access_config_id is not None:
            body["default_data_access_config_id"] = self.default_data_access_config_id
        if self.delta_sharing_organization_name is not None:
            body["delta_sharing_organization_name"] = self.delta_sharing_organization_name
        if self.delta_sharing_recipient_token_lifetime_in_seconds is not None:
            body["delta_sharing_recipient_token_lifetime_in_seconds"] = (
                self.delta_sharing_recipient_token_lifetime_in_seconds
            )
        if self.delta_sharing_scope is not None:
            body["delta_sharing_scope"] = self.delta_sharing_scope.value
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.global_metastore_id is not None:
            body["global_metastore_id"] = self.global_metastore_id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.privilege_model_version is not None:
            body["privilege_model_version"] = self.privilege_model_version
        if self.region is not None:
            body["region"] = self.region
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.storage_root_credential_id is not None:
            body["storage_root_credential_id"] = self.storage_root_credential_id
        if self.storage_root_credential_name is not None:
            body["storage_root_credential_name"] = self.storage_root_credential_name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MetastoreInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.default_data_access_config_id is not None:
            body["default_data_access_config_id"] = self.default_data_access_config_id
        if self.delta_sharing_organization_name is not None:
            body["delta_sharing_organization_name"] = self.delta_sharing_organization_name
        if self.delta_sharing_recipient_token_lifetime_in_seconds is not None:
            body["delta_sharing_recipient_token_lifetime_in_seconds"] = (
                self.delta_sharing_recipient_token_lifetime_in_seconds
            )
        if self.delta_sharing_scope is not None:
            body["delta_sharing_scope"] = self.delta_sharing_scope
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.global_metastore_id is not None:
            body["global_metastore_id"] = self.global_metastore_id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.privilege_model_version is not None:
            body["privilege_model_version"] = self.privilege_model_version
        if self.region is not None:
            body["region"] = self.region
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.storage_root_credential_id is not None:
            body["storage_root_credential_id"] = self.storage_root_credential_id
        if self.storage_root_credential_name is not None:
            body["storage_root_credential_name"] = self.storage_root_credential_name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MetastoreInfo:
        """Deserializes the MetastoreInfo from a dictionary."""
        return cls(
            cloud=d.get("cloud", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            default_data_access_config_id=d.get("default_data_access_config_id", None),
            delta_sharing_organization_name=d.get("delta_sharing_organization_name", None),
            delta_sharing_recipient_token_lifetime_in_seconds=d.get(
                "delta_sharing_recipient_token_lifetime_in_seconds", None
            ),
            delta_sharing_scope=_enum(d, "delta_sharing_scope", DeltaSharingScopeEnum),
            external_access_enabled=d.get("external_access_enabled", None),
            global_metastore_id=d.get("global_metastore_id", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            privilege_model_version=d.get("privilege_model_version", None),
            region=d.get("region", None),
            storage_root=d.get("storage_root", None),
            storage_root_credential_id=d.get("storage_root_credential_id", None),
            storage_root_credential_name=d.get("storage_root_credential_name", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class ModelVersionInfo:
    aliases: Optional[List[RegisteredModelAlias]] = None
    """List of aliases associated with the model version"""

    catalog_name: Optional[str] = None
    """The name of the catalog containing the model version"""

    comment: Optional[str] = None
    """The comment attached to the model version"""

    created_at: Optional[int] = None

    created_by: Optional[str] = None
    """The identifier of the user who created the model version"""

    id: Optional[str] = None
    """The unique identifier of the model version"""

    metastore_id: Optional[str] = None
    """The unique identifier of the metastore containing the model version"""

    model_name: Optional[str] = None
    """The name of the parent registered model of the model version, relative to parent schema"""

    model_version_dependencies: Optional[DependencyList] = None
    """Model version dependencies, for feature-store packaged models"""

    run_id: Optional[str] = None
    """MLflow run ID used when creating the model version, if ``source`` was generated by an experiment
    run stored in an MLflow tracking server"""

    run_workspace_id: Optional[int] = None
    """ID of the Databricks workspace containing the MLflow run that generated this model version, if
    applicable"""

    schema_name: Optional[str] = None
    """The name of the schema containing the model version, relative to parent catalog"""

    source: Optional[str] = None
    """URI indicating the location of the source artifacts (files) for the model version"""

    status: Optional[ModelVersionInfoStatus] = None
    """Current status of the model version. Newly created model versions start in PENDING_REGISTRATION
    status, then move to READY status once the model version files are uploaded and the model
    version is finalized. Only model versions in READY status can be loaded for inference or served."""

    storage_location: Optional[str] = None
    """The storage location on the cloud under which model version data files are stored"""

    updated_at: Optional[int] = None

    updated_by: Optional[str] = None
    """The identifier of the user who updated the model version last time"""

    version: Optional[int] = None
    """Integer model version number, used to reference the model version in API requests."""

    def as_dict(self) -> dict:
        """Serializes the ModelVersionInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aliases:
            body["aliases"] = [v.as_dict() for v in self.aliases]
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.id is not None:
            body["id"] = self.id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.model_version_dependencies:
            body["model_version_dependencies"] = self.model_version_dependencies.as_dict()
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_workspace_id is not None:
            body["run_workspace_id"] = self.run_workspace_id
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.source is not None:
            body["source"] = self.source
        if self.status is not None:
            body["status"] = self.status.value
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelVersionInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aliases:
            body["aliases"] = self.aliases
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.id is not None:
            body["id"] = self.id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.model_version_dependencies:
            body["model_version_dependencies"] = self.model_version_dependencies
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_workspace_id is not None:
            body["run_workspace_id"] = self.run_workspace_id
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.source is not None:
            body["source"] = self.source
        if self.status is not None:
            body["status"] = self.status
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelVersionInfo:
        """Deserializes the ModelVersionInfo from a dictionary."""
        return cls(
            aliases=_repeated_dict(d, "aliases", RegisteredModelAlias),
            catalog_name=d.get("catalog_name", None),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            id=d.get("id", None),
            metastore_id=d.get("metastore_id", None),
            model_name=d.get("model_name", None),
            model_version_dependencies=_from_dict(d, "model_version_dependencies", DependencyList),
            run_id=d.get("run_id", None),
            run_workspace_id=d.get("run_workspace_id", None),
            schema_name=d.get("schema_name", None),
            source=d.get("source", None),
            status=_enum(d, "status", ModelVersionInfoStatus),
            storage_location=d.get("storage_location", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            version=d.get("version", None),
        )


class ModelVersionInfoStatus(Enum):

    FAILED_REGISTRATION = "FAILED_REGISTRATION"
    MODEL_VERSION_STATUS_UNKNOWN = "MODEL_VERSION_STATUS_UNKNOWN"
    PENDING_REGISTRATION = "PENDING_REGISTRATION"
    READY = "READY"


@dataclass
class MonitorCronSchedule:
    quartz_cron_expression: str
    """The expression that determines when to run the monitor. See [examples].
    
    [examples]: https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html"""

    timezone_id: str
    """The timezone id (e.g., ``PST``) in which to evaluate the quartz expression."""

    pause_status: Optional[MonitorCronSchedulePauseStatus] = None
    """Read only field that indicates whether a schedule is paused or not."""

    def as_dict(self) -> dict:
        """Serializes the MonitorCronSchedule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status.value
        if self.quartz_cron_expression is not None:
            body["quartz_cron_expression"] = self.quartz_cron_expression
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorCronSchedule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status
        if self.quartz_cron_expression is not None:
            body["quartz_cron_expression"] = self.quartz_cron_expression
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorCronSchedule:
        """Deserializes the MonitorCronSchedule from a dictionary."""
        return cls(
            pause_status=_enum(d, "pause_status", MonitorCronSchedulePauseStatus),
            quartz_cron_expression=d.get("quartz_cron_expression", None),
            timezone_id=d.get("timezone_id", None),
        )


class MonitorCronSchedulePauseStatus(Enum):
    """Source link:
    https://src.dev.databricks.com/databricks/universe/-/blob/elastic-spark-common/api/messages/schedule.proto
    Monitoring workflow schedule pause status."""

    PAUSED = "PAUSED"
    UNPAUSED = "UNPAUSED"
    UNSPECIFIED = "UNSPECIFIED"


@dataclass
class MonitorDataClassificationConfig:
    """Data classification related configuration."""

    enabled: Optional[bool] = None
    """Whether to enable data classification."""

    def as_dict(self) -> dict:
        """Serializes the MonitorDataClassificationConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorDataClassificationConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorDataClassificationConfig:
        """Deserializes the MonitorDataClassificationConfig from a dictionary."""
        return cls(enabled=d.get("enabled", None))


@dataclass
class MonitorDestination:
    email_addresses: Optional[List[str]] = None
    """The list of email addresses to send the notification to. A maximum of 5 email addresses is
    supported."""

    def as_dict(self) -> dict:
        """Serializes the MonitorDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.email_addresses:
            body["email_addresses"] = [v for v in self.email_addresses]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.email_addresses:
            body["email_addresses"] = self.email_addresses
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorDestination:
        """Deserializes the MonitorDestination from a dictionary."""
        return cls(email_addresses=d.get("email_addresses", None))


@dataclass
class MonitorInferenceLog:
    problem_type: MonitorInferenceLogProblemType
    """Problem type the model aims to solve."""

    timestamp_col: str
    """Column for the timestamp."""

    granularities: List[str]
    """List of granularities to use when aggregating data into time windows based on their timestamp."""

    prediction_col: str
    """Column for the prediction."""

    model_id_col: str
    """Column for the model identifier."""

    label_col: Optional[str] = None
    """Column for the label."""

    prediction_proba_col: Optional[str] = None
    """Column for prediction probabilities"""

    def as_dict(self) -> dict:
        """Serializes the MonitorInferenceLog into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.granularities:
            body["granularities"] = [v for v in self.granularities]
        if self.label_col is not None:
            body["label_col"] = self.label_col
        if self.model_id_col is not None:
            body["model_id_col"] = self.model_id_col
        if self.prediction_col is not None:
            body["prediction_col"] = self.prediction_col
        if self.prediction_proba_col is not None:
            body["prediction_proba_col"] = self.prediction_proba_col
        if self.problem_type is not None:
            body["problem_type"] = self.problem_type.value
        if self.timestamp_col is not None:
            body["timestamp_col"] = self.timestamp_col
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorInferenceLog into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.granularities:
            body["granularities"] = self.granularities
        if self.label_col is not None:
            body["label_col"] = self.label_col
        if self.model_id_col is not None:
            body["model_id_col"] = self.model_id_col
        if self.prediction_col is not None:
            body["prediction_col"] = self.prediction_col
        if self.prediction_proba_col is not None:
            body["prediction_proba_col"] = self.prediction_proba_col
        if self.problem_type is not None:
            body["problem_type"] = self.problem_type
        if self.timestamp_col is not None:
            body["timestamp_col"] = self.timestamp_col
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorInferenceLog:
        """Deserializes the MonitorInferenceLog from a dictionary."""
        return cls(
            granularities=d.get("granularities", None),
            label_col=d.get("label_col", None),
            model_id_col=d.get("model_id_col", None),
            prediction_col=d.get("prediction_col", None),
            prediction_proba_col=d.get("prediction_proba_col", None),
            problem_type=_enum(d, "problem_type", MonitorInferenceLogProblemType),
            timestamp_col=d.get("timestamp_col", None),
        )


class MonitorInferenceLogProblemType(Enum):

    PROBLEM_TYPE_CLASSIFICATION = "PROBLEM_TYPE_CLASSIFICATION"
    PROBLEM_TYPE_REGRESSION = "PROBLEM_TYPE_REGRESSION"


@dataclass
class MonitorInfo:
    output_schema_name: str
    """[Create:REQ Update:REQ] Schema where output tables are created. Needs to be in 2-level format
    {catalog}.{schema}"""

    table_name: str
    """[Create:ERR Update:IGN] UC table to monitor. Format: `catalog.schema.table_name`"""

    status: MonitorInfoStatus
    """[Create:ERR Update:IGN] The monitor status."""

    profile_metrics_table_name: str
    """[Create:ERR Update:IGN] Table that stores profile metrics data. Format:
    `catalog.schema.table_name`."""

    drift_metrics_table_name: str
    """[Create:ERR Update:IGN] Table that stores drift metrics data. Format:
    `catalog.schema.table_name`."""

    monitor_version: int
    """[Create:ERR Update:IGN] Represents the current monitor configuration version in use. The version
    will be represented in a numeric fashion (1,2,3...). The field has flexibility to take on
    negative values, which can indicate corrupted monitor_version numbers."""

    assets_dir: Optional[str] = None
    """[Create:REQ Update:IGN] Field for specifying the absolute path to a custom directory to store
    data-monitoring assets. Normally prepopulated to a default user location via UI and Python APIs."""

    baseline_table_name: Optional[str] = None
    """[Create:OPT Update:OPT] Baseline table name. Baseline data is used to compute drift from the
    data in the monitored `table_name`. The baseline table and the monitored table shall have the
    same schema."""

    custom_metrics: Optional[List[MonitorMetric]] = None
    """[Create:OPT Update:OPT] Custom metrics."""

    dashboard_id: Optional[str] = None
    """[Create:ERR Update:OPT] Id of dashboard that visualizes the computed metrics. This can be empty
    if the monitor is in PENDING state."""

    data_classification_config: Optional[MonitorDataClassificationConfig] = None
    """[Create:OPT Update:OPT] Data classification related config."""

    inference_log: Optional[MonitorInferenceLog] = None

    latest_monitor_failure_msg: Optional[str] = None
    """[Create:ERR Update:IGN] The latest error message for a monitor failure."""

    notifications: Optional[MonitorNotifications] = None
    """[Create:OPT Update:OPT] Field for specifying notification settings."""

    schedule: Optional[MonitorCronSchedule] = None
    """[Create:OPT Update:OPT] The monitor schedule."""

    slicing_exprs: Optional[List[str]] = None
    """[Create:OPT Update:OPT] List of column expressions to slice data with for targeted analysis. The
    data is grouped by each expression independently, resulting in a separate slice for each
    predicate and its complements. For example `slicing_exprs=[“col_1”, “col_2 > 10”]` will
    generate the following slices: two slices for `col_2 > 10` (True and False), and one slice per
    unique value in `col1`. For high-cardinality columns, only the top 100 unique values by
    frequency will generate slices."""

    snapshot: Optional[MonitorSnapshot] = None
    """Configuration for monitoring snapshot tables."""

    time_series: Optional[MonitorTimeSeries] = None
    """Configuration for monitoring time series tables."""

    def as_dict(self) -> dict:
        """Serializes the MonitorInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.assets_dir is not None:
            body["assets_dir"] = self.assets_dir
        if self.baseline_table_name is not None:
            body["baseline_table_name"] = self.baseline_table_name
        if self.custom_metrics:
            body["custom_metrics"] = [v.as_dict() for v in self.custom_metrics]
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.data_classification_config:
            body["data_classification_config"] = self.data_classification_config.as_dict()
        if self.drift_metrics_table_name is not None:
            body["drift_metrics_table_name"] = self.drift_metrics_table_name
        if self.inference_log:
            body["inference_log"] = self.inference_log.as_dict()
        if self.latest_monitor_failure_msg is not None:
            body["latest_monitor_failure_msg"] = self.latest_monitor_failure_msg
        if self.monitor_version is not None:
            body["monitor_version"] = self.monitor_version
        if self.notifications:
            body["notifications"] = self.notifications.as_dict()
        if self.output_schema_name is not None:
            body["output_schema_name"] = self.output_schema_name
        if self.profile_metrics_table_name is not None:
            body["profile_metrics_table_name"] = self.profile_metrics_table_name
        if self.schedule:
            body["schedule"] = self.schedule.as_dict()
        if self.slicing_exprs:
            body["slicing_exprs"] = [v for v in self.slicing_exprs]
        if self.snapshot:
            body["snapshot"] = self.snapshot.as_dict()
        if self.status is not None:
            body["status"] = self.status.value
        if self.table_name is not None:
            body["table_name"] = self.table_name
        if self.time_series:
            body["time_series"] = self.time_series.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.assets_dir is not None:
            body["assets_dir"] = self.assets_dir
        if self.baseline_table_name is not None:
            body["baseline_table_name"] = self.baseline_table_name
        if self.custom_metrics:
            body["custom_metrics"] = self.custom_metrics
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.data_classification_config:
            body["data_classification_config"] = self.data_classification_config
        if self.drift_metrics_table_name is not None:
            body["drift_metrics_table_name"] = self.drift_metrics_table_name
        if self.inference_log:
            body["inference_log"] = self.inference_log
        if self.latest_monitor_failure_msg is not None:
            body["latest_monitor_failure_msg"] = self.latest_monitor_failure_msg
        if self.monitor_version is not None:
            body["monitor_version"] = self.monitor_version
        if self.notifications:
            body["notifications"] = self.notifications
        if self.output_schema_name is not None:
            body["output_schema_name"] = self.output_schema_name
        if self.profile_metrics_table_name is not None:
            body["profile_metrics_table_name"] = self.profile_metrics_table_name
        if self.schedule:
            body["schedule"] = self.schedule
        if self.slicing_exprs:
            body["slicing_exprs"] = self.slicing_exprs
        if self.snapshot:
            body["snapshot"] = self.snapshot
        if self.status is not None:
            body["status"] = self.status
        if self.table_name is not None:
            body["table_name"] = self.table_name
        if self.time_series:
            body["time_series"] = self.time_series
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorInfo:
        """Deserializes the MonitorInfo from a dictionary."""
        return cls(
            assets_dir=d.get("assets_dir", None),
            baseline_table_name=d.get("baseline_table_name", None),
            custom_metrics=_repeated_dict(d, "custom_metrics", MonitorMetric),
            dashboard_id=d.get("dashboard_id", None),
            data_classification_config=_from_dict(d, "data_classification_config", MonitorDataClassificationConfig),
            drift_metrics_table_name=d.get("drift_metrics_table_name", None),
            inference_log=_from_dict(d, "inference_log", MonitorInferenceLog),
            latest_monitor_failure_msg=d.get("latest_monitor_failure_msg", None),
            monitor_version=d.get("monitor_version", None),
            notifications=_from_dict(d, "notifications", MonitorNotifications),
            output_schema_name=d.get("output_schema_name", None),
            profile_metrics_table_name=d.get("profile_metrics_table_name", None),
            schedule=_from_dict(d, "schedule", MonitorCronSchedule),
            slicing_exprs=d.get("slicing_exprs", None),
            snapshot=_from_dict(d, "snapshot", MonitorSnapshot),
            status=_enum(d, "status", MonitorInfoStatus),
            table_name=d.get("table_name", None),
            time_series=_from_dict(d, "time_series", MonitorTimeSeries),
        )


class MonitorInfoStatus(Enum):

    MONITOR_STATUS_ACTIVE = "MONITOR_STATUS_ACTIVE"
    MONITOR_STATUS_DELETE_PENDING = "MONITOR_STATUS_DELETE_PENDING"
    MONITOR_STATUS_ERROR = "MONITOR_STATUS_ERROR"
    MONITOR_STATUS_FAILED = "MONITOR_STATUS_FAILED"
    MONITOR_STATUS_PENDING = "MONITOR_STATUS_PENDING"


@dataclass
class MonitorMetric:
    """Custom metric definition."""

    name: str
    """Name of the metric in the output tables."""

    definition: str
    """Jinja template for a SQL expression that specifies how to compute the metric. See [create metric
    definition].
    
    [create metric definition]: https://docs.databricks.com/en/lakehouse-monitoring/custom-metrics.html#create-definition"""

    input_columns: List[str]
    """A list of column names in the input table the metric should be computed for. Can use
    ``":table"`` to indicate that the metric needs information from multiple columns."""

    output_data_type: str
    """The output type of the custom metric."""

    type: MonitorMetricType
    """Can only be one of ``"CUSTOM_METRIC_TYPE_AGGREGATE"``, ``"CUSTOM_METRIC_TYPE_DERIVED"``, or
    ``"CUSTOM_METRIC_TYPE_DRIFT"``. The ``"CUSTOM_METRIC_TYPE_AGGREGATE"`` and
    ``"CUSTOM_METRIC_TYPE_DERIVED"`` metrics are computed on a single table, whereas the
    ``"CUSTOM_METRIC_TYPE_DRIFT"`` compare metrics across baseline and input table, or across the
    two consecutive time windows. - CUSTOM_METRIC_TYPE_AGGREGATE: only depend on the existing
    columns in your table - CUSTOM_METRIC_TYPE_DERIVED: depend on previously computed aggregate
    metrics - CUSTOM_METRIC_TYPE_DRIFT: depend on previously computed aggregate or derived metrics"""

    def as_dict(self) -> dict:
        """Serializes the MonitorMetric into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.definition is not None:
            body["definition"] = self.definition
        if self.input_columns:
            body["input_columns"] = [v for v in self.input_columns]
        if self.name is not None:
            body["name"] = self.name
        if self.output_data_type is not None:
            body["output_data_type"] = self.output_data_type
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorMetric into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.definition is not None:
            body["definition"] = self.definition
        if self.input_columns:
            body["input_columns"] = self.input_columns
        if self.name is not None:
            body["name"] = self.name
        if self.output_data_type is not None:
            body["output_data_type"] = self.output_data_type
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorMetric:
        """Deserializes the MonitorMetric from a dictionary."""
        return cls(
            definition=d.get("definition", None),
            input_columns=d.get("input_columns", None),
            name=d.get("name", None),
            output_data_type=d.get("output_data_type", None),
            type=_enum(d, "type", MonitorMetricType),
        )


class MonitorMetricType(Enum):
    """Can only be one of ``\"CUSTOM_METRIC_TYPE_AGGREGATE\"``, ``\"CUSTOM_METRIC_TYPE_DERIVED\"``, or
    ``\"CUSTOM_METRIC_TYPE_DRIFT\"``. The ``\"CUSTOM_METRIC_TYPE_AGGREGATE\"`` and
    ``\"CUSTOM_METRIC_TYPE_DERIVED\"`` metrics are computed on a single table, whereas the
    ``\"CUSTOM_METRIC_TYPE_DRIFT\"`` compare metrics across baseline and input table, or across the
    two consecutive time windows. - CUSTOM_METRIC_TYPE_AGGREGATE: only depend on the existing
    columns in your table - CUSTOM_METRIC_TYPE_DERIVED: depend on previously computed aggregate
    metrics - CUSTOM_METRIC_TYPE_DRIFT: depend on previously computed aggregate or derived metrics"""

    CUSTOM_METRIC_TYPE_AGGREGATE = "CUSTOM_METRIC_TYPE_AGGREGATE"
    CUSTOM_METRIC_TYPE_DERIVED = "CUSTOM_METRIC_TYPE_DERIVED"
    CUSTOM_METRIC_TYPE_DRIFT = "CUSTOM_METRIC_TYPE_DRIFT"


@dataclass
class MonitorNotifications:
    on_failure: Optional[MonitorDestination] = None
    """Destinations to send notifications on failure/timeout."""

    on_new_classification_tag_detected: Optional[MonitorDestination] = None
    """Destinations to send notifications on new classification tag detected."""

    def as_dict(self) -> dict:
        """Serializes the MonitorNotifications into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.on_failure:
            body["on_failure"] = self.on_failure.as_dict()
        if self.on_new_classification_tag_detected:
            body["on_new_classification_tag_detected"] = self.on_new_classification_tag_detected.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorNotifications into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.on_failure:
            body["on_failure"] = self.on_failure
        if self.on_new_classification_tag_detected:
            body["on_new_classification_tag_detected"] = self.on_new_classification_tag_detected
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorNotifications:
        """Deserializes the MonitorNotifications from a dictionary."""
        return cls(
            on_failure=_from_dict(d, "on_failure", MonitorDestination),
            on_new_classification_tag_detected=_from_dict(d, "on_new_classification_tag_detected", MonitorDestination),
        )


@dataclass
class MonitorRefreshInfo:
    refresh_id: int
    """Unique id of the refresh operation."""

    state: MonitorRefreshInfoState
    """The current state of the refresh."""

    start_time_ms: int
    """Time at which refresh operation was initiated (milliseconds since 1/1/1970 UTC)."""

    end_time_ms: Optional[int] = None
    """Time at which refresh operation completed (milliseconds since 1/1/1970 UTC)."""

    message: Optional[str] = None
    """An optional message to give insight into the current state of the job (e.g. FAILURE messages)."""

    trigger: Optional[MonitorRefreshInfoTrigger] = None
    """The method by which the refresh was triggered."""

    def as_dict(self) -> dict:
        """Serializes the MonitorRefreshInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.end_time_ms is not None:
            body["end_time_ms"] = self.end_time_ms
        if self.message is not None:
            body["message"] = self.message
        if self.refresh_id is not None:
            body["refresh_id"] = self.refresh_id
        if self.start_time_ms is not None:
            body["start_time_ms"] = self.start_time_ms
        if self.state is not None:
            body["state"] = self.state.value
        if self.trigger is not None:
            body["trigger"] = self.trigger.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorRefreshInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.end_time_ms is not None:
            body["end_time_ms"] = self.end_time_ms
        if self.message is not None:
            body["message"] = self.message
        if self.refresh_id is not None:
            body["refresh_id"] = self.refresh_id
        if self.start_time_ms is not None:
            body["start_time_ms"] = self.start_time_ms
        if self.state is not None:
            body["state"] = self.state
        if self.trigger is not None:
            body["trigger"] = self.trigger
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorRefreshInfo:
        """Deserializes the MonitorRefreshInfo from a dictionary."""
        return cls(
            end_time_ms=d.get("end_time_ms", None),
            message=d.get("message", None),
            refresh_id=d.get("refresh_id", None),
            start_time_ms=d.get("start_time_ms", None),
            state=_enum(d, "state", MonitorRefreshInfoState),
            trigger=_enum(d, "trigger", MonitorRefreshInfoTrigger),
        )


class MonitorRefreshInfoState(Enum):
    """The current state of the refresh."""

    CANCELED = "CANCELED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"


class MonitorRefreshInfoTrigger(Enum):

    MANUAL = "MANUAL"
    SCHEDULE = "SCHEDULE"
    UNKNOWN_TRIGGER = "UNKNOWN_TRIGGER"


@dataclass
class MonitorRefreshListResponse:
    refreshes: Optional[List[MonitorRefreshInfo]] = None
    """List of refreshes."""

    def as_dict(self) -> dict:
        """Serializes the MonitorRefreshListResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.refreshes:
            body["refreshes"] = [v.as_dict() for v in self.refreshes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorRefreshListResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.refreshes:
            body["refreshes"] = self.refreshes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorRefreshListResponse:
        """Deserializes the MonitorRefreshListResponse from a dictionary."""
        return cls(refreshes=_repeated_dict(d, "refreshes", MonitorRefreshInfo))


@dataclass
class MonitorSnapshot:
    """Snapshot analysis configuration"""

    def as_dict(self) -> dict:
        """Serializes the MonitorSnapshot into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorSnapshot into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorSnapshot:
        """Deserializes the MonitorSnapshot from a dictionary."""
        return cls()


@dataclass
class MonitorTimeSeries:
    """Time series analysis configuration."""

    timestamp_col: str
    """Column for the timestamp."""

    granularities: List[str]
    """Granularities for aggregating data into time windows based on their timestamp. Currently the
    following static granularities are supported: {``\"5 minutes\"``, ``\"30 minutes\"``, ``\"1
    hour\"``, ``\"1 day\"``, ``\"\u003cn\u003e week(s)\"``, ``\"1 month\"``, ``\"1 year\"``}."""

    def as_dict(self) -> dict:
        """Serializes the MonitorTimeSeries into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.granularities:
            body["granularities"] = [v for v in self.granularities]
        if self.timestamp_col is not None:
            body["timestamp_col"] = self.timestamp_col
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MonitorTimeSeries into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.granularities:
            body["granularities"] = self.granularities
        if self.timestamp_col is not None:
            body["timestamp_col"] = self.timestamp_col
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MonitorTimeSeries:
        """Deserializes the MonitorTimeSeries from a dictionary."""
        return cls(granularities=d.get("granularities", None), timestamp_col=d.get("timestamp_col", None))


@dataclass
class NamedTableConstraint:
    name: str
    """The name of the constraint."""

    def as_dict(self) -> dict:
        """Serializes the NamedTableConstraint into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NamedTableConstraint into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NamedTableConstraint:
        """Deserializes the NamedTableConstraint from a dictionary."""
        return cls(name=d.get("name", None))


@dataclass
class NotificationDestination:
    destination_id: Optional[str] = None
    """The identifier for the destination. This is the email address for EMAIL destinations, the URL
    for URL destinations, or the unique Databricks notification destination ID for all other
    external destinations."""

    destination_type: Optional[DestinationType] = None
    """The type of the destination."""

    special_destination: Optional[SpecialDestination] = None
    """This field is used to denote whether the destination is the email of the owner of the securable
    object. The special destination cannot be assigned to a securable and only represents the
    default destination of the securable. The securable types that support default special
    destinations are: "catalog", "external_location", "connection", "credential", and "metastore".
    The **destination_type** of a **special_destination** is always EMAIL."""

    def as_dict(self) -> dict:
        """Serializes the NotificationDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.destination_type is not None:
            body["destination_type"] = self.destination_type.value
        if self.special_destination is not None:
            body["special_destination"] = self.special_destination.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotificationDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.destination_type is not None:
            body["destination_type"] = self.destination_type
        if self.special_destination is not None:
            body["special_destination"] = self.special_destination
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotificationDestination:
        """Deserializes the NotificationDestination from a dictionary."""
        return cls(
            destination_id=d.get("destination_id", None),
            destination_type=_enum(d, "destination_type", DestinationType),
            special_destination=_enum(d, "special_destination", SpecialDestination),
        )


@dataclass
class OnlineTable:
    """Online Table information."""

    name: Optional[str] = None
    """Full three-part (catalog, schema, table) name of the table."""

    spec: Optional[OnlineTableSpec] = None
    """Specification of the online table."""

    status: Optional[OnlineTableStatus] = None
    """Online Table data synchronization status"""

    table_serving_url: Optional[str] = None
    """Data serving REST API URL for this table"""

    unity_catalog_provisioning_state: Optional[ProvisioningInfoState] = None
    """The provisioning state of the online table entity in Unity Catalog. This is distinct from the
    state of the data synchronization pipeline (i.e. the table may be in "ACTIVE" but the pipeline
    may be in "PROVISIONING" as it runs asynchronously)."""

    def as_dict(self) -> dict:
        """Serializes the OnlineTable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.table_serving_url is not None:
            body["table_serving_url"] = self.table_serving_url
        if self.unity_catalog_provisioning_state is not None:
            body["unity_catalog_provisioning_state"] = self.unity_catalog_provisioning_state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OnlineTable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.spec:
            body["spec"] = self.spec
        if self.status:
            body["status"] = self.status
        if self.table_serving_url is not None:
            body["table_serving_url"] = self.table_serving_url
        if self.unity_catalog_provisioning_state is not None:
            body["unity_catalog_provisioning_state"] = self.unity_catalog_provisioning_state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OnlineTable:
        """Deserializes the OnlineTable from a dictionary."""
        return cls(
            name=d.get("name", None),
            spec=_from_dict(d, "spec", OnlineTableSpec),
            status=_from_dict(d, "status", OnlineTableStatus),
            table_serving_url=d.get("table_serving_url", None),
            unity_catalog_provisioning_state=_enum(d, "unity_catalog_provisioning_state", ProvisioningInfoState),
        )


@dataclass
class OnlineTableSpec:
    """Specification of an online table."""

    perform_full_copy: Optional[bool] = None
    """Whether to create a full-copy pipeline -- a pipeline that stops after creates a full copy of the
    source table upon initialization and does not process any change data feeds (CDFs) afterwards.
    The pipeline can still be manually triggered afterwards, but it always perform a full copy of
    the source table and there are no incremental updates. This mode is useful for syncing views or
    tables without CDFs to online tables. Note that the full-copy pipeline only supports "triggered"
    scheduling policy."""

    pipeline_id: Optional[str] = None
    """ID of the associated pipeline. Generated by the server - cannot be set by the caller."""

    primary_key_columns: Optional[List[str]] = None
    """Primary Key columns to be used for data insert/update in the destination."""

    run_continuously: Optional[OnlineTableSpecContinuousSchedulingPolicy] = None
    """Pipeline runs continuously after generating the initial data."""

    run_triggered: Optional[OnlineTableSpecTriggeredSchedulingPolicy] = None
    """Pipeline stops after generating the initial data and can be triggered later (manually, through a
    cron job or through data triggers)"""

    source_table_full_name: Optional[str] = None
    """Three-part (catalog, schema, table) name of the source Delta table."""

    timeseries_key: Optional[str] = None
    """Time series key to deduplicate (tie-break) rows with the same primary key."""

    def as_dict(self) -> dict:
        """Serializes the OnlineTableSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.perform_full_copy is not None:
            body["perform_full_copy"] = self.perform_full_copy
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.primary_key_columns:
            body["primary_key_columns"] = [v for v in self.primary_key_columns]
        if self.run_continuously:
            body["run_continuously"] = self.run_continuously.as_dict()
        if self.run_triggered:
            body["run_triggered"] = self.run_triggered.as_dict()
        if self.source_table_full_name is not None:
            body["source_table_full_name"] = self.source_table_full_name
        if self.timeseries_key is not None:
            body["timeseries_key"] = self.timeseries_key
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OnlineTableSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.perform_full_copy is not None:
            body["perform_full_copy"] = self.perform_full_copy
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.primary_key_columns:
            body["primary_key_columns"] = self.primary_key_columns
        if self.run_continuously:
            body["run_continuously"] = self.run_continuously
        if self.run_triggered:
            body["run_triggered"] = self.run_triggered
        if self.source_table_full_name is not None:
            body["source_table_full_name"] = self.source_table_full_name
        if self.timeseries_key is not None:
            body["timeseries_key"] = self.timeseries_key
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OnlineTableSpec:
        """Deserializes the OnlineTableSpec from a dictionary."""
        return cls(
            perform_full_copy=d.get("perform_full_copy", None),
            pipeline_id=d.get("pipeline_id", None),
            primary_key_columns=d.get("primary_key_columns", None),
            run_continuously=_from_dict(d, "run_continuously", OnlineTableSpecContinuousSchedulingPolicy),
            run_triggered=_from_dict(d, "run_triggered", OnlineTableSpecTriggeredSchedulingPolicy),
            source_table_full_name=d.get("source_table_full_name", None),
            timeseries_key=d.get("timeseries_key", None),
        )


@dataclass
class OnlineTableSpecContinuousSchedulingPolicy:
    def as_dict(self) -> dict:
        """Serializes the OnlineTableSpecContinuousSchedulingPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OnlineTableSpecContinuousSchedulingPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OnlineTableSpecContinuousSchedulingPolicy:
        """Deserializes the OnlineTableSpecContinuousSchedulingPolicy from a dictionary."""
        return cls()


@dataclass
class OnlineTableSpecTriggeredSchedulingPolicy:
    def as_dict(self) -> dict:
        """Serializes the OnlineTableSpecTriggeredSchedulingPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OnlineTableSpecTriggeredSchedulingPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OnlineTableSpecTriggeredSchedulingPolicy:
        """Deserializes the OnlineTableSpecTriggeredSchedulingPolicy from a dictionary."""
        return cls()


class OnlineTableState(Enum):
    """The state of an online table."""

    OFFLINE = "OFFLINE"
    OFFLINE_FAILED = "OFFLINE_FAILED"
    ONLINE = "ONLINE"
    ONLINE_CONTINUOUS_UPDATE = "ONLINE_CONTINUOUS_UPDATE"
    ONLINE_NO_PENDING_UPDATE = "ONLINE_NO_PENDING_UPDATE"
    ONLINE_PIPELINE_FAILED = "ONLINE_PIPELINE_FAILED"
    ONLINE_TRIGGERED_UPDATE = "ONLINE_TRIGGERED_UPDATE"
    ONLINE_UPDATING_PIPELINE_RESOURCES = "ONLINE_UPDATING_PIPELINE_RESOURCES"
    PROVISIONING = "PROVISIONING"
    PROVISIONING_INITIAL_SNAPSHOT = "PROVISIONING_INITIAL_SNAPSHOT"
    PROVISIONING_PIPELINE_RESOURCES = "PROVISIONING_PIPELINE_RESOURCES"


@dataclass
class OnlineTableStatus:
    """Status of an online table."""

    continuous_update_status: Optional[ContinuousUpdateStatus] = None

    detailed_state: Optional[OnlineTableState] = None
    """The state of the online table."""

    failed_status: Optional[FailedStatus] = None

    message: Optional[str] = None
    """A text description of the current state of the online table."""

    provisioning_status: Optional[ProvisioningStatus] = None

    triggered_update_status: Optional[TriggeredUpdateStatus] = None

    def as_dict(self) -> dict:
        """Serializes the OnlineTableStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.continuous_update_status:
            body["continuous_update_status"] = self.continuous_update_status.as_dict()
        if self.detailed_state is not None:
            body["detailed_state"] = self.detailed_state.value
        if self.failed_status:
            body["failed_status"] = self.failed_status.as_dict()
        if self.message is not None:
            body["message"] = self.message
        if self.provisioning_status:
            body["provisioning_status"] = self.provisioning_status.as_dict()
        if self.triggered_update_status:
            body["triggered_update_status"] = self.triggered_update_status.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OnlineTableStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.continuous_update_status:
            body["continuous_update_status"] = self.continuous_update_status
        if self.detailed_state is not None:
            body["detailed_state"] = self.detailed_state
        if self.failed_status:
            body["failed_status"] = self.failed_status
        if self.message is not None:
            body["message"] = self.message
        if self.provisioning_status:
            body["provisioning_status"] = self.provisioning_status
        if self.triggered_update_status:
            body["triggered_update_status"] = self.triggered_update_status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OnlineTableStatus:
        """Deserializes the OnlineTableStatus from a dictionary."""
        return cls(
            continuous_update_status=_from_dict(d, "continuous_update_status", ContinuousUpdateStatus),
            detailed_state=_enum(d, "detailed_state", OnlineTableState),
            failed_status=_from_dict(d, "failed_status", FailedStatus),
            message=d.get("message", None),
            provisioning_status=_from_dict(d, "provisioning_status", ProvisioningStatus),
            triggered_update_status=_from_dict(d, "triggered_update_status", TriggeredUpdateStatus),
        )


@dataclass
class OptionSpec:
    """Spec of an allowed option on a securable kind and its attributes. This is mostly used by UI to
    provide user friendly hints and descriptions in order to facilitate the securable creation
    process."""

    allowed_values: Optional[List[str]] = None
    """For drop down / radio button selections, UI will want to know the possible input values, it can
    also be used by other option types to limit input selections."""

    default_value: Optional[str] = None
    """The default value of the option, for example, value '443' for 'port' option."""

    description: Optional[str] = None
    """A concise user facing description of what the input value of this option should look like."""

    hint: Optional[str] = None
    """The hint is used on the UI to suggest what the input value can possibly be like, for example:
    example.com for 'host' option. Unlike default value, it will not be applied automatically
    without user input."""

    is_copiable: Optional[bool] = None
    """Indicates whether an option should be displayed with copy button on the UI."""

    is_creatable: Optional[bool] = None
    """Indicates whether an option can be provided by users in the create/update path of an entity."""

    is_hidden: Optional[bool] = None
    """Is the option value not user settable and is thus not shown on the UI."""

    is_loggable: Optional[bool] = None
    """Specifies whether this option is safe to log, i.e. no sensitive information."""

    is_required: Optional[bool] = None
    """Is the option required."""

    is_secret: Optional[bool] = None
    """Is the option value considered secret and thus redacted on the UI."""

    is_updatable: Optional[bool] = None
    """Is the option updatable by users."""

    name: Optional[str] = None
    """The unique name of the option."""

    oauth_stage: Optional[OptionSpecOauthStage] = None
    """Specifies when the option value is displayed on the UI within the OAuth flow."""

    type: Optional[OptionSpecOptionType] = None
    """The type of the option."""

    def as_dict(self) -> dict:
        """Serializes the OptionSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.allowed_values:
            body["allowed_values"] = [v for v in self.allowed_values]
        if self.default_value is not None:
            body["default_value"] = self.default_value
        if self.description is not None:
            body["description"] = self.description
        if self.hint is not None:
            body["hint"] = self.hint
        if self.is_copiable is not None:
            body["is_copiable"] = self.is_copiable
        if self.is_creatable is not None:
            body["is_creatable"] = self.is_creatable
        if self.is_hidden is not None:
            body["is_hidden"] = self.is_hidden
        if self.is_loggable is not None:
            body["is_loggable"] = self.is_loggable
        if self.is_required is not None:
            body["is_required"] = self.is_required
        if self.is_secret is not None:
            body["is_secret"] = self.is_secret
        if self.is_updatable is not None:
            body["is_updatable"] = self.is_updatable
        if self.name is not None:
            body["name"] = self.name
        if self.oauth_stage is not None:
            body["oauth_stage"] = self.oauth_stage.value
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OptionSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.allowed_values:
            body["allowed_values"] = self.allowed_values
        if self.default_value is not None:
            body["default_value"] = self.default_value
        if self.description is not None:
            body["description"] = self.description
        if self.hint is not None:
            body["hint"] = self.hint
        if self.is_copiable is not None:
            body["is_copiable"] = self.is_copiable
        if self.is_creatable is not None:
            body["is_creatable"] = self.is_creatable
        if self.is_hidden is not None:
            body["is_hidden"] = self.is_hidden
        if self.is_loggable is not None:
            body["is_loggable"] = self.is_loggable
        if self.is_required is not None:
            body["is_required"] = self.is_required
        if self.is_secret is not None:
            body["is_secret"] = self.is_secret
        if self.is_updatable is not None:
            body["is_updatable"] = self.is_updatable
        if self.name is not None:
            body["name"] = self.name
        if self.oauth_stage is not None:
            body["oauth_stage"] = self.oauth_stage
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OptionSpec:
        """Deserializes the OptionSpec from a dictionary."""
        return cls(
            allowed_values=d.get("allowed_values", None),
            default_value=d.get("default_value", None),
            description=d.get("description", None),
            hint=d.get("hint", None),
            is_copiable=d.get("is_copiable", None),
            is_creatable=d.get("is_creatable", None),
            is_hidden=d.get("is_hidden", None),
            is_loggable=d.get("is_loggable", None),
            is_required=d.get("is_required", None),
            is_secret=d.get("is_secret", None),
            is_updatable=d.get("is_updatable", None),
            name=d.get("name", None),
            oauth_stage=_enum(d, "oauth_stage", OptionSpecOauthStage),
            type=_enum(d, "type", OptionSpecOptionType),
        )


class OptionSpecOauthStage(Enum):
    """During the OAuth flow, specifies which stage the option should be displayed in the UI.
    OAUTH_STAGE_UNSPECIFIED is the default value for options unrelated to the OAuth flow.
    BEFORE_AUTHORIZATION_CODE corresponds to options necessary to initiate the OAuth process.
    BEFORE_ACCESS_TOKEN corresponds to options that are necessary to create a foreign connection,
    but that should be displayed after the authorization code has already been received."""

    BEFORE_ACCESS_TOKEN = "BEFORE_ACCESS_TOKEN"
    BEFORE_AUTHORIZATION_CODE = "BEFORE_AUTHORIZATION_CODE"


class OptionSpecOptionType(Enum):
    """Type of the option, we purposely follow JavaScript types so that the UI can map the options to
    JS types. https://www.w3schools.com/js/js_datatypes.asp Enum is a special case that it's just
    string with selections."""

    OPTION_BIGINT = "OPTION_BIGINT"
    OPTION_BOOLEAN = "OPTION_BOOLEAN"
    OPTION_ENUM = "OPTION_ENUM"
    OPTION_MULTILINE_STRING = "OPTION_MULTILINE_STRING"
    OPTION_NUMBER = "OPTION_NUMBER"
    OPTION_SERVICE_CREDENTIAL = "OPTION_SERVICE_CREDENTIAL"
    OPTION_STRING = "OPTION_STRING"


class PathOperation(Enum):

    PATH_CREATE_TABLE = "PATH_CREATE_TABLE"
    PATH_READ = "PATH_READ"
    PATH_READ_WRITE = "PATH_READ_WRITE"


@dataclass
class PermissionsChange:
    add: Optional[List[Privilege]] = None
    """The set of privileges to add."""

    principal: Optional[str] = None
    """The principal whose privileges we are changing. Only one of principal or principal_id should be
    specified, never both at the same time."""

    remove: Optional[List[Privilege]] = None
    """The set of privileges to remove."""

    def as_dict(self) -> dict:
        """Serializes the PermissionsChange into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.add:
            body["add"] = [v.value for v in self.add]
        if self.principal is not None:
            body["principal"] = self.principal
        if self.remove:
            body["remove"] = [v.value for v in self.remove]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PermissionsChange into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.add:
            body["add"] = self.add
        if self.principal is not None:
            body["principal"] = self.principal
        if self.remove:
            body["remove"] = self.remove
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PermissionsChange:
        """Deserializes the PermissionsChange from a dictionary."""
        return cls(
            add=_repeated_enum(d, "add", Privilege),
            principal=d.get("principal", None),
            remove=_repeated_enum(d, "remove", Privilege),
        )


@dataclass
class PipelineProgress:
    """Progress information of the Online Table data synchronization pipeline."""

    estimated_completion_time_seconds: Optional[float] = None
    """The estimated time remaining to complete this update in seconds."""

    latest_version_currently_processing: Optional[int] = None
    """The source table Delta version that was last processed by the pipeline. The pipeline may not
    have completely processed this version yet."""

    sync_progress_completion: Optional[float] = None
    """The completion ratio of this update. This is a number between 0 and 1."""

    synced_row_count: Optional[int] = None
    """The number of rows that have been synced in this update."""

    total_row_count: Optional[int] = None
    """The total number of rows that need to be synced in this update. This number may be an estimate."""

    def as_dict(self) -> dict:
        """Serializes the PipelineProgress into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.estimated_completion_time_seconds is not None:
            body["estimated_completion_time_seconds"] = self.estimated_completion_time_seconds
        if self.latest_version_currently_processing is not None:
            body["latest_version_currently_processing"] = self.latest_version_currently_processing
        if self.sync_progress_completion is not None:
            body["sync_progress_completion"] = self.sync_progress_completion
        if self.synced_row_count is not None:
            body["synced_row_count"] = self.synced_row_count
        if self.total_row_count is not None:
            body["total_row_count"] = self.total_row_count
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineProgress into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.estimated_completion_time_seconds is not None:
            body["estimated_completion_time_seconds"] = self.estimated_completion_time_seconds
        if self.latest_version_currently_processing is not None:
            body["latest_version_currently_processing"] = self.latest_version_currently_processing
        if self.sync_progress_completion is not None:
            body["sync_progress_completion"] = self.sync_progress_completion
        if self.synced_row_count is not None:
            body["synced_row_count"] = self.synced_row_count
        if self.total_row_count is not None:
            body["total_row_count"] = self.total_row_count
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineProgress:
        """Deserializes the PipelineProgress from a dictionary."""
        return cls(
            estimated_completion_time_seconds=d.get("estimated_completion_time_seconds", None),
            latest_version_currently_processing=d.get("latest_version_currently_processing", None),
            sync_progress_completion=d.get("sync_progress_completion", None),
            synced_row_count=d.get("synced_row_count", None),
            total_row_count=d.get("total_row_count", None),
        )


@dataclass
class PolicyFunctionArgument:
    """A positional argument passed to a row filter or column mask function. Distinguishes between
    column references and literals."""

    column: Optional[str] = None
    """A column reference."""

    constant: Optional[str] = None
    """A constant literal."""

    def as_dict(self) -> dict:
        """Serializes the PolicyFunctionArgument into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.column is not None:
            body["column"] = self.column
        if self.constant is not None:
            body["constant"] = self.constant
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PolicyFunctionArgument into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.column is not None:
            body["column"] = self.column
        if self.constant is not None:
            body["constant"] = self.constant
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PolicyFunctionArgument:
        """Deserializes the PolicyFunctionArgument from a dictionary."""
        return cls(column=d.get("column", None), constant=d.get("constant", None))


@dataclass
class PolicyInfo:
    to_principals: List[str]
    """List of user or group names that the policy applies to. Required on create and optional on
    update."""

    for_securable_type: SecurableType
    """Type of securables that the policy should take effect on. Only `TABLE` is supported at this
    moment. Required on create and optional on update."""

    policy_type: PolicyType
    """Type of the policy. Required on create."""

    column_mask: Optional[ColumnMaskOptions] = None
    """Options for column mask policies. Valid only if `policy_type` is `POLICY_TYPE_COLUMN_MASK`.
    Required on create and optional on update. When specified on update, the new options will
    replace the existing options as a whole."""

    comment: Optional[str] = None
    """Optional description of the policy."""

    created_at: Optional[int] = None
    """Time at which the policy was created, in epoch milliseconds. Output only."""

    created_by: Optional[str] = None
    """Username of the user who created the policy. Output only."""

    except_principals: Optional[List[str]] = None
    """Optional list of user or group names that should be excluded from the policy."""

    id: Optional[str] = None
    """Unique identifier of the policy. This field is output only and is generated by the system."""

    match_columns: Optional[List[MatchColumn]] = None
    """Optional list of condition expressions used to match table columns. Only valid when
    `for_securable_type` is `TABLE`. When specified, the policy only applies to tables whose columns
    satisfy all match conditions."""

    name: Optional[str] = None
    """Name of the policy. Required on create and optional on update. To rename the policy, set `name`
    to a different value on update."""

    on_securable_fullname: Optional[str] = None
    """Full name of the securable on which the policy is defined. Required on create."""

    on_securable_type: Optional[SecurableType] = None
    """Type of the securable on which the policy is defined. Only `CATALOG`, `SCHEMA` and `TABLE` are
    supported at this moment. Required on create."""

    row_filter: Optional[RowFilterOptions] = None
    """Options for row filter policies. Valid only if `policy_type` is `POLICY_TYPE_ROW_FILTER`.
    Required on create and optional on update. When specified on update, the new options will
    replace the existing options as a whole."""

    updated_at: Optional[int] = None
    """Time at which the policy was last modified, in epoch milliseconds. Output only."""

    updated_by: Optional[str] = None
    """Username of the user who last modified the policy. Output only."""

    when_condition: Optional[str] = None
    """Optional condition when the policy should take effect."""

    def as_dict(self) -> dict:
        """Serializes the PolicyInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.column_mask:
            body["column_mask"] = self.column_mask.as_dict()
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.except_principals:
            body["except_principals"] = [v for v in self.except_principals]
        if self.for_securable_type is not None:
            body["for_securable_type"] = self.for_securable_type.value
        if self.id is not None:
            body["id"] = self.id
        if self.match_columns:
            body["match_columns"] = [v.as_dict() for v in self.match_columns]
        if self.name is not None:
            body["name"] = self.name
        if self.on_securable_fullname is not None:
            body["on_securable_fullname"] = self.on_securable_fullname
        if self.on_securable_type is not None:
            body["on_securable_type"] = self.on_securable_type.value
        if self.policy_type is not None:
            body["policy_type"] = self.policy_type.value
        if self.row_filter:
            body["row_filter"] = self.row_filter.as_dict()
        if self.to_principals:
            body["to_principals"] = [v for v in self.to_principals]
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.when_condition is not None:
            body["when_condition"] = self.when_condition
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PolicyInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.column_mask:
            body["column_mask"] = self.column_mask
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.except_principals:
            body["except_principals"] = self.except_principals
        if self.for_securable_type is not None:
            body["for_securable_type"] = self.for_securable_type
        if self.id is not None:
            body["id"] = self.id
        if self.match_columns:
            body["match_columns"] = self.match_columns
        if self.name is not None:
            body["name"] = self.name
        if self.on_securable_fullname is not None:
            body["on_securable_fullname"] = self.on_securable_fullname
        if self.on_securable_type is not None:
            body["on_securable_type"] = self.on_securable_type
        if self.policy_type is not None:
            body["policy_type"] = self.policy_type
        if self.row_filter:
            body["row_filter"] = self.row_filter
        if self.to_principals:
            body["to_principals"] = self.to_principals
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.when_condition is not None:
            body["when_condition"] = self.when_condition
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PolicyInfo:
        """Deserializes the PolicyInfo from a dictionary."""
        return cls(
            column_mask=_from_dict(d, "column_mask", ColumnMaskOptions),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            except_principals=d.get("except_principals", None),
            for_securable_type=_enum(d, "for_securable_type", SecurableType),
            id=d.get("id", None),
            match_columns=_repeated_dict(d, "match_columns", MatchColumn),
            name=d.get("name", None),
            on_securable_fullname=d.get("on_securable_fullname", None),
            on_securable_type=_enum(d, "on_securable_type", SecurableType),
            policy_type=_enum(d, "policy_type", PolicyType),
            row_filter=_from_dict(d, "row_filter", RowFilterOptions),
            to_principals=d.get("to_principals", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            when_condition=d.get("when_condition", None),
        )


class PolicyType(Enum):

    POLICY_TYPE_COLUMN_MASK = "POLICY_TYPE_COLUMN_MASK"
    POLICY_TYPE_ROW_FILTER = "POLICY_TYPE_ROW_FILTER"


@dataclass
class PrimaryKeyConstraint:
    name: str
    """The name of the constraint."""

    child_columns: List[str]
    """Column names for this constraint."""

    rely: Optional[bool] = None
    """True if the constraint is RELY, false or unset if NORELY."""

    timeseries_columns: Optional[List[str]] = None
    """Column names that represent a timeseries."""

    def as_dict(self) -> dict:
        """Serializes the PrimaryKeyConstraint into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.child_columns:
            body["child_columns"] = [v for v in self.child_columns]
        if self.name is not None:
            body["name"] = self.name
        if self.rely is not None:
            body["rely"] = self.rely
        if self.timeseries_columns:
            body["timeseries_columns"] = [v for v in self.timeseries_columns]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PrimaryKeyConstraint into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.child_columns:
            body["child_columns"] = self.child_columns
        if self.name is not None:
            body["name"] = self.name
        if self.rely is not None:
            body["rely"] = self.rely
        if self.timeseries_columns:
            body["timeseries_columns"] = self.timeseries_columns
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PrimaryKeyConstraint:
        """Deserializes the PrimaryKeyConstraint from a dictionary."""
        return cls(
            child_columns=d.get("child_columns", None),
            name=d.get("name", None),
            rely=d.get("rely", None),
            timeseries_columns=d.get("timeseries_columns", None),
        )


@dataclass
class Principal:
    id: Optional[str] = None
    """Databricks user, group or service principal ID."""

    principal_type: Optional[PrincipalType] = None

    def as_dict(self) -> dict:
        """Serializes the Principal into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.principal_type is not None:
            body["principal_type"] = self.principal_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Principal into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.principal_type is not None:
            body["principal_type"] = self.principal_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Principal:
        """Deserializes the Principal from a dictionary."""
        return cls(id=d.get("id", None), principal_type=_enum(d, "principal_type", PrincipalType))


class PrincipalType(Enum):

    GROUP_PRINCIPAL = "GROUP_PRINCIPAL"
    SERVICE_PRINCIPAL = "SERVICE_PRINCIPAL"
    USER_PRINCIPAL = "USER_PRINCIPAL"


class Privilege(Enum):

    ACCESS = "ACCESS"
    ALL_PRIVILEGES = "ALL_PRIVILEGES"
    APPLY_TAG = "APPLY_TAG"
    BROWSE = "BROWSE"
    CREATE = "CREATE"
    CREATE_CATALOG = "CREATE_CATALOG"
    CREATE_CLEAN_ROOM = "CREATE_CLEAN_ROOM"
    CREATE_CONNECTION = "CREATE_CONNECTION"
    CREATE_EXTERNAL_LOCATION = "CREATE_EXTERNAL_LOCATION"
    CREATE_EXTERNAL_TABLE = "CREATE_EXTERNAL_TABLE"
    CREATE_EXTERNAL_VOLUME = "CREATE_EXTERNAL_VOLUME"
    CREATE_FOREIGN_CATALOG = "CREATE_FOREIGN_CATALOG"
    CREATE_FOREIGN_SECURABLE = "CREATE_FOREIGN_SECURABLE"
    CREATE_FUNCTION = "CREATE_FUNCTION"
    CREATE_MANAGED_STORAGE = "CREATE_MANAGED_STORAGE"
    CREATE_MATERIALIZED_VIEW = "CREATE_MATERIALIZED_VIEW"
    CREATE_MODEL = "CREATE_MODEL"
    CREATE_PROVIDER = "CREATE_PROVIDER"
    CREATE_RECIPIENT = "CREATE_RECIPIENT"
    CREATE_SCHEMA = "CREATE_SCHEMA"
    CREATE_SERVICE_CREDENTIAL = "CREATE_SERVICE_CREDENTIAL"
    CREATE_SHARE = "CREATE_SHARE"
    CREATE_STORAGE_CREDENTIAL = "CREATE_STORAGE_CREDENTIAL"
    CREATE_TABLE = "CREATE_TABLE"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_VOLUME = "CREATE_VOLUME"
    EXECUTE = "EXECUTE"
    EXECUTE_CLEAN_ROOM_TASK = "EXECUTE_CLEAN_ROOM_TASK"
    EXTERNAL_USE_SCHEMA = "EXTERNAL_USE_SCHEMA"
    MANAGE = "MANAGE"
    MANAGE_ALLOWLIST = "MANAGE_ALLOWLIST"
    MODIFY = "MODIFY"
    MODIFY_CLEAN_ROOM = "MODIFY_CLEAN_ROOM"
    READ_FILES = "READ_FILES"
    READ_PRIVATE_FILES = "READ_PRIVATE_FILES"
    READ_VOLUME = "READ_VOLUME"
    REFRESH = "REFRESH"
    SELECT = "SELECT"
    SET_SHARE_PERMISSION = "SET_SHARE_PERMISSION"
    USAGE = "USAGE"
    USE_CATALOG = "USE_CATALOG"
    USE_CONNECTION = "USE_CONNECTION"
    USE_MARKETPLACE_ASSETS = "USE_MARKETPLACE_ASSETS"
    USE_PROVIDER = "USE_PROVIDER"
    USE_RECIPIENT = "USE_RECIPIENT"
    USE_SCHEMA = "USE_SCHEMA"
    USE_SHARE = "USE_SHARE"
    WRITE_FILES = "WRITE_FILES"
    WRITE_PRIVATE_FILES = "WRITE_PRIVATE_FILES"
    WRITE_VOLUME = "WRITE_VOLUME"


@dataclass
class PrivilegeAssignment:
    principal: Optional[str] = None
    """The principal (user email address or group name). For deleted principals, `principal` is empty
    while `principal_id` is populated."""

    privileges: Optional[List[Privilege]] = None
    """The privileges assigned to the principal."""

    def as_dict(self) -> dict:
        """Serializes the PrivilegeAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.principal is not None:
            body["principal"] = self.principal
        if self.privileges:
            body["privileges"] = [v.value for v in self.privileges]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PrivilegeAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.principal is not None:
            body["principal"] = self.principal
        if self.privileges:
            body["privileges"] = self.privileges
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PrivilegeAssignment:
        """Deserializes the PrivilegeAssignment from a dictionary."""
        return cls(principal=d.get("principal", None), privileges=_repeated_enum(d, "privileges", Privilege))


@dataclass
class ProvisioningInfo:
    """Status of an asynchronously provisioned resource."""

    state: Optional[ProvisioningInfoState] = None
    """The provisioning state of the resource."""

    def as_dict(self) -> dict:
        """Serializes the ProvisioningInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProvisioningInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProvisioningInfo:
        """Deserializes the ProvisioningInfo from a dictionary."""
        return cls(state=_enum(d, "state", ProvisioningInfoState))


class ProvisioningInfoState(Enum):

    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    DELETING = "DELETING"
    FAILED = "FAILED"
    PROVISIONING = "PROVISIONING"
    UPDATING = "UPDATING"


@dataclass
class ProvisioningStatus:
    """Detailed status of an online table. Shown if the online table is in the
    PROVISIONING_PIPELINE_RESOURCES or the PROVISIONING_INITIAL_SNAPSHOT state."""

    initial_pipeline_sync_progress: Optional[PipelineProgress] = None
    """Details about initial data synchronization. Only populated when in the
    PROVISIONING_INITIAL_SNAPSHOT state."""

    def as_dict(self) -> dict:
        """Serializes the ProvisioningStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProvisioningStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProvisioningStatus:
        """Deserializes the ProvisioningStatus from a dictionary."""
        return cls(initial_pipeline_sync_progress=_from_dict(d, "initial_pipeline_sync_progress", PipelineProgress))


@dataclass
class QuotaInfo:
    last_refreshed_at: Optional[int] = None
    """The timestamp that indicates when the quota count was last updated."""

    parent_full_name: Optional[str] = None
    """Name of the parent resource. Returns metastore ID if the parent is a metastore."""

    parent_securable_type: Optional[SecurableType] = None
    """The quota parent securable type."""

    quota_count: Optional[int] = None
    """The current usage of the resource quota."""

    quota_limit: Optional[int] = None
    """The current limit of the resource quota."""

    quota_name: Optional[str] = None
    """The name of the quota."""

    def as_dict(self) -> dict:
        """Serializes the QuotaInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.last_refreshed_at is not None:
            body["last_refreshed_at"] = self.last_refreshed_at
        if self.parent_full_name is not None:
            body["parent_full_name"] = self.parent_full_name
        if self.parent_securable_type is not None:
            body["parent_securable_type"] = self.parent_securable_type.value
        if self.quota_count is not None:
            body["quota_count"] = self.quota_count
        if self.quota_limit is not None:
            body["quota_limit"] = self.quota_limit
        if self.quota_name is not None:
            body["quota_name"] = self.quota_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QuotaInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.last_refreshed_at is not None:
            body["last_refreshed_at"] = self.last_refreshed_at
        if self.parent_full_name is not None:
            body["parent_full_name"] = self.parent_full_name
        if self.parent_securable_type is not None:
            body["parent_securable_type"] = self.parent_securable_type
        if self.quota_count is not None:
            body["quota_count"] = self.quota_count
        if self.quota_limit is not None:
            body["quota_limit"] = self.quota_limit
        if self.quota_name is not None:
            body["quota_name"] = self.quota_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QuotaInfo:
        """Deserializes the QuotaInfo from a dictionary."""
        return cls(
            last_refreshed_at=d.get("last_refreshed_at", None),
            parent_full_name=d.get("parent_full_name", None),
            parent_securable_type=_enum(d, "parent_securable_type", SecurableType),
            quota_count=d.get("quota_count", None),
            quota_limit=d.get("quota_limit", None),
            quota_name=d.get("quota_name", None),
        )


@dataclass
class R2Credentials:
    """R2 temporary credentials for API authentication. Read more at
    https://developers.cloudflare.com/r2/api/s3/tokens/."""

    access_key_id: Optional[str] = None
    """The access key ID that identifies the temporary credentials."""

    secret_access_key: Optional[str] = None
    """The secret access key associated with the access key."""

    session_token: Optional[str] = None
    """The generated JWT that users must pass to use the temporary credentials."""

    def as_dict(self) -> dict:
        """Serializes the R2Credentials into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_key_id is not None:
            body["access_key_id"] = self.access_key_id
        if self.secret_access_key is not None:
            body["secret_access_key"] = self.secret_access_key
        if self.session_token is not None:
            body["session_token"] = self.session_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the R2Credentials into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_key_id is not None:
            body["access_key_id"] = self.access_key_id
        if self.secret_access_key is not None:
            body["secret_access_key"] = self.secret_access_key
        if self.session_token is not None:
            body["session_token"] = self.session_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> R2Credentials:
        """Deserializes the R2Credentials from a dictionary."""
        return cls(
            access_key_id=d.get("access_key_id", None),
            secret_access_key=d.get("secret_access_key", None),
            session_token=d.get("session_token", None),
        )


@dataclass
class RegenerateDashboardResponse:
    dashboard_id: Optional[str] = None

    parent_folder: Optional[str] = None
    """Parent folder is equivalent to {assets_dir}/{tableName}"""

    def as_dict(self) -> dict:
        """Serializes the RegenerateDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.parent_folder is not None:
            body["parent_folder"] = self.parent_folder
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegenerateDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.parent_folder is not None:
            body["parent_folder"] = self.parent_folder
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegenerateDashboardResponse:
        """Deserializes the RegenerateDashboardResponse from a dictionary."""
        return cls(dashboard_id=d.get("dashboard_id", None), parent_folder=d.get("parent_folder", None))


@dataclass
class RegisteredModelAlias:
    alias_name: Optional[str] = None
    """Name of the alias, e.g. 'champion' or 'latest_stable'"""

    catalog_name: Optional[str] = None
    """The name of the catalog containing the model version"""

    id: Optional[str] = None
    """The unique identifier of the alias"""

    model_name: Optional[str] = None
    """The name of the parent registered model of the model version, relative to parent schema"""

    schema_name: Optional[str] = None
    """The name of the schema containing the model version, relative to parent catalog"""

    version_num: Optional[int] = None
    """Integer version number of the model version to which this alias points."""

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelAlias into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alias_name is not None:
            body["alias_name"] = self.alias_name
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.id is not None:
            body["id"] = self.id
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.version_num is not None:
            body["version_num"] = self.version_num
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelAlias into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alias_name is not None:
            body["alias_name"] = self.alias_name
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.id is not None:
            body["id"] = self.id
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.version_num is not None:
            body["version_num"] = self.version_num
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelAlias:
        """Deserializes the RegisteredModelAlias from a dictionary."""
        return cls(
            alias_name=d.get("alias_name", None),
            catalog_name=d.get("catalog_name", None),
            id=d.get("id", None),
            model_name=d.get("model_name", None),
            schema_name=d.get("schema_name", None),
            version_num=d.get("version_num", None),
        )


@dataclass
class RegisteredModelInfo:
    aliases: Optional[List[RegisteredModelAlias]] = None
    """List of aliases associated with the registered model"""

    browse_only: Optional[bool] = None
    """Indicates whether the principal is limited to retrieving metadata for the associated object
    through the BROWSE privilege when include_browse is enabled in the request."""

    catalog_name: Optional[str] = None
    """The name of the catalog where the schema and the registered model reside"""

    comment: Optional[str] = None
    """The comment attached to the registered model"""

    created_at: Optional[int] = None
    """Creation timestamp of the registered model in milliseconds since the Unix epoch"""

    created_by: Optional[str] = None
    """The identifier of the user who created the registered model"""

    full_name: Optional[str] = None
    """The three-level (fully qualified) name of the registered model"""

    metastore_id: Optional[str] = None
    """The unique identifier of the metastore"""

    name: Optional[str] = None
    """The name of the registered model"""

    owner: Optional[str] = None
    """The identifier of the user who owns the registered model"""

    schema_name: Optional[str] = None
    """The name of the schema where the registered model resides"""

    storage_location: Optional[str] = None
    """The storage location on the cloud under which model version data files are stored"""

    updated_at: Optional[int] = None
    """Last-update timestamp of the registered model in milliseconds since the Unix epoch"""

    updated_by: Optional[str] = None
    """The identifier of the user who updated the registered model last time"""

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aliases:
            body["aliases"] = [v.as_dict() for v in self.aliases]
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aliases:
            body["aliases"] = self.aliases
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelInfo:
        """Deserializes the RegisteredModelInfo from a dictionary."""
        return cls(
            aliases=_repeated_dict(d, "aliases", RegisteredModelAlias),
            browse_only=d.get("browse_only", None),
            catalog_name=d.get("catalog_name", None),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            full_name=d.get("full_name", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            schema_name=d.get("schema_name", None),
            storage_location=d.get("storage_location", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class RowFilterOptions:
    function_name: str
    """The fully qualified name of the row filter function. The function is called on each row of the
    target table. It should return a boolean value indicating whether the row should be visible to
    the user. Required on create and update."""

    using: Optional[List[FunctionArgument]] = None
    """Optional list of column aliases or constant literals to be passed as arguments to the row filter
    function. The type of each column should match the positional argument of the row filter
    function."""

    def as_dict(self) -> dict:
        """Serializes the RowFilterOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.using:
            body["using"] = [v.as_dict() for v in self.using]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RowFilterOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.using:
            body["using"] = self.using
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RowFilterOptions:
        """Deserializes the RowFilterOptions from a dictionary."""
        return cls(function_name=d.get("function_name", None), using=_repeated_dict(d, "using", FunctionArgument))


@dataclass
class SchemaInfo:
    """Next ID: 45"""

    browse_only: Optional[bool] = None
    """Indicates whether the principal is limited to retrieving metadata for the associated object
    through the BROWSE privilege when include_browse is enabled in the request."""

    catalog_name: Optional[str] = None
    """Name of parent catalog."""

    catalog_type: Optional[CatalogType] = None
    """The type of the parent catalog."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this schema was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of schema creator."""

    effective_predictive_optimization_flag: Optional[EffectivePredictiveOptimizationFlag] = None

    enable_predictive_optimization: Optional[EnablePredictiveOptimization] = None
    """Whether predictive optimization should be enabled for this object and objects under it."""

    full_name: Optional[str] = None
    """Full name of schema, in form of __catalog_name__.__schema_name__."""

    metastore_id: Optional[str] = None
    """Unique identifier of parent metastore."""

    name: Optional[str] = None
    """Name of schema, relative to parent catalog."""

    owner: Optional[str] = None
    """Username of current owner of schema."""

    properties: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    schema_id: Optional[str] = None
    """The unique identifier of the schema."""

    storage_location: Optional[str] = None
    """Storage location for managed tables within schema."""

    storage_root: Optional[str] = None
    """Storage root URL for managed tables within schema."""

    updated_at: Optional[int] = None
    """Time at which this schema was created, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified schema."""

    def as_dict(self) -> dict:
        """Serializes the SchemaInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.catalog_type is not None:
            body["catalog_type"] = self.catalog_type.value
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.effective_predictive_optimization_flag:
            body["effective_predictive_optimization_flag"] = self.effective_predictive_optimization_flag.as_dict()
        if self.enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = self.enable_predictive_optimization.value
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.schema_id is not None:
            body["schema_id"] = self.schema_id
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SchemaInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.catalog_type is not None:
            body["catalog_type"] = self.catalog_type
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.effective_predictive_optimization_flag:
            body["effective_predictive_optimization_flag"] = self.effective_predictive_optimization_flag
        if self.enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = self.enable_predictive_optimization
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties:
            body["properties"] = self.properties
        if self.schema_id is not None:
            body["schema_id"] = self.schema_id
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.storage_root is not None:
            body["storage_root"] = self.storage_root
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SchemaInfo:
        """Deserializes the SchemaInfo from a dictionary."""
        return cls(
            browse_only=d.get("browse_only", None),
            catalog_name=d.get("catalog_name", None),
            catalog_type=_enum(d, "catalog_type", CatalogType),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            effective_predictive_optimization_flag=_from_dict(
                d, "effective_predictive_optimization_flag", EffectivePredictiveOptimizationFlag
            ),
            enable_predictive_optimization=_enum(d, "enable_predictive_optimization", EnablePredictiveOptimization),
            full_name=d.get("full_name", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            properties=d.get("properties", None),
            schema_id=d.get("schema_id", None),
            storage_location=d.get("storage_location", None),
            storage_root=d.get("storage_root", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class Securable:
    """Generic definition of a securable, which is uniquely defined in a metastore by its type and full
    name."""

    full_name: Optional[str] = None
    """Required. The full name of the catalog/schema/table. Optional if resource_name is present."""

    provider_share: Optional[str] = None
    """Optional. The name of the Share object that contains the securable when the securable is getting
    shared in D2D Delta Sharing."""

    type: Optional[SecurableType] = None
    """Required. The type of securable (catalog/schema/table). Optional if resource_name is present."""

    def as_dict(self) -> dict:
        """Serializes the Securable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.provider_share is not None:
            body["provider_share"] = self.provider_share
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Securable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.provider_share is not None:
            body["provider_share"] = self.provider_share
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Securable:
        """Deserializes the Securable from a dictionary."""
        return cls(
            full_name=d.get("full_name", None),
            provider_share=d.get("provider_share", None),
            type=_enum(d, "type", SecurableType),
        )


class SecurableKind(Enum):
    """Latest kind: CONNECTION_JDBC_OAUTH_M2M = 298; Next id: 299"""

    TABLE_DB_STORAGE = "TABLE_DB_STORAGE"
    TABLE_DELTA = "TABLE_DELTA"
    TABLE_DELTASHARING = "TABLE_DELTASHARING"
    TABLE_DELTASHARING_MUTABLE = "TABLE_DELTASHARING_MUTABLE"
    TABLE_DELTASHARING_OPEN_DIR_BASED = "TABLE_DELTASHARING_OPEN_DIR_BASED"
    TABLE_DELTA_EXTERNAL = "TABLE_DELTA_EXTERNAL"
    TABLE_DELTA_ICEBERG_DELTASHARING = "TABLE_DELTA_ICEBERG_DELTASHARING"
    TABLE_DELTA_ICEBERG_MANAGED = "TABLE_DELTA_ICEBERG_MANAGED"
    TABLE_DELTA_UNIFORM_HUDI_EXTERNAL = "TABLE_DELTA_UNIFORM_HUDI_EXTERNAL"
    TABLE_DELTA_UNIFORM_ICEBERG_EXTERNAL = "TABLE_DELTA_UNIFORM_ICEBERG_EXTERNAL"
    TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_DELTASHARING = "TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_DELTASHARING"
    TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_HIVE_METASTORE_EXTERNAL = (
        "TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_HIVE_METASTORE_EXTERNAL"
    )
    TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_HIVE_METASTORE_MANAGED = (
        "TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_HIVE_METASTORE_MANAGED"
    )
    TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_SNOWFLAKE = "TABLE_DELTA_UNIFORM_ICEBERG_FOREIGN_SNOWFLAKE"
    TABLE_EXTERNAL = "TABLE_EXTERNAL"
    TABLE_FEATURE_STORE = "TABLE_FEATURE_STORE"
    TABLE_FEATURE_STORE_EXTERNAL = "TABLE_FEATURE_STORE_EXTERNAL"
    TABLE_FOREIGN_BIGQUERY = "TABLE_FOREIGN_BIGQUERY"
    TABLE_FOREIGN_DATABRICKS = "TABLE_FOREIGN_DATABRICKS"
    TABLE_FOREIGN_DELTASHARING = "TABLE_FOREIGN_DELTASHARING"
    TABLE_FOREIGN_HIVE_METASTORE = "TABLE_FOREIGN_HIVE_METASTORE"
    TABLE_FOREIGN_HIVE_METASTORE_DBFS_EXTERNAL = "TABLE_FOREIGN_HIVE_METASTORE_DBFS_EXTERNAL"
    TABLE_FOREIGN_HIVE_METASTORE_DBFS_MANAGED = "TABLE_FOREIGN_HIVE_METASTORE_DBFS_MANAGED"
    TABLE_FOREIGN_HIVE_METASTORE_DBFS_SHALLOW_CLONE_EXTERNAL = (
        "TABLE_FOREIGN_HIVE_METASTORE_DBFS_SHALLOW_CLONE_EXTERNAL"
    )
    TABLE_FOREIGN_HIVE_METASTORE_DBFS_SHALLOW_CLONE_MANAGED = "TABLE_FOREIGN_HIVE_METASTORE_DBFS_SHALLOW_CLONE_MANAGED"
    TABLE_FOREIGN_HIVE_METASTORE_DBFS_VIEW = "TABLE_FOREIGN_HIVE_METASTORE_DBFS_VIEW"
    TABLE_FOREIGN_HIVE_METASTORE_EXTERNAL = "TABLE_FOREIGN_HIVE_METASTORE_EXTERNAL"
    TABLE_FOREIGN_HIVE_METASTORE_MANAGED = "TABLE_FOREIGN_HIVE_METASTORE_MANAGED"
    TABLE_FOREIGN_HIVE_METASTORE_SHALLOW_CLONE_EXTERNAL = "TABLE_FOREIGN_HIVE_METASTORE_SHALLOW_CLONE_EXTERNAL"
    TABLE_FOREIGN_HIVE_METASTORE_SHALLOW_CLONE_MANAGED = "TABLE_FOREIGN_HIVE_METASTORE_SHALLOW_CLONE_MANAGED"
    TABLE_FOREIGN_HIVE_METASTORE_VIEW = "TABLE_FOREIGN_HIVE_METASTORE_VIEW"
    TABLE_FOREIGN_MONGODB = "TABLE_FOREIGN_MONGODB"
    TABLE_FOREIGN_MYSQL = "TABLE_FOREIGN_MYSQL"
    TABLE_FOREIGN_NETSUITE = "TABLE_FOREIGN_NETSUITE"
    TABLE_FOREIGN_ORACLE = "TABLE_FOREIGN_ORACLE"
    TABLE_FOREIGN_POSTGRESQL = "TABLE_FOREIGN_POSTGRESQL"
    TABLE_FOREIGN_REDSHIFT = "TABLE_FOREIGN_REDSHIFT"
    TABLE_FOREIGN_SALESFORCE = "TABLE_FOREIGN_SALESFORCE"
    TABLE_FOREIGN_SALESFORCE_DATA_CLOUD = "TABLE_FOREIGN_SALESFORCE_DATA_CLOUD"
    TABLE_FOREIGN_SALESFORCE_DATA_CLOUD_FILE_SHARING = "TABLE_FOREIGN_SALESFORCE_DATA_CLOUD_FILE_SHARING"
    TABLE_FOREIGN_SALESFORCE_DATA_CLOUD_FILE_SHARING_VIEW = "TABLE_FOREIGN_SALESFORCE_DATA_CLOUD_FILE_SHARING_VIEW"
    TABLE_FOREIGN_SNOWFLAKE = "TABLE_FOREIGN_SNOWFLAKE"
    TABLE_FOREIGN_SQLDW = "TABLE_FOREIGN_SQLDW"
    TABLE_FOREIGN_SQLSERVER = "TABLE_FOREIGN_SQLSERVER"
    TABLE_FOREIGN_TERADATA = "TABLE_FOREIGN_TERADATA"
    TABLE_FOREIGN_WORKDAY_RAAS = "TABLE_FOREIGN_WORKDAY_RAAS"
    TABLE_ICEBERG_UNIFORM_MANAGED = "TABLE_ICEBERG_UNIFORM_MANAGED"
    TABLE_INTERNAL = "TABLE_INTERNAL"
    TABLE_MANAGED_POSTGRESQL = "TABLE_MANAGED_POSTGRESQL"
    TABLE_MATERIALIZED_VIEW = "TABLE_MATERIALIZED_VIEW"
    TABLE_MATERIALIZED_VIEW_DELTASHARING = "TABLE_MATERIALIZED_VIEW_DELTASHARING"
    TABLE_METRIC_VIEW = "TABLE_METRIC_VIEW"
    TABLE_METRIC_VIEW_DELTASHARING = "TABLE_METRIC_VIEW_DELTASHARING"
    TABLE_ONLINE_VECTOR_INDEX_DIRECT = "TABLE_ONLINE_VECTOR_INDEX_DIRECT"
    TABLE_ONLINE_VECTOR_INDEX_REPLICA = "TABLE_ONLINE_VECTOR_INDEX_REPLICA"
    TABLE_ONLINE_VIEW = "TABLE_ONLINE_VIEW"
    TABLE_STANDARD = "TABLE_STANDARD"
    TABLE_STREAMING_LIVE_TABLE = "TABLE_STREAMING_LIVE_TABLE"
    TABLE_STREAMING_LIVE_TABLE_DELTASHARING = "TABLE_STREAMING_LIVE_TABLE_DELTASHARING"
    TABLE_SYSTEM = "TABLE_SYSTEM"
    TABLE_SYSTEM_DELTASHARING = "TABLE_SYSTEM_DELTASHARING"
    TABLE_VIEW = "TABLE_VIEW"
    TABLE_VIEW_DELTASHARING = "TABLE_VIEW_DELTASHARING"


@dataclass
class SecurableKindManifest:
    """Manifest of a specific securable kind."""

    assignable_privileges: Optional[List[str]] = None
    """Privileges that can be assigned to the securable."""

    capabilities: Optional[List[str]] = None
    """A list of capabilities in the securable kind."""

    options: Optional[List[OptionSpec]] = None
    """Detailed specs of allowed options."""

    securable_kind: Optional[SecurableKind] = None
    """Securable kind to get manifest of."""

    securable_type: Optional[SecurableType] = None
    """Securable Type of the kind."""

    def as_dict(self) -> dict:
        """Serializes the SecurableKindManifest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.assignable_privileges:
            body["assignable_privileges"] = [v for v in self.assignable_privileges]
        if self.capabilities:
            body["capabilities"] = [v for v in self.capabilities]
        if self.options:
            body["options"] = [v.as_dict() for v in self.options]
        if self.securable_kind is not None:
            body["securable_kind"] = self.securable_kind.value
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SecurableKindManifest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.assignable_privileges:
            body["assignable_privileges"] = self.assignable_privileges
        if self.capabilities:
            body["capabilities"] = self.capabilities
        if self.options:
            body["options"] = self.options
        if self.securable_kind is not None:
            body["securable_kind"] = self.securable_kind
        if self.securable_type is not None:
            body["securable_type"] = self.securable_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SecurableKindManifest:
        """Deserializes the SecurableKindManifest from a dictionary."""
        return cls(
            assignable_privileges=d.get("assignable_privileges", None),
            capabilities=d.get("capabilities", None),
            options=_repeated_dict(d, "options", OptionSpec),
            securable_kind=_enum(d, "securable_kind", SecurableKind),
            securable_type=_enum(d, "securable_type", SecurableType),
        )


@dataclass
class SecurablePermissions:
    permissions: Optional[List[str]] = None
    """List of requested Unity Catalog permissions."""

    securable: Optional[Securable] = None
    """The securable for which the access request destinations are being requested."""

    def as_dict(self) -> dict:
        """Serializes the SecurablePermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permissions:
            body["permissions"] = [v for v in self.permissions]
        if self.securable:
            body["securable"] = self.securable.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SecurablePermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permissions:
            body["permissions"] = self.permissions
        if self.securable:
            body["securable"] = self.securable
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SecurablePermissions:
        """Deserializes the SecurablePermissions from a dictionary."""
        return cls(permissions=d.get("permissions", None), securable=_from_dict(d, "securable", Securable))


class SecurableType(Enum):
    """The type of Unity Catalog securable."""

    CATALOG = "CATALOG"
    CLEAN_ROOM = "CLEAN_ROOM"
    CONNECTION = "CONNECTION"
    CREDENTIAL = "CREDENTIAL"
    EXTERNAL_LOCATION = "EXTERNAL_LOCATION"
    EXTERNAL_METADATA = "EXTERNAL_METADATA"
    FUNCTION = "FUNCTION"
    METASTORE = "METASTORE"
    PIPELINE = "PIPELINE"
    PROVIDER = "PROVIDER"
    RECIPIENT = "RECIPIENT"
    SCHEMA = "SCHEMA"
    SHARE = "SHARE"
    STAGING_TABLE = "STAGING_TABLE"
    STORAGE_CREDENTIAL = "STORAGE_CREDENTIAL"
    TABLE = "TABLE"
    VOLUME = "VOLUME"


class SpecialDestination(Enum):

    SPECIAL_DESTINATION_CATALOG_OWNER = "SPECIAL_DESTINATION_CATALOG_OWNER"
    SPECIAL_DESTINATION_CONNECTION_OWNER = "SPECIAL_DESTINATION_CONNECTION_OWNER"
    SPECIAL_DESTINATION_CREDENTIAL_OWNER = "SPECIAL_DESTINATION_CREDENTIAL_OWNER"
    SPECIAL_DESTINATION_EXTERNAL_LOCATION_OWNER = "SPECIAL_DESTINATION_EXTERNAL_LOCATION_OWNER"
    SPECIAL_DESTINATION_METASTORE_OWNER = "SPECIAL_DESTINATION_METASTORE_OWNER"


@dataclass
class SseEncryptionDetails:
    """Server-Side Encryption properties for clients communicating with AWS s3."""

    algorithm: Optional[SseEncryptionDetailsAlgorithm] = None
    """Sets the value of the 'x-amz-server-side-encryption' header in S3 request."""

    aws_kms_key_arn: Optional[str] = None
    """Optional. The ARN of the SSE-KMS key used with the S3 location, when algorithm = "SSE-KMS". Sets
    the value of the 'x-amz-server-side-encryption-aws-kms-key-id' header."""

    def as_dict(self) -> dict:
        """Serializes the SseEncryptionDetails into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.algorithm is not None:
            body["algorithm"] = self.algorithm.value
        if self.aws_kms_key_arn is not None:
            body["aws_kms_key_arn"] = self.aws_kms_key_arn
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SseEncryptionDetails into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.algorithm is not None:
            body["algorithm"] = self.algorithm
        if self.aws_kms_key_arn is not None:
            body["aws_kms_key_arn"] = self.aws_kms_key_arn
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SseEncryptionDetails:
        """Deserializes the SseEncryptionDetails from a dictionary."""
        return cls(
            algorithm=_enum(d, "algorithm", SseEncryptionDetailsAlgorithm),
            aws_kms_key_arn=d.get("aws_kms_key_arn", None),
        )


class SseEncryptionDetailsAlgorithm(Enum):

    AWS_SSE_KMS = "AWS_SSE_KMS"
    AWS_SSE_S3 = "AWS_SSE_S3"


@dataclass
class StorageCredentialInfo:
    aws_iam_role: Optional[AwsIamRoleResponse] = None
    """The AWS IAM role configuration."""

    azure_managed_identity: Optional[AzureManagedIdentityResponse] = None
    """The Azure managed identity configuration."""

    azure_service_principal: Optional[AzureServicePrincipal] = None
    """The Azure service principal configuration."""

    cloudflare_api_token: Optional[CloudflareApiToken] = None
    """The Cloudflare API token configuration."""

    comment: Optional[str] = None
    """Comment associated with the credential."""

    created_at: Optional[int] = None
    """Time at which this credential was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of credential creator."""

    databricks_gcp_service_account: Optional[DatabricksGcpServiceAccountResponse] = None
    """The Databricks managed GCP service account configuration."""

    full_name: Optional[str] = None
    """The full name of the credential."""

    id: Optional[str] = None
    """The unique identifier of the credential."""

    isolation_mode: Optional[IsolationMode] = None
    """Whether the current securable is accessible from all workspaces or a specific set of workspaces."""

    metastore_id: Optional[str] = None
    """Unique identifier of the parent metastore."""

    name: Optional[str] = None
    """The credential name. The name must be unique among storage and service credentials within the
    metastore."""

    owner: Optional[str] = None
    """Username of current owner of credential."""

    read_only: Optional[bool] = None
    """Whether the credential is usable only for read operations. Only applicable when purpose is
    **STORAGE**."""

    updated_at: Optional[int] = None
    """Time at which this credential was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified the credential."""

    used_for_managed_storage: Optional[bool] = None
    """Whether this credential is the current metastore's root storage credential. Only applicable when
    purpose is **STORAGE**."""

    def as_dict(self) -> dict:
        """Serializes the StorageCredentialInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role.as_dict()
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity.as_dict()
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal.as_dict()
        if self.cloudflare_api_token:
            body["cloudflare_api_token"] = self.cloudflare_api_token.as_dict()
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account.as_dict()
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.id is not None:
            body["id"] = self.id
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode.value
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.used_for_managed_storage is not None:
            body["used_for_managed_storage"] = self.used_for_managed_storage
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StorageCredentialInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal
        if self.cloudflare_api_token:
            body["cloudflare_api_token"] = self.cloudflare_api_token
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.id is not None:
            body["id"] = self.id
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.read_only is not None:
            body["read_only"] = self.read_only
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.used_for_managed_storage is not None:
            body["used_for_managed_storage"] = self.used_for_managed_storage
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StorageCredentialInfo:
        """Deserializes the StorageCredentialInfo from a dictionary."""
        return cls(
            aws_iam_role=_from_dict(d, "aws_iam_role", AwsIamRoleResponse),
            azure_managed_identity=_from_dict(d, "azure_managed_identity", AzureManagedIdentityResponse),
            azure_service_principal=_from_dict(d, "azure_service_principal", AzureServicePrincipal),
            cloudflare_api_token=_from_dict(d, "cloudflare_api_token", CloudflareApiToken),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            databricks_gcp_service_account=_from_dict(
                d, "databricks_gcp_service_account", DatabricksGcpServiceAccountResponse
            ),
            full_name=d.get("full_name", None),
            id=d.get("id", None),
            isolation_mode=_enum(d, "isolation_mode", IsolationMode),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            read_only=d.get("read_only", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            used_for_managed_storage=d.get("used_for_managed_storage", None),
        )


@dataclass
class SystemSchemaInfo:
    schema: str
    """Name of the system schema."""

    state: str
    """The current state of enablement for the system schema. An empty string means the system schema
    is available and ready for opt-in. Possible values: AVAILABLE | ENABLE_INITIALIZED |
    ENABLE_COMPLETED | DISABLE_INITIALIZED | UNAVAILABLE | MANAGED"""

    def as_dict(self) -> dict:
        """Serializes the SystemSchemaInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.schema is not None:
            body["schema"] = self.schema
        if self.state is not None:
            body["state"] = self.state
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SystemSchemaInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.schema is not None:
            body["schema"] = self.schema
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SystemSchemaInfo:
        """Deserializes the SystemSchemaInfo from a dictionary."""
        return cls(schema=d.get("schema", None), state=d.get("state", None))


class SystemType(Enum):

    AMAZON_REDSHIFT = "AMAZON_REDSHIFT"
    AZURE_SYNAPSE = "AZURE_SYNAPSE"
    CONFLUENT = "CONFLUENT"
    DATABRICKS = "DATABRICKS"
    GOOGLE_BIGQUERY = "GOOGLE_BIGQUERY"
    KAFKA = "KAFKA"
    LOOKER = "LOOKER"
    MICROSOFT_FABRIC = "MICROSOFT_FABRIC"
    MICROSOFT_SQL_SERVER = "MICROSOFT_SQL_SERVER"
    MONGODB = "MONGODB"
    MYSQL = "MYSQL"
    ORACLE = "ORACLE"
    OTHER = "OTHER"
    POSTGRESQL = "POSTGRESQL"
    POWER_BI = "POWER_BI"
    SALESFORCE = "SALESFORCE"
    SAP = "SAP"
    SERVICENOW = "SERVICENOW"
    SNOWFLAKE = "SNOWFLAKE"
    STREAM_NATIVE = "STREAM_NATIVE"
    TABLEAU = "TABLEAU"
    TERADATA = "TERADATA"
    WORKDAY = "WORKDAY"


@dataclass
class TableConstraint:
    """A table constraint, as defined by *one* of the following fields being set:
    __primary_key_constraint__, __foreign_key_constraint__, __named_table_constraint__."""

    foreign_key_constraint: Optional[ForeignKeyConstraint] = None

    named_table_constraint: Optional[NamedTableConstraint] = None

    primary_key_constraint: Optional[PrimaryKeyConstraint] = None

    def as_dict(self) -> dict:
        """Serializes the TableConstraint into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.foreign_key_constraint:
            body["foreign_key_constraint"] = self.foreign_key_constraint.as_dict()
        if self.named_table_constraint:
            body["named_table_constraint"] = self.named_table_constraint.as_dict()
        if self.primary_key_constraint:
            body["primary_key_constraint"] = self.primary_key_constraint.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableConstraint into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.foreign_key_constraint:
            body["foreign_key_constraint"] = self.foreign_key_constraint
        if self.named_table_constraint:
            body["named_table_constraint"] = self.named_table_constraint
        if self.primary_key_constraint:
            body["primary_key_constraint"] = self.primary_key_constraint
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableConstraint:
        """Deserializes the TableConstraint from a dictionary."""
        return cls(
            foreign_key_constraint=_from_dict(d, "foreign_key_constraint", ForeignKeyConstraint),
            named_table_constraint=_from_dict(d, "named_table_constraint", NamedTableConstraint),
            primary_key_constraint=_from_dict(d, "primary_key_constraint", PrimaryKeyConstraint),
        )


@dataclass
class TableDependency:
    """A table that is dependent on a SQL object."""

    table_full_name: str
    """Full name of the dependent table, in the form of
    __catalog_name__.__schema_name__.__table_name__."""

    def as_dict(self) -> dict:
        """Serializes the TableDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.table_full_name is not None:
            body["table_full_name"] = self.table_full_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.table_full_name is not None:
            body["table_full_name"] = self.table_full_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableDependency:
        """Deserializes the TableDependency from a dictionary."""
        return cls(table_full_name=d.get("table_full_name", None))


@dataclass
class TableExistsResponse:
    table_exists: Optional[bool] = None
    """Whether the table exists or not."""

    def as_dict(self) -> dict:
        """Serializes the TableExistsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.table_exists is not None:
            body["table_exists"] = self.table_exists
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableExistsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.table_exists is not None:
            body["table_exists"] = self.table_exists
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableExistsResponse:
        """Deserializes the TableExistsResponse from a dictionary."""
        return cls(table_exists=d.get("table_exists", None))


@dataclass
class TableInfo:
    access_point: Optional[str] = None
    """The AWS access point to use when accesing s3 for this external location."""

    browse_only: Optional[bool] = None
    """Indicates whether the principal is limited to retrieving metadata for the associated object
    through the BROWSE privilege when include_browse is enabled in the request."""

    catalog_name: Optional[str] = None
    """Name of parent catalog."""

    columns: Optional[List[ColumnInfo]] = None
    """The array of __ColumnInfo__ definitions of the table's columns."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this table was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of table creator."""

    data_access_configuration_id: Optional[str] = None
    """Unique ID of the Data Access Configuration to use with the table data."""

    data_source_format: Optional[DataSourceFormat] = None

    deleted_at: Optional[int] = None
    """Time at which this table was deleted, in epoch milliseconds. Field is omitted if table is not
    deleted."""

    delta_runtime_properties_kvpairs: Optional[DeltaRuntimePropertiesKvPairs] = None
    """Information pertaining to current state of the delta table."""

    effective_predictive_optimization_flag: Optional[EffectivePredictiveOptimizationFlag] = None

    enable_predictive_optimization: Optional[EnablePredictiveOptimization] = None

    encryption_details: Optional[EncryptionDetails] = None

    full_name: Optional[str] = None
    """Full name of table, in form of __catalog_name__.__schema_name__.__table_name__"""

    metastore_id: Optional[str] = None
    """Unique identifier of parent metastore."""

    name: Optional[str] = None
    """Name of table, relative to parent schema."""

    owner: Optional[str] = None
    """Username of current owner of table."""

    pipeline_id: Optional[str] = None
    """The pipeline ID of the table. Applicable for tables created by pipelines (Materialized View,
    Streaming Table, etc.)."""

    properties: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    row_filter: Optional[TableRowFilter] = None

    schema_name: Optional[str] = None
    """Name of parent schema relative to its parent catalog."""

    securable_kind_manifest: Optional[SecurableKindManifest] = None
    """SecurableKindManifest of table, including capabilities the table has."""

    sql_path: Optional[str] = None
    """List of schemes whose objects can be referenced without qualification."""

    storage_credential_name: Optional[str] = None
    """Name of the storage credential, when a storage credential is configured for use with this table."""

    storage_location: Optional[str] = None
    """Storage root URL for table (for **MANAGED**, **EXTERNAL** tables)."""

    table_constraints: Optional[List[TableConstraint]] = None
    """List of table constraints. Note: this field is not set in the output of the __listTables__ API."""

    table_id: Optional[str] = None
    """The unique identifier of the table."""

    table_type: Optional[TableType] = None

    updated_at: Optional[int] = None
    """Time at which this table was last modified, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified the table."""

    view_definition: Optional[str] = None
    """View definition SQL (when __table_type__ is **VIEW**, **MATERIALIZED_VIEW**, or
    **STREAMING_TABLE**)"""

    view_dependencies: Optional[DependencyList] = None
    """View dependencies (when table_type == **VIEW** or **MATERIALIZED_VIEW**, **STREAMING_TABLE**) -
    when DependencyList is None, the dependency is not provided; - when DependencyList is an empty
    list, the dependency is provided but is empty; - when DependencyList is not an empty list,
    dependencies are provided and recorded. Note: this field is not set in the output of the
    __listTables__ API."""

    def as_dict(self) -> dict:
        """Serializes the TableInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_point is not None:
            body["access_point"] = self.access_point
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_access_configuration_id is not None:
            body["data_access_configuration_id"] = self.data_access_configuration_id
        if self.data_source_format is not None:
            body["data_source_format"] = self.data_source_format.value
        if self.deleted_at is not None:
            body["deleted_at"] = self.deleted_at
        if self.delta_runtime_properties_kvpairs:
            body["delta_runtime_properties_kvpairs"] = self.delta_runtime_properties_kvpairs.as_dict()
        if self.effective_predictive_optimization_flag:
            body["effective_predictive_optimization_flag"] = self.effective_predictive_optimization_flag.as_dict()
        if self.enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = self.enable_predictive_optimization.value
        if self.encryption_details:
            body["encryption_details"] = self.encryption_details.as_dict()
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.properties:
            body["properties"] = self.properties
        if self.row_filter:
            body["row_filter"] = self.row_filter.as_dict()
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.securable_kind_manifest:
            body["securable_kind_manifest"] = self.securable_kind_manifest.as_dict()
        if self.sql_path is not None:
            body["sql_path"] = self.sql_path
        if self.storage_credential_name is not None:
            body["storage_credential_name"] = self.storage_credential_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.table_constraints:
            body["table_constraints"] = [v.as_dict() for v in self.table_constraints]
        if self.table_id is not None:
            body["table_id"] = self.table_id
        if self.table_type is not None:
            body["table_type"] = self.table_type.value
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.view_definition is not None:
            body["view_definition"] = self.view_definition
        if self.view_dependencies:
            body["view_dependencies"] = self.view_dependencies.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_point is not None:
            body["access_point"] = self.access_point
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.columns:
            body["columns"] = self.columns
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_access_configuration_id is not None:
            body["data_access_configuration_id"] = self.data_access_configuration_id
        if self.data_source_format is not None:
            body["data_source_format"] = self.data_source_format
        if self.deleted_at is not None:
            body["deleted_at"] = self.deleted_at
        if self.delta_runtime_properties_kvpairs:
            body["delta_runtime_properties_kvpairs"] = self.delta_runtime_properties_kvpairs
        if self.effective_predictive_optimization_flag:
            body["effective_predictive_optimization_flag"] = self.effective_predictive_optimization_flag
        if self.enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = self.enable_predictive_optimization
        if self.encryption_details:
            body["encryption_details"] = self.encryption_details
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.properties:
            body["properties"] = self.properties
        if self.row_filter:
            body["row_filter"] = self.row_filter
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.securable_kind_manifest:
            body["securable_kind_manifest"] = self.securable_kind_manifest
        if self.sql_path is not None:
            body["sql_path"] = self.sql_path
        if self.storage_credential_name is not None:
            body["storage_credential_name"] = self.storage_credential_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.table_constraints:
            body["table_constraints"] = self.table_constraints
        if self.table_id is not None:
            body["table_id"] = self.table_id
        if self.table_type is not None:
            body["table_type"] = self.table_type
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.view_definition is not None:
            body["view_definition"] = self.view_definition
        if self.view_dependencies:
            body["view_dependencies"] = self.view_dependencies
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableInfo:
        """Deserializes the TableInfo from a dictionary."""
        return cls(
            access_point=d.get("access_point", None),
            browse_only=d.get("browse_only", None),
            catalog_name=d.get("catalog_name", None),
            columns=_repeated_dict(d, "columns", ColumnInfo),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            data_access_configuration_id=d.get("data_access_configuration_id", None),
            data_source_format=_enum(d, "data_source_format", DataSourceFormat),
            deleted_at=d.get("deleted_at", None),
            delta_runtime_properties_kvpairs=_from_dict(
                d, "delta_runtime_properties_kvpairs", DeltaRuntimePropertiesKvPairs
            ),
            effective_predictive_optimization_flag=_from_dict(
                d, "effective_predictive_optimization_flag", EffectivePredictiveOptimizationFlag
            ),
            enable_predictive_optimization=_enum(d, "enable_predictive_optimization", EnablePredictiveOptimization),
            encryption_details=_from_dict(d, "encryption_details", EncryptionDetails),
            full_name=d.get("full_name", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            pipeline_id=d.get("pipeline_id", None),
            properties=d.get("properties", None),
            row_filter=_from_dict(d, "row_filter", TableRowFilter),
            schema_name=d.get("schema_name", None),
            securable_kind_manifest=_from_dict(d, "securable_kind_manifest", SecurableKindManifest),
            sql_path=d.get("sql_path", None),
            storage_credential_name=d.get("storage_credential_name", None),
            storage_location=d.get("storage_location", None),
            table_constraints=_repeated_dict(d, "table_constraints", TableConstraint),
            table_id=d.get("table_id", None),
            table_type=_enum(d, "table_type", TableType),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            view_definition=d.get("view_definition", None),
            view_dependencies=_from_dict(d, "view_dependencies", DependencyList),
        )


class TableOperation(Enum):

    READ = "READ"
    READ_WRITE = "READ_WRITE"


@dataclass
class TableRowFilter:
    function_name: str
    """The full name of the row filter SQL UDF."""

    input_column_names: List[str]
    """The list of table columns to be passed as input to the row filter function. The column types
    should match the types of the filter function arguments."""

    input_arguments: Optional[List[PolicyFunctionArgument]] = None
    """The list of additional table columns or literals to be passed as additional arguments to a row
    filter function. This is the replacement of the deprecated input_column_names field and carries
    information about the types (alias or constant) of the arguments to the filter function."""

    def as_dict(self) -> dict:
        """Serializes the TableRowFilter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.input_arguments:
            body["input_arguments"] = [v.as_dict() for v in self.input_arguments]
        if self.input_column_names:
            body["input_column_names"] = [v for v in self.input_column_names]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableRowFilter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.input_arguments:
            body["input_arguments"] = self.input_arguments
        if self.input_column_names:
            body["input_column_names"] = self.input_column_names
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableRowFilter:
        """Deserializes the TableRowFilter from a dictionary."""
        return cls(
            function_name=d.get("function_name", None),
            input_arguments=_repeated_dict(d, "input_arguments", PolicyFunctionArgument),
            input_column_names=d.get("input_column_names", None),
        )


@dataclass
class TableSummary:
    full_name: Optional[str] = None
    """The full name of the table."""

    securable_kind_manifest: Optional[SecurableKindManifest] = None
    """SecurableKindManifest of table, including capabilities the table has."""

    table_type: Optional[TableType] = None

    def as_dict(self) -> dict:
        """Serializes the TableSummary into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.securable_kind_manifest:
            body["securable_kind_manifest"] = self.securable_kind_manifest.as_dict()
        if self.table_type is not None:
            body["table_type"] = self.table_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableSummary into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.securable_kind_manifest:
            body["securable_kind_manifest"] = self.securable_kind_manifest
        if self.table_type is not None:
            body["table_type"] = self.table_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableSummary:
        """Deserializes the TableSummary from a dictionary."""
        return cls(
            full_name=d.get("full_name", None),
            securable_kind_manifest=_from_dict(d, "securable_kind_manifest", SecurableKindManifest),
            table_type=_enum(d, "table_type", TableType),
        )


class TableType(Enum):

    EXTERNAL = "EXTERNAL"
    EXTERNAL_SHALLOW_CLONE = "EXTERNAL_SHALLOW_CLONE"
    FOREIGN = "FOREIGN"
    MANAGED = "MANAGED"
    MANAGED_SHALLOW_CLONE = "MANAGED_SHALLOW_CLONE"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    METRIC_VIEW = "METRIC_VIEW"
    STREAMING_TABLE = "STREAMING_TABLE"
    VIEW = "VIEW"


class TagAssignmentSourceType(Enum):
    """Enum representing the source type of a tag assignment"""

    TAG_ASSIGNMENT_SOURCE_TYPE_SYSTEM_DATA_CLASSIFICATION = "TAG_ASSIGNMENT_SOURCE_TYPE_SYSTEM_DATA_CLASSIFICATION"


@dataclass
class TagKeyValue:
    key: Optional[str] = None
    """name of the tag"""

    value: Optional[str] = None
    """value of the tag associated with the key, could be optional"""

    def as_dict(self) -> dict:
        """Serializes the TagKeyValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TagKeyValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TagKeyValue:
        """Deserializes the TagKeyValue from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class TemporaryCredentials:
    aws_temp_credentials: Optional[AwsCredentials] = None

    azure_aad: Optional[AzureActiveDirectoryToken] = None

    expiration_time: Optional[int] = None
    """Server time when the credential will expire, in epoch milliseconds. The API client is advised to
    cache the credential given this expiration time."""

    gcp_oauth_token: Optional[GcpOauthToken] = None

    def as_dict(self) -> dict:
        """Serializes the TemporaryCredentials into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_temp_credentials:
            body["aws_temp_credentials"] = self.aws_temp_credentials.as_dict()
        if self.azure_aad:
            body["azure_aad"] = self.azure_aad.as_dict()
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.gcp_oauth_token:
            body["gcp_oauth_token"] = self.gcp_oauth_token.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TemporaryCredentials into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_temp_credentials:
            body["aws_temp_credentials"] = self.aws_temp_credentials
        if self.azure_aad:
            body["azure_aad"] = self.azure_aad
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.gcp_oauth_token:
            body["gcp_oauth_token"] = self.gcp_oauth_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TemporaryCredentials:
        """Deserializes the TemporaryCredentials from a dictionary."""
        return cls(
            aws_temp_credentials=_from_dict(d, "aws_temp_credentials", AwsCredentials),
            azure_aad=_from_dict(d, "azure_aad", AzureActiveDirectoryToken),
            expiration_time=d.get("expiration_time", None),
            gcp_oauth_token=_from_dict(d, "gcp_oauth_token", GcpOauthToken),
        )


@dataclass
class TriggeredUpdateStatus:
    """Detailed status of an online table. Shown if the online table is in the ONLINE_TRIGGERED_UPDATE
    or the ONLINE_NO_PENDING_UPDATE state."""

    last_processed_commit_version: Optional[int] = None
    """The last source table Delta version that was synced to the online table. Note that this Delta
    version may not be completely synced to the online table yet."""

    timestamp: Optional[str] = None
    """The timestamp of the last time any data was synchronized from the source table to the online
    table."""

    triggered_update_progress: Optional[PipelineProgress] = None
    """Progress of the active data synchronization pipeline."""

    def as_dict(self) -> dict:
        """Serializes the TriggeredUpdateStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.triggered_update_progress:
            body["triggered_update_progress"] = self.triggered_update_progress.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TriggeredUpdateStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.triggered_update_progress:
            body["triggered_update_progress"] = self.triggered_update_progress
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TriggeredUpdateStatus:
        """Deserializes the TriggeredUpdateStatus from a dictionary."""
        return cls(
            last_processed_commit_version=d.get("last_processed_commit_version", None),
            timestamp=d.get("timestamp", None),
            triggered_update_progress=_from_dict(d, "triggered_update_progress", PipelineProgress),
        )


@dataclass
class UnassignResponse:
    def as_dict(self) -> dict:
        """Serializes the UnassignResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UnassignResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UnassignResponse:
        """Deserializes the UnassignResponse from a dictionary."""
        return cls()


@dataclass
class UpdateAccountsMetastore:
    delta_sharing_organization_name: Optional[str] = None
    """The organization name of a Delta Sharing entity, to be used in Databricks-to-Databricks Delta
    Sharing as the official name."""

    delta_sharing_recipient_token_lifetime_in_seconds: Optional[int] = None
    """The lifetime of delta sharing recipient token in seconds."""

    delta_sharing_scope: Optional[DeltaSharingScopeEnum] = None
    """The scope of Delta Sharing enabled for the metastore."""

    external_access_enabled: Optional[bool] = None
    """Whether to allow non-DBR clients to directly access entities under the metastore."""

    owner: Optional[str] = None
    """The owner of the metastore."""

    privilege_model_version: Optional[str] = None
    """Privilege model version of the metastore, of the form `major.minor` (e.g., `1.0`)."""

    storage_root_credential_id: Optional[str] = None
    """UUID of storage credential to access the metastore storage_root."""

    def as_dict(self) -> dict:
        """Serializes the UpdateAccountsMetastore into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.delta_sharing_organization_name is not None:
            body["delta_sharing_organization_name"] = self.delta_sharing_organization_name
        if self.delta_sharing_recipient_token_lifetime_in_seconds is not None:
            body["delta_sharing_recipient_token_lifetime_in_seconds"] = (
                self.delta_sharing_recipient_token_lifetime_in_seconds
            )
        if self.delta_sharing_scope is not None:
            body["delta_sharing_scope"] = self.delta_sharing_scope.value
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.owner is not None:
            body["owner"] = self.owner
        if self.privilege_model_version is not None:
            body["privilege_model_version"] = self.privilege_model_version
        if self.storage_root_credential_id is not None:
            body["storage_root_credential_id"] = self.storage_root_credential_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateAccountsMetastore into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.delta_sharing_organization_name is not None:
            body["delta_sharing_organization_name"] = self.delta_sharing_organization_name
        if self.delta_sharing_recipient_token_lifetime_in_seconds is not None:
            body["delta_sharing_recipient_token_lifetime_in_seconds"] = (
                self.delta_sharing_recipient_token_lifetime_in_seconds
            )
        if self.delta_sharing_scope is not None:
            body["delta_sharing_scope"] = self.delta_sharing_scope
        if self.external_access_enabled is not None:
            body["external_access_enabled"] = self.external_access_enabled
        if self.owner is not None:
            body["owner"] = self.owner
        if self.privilege_model_version is not None:
            body["privilege_model_version"] = self.privilege_model_version
        if self.storage_root_credential_id is not None:
            body["storage_root_credential_id"] = self.storage_root_credential_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateAccountsMetastore:
        """Deserializes the UpdateAccountsMetastore from a dictionary."""
        return cls(
            delta_sharing_organization_name=d.get("delta_sharing_organization_name", None),
            delta_sharing_recipient_token_lifetime_in_seconds=d.get(
                "delta_sharing_recipient_token_lifetime_in_seconds", None
            ),
            delta_sharing_scope=_enum(d, "delta_sharing_scope", DeltaSharingScopeEnum),
            external_access_enabled=d.get("external_access_enabled", None),
            owner=d.get("owner", None),
            privilege_model_version=d.get("privilege_model_version", None),
            storage_root_credential_id=d.get("storage_root_credential_id", None),
        )


@dataclass
class UpdateAccountsStorageCredential:
    aws_iam_role: Optional[AwsIamRoleRequest] = None
    """The AWS IAM role configuration."""

    azure_managed_identity: Optional[AzureManagedIdentityResponse] = None
    """The Azure managed identity configuration."""

    azure_service_principal: Optional[AzureServicePrincipal] = None
    """The Azure service principal configuration."""

    cloudflare_api_token: Optional[CloudflareApiToken] = None
    """The Cloudflare API token configuration."""

    comment: Optional[str] = None
    """Comment associated with the credential."""

    databricks_gcp_service_account: Optional[DatabricksGcpServiceAccountRequest] = None
    """The Databricks managed GCP service account configuration."""

    isolation_mode: Optional[IsolationMode] = None
    """Whether the current securable is accessible from all workspaces or a specific set of workspaces."""

    owner: Optional[str] = None
    """Username of current owner of credential."""

    read_only: Optional[bool] = None
    """Whether the credential is usable only for read operations. Only applicable when purpose is
    **STORAGE**."""

    def as_dict(self) -> dict:
        """Serializes the UpdateAccountsStorageCredential into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role.as_dict()
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity.as_dict()
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal.as_dict()
        if self.cloudflare_api_token:
            body["cloudflare_api_token"] = self.cloudflare_api_token.as_dict()
        if self.comment is not None:
            body["comment"] = self.comment
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account.as_dict()
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode.value
        if self.owner is not None:
            body["owner"] = self.owner
        if self.read_only is not None:
            body["read_only"] = self.read_only
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateAccountsStorageCredential into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_iam_role:
            body["aws_iam_role"] = self.aws_iam_role
        if self.azure_managed_identity:
            body["azure_managed_identity"] = self.azure_managed_identity
        if self.azure_service_principal:
            body["azure_service_principal"] = self.azure_service_principal
        if self.cloudflare_api_token:
            body["cloudflare_api_token"] = self.cloudflare_api_token
        if self.comment is not None:
            body["comment"] = self.comment
        if self.databricks_gcp_service_account:
            body["databricks_gcp_service_account"] = self.databricks_gcp_service_account
        if self.isolation_mode is not None:
            body["isolation_mode"] = self.isolation_mode
        if self.owner is not None:
            body["owner"] = self.owner
        if self.read_only is not None:
            body["read_only"] = self.read_only
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateAccountsStorageCredential:
        """Deserializes the UpdateAccountsStorageCredential from a dictionary."""
        return cls(
            aws_iam_role=_from_dict(d, "aws_iam_role", AwsIamRoleRequest),
            azure_managed_identity=_from_dict(d, "azure_managed_identity", AzureManagedIdentityResponse),
            azure_service_principal=_from_dict(d, "azure_service_principal", AzureServicePrincipal),
            cloudflare_api_token=_from_dict(d, "cloudflare_api_token", CloudflareApiToken),
            comment=d.get("comment", None),
            databricks_gcp_service_account=_from_dict(
                d, "databricks_gcp_service_account", DatabricksGcpServiceAccountRequest
            ),
            isolation_mode=_enum(d, "isolation_mode", IsolationMode),
            owner=d.get("owner", None),
            read_only=d.get("read_only", None),
        )


@dataclass
class UpdateAssignmentResponse:
    def as_dict(self) -> dict:
        """Serializes the UpdateAssignmentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateAssignmentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateAssignmentResponse:
        """Deserializes the UpdateAssignmentResponse from a dictionary."""
        return cls()


@dataclass
class UpdateCatalogWorkspaceBindingsResponse:
    workspaces: Optional[List[int]] = None
    """A list of workspace IDs"""

    def as_dict(self) -> dict:
        """Serializes the UpdateCatalogWorkspaceBindingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.workspaces:
            body["workspaces"] = [v for v in self.workspaces]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateCatalogWorkspaceBindingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.workspaces:
            body["workspaces"] = self.workspaces
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateCatalogWorkspaceBindingsResponse:
        """Deserializes the UpdateCatalogWorkspaceBindingsResponse from a dictionary."""
        return cls(workspaces=d.get("workspaces", None))


@dataclass
class UpdateMetastoreAssignment:
    workspace_id: int
    """A workspace ID."""

    default_catalog_name: Optional[str] = None
    """The name of the default catalog in the metastore. This field is deprecated. Please use "Default
    Namespace API" to configure the default catalog for a Databricks workspace."""

    metastore_id: Optional[str] = None
    """The unique ID of the metastore."""

    def as_dict(self) -> dict:
        """Serializes the UpdateMetastoreAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default_catalog_name is not None:
            body["default_catalog_name"] = self.default_catalog_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateMetastoreAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default_catalog_name is not None:
            body["default_catalog_name"] = self.default_catalog_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateMetastoreAssignment:
        """Deserializes the UpdateMetastoreAssignment from a dictionary."""
        return cls(
            default_catalog_name=d.get("default_catalog_name", None),
            metastore_id=d.get("metastore_id", None),
            workspace_id=d.get("workspace_id", None),
        )


@dataclass
class UpdatePermissionsResponse:
    privilege_assignments: Optional[List[PrivilegeAssignment]] = None
    """The privileges assigned to each principal"""

    def as_dict(self) -> dict:
        """Serializes the UpdatePermissionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.privilege_assignments:
            body["privilege_assignments"] = [v.as_dict() for v in self.privilege_assignments]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdatePermissionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.privilege_assignments:
            body["privilege_assignments"] = self.privilege_assignments
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdatePermissionsResponse:
        """Deserializes the UpdatePermissionsResponse from a dictionary."""
        return cls(privilege_assignments=_repeated_dict(d, "privilege_assignments", PrivilegeAssignment))


@dataclass
class UpdateRequestExternalLineage:
    source: ExternalLineageObject
    """Source object of the external lineage relationship."""

    target: ExternalLineageObject
    """Target object of the external lineage relationship."""

    columns: Optional[List[ColumnRelationship]] = None
    """List of column relationships between source and target objects."""

    id: Optional[str] = None
    """Unique identifier of the external lineage relationship."""

    properties: Optional[Dict[str, str]] = None
    """Key-value properties associated with the external lineage relationship."""

    def as_dict(self) -> dict:
        """Serializes the UpdateRequestExternalLineage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source.as_dict()
        if self.target:
            body["target"] = self.target.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateRequestExternalLineage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.columns:
            body["columns"] = self.columns
        if self.id is not None:
            body["id"] = self.id
        if self.properties:
            body["properties"] = self.properties
        if self.source:
            body["source"] = self.source
        if self.target:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateRequestExternalLineage:
        """Deserializes the UpdateRequestExternalLineage from a dictionary."""
        return cls(
            columns=_repeated_dict(d, "columns", ColumnRelationship),
            id=d.get("id", None),
            properties=d.get("properties", None),
            source=_from_dict(d, "source", ExternalLineageObject),
            target=_from_dict(d, "target", ExternalLineageObject),
        )


@dataclass
class UpdateResponse:
    def as_dict(self) -> dict:
        """Serializes the UpdateResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateResponse:
        """Deserializes the UpdateResponse from a dictionary."""
        return cls()


@dataclass
class UpdateWorkspaceBindingsResponse:
    """A list of workspace IDs that are bound to the securable"""

    bindings: Optional[List[WorkspaceBinding]] = None
    """List of workspace bindings."""

    def as_dict(self) -> dict:
        """Serializes the UpdateWorkspaceBindingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bindings:
            body["bindings"] = [v.as_dict() for v in self.bindings]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateWorkspaceBindingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bindings:
            body["bindings"] = self.bindings
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateWorkspaceBindingsResponse:
        """Deserializes the UpdateWorkspaceBindingsResponse from a dictionary."""
        return cls(bindings=_repeated_dict(d, "bindings", WorkspaceBinding))


@dataclass
class ValidateCredentialResponse:
    is_dir: Optional[bool] = None
    """Whether the tested location is a directory in cloud storage. Only applicable for when purpose is
    **STORAGE**."""

    results: Optional[List[CredentialValidationResult]] = None
    """The results of the validation check."""

    def as_dict(self) -> dict:
        """Serializes the ValidateCredentialResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_dir is not None:
            body["isDir"] = self.is_dir
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ValidateCredentialResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_dir is not None:
            body["isDir"] = self.is_dir
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ValidateCredentialResponse:
        """Deserializes the ValidateCredentialResponse from a dictionary."""
        return cls(is_dir=d.get("isDir", None), results=_repeated_dict(d, "results", CredentialValidationResult))


class ValidateCredentialResult(Enum):
    """A enum represents the result of the file operation"""

    FAIL = "FAIL"
    PASS = "PASS"
    SKIP = "SKIP"


@dataclass
class ValidateStorageCredentialResponse:
    is_dir: Optional[bool] = None
    """Whether the tested location is a directory in cloud storage."""

    results: Optional[List[ValidationResult]] = None
    """The results of the validation check."""

    def as_dict(self) -> dict:
        """Serializes the ValidateStorageCredentialResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_dir is not None:
            body["isDir"] = self.is_dir
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ValidateStorageCredentialResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_dir is not None:
            body["isDir"] = self.is_dir
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ValidateStorageCredentialResponse:
        """Deserializes the ValidateStorageCredentialResponse from a dictionary."""
        return cls(is_dir=d.get("isDir", None), results=_repeated_dict(d, "results", ValidationResult))


@dataclass
class ValidationResult:
    message: Optional[str] = None
    """Error message would exist when the result does not equal to **PASS**."""

    operation: Optional[ValidationResultOperation] = None
    """The operation tested."""

    result: Optional[ValidationResultResult] = None
    """The results of the tested operation."""

    def as_dict(self) -> dict:
        """Serializes the ValidationResult into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.operation is not None:
            body["operation"] = self.operation.value
        if self.result is not None:
            body["result"] = self.result.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ValidationResult into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        if self.operation is not None:
            body["operation"] = self.operation
        if self.result is not None:
            body["result"] = self.result
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ValidationResult:
        """Deserializes the ValidationResult from a dictionary."""
        return cls(
            message=d.get("message", None),
            operation=_enum(d, "operation", ValidationResultOperation),
            result=_enum(d, "result", ValidationResultResult),
        )


class ValidationResultOperation(Enum):
    """A enum represents the file operation performed on the external location with the storage
    credential"""

    DELETE = "DELETE"
    LIST = "LIST"
    PATH_EXISTS = "PATH_EXISTS"
    READ = "READ"
    WRITE = "WRITE"


class ValidationResultResult(Enum):
    """A enum represents the result of the file operation"""

    FAIL = "FAIL"
    PASS = "PASS"
    SKIP = "SKIP"


@dataclass
class VolumeInfo:
    access_point: Optional[str] = None
    """The AWS access point to use when accesing s3 for this external location."""

    browse_only: Optional[bool] = None
    """Indicates whether the principal is limited to retrieving metadata for the associated object
    through the BROWSE privilege when include_browse is enabled in the request."""

    catalog_name: Optional[str] = None
    """The name of the catalog where the schema and the volume are"""

    comment: Optional[str] = None
    """The comment attached to the volume"""

    created_at: Optional[int] = None

    created_by: Optional[str] = None
    """The identifier of the user who created the volume"""

    encryption_details: Optional[EncryptionDetails] = None

    full_name: Optional[str] = None
    """The three-level (fully qualified) name of the volume"""

    metastore_id: Optional[str] = None
    """The unique identifier of the metastore"""

    name: Optional[str] = None
    """The name of the volume"""

    owner: Optional[str] = None
    """The identifier of the user who owns the volume"""

    schema_name: Optional[str] = None
    """The name of the schema where the volume is"""

    storage_location: Optional[str] = None
    """The storage location on the cloud"""

    updated_at: Optional[int] = None

    updated_by: Optional[str] = None
    """The identifier of the user who updated the volume last time"""

    volume_id: Optional[str] = None
    """The unique identifier of the volume"""

    volume_type: Optional[VolumeType] = None
    """The type of the volume. An external volume is located in the specified external location. A
    managed volume is located in the default location which is specified by the parent schema, or
    the parent catalog, or the Metastore. [Learn more]
    
    [Learn more]: https://docs.databricks.com/aws/en/volumes/managed-vs-external"""

    def as_dict(self) -> dict:
        """Serializes the VolumeInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_point is not None:
            body["access_point"] = self.access_point
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.encryption_details:
            body["encryption_details"] = self.encryption_details.as_dict()
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.volume_id is not None:
            body["volume_id"] = self.volume_id
        if self.volume_type is not None:
            body["volume_type"] = self.volume_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the VolumeInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_point is not None:
            body["access_point"] = self.access_point
        if self.browse_only is not None:
            body["browse_only"] = self.browse_only
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.encryption_details:
            body["encryption_details"] = self.encryption_details
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.volume_id is not None:
            body["volume_id"] = self.volume_id
        if self.volume_type is not None:
            body["volume_type"] = self.volume_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> VolumeInfo:
        """Deserializes the VolumeInfo from a dictionary."""
        return cls(
            access_point=d.get("access_point", None),
            browse_only=d.get("browse_only", None),
            catalog_name=d.get("catalog_name", None),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            encryption_details=_from_dict(d, "encryption_details", EncryptionDetails),
            full_name=d.get("full_name", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            schema_name=d.get("schema_name", None),
            storage_location=d.get("storage_location", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            volume_id=d.get("volume_id", None),
            volume_type=_enum(d, "volume_type", VolumeType),
        )


class VolumeType(Enum):

    EXTERNAL = "EXTERNAL"
    MANAGED = "MANAGED"


@dataclass
class WorkspaceBinding:
    workspace_id: int
    """Required"""

    binding_type: Optional[WorkspaceBindingBindingType] = None
    """One of READ_WRITE/READ_ONLY. Default is READ_WRITE."""

    def as_dict(self) -> dict:
        """Serializes the WorkspaceBinding into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.binding_type is not None:
            body["binding_type"] = self.binding_type.value
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceBinding into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.binding_type is not None:
            body["binding_type"] = self.binding_type
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceBinding:
        """Deserializes the WorkspaceBinding from a dictionary."""
        return cls(
            binding_type=_enum(d, "binding_type", WorkspaceBindingBindingType), workspace_id=d.get("workspace_id", None)
        )


class WorkspaceBindingBindingType(Enum):
    """Using `BINDING_TYPE_` prefix here to avoid conflict with `TableOperation` enum in
    `credentials_common.proto`."""

    BINDING_TYPE_READ_ONLY = "BINDING_TYPE_READ_ONLY"
    BINDING_TYPE_READ_WRITE = "BINDING_TYPE_READ_WRITE"


class AccountMetastoreAssignmentsAPI:
    """These APIs manage metastore assignments to a workspace."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, workspace_id: int, metastore_id: str, *, metastore_assignment: Optional[CreateMetastoreAssignment] = None
    ) -> AccountsCreateMetastoreAssignmentResponse:
        """Creates an assignment to a metastore for a workspace

        :param workspace_id: int
          Workspace ID.
        :param metastore_id: str
          Unity Catalog metastore ID
        :param metastore_assignment: :class:`CreateMetastoreAssignment` (optional)

        :returns: :class:`AccountsCreateMetastoreAssignmentResponse`
        """

        body = {}
        if metastore_assignment is not None:
            body["metastore_assignment"] = metastore_assignment.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST",
            f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}/metastores/{metastore_id}",
            body=body,
            headers=headers,
        )
        return AccountsCreateMetastoreAssignmentResponse.from_dict(res)

    def delete(self, workspace_id: int, metastore_id: str) -> AccountsDeleteMetastoreAssignmentResponse:
        """Deletes a metastore assignment to a workspace, leaving the workspace with no metastore.

        :param workspace_id: int
          Workspace ID.
        :param metastore_id: str
          Unity Catalog metastore ID

        :returns: :class:`AccountsDeleteMetastoreAssignmentResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}/metastores/{metastore_id}",
            headers=headers,
        )
        return AccountsDeleteMetastoreAssignmentResponse.from_dict(res)

    def get(self, workspace_id: int) -> AccountsMetastoreAssignment:
        """Gets the metastore assignment, if any, for the workspace specified by ID. If the workspace is assigned
        a metastore, the mapping will be returned. If no metastore is assigned to the workspace, the
        assignment will not be found and a 404 returned.

        :param workspace_id: int
          Workspace ID.

        :returns: :class:`AccountsMetastoreAssignment`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}/metastore", headers=headers
        )
        return AccountsMetastoreAssignment.from_dict(res)

    def list(self, metastore_id: str) -> Iterator[int]:
        """Gets a list of all Databricks workspace IDs that have been assigned to given metastore.

        :param metastore_id: str
          Unity Catalog metastore ID

        :returns: Iterator over int
        """

        headers = {
            "Accept": "application/json",
        }

        json = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}/workspaces", headers=headers
        )
        parsed = ListAccountMetastoreAssignmentsResponse.from_dict(json).workspace_ids
        return parsed if parsed is not None else []

    def update(
        self, workspace_id: int, metastore_id: str, *, metastore_assignment: Optional[UpdateMetastoreAssignment] = None
    ) -> AccountsUpdateMetastoreAssignmentResponse:
        """Updates an assignment to a metastore for a workspace. Currently, only the default catalog may be
        updated.

        :param workspace_id: int
          Workspace ID.
        :param metastore_id: str
          Unity Catalog metastore ID
        :param metastore_assignment: :class:`UpdateMetastoreAssignment` (optional)

        :returns: :class:`AccountsUpdateMetastoreAssignmentResponse`
        """

        body = {}
        if metastore_assignment is not None:
            body["metastore_assignment"] = metastore_assignment.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PUT",
            f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}/metastores/{metastore_id}",
            body=body,
            headers=headers,
        )
        return AccountsUpdateMetastoreAssignmentResponse.from_dict(res)


class AccountMetastoresAPI:
    """These APIs manage Unity Catalog metastores for an account. A metastore contains catalogs that can be
    associated with workspaces"""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, *, metastore_info: Optional[CreateAccountsMetastore] = None) -> AccountsCreateMetastoreResponse:
        """Creates a Unity Catalog metastore.

        :param metastore_info: :class:`CreateAccountsMetastore` (optional)

        :returns: :class:`AccountsCreateMetastoreResponse`
        """

        body = {}
        if metastore_info is not None:
            body["metastore_info"] = metastore_info.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do("POST", f"/api/2.0/accounts/{self._api.account_id}/metastores", body=body, headers=headers)
        return AccountsCreateMetastoreResponse.from_dict(res)

    def delete(self, metastore_id: str, *, force: Optional[bool] = None) -> AccountsDeleteMetastoreResponse:
        """Deletes a Unity Catalog metastore for an account, both specified by ID.

        :param metastore_id: str
          Unity Catalog metastore ID
        :param force: bool (optional)
          Force deletion even if the metastore is not empty. Default is false.

        :returns: :class:`AccountsDeleteMetastoreResponse`
        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}",
            query=query,
            headers=headers,
        )
        return AccountsDeleteMetastoreResponse.from_dict(res)

    def get(self, metastore_id: str) -> AccountsGetMetastoreResponse:
        """Gets a Unity Catalog metastore from an account, both specified by ID.

        :param metastore_id: str
          Unity Catalog metastore ID

        :returns: :class:`AccountsGetMetastoreResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}", headers=headers
        )
        return AccountsGetMetastoreResponse.from_dict(res)

    def list(self) -> Iterator[MetastoreInfo]:
        """Gets all Unity Catalog metastores associated with an account specified by ID.


        :returns: Iterator over :class:`MetastoreInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        json = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/metastores", headers=headers)
        parsed = AccountsListMetastoresResponse.from_dict(json).metastores
        return parsed if parsed is not None else []

    def update(
        self, metastore_id: str, *, metastore_info: Optional[UpdateAccountsMetastore] = None
    ) -> AccountsUpdateMetastoreResponse:
        """Updates an existing Unity Catalog metastore.

        :param metastore_id: str
          Unity Catalog metastore ID
        :param metastore_info: :class:`UpdateAccountsMetastore` (optional)
          Properties of the metastore to change.

        :returns: :class:`AccountsUpdateMetastoreResponse`
        """

        body = {}
        if metastore_info is not None:
            body["metastore_info"] = metastore_info.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PUT", f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}", body=body, headers=headers
        )
        return AccountsUpdateMetastoreResponse.from_dict(res)


class AccountStorageCredentialsAPI:
    """These APIs manage storage credentials for a particular metastore."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        metastore_id: str,
        *,
        credential_info: Optional[CreateAccountsStorageCredential] = None,
        skip_validation: Optional[bool] = None,
    ) -> AccountsCreateStorageCredentialInfo:
        """Creates a new storage credential. The request object is specific to the cloud: - **AwsIamRole** for
        AWS credentials - **AzureServicePrincipal** for Azure credentials - **GcpServiceAccountKey** for GCP
        credentials

        The caller must be a metastore admin and have the `CREATE_STORAGE_CREDENTIAL` privilege on the
        metastore.

        :param metastore_id: str
          Unity Catalog metastore ID
        :param credential_info: :class:`CreateAccountsStorageCredential` (optional)
        :param skip_validation: bool (optional)
          Optional, default false. Supplying true to this argument skips validation of the created set of
          credentials.

        :returns: :class:`AccountsCreateStorageCredentialInfo`
        """

        body = {}
        if credential_info is not None:
            body["credential_info"] = credential_info.as_dict()
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST",
            f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}/storage-credentials",
            body=body,
            headers=headers,
        )
        return AccountsCreateStorageCredentialInfo.from_dict(res)

    def delete(
        self, metastore_id: str, storage_credential_name: str, *, force: Optional[bool] = None
    ) -> AccountsDeleteStorageCredentialResponse:
        """Deletes a storage credential from the metastore. The caller must be an owner of the storage
        credential.

        :param metastore_id: str
          Unity Catalog metastore ID
        :param storage_credential_name: str
          Name of the storage credential.
        :param force: bool (optional)
          Force deletion even if the Storage Credential is not empty. Default is false.

        :returns: :class:`AccountsDeleteStorageCredentialResponse`
        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}/storage-credentials/{storage_credential_name}",
            query=query,
            headers=headers,
        )
        return AccountsDeleteStorageCredentialResponse.from_dict(res)

    def get(self, metastore_id: str, storage_credential_name: str) -> AccountsStorageCredentialInfo:
        """Gets a storage credential from the metastore. The caller must be a metastore admin, the owner of the
        storage credential, or have a level of privilege on the storage credential.

        :param metastore_id: str
          Unity Catalog metastore ID
        :param storage_credential_name: str
          Required. Name of the storage credential.

        :returns: :class:`AccountsStorageCredentialInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}/storage-credentials/{storage_credential_name}",
            headers=headers,
        )
        return AccountsStorageCredentialInfo.from_dict(res)

    def list(self, metastore_id: str) -> Iterator[StorageCredentialInfo]:
        """Gets a list of all storage credentials that have been assigned to given metastore.

        :param metastore_id: str
          Unity Catalog metastore ID

        :returns: Iterator over :class:`StorageCredentialInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        json = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}/storage-credentials",
            headers=headers,
        )
        parsed = ListAccountStorageCredentialsResponse.from_dict(json).storage_credentials
        return parsed if parsed is not None else []

    def update(
        self,
        metastore_id: str,
        storage_credential_name: str,
        *,
        credential_info: Optional[UpdateAccountsStorageCredential] = None,
        skip_validation: Optional[bool] = None,
    ) -> AccountsUpdateStorageCredentialResponse:
        """Updates a storage credential on the metastore. The caller must be the owner of the storage credential.
        If the caller is a metastore admin, only the **owner** credential can be changed.

        :param metastore_id: str
          Unity Catalog metastore ID
        :param storage_credential_name: str
          Name of the storage credential.
        :param credential_info: :class:`UpdateAccountsStorageCredential` (optional)
        :param skip_validation: bool (optional)
          Optional. Supplying true to this argument skips validation of the updated set of credentials.

        :returns: :class:`AccountsUpdateStorageCredentialResponse`
        """

        body = {}
        if credential_info is not None:
            body["credential_info"] = credential_info.as_dict()
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PUT",
            f"/api/2.0/accounts/{self._api.account_id}/metastores/{metastore_id}/storage-credentials/{storage_credential_name}",
            body=body,
            headers=headers,
        )
        return AccountsUpdateStorageCredentialResponse.from_dict(res)


class ArtifactAllowlistsAPI:
    """In Databricks Runtime 13.3 and above, you can add libraries and init scripts to the `allowlist` in UC so
    that users can leverage these artifacts on compute configured with shared access mode."""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, artifact_type: ArtifactType) -> ArtifactAllowlistInfo:
        """Get the artifact allowlist of a certain artifact type. The caller must be a metastore admin or have
        the **MANAGE ALLOWLIST** privilege on the metastore.

        :param artifact_type: :class:`ArtifactType`
          The artifact type of the allowlist.

        :returns: :class:`ArtifactAllowlistInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/artifact-allowlists/{artifact_type.value}", headers=headers)
        return ArtifactAllowlistInfo.from_dict(res)

    def update(
        self,
        artifact_type: ArtifactType,
        artifact_matchers: List[ArtifactMatcher],
        *,
        created_at: Optional[int] = None,
        created_by: Optional[str] = None,
        metastore_id: Optional[str] = None,
    ) -> ArtifactAllowlistInfo:
        """Set the artifact allowlist of a certain artifact type. The whole artifact allowlist is replaced with
        the new allowlist. The caller must be a metastore admin or have the **MANAGE ALLOWLIST** privilege on
        the metastore.

        :param artifact_type: :class:`ArtifactType`
          The artifact type of the allowlist.
        :param artifact_matchers: List[:class:`ArtifactMatcher`]
          A list of allowed artifact match patterns.
        :param created_at: int (optional)
          Time at which this artifact allowlist was set, in epoch milliseconds.
        :param created_by: str (optional)
          Username of the user who set the artifact allowlist.
        :param metastore_id: str (optional)
          Unique identifier of parent metastore.

        :returns: :class:`ArtifactAllowlistInfo`
        """

        body = {}
        if artifact_matchers is not None:
            body["artifact_matchers"] = [v.as_dict() for v in artifact_matchers]
        if created_at is not None:
            body["created_at"] = created_at
        if created_by is not None:
            body["created_by"] = created_by
        if metastore_id is not None:
            body["metastore_id"] = metastore_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PUT", f"/api/2.1/unity-catalog/artifact-allowlists/{artifact_type.value}", body=body, headers=headers
        )
        return ArtifactAllowlistInfo.from_dict(res)


class CatalogsAPI:
    """A catalog is the first layer of Unity Catalog’s three-level namespace. It’s used to organize your data
    assets. Users can see all catalogs on which they have been assigned the USE_CATALOG data permission.

    In Unity Catalog, admins and data stewards manage users and their access to data centrally across all of
    the workspaces in a Databricks account. Users in different workspaces can share access to the same data,
    depending on privileges granted centrally in Unity Catalog."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        *,
        comment: Optional[str] = None,
        connection_name: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
        properties: Optional[Dict[str, str]] = None,
        provider_name: Optional[str] = None,
        share_name: Optional[str] = None,
        storage_root: Optional[str] = None,
    ) -> CatalogInfo:
        """Creates a new catalog instance in the parent metastore if the caller is a metastore admin or has the
        **CREATE_CATALOG** privilege.

        :param name: str
          Name of catalog.
        :param comment: str (optional)
          User-provided free-form text description.
        :param connection_name: str (optional)
          The name of the connection to an external data source.
        :param options: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.
        :param properties: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.
        :param provider_name: str (optional)
          The name of delta sharing provider.

          A Delta Sharing catalog is a catalog that is based on a Delta share on a remote sharing server.
        :param share_name: str (optional)
          The name of the share under the share provider.
        :param storage_root: str (optional)
          Storage root URL for managed tables within catalog.

        :returns: :class:`CatalogInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if connection_name is not None:
            body["connection_name"] = connection_name
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options
        if properties is not None:
            body["properties"] = properties
        if provider_name is not None:
            body["provider_name"] = provider_name
        if share_name is not None:
            body["share_name"] = share_name
        if storage_root is not None:
            body["storage_root"] = storage_root
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/catalogs", body=body, headers=headers)
        return CatalogInfo.from_dict(res)

    def delete(self, name: str, *, force: Optional[bool] = None):
        """Deletes the catalog that matches the supplied name. The caller must be a metastore admin or the owner
        of the catalog.

        :param name: str
          The name of the catalog.
        :param force: bool (optional)
          Force deletion even if the catalog is not empty.


        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/catalogs/{name}", query=query, headers=headers)

    def get(self, name: str, *, include_browse: Optional[bool] = None) -> CatalogInfo:
        """Gets the specified catalog in a metastore. The caller must be a metastore admin, the owner of the
        catalog, or a user that has the **USE_CATALOG** privilege set for their account.

        :param name: str
          The name of the catalog.
        :param include_browse: bool (optional)
          Whether to include catalogs in the response for which the principal can only access selective
          metadata for

        :returns: :class:`CatalogInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/catalogs/{name}", query=query, headers=headers)
        return CatalogInfo.from_dict(res)

    def list(
        self,
        *,
        include_browse: Optional[bool] = None,
        include_unbound: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[CatalogInfo]:
        """Gets an array of catalogs in the metastore. If the caller is the metastore admin, all catalogs will be
        retrieved. Otherwise, only catalogs owned by the caller (or for which the caller has the
        **USE_CATALOG** privilege) will be retrieved. There is no guarantee of a specific ordering of the
        elements in the array.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param include_browse: bool (optional)
          Whether to include catalogs in the response for which the principal can only access selective
          metadata for
        :param include_unbound: bool (optional)
          Whether to include catalogs not bound to the workspace. Effective only if the user has permission to
          update the catalog–workspace binding.
        :param max_results: int (optional)
          Maximum number of catalogs to return. - when set to 0, the page length is set to a server configured
          value (recommended); - when set to a value greater than 0, the page length is the minimum of this
          value and a server configured value; - when set to a value less than 0, an invalid parameter error
          is returned; - If not set, all valid catalogs are returned (not recommended). - Note: The number of
          returned catalogs might be less than the specified max_results size, even zero. The only definitive
          indication that no further catalogs can be fetched is when the next_page_token is unset from the
          response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`CatalogInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        if include_unbound is not None:
            query["include_unbound"] = include_unbound
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/catalogs", query=query, headers=headers)
            if "catalogs" in json:
                for v in json["catalogs"]:
                    yield CatalogInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        name: str,
        *,
        comment: Optional[str] = None,
        enable_predictive_optimization: Optional[EnablePredictiveOptimization] = None,
        isolation_mode: Optional[CatalogIsolationMode] = None,
        new_name: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
        owner: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> CatalogInfo:
        """Updates the catalog that matches the supplied name. The caller must be either the owner of the
        catalog, or a metastore admin (when changing the owner field of the catalog).

        :param name: str
          The name of the catalog.
        :param comment: str (optional)
          User-provided free-form text description.
        :param enable_predictive_optimization: :class:`EnablePredictiveOptimization` (optional)
          Whether predictive optimization should be enabled for this object and objects under it.
        :param isolation_mode: :class:`CatalogIsolationMode` (optional)
          Whether the current securable is accessible from all workspaces or a specific set of workspaces.
        :param new_name: str (optional)
          New name for the catalog.
        :param options: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.
        :param owner: str (optional)
          Username of current owner of catalog.
        :param properties: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.

        :returns: :class:`CatalogInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = enable_predictive_optimization.value
        if isolation_mode is not None:
            body["isolation_mode"] = isolation_mode.value
        if new_name is not None:
            body["new_name"] = new_name
        if options is not None:
            body["options"] = options
        if owner is not None:
            body["owner"] = owner
        if properties is not None:
            body["properties"] = properties
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/catalogs/{name}", body=body, headers=headers)
        return CatalogInfo.from_dict(res)


class ConnectionsAPI:
    """Connections allow for creating a connection to an external data source.

    A connection is an abstraction of an external data source that can be connected from Databricks Compute.
    Creating a connection object is the first step to managing external data sources within Unity Catalog,
    with the second step being creating a data object (catalog, schema, or table) using the connection. Data
    objects derived from a connection can be written to or read from similar to other Unity Catalog data
    objects based on cloud storage. Users may create different types of connections with each connection
    having a unique set of configuration options to support credential management and other settings."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        connection_type: ConnectionType,
        options: Dict[str, str],
        *,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        read_only: Optional[bool] = None,
    ) -> ConnectionInfo:
        """Creates a new connection

        Creates a new connection to an external data source. It allows users to specify connection details and
        configurations for interaction with the external server.

        :param name: str
          Name of the connection.
        :param connection_type: :class:`ConnectionType`
          The type of connection.
        :param options: Dict[str,str]
          A map of key-value properties attached to the securable.
        :param comment: str (optional)
          User-provided free-form text description.
        :param properties: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.
        :param read_only: bool (optional)
          If the connection is read only.

        :returns: :class:`ConnectionInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if connection_type is not None:
            body["connection_type"] = connection_type.value
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options
        if properties is not None:
            body["properties"] = properties
        if read_only is not None:
            body["read_only"] = read_only
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/connections", body=body, headers=headers)
        return ConnectionInfo.from_dict(res)

    def delete(self, name: str):
        """Deletes the connection that matches the supplied name.

        :param name: str
          The name of the connection to be deleted.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/connections/{name}", headers=headers)

    def get(self, name: str) -> ConnectionInfo:
        """Gets a connection from it's name.

        :param name: str
          Name of the connection.

        :returns: :class:`ConnectionInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/connections/{name}", headers=headers)
        return ConnectionInfo.from_dict(res)

    def list(self, *, max_results: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[ConnectionInfo]:
        """List all connections.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param max_results: int (optional)
          Maximum number of connections to return. - If not set, all connections are returned (not
          recommended). - when set to a value greater than 0, the page length is the minimum of this value and
          a server configured value; - when set to 0, the page length is set to a server configured value
          (recommended); - when set to a value less than 0, an invalid parameter error is returned;
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ConnectionInfo`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/connections", query=query, headers=headers)
            if "connections" in json:
                for v in json["connections"]:
                    yield ConnectionInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self, name: str, options: Dict[str, str], *, new_name: Optional[str] = None, owner: Optional[str] = None
    ) -> ConnectionInfo:
        """Updates the connection that matches the supplied name.

        :param name: str
          Name of the connection.
        :param options: Dict[str,str]
          A map of key-value properties attached to the securable.
        :param new_name: str (optional)
          New name for the connection.
        :param owner: str (optional)
          Username of current owner of the connection.

        :returns: :class:`ConnectionInfo`
        """

        body = {}
        if new_name is not None:
            body["new_name"] = new_name
        if options is not None:
            body["options"] = options
        if owner is not None:
            body["owner"] = owner
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/connections/{name}", body=body, headers=headers)
        return ConnectionInfo.from_dict(res)


class CredentialsAPI:
    """A credential represents an authentication and authorization mechanism for accessing services on your cloud
    tenant. Each credential is subject to Unity Catalog access-control policies that control which users and
    groups can access the credential.

    To create credentials, you must be a Databricks account admin or have the `CREATE SERVICE CREDENTIAL`
    privilege. The user who creates the credential can delegate ownership to another user or group to manage
    permissions on it."""

    def __init__(self, api_client):
        self._api = api_client

    def create_credential(
        self,
        name: str,
        *,
        aws_iam_role: Optional[AwsIamRole] = None,
        azure_managed_identity: Optional[AzureManagedIdentity] = None,
        azure_service_principal: Optional[AzureServicePrincipal] = None,
        comment: Optional[str] = None,
        databricks_gcp_service_account: Optional[DatabricksGcpServiceAccount] = None,
        purpose: Optional[CredentialPurpose] = None,
        read_only: Optional[bool] = None,
        skip_validation: Optional[bool] = None,
    ) -> CredentialInfo:
        """Creates a new credential. The type of credential to be created is determined by the **purpose** field,
        which should be either **SERVICE** or **STORAGE**.

        The caller must be a metastore admin or have the metastore privilege **CREATE_STORAGE_CREDENTIAL** for
        storage credentials, or **CREATE_SERVICE_CREDENTIAL** for service credentials.

        :param name: str
          The credential name. The name must be unique among storage and service credentials within the
          metastore.
        :param aws_iam_role: :class:`AwsIamRole` (optional)
          The AWS IAM role configuration.
        :param azure_managed_identity: :class:`AzureManagedIdentity` (optional)
          The Azure managed identity configuration.
        :param azure_service_principal: :class:`AzureServicePrincipal` (optional)
          The Azure service principal configuration.
        :param comment: str (optional)
          Comment associated with the credential.
        :param databricks_gcp_service_account: :class:`DatabricksGcpServiceAccount` (optional)
          The Databricks managed GCP service account configuration.
        :param purpose: :class:`CredentialPurpose` (optional)
          Indicates the purpose of the credential.
        :param read_only: bool (optional)
          Whether the credential is usable only for read operations. Only applicable when purpose is
          **STORAGE**.
        :param skip_validation: bool (optional)
          Optional. Supplying true to this argument skips validation of the created set of credentials.

        :returns: :class:`CredentialInfo`
        """

        body = {}
        if aws_iam_role is not None:
            body["aws_iam_role"] = aws_iam_role.as_dict()
        if azure_managed_identity is not None:
            body["azure_managed_identity"] = azure_managed_identity.as_dict()
        if azure_service_principal is not None:
            body["azure_service_principal"] = azure_service_principal.as_dict()
        if comment is not None:
            body["comment"] = comment
        if databricks_gcp_service_account is not None:
            body["databricks_gcp_service_account"] = databricks_gcp_service_account.as_dict()
        if name is not None:
            body["name"] = name
        if purpose is not None:
            body["purpose"] = purpose.value
        if read_only is not None:
            body["read_only"] = read_only
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/credentials", body=body, headers=headers)
        return CredentialInfo.from_dict(res)

    def delete_credential(self, name_arg: str, *, force: Optional[bool] = None):
        """Deletes a service or storage credential from the metastore. The caller must be an owner of the
        credential.

        :param name_arg: str
          Name of the credential.
        :param force: bool (optional)
          Force an update even if there are dependent services (when purpose is **SERVICE**) or dependent
          external locations and external tables (when purpose is **STORAGE**).


        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/credentials/{name_arg}", query=query, headers=headers)

    def generate_temporary_service_credential(
        self,
        credential_name: str,
        *,
        azure_options: Optional[GenerateTemporaryServiceCredentialAzureOptions] = None,
        gcp_options: Optional[GenerateTemporaryServiceCredentialGcpOptions] = None,
    ) -> TemporaryCredentials:
        """Returns a set of temporary credentials generated using the specified service credential. The caller
        must be a metastore admin or have the metastore privilege **ACCESS** on the service credential.

        :param credential_name: str
          The name of the service credential used to generate a temporary credential
        :param azure_options: :class:`GenerateTemporaryServiceCredentialAzureOptions` (optional)
        :param gcp_options: :class:`GenerateTemporaryServiceCredentialGcpOptions` (optional)

        :returns: :class:`TemporaryCredentials`
        """

        body = {}
        if azure_options is not None:
            body["azure_options"] = azure_options.as_dict()
        if credential_name is not None:
            body["credential_name"] = credential_name
        if gcp_options is not None:
            body["gcp_options"] = gcp_options.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/temporary-service-credentials", body=body, headers=headers)
        return TemporaryCredentials.from_dict(res)

    def get_credential(self, name_arg: str) -> CredentialInfo:
        """Gets a service or storage credential from the metastore. The caller must be a metastore admin, the
        owner of the credential, or have any permission on the credential.

        :param name_arg: str
          Name of the credential.

        :returns: :class:`CredentialInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/credentials/{name_arg}", headers=headers)
        return CredentialInfo.from_dict(res)

    def list_credentials(
        self,
        *,
        include_unbound: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        purpose: Optional[CredentialPurpose] = None,
    ) -> Iterator[CredentialInfo]:
        """Gets an array of credentials (as __CredentialInfo__ objects).

        The array is limited to only the credentials that the caller has permission to access. If the caller
        is a metastore admin, retrieval of credentials is unrestricted. There is no guarantee of a specific
        ordering of the elements in the array.

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param include_unbound: bool (optional)
          Whether to include credentials not bound to the workspace. Effective only if the user has permission
          to update the credential–workspace binding.
        :param max_results: int (optional)
          Maximum number of credentials to return. - If not set, the default max page size is used. - When set
          to a value greater than 0, the page length is the minimum of this value and a server-configured
          value. - When set to 0, the page length is set to a server-configured value (recommended). - When
          set to a value less than 0, an invalid parameter error is returned.
        :param page_token: str (optional)
          Opaque token to retrieve the next page of results.
        :param purpose: :class:`CredentialPurpose` (optional)
          Return only credentials for the specified purpose.

        :returns: Iterator over :class:`CredentialInfo`
        """

        query = {}
        if include_unbound is not None:
            query["include_unbound"] = include_unbound
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if purpose is not None:
            query["purpose"] = purpose.value
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/credentials", query=query, headers=headers)
            if "credentials" in json:
                for v in json["credentials"]:
                    yield CredentialInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_credential(
        self,
        name_arg: str,
        *,
        aws_iam_role: Optional[AwsIamRole] = None,
        azure_managed_identity: Optional[AzureManagedIdentity] = None,
        azure_service_principal: Optional[AzureServicePrincipal] = None,
        comment: Optional[str] = None,
        databricks_gcp_service_account: Optional[DatabricksGcpServiceAccount] = None,
        force: Optional[bool] = None,
        isolation_mode: Optional[IsolationMode] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        read_only: Optional[bool] = None,
        skip_validation: Optional[bool] = None,
    ) -> CredentialInfo:
        """Updates a service or storage credential on the metastore.

        The caller must be the owner of the credential or a metastore admin or have the `MANAGE` permission.
        If the caller is a metastore admin, only the __owner__ field can be changed.

        :param name_arg: str
          Name of the credential.
        :param aws_iam_role: :class:`AwsIamRole` (optional)
          The AWS IAM role configuration.
        :param azure_managed_identity: :class:`AzureManagedIdentity` (optional)
          The Azure managed identity configuration.
        :param azure_service_principal: :class:`AzureServicePrincipal` (optional)
          The Azure service principal configuration.
        :param comment: str (optional)
          Comment associated with the credential.
        :param databricks_gcp_service_account: :class:`DatabricksGcpServiceAccount` (optional)
          The Databricks managed GCP service account configuration.
        :param force: bool (optional)
          Force an update even if there are dependent services (when purpose is **SERVICE**) or dependent
          external locations and external tables (when purpose is **STORAGE**).
        :param isolation_mode: :class:`IsolationMode` (optional)
          Whether the current securable is accessible from all workspaces or a specific set of workspaces.
        :param new_name: str (optional)
          New name of credential.
        :param owner: str (optional)
          Username of current owner of credential.
        :param read_only: bool (optional)
          Whether the credential is usable only for read operations. Only applicable when purpose is
          **STORAGE**.
        :param skip_validation: bool (optional)
          Supply true to this argument to skip validation of the updated credential.

        :returns: :class:`CredentialInfo`
        """

        body = {}
        if aws_iam_role is not None:
            body["aws_iam_role"] = aws_iam_role.as_dict()
        if azure_managed_identity is not None:
            body["azure_managed_identity"] = azure_managed_identity.as_dict()
        if azure_service_principal is not None:
            body["azure_service_principal"] = azure_service_principal.as_dict()
        if comment is not None:
            body["comment"] = comment
        if databricks_gcp_service_account is not None:
            body["databricks_gcp_service_account"] = databricks_gcp_service_account.as_dict()
        if force is not None:
            body["force"] = force
        if isolation_mode is not None:
            body["isolation_mode"] = isolation_mode.value
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if read_only is not None:
            body["read_only"] = read_only
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/credentials/{name_arg}", body=body, headers=headers)
        return CredentialInfo.from_dict(res)

    def validate_credential(
        self,
        *,
        aws_iam_role: Optional[AwsIamRole] = None,
        azure_managed_identity: Optional[AzureManagedIdentity] = None,
        credential_name: Optional[str] = None,
        databricks_gcp_service_account: Optional[DatabricksGcpServiceAccount] = None,
        external_location_name: Optional[str] = None,
        purpose: Optional[CredentialPurpose] = None,
        read_only: Optional[bool] = None,
        url: Optional[str] = None,
    ) -> ValidateCredentialResponse:
        """Validates a credential.

        For service credentials (purpose is **SERVICE**), either the __credential_name__ or the cloud-specific
        credential must be provided.

        For storage credentials (purpose is **STORAGE**), at least one of __external_location_name__ and
        __url__ need to be provided. If only one of them is provided, it will be used for validation. And if
        both are provided, the __url__ will be used for validation, and __external_location_name__ will be
        ignored when checking overlapping urls. Either the __credential_name__ or the cloud-specific
        credential must be provided.

        The caller must be a metastore admin or the credential owner or have the required permission on the
        metastore and the credential (e.g., **CREATE_EXTERNAL_LOCATION** when purpose is **STORAGE**).

        :param aws_iam_role: :class:`AwsIamRole` (optional)
        :param azure_managed_identity: :class:`AzureManagedIdentity` (optional)
        :param credential_name: str (optional)
          Required. The name of an existing credential or long-lived cloud credential to validate.
        :param databricks_gcp_service_account: :class:`DatabricksGcpServiceAccount` (optional)
        :param external_location_name: str (optional)
          The name of an existing external location to validate. Only applicable for storage credentials
          (purpose is **STORAGE**.)
        :param purpose: :class:`CredentialPurpose` (optional)
          The purpose of the credential. This should only be used when the credential is specified.
        :param read_only: bool (optional)
          Whether the credential is only usable for read operations. Only applicable for storage credentials
          (purpose is **STORAGE**.)
        :param url: str (optional)
          The external location url to validate. Only applicable when purpose is **STORAGE**.

        :returns: :class:`ValidateCredentialResponse`
        """

        body = {}
        if aws_iam_role is not None:
            body["aws_iam_role"] = aws_iam_role.as_dict()
        if azure_managed_identity is not None:
            body["azure_managed_identity"] = azure_managed_identity.as_dict()
        if credential_name is not None:
            body["credential_name"] = credential_name
        if databricks_gcp_service_account is not None:
            body["databricks_gcp_service_account"] = databricks_gcp_service_account.as_dict()
        if external_location_name is not None:
            body["external_location_name"] = external_location_name
        if purpose is not None:
            body["purpose"] = purpose.value
        if read_only is not None:
            body["read_only"] = read_only
        if url is not None:
            body["url"] = url
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/validate-credentials", body=body, headers=headers)
        return ValidateCredentialResponse.from_dict(res)


class EntityTagAssignmentsAPI:
    """Tags are attributes that include keys and optional values that you can use to organize and categorize
    entities in Unity Catalog. Entity tagging is currently supported on catalogs, schemas, tables (including
    views), columns, volumes. With these APIs, users can create, update, delete, and list tag assignments
    across Unity Catalog entities"""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, tag_assignment: EntityTagAssignment) -> EntityTagAssignment:
        """Creates a tag assignment for an Unity Catalog entity.

        To add tags to Unity Catalog entities, you must own the entity or have the following privileges: -
        **APPLY TAG** on the entity - **USE SCHEMA** on the entity's parent schema - **USE CATALOG** on the
        entity's parent catalog

        To add a governed tag to Unity Catalog entities, you must also have the **ASSIGN** or **MANAGE**
        permission on the tag policy. See [Manage tag policy permissions].

        [Manage tag policy permissions]: https://docs.databricks.com/aws/en/admin/tag-policies/manage-permissions

        :param tag_assignment: :class:`EntityTagAssignment`

        :returns: :class:`EntityTagAssignment`
        """

        body = tag_assignment.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/entity-tag-assignments", body=body, headers=headers)
        return EntityTagAssignment.from_dict(res)

    def delete(self, entity_type: str, entity_name: str, tag_key: str):
        """Deletes a tag assignment for an Unity Catalog entity by its key.

        To delete tags from Unity Catalog entities, you must own the entity or have the following privileges:
        - **APPLY TAG** on the entity - **USE_SCHEMA** on the entity's parent schema - **USE_CATALOG** on the
        entity's parent catalog

        To delete a governed tag from Unity Catalog entities, you must also have the **ASSIGN** or **MANAGE**
        permission on the tag policy. See [Manage tag policy permissions].

        [Manage tag policy permissions]: https://docs.databricks.com/aws/en/admin/tag-policies/manage-permissions

        :param entity_type: str
          The type of the entity to which the tag is assigned. Allowed values are: catalogs, schemas, tables,
          columns, volumes.
        :param entity_name: str
          The fully qualified name of the entity to which the tag is assigned
        :param tag_key: str
          Required. The key of the tag to delete


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE",
            f"/api/2.1/unity-catalog/entity-tag-assignments/{entity_type}/{entity_name}/tags/{tag_key}",
            headers=headers,
        )

    def get(self, entity_type: str, entity_name: str, tag_key: str) -> EntityTagAssignment:
        """Gets a tag assignment for an Unity Catalog entity by tag key.

        :param entity_type: str
          The type of the entity to which the tag is assigned. Allowed values are: catalogs, schemas, tables,
          columns, volumes.
        :param entity_name: str
          The fully qualified name of the entity to which the tag is assigned
        :param tag_key: str
          Required. The key of the tag

        :returns: :class:`EntityTagAssignment`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.1/unity-catalog/entity-tag-assignments/{entity_type}/{entity_name}/tags/{tag_key}",
            headers=headers,
        )
        return EntityTagAssignment.from_dict(res)

    def list(
        self, entity_type: str, entity_name: str, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[EntityTagAssignment]:
        """List tag assignments for an Unity Catalog entity

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param entity_type: str
          The type of the entity to which the tag is assigned. Allowed values are: catalogs, schemas, tables,
          columns, volumes.
        :param entity_name: str
          The fully qualified name of the entity to which the tag is assigned
        :param max_results: int (optional)
          Optional. Maximum number of tag assignments to return in a single page
        :param page_token: str (optional)
          Optional. Pagination token to retrieve the next page of results

        :returns: Iterator over :class:`EntityTagAssignment`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET",
                f"/api/2.1/unity-catalog/entity-tag-assignments/{entity_type}/{entity_name}/tags",
                query=query,
                headers=headers,
            )
            if "tag_assignments" in json:
                for v in json["tag_assignments"]:
                    yield EntityTagAssignment.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self, entity_type: str, entity_name: str, tag_key: str, tag_assignment: EntityTagAssignment, update_mask: str
    ) -> EntityTagAssignment:
        """Updates an existing tag assignment for an Unity Catalog entity.

        To update tags to Unity Catalog entities, you must own the entity or have the following privileges: -
        **APPLY TAG** on the entity - **USE SCHEMA** on the entity's parent schema - **USE CATALOG** on the
        entity's parent catalog

        To update a governed tag to Unity Catalog entities, you must also have the **ASSIGN** or **MANAGE**
        permission on the tag policy. See [Manage tag policy permissions].

        [Manage tag policy permissions]: https://docs.databricks.com/aws/en/admin/tag-policies/manage-permissions

        :param entity_type: str
          The type of the entity to which the tag is assigned. Allowed values are: catalogs, schemas, tables,
          columns, volumes.
        :param entity_name: str
          The fully qualified name of the entity to which the tag is assigned
        :param tag_key: str
          The key of the tag
        :param tag_assignment: :class:`EntityTagAssignment`
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`EntityTagAssignment`
        """

        body = tag_assignment.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH",
            f"/api/2.1/unity-catalog/entity-tag-assignments/{entity_type}/{entity_name}/tags/{tag_key}",
            query=query,
            body=body,
            headers=headers,
        )
        return EntityTagAssignment.from_dict(res)


class ExternalLineageAPI:
    """External Lineage APIs enable defining and managing lineage relationships between Databricks objects and
    external systems. These APIs allow users to capture data flows connecting Databricks tables, models, and
    file paths with external metadata objects.

    With these APIs, users can create, update, delete, and list lineage relationships with support for
    column-level mappings and custom properties."""

    def __init__(self, api_client):
        self._api = api_client

    def create_external_lineage_relationship(
        self, external_lineage_relationship: CreateRequestExternalLineage
    ) -> ExternalLineageRelationship:
        """Creates an external lineage relationship between a Databricks or external metadata object and another
        external metadata object.

        :param external_lineage_relationship: :class:`CreateRequestExternalLineage`

        :returns: :class:`ExternalLineageRelationship`
        """

        body = external_lineage_relationship.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/lineage-tracking/external-lineage", body=body, headers=headers)
        return ExternalLineageRelationship.from_dict(res)

    def delete_external_lineage_relationship(self, external_lineage_relationship: DeleteRequestExternalLineage):
        """Deletes an external lineage relationship between a Databricks or external metadata object and another
        external metadata object.

        :param external_lineage_relationship: :class:`DeleteRequestExternalLineage`


        """

        query = {}
        if external_lineage_relationship is not None:
            query["external_lineage_relationship"] = external_lineage_relationship.as_dict()
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", "/api/2.0/lineage-tracking/external-lineage", query=query, headers=headers)

    def list_external_lineage_relationships(
        self,
        object_info: ExternalLineageObject,
        lineage_direction: LineageDirection,
        *,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[ExternalLineageInfo]:
        """Lists external lineage relationships of a Databricks object or external metadata given a supplied
        direction.

        :param object_info: :class:`ExternalLineageObject`
          The object to query external lineage relationships for. Since this field is a query parameter,
          please flatten the nested fields. For example, if the object is a table, the query parameter should
          look like: `object_info.table.name=main.sales.customers`
        :param lineage_direction: :class:`LineageDirection`
          The lineage direction to filter on.
        :param page_size: int (optional)
          Specifies the maximum number of external lineage relationships to return in a single response. The
          value must be less than or equal to 1000.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ExternalLineageInfo`
        """

        query = {}
        if lineage_direction is not None:
            query["lineage_direction"] = lineage_direction.value
        if object_info is not None:
            query["object_info"] = object_info.as_dict()
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
            json = self._api.do("GET", "/api/2.0/lineage-tracking/external-lineage", query=query, headers=headers)
            if "external_lineage_relationships" in json:
                for v in json["external_lineage_relationships"]:
                    yield ExternalLineageInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_external_lineage_relationship(
        self, external_lineage_relationship: UpdateRequestExternalLineage, update_mask: str
    ) -> ExternalLineageRelationship:
        """Updates an external lineage relationship between a Databricks or external metadata object and another
        external metadata object.

        :param external_lineage_relationship: :class:`UpdateRequestExternalLineage`
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`ExternalLineageRelationship`
        """

        body = external_lineage_relationship.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/lineage-tracking/external-lineage", query=query, body=body, headers=headers
        )
        return ExternalLineageRelationship.from_dict(res)


class ExternalLocationsAPI:
    """An external location is an object that combines a cloud storage path with a storage credential that
    authorizes access to the cloud storage path. Each external location is subject to Unity Catalog
    access-control policies that control which users and groups can access the credential. If a user does not
    have access to an external location in Unity Catalog, the request fails and Unity Catalog does not attempt
    to authenticate to your cloud tenant on the user’s behalf.

    Databricks recommends using external locations rather than using storage credentials directly.

    To create external locations, you must be a metastore admin or a user with the
    **CREATE_EXTERNAL_LOCATION** privilege."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        url: str,
        credential_name: str,
        *,
        comment: Optional[str] = None,
        effective_enable_file_events: Optional[bool] = None,
        enable_file_events: Optional[bool] = None,
        encryption_details: Optional[EncryptionDetails] = None,
        fallback: Optional[bool] = None,
        file_event_queue: Optional[FileEventQueue] = None,
        read_only: Optional[bool] = None,
        skip_validation: Optional[bool] = None,
    ) -> ExternalLocationInfo:
        """Creates a new external location entry in the metastore. The caller must be a metastore admin or have
        the **CREATE_EXTERNAL_LOCATION** privilege on both the metastore and the associated storage
        credential.

        :param name: str
          Name of the external location.
        :param url: str
          Path URL of the external location.
        :param credential_name: str
          Name of the storage credential used with this location.
        :param comment: str (optional)
          User-provided free-form text description.
        :param effective_enable_file_events: bool (optional)
          The effective value of `enable_file_events` after applying server-side defaults.
        :param enable_file_events: bool (optional)
          Whether to enable file events on this external location. Default to `true`. Set to `false` to
          disable file events. The actual applied value may differ due to server-side defaults; check
          `effective_enable_file_events` for the effective state.
        :param encryption_details: :class:`EncryptionDetails` (optional)
        :param fallback: bool (optional)
          Indicates whether fallback mode is enabled for this external location. When fallback mode is
          enabled, the access to the location falls back to cluster credentials if UC credentials are not
          sufficient.
        :param file_event_queue: :class:`FileEventQueue` (optional)
          File event queue settings. If `enable_file_events` is not `false`, must be defined and have exactly
          one of the documented properties.
        :param read_only: bool (optional)
          Indicates whether the external location is read-only.
        :param skip_validation: bool (optional)
          Skips validation of the storage credential associated with the external location.

        :returns: :class:`ExternalLocationInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if credential_name is not None:
            body["credential_name"] = credential_name
        if effective_enable_file_events is not None:
            body["effective_enable_file_events"] = effective_enable_file_events
        if enable_file_events is not None:
            body["enable_file_events"] = enable_file_events
        if encryption_details is not None:
            body["encryption_details"] = encryption_details.as_dict()
        if fallback is not None:
            body["fallback"] = fallback
        if file_event_queue is not None:
            body["file_event_queue"] = file_event_queue.as_dict()
        if name is not None:
            body["name"] = name
        if read_only is not None:
            body["read_only"] = read_only
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        if url is not None:
            body["url"] = url
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/external-locations", body=body, headers=headers)
        return ExternalLocationInfo.from_dict(res)

    def delete(self, name: str, *, force: Optional[bool] = None):
        """Deletes the specified external location from the metastore. The caller must be the owner of the
        external location.

        :param name: str
          Name of the external location.
        :param force: bool (optional)
          Force deletion even if there are dependent external tables or mounts.


        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/external-locations/{name}", query=query, headers=headers)

    def get(self, name: str, *, include_browse: Optional[bool] = None) -> ExternalLocationInfo:
        """Gets an external location from the metastore. The caller must be either a metastore admin, the owner
        of the external location, or a user that has some privilege on the external location.

        :param name: str
          Name of the external location.
        :param include_browse: bool (optional)
          Whether to include external locations in the response for which the principal can only access
          selective metadata for

        :returns: :class:`ExternalLocationInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/external-locations/{name}", query=query, headers=headers)
        return ExternalLocationInfo.from_dict(res)

    def list(
        self,
        *,
        include_browse: Optional[bool] = None,
        include_unbound: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[ExternalLocationInfo]:
        """Gets an array of external locations (__ExternalLocationInfo__ objects) from the metastore. The caller
        must be a metastore admin, the owner of the external location, or a user that has some privilege on
        the external location. There is no guarantee of a specific ordering of the elements in the array.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param include_browse: bool (optional)
          Whether to include external locations in the response for which the principal can only access
          selective metadata for
        :param include_unbound: bool (optional)
          Whether to include external locations not bound to the workspace. Effective only if the user has
          permission to update the location–workspace binding.
        :param max_results: int (optional)
          Maximum number of external locations to return. If not set, all the external locations are returned
          (not recommended). - when set to a value greater than 0, the page length is the minimum of this
          value and a server configured value; - when set to 0, the page length is set to a server configured
          value (recommended); - when set to a value less than 0, an invalid parameter error is returned;
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ExternalLocationInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        if include_unbound is not None:
            query["include_unbound"] = include_unbound
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/external-locations", query=query, headers=headers)
            if "external_locations" in json:
                for v in json["external_locations"]:
                    yield ExternalLocationInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        name: str,
        *,
        comment: Optional[str] = None,
        credential_name: Optional[str] = None,
        effective_enable_file_events: Optional[bool] = None,
        enable_file_events: Optional[bool] = None,
        encryption_details: Optional[EncryptionDetails] = None,
        fallback: Optional[bool] = None,
        file_event_queue: Optional[FileEventQueue] = None,
        force: Optional[bool] = None,
        isolation_mode: Optional[IsolationMode] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        read_only: Optional[bool] = None,
        skip_validation: Optional[bool] = None,
        url: Optional[str] = None,
    ) -> ExternalLocationInfo:
        """Updates an external location in the metastore. The caller must be the owner of the external location,
        or be a metastore admin. In the second case, the admin can only update the name of the external
        location.

        :param name: str
          Name of the external location.
        :param comment: str (optional)
          User-provided free-form text description.
        :param credential_name: str (optional)
          Name of the storage credential used with this location.
        :param effective_enable_file_events: bool (optional)
          The effective value of `enable_file_events` after applying server-side defaults.
        :param enable_file_events: bool (optional)
          Whether to enable file events on this external location. Default to `true`. Set to `false` to
          disable file events. The actual applied value may differ due to server-side defaults; check
          `effective_enable_file_events` for the effective state.
        :param encryption_details: :class:`EncryptionDetails` (optional)
        :param fallback: bool (optional)
          Indicates whether fallback mode is enabled for this external location. When fallback mode is
          enabled, the access to the location falls back to cluster credentials if UC credentials are not
          sufficient.
        :param file_event_queue: :class:`FileEventQueue` (optional)
          File event queue settings. If `enable_file_events` is not `false`, must be defined and have exactly
          one of the documented properties.
        :param force: bool (optional)
          Force update even if changing url invalidates dependent external tables or mounts.
        :param isolation_mode: :class:`IsolationMode` (optional)
        :param new_name: str (optional)
          New name for the external location.
        :param owner: str (optional)
          The owner of the external location.
        :param read_only: bool (optional)
          Indicates whether the external location is read-only.
        :param skip_validation: bool (optional)
          Skips validation of the storage credential associated with the external location.
        :param url: str (optional)
          Path URL of the external location.

        :returns: :class:`ExternalLocationInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if credential_name is not None:
            body["credential_name"] = credential_name
        if effective_enable_file_events is not None:
            body["effective_enable_file_events"] = effective_enable_file_events
        if enable_file_events is not None:
            body["enable_file_events"] = enable_file_events
        if encryption_details is not None:
            body["encryption_details"] = encryption_details.as_dict()
        if fallback is not None:
            body["fallback"] = fallback
        if file_event_queue is not None:
            body["file_event_queue"] = file_event_queue.as_dict()
        if force is not None:
            body["force"] = force
        if isolation_mode is not None:
            body["isolation_mode"] = isolation_mode.value
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if read_only is not None:
            body["read_only"] = read_only
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        if url is not None:
            body["url"] = url
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/external-locations/{name}", body=body, headers=headers)
        return ExternalLocationInfo.from_dict(res)


class ExternalMetadataAPI:
    """External Metadata objects enable customers to register and manage metadata about external systems within
    Unity Catalog.

    These APIs provide a standardized way to create, update, retrieve, list, and delete external metadata
    objects. Fine-grained authorization ensures that only users with appropriate permissions can view and
    manage external metadata objects."""

    def __init__(self, api_client):
        self._api = api_client

    def create_external_metadata(self, external_metadata: ExternalMetadata) -> ExternalMetadata:
        """Creates a new external metadata object in the parent metastore if the caller is a metastore admin or
        has the **CREATE_EXTERNAL_METADATA** privilege. Grants **BROWSE** to all account users upon creation
        by default.

        :param external_metadata: :class:`ExternalMetadata`

        :returns: :class:`ExternalMetadata`
        """

        body = external_metadata.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/lineage-tracking/external-metadata", body=body, headers=headers)
        return ExternalMetadata.from_dict(res)

    def delete_external_metadata(self, name: str):
        """Deletes the external metadata object that matches the supplied name. The caller must be a metastore
        admin, the owner of the external metadata object, or a user that has the **MANAGE** privilege.

        :param name: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/lineage-tracking/external-metadata/{name}", headers=headers)

    def get_external_metadata(self, name: str) -> ExternalMetadata:
        """Gets the specified external metadata object in a metastore. The caller must be a metastore admin, the
        owner of the external metadata object, or a user that has the **BROWSE** privilege.

        :param name: str

        :returns: :class:`ExternalMetadata`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/lineage-tracking/external-metadata/{name}", headers=headers)
        return ExternalMetadata.from_dict(res)

    def list_external_metadata(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ExternalMetadata]:
        """Gets an array of external metadata objects in the metastore. If the caller is the metastore admin, all
        external metadata objects will be retrieved. Otherwise, only external metadata objects that the caller
        has **BROWSE** on will be retrieved. There is no guarantee of a specific ordering of the elements in
        the array.

        :param page_size: int (optional)
          Specifies the maximum number of external metadata objects to return in a single response. The value
          must be less than or equal to 1000.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ExternalMetadata`
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
            json = self._api.do("GET", "/api/2.0/lineage-tracking/external-metadata", query=query, headers=headers)
            if "external_metadata" in json:
                for v in json["external_metadata"]:
                    yield ExternalMetadata.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_external_metadata(
        self, name: str, external_metadata: ExternalMetadata, update_mask: str
    ) -> ExternalMetadata:
        """Updates the external metadata object that matches the supplied name. The caller can only update either
        the owner or other metadata fields in one request. The caller must be a metastore admin, the owner of
        the external metadata object, or a user that has the **MODIFY** privilege. If the caller is updating
        the owner, they must also have the **MANAGE** privilege.

        :param name: str
          Name of the external metadata object.
        :param external_metadata: :class:`ExternalMetadata`
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`ExternalMetadata`
        """

        body = external_metadata.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.0/lineage-tracking/external-metadata/{name}", query=query, body=body, headers=headers
        )
        return ExternalMetadata.from_dict(res)


class FunctionsAPI:
    """Functions implement User-Defined Functions (UDFs) in Unity Catalog.

    The function implementation can be any SQL expression or Query, and it can be invoked wherever a table
    reference is allowed in a query. In Unity Catalog, a function resides at the same level as a table, so it
    can be referenced with the form __catalog_name__.__schema_name__.__function_name__."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, function_info: CreateFunction) -> FunctionInfo:
        """**WARNING: This API is experimental and will change in future versions**

        Creates a new function

        The user must have the following permissions in order for the function to be created: -
        **USE_CATALOG** on the function's parent catalog - **USE_SCHEMA** and **CREATE_FUNCTION** on the
        function's parent schema

        :param function_info: :class:`CreateFunction`
          Partial __FunctionInfo__ specifying the function to be created.

        :returns: :class:`FunctionInfo`
        """

        body = {}
        if function_info is not None:
            body["function_info"] = function_info.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/functions", body=body, headers=headers)
        return FunctionInfo.from_dict(res)

    def delete(self, name: str, *, force: Optional[bool] = None):
        """Deletes the function that matches the supplied name. For the deletion to succeed, the user must
        satisfy one of the following conditions: - Is the owner of the function's parent catalog - Is the
        owner of the function's parent schema and have the **USE_CATALOG** privilege on its parent catalog -
        Is the owner of the function itself and have both the **USE_CATALOG** privilege on its parent catalog
        and the **USE_SCHEMA** privilege on its parent schema

        :param name: str
          The fully-qualified name of the function (of the form
          __catalog_name__.__schema_name__.__function__name__) .
        :param force: bool (optional)
          Force deletion even if the function is notempty.


        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/functions/{name}", query=query, headers=headers)

    def get(self, name: str, *, include_browse: Optional[bool] = None) -> FunctionInfo:
        """Gets a function from within a parent catalog and schema. For the fetch to succeed, the user must
        satisfy one of the following requirements: - Is a metastore admin - Is an owner of the function's
        parent catalog - Have the **USE_CATALOG** privilege on the function's parent catalog and be the owner
        of the function - Have the **USE_CATALOG** privilege on the function's parent catalog, the
        **USE_SCHEMA** privilege on the function's parent schema, and the **EXECUTE** privilege on the
        function itself

        :param name: str
          The fully-qualified name of the function (of the form
          __catalog_name__.__schema_name__.__function__name__).
        :param include_browse: bool (optional)
          Whether to include functions in the response for which the principal can only access selective
          metadata for

        :returns: :class:`FunctionInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/functions/{name}", query=query, headers=headers)
        return FunctionInfo.from_dict(res)

    def list(
        self,
        catalog_name: str,
        schema_name: str,
        *,
        include_browse: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[FunctionInfo]:
        """List functions within the specified parent catalog and schema. If the user is a metastore admin, all
        functions are returned in the output list. Otherwise, the user must have the **USE_CATALOG** privilege
        on the catalog and the **USE_SCHEMA** privilege on the schema, and the output list contains only
        functions for which either the user has the **EXECUTE** privilege or the user is the owner. There is
        no guarantee of a specific ordering of the elements in the array.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param catalog_name: str
          Name of parent catalog for functions of interest.
        :param schema_name: str
          Parent schema of functions.
        :param include_browse: bool (optional)
          Whether to include functions in the response for which the principal can only access selective
          metadata for
        :param max_results: int (optional)
          Maximum number of functions to return. If not set, all the functions are returned (not recommended).
          - when set to a value greater than 0, the page length is the minimum of this value and a server
          configured value; - when set to 0, the page length is set to a server configured value
          (recommended); - when set to a value less than 0, an invalid parameter error is returned;
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`FunctionInfo`
        """

        query = {}
        if catalog_name is not None:
            query["catalog_name"] = catalog_name
        if include_browse is not None:
            query["include_browse"] = include_browse
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if schema_name is not None:
            query["schema_name"] = schema_name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/functions", query=query, headers=headers)
            if "functions" in json:
                for v in json["functions"]:
                    yield FunctionInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(self, name: str, *, owner: Optional[str] = None) -> FunctionInfo:
        """Updates the function that matches the supplied name. Only the owner of the function can be updated. If
        the user is not a metastore admin, the user must be a member of the group that is the new function
        owner. - Is a metastore admin - Is the owner of the function's parent catalog - Is the owner of the
        function's parent schema and has the **USE_CATALOG** privilege on its parent catalog - Is the owner of
        the function itself and has the **USE_CATALOG** privilege on its parent catalog as well as the
        **USE_SCHEMA** privilege on the function's parent schema.

        :param name: str
          The fully-qualified name of the function (of the form
          __catalog_name__.__schema_name__.__function__name__).
        :param owner: str (optional)
          Username of current owner of the function.

        :returns: :class:`FunctionInfo`
        """

        body = {}
        if owner is not None:
            body["owner"] = owner
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/functions/{name}", body=body, headers=headers)
        return FunctionInfo.from_dict(res)


class GrantsAPI:
    """In Unity Catalog, data is secure by default. Initially, users have no access to data in a metastore.
    Access can be granted by either a metastore admin, the owner of an object, or the owner of the catalog or
    schema that contains the object. Securable objects in Unity Catalog are hierarchical and privileges are
    inherited downward.

    Securable objects in Unity Catalog are hierarchical and privileges are inherited downward. This means that
    granting a privilege on the catalog automatically grants the privilege to all current and future objects
    within the catalog. Similarly, privileges granted on a schema are inherited by all current and future
    objects within that schema."""

    def __init__(self, api_client):
        self._api = api_client

    def get(
        self,
        securable_type: str,
        full_name: str,
        *,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        principal: Optional[str] = None,
    ) -> GetPermissionsResponse:
        """Gets the permissions for a securable. Does not include inherited permissions.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param securable_type: str
          Type of securable.
        :param full_name: str
          Full name of securable.
        :param max_results: int (optional)
          Specifies the maximum number of privileges to return (page length). Every PrivilegeAssignment
          present in a single page response is guaranteed to contain all the privileges granted on the
          requested Securable for the respective principal.

          If not set, all the permissions are returned. If set to - lesser than 0: invalid parameter error -
          0: page length is set to a server configured value - lesser than 150 but greater than 0: invalid
          parameter error (this is to ensure that server is able to return at least one complete
          PrivilegeAssignment in a single page response) - greater than (or equal to) 150: page length is the
          minimum of this value and a server configured value
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.
        :param principal: str (optional)
          If provided, only the permissions for the specified principal (user or group) are returned.

        :returns: :class:`GetPermissionsResponse`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if principal is not None:
            query["principal"] = principal
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}", query=query, headers=headers
        )
        return GetPermissionsResponse.from_dict(res)

    def get_effective(
        self,
        securable_type: str,
        full_name: str,
        *,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        principal: Optional[str] = None,
    ) -> EffectivePermissionsList:
        """Gets the effective permissions for a securable. Includes inherited permissions from any parent
        securables.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param securable_type: str
          Type of securable.
        :param full_name: str
          Full name of securable.
        :param max_results: int (optional)
          Specifies the maximum number of privileges to return (page length). Every
          EffectivePrivilegeAssignment present in a single page response is guaranteed to contain all the
          effective privileges granted on (or inherited by) the requested Securable for the respective
          principal.

          If not set, all the effective permissions are returned. If set to - lesser than 0: invalid parameter
          error - 0: page length is set to a server configured value - lesser than 150 but greater than 0:
          invalid parameter error (this is to ensure that server is able to return at least one complete
          EffectivePrivilegeAssignment in a single page response) - greater than (or equal to) 150: page
          length is the minimum of this value and a server configured value
        :param page_token: str (optional)
          Opaque token for the next page of results (pagination).
        :param principal: str (optional)
          If provided, only the effective permissions for the specified principal (user or group) are
          returned.

        :returns: :class:`EffectivePermissionsList`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if principal is not None:
            query["principal"] = principal
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.1/unity-catalog/effective-permissions/{securable_type}/{full_name}",
            query=query,
            headers=headers,
        )
        return EffectivePermissionsList.from_dict(res)

    def update(
        self, securable_type: str, full_name: str, *, changes: Optional[List[PermissionsChange]] = None
    ) -> UpdatePermissionsResponse:
        """Updates the permissions for a securable.

        :param securable_type: str
          Type of securable.
        :param full_name: str
          Full name of securable.
        :param changes: List[:class:`PermissionsChange`] (optional)
          Array of permissions change objects.

        :returns: :class:`UpdatePermissionsResponse`
        """

        body = {}
        if changes is not None:
            body["changes"] = [v.as_dict() for v in changes]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}", body=body, headers=headers
        )
        return UpdatePermissionsResponse.from_dict(res)


class MetastoresAPI:
    """A metastore is the top-level container of objects in Unity Catalog. It stores data assets (tables and
    views) and the permissions that govern access to them. Databricks account admins can create metastores and
    assign them to Databricks workspaces to control which workloads use each metastore. For a workspace to use
    Unity Catalog, it must have a Unity Catalog metastore attached.

    Each metastore is configured with a root storage location in a cloud storage account. This storage
    location is used for metadata and managed tables data.

    NOTE: This metastore is distinct from the metastore included in Databricks workspaces created before Unity
    Catalog was released. If your workspace includes a legacy Hive metastore, the data in that metastore is
    available in a catalog named hive_metastore."""

    def __init__(self, api_client):
        self._api = api_client

    def assign(self, workspace_id: int, metastore_id: str, default_catalog_name: str):
        """Creates a new metastore assignment. If an assignment for the same __workspace_id__ exists, it will be
        overwritten by the new __metastore_id__ and __default_catalog_name__. The caller must be an account
        admin.

        :param workspace_id: int
          A workspace ID.
        :param metastore_id: str
          The unique ID of the metastore.
        :param default_catalog_name: str
          The name of the default catalog in the metastore. This field is deprecated. Please use "Default
          Namespace API" to configure the default catalog for a Databricks workspace.


        """

        body = {}
        if default_catalog_name is not None:
            body["default_catalog_name"] = default_catalog_name
        if metastore_id is not None:
            body["metastore_id"] = metastore_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PUT", f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", body=body, headers=headers)

    def create(
        self,
        name: str,
        *,
        external_access_enabled: Optional[bool] = None,
        region: Optional[str] = None,
        storage_root: Optional[str] = None,
    ) -> MetastoreInfo:
        """Creates a new metastore based on a provided name and optional storage root path. By default (if the
        __owner__ field is not set), the owner of the new metastore is the user calling the
        __createMetastore__ API. If the __owner__ field is set to the empty string (**""**), the ownership is
        assigned to the System User instead.

        :param name: str
          The user-specified name of the metastore.
        :param external_access_enabled: bool (optional)
          Whether to allow non-DBR clients to directly access entities under the metastore.
        :param region: str (optional)
          Cloud region which the metastore serves (e.g., `us-west-2`, `westus`).
        :param storage_root: str (optional)
          The storage root URL for metastore

        :returns: :class:`MetastoreInfo`
        """

        body = {}
        if external_access_enabled is not None:
            body["external_access_enabled"] = external_access_enabled
        if name is not None:
            body["name"] = name
        if region is not None:
            body["region"] = region
        if storage_root is not None:
            body["storage_root"] = storage_root
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/metastores", body=body, headers=headers)
        return MetastoreInfo.from_dict(res)

    def current(self) -> MetastoreAssignment:
        """Gets the metastore assignment for the workspace being accessed.


        :returns: :class:`MetastoreAssignment`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.1/unity-catalog/current-metastore-assignment", headers=headers)
        return MetastoreAssignment.from_dict(res)

    def delete(self, id: str, *, force: Optional[bool] = None):
        """Deletes a metastore. The caller must be a metastore admin.

        :param id: str
          Unique ID of the metastore.
        :param force: bool (optional)
          Force deletion even if the metastore is not empty. Default is false.


        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/metastores/{id}", query=query, headers=headers)

    def get(self, id: str) -> MetastoreInfo:
        """Gets a metastore that matches the supplied ID. The caller must be a metastore admin to retrieve this
        info.

        :param id: str
          Unique ID of the metastore.

        :returns: :class:`MetastoreInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/metastores/{id}", headers=headers)
        return MetastoreInfo.from_dict(res)

    def list(self, *, max_results: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[MetastoreInfo]:
        """Gets an array of the available metastores (as __MetastoreInfo__ objects). The caller must be an admin
        to retrieve this info. There is no guarantee of a specific ordering of the elements in the array.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param max_results: int (optional)
          Maximum number of metastores to return. - when set to a value greater than 0, the page length is the
          minimum of this value and a server configured value; - when set to 0, the page length is set to a
          server configured value (recommended); - when set to a value less than 0, an invalid parameter error
          is returned; - If not set, all the metastores are returned (not recommended). - Note: The number of
          returned metastores might be less than the specified max_results size, even zero. The only
          definitive indication that no further metastores can be fetched is when the next_page_token is unset
          from the response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`MetastoreInfo`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/metastores", query=query, headers=headers)
            if "metastores" in json:
                for v in json["metastores"]:
                    yield MetastoreInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def summary(self) -> GetMetastoreSummaryResponse:
        """Gets information about a metastore. This summary includes the storage credential, the cloud vendor,
        the cloud region, and the global metastore ID.


        :returns: :class:`GetMetastoreSummaryResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.1/unity-catalog/metastore_summary", headers=headers)
        return GetMetastoreSummaryResponse.from_dict(res)

    def unassign(self, workspace_id: int, metastore_id: str):
        """Deletes a metastore assignment. The caller must be an account administrator.

        :param workspace_id: int
          A workspace ID.
        :param metastore_id: str
          Query for the ID of the metastore to delete.


        """

        query = {}
        if metastore_id is not None:
            query["metastore_id"] = metastore_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", query=query, headers=headers
        )

    def update(
        self,
        id: str,
        *,
        delta_sharing_organization_name: Optional[str] = None,
        delta_sharing_recipient_token_lifetime_in_seconds: Optional[int] = None,
        delta_sharing_scope: Optional[DeltaSharingScopeEnum] = None,
        external_access_enabled: Optional[bool] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        privilege_model_version: Optional[str] = None,
        storage_root_credential_id: Optional[str] = None,
    ) -> MetastoreInfo:
        """Updates information for a specific metastore. The caller must be a metastore admin. If the __owner__
        field is set to the empty string (**""**), the ownership is updated to the System User.

        :param id: str
          Unique ID of the metastore.
        :param delta_sharing_organization_name: str (optional)
          The organization name of a Delta Sharing entity, to be used in Databricks-to-Databricks Delta
          Sharing as the official name.
        :param delta_sharing_recipient_token_lifetime_in_seconds: int (optional)
          The lifetime of delta sharing recipient token in seconds.
        :param delta_sharing_scope: :class:`DeltaSharingScopeEnum` (optional)
          The scope of Delta Sharing enabled for the metastore.
        :param external_access_enabled: bool (optional)
          Whether to allow non-DBR clients to directly access entities under the metastore.
        :param new_name: str (optional)
          New name for the metastore.
        :param owner: str (optional)
          The owner of the metastore.
        :param privilege_model_version: str (optional)
          Privilege model version of the metastore, of the form `major.minor` (e.g., `1.0`).
        :param storage_root_credential_id: str (optional)
          UUID of storage credential to access the metastore storage_root.

        :returns: :class:`MetastoreInfo`
        """

        body = {}
        if delta_sharing_organization_name is not None:
            body["delta_sharing_organization_name"] = delta_sharing_organization_name
        if delta_sharing_recipient_token_lifetime_in_seconds is not None:
            body["delta_sharing_recipient_token_lifetime_in_seconds"] = (
                delta_sharing_recipient_token_lifetime_in_seconds
            )
        if delta_sharing_scope is not None:
            body["delta_sharing_scope"] = delta_sharing_scope.value
        if external_access_enabled is not None:
            body["external_access_enabled"] = external_access_enabled
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if privilege_model_version is not None:
            body["privilege_model_version"] = privilege_model_version
        if storage_root_credential_id is not None:
            body["storage_root_credential_id"] = storage_root_credential_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/metastores/{id}", body=body, headers=headers)
        return MetastoreInfo.from_dict(res)

    def update_assignment(
        self, workspace_id: int, *, default_catalog_name: Optional[str] = None, metastore_id: Optional[str] = None
    ):
        """Updates a metastore assignment. This operation can be used to update __metastore_id__ or
        __default_catalog_name__ for a specified Workspace, if the Workspace is already assigned a metastore.
        The caller must be an account admin to update __metastore_id__; otherwise, the caller can be a
        Workspace admin.

        :param workspace_id: int
          A workspace ID.
        :param default_catalog_name: str (optional)
          The name of the default catalog in the metastore. This field is deprecated. Please use "Default
          Namespace API" to configure the default catalog for a Databricks workspace.
        :param metastore_id: str (optional)
          The unique ID of the metastore.


        """

        body = {}
        if default_catalog_name is not None:
            body["default_catalog_name"] = default_catalog_name
        if metastore_id is not None:
            body["metastore_id"] = metastore_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PATCH", f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", body=body, headers=headers)


class ModelVersionsAPI:
    """Databricks provides a hosted version of MLflow Model Registry in Unity Catalog. Models in Unity Catalog
    provide centralized access control, auditing, lineage, and discovery of ML models across Databricks
    workspaces.

    This API reference documents the REST endpoints for managing model versions in Unity Catalog. For more
    details, see the [registered models API docs](/api/workspace/registeredmodels)."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, full_name: str, version: int):
        """Deletes a model version from the specified registered model. Any aliases assigned to the model version
        will also be deleted.

        The caller must be a metastore admin or an owner of the parent registered model. For the latter case,
        the caller must also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the
        **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          The three-level (fully qualified) name of the model version
        :param version: int
          The integer version number of the model version


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/models/{full_name}/versions/{version}", headers=headers)

    def get(
        self,
        full_name: str,
        version: int,
        *,
        include_aliases: Optional[bool] = None,
        include_browse: Optional[bool] = None,
    ) -> ModelVersionInfo:
        """Get a model version.

        The caller must be a metastore admin or an owner of (or have the **EXECUTE** privilege on) the parent
        registered model. For the latter case, the caller must also be the owner or have the **USE_CATALOG**
        privilege on the parent catalog and the **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          The three-level (fully qualified) name of the model version
        :param version: int
          The integer version number of the model version
        :param include_aliases: bool (optional)
          Whether to include aliases associated with the model version in the response
        :param include_browse: bool (optional)
          Whether to include model versions in the response for which the principal can only access selective
          metadata for

        :returns: :class:`ModelVersionInfo`
        """

        query = {}
        if include_aliases is not None:
            query["include_aliases"] = include_aliases
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.1/unity-catalog/models/{full_name}/versions/{version}", query=query, headers=headers
        )
        return ModelVersionInfo.from_dict(res)

    def get_by_alias(self, full_name: str, alias: str, *, include_aliases: Optional[bool] = None) -> ModelVersionInfo:
        """Get a model version by alias.

        The caller must be a metastore admin or an owner of (or have the **EXECUTE** privilege on) the
        registered model. For the latter case, the caller must also be the owner or have the **USE_CATALOG**
        privilege on the parent catalog and the **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          The three-level (fully qualified) name of the registered model
        :param alias: str
          The name of the alias
        :param include_aliases: bool (optional)
          Whether to include aliases associated with the model version in the response

        :returns: :class:`ModelVersionInfo`
        """

        query = {}
        if include_aliases is not None:
            query["include_aliases"] = include_aliases
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.1/unity-catalog/models/{full_name}/aliases/{alias}", query=query, headers=headers
        )
        return ModelVersionInfo.from_dict(res)

    def list(
        self,
        full_name: str,
        *,
        include_browse: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[ModelVersionInfo]:
        """List model versions. You can list model versions under a particular schema, or list all model versions
        in the current metastore.

        The returned models are filtered based on the privileges of the calling user. For example, the
        metastore admin is able to list all the model versions. A regular user needs to be the owner or have
        the **EXECUTE** privilege on the parent registered model to recieve the model versions in the
        response. For the latter case, the caller must also be the owner or have the **USE_CATALOG** privilege
        on the parent catalog and the **USE_SCHEMA** privilege on the parent schema.

        There is no guarantee of a specific ordering of the elements in the response. The elements in the
        response will not contain any aliases or tags.

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param full_name: str
          The full three-level name of the registered model under which to list model versions
        :param include_browse: bool (optional)
          Whether to include model versions in the response for which the principal can only access selective
          metadata for
        :param max_results: int (optional)
          Maximum number of model versions to return. If not set, the page length is set to a server
          configured value (100, as of 1/3/2024). - when set to a value greater than 0, the page length is the
          minimum of this value and a server configured value(1000, as of 1/3/2024); - when set to 0, the page
          length is set to a server configured value (100, as of 1/3/2024) (recommended); - when set to a
          value less than 0, an invalid parameter error is returned;
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ModelVersionInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", f"/api/2.1/unity-catalog/models/{full_name}/versions", query=query, headers=headers
            )
            if "model_versions" in json:
                for v in json["model_versions"]:
                    yield ModelVersionInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        full_name: str,
        version: int,
        *,
        aliases: Optional[List[RegisteredModelAlias]] = None,
        catalog_name: Optional[str] = None,
        comment: Optional[str] = None,
        created_at: Optional[int] = None,
        created_by: Optional[str] = None,
        id: Optional[str] = None,
        metastore_id: Optional[str] = None,
        model_name: Optional[str] = None,
        model_version_dependencies: Optional[DependencyList] = None,
        run_id: Optional[str] = None,
        run_workspace_id: Optional[int] = None,
        schema_name: Optional[str] = None,
        source: Optional[str] = None,
        status: Optional[ModelVersionInfoStatus] = None,
        storage_location: Optional[str] = None,
        updated_at: Optional[int] = None,
        updated_by: Optional[str] = None,
    ) -> ModelVersionInfo:
        """Updates the specified model version.

        The caller must be a metastore admin or an owner of the parent registered model. For the latter case,
        the caller must also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the
        **USE_SCHEMA** privilege on the parent schema.

        Currently only the comment of the model version can be updated.

        :param full_name: str
          The three-level (fully qualified) name of the model version
        :param version: int
          The integer version number of the model version
        :param aliases: List[:class:`RegisteredModelAlias`] (optional)
          List of aliases associated with the model version
        :param catalog_name: str (optional)
          The name of the catalog containing the model version
        :param comment: str (optional)
          The comment attached to the model version
        :param created_at: int (optional)
        :param created_by: str (optional)
          The identifier of the user who created the model version
        :param id: str (optional)
          The unique identifier of the model version
        :param metastore_id: str (optional)
          The unique identifier of the metastore containing the model version
        :param model_name: str (optional)
          The name of the parent registered model of the model version, relative to parent schema
        :param model_version_dependencies: :class:`DependencyList` (optional)
          Model version dependencies, for feature-store packaged models
        :param run_id: str (optional)
          MLflow run ID used when creating the model version, if ``source`` was generated by an experiment run
          stored in an MLflow tracking server
        :param run_workspace_id: int (optional)
          ID of the Databricks workspace containing the MLflow run that generated this model version, if
          applicable
        :param schema_name: str (optional)
          The name of the schema containing the model version, relative to parent catalog
        :param source: str (optional)
          URI indicating the location of the source artifacts (files) for the model version
        :param status: :class:`ModelVersionInfoStatus` (optional)
          Current status of the model version. Newly created model versions start in PENDING_REGISTRATION
          status, then move to READY status once the model version files are uploaded and the model version is
          finalized. Only model versions in READY status can be loaded for inference or served.
        :param storage_location: str (optional)
          The storage location on the cloud under which model version data files are stored
        :param updated_at: int (optional)
        :param updated_by: str (optional)
          The identifier of the user who updated the model version last time

        :returns: :class:`ModelVersionInfo`
        """

        body = {}
        if aliases is not None:
            body["aliases"] = [v.as_dict() for v in aliases]
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        if comment is not None:
            body["comment"] = comment
        if created_at is not None:
            body["created_at"] = created_at
        if created_by is not None:
            body["created_by"] = created_by
        if id is not None:
            body["id"] = id
        if metastore_id is not None:
            body["metastore_id"] = metastore_id
        if model_name is not None:
            body["model_name"] = model_name
        if model_version_dependencies is not None:
            body["model_version_dependencies"] = model_version_dependencies.as_dict()
        if run_id is not None:
            body["run_id"] = run_id
        if run_workspace_id is not None:
            body["run_workspace_id"] = run_workspace_id
        if schema_name is not None:
            body["schema_name"] = schema_name
        if source is not None:
            body["source"] = source
        if status is not None:
            body["status"] = status.value
        if storage_location is not None:
            body["storage_location"] = storage_location
        if updated_at is not None:
            body["updated_at"] = updated_at
        if updated_by is not None:
            body["updated_by"] = updated_by
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.1/unity-catalog/models/{full_name}/versions/{version}", body=body, headers=headers
        )
        return ModelVersionInfo.from_dict(res)


class OnlineTablesAPI:
    """Online tables provide lower latency and higher QPS access to data from Delta tables."""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_online_table_active(
        self, name: str, timeout=timedelta(minutes=20), callback: Optional[Callable[[OnlineTable], None]] = None
    ) -> OnlineTable:
        deadline = time.time() + timeout.total_seconds()
        target_states = (ProvisioningInfoState.ACTIVE,)
        failure_states = (ProvisioningInfoState.FAILED,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get(name=name)
            status = poll.unity_catalog_provisioning_state
            status_message = f"current status: {status}"
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

    def create(self, table: OnlineTable) -> Wait[OnlineTable]:
        """Create a new Online Table.

        :param table: :class:`OnlineTable`
          Specification of the online table to be created.

        :returns:
          Long-running operation waiter for :class:`OnlineTable`.
          See :method:wait_get_online_table_active for more details.
        """

        body = table.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.0/online-tables", body=body, headers=headers)
        return Wait(
            self.wait_get_online_table_active, response=OnlineTable.from_dict(op_response), name=op_response["name"]
        )

    def create_and_wait(self, table: OnlineTable, timeout=timedelta(minutes=20)) -> OnlineTable:
        return self.create(table=table).result(timeout=timeout)

    def delete(self, name: str):
        """Delete an online table. Warning: This will delete all the data in the online table. If the source
        Delta table was deleted or modified since this Online Table was created, this will lose the data
        forever!

        :param name: str
          Full three-part (catalog, schema, table) name of the table.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/online-tables/{name}", headers=headers)

    def get(self, name: str) -> OnlineTable:
        """Get information about an existing online table and its status.

        :param name: str
          Full three-part (catalog, schema, table) name of the table.

        :returns: :class:`OnlineTable`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/online-tables/{name}", headers=headers)
        return OnlineTable.from_dict(res)


class PoliciesAPI:
    """Attribute-Based Access Control (ABAC) provides high leverage governance for enforcing compliance policies
    in Unity Catalog. With ABAC policies, access is controlled in a hierarchical and scalable manner, based on
    data attributes rather than specific resources, enabling more flexible and comprehensive access control.
    ABAC policies in Unity Catalog support conditions on securable properties, governance tags, and
    environment contexts. Callers must have the `MANAGE` privilege on a securable to view, create, update, or
    delete ABAC policies."""

    def __init__(self, api_client):
        self._api = api_client

    def create_policy(self, policy_info: PolicyInfo) -> PolicyInfo:
        """Creates a new policy on a securable. The new policy applies to the securable and all its descendants.

        :param policy_info: :class:`PolicyInfo`
          Required. The policy to create.

        :returns: :class:`PolicyInfo`
        """

        body = policy_info.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/policies", body=body, headers=headers)
        return PolicyInfo.from_dict(res)

    def delete_policy(self, on_securable_type: str, on_securable_fullname: str, name: str) -> DeletePolicyResponse:
        """Delete an ABAC policy defined on a securable.

        :param on_securable_type: str
          Required. The type of the securable to delete the policy from.
        :param on_securable_fullname: str
          Required. The fully qualified name of the securable to delete the policy from.
        :param name: str
          Required. The name of the policy to delete

        :returns: :class:`DeletePolicyResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE",
            f"/api/2.1/unity-catalog/policies/{on_securable_type}/{on_securable_fullname}/{name}",
            headers=headers,
        )
        return DeletePolicyResponse.from_dict(res)

    def get_policy(self, on_securable_type: str, on_securable_fullname: str, name: str) -> PolicyInfo:
        """Get the policy definition on a securable

        :param on_securable_type: str
          Required. The type of the securable to retrieve the policy for.
        :param on_securable_fullname: str
          Required. The fully qualified name of securable to retrieve policy for.
        :param name: str
          Required. The name of the policy to retrieve.

        :returns: :class:`PolicyInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.1/unity-catalog/policies/{on_securable_type}/{on_securable_fullname}/{name}",
            headers=headers,
        )
        return PolicyInfo.from_dict(res)

    def list_policies(
        self,
        on_securable_type: str,
        on_securable_fullname: str,
        *,
        include_inherited: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[PolicyInfo]:
        """List all policies defined on a securable. Optionally, the list can include inherited policies defined
        on the securable's parent schema or catalog.

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param on_securable_type: str
          Required. The type of the securable to list policies for.
        :param on_securable_fullname: str
          Required. The fully qualified name of securable to list policies for.
        :param include_inherited: bool (optional)
          Optional. Whether to include policies defined on parent securables. By default, the inherited
          policies are not included.
        :param max_results: int (optional)
          Optional. Maximum number of policies to return on a single page (page length). - When not set or set
          to 0, the page length is set to a server configured value (recommended); - When set to a value
          greater than 0, the page length is the minimum of this value and a server configured value;
        :param page_token: str (optional)
          Optional. Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`PolicyInfo`
        """

        query = {}
        if include_inherited is not None:
            query["include_inherited"] = include_inherited
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET",
                f"/api/2.1/unity-catalog/policies/{on_securable_type}/{on_securable_fullname}",
                query=query,
                headers=headers,
            )
            if "policies" in json:
                for v in json["policies"]:
                    yield PolicyInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_policy(
        self,
        on_securable_type: str,
        on_securable_fullname: str,
        name: str,
        policy_info: PolicyInfo,
        *,
        update_mask: Optional[str] = None,
    ) -> PolicyInfo:
        """Update an ABAC policy on a securable.

        :param on_securable_type: str
          Required. The type of the securable to update the policy for.
        :param on_securable_fullname: str
          Required. The fully qualified name of the securable to update the policy for.
        :param name: str
          Required. The name of the policy to update.
        :param policy_info: :class:`PolicyInfo`
          Optional fields to update. This is the request body for updating a policy. Use `update_mask` field
          to specify which fields in the request is to be updated. - If `update_mask` is empty or "*", all
          specified fields will be updated. - If `update_mask` is specified, only the fields specified in the
          `update_mask` will be updated. If a field is specified in `update_mask` and not set in the request,
          the field will be cleared. Users can use the update mask to explicitly unset optional fields such as
          `exception_principals` and `when_condition`.
        :param update_mask: str (optional)
          Optional. The update mask field for specifying user intentions on which fields to update in the
          request.

        :returns: :class:`PolicyInfo`
        """

        body = policy_info.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH",
            f"/api/2.1/unity-catalog/policies/{on_securable_type}/{on_securable_fullname}/{name}",
            query=query,
            body=body,
            headers=headers,
        )
        return PolicyInfo.from_dict(res)


class QualityMonitorsAPI:
    """Deprecated: Please use the Data Quality Monitors API instead (REST: /api/data-quality/v1/monitors), which
    manages both Data Profiling and Anomaly Detection.

    A monitor computes and monitors data or model quality metrics for a table over time. It generates metrics
    tables and a dashboard that you can use to monitor table health and set alerts. Most write operations
    require the user to be the owner of the table (or its parent schema or parent catalog). Viewing the
    dashboard, computed metrics, or monitor configuration only requires the user to have **SELECT** privileges
    on the table (along with **USE_SCHEMA** and **USE_CATALOG**)."""

    def __init__(self, api_client):
        self._api = api_client

    def cancel_refresh(self, table_name: str, refresh_id: int):
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Cancels an
        already-initiated refresh job.

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. table_name is case insensitive and spaces are
          disallowed.
        :param refresh_id: int


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "POST", f"/api/2.1/unity-catalog/tables/{table_name}/monitor/refreshes/{refresh_id}/cancel", headers=headers
        )

    def create(
        self,
        table_name: str,
        output_schema_name: str,
        assets_dir: str,
        *,
        baseline_table_name: Optional[str] = None,
        custom_metrics: Optional[List[MonitorMetric]] = None,
        data_classification_config: Optional[MonitorDataClassificationConfig] = None,
        inference_log: Optional[MonitorInferenceLog] = None,
        latest_monitor_failure_msg: Optional[str] = None,
        notifications: Optional[MonitorNotifications] = None,
        schedule: Optional[MonitorCronSchedule] = None,
        skip_builtin_dashboard: Optional[bool] = None,
        slicing_exprs: Optional[List[str]] = None,
        snapshot: Optional[MonitorSnapshot] = None,
        time_series: Optional[MonitorTimeSeries] = None,
        warehouse_id: Optional[str] = None,
    ) -> MonitorInfo:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Creates a new
        monitor for the specified table.

        The caller must either: 1. be an owner of the table's parent catalog, have **USE_SCHEMA** on the
        table's parent schema, and have **SELECT** access on the table 2. have **USE_CATALOG** on the table's
        parent catalog, be an owner of the table's parent schema, and have **SELECT** access on the table. 3.
        have the following permissions: - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on
        the table's parent schema - be an owner of the table.

        Workspace assets, such as the dashboard, will be created in the workspace where this call was made.

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. This field corresponds to the
          {full_table_name_arg} arg in the endpoint path.
        :param output_schema_name: str
          [Create:REQ Update:REQ] Schema where output tables are created. Needs to be in 2-level format
          {catalog}.{schema}
        :param assets_dir: str
          [Create:REQ Update:IGN] Field for specifying the absolute path to a custom directory to store
          data-monitoring assets. Normally prepopulated to a default user location via UI and Python APIs.
        :param baseline_table_name: str (optional)
          [Create:OPT Update:OPT] Baseline table name. Baseline data is used to compute drift from the data in
          the monitored `table_name`. The baseline table and the monitored table shall have the same schema.
        :param custom_metrics: List[:class:`MonitorMetric`] (optional)
          [Create:OPT Update:OPT] Custom metrics.
        :param data_classification_config: :class:`MonitorDataClassificationConfig` (optional)
          [Create:OPT Update:OPT] Data classification related config.
        :param inference_log: :class:`MonitorInferenceLog` (optional)
        :param latest_monitor_failure_msg: str (optional)
          [Create:ERR Update:IGN] The latest error message for a monitor failure.
        :param notifications: :class:`MonitorNotifications` (optional)
          [Create:OPT Update:OPT] Field for specifying notification settings.
        :param schedule: :class:`MonitorCronSchedule` (optional)
          [Create:OPT Update:OPT] The monitor schedule.
        :param skip_builtin_dashboard: bool (optional)
          Whether to skip creating a default dashboard summarizing data quality metrics.
        :param slicing_exprs: List[str] (optional)
          [Create:OPT Update:OPT] List of column expressions to slice data with for targeted analysis. The
          data is grouped by each expression independently, resulting in a separate slice for each predicate
          and its complements. For example `slicing_exprs=[“col_1”, “col_2 > 10”]` will generate the
          following slices: two slices for `col_2 > 10` (True and False), and one slice per unique value in
          `col1`. For high-cardinality columns, only the top 100 unique values by frequency will generate
          slices.
        :param snapshot: :class:`MonitorSnapshot` (optional)
          Configuration for monitoring snapshot tables.
        :param time_series: :class:`MonitorTimeSeries` (optional)
          Configuration for monitoring time series tables.
        :param warehouse_id: str (optional)
          Optional argument to specify the warehouse for dashboard creation. If not specified, the first
          running warehouse will be used.

        :returns: :class:`MonitorInfo`
        """

        body = {}
        if assets_dir is not None:
            body["assets_dir"] = assets_dir
        if baseline_table_name is not None:
            body["baseline_table_name"] = baseline_table_name
        if custom_metrics is not None:
            body["custom_metrics"] = [v.as_dict() for v in custom_metrics]
        if data_classification_config is not None:
            body["data_classification_config"] = data_classification_config.as_dict()
        if inference_log is not None:
            body["inference_log"] = inference_log.as_dict()
        if latest_monitor_failure_msg is not None:
            body["latest_monitor_failure_msg"] = latest_monitor_failure_msg
        if notifications is not None:
            body["notifications"] = notifications.as_dict()
        if output_schema_name is not None:
            body["output_schema_name"] = output_schema_name
        if schedule is not None:
            body["schedule"] = schedule.as_dict()
        if skip_builtin_dashboard is not None:
            body["skip_builtin_dashboard"] = skip_builtin_dashboard
        if slicing_exprs is not None:
            body["slicing_exprs"] = [v for v in slicing_exprs]
        if snapshot is not None:
            body["snapshot"] = snapshot.as_dict()
        if time_series is not None:
            body["time_series"] = time_series.as_dict()
        if warehouse_id is not None:
            body["warehouse_id"] = warehouse_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.1/unity-catalog/tables/{table_name}/monitor", body=body, headers=headers)
        return MonitorInfo.from_dict(res)

    def delete(self, table_name: str) -> DeleteMonitorResponse:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Deletes a monitor
        for the specified table.

        The caller must either: 1. be an owner of the table's parent catalog 2. have **USE_CATALOG** on the
        table's parent catalog and be an owner of the table's parent schema 3. have the following permissions:
        - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on the table's parent schema - be an
        owner of the table.

        Additionally, the call must be made from the workspace where the monitor was created.

        Note that the metric tables and dashboard will not be deleted as part of this call; those assets must
        be manually cleaned up (if desired).

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. This field corresponds to the
          {full_table_name_arg} arg in the endpoint path.

        :returns: :class:`DeleteMonitorResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.1/unity-catalog/tables/{table_name}/monitor", headers=headers)
        return DeleteMonitorResponse.from_dict(res)

    def get(self, table_name: str) -> MonitorInfo:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Gets a monitor for
        the specified table.

        The caller must either: 1. be an owner of the table's parent catalog 2. have **USE_CATALOG** on the
        table's parent catalog and be an owner of the table's parent schema. 3. have the following
        permissions: - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on the table's parent
        schema - **SELECT** privilege on the table.

        The returned information includes configuration values, as well as information on assets created by
        the monitor. Some information (e.g., dashboard) may be filtered out if the caller is in a different
        workspace than where the monitor was created.

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. This field corresponds to the
          {full_table_name_arg} arg in the endpoint path.

        :returns: :class:`MonitorInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/tables/{table_name}/monitor", headers=headers)
        return MonitorInfo.from_dict(res)

    def get_refresh(self, table_name: str, refresh_id: int) -> MonitorRefreshInfo:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Gets info about a
        specific monitor refresh using the given refresh ID.

        The caller must either: 1. be an owner of the table's parent catalog 2. have **USE_CATALOG** on the
        table's parent catalog and be an owner of the table's parent schema 3. have the following permissions:
        - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on the table's parent schema -
        **SELECT** privilege on the table.

        Additionally, the call must be made from the workspace where the monitor was created.

        :param table_name: str
          Full name of the table.
        :param refresh_id: int
          ID of the refresh.

        :returns: :class:`MonitorRefreshInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.1/unity-catalog/tables/{table_name}/monitor/refreshes/{refresh_id}", headers=headers
        )
        return MonitorRefreshInfo.from_dict(res)

    def list_refreshes(self, table_name: str) -> MonitorRefreshListResponse:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Gets an array
        containing the history of the most recent refreshes (up to 25) for this table.

        The caller must either: 1. be an owner of the table's parent catalog 2. have **USE_CATALOG** on the
        table's parent catalog and be an owner of the table's parent schema 3. have the following permissions:
        - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on the table's parent schema -
        **SELECT** privilege on the table.

        Additionally, the call must be made from the workspace where the monitor was created.

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. table_name is case insensitive and spaces are
          disallowed.

        :returns: :class:`MonitorRefreshListResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/tables/{table_name}/monitor/refreshes", headers=headers)
        return MonitorRefreshListResponse.from_dict(res)

    def regenerate_dashboard(
        self, table_name: str, *, warehouse_id: Optional[str] = None
    ) -> RegenerateDashboardResponse:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Regenerates the
        monitoring dashboard for the specified table.

        The caller must either: 1. be an owner of the table's parent catalog 2. have **USE_CATALOG** on the
        table's parent catalog and be an owner of the table's parent schema 3. have the following permissions:
        - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on the table's parent schema - be an
        owner of the table

        The call must be made from the workspace where the monitor was created. The dashboard will be
        regenerated in the assets directory that was specified when the monitor was created.

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. This field corresponds to the
          {full_table_name_arg} arg in the endpoint path.
        :param warehouse_id: str (optional)
          Optional argument to specify the warehouse for dashboard regeneration. If not specified, the first
          running warehouse will be used.

        :returns: :class:`RegenerateDashboardResponse`
        """

        body = {}
        if warehouse_id is not None:
            body["warehouse_id"] = warehouse_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", f"/api/2.1/quality-monitoring/tables/{table_name}/monitor/dashboard", body=body, headers=headers
        )
        return RegenerateDashboardResponse.from_dict(res)

    def run_refresh(self, table_name: str) -> MonitorRefreshInfo:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Queues a metric
        refresh on the monitor for the specified table. The refresh will execute in the background.

        The caller must either: 1. be an owner of the table's parent catalog 2. have **USE_CATALOG** on the
        table's parent catalog and be an owner of the table's parent schema 3. have the following permissions:
        - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on the table's parent schema - be an
        owner of the table

        Additionally, the call must be made from the workspace where the monitor was created.

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. table_name is case insensitive and spaces are
          disallowed.

        :returns: :class:`MonitorRefreshInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.1/unity-catalog/tables/{table_name}/monitor/refreshes", headers=headers)
        return MonitorRefreshInfo.from_dict(res)

    def update(
        self,
        table_name: str,
        output_schema_name: str,
        *,
        baseline_table_name: Optional[str] = None,
        custom_metrics: Optional[List[MonitorMetric]] = None,
        dashboard_id: Optional[str] = None,
        data_classification_config: Optional[MonitorDataClassificationConfig] = None,
        inference_log: Optional[MonitorInferenceLog] = None,
        latest_monitor_failure_msg: Optional[str] = None,
        notifications: Optional[MonitorNotifications] = None,
        schedule: Optional[MonitorCronSchedule] = None,
        slicing_exprs: Optional[List[str]] = None,
        snapshot: Optional[MonitorSnapshot] = None,
        time_series: Optional[MonitorTimeSeries] = None,
    ) -> MonitorInfo:
        """Deprecated: Use Data Quality Monitors API instead (/api/data-quality/v1/monitors). Updates a monitor
        for the specified table.

        The caller must either: 1. be an owner of the table's parent catalog 2. have **USE_CATALOG** on the
        table's parent catalog and be an owner of the table's parent schema 3. have the following permissions:
        - **USE_CATALOG** on the table's parent catalog - **USE_SCHEMA** on the table's parent schema - be an
        owner of the table.

        Additionally, the call must be made from the workspace where the monitor was created, and the caller
        must be the original creator of the monitor.

        Certain configuration fields, such as output asset identifiers, cannot be updated.

        :param table_name: str
          UC table name in format `catalog.schema.table_name`. This field corresponds to the
          {full_table_name_arg} arg in the endpoint path.
        :param output_schema_name: str
          [Create:REQ Update:REQ] Schema where output tables are created. Needs to be in 2-level format
          {catalog}.{schema}
        :param baseline_table_name: str (optional)
          [Create:OPT Update:OPT] Baseline table name. Baseline data is used to compute drift from the data in
          the monitored `table_name`. The baseline table and the monitored table shall have the same schema.
        :param custom_metrics: List[:class:`MonitorMetric`] (optional)
          [Create:OPT Update:OPT] Custom metrics.
        :param dashboard_id: str (optional)
          [Create:ERR Update:OPT] Id of dashboard that visualizes the computed metrics. This can be empty if
          the monitor is in PENDING state.
        :param data_classification_config: :class:`MonitorDataClassificationConfig` (optional)
          [Create:OPT Update:OPT] Data classification related config.
        :param inference_log: :class:`MonitorInferenceLog` (optional)
        :param latest_monitor_failure_msg: str (optional)
          [Create:ERR Update:IGN] The latest error message for a monitor failure.
        :param notifications: :class:`MonitorNotifications` (optional)
          [Create:OPT Update:OPT] Field for specifying notification settings.
        :param schedule: :class:`MonitorCronSchedule` (optional)
          [Create:OPT Update:OPT] The monitor schedule.
        :param slicing_exprs: List[str] (optional)
          [Create:OPT Update:OPT] List of column expressions to slice data with for targeted analysis. The
          data is grouped by each expression independently, resulting in a separate slice for each predicate
          and its complements. For example `slicing_exprs=[“col_1”, “col_2 > 10”]` will generate the
          following slices: two slices for `col_2 > 10` (True and False), and one slice per unique value in
          `col1`. For high-cardinality columns, only the top 100 unique values by frequency will generate
          slices.
        :param snapshot: :class:`MonitorSnapshot` (optional)
          Configuration for monitoring snapshot tables.
        :param time_series: :class:`MonitorTimeSeries` (optional)
          Configuration for monitoring time series tables.

        :returns: :class:`MonitorInfo`
        """

        body = {}
        if baseline_table_name is not None:
            body["baseline_table_name"] = baseline_table_name
        if custom_metrics is not None:
            body["custom_metrics"] = [v.as_dict() for v in custom_metrics]
        if dashboard_id is not None:
            body["dashboard_id"] = dashboard_id
        if data_classification_config is not None:
            body["data_classification_config"] = data_classification_config.as_dict()
        if inference_log is not None:
            body["inference_log"] = inference_log.as_dict()
        if latest_monitor_failure_msg is not None:
            body["latest_monitor_failure_msg"] = latest_monitor_failure_msg
        if notifications is not None:
            body["notifications"] = notifications.as_dict()
        if output_schema_name is not None:
            body["output_schema_name"] = output_schema_name
        if schedule is not None:
            body["schedule"] = schedule.as_dict()
        if slicing_exprs is not None:
            body["slicing_exprs"] = [v for v in slicing_exprs]
        if snapshot is not None:
            body["snapshot"] = snapshot.as_dict()
        if time_series is not None:
            body["time_series"] = time_series.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.1/unity-catalog/tables/{table_name}/monitor", body=body, headers=headers)
        return MonitorInfo.from_dict(res)


class RegisteredModelsAPI:
    """Databricks provides a hosted version of MLflow Model Registry in Unity Catalog. Models in Unity Catalog
    provide centralized access control, auditing, lineage, and discovery of ML models across Databricks
    workspaces.

    An MLflow registered model resides in the third layer of Unity Catalog’s three-level namespace.
    Registered models contain model versions, which correspond to actual ML models (MLflow models). Creating
    new model versions currently requires use of the MLflow Python client. Once model versions are created,
    you can load them for batch inference using MLflow Python client APIs, or deploy them for real-time
    serving using Databricks Model Serving.

    All operations on registered models and model versions require USE_CATALOG permissions on the enclosing
    catalog and USE_SCHEMA permissions on the enclosing schema. In addition, the following additional
    privileges are required for various operations:

    * To create a registered model, users must additionally have the CREATE_MODEL permission on the target
    schema. * To view registered model or model version metadata, model version data files, or invoke a model
    version, users must additionally have the EXECUTE permission on the registered model * To update
    registered model or model version tags, users must additionally have APPLY TAG permissions on the
    registered model * To update other registered model or model version metadata (comments, aliases) create a
    new model version, or update permissions on the registered model, users must be owners of the registered
    model.

    Note: The securable type for models is FUNCTION. When using REST APIs (e.g. tagging, grants) that specify
    a securable type, use FUNCTION as the securable type."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        *,
        aliases: Optional[List[RegisteredModelAlias]] = None,
        browse_only: Optional[bool] = None,
        catalog_name: Optional[str] = None,
        comment: Optional[str] = None,
        created_at: Optional[int] = None,
        created_by: Optional[str] = None,
        full_name: Optional[str] = None,
        metastore_id: Optional[str] = None,
        name: Optional[str] = None,
        owner: Optional[str] = None,
        schema_name: Optional[str] = None,
        storage_location: Optional[str] = None,
        updated_at: Optional[int] = None,
        updated_by: Optional[str] = None,
    ) -> RegisteredModelInfo:
        """Creates a new registered model in Unity Catalog.

        File storage for model versions in the registered model will be located in the default location which
        is specified by the parent schema, or the parent catalog, or the Metastore.

        For registered model creation to succeed, the user must satisfy the following conditions: - The caller
        must be a metastore admin, or be the owner of the parent catalog and schema, or have the
        **USE_CATALOG** privilege on the parent catalog and the **USE_SCHEMA** privilege on the parent schema.
        - The caller must have the **CREATE MODEL** or **CREATE FUNCTION** privilege on the parent schema.

        :param aliases: List[:class:`RegisteredModelAlias`] (optional)
          List of aliases associated with the registered model
        :param browse_only: bool (optional)
          Indicates whether the principal is limited to retrieving metadata for the associated object through
          the BROWSE privilege when include_browse is enabled in the request.
        :param catalog_name: str (optional)
          The name of the catalog where the schema and the registered model reside
        :param comment: str (optional)
          The comment attached to the registered model
        :param created_at: int (optional)
          Creation timestamp of the registered model in milliseconds since the Unix epoch
        :param created_by: str (optional)
          The identifier of the user who created the registered model
        :param full_name: str (optional)
          The three-level (fully qualified) name of the registered model
        :param metastore_id: str (optional)
          The unique identifier of the metastore
        :param name: str (optional)
          The name of the registered model
        :param owner: str (optional)
          The identifier of the user who owns the registered model
        :param schema_name: str (optional)
          The name of the schema where the registered model resides
        :param storage_location: str (optional)
          The storage location on the cloud under which model version data files are stored
        :param updated_at: int (optional)
          Last-update timestamp of the registered model in milliseconds since the Unix epoch
        :param updated_by: str (optional)
          The identifier of the user who updated the registered model last time

        :returns: :class:`RegisteredModelInfo`
        """

        body = {}
        if aliases is not None:
            body["aliases"] = [v.as_dict() for v in aliases]
        if browse_only is not None:
            body["browse_only"] = browse_only
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        if comment is not None:
            body["comment"] = comment
        if created_at is not None:
            body["created_at"] = created_at
        if created_by is not None:
            body["created_by"] = created_by
        if full_name is not None:
            body["full_name"] = full_name
        if metastore_id is not None:
            body["metastore_id"] = metastore_id
        if name is not None:
            body["name"] = name
        if owner is not None:
            body["owner"] = owner
        if schema_name is not None:
            body["schema_name"] = schema_name
        if storage_location is not None:
            body["storage_location"] = storage_location
        if updated_at is not None:
            body["updated_at"] = updated_at
        if updated_by is not None:
            body["updated_by"] = updated_by
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/models", body=body, headers=headers)
        return RegisteredModelInfo.from_dict(res)

    def delete(self, full_name: str):
        """Deletes a registered model and all its model versions from the specified parent catalog and schema.

        The caller must be a metastore admin or an owner of the registered model. For the latter case, the
        caller must also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the
        **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          The three-level (fully qualified) name of the registered model


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/models/{full_name}", headers=headers)

    def delete_alias(self, full_name: str, alias: str):
        """Deletes a registered model alias.

        The caller must be a metastore admin or an owner of the registered model. For the latter case, the
        caller must also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the
        **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          The three-level (fully qualified) name of the registered model
        :param alias: str
          The name of the alias


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/models/{full_name}/aliases/{alias}", headers=headers)

    def get(
        self, full_name: str, *, include_aliases: Optional[bool] = None, include_browse: Optional[bool] = None
    ) -> RegisteredModelInfo:
        """Get a registered model.

        The caller must be a metastore admin or an owner of (or have the **EXECUTE** privilege on) the
        registered model. For the latter case, the caller must also be the owner or have the **USE_CATALOG**
        privilege on the parent catalog and the **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          The three-level (fully qualified) name of the registered model
        :param include_aliases: bool (optional)
          Whether to include registered model aliases in the response
        :param include_browse: bool (optional)
          Whether to include registered models in the response for which the principal can only access
          selective metadata for

        :returns: :class:`RegisteredModelInfo`
        """

        query = {}
        if include_aliases is not None:
            query["include_aliases"] = include_aliases
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/models/{full_name}", query=query, headers=headers)
        return RegisteredModelInfo.from_dict(res)

    def list(
        self,
        *,
        catalog_name: Optional[str] = None,
        include_browse: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Iterator[RegisteredModelInfo]:
        """List registered models. You can list registered models under a particular schema, or list all
        registered models in the current metastore.

        The returned models are filtered based on the privileges of the calling user. For example, the
        metastore admin is able to list all the registered models. A regular user needs to be the owner or
        have the **EXECUTE** privilege on the registered model to recieve the registered models in the
        response. For the latter case, the caller must also be the owner or have the **USE_CATALOG** privilege
        on the parent catalog and the **USE_SCHEMA** privilege on the parent schema.

        There is no guarantee of a specific ordering of the elements in the response.

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param catalog_name: str (optional)
          The identifier of the catalog under which to list registered models. If specified, schema_name must
          be specified.
        :param include_browse: bool (optional)
          Whether to include registered models in the response for which the principal can only access
          selective metadata for
        :param max_results: int (optional)
          Max number of registered models to return.

          If both catalog and schema are specified: - when max_results is not specified, the page length is
          set to a server configured value (10000, as of 4/2/2024). - when set to a value greater than 0, the
          page length is the minimum of this value and a server configured value (10000, as of 4/2/2024); -
          when set to 0, the page length is set to a server configured value (10000, as of 4/2/2024); - when
          set to a value less than 0, an invalid parameter error is returned;

          If neither schema nor catalog is specified: - when max_results is not specified, the page length is
          set to a server configured value (100, as of 4/2/2024). - when set to a value greater than 0, the
          page length is the minimum of this value and a server configured value (1000, as of 4/2/2024); -
          when set to 0, the page length is set to a server configured value (100, as of 4/2/2024); - when set
          to a value less than 0, an invalid parameter error is returned;
        :param page_token: str (optional)
          Opaque token to send for the next page of results (pagination).
        :param schema_name: str (optional)
          The identifier of the schema under which to list registered models. If specified, catalog_name must
          be specified.

        :returns: Iterator over :class:`RegisteredModelInfo`
        """

        query = {}
        if catalog_name is not None:
            query["catalog_name"] = catalog_name
        if include_browse is not None:
            query["include_browse"] = include_browse
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if schema_name is not None:
            query["schema_name"] = schema_name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/models", query=query, headers=headers)
            if "registered_models" in json:
                for v in json["registered_models"]:
                    yield RegisteredModelInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def set_alias(self, full_name: str, alias: str, version_num: int) -> RegisteredModelAlias:
        """Set an alias on the specified registered model.

        The caller must be a metastore admin or an owner of the registered model. For the latter case, the
        caller must also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the
        **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          The three-level (fully qualified) name of the registered model
        :param alias: str
          The name of the alias
        :param version_num: int
          The version number of the model version to which the alias points

        :returns: :class:`RegisteredModelAlias`
        """

        body = {}
        if version_num is not None:
            body["version_num"] = version_num
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PUT", f"/api/2.1/unity-catalog/models/{full_name}/aliases/{alias}", body=body, headers=headers
        )
        return RegisteredModelAlias.from_dict(res)

    def update(
        self,
        full_name: str,
        *,
        aliases: Optional[List[RegisteredModelAlias]] = None,
        browse_only: Optional[bool] = None,
        catalog_name: Optional[str] = None,
        comment: Optional[str] = None,
        created_at: Optional[int] = None,
        created_by: Optional[str] = None,
        metastore_id: Optional[str] = None,
        name: Optional[str] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        schema_name: Optional[str] = None,
        storage_location: Optional[str] = None,
        updated_at: Optional[int] = None,
        updated_by: Optional[str] = None,
    ) -> RegisteredModelInfo:
        """Updates the specified registered model.

        The caller must be a metastore admin or an owner of the registered model. For the latter case, the
        caller must also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the
        **USE_SCHEMA** privilege on the parent schema.

        Currently only the name, the owner or the comment of the registered model can be updated.

        :param full_name: str
          The three-level (fully qualified) name of the registered model
        :param aliases: List[:class:`RegisteredModelAlias`] (optional)
          List of aliases associated with the registered model
        :param browse_only: bool (optional)
          Indicates whether the principal is limited to retrieving metadata for the associated object through
          the BROWSE privilege when include_browse is enabled in the request.
        :param catalog_name: str (optional)
          The name of the catalog where the schema and the registered model reside
        :param comment: str (optional)
          The comment attached to the registered model
        :param created_at: int (optional)
          Creation timestamp of the registered model in milliseconds since the Unix epoch
        :param created_by: str (optional)
          The identifier of the user who created the registered model
        :param metastore_id: str (optional)
          The unique identifier of the metastore
        :param name: str (optional)
          The name of the registered model
        :param new_name: str (optional)
          New name for the registered model.
        :param owner: str (optional)
          The identifier of the user who owns the registered model
        :param schema_name: str (optional)
          The name of the schema where the registered model resides
        :param storage_location: str (optional)
          The storage location on the cloud under which model version data files are stored
        :param updated_at: int (optional)
          Last-update timestamp of the registered model in milliseconds since the Unix epoch
        :param updated_by: str (optional)
          The identifier of the user who updated the registered model last time

        :returns: :class:`RegisteredModelInfo`
        """

        body = {}
        if aliases is not None:
            body["aliases"] = [v.as_dict() for v in aliases]
        if browse_only is not None:
            body["browse_only"] = browse_only
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        if comment is not None:
            body["comment"] = comment
        if created_at is not None:
            body["created_at"] = created_at
        if created_by is not None:
            body["created_by"] = created_by
        if metastore_id is not None:
            body["metastore_id"] = metastore_id
        if name is not None:
            body["name"] = name
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if schema_name is not None:
            body["schema_name"] = schema_name
        if storage_location is not None:
            body["storage_location"] = storage_location
        if updated_at is not None:
            body["updated_at"] = updated_at
        if updated_by is not None:
            body["updated_by"] = updated_by
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/models/{full_name}", body=body, headers=headers)
        return RegisteredModelInfo.from_dict(res)


class ResourceQuotasAPI:
    """Unity Catalog enforces resource quotas on all securable objects, which limits the number of resources that
    can be created. Quotas are expressed in terms of a resource type and a parent (for example, tables per
    metastore or schemas per catalog). The resource quota APIs enable you to monitor your current usage and
    limits. For more information on resource quotas see the [Unity Catalog documentation].

    [Unity Catalog documentation]: https://docs.databricks.com/en/data-governance/unity-catalog/index.html#resource-quotas
    """

    def __init__(self, api_client):
        self._api = api_client

    def get_quota(self, parent_securable_type: str, parent_full_name: str, quota_name: str) -> GetQuotaResponse:
        """The GetQuota API returns usage information for a single resource quota, defined as a child-parent
        pair. This API also refreshes the quota count if it is out of date. Refreshes are triggered
        asynchronously. The updated count might not be returned in the first call.

        :param parent_securable_type: str
          Securable type of the quota parent.
        :param parent_full_name: str
          Full name of the parent resource. Provide the metastore ID if the parent is a metastore.
        :param quota_name: str
          Name of the quota. Follows the pattern of the quota type, with "-quota" added as a suffix.

        :returns: :class:`GetQuotaResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.1/unity-catalog/resource-quotas/{parent_securable_type}/{parent_full_name}/{quota_name}",
            headers=headers,
        )
        return GetQuotaResponse.from_dict(res)

    def list_quotas(
        self, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[QuotaInfo]:
        """ListQuotas returns all quota values under the metastore. There are no SLAs on the freshness of the
        counts returned. This API does not trigger a refresh of quota counts.

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param max_results: int (optional)
          The number of quotas to return.
        :param page_token: str (optional)
          Opaque token for the next page of results.

        :returns: Iterator over :class:`QuotaInfo`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", "/api/2.1/unity-catalog/resource-quotas/all-resource-quotas", query=query, headers=headers
            )
            if "quotas" in json:
                for v in json["quotas"]:
                    yield QuotaInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class RfaAPI:
    """Request for Access enables users to request access for Unity Catalog securables.

    These APIs provide a standardized way for securable owners (or users with MANAGE privileges) to manage
    access request destinations."""

    def __init__(self, api_client):
        self._api = api_client

    def batch_create_access_requests(
        self, *, requests: Optional[List[CreateAccessRequest]] = None
    ) -> BatchCreateAccessRequestsResponse:
        """Creates access requests for Unity Catalog permissions for a specified principal on a securable object.
        This Batch API can take in multiple principals, securable objects, and permissions as the input and
        returns the access request destinations for each. Principals must be unique across the API call.

        The supported securable types are: "metastore", "catalog", "schema", "table", "external_location",
        "connection", "credential", "function", "registered_model", and "volume".

        :param requests: List[:class:`CreateAccessRequest`] (optional)
          A list of individual access requests, where each request corresponds to a set of permissions being
          requested on a list of securables for a specified principal.

          At most 30 requests per API call.

        :returns: :class:`BatchCreateAccessRequestsResponse`
        """

        body = {}
        if requests is not None:
            body["requests"] = [v.as_dict() for v in requests]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/3.0/rfa/requests", body=body, headers=headers)
        return BatchCreateAccessRequestsResponse.from_dict(res)

    def get_access_request_destinations(self, securable_type: str, full_name: str) -> AccessRequestDestinations:
        """Gets an array of access request destinations for the specified securable. Any caller can see URL
        destinations or the destinations on the metastore. Otherwise, only those with **BROWSE** permissions
        on the securable can see destinations.

        The supported securable types are: "metastore", "catalog", "schema", "table", "external_location",
        "connection", "credential", "function", "registered_model", and "volume".

        :param securable_type: str
          The type of the securable.
        :param full_name: str
          The full name of the securable.

        :returns: :class:`AccessRequestDestinations`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/3.0/rfa/destinations/{securable_type}/{full_name}", headers=headers)
        return AccessRequestDestinations.from_dict(res)

    def update_access_request_destinations(
        self, access_request_destinations: AccessRequestDestinations, update_mask: str
    ) -> AccessRequestDestinations:
        """Updates the access request destinations for the given securable. The caller must be a metastore admin,
        the owner of the securable, or a user that has the **MANAGE** privilege on the securable in order to
        assign destinations. Destinations cannot be updated for securables underneath schemas (tables,
        volumes, functions, and models). For these securable types, destinations are inherited from the parent
        securable. A maximum of 5 emails and 5 external notification destinations (Slack, Microsoft Teams, and
        Generic Webhook destinations) can be assigned to a securable. If a URL destination is assigned, no
        other destinations can be set.

        The supported securable types are: "metastore", "catalog", "schema", "table", "external_location",
        "connection", "credential", "function", "registered_model", and "volume".

        :param access_request_destinations: :class:`AccessRequestDestinations`
          The access request destinations to assign to the securable. For each destination, a
          **destination_id** and **destination_type** must be defined.
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`AccessRequestDestinations`
        """

        body = access_request_destinations.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", "/api/3.0/rfa/destinations", query=query, body=body, headers=headers)
        return AccessRequestDestinations.from_dict(res)


class SchemasAPI:
    """A schema (also called a database) is the second layer of Unity Catalog’s three-level namespace. A schema
    organizes tables, views and functions. To access (or list) a table or view in a schema, users must have
    the USE_SCHEMA data permission on the schema and its parent catalog, and they must have the SELECT
    permission on the table or view."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        catalog_name: str,
        *,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        storage_root: Optional[str] = None,
    ) -> SchemaInfo:
        """Creates a new schema for catalog in the Metastore. The caller must be a metastore admin, or have the
        **CREATE_SCHEMA** privilege in the parent catalog.

        :param name: str
          Name of schema, relative to parent catalog.
        :param catalog_name: str
          Name of parent catalog.
        :param comment: str (optional)
          User-provided free-form text description.
        :param properties: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.
        :param storage_root: str (optional)
          Storage root URL for managed tables within schema.

        :returns: :class:`SchemaInfo`
        """

        body = {}
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if properties is not None:
            body["properties"] = properties
        if storage_root is not None:
            body["storage_root"] = storage_root
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/schemas", body=body, headers=headers)
        return SchemaInfo.from_dict(res)

    def delete(self, full_name: str, *, force: Optional[bool] = None):
        """Deletes the specified schema from the parent catalog. The caller must be the owner of the schema or an
        owner of the parent catalog.

        :param full_name: str
          Full name of the schema.
        :param force: bool (optional)
          Force deletion even if the schema is not empty.


        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/schemas/{full_name}", query=query, headers=headers)

    def get(self, full_name: str, *, include_browse: Optional[bool] = None) -> SchemaInfo:
        """Gets the specified schema within the metastore. The caller must be a metastore admin, the owner of the
        schema, or a user that has the **USE_SCHEMA** privilege on the schema.

        :param full_name: str
          Full name of the schema.
        :param include_browse: bool (optional)
          Whether to include schemas in the response for which the principal can only access selective
          metadata for

        :returns: :class:`SchemaInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/schemas/{full_name}", query=query, headers=headers)
        return SchemaInfo.from_dict(res)

    def list(
        self,
        catalog_name: str,
        *,
        include_browse: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[SchemaInfo]:
        """Gets an array of schemas for a catalog in the metastore. If the caller is the metastore admin or the
        owner of the parent catalog, all schemas for the catalog will be retrieved. Otherwise, only schemas
        owned by the caller (or for which the caller has the **USE_SCHEMA** privilege) will be retrieved.
        There is no guarantee of a specific ordering of the elements in the array.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param catalog_name: str
          Parent catalog for schemas of interest.
        :param include_browse: bool (optional)
          Whether to include schemas in the response for which the principal can only access selective
          metadata for
        :param max_results: int (optional)
          Maximum number of schemas to return. If not set, all the schemas are returned (not recommended). -
          when set to a value greater than 0, the page length is the minimum of this value and a server
          configured value; - when set to 0, the page length is set to a server configured value
          (recommended); - when set to a value less than 0, an invalid parameter error is returned;
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`SchemaInfo`
        """

        query = {}
        if catalog_name is not None:
            query["catalog_name"] = catalog_name
        if include_browse is not None:
            query["include_browse"] = include_browse
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/schemas", query=query, headers=headers)
            if "schemas" in json:
                for v in json["schemas"]:
                    yield SchemaInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        full_name: str,
        *,
        comment: Optional[str] = None,
        enable_predictive_optimization: Optional[EnablePredictiveOptimization] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> SchemaInfo:
        """Updates a schema for a catalog. The caller must be the owner of the schema or a metastore admin. If
        the caller is a metastore admin, only the __owner__ field can be changed in the update. If the
        __name__ field must be updated, the caller must be a metastore admin or have the **CREATE_SCHEMA**
        privilege on the parent catalog.

        :param full_name: str
          Full name of the schema.
        :param comment: str (optional)
          User-provided free-form text description.
        :param enable_predictive_optimization: :class:`EnablePredictiveOptimization` (optional)
          Whether predictive optimization should be enabled for this object and objects under it.
        :param new_name: str (optional)
          New name for the schema.
        :param owner: str (optional)
          Username of current owner of schema.
        :param properties: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.

        :returns: :class:`SchemaInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if enable_predictive_optimization is not None:
            body["enable_predictive_optimization"] = enable_predictive_optimization.value
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if properties is not None:
            body["properties"] = properties
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/schemas/{full_name}", body=body, headers=headers)
        return SchemaInfo.from_dict(res)


class StorageCredentialsAPI:
    """A storage credential represents an authentication and authorization mechanism for accessing data stored on
    your cloud tenant. Each storage credential is subject to Unity Catalog access-control policies that
    control which users and groups can access the credential. If a user does not have access to a storage
    credential in Unity Catalog, the request fails and Unity Catalog does not attempt to authenticate to your
    cloud tenant on the user’s behalf.

    Databricks recommends using external locations rather than using storage credentials directly.

    To create storage credentials, you must be a Databricks account admin. The account admin who creates the
    storage credential can delegate ownership to another user or group to manage permissions on it."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        *,
        aws_iam_role: Optional[AwsIamRoleRequest] = None,
        azure_managed_identity: Optional[AzureManagedIdentityRequest] = None,
        azure_service_principal: Optional[AzureServicePrincipal] = None,
        cloudflare_api_token: Optional[CloudflareApiToken] = None,
        comment: Optional[str] = None,
        databricks_gcp_service_account: Optional[DatabricksGcpServiceAccountRequest] = None,
        read_only: Optional[bool] = None,
        skip_validation: Optional[bool] = None,
    ) -> StorageCredentialInfo:
        """Creates a new storage credential.

        The caller must be a metastore admin or have the **CREATE_STORAGE_CREDENTIAL** privilege on the
        metastore.

        :param name: str
          The credential name. The name must be unique among storage and service credentials within the
          metastore.
        :param aws_iam_role: :class:`AwsIamRoleRequest` (optional)
          The AWS IAM role configuration.
        :param azure_managed_identity: :class:`AzureManagedIdentityRequest` (optional)
          The Azure managed identity configuration.
        :param azure_service_principal: :class:`AzureServicePrincipal` (optional)
          The Azure service principal configuration.
        :param cloudflare_api_token: :class:`CloudflareApiToken` (optional)
          The Cloudflare API token configuration.
        :param comment: str (optional)
          Comment associated with the credential.
        :param databricks_gcp_service_account: :class:`DatabricksGcpServiceAccountRequest` (optional)
          The Databricks managed GCP service account configuration.
        :param read_only: bool (optional)
          Whether the credential is usable only for read operations. Only applicable when purpose is
          **STORAGE**.
        :param skip_validation: bool (optional)
          Supplying true to this argument skips validation of the created credential.

        :returns: :class:`StorageCredentialInfo`
        """

        body = {}
        if aws_iam_role is not None:
            body["aws_iam_role"] = aws_iam_role.as_dict()
        if azure_managed_identity is not None:
            body["azure_managed_identity"] = azure_managed_identity.as_dict()
        if azure_service_principal is not None:
            body["azure_service_principal"] = azure_service_principal.as_dict()
        if cloudflare_api_token is not None:
            body["cloudflare_api_token"] = cloudflare_api_token.as_dict()
        if comment is not None:
            body["comment"] = comment
        if databricks_gcp_service_account is not None:
            body["databricks_gcp_service_account"] = databricks_gcp_service_account.as_dict()
        if name is not None:
            body["name"] = name
        if read_only is not None:
            body["read_only"] = read_only
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/storage-credentials", body=body, headers=headers)
        return StorageCredentialInfo.from_dict(res)

    def delete(self, name: str, *, force: Optional[bool] = None):
        """Deletes a storage credential from the metastore. The caller must be an owner of the storage
        credential.

        :param name: str
          Name of the storage credential.
        :param force: bool (optional)
          Force an update even if there are dependent external locations or external tables (when purpose is
          **STORAGE**) or dependent services (when purpose is **SERVICE**).


        """

        query = {}
        if force is not None:
            query["force"] = force
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/storage-credentials/{name}", query=query, headers=headers)

    def get(self, name: str) -> StorageCredentialInfo:
        """Gets a storage credential from the metastore. The caller must be a metastore admin, the owner of the
        storage credential, or have some permission on the storage credential.

        :param name: str
          Name of the storage credential.

        :returns: :class:`StorageCredentialInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/storage-credentials/{name}", headers=headers)
        return StorageCredentialInfo.from_dict(res)

    def list(
        self,
        *,
        include_unbound: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[StorageCredentialInfo]:
        """Gets an array of storage credentials (as __StorageCredentialInfo__ objects). The array is limited to
        only those storage credentials the caller has permission to access. If the caller is a metastore
        admin, retrieval of credentials is unrestricted. There is no guarantee of a specific ordering of the
        elements in the array.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param include_unbound: bool (optional)
          Whether to include credentials not bound to the workspace. Effective only if the user has permission
          to update the credential–workspace binding.
        :param max_results: int (optional)
          Maximum number of storage credentials to return. If not set, all the storage credentials are
          returned (not recommended). - when set to a value greater than 0, the page length is the minimum of
          this value and a server configured value; - when set to 0, the page length is set to a server
          configured value (recommended); - when set to a value less than 0, an invalid parameter error is
          returned;
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`StorageCredentialInfo`
        """

        query = {}
        if include_unbound is not None:
            query["include_unbound"] = include_unbound
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/storage-credentials", query=query, headers=headers)
            if "storage_credentials" in json:
                for v in json["storage_credentials"]:
                    yield StorageCredentialInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        name: str,
        *,
        aws_iam_role: Optional[AwsIamRoleRequest] = None,
        azure_managed_identity: Optional[AzureManagedIdentityResponse] = None,
        azure_service_principal: Optional[AzureServicePrincipal] = None,
        cloudflare_api_token: Optional[CloudflareApiToken] = None,
        comment: Optional[str] = None,
        databricks_gcp_service_account: Optional[DatabricksGcpServiceAccountRequest] = None,
        force: Optional[bool] = None,
        isolation_mode: Optional[IsolationMode] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        read_only: Optional[bool] = None,
        skip_validation: Optional[bool] = None,
    ) -> StorageCredentialInfo:
        """Updates a storage credential on the metastore.

        The caller must be the owner of the storage credential or a metastore admin. If the caller is a
        metastore admin, only the **owner** field can be changed.

        :param name: str
          Name of the storage credential.
        :param aws_iam_role: :class:`AwsIamRoleRequest` (optional)
          The AWS IAM role configuration.
        :param azure_managed_identity: :class:`AzureManagedIdentityResponse` (optional)
          The Azure managed identity configuration.
        :param azure_service_principal: :class:`AzureServicePrincipal` (optional)
          The Azure service principal configuration.
        :param cloudflare_api_token: :class:`CloudflareApiToken` (optional)
          The Cloudflare API token configuration.
        :param comment: str (optional)
          Comment associated with the credential.
        :param databricks_gcp_service_account: :class:`DatabricksGcpServiceAccountRequest` (optional)
          The Databricks managed GCP service account configuration.
        :param force: bool (optional)
          Force update even if there are dependent external locations or external tables.
        :param isolation_mode: :class:`IsolationMode` (optional)
          Whether the current securable is accessible from all workspaces or a specific set of workspaces.
        :param new_name: str (optional)
          New name for the storage credential.
        :param owner: str (optional)
          Username of current owner of credential.
        :param read_only: bool (optional)
          Whether the credential is usable only for read operations. Only applicable when purpose is
          **STORAGE**.
        :param skip_validation: bool (optional)
          Supplying true to this argument skips validation of the updated credential.

        :returns: :class:`StorageCredentialInfo`
        """

        body = {}
        if aws_iam_role is not None:
            body["aws_iam_role"] = aws_iam_role.as_dict()
        if azure_managed_identity is not None:
            body["azure_managed_identity"] = azure_managed_identity.as_dict()
        if azure_service_principal is not None:
            body["azure_service_principal"] = azure_service_principal.as_dict()
        if cloudflare_api_token is not None:
            body["cloudflare_api_token"] = cloudflare_api_token.as_dict()
        if comment is not None:
            body["comment"] = comment
        if databricks_gcp_service_account is not None:
            body["databricks_gcp_service_account"] = databricks_gcp_service_account.as_dict()
        if force is not None:
            body["force"] = force
        if isolation_mode is not None:
            body["isolation_mode"] = isolation_mode.value
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if read_only is not None:
            body["read_only"] = read_only
        if skip_validation is not None:
            body["skip_validation"] = skip_validation
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/storage-credentials/{name}", body=body, headers=headers)
        return StorageCredentialInfo.from_dict(res)

    def validate(
        self,
        *,
        aws_iam_role: Optional[AwsIamRoleRequest] = None,
        azure_managed_identity: Optional[AzureManagedIdentityRequest] = None,
        azure_service_principal: Optional[AzureServicePrincipal] = None,
        cloudflare_api_token: Optional[CloudflareApiToken] = None,
        databricks_gcp_service_account: Optional[DatabricksGcpServiceAccountRequest] = None,
        external_location_name: Optional[str] = None,
        read_only: Optional[bool] = None,
        storage_credential_name: Optional[str] = None,
        url: Optional[str] = None,
    ) -> ValidateStorageCredentialResponse:
        """Validates a storage credential. At least one of __external_location_name__ and __url__ need to be
        provided. If only one of them is provided, it will be used for validation. And if both are provided,
        the __url__ will be used for validation, and __external_location_name__ will be ignored when checking
        overlapping urls.

        Either the __storage_credential_name__ or the cloud-specific credential must be provided.

        The caller must be a metastore admin or the storage credential owner or have the
        **CREATE_EXTERNAL_LOCATION** privilege on the metastore and the storage credential.

        :param aws_iam_role: :class:`AwsIamRoleRequest` (optional)
          The AWS IAM role configuration.
        :param azure_managed_identity: :class:`AzureManagedIdentityRequest` (optional)
          The Azure managed identity configuration.
        :param azure_service_principal: :class:`AzureServicePrincipal` (optional)
          The Azure service principal configuration.
        :param cloudflare_api_token: :class:`CloudflareApiToken` (optional)
          The Cloudflare API token configuration.
        :param databricks_gcp_service_account: :class:`DatabricksGcpServiceAccountRequest` (optional)
          The Databricks created GCP service account configuration.
        :param external_location_name: str (optional)
          The name of an existing external location to validate.
        :param read_only: bool (optional)
          Whether the storage credential is only usable for read operations.
        :param storage_credential_name: str (optional)
          Required. The name of an existing credential or long-lived cloud credential to validate.
        :param url: str (optional)
          The external location url to validate.

        :returns: :class:`ValidateStorageCredentialResponse`
        """

        body = {}
        if aws_iam_role is not None:
            body["aws_iam_role"] = aws_iam_role.as_dict()
        if azure_managed_identity is not None:
            body["azure_managed_identity"] = azure_managed_identity.as_dict()
        if azure_service_principal is not None:
            body["azure_service_principal"] = azure_service_principal.as_dict()
        if cloudflare_api_token is not None:
            body["cloudflare_api_token"] = cloudflare_api_token.as_dict()
        if databricks_gcp_service_account is not None:
            body["databricks_gcp_service_account"] = databricks_gcp_service_account.as_dict()
        if external_location_name is not None:
            body["external_location_name"] = external_location_name
        if read_only is not None:
            body["read_only"] = read_only
        if storage_credential_name is not None:
            body["storage_credential_name"] = storage_credential_name
        if url is not None:
            body["url"] = url
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/validate-storage-credentials", body=body, headers=headers)
        return ValidateStorageCredentialResponse.from_dict(res)


class SystemSchemasAPI:
    """A system schema is a schema that lives within the system catalog. A system schema may contain information
    about customer usage of Unity Catalog such as audit-logs, billing-logs, lineage information, etc."""

    def __init__(self, api_client):
        self._api = api_client

    def disable(self, metastore_id: str, schema_name: str):
        """Disables the system schema and removes it from the system catalog. The caller must be an account admin
        or a metastore admin.

        :param metastore_id: str
          The metastore ID under which the system schema lives.
        :param schema_name: str
          Full name of the system schema.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}", headers=headers
        )

    def enable(self, metastore_id: str, schema_name: str, *, catalog_name: Optional[str] = None):
        """Enables the system schema and adds it to the system catalog. The caller must be an account admin or a
        metastore admin.

        :param metastore_id: str
          The metastore ID under which the system schema lives.
        :param schema_name: str
          Full name of the system schema.
        :param catalog_name: str (optional)
          the catalog for which the system schema is to enabled in


        """

        body = {}
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "PUT",
            f"/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}",
            body=body,
            headers=headers,
        )

    def list(
        self, metastore_id: str, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[SystemSchemaInfo]:
        """Gets an array of system schemas for a metastore. The caller must be an account admin or a metastore
        admin.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param metastore_id: str
          The ID for the metastore in which the system schema resides.
        :param max_results: int (optional)
          Maximum number of schemas to return. - When set to 0, the page length is set to a server configured
          value (recommended); - When set to a value greater than 0, the page length is the minimum of this
          value and a server configured value; - When set to a value less than 0, an invalid parameter error
          is returned; - If not set, all the schemas are returned (not recommended).
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`SystemSchemaInfo`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do(
                "GET", f"/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas", query=query, headers=headers
            )
            if "schemas" in json:
                for v in json["schemas"]:
                    yield SystemSchemaInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class TableConstraintsAPI:
    """Primary key and foreign key constraints encode relationships between fields in tables.

    Primary and foreign keys are informational only and are not enforced. Foreign keys must reference a
    primary key in another table. This primary key is the parent constraint of the foreign key and the table
    this primary key is on is the parent table of the foreign key. Similarly, the foreign key is the child
    constraint of its referenced primary key; the table of the foreign key is the child table of the primary
    key.

    You can declare primary keys and foreign keys as part of the table specification during table creation.
    You can also add or drop constraints on existing tables."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, full_name_arg: str, constraint: TableConstraint) -> TableConstraint:
        """Creates a new table constraint.

        For the table constraint creation to succeed, the user must satisfy both of these conditions: - the
        user must have the **USE_CATALOG** privilege on the table's parent catalog, the **USE_SCHEMA**
        privilege on the table's parent schema, and be the owner of the table. - if the new constraint is a
        __ForeignKeyConstraint__, the user must have the **USE_CATALOG** privilege on the referenced parent
        table's catalog, the **USE_SCHEMA** privilege on the referenced parent table's schema, and be the
        owner of the referenced parent table.

        :param full_name_arg: str
          The full name of the table referenced by the constraint.
        :param constraint: :class:`TableConstraint`

        :returns: :class:`TableConstraint`
        """

        body = {}
        if constraint is not None:
            body["constraint"] = constraint.as_dict()
        if full_name_arg is not None:
            body["full_name_arg"] = full_name_arg
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/constraints", body=body, headers=headers)
        return TableConstraint.from_dict(res)

    def delete(self, full_name: str, constraint_name: str, cascade: bool):
        """Deletes a table constraint.

        For the table constraint deletion to succeed, the user must satisfy both of these conditions: - the
        user must have the **USE_CATALOG** privilege on the table's parent catalog, the **USE_SCHEMA**
        privilege on the table's parent schema, and be the owner of the table. - if __cascade__ argument is
        **true**, the user must have the following permissions on all of the child tables: the **USE_CATALOG**
        privilege on the table's catalog, the **USE_SCHEMA** privilege on the table's schema, and be the owner
        of the table.

        :param full_name: str
          Full name of the table referenced by the constraint.
        :param constraint_name: str
          The name of the constraint to delete.
        :param cascade: bool
          If true, try deleting all child constraints of the current constraint. If false, reject this
          operation if the current constraint has any child constraints.


        """

        query = {}
        if cascade is not None:
            query["cascade"] = cascade
        if constraint_name is not None:
            query["constraint_name"] = constraint_name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/constraints/{full_name}", query=query, headers=headers)


class TablesAPI:
    """A table resides in the third layer of Unity Catalog’s three-level namespace. It contains rows of data.
    To create a table, users must have CREATE_TABLE and USE_SCHEMA permissions on the schema, and they must
    have the USE_CATALOG permission on its parent catalog. To query a table, users must have the SELECT
    permission on the table, and they must have the USE_CATALOG permission on its parent catalog and the
    USE_SCHEMA permission on its parent schema.

    A table can be managed or external. From an API perspective, a __VIEW__ is a particular kind of table
    (rather than a managed or external table)."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        catalog_name: str,
        schema_name: str,
        table_type: TableType,
        data_source_format: DataSourceFormat,
        storage_location: str,
        *,
        columns: Optional[List[ColumnInfo]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> TableInfo:
        """Creates a new table in the specified catalog and schema.

        To create an external delta table, the caller must have the **EXTERNAL_USE_SCHEMA** privilege on the
        parent schema and the **EXTERNAL_USE_LOCATION** privilege on the external location. These privileges
        must always be granted explicitly, and cannot be inherited through ownership or **ALL_PRIVILEGES**.

        Standard UC permissions needed to create tables still apply: **USE_CATALOG** on the parent catalog (or
        ownership of the parent catalog), **CREATE_TABLE** and **USE_SCHEMA** on the parent schema (or
        ownership of the parent schema), and **CREATE_EXTERNAL_TABLE** on external location.

        The **columns** field needs to be in a Spark compatible format, so we recommend you use Spark to
        create these tables. The API itself does not validate the correctness of the column spec. If the spec
        is not Spark compatible, the tables may not be readable by Databricks Runtime.

        NOTE: The Create Table API for external clients only supports creating **external delta tables**. The
        values shown in the respective enums are all values supported by Databricks, however for this specific
        Create Table API, only **table_type** **EXTERNAL** and **data_source_format** **DELTA** are supported.
        Additionally, column masks are not supported when creating tables through this API.

        :param name: str
          Name of table, relative to parent schema.
        :param catalog_name: str
          Name of parent catalog.
        :param schema_name: str
          Name of parent schema relative to its parent catalog.
        :param table_type: :class:`TableType`
        :param data_source_format: :class:`DataSourceFormat`
        :param storage_location: str
          Storage root URL for table (for **MANAGED**, **EXTERNAL** tables).
        :param columns: List[:class:`ColumnInfo`] (optional)
          The array of __ColumnInfo__ definitions of the table's columns.
        :param properties: Dict[str,str] (optional)
          A map of key-value properties attached to the securable.

        :returns: :class:`TableInfo`
        """

        body = {}
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        if columns is not None:
            body["columns"] = [v.as_dict() for v in columns]
        if data_source_format is not None:
            body["data_source_format"] = data_source_format.value
        if name is not None:
            body["name"] = name
        if properties is not None:
            body["properties"] = properties
        if schema_name is not None:
            body["schema_name"] = schema_name
        if storage_location is not None:
            body["storage_location"] = storage_location
        if table_type is not None:
            body["table_type"] = table_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/tables", body=body, headers=headers)
        return TableInfo.from_dict(res)

    def delete(self, full_name: str):
        """Deletes a table from the specified parent catalog and schema. The caller must be the owner of the
        parent catalog, have the **USE_CATALOG** privilege on the parent catalog and be the owner of the
        parent schema, or be the owner of the table and have the **USE_CATALOG** privilege on the parent
        catalog and the **USE_SCHEMA** privilege on the parent schema.

        :param full_name: str
          Full name of the table.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/tables/{full_name}", headers=headers)

    def exists(self, full_name: str) -> TableExistsResponse:
        """Gets if a table exists in the metastore for a specific catalog and schema. The caller must satisfy one
        of the following requirements: * Be a metastore admin * Be the owner of the parent catalog * Be the
        owner of the parent schema and have the **USE_CATALOG** privilege on the parent catalog * Have the
        **USE_CATALOG** privilege on the parent catalog and the **USE_SCHEMA** privilege on the parent schema,
        and either be the table owner or have the **SELECT** privilege on the table. * Have **BROWSE**
        privilege on the parent catalog * Have **BROWSE** privilege on the parent schema

        :param full_name: str
          Full name of the table.

        :returns: :class:`TableExistsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/tables/{full_name}/exists", headers=headers)
        return TableExistsResponse.from_dict(res)

    def get(
        self,
        full_name: str,
        *,
        include_browse: Optional[bool] = None,
        include_delta_metadata: Optional[bool] = None,
        include_manifest_capabilities: Optional[bool] = None,
    ) -> TableInfo:
        """Gets a table from the metastore for a specific catalog and schema. The caller must satisfy one of the
        following requirements: * Be a metastore admin * Be the owner of the parent catalog * Be the owner of
        the parent schema and have the **USE_CATALOG** privilege on the parent catalog * Have the
        **USE_CATALOG** privilege on the parent catalog and the **USE_SCHEMA** privilege on the parent schema,
        and either be the table owner or have the **SELECT** privilege on the table.

        :param full_name: str
          Full name of the table.
        :param include_browse: bool (optional)
          Whether to include tables in the response for which the principal can only access selective metadata
          for.
        :param include_delta_metadata: bool (optional)
          Whether delta metadata should be included in the response.
        :param include_manifest_capabilities: bool (optional)
          Whether to include a manifest containing table capabilities in the response.

        :returns: :class:`TableInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        if include_delta_metadata is not None:
            query["include_delta_metadata"] = include_delta_metadata
        if include_manifest_capabilities is not None:
            query["include_manifest_capabilities"] = include_manifest_capabilities
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/tables/{full_name}", query=query, headers=headers)
        return TableInfo.from_dict(res)

    def list(
        self,
        catalog_name: str,
        schema_name: str,
        *,
        include_browse: Optional[bool] = None,
        include_manifest_capabilities: Optional[bool] = None,
        max_results: Optional[int] = None,
        omit_columns: Optional[bool] = None,
        omit_properties: Optional[bool] = None,
        omit_username: Optional[bool] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[TableInfo]:
        """Gets an array of all tables for the current metastore under the parent catalog and schema. The caller
        must be a metastore admin or an owner of (or have the **SELECT** privilege on) the table. For the
        latter case, the caller must also be the owner or have the **USE_CATALOG** privilege on the parent
        catalog and the **USE_SCHEMA** privilege on the parent schema. There is no guarantee of a specific
        ordering of the elements in the array.

        NOTE: **view_dependencies** and **table_constraints** are not returned by ListTables queries.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param catalog_name: str
          Name of parent catalog for tables of interest.
        :param schema_name: str
          Parent schema of tables.
        :param include_browse: bool (optional)
          Whether to include tables in the response for which the principal can only access selective metadata
          for.
        :param include_manifest_capabilities: bool (optional)
          Whether to include a manifest containing table capabilities in the response.
        :param max_results: int (optional)
          Maximum number of tables to return. If not set, all the tables are returned (not recommended). -
          when set to a value greater than 0, the page length is the minimum of this value and a server
          configured value; - when set to 0, the page length is set to a server configured value
          (recommended); - when set to a value less than 0, an invalid parameter error is returned;
        :param omit_columns: bool (optional)
          Whether to omit the columns of the table from the response or not.
        :param omit_properties: bool (optional)
          Whether to omit the properties of the table from the response or not.
        :param omit_username: bool (optional)
          Whether to omit the username of the table (e.g. owner, updated_by, created_by) from the response or
          not.
        :param page_token: str (optional)
          Opaque token to send for the next page of results (pagination).

        :returns: Iterator over :class:`TableInfo`
        """

        query = {}
        if catalog_name is not None:
            query["catalog_name"] = catalog_name
        if include_browse is not None:
            query["include_browse"] = include_browse
        if include_manifest_capabilities is not None:
            query["include_manifest_capabilities"] = include_manifest_capabilities
        if max_results is not None:
            query["max_results"] = max_results
        if omit_columns is not None:
            query["omit_columns"] = omit_columns
        if omit_properties is not None:
            query["omit_properties"] = omit_properties
        if omit_username is not None:
            query["omit_username"] = omit_username
        if page_token is not None:
            query["page_token"] = page_token
        if schema_name is not None:
            query["schema_name"] = schema_name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/tables", query=query, headers=headers)
            if "tables" in json:
                for v in json["tables"]:
                    yield TableInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_summaries(
        self,
        catalog_name: str,
        *,
        include_manifest_capabilities: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        schema_name_pattern: Optional[str] = None,
        table_name_pattern: Optional[str] = None,
    ) -> Iterator[TableSummary]:
        """Gets an array of summaries for tables for a schema and catalog within the metastore. The table
        summaries returned are either:

        * summaries for tables (within the current metastore and parent catalog and schema), when the user is
        a metastore admin, or: * summaries for tables and schemas (within the current metastore and parent
        catalog) for which the user has ownership or the **SELECT** privilege on the table and ownership or
        **USE_SCHEMA** privilege on the schema, provided that the user also has ownership or the
        **USE_CATALOG** privilege on the parent catalog.

        There is no guarantee of a specific ordering of the elements in the array.

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param catalog_name: str
          Name of parent catalog for tables of interest.
        :param include_manifest_capabilities: bool (optional)
          Whether to include a manifest containing table capabilities in the response.
        :param max_results: int (optional)
          Maximum number of summaries for tables to return. If not set, the page length is set to a server
          configured value (10000, as of 1/5/2024). - when set to a value greater than 0, the page length is
          the minimum of this value and a server configured value (10000, as of 1/5/2024); - when set to 0,
          the page length is set to a server configured value (10000, as of 1/5/2024) (recommended); - when
          set to a value less than 0, an invalid parameter error is returned;
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.
        :param schema_name_pattern: str (optional)
          A sql LIKE pattern (% and _) for schema names. All schemas will be returned if not set or empty.
        :param table_name_pattern: str (optional)
          A sql LIKE pattern (% and _) for table names. All tables will be returned if not set or empty.

        :returns: Iterator over :class:`TableSummary`
        """

        query = {}
        if catalog_name is not None:
            query["catalog_name"] = catalog_name
        if include_manifest_capabilities is not None:
            query["include_manifest_capabilities"] = include_manifest_capabilities
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if schema_name_pattern is not None:
            query["schema_name_pattern"] = schema_name_pattern
        if table_name_pattern is not None:
            query["table_name_pattern"] = table_name_pattern
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/table-summaries", query=query, headers=headers)
            if "tables" in json:
                for v in json["tables"]:
                    yield TableSummary.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(self, full_name: str, *, owner: Optional[str] = None):
        """Change the owner of the table. The caller must be the owner of the parent catalog, have the
        **USE_CATALOG** privilege on the parent catalog and be the owner of the parent schema, or be the owner
        of the table and have the **USE_CATALOG** privilege on the parent catalog and the **USE_SCHEMA**
        privilege on the parent schema.

        :param full_name: str
          Full name of the table.
        :param owner: str (optional)
          Username of current owner of table.


        """

        body = {}
        if owner is not None:
            body["owner"] = owner
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PATCH", f"/api/2.1/unity-catalog/tables/{full_name}", body=body, headers=headers)


class TemporaryPathCredentialsAPI:
    """Temporary Path Credentials refer to short-lived, downscoped credentials used to access external cloud
    storage locations registered in Databricks. These credentials are employed to provide secure and
    time-limited access to data in cloud environments such as AWS, Azure, and Google Cloud. Each cloud
    provider has its own type of credentials: AWS uses temporary session tokens via AWS Security Token Service
    (STS), Azure utilizes Shared Access Signatures (SAS) for its data storage services, and Google Cloud
    supports temporary credentials through OAuth 2.0.

    Temporary path credentials ensure that data access is limited in scope and duration, reducing the risk of
    unauthorized access or misuse. To use the temporary path credentials API, a metastore admin needs to
    enable the external_access_enabled flag (off by default) at the metastore level. A user needs to be
    granted the EXTERNAL USE LOCATION permission by external location owner. For requests on existing external
    tables, user also needs to be granted the EXTERNAL USE SCHEMA permission at the schema level by catalog
    admin.

    Note that EXTERNAL USE SCHEMA is a schema level permission that can only be granted by catalog admin
    explicitly and is not included in schema ownership or ALL PRIVILEGES on the schema for security reasons.
    Similarly, EXTERNAL USE LOCATION is an external location level permission that can only be granted by
    external location owner explicitly and is not included in external location ownership or ALL PRIVILEGES on
    the external location for security reasons.

    This API only supports temporary path credentials for external locations and external tables, and volumes
    will be supported in the future."""

    def __init__(self, api_client):
        self._api = api_client

    def generate_temporary_path_credentials(
        self, url: str, operation: PathOperation, *, dry_run: Optional[bool] = None
    ) -> GenerateTemporaryPathCredentialResponse:
        """Get a short-lived credential for directly accessing cloud storage locations registered in Databricks.
        The Generate Temporary Path Credentials API is only supported for external storage paths, specifically
        external locations and external tables. Managed tables are not supported by this API. The metastore
        must have **external_access_enabled** flag set to true (default false). The caller must have the
        **EXTERNAL_USE_LOCATION** privilege on the external location; this privilege can only be granted by
        external location owners. For requests on existing external tables, the caller must also have the
        **EXTERNAL_USE_SCHEMA** privilege on the parent schema; this privilege can only be granted by catalog
        owners.

        :param url: str
          URL for path-based access.
        :param operation: :class:`PathOperation`
          The operation being performed on the path.
        :param dry_run: bool (optional)
          Optional. When set to true, the service will not validate that the generated credentials can perform
          write operations, therefore no new paths will be created and the response will not contain valid
          credentials. Defaults to false.

        :returns: :class:`GenerateTemporaryPathCredentialResponse`
        """

        body = {}
        if dry_run is not None:
            body["dry_run"] = dry_run
        if operation is not None:
            body["operation"] = operation.value
        if url is not None:
            body["url"] = url
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/unity-catalog/temporary-path-credentials", body=body, headers=headers)
        return GenerateTemporaryPathCredentialResponse.from_dict(res)


class TemporaryTableCredentialsAPI:
    """Temporary Table Credentials refer to short-lived, downscoped credentials used to access cloud storage
    locations where table data is stored in Databricks. These credentials are employed to provide secure and
    time-limited access to data in cloud environments such as AWS, Azure, and Google Cloud. Each cloud
    provider has its own type of credentials: AWS uses temporary session tokens via AWS Security Token Service
    (STS), Azure utilizes Shared Access Signatures (SAS) for its data storage services, and Google Cloud
    supports temporary credentials through OAuth 2.0.

    Temporary table credentials ensure that data access is limited in scope and duration, reducing the risk of
    unauthorized access or misuse. To use the temporary table credentials API, a metastore admin needs to
    enable the external_access_enabled flag (off by default) at the metastore level, and user needs to be
    granted the EXTERNAL USE SCHEMA permission at the schema level by catalog admin. Note that EXTERNAL USE
    SCHEMA is a schema level permission that can only be granted by catalog admin explicitly and is not
    included in schema ownership or ALL PRIVILEGES on the schema for security reasons."""

    def __init__(self, api_client):
        self._api = api_client

    def generate_temporary_table_credentials(
        self, *, operation: Optional[TableOperation] = None, table_id: Optional[str] = None
    ) -> GenerateTemporaryTableCredentialResponse:
        """Get a short-lived credential for directly accessing the table data on cloud storage. The metastore
        must have **external_access_enabled** flag set to true (default false). The caller must have the
        **EXTERNAL_USE_SCHEMA** privilege on the parent schema and this privilege can only be granted by
        catalog owners.

        :param operation: :class:`TableOperation` (optional)
          The operation performed against the table data, either READ or READ_WRITE. If READ_WRITE is
          specified, the credentials returned will have write permissions, otherwise, it will be read only.
        :param table_id: str (optional)
          UUID of the table to read or write.

        :returns: :class:`GenerateTemporaryTableCredentialResponse`
        """

        body = {}
        if operation is not None:
            body["operation"] = operation.value
        if table_id is not None:
            body["table_id"] = table_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/unity-catalog/temporary-table-credentials", body=body, headers=headers)
        return GenerateTemporaryTableCredentialResponse.from_dict(res)


class VolumesAPI:
    """Volumes are a Unity Catalog (UC) capability for accessing, storing, governing, organizing and processing
    files. Use cases include running machine learning on unstructured data such as image, audio, video, or PDF
    files, organizing data sets during the data exploration stages in data science, working with libraries
    that require access to the local file system on cluster machines, storing library and config files of
    arbitrary formats such as .whl or .txt centrally and providing secure access across workspaces to it, or
    transforming and querying non-tabular data files in ETL."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        volume_type: VolumeType,
        *,
        comment: Optional[str] = None,
        storage_location: Optional[str] = None,
    ) -> VolumeInfo:
        """Creates a new volume.

        The user could create either an external volume or a managed volume. An external volume will be
        created in the specified external location, while a managed volume will be located in the default
        location which is specified by the parent schema, or the parent catalog, or the Metastore.

        For the volume creation to succeed, the user must satisfy following conditions: - The caller must be a
        metastore admin, or be the owner of the parent catalog and schema, or have the **USE_CATALOG**
        privilege on the parent catalog and the **USE_SCHEMA** privilege on the parent schema. - The caller
        must have **CREATE VOLUME** privilege on the parent schema.

        For an external volume, following conditions also need to satisfy - The caller must have **CREATE
        EXTERNAL VOLUME** privilege on the external location. - There are no other tables, nor volumes
        existing in the specified storage location. - The specified storage location is not under the location
        of other tables, nor volumes, or catalogs or schemas.

        :param catalog_name: str
          The name of the catalog where the schema and the volume are
        :param schema_name: str
          The name of the schema where the volume is
        :param name: str
          The name of the volume
        :param volume_type: :class:`VolumeType`
          The type of the volume. An external volume is located in the specified external location. A managed
          volume is located in the default location which is specified by the parent schema, or the parent
          catalog, or the Metastore. [Learn more]

          [Learn more]: https://docs.databricks.com/aws/en/volumes/managed-vs-external
        :param comment: str (optional)
          The comment attached to the volume
        :param storage_location: str (optional)
          The storage location on the cloud

        :returns: :class:`VolumeInfo`
        """

        body = {}
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if schema_name is not None:
            body["schema_name"] = schema_name
        if storage_location is not None:
            body["storage_location"] = storage_location
        if volume_type is not None:
            body["volume_type"] = volume_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/volumes", body=body, headers=headers)
        return VolumeInfo.from_dict(res)

    def delete(self, name: str):
        """Deletes a volume from the specified parent catalog and schema.

        The caller must be a metastore admin or an owner of the volume. For the latter case, the caller must
        also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the **USE_SCHEMA**
        privilege on the parent schema.

        :param name: str
          The three-level (fully qualified) name of the volume


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/volumes/{name}", headers=headers)

    def list(
        self,
        catalog_name: str,
        schema_name: str,
        *,
        include_browse: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[VolumeInfo]:
        """Gets an array of volumes for the current metastore under the parent catalog and schema.

        The returned volumes are filtered based on the privileges of the calling user. For example, the
        metastore admin is able to list all the volumes. A regular user needs to be the owner or have the
        **READ VOLUME** privilege on the volume to receive the volumes in the response. For the latter case,
        the caller must also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the
        **USE_SCHEMA** privilege on the parent schema.

        There is no guarantee of a specific ordering of the elements in the array.

        PAGINATION BEHAVIOR: The API is by default paginated, a page may contain zero results while still
        providing a next_page_token. Clients must continue reading pages until next_page_token is absent,
        which is the only indication that the end of results has been reached.

        :param catalog_name: str
          The identifier of the catalog
        :param schema_name: str
          The identifier of the schema
        :param include_browse: bool (optional)
          Whether to include volumes in the response for which the principal can only access selective
          metadata for
        :param max_results: int (optional)
          Maximum number of volumes to return (page length).

          If not set, the page length is set to a server configured value (10000, as of 1/29/2024). - when set
          to a value greater than 0, the page length is the minimum of this value and a server configured
          value (10000, as of 1/29/2024); - when set to 0, the page length is set to a server configured value
          (10000, as of 1/29/2024) (recommended); - when set to a value less than 0, an invalid parameter
          error is returned;

          Note: this parameter controls only the maximum number of volumes to return. The actual number of
          volumes returned in a page may be smaller than this value, including 0, even if there are more
          pages.
        :param page_token: str (optional)
          Opaque token returned by a previous request. It must be included in the request to retrieve the next
          page of results (pagination).

        :returns: Iterator over :class:`VolumeInfo`
        """

        query = {}
        if catalog_name is not None:
            query["catalog_name"] = catalog_name
        if include_browse is not None:
            query["include_browse"] = include_browse
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if schema_name is not None:
            query["schema_name"] = schema_name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/volumes", query=query, headers=headers)
            if "volumes" in json:
                for v in json["volumes"]:
                    yield VolumeInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def read(self, name: str, *, include_browse: Optional[bool] = None) -> VolumeInfo:
        """Gets a volume from the metastore for a specific catalog and schema.

        The caller must be a metastore admin or an owner of (or have the **READ VOLUME** privilege on) the
        volume. For the latter case, the caller must also be the owner or have the **USE_CATALOG** privilege
        on the parent catalog and the **USE_SCHEMA** privilege on the parent schema.

        :param name: str
          The three-level (fully qualified) name of the volume
        :param include_browse: bool (optional)
          Whether to include volumes in the response for which the principal can only access selective
          metadata for

        :returns: :class:`VolumeInfo`
        """

        query = {}
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/volumes/{name}", query=query, headers=headers)
        return VolumeInfo.from_dict(res)

    def update(
        self, name: str, *, comment: Optional[str] = None, new_name: Optional[str] = None, owner: Optional[str] = None
    ) -> VolumeInfo:
        """Updates the specified volume under the specified parent catalog and schema.

        The caller must be a metastore admin or an owner of the volume. For the latter case, the caller must
        also be the owner or have the **USE_CATALOG** privilege on the parent catalog and the **USE_SCHEMA**
        privilege on the parent schema.

        Currently only the name, the owner or the comment of the volume could be updated.

        :param name: str
          The three-level (fully qualified) name of the volume
        :param comment: str (optional)
          The comment attached to the volume
        :param new_name: str (optional)
          New name for the volume.
        :param owner: str (optional)
          The identifier of the user who owns the volume

        :returns: :class:`VolumeInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/volumes/{name}", body=body, headers=headers)
        return VolumeInfo.from_dict(res)


class WorkspaceBindingsAPI:
    """A securable in Databricks can be configured as __OPEN__ or __ISOLATED__. An __OPEN__ securable can be
    accessed from any workspace, while an __ISOLATED__ securable can only be accessed from a configured list
    of workspaces. This API allows you to configure (bind) securables to workspaces.

    NOTE: The __isolation_mode__ is configured for the securable itself (using its Update method) and the
    workspace bindings are only consulted when the securable's __isolation_mode__ is set to __ISOLATED__.

    A securable's workspace bindings can be configured by a metastore admin or the owner of the securable.

    The original path (/api/2.1/unity-catalog/workspace-bindings/catalogs/{name}) is deprecated. Please use
    the new path (/api/2.1/unity-catalog/bindings/{securable_type}/{securable_name}) which introduces the
    ability to bind a securable in READ_ONLY mode (catalogs only).

    Securable types that support binding: - catalog - storage_credential - credential - external_location"""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, name: str) -> GetCatalogWorkspaceBindingsResponse:
        """Gets workspace bindings of the catalog. The caller must be a metastore admin or an owner of the
        catalog.

        :param name: str
          The name of the catalog.

        :returns: :class:`GetCatalogWorkspaceBindingsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/workspace-bindings/catalogs/{name}", headers=headers)
        return GetCatalogWorkspaceBindingsResponse.from_dict(res)

    def get_bindings(
        self,
        securable_type: str,
        securable_name: str,
        *,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[WorkspaceBinding]:
        """Gets workspace bindings of the securable. The caller must be a metastore admin or an owner of the
        securable.

        NOTE: we recommend using max_results=0 to use the paginated version of this API. Unpaginated calls
        will be deprecated soon.

        PAGINATION BEHAVIOR: When using pagination (max_results >= 0), a page may contain zero results while
        still providing a next_page_token. Clients must continue reading pages until next_page_token is
        absent, which is the only indication that the end of results has been reached.

        :param securable_type: str
          The type of the securable to bind to a workspace (catalog, storage_credential, credential, or
          external_location).
        :param securable_name: str
          The name of the securable.
        :param max_results: int (optional)
          Maximum number of workspace bindings to return. - When set to 0, the page length is set to a server
          configured value (recommended); - When set to a value greater than 0, the page length is the minimum
          of this value and a server configured value; - When set to a value less than 0, an invalid parameter
          error is returned; - If not set, all the workspace bindings are returned (not recommended).
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`WorkspaceBinding`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do(
                "GET",
                f"/api/2.1/unity-catalog/bindings/{securable_type}/{securable_name}",
                query=query,
                headers=headers,
            )
            if "bindings" in json:
                for v in json["bindings"]:
                    yield WorkspaceBinding.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        name: str,
        *,
        assign_workspaces: Optional[List[int]] = None,
        unassign_workspaces: Optional[List[int]] = None,
    ) -> UpdateCatalogWorkspaceBindingsResponse:
        """Updates workspace bindings of the catalog. The caller must be a metastore admin or an owner of the
        catalog.

        :param name: str
          The name of the catalog.
        :param assign_workspaces: List[int] (optional)
          A list of workspace IDs.
        :param unassign_workspaces: List[int] (optional)
          A list of workspace IDs.

        :returns: :class:`UpdateCatalogWorkspaceBindingsResponse`
        """

        body = {}
        if assign_workspaces is not None:
            body["assign_workspaces"] = [v for v in assign_workspaces]
        if unassign_workspaces is not None:
            body["unassign_workspaces"] = [v for v in unassign_workspaces]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.1/unity-catalog/workspace-bindings/catalogs/{name}", body=body, headers=headers
        )
        return UpdateCatalogWorkspaceBindingsResponse.from_dict(res)

    def update_bindings(
        self,
        securable_type: str,
        securable_name: str,
        *,
        add: Optional[List[WorkspaceBinding]] = None,
        remove: Optional[List[WorkspaceBinding]] = None,
    ) -> UpdateWorkspaceBindingsResponse:
        """Updates workspace bindings of the securable. The caller must be a metastore admin or an owner of the
        securable.

        :param securable_type: str
          The type of the securable to bind to a workspace (catalog, storage_credential, credential, or
          external_location).
        :param securable_name: str
          The name of the securable.
        :param add: List[:class:`WorkspaceBinding`] (optional)
          List of workspace bindings to add. If a binding for the workspace already exists with a different
          binding_type, adding it again with a new binding_type will update the existing binding (e.g., from
          READ_WRITE to READ_ONLY).
        :param remove: List[:class:`WorkspaceBinding`] (optional)
          List of workspace bindings to remove.

        :returns: :class:`UpdateWorkspaceBindingsResponse`
        """

        body = {}
        if add is not None:
            body["add"] = [v.as_dict() for v in add]
        if remove is not None:
            body["remove"] = [v.as_dict() for v in remove]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.1/unity-catalog/bindings/{securable_type}/{securable_name}", body=body, headers=headers
        )
        return UpdateWorkspaceBindingsResponse.from_dict(res)
