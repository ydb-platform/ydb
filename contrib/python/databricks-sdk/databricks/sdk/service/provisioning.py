# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional

from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict, _repeated_enum)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AwsCredentials:
    sts_role: Optional[StsRole] = None

    def as_dict(self) -> dict:
        """Serializes the AwsCredentials into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.sts_role:
            body["sts_role"] = self.sts_role.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AwsCredentials into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.sts_role:
            body["sts_role"] = self.sts_role
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AwsCredentials:
        """Deserializes the AwsCredentials from a dictionary."""
        return cls(sts_role=_from_dict(d, "sts_role", StsRole))


@dataclass
class AwsKeyInfo:
    key_arn: str
    """The AWS KMS key's Amazon Resource Name (ARN)."""

    key_region: str
    """The AWS KMS key region."""

    key_alias: Optional[str] = None
    """The AWS KMS key alias."""

    reuse_key_for_cluster_volumes: Optional[bool] = None
    """This field applies only if the `use_cases` property includes `STORAGE`. If this is set to true
    or omitted, the key is also used to encrypt cluster EBS volumes. If you do not want to use this
    key for encrypting EBS volumes, set to false."""

    def as_dict(self) -> dict:
        """Serializes the AwsKeyInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key_alias is not None:
            body["key_alias"] = self.key_alias
        if self.key_arn is not None:
            body["key_arn"] = self.key_arn
        if self.key_region is not None:
            body["key_region"] = self.key_region
        if self.reuse_key_for_cluster_volumes is not None:
            body["reuse_key_for_cluster_volumes"] = self.reuse_key_for_cluster_volumes
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AwsKeyInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key_alias is not None:
            body["key_alias"] = self.key_alias
        if self.key_arn is not None:
            body["key_arn"] = self.key_arn
        if self.key_region is not None:
            body["key_region"] = self.key_region
        if self.reuse_key_for_cluster_volumes is not None:
            body["reuse_key_for_cluster_volumes"] = self.reuse_key_for_cluster_volumes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AwsKeyInfo:
        """Deserializes the AwsKeyInfo from a dictionary."""
        return cls(
            key_alias=d.get("key_alias", None),
            key_arn=d.get("key_arn", None),
            key_region=d.get("key_region", None),
            reuse_key_for_cluster_volumes=d.get("reuse_key_for_cluster_volumes", None),
        )


@dataclass
class AzureKeyInfo:
    disk_encryption_set_id: Optional[str] = None
    """The Disk Encryption Set id that is used to represent the key info used for Managed Disk BYOK use
    case"""

    key_access_configuration: Optional[KeyAccessConfiguration] = None
    """The structure to store key access credential This is set if the Managed Identity is being used
    to access the Azure Key Vault key."""

    key_name: Optional[str] = None
    """The name of the key in KeyVault."""

    key_vault_uri: Optional[str] = None
    """The base URI of the KeyVault."""

    tenant_id: Optional[str] = None
    """The tenant id where the KeyVault lives."""

    version: Optional[str] = None
    """The current key version."""

    def as_dict(self) -> dict:
        """Serializes the AzureKeyInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.disk_encryption_set_id is not None:
            body["disk_encryption_set_id"] = self.disk_encryption_set_id
        if self.key_access_configuration:
            body["key_access_configuration"] = self.key_access_configuration.as_dict()
        if self.key_name is not None:
            body["key_name"] = self.key_name
        if self.key_vault_uri is not None:
            body["key_vault_uri"] = self.key_vault_uri
        if self.tenant_id is not None:
            body["tenant_id"] = self.tenant_id
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureKeyInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.disk_encryption_set_id is not None:
            body["disk_encryption_set_id"] = self.disk_encryption_set_id
        if self.key_access_configuration:
            body["key_access_configuration"] = self.key_access_configuration
        if self.key_name is not None:
            body["key_name"] = self.key_name
        if self.key_vault_uri is not None:
            body["key_vault_uri"] = self.key_vault_uri
        if self.tenant_id is not None:
            body["tenant_id"] = self.tenant_id
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureKeyInfo:
        """Deserializes the AzureKeyInfo from a dictionary."""
        return cls(
            disk_encryption_set_id=d.get("disk_encryption_set_id", None),
            key_access_configuration=_from_dict(d, "key_access_configuration", KeyAccessConfiguration),
            key_name=d.get("key_name", None),
            key_vault_uri=d.get("key_vault_uri", None),
            tenant_id=d.get("tenant_id", None),
            version=d.get("version", None),
        )


@dataclass
class AzureWorkspaceInfo:
    resource_group: Optional[str] = None
    """Azure Resource Group name"""

    subscription_id: Optional[str] = None
    """Azure Subscription ID"""

    def as_dict(self) -> dict:
        """Serializes the AzureWorkspaceInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.resource_group is not None:
            body["resource_group"] = self.resource_group
        if self.subscription_id is not None:
            body["subscription_id"] = self.subscription_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AzureWorkspaceInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.resource_group is not None:
            body["resource_group"] = self.resource_group
        if self.subscription_id is not None:
            body["subscription_id"] = self.subscription_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AzureWorkspaceInfo:
        """Deserializes the AzureWorkspaceInfo from a dictionary."""
        return cls(resource_group=d.get("resource_group", None), subscription_id=d.get("subscription_id", None))


@dataclass
class CloudResourceContainer:
    gcp: Optional[CustomerFacingGcpCloudResourceContainer] = None

    def as_dict(self) -> dict:
        """Serializes the CloudResourceContainer into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.gcp:
            body["gcp"] = self.gcp.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CloudResourceContainer into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.gcp:
            body["gcp"] = self.gcp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CloudResourceContainer:
        """Deserializes the CloudResourceContainer from a dictionary."""
        return cls(gcp=_from_dict(d, "gcp", CustomerFacingGcpCloudResourceContainer))


@dataclass
class CreateAwsKeyInfo:
    key_arn: str
    """The AWS KMS key's Amazon Resource Name (ARN)."""

    key_alias: Optional[str] = None
    """The AWS KMS key alias."""

    key_region: Optional[str] = None
    """The AWS KMS key region."""

    reuse_key_for_cluster_volumes: Optional[bool] = None
    """This field applies only if the `use_cases` property includes `STORAGE`. If this is set to true
    or omitted, the key is also used to encrypt cluster EBS volumes. If you do not want to use this
    key for encrypting EBS volumes, set to false."""

    def as_dict(self) -> dict:
        """Serializes the CreateAwsKeyInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key_alias is not None:
            body["key_alias"] = self.key_alias
        if self.key_arn is not None:
            body["key_arn"] = self.key_arn
        if self.key_region is not None:
            body["key_region"] = self.key_region
        if self.reuse_key_for_cluster_volumes is not None:
            body["reuse_key_for_cluster_volumes"] = self.reuse_key_for_cluster_volumes
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateAwsKeyInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key_alias is not None:
            body["key_alias"] = self.key_alias
        if self.key_arn is not None:
            body["key_arn"] = self.key_arn
        if self.key_region is not None:
            body["key_region"] = self.key_region
        if self.reuse_key_for_cluster_volumes is not None:
            body["reuse_key_for_cluster_volumes"] = self.reuse_key_for_cluster_volumes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateAwsKeyInfo:
        """Deserializes the CreateAwsKeyInfo from a dictionary."""
        return cls(
            key_alias=d.get("key_alias", None),
            key_arn=d.get("key_arn", None),
            key_region=d.get("key_region", None),
            reuse_key_for_cluster_volumes=d.get("reuse_key_for_cluster_volumes", None),
        )


@dataclass
class CreateCredentialAwsCredentials:
    sts_role: Optional[CreateCredentialStsRole] = None

    def as_dict(self) -> dict:
        """Serializes the CreateCredentialAwsCredentials into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.sts_role:
            body["sts_role"] = self.sts_role.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateCredentialAwsCredentials into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.sts_role:
            body["sts_role"] = self.sts_role
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateCredentialAwsCredentials:
        """Deserializes the CreateCredentialAwsCredentials from a dictionary."""
        return cls(sts_role=_from_dict(d, "sts_role", CreateCredentialStsRole))


@dataclass
class CreateCredentialStsRole:
    role_arn: Optional[str] = None
    """The Amazon Resource Name (ARN) of the cross account IAM role."""

    def as_dict(self) -> dict:
        """Serializes the CreateCredentialStsRole into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateCredentialStsRole into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateCredentialStsRole:
        """Deserializes the CreateCredentialStsRole from a dictionary."""
        return cls(role_arn=d.get("role_arn", None))


@dataclass
class CreateGcpKeyInfo:
    kms_key_id: str
    """Globally unique kms key resource id of the form
    projects/testProjectId/locations/us-east4/keyRings/gcpCmkKeyRing/cryptoKeys/cmk-eastus4"""

    gcp_service_account: Optional[GcpServiceAccount] = None
    """Globally unique service account email that has access to the KMS key. The service account exists
    within the Databricks CP project."""

    def as_dict(self) -> dict:
        """Serializes the CreateGcpKeyInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.gcp_service_account:
            body["gcp_service_account"] = self.gcp_service_account.as_dict()
        if self.kms_key_id is not None:
            body["kms_key_id"] = self.kms_key_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateGcpKeyInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.gcp_service_account:
            body["gcp_service_account"] = self.gcp_service_account
        if self.kms_key_id is not None:
            body["kms_key_id"] = self.kms_key_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateGcpKeyInfo:
        """Deserializes the CreateGcpKeyInfo from a dictionary."""
        return cls(
            gcp_service_account=_from_dict(d, "gcp_service_account", GcpServiceAccount),
            kms_key_id=d.get("kms_key_id", None),
        )


@dataclass
class Credential:
    account_id: Optional[str] = None
    """The Databricks account ID that hosts the credential."""

    aws_credentials: Optional[AwsCredentials] = None

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when the credential was created."""

    credentials_id: Optional[str] = None
    """Databricks credential configuration ID."""

    credentials_name: Optional[str] = None
    """The human-readable name of the credential configuration object."""

    def as_dict(self) -> dict:
        """Serializes the Credential into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_credentials:
            body["aws_credentials"] = self.aws_credentials.as_dict()
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.credentials_name is not None:
            body["credentials_name"] = self.credentials_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Credential into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_credentials:
            body["aws_credentials"] = self.aws_credentials
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.credentials_name is not None:
            body["credentials_name"] = self.credentials_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Credential:
        """Deserializes the Credential from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            aws_credentials=_from_dict(d, "aws_credentials", AwsCredentials),
            creation_time=d.get("creation_time", None),
            credentials_id=d.get("credentials_id", None),
            credentials_name=d.get("credentials_name", None),
        )


class CustomerFacingComputeMode(Enum):
    """Corresponds to compute mode defined here:
    https://src.dev.databricks.com/databricks/universe@9076536b18479afd639d1c1f9dd5a59f72215e69/-/blob/central/api/common.proto?L872
    """

    HYBRID = "HYBRID"
    SERVERLESS = "SERVERLESS"


@dataclass
class CustomerFacingGcpCloudResourceContainer:
    project_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the CustomerFacingGcpCloudResourceContainer into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.project_id is not None:
            body["project_id"] = self.project_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CustomerFacingGcpCloudResourceContainer into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.project_id is not None:
            body["project_id"] = self.project_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CustomerFacingGcpCloudResourceContainer:
        """Deserializes the CustomerFacingGcpCloudResourceContainer from a dictionary."""
        return cls(project_id=d.get("project_id", None))


class CustomerFacingStorageMode(Enum):

    CUSTOMER_HOSTED = "CUSTOMER_HOSTED"
    DEFAULT_STORAGE = "DEFAULT_STORAGE"


@dataclass
class CustomerManagedKey:
    account_id: Optional[str] = None
    """The Databricks account ID that holds the customer-managed key."""

    aws_key_info: Optional[AwsKeyInfo] = None

    azure_key_info: Optional[AzureKeyInfo] = None

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when the customer key was created."""

    customer_managed_key_id: Optional[str] = None
    """ID of the encryption key configuration object."""

    gcp_key_info: Optional[GcpKeyInfo] = None

    use_cases: Optional[List[KeyUseCase]] = None
    """The cases that the key can be used for."""

    def as_dict(self) -> dict:
        """Serializes the CustomerManagedKey into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_key_info:
            body["aws_key_info"] = self.aws_key_info.as_dict()
        if self.azure_key_info:
            body["azure_key_info"] = self.azure_key_info.as_dict()
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.customer_managed_key_id is not None:
            body["customer_managed_key_id"] = self.customer_managed_key_id
        if self.gcp_key_info:
            body["gcp_key_info"] = self.gcp_key_info.as_dict()
        if self.use_cases:
            body["use_cases"] = [v.value for v in self.use_cases]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CustomerManagedKey into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_key_info:
            body["aws_key_info"] = self.aws_key_info
        if self.azure_key_info:
            body["azure_key_info"] = self.azure_key_info
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.customer_managed_key_id is not None:
            body["customer_managed_key_id"] = self.customer_managed_key_id
        if self.gcp_key_info:
            body["gcp_key_info"] = self.gcp_key_info
        if self.use_cases:
            body["use_cases"] = self.use_cases
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CustomerManagedKey:
        """Deserializes the CustomerManagedKey from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            aws_key_info=_from_dict(d, "aws_key_info", AwsKeyInfo),
            azure_key_info=_from_dict(d, "azure_key_info", AzureKeyInfo),
            creation_time=d.get("creation_time", None),
            customer_managed_key_id=d.get("customer_managed_key_id", None),
            gcp_key_info=_from_dict(d, "gcp_key_info", GcpKeyInfo),
            use_cases=_repeated_enum(d, "use_cases", KeyUseCase),
        )


class EndpointUseCase(Enum):

    DATAPLANE_RELAY_ACCESS = "DATAPLANE_RELAY_ACCESS"
    GENERAL_ACCESS = "GENERAL_ACCESS"
    WORKSPACE_ACCESS = "WORKSPACE_ACCESS"


class ErrorType(Enum):
    """ErrorType and WarningType are used to represent the type of error or warning by NetworkHealth
    and NetworkWarning defined in central/api/accounts/accounts.proto"""

    CREDENTIALS = "credentials"
    NETWORK_ACL = "networkAcl"
    SECURITY_GROUP = "securityGroup"
    SUBNET = "subnet"
    VPC = "vpc"


@dataclass
class GcpCommonNetworkConfig:
    """The shared network config for GCP workspace. This object has common network configurations that
    are network attributions of a workspace. DEPRECATED. Use GkeConfig instead."""

    gke_cluster_master_ip_range: Optional[str] = None
    """The IP range that will be used to allocate GKE cluster master resources from. This field must
    not be set if gke_cluster_type=PUBLIC_NODE_PUBLIC_MASTER."""

    gke_connectivity_type: Optional[GkeConfigConnectivityType] = None
    """The type of network connectivity of the GKE cluster."""

    def as_dict(self) -> dict:
        """Serializes the GcpCommonNetworkConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.gke_cluster_master_ip_range is not None:
            body["gke_cluster_master_ip_range"] = self.gke_cluster_master_ip_range
        if self.gke_connectivity_type is not None:
            body["gke_connectivity_type"] = self.gke_connectivity_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpCommonNetworkConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.gke_cluster_master_ip_range is not None:
            body["gke_cluster_master_ip_range"] = self.gke_cluster_master_ip_range
        if self.gke_connectivity_type is not None:
            body["gke_connectivity_type"] = self.gke_connectivity_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpCommonNetworkConfig:
        """Deserializes the GcpCommonNetworkConfig from a dictionary."""
        return cls(
            gke_cluster_master_ip_range=d.get("gke_cluster_master_ip_range", None),
            gke_connectivity_type=_enum(d, "gke_connectivity_type", GkeConfigConnectivityType),
        )


@dataclass
class GcpKeyInfo:
    kms_key_id: str
    """Globally unique kms key resource id of the form
    projects/testProjectId/locations/us-east4/keyRings/gcpCmkKeyRing/cryptoKeys/cmk-eastus4"""

    gcp_service_account: Optional[GcpServiceAccount] = None
    """Globally unique service account email that has access to the KMS key. The service account exists
    within the Databricks CP project."""

    def as_dict(self) -> dict:
        """Serializes the GcpKeyInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.gcp_service_account:
            body["gcp_service_account"] = self.gcp_service_account.as_dict()
        if self.kms_key_id is not None:
            body["kms_key_id"] = self.kms_key_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpKeyInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.gcp_service_account:
            body["gcp_service_account"] = self.gcp_service_account
        if self.kms_key_id is not None:
            body["kms_key_id"] = self.kms_key_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpKeyInfo:
        """Deserializes the GcpKeyInfo from a dictionary."""
        return cls(
            gcp_service_account=_from_dict(d, "gcp_service_account", GcpServiceAccount),
            kms_key_id=d.get("kms_key_id", None),
        )


@dataclass
class GcpManagedNetworkConfig:
    """The network configuration for the workspace."""

    gke_cluster_pod_ip_range: Optional[str] = None
    """The IP range that will be used to allocate GKE cluster Pods from."""

    gke_cluster_service_ip_range: Optional[str] = None
    """The IP range that will be used to allocate GKE cluster Services from."""

    subnet_cidr: Optional[str] = None
    """The IP range which will be used to allocate GKE cluster nodes from. Note: Pods, services and
    master IP range must be mutually exclusive."""

    def as_dict(self) -> dict:
        """Serializes the GcpManagedNetworkConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.gke_cluster_pod_ip_range is not None:
            body["gke_cluster_pod_ip_range"] = self.gke_cluster_pod_ip_range
        if self.gke_cluster_service_ip_range is not None:
            body["gke_cluster_service_ip_range"] = self.gke_cluster_service_ip_range
        if self.subnet_cidr is not None:
            body["subnet_cidr"] = self.subnet_cidr
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpManagedNetworkConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.gke_cluster_pod_ip_range is not None:
            body["gke_cluster_pod_ip_range"] = self.gke_cluster_pod_ip_range
        if self.gke_cluster_service_ip_range is not None:
            body["gke_cluster_service_ip_range"] = self.gke_cluster_service_ip_range
        if self.subnet_cidr is not None:
            body["subnet_cidr"] = self.subnet_cidr
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpManagedNetworkConfig:
        """Deserializes the GcpManagedNetworkConfig from a dictionary."""
        return cls(
            gke_cluster_pod_ip_range=d.get("gke_cluster_pod_ip_range", None),
            gke_cluster_service_ip_range=d.get("gke_cluster_service_ip_range", None),
            subnet_cidr=d.get("subnet_cidr", None),
        )


@dataclass
class GcpNetworkInfo:
    network_project_id: str
    """The GCP project ID for network resources. This project is where the VPC and subnet resides."""

    vpc_id: str
    """The customer-provided VPC ID."""

    subnet_id: str
    """The customer-provided Subnet ID that will be available to Clusters in Workspaces using this
    Network."""

    subnet_region: str

    pod_ip_range_name: str
    """Name of the secondary range within the subnet that will be used by GKE as Pod IP range. This is
    BYO VPC specific. DB VPC uses network.getGcpManagedNetworkConfig.getGkeClusterPodIpRange"""

    service_ip_range_name: str
    """Name of the secondary range within the subnet that will be used by GKE as Service IP range."""

    def as_dict(self) -> dict:
        """Serializes the GcpNetworkInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.network_project_id is not None:
            body["network_project_id"] = self.network_project_id
        if self.pod_ip_range_name is not None:
            body["pod_ip_range_name"] = self.pod_ip_range_name
        if self.service_ip_range_name is not None:
            body["service_ip_range_name"] = self.service_ip_range_name
        if self.subnet_id is not None:
            body["subnet_id"] = self.subnet_id
        if self.subnet_region is not None:
            body["subnet_region"] = self.subnet_region
        if self.vpc_id is not None:
            body["vpc_id"] = self.vpc_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpNetworkInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.network_project_id is not None:
            body["network_project_id"] = self.network_project_id
        if self.pod_ip_range_name is not None:
            body["pod_ip_range_name"] = self.pod_ip_range_name
        if self.service_ip_range_name is not None:
            body["service_ip_range_name"] = self.service_ip_range_name
        if self.subnet_id is not None:
            body["subnet_id"] = self.subnet_id
        if self.subnet_region is not None:
            body["subnet_region"] = self.subnet_region
        if self.vpc_id is not None:
            body["vpc_id"] = self.vpc_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpNetworkInfo:
        """Deserializes the GcpNetworkInfo from a dictionary."""
        return cls(
            network_project_id=d.get("network_project_id", None),
            pod_ip_range_name=d.get("pod_ip_range_name", None),
            service_ip_range_name=d.get("service_ip_range_name", None),
            subnet_id=d.get("subnet_id", None),
            subnet_region=d.get("subnet_region", None),
            vpc_id=d.get("vpc_id", None),
        )


@dataclass
class GcpServiceAccount:
    service_account_email: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the GcpServiceAccount into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.service_account_email is not None:
            body["service_account_email"] = self.service_account_email
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpServiceAccount into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.service_account_email is not None:
            body["service_account_email"] = self.service_account_email
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpServiceAccount:
        """Deserializes the GcpServiceAccount from a dictionary."""
        return cls(service_account_email=d.get("service_account_email", None))


@dataclass
class GcpVpcEndpointInfo:
    project_id: str

    psc_endpoint_name: str

    endpoint_region: str

    psc_connection_id: Optional[str] = None

    service_attachment_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the GcpVpcEndpointInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.endpoint_region is not None:
            body["endpoint_region"] = self.endpoint_region
        if self.project_id is not None:
            body["project_id"] = self.project_id
        if self.psc_connection_id is not None:
            body["psc_connection_id"] = self.psc_connection_id
        if self.psc_endpoint_name is not None:
            body["psc_endpoint_name"] = self.psc_endpoint_name
        if self.service_attachment_id is not None:
            body["service_attachment_id"] = self.service_attachment_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GcpVpcEndpointInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.endpoint_region is not None:
            body["endpoint_region"] = self.endpoint_region
        if self.project_id is not None:
            body["project_id"] = self.project_id
        if self.psc_connection_id is not None:
            body["psc_connection_id"] = self.psc_connection_id
        if self.psc_endpoint_name is not None:
            body["psc_endpoint_name"] = self.psc_endpoint_name
        if self.service_attachment_id is not None:
            body["service_attachment_id"] = self.service_attachment_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GcpVpcEndpointInfo:
        """Deserializes the GcpVpcEndpointInfo from a dictionary."""
        return cls(
            endpoint_region=d.get("endpoint_region", None),
            project_id=d.get("project_id", None),
            psc_connection_id=d.get("psc_connection_id", None),
            psc_endpoint_name=d.get("psc_endpoint_name", None),
            service_attachment_id=d.get("service_attachment_id", None),
        )


@dataclass
class GkeConfig:
    """The configurations of the GKE cluster used by the GCP workspace."""

    connectivity_type: Optional[GkeConfigConnectivityType] = None
    """The type of network connectivity of the GKE cluster."""

    master_ip_range: Optional[str] = None
    """The IP range that will be used to allocate GKE cluster master resources from. This field must
    not be set if gke_cluster_type=PUBLIC_NODE_PUBLIC_MASTER."""

    def as_dict(self) -> dict:
        """Serializes the GkeConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connectivity_type is not None:
            body["connectivity_type"] = self.connectivity_type.value
        if self.master_ip_range is not None:
            body["master_ip_range"] = self.master_ip_range
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GkeConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connectivity_type is not None:
            body["connectivity_type"] = self.connectivity_type
        if self.master_ip_range is not None:
            body["master_ip_range"] = self.master_ip_range
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GkeConfig:
        """Deserializes the GkeConfig from a dictionary."""
        return cls(
            connectivity_type=_enum(d, "connectivity_type", GkeConfigConnectivityType),
            master_ip_range=d.get("master_ip_range", None),
        )


class GkeConfigConnectivityType(Enum):
    """Specifies the network connectivity types for the GKE nodes and the GKE master network.

    Set to `PRIVATE_NODE_PUBLIC_MASTER` for a private GKE cluster for the workspace. The GKE nodes
    will not have public IPs.

    Set to `PUBLIC_NODE_PUBLIC_MASTER` for a public GKE cluster. The nodes of a public GKE cluster
    have public IP addresses."""

    PRIVATE_NODE_PUBLIC_MASTER = "PRIVATE_NODE_PUBLIC_MASTER"
    PUBLIC_NODE_PUBLIC_MASTER = "PUBLIC_NODE_PUBLIC_MASTER"


@dataclass
class KeyAccessConfiguration:
    """The credential ID that is used to access the key vault."""

    credential_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the KeyAccessConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the KeyAccessConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential_id is not None:
            body["credential_id"] = self.credential_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> KeyAccessConfiguration:
        """Deserializes the KeyAccessConfiguration from a dictionary."""
        return cls(credential_id=d.get("credential_id", None))


class KeyUseCase(Enum):

    MANAGED_SERVICES = "MANAGED_SERVICES"
    STORAGE = "STORAGE"


@dataclass
class Network:
    account_id: Optional[str] = None
    """The Databricks account ID associated with this network configuration."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when the network was created."""

    error_messages: Optional[List[NetworkHealth]] = None
    """Array of error messages about the network configuration."""

    gcp_network_info: Optional[GcpNetworkInfo] = None

    network_id: Optional[str] = None
    """The Databricks network configuration ID."""

    network_name: Optional[str] = None
    """The human-readable name of the network configuration."""

    security_group_ids: Optional[List[str]] = None
    """IDs of one to five security groups associated with this network. Security group IDs **cannot**
    be used in multiple network configurations."""

    subnet_ids: Optional[List[str]] = None
    """IDs of at least two subnets associated with this network. Subnet IDs **cannot** be used in
    multiple network configurations."""

    vpc_endpoints: Optional[NetworkVpcEndpoints] = None

    vpc_id: Optional[str] = None
    """The ID of the VPC associated with this network configuration. VPC IDs can be used in multiple
    networks."""

    vpc_status: Optional[VpcStatus] = None

    warning_messages: Optional[List[NetworkWarning]] = None
    """Array of warning messages about the network configuration."""

    workspace_id: Optional[int] = None
    """Workspace ID associated with this network configuration."""

    def as_dict(self) -> dict:
        """Serializes the Network into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.error_messages:
            body["error_messages"] = [v.as_dict() for v in self.error_messages]
        if self.gcp_network_info:
            body["gcp_network_info"] = self.gcp_network_info.as_dict()
        if self.network_id is not None:
            body["network_id"] = self.network_id
        if self.network_name is not None:
            body["network_name"] = self.network_name
        if self.security_group_ids:
            body["security_group_ids"] = [v for v in self.security_group_ids]
        if self.subnet_ids:
            body["subnet_ids"] = [v for v in self.subnet_ids]
        if self.vpc_endpoints:
            body["vpc_endpoints"] = self.vpc_endpoints.as_dict()
        if self.vpc_id is not None:
            body["vpc_id"] = self.vpc_id
        if self.vpc_status is not None:
            body["vpc_status"] = self.vpc_status.value
        if self.warning_messages:
            body["warning_messages"] = [v.as_dict() for v in self.warning_messages]
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Network into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.error_messages:
            body["error_messages"] = self.error_messages
        if self.gcp_network_info:
            body["gcp_network_info"] = self.gcp_network_info
        if self.network_id is not None:
            body["network_id"] = self.network_id
        if self.network_name is not None:
            body["network_name"] = self.network_name
        if self.security_group_ids:
            body["security_group_ids"] = self.security_group_ids
        if self.subnet_ids:
            body["subnet_ids"] = self.subnet_ids
        if self.vpc_endpoints:
            body["vpc_endpoints"] = self.vpc_endpoints
        if self.vpc_id is not None:
            body["vpc_id"] = self.vpc_id
        if self.vpc_status is not None:
            body["vpc_status"] = self.vpc_status
        if self.warning_messages:
            body["warning_messages"] = self.warning_messages
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Network:
        """Deserializes the Network from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            creation_time=d.get("creation_time", None),
            error_messages=_repeated_dict(d, "error_messages", NetworkHealth),
            gcp_network_info=_from_dict(d, "gcp_network_info", GcpNetworkInfo),
            network_id=d.get("network_id", None),
            network_name=d.get("network_name", None),
            security_group_ids=d.get("security_group_ids", None),
            subnet_ids=d.get("subnet_ids", None),
            vpc_endpoints=_from_dict(d, "vpc_endpoints", NetworkVpcEndpoints),
            vpc_id=d.get("vpc_id", None),
            vpc_status=_enum(d, "vpc_status", VpcStatus),
            warning_messages=_repeated_dict(d, "warning_messages", NetworkWarning),
            workspace_id=d.get("workspace_id", None),
        )


@dataclass
class NetworkHealth:
    error_message: Optional[str] = None
    """Details of the error."""

    error_type: Optional[ErrorType] = None

    def as_dict(self) -> dict:
        """Serializes the NetworkHealth into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.error_type is not None:
            body["error_type"] = self.error_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NetworkHealth into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.error_type is not None:
            body["error_type"] = self.error_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NetworkHealth:
        """Deserializes the NetworkHealth from a dictionary."""
        return cls(error_message=d.get("error_message", None), error_type=_enum(d, "error_type", ErrorType))


@dataclass
class NetworkVpcEndpoints:
    dataplane_relay: Optional[List[str]] = None
    """The VPC endpoint ID used by this network to access the Databricks secure cluster connectivity
    relay."""

    rest_api: Optional[List[str]] = None
    """The VPC endpoint ID used by this network to access the Databricks REST API."""

    def as_dict(self) -> dict:
        """Serializes the NetworkVpcEndpoints into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dataplane_relay:
            body["dataplane_relay"] = [v for v in self.dataplane_relay]
        if self.rest_api:
            body["rest_api"] = [v for v in self.rest_api]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NetworkVpcEndpoints into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dataplane_relay:
            body["dataplane_relay"] = self.dataplane_relay
        if self.rest_api:
            body["rest_api"] = self.rest_api
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NetworkVpcEndpoints:
        """Deserializes the NetworkVpcEndpoints from a dictionary."""
        return cls(dataplane_relay=d.get("dataplane_relay", None), rest_api=d.get("rest_api", None))


@dataclass
class NetworkWarning:
    warning_message: Optional[str] = None
    """Details of the warning."""

    warning_type: Optional[WarningType] = None

    def as_dict(self) -> dict:
        """Serializes the NetworkWarning into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.warning_message is not None:
            body["warning_message"] = self.warning_message
        if self.warning_type is not None:
            body["warning_type"] = self.warning_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NetworkWarning into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.warning_message is not None:
            body["warning_message"] = self.warning_message
        if self.warning_type is not None:
            body["warning_type"] = self.warning_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NetworkWarning:
        """Deserializes the NetworkWarning from a dictionary."""
        return cls(warning_message=d.get("warning_message", None), warning_type=_enum(d, "warning_type", WarningType))


class PricingTier(Enum):

    COMMUNITY_EDITION = "COMMUNITY_EDITION"
    DEDICATED = "DEDICATED"
    ENTERPRISE = "ENTERPRISE"
    PREMIUM = "PREMIUM"
    STANDARD = "STANDARD"
    UNKNOWN = "UNKNOWN"


class PrivateAccessLevel(Enum):

    ACCOUNT = "ACCOUNT"
    ENDPOINT = "ENDPOINT"


@dataclass
class PrivateAccessSettings:
    """*"""

    account_id: Optional[str] = None
    """The Databricks account ID that hosts the private access settings."""

    allowed_vpc_endpoint_ids: Optional[List[str]] = None
    """An array of Databricks VPC endpoint IDs. This is the Databricks ID that is returned when
    registering the VPC endpoint configuration in your Databricks account. This is not the ID of the
    VPC endpoint in AWS. Only used when private_access_level is set to ENDPOINT. This is an allow
    list of VPC endpoints that in your account that can connect to your workspace over AWS
    PrivateLink. If hybrid access to your workspace is enabled by setting public_access_enabled to
    true, this control only works for PrivateLink connections. To control how your workspace is
    accessed via public internet, see IP access lists."""

    private_access_level: Optional[PrivateAccessLevel] = None
    """The private access level controls which VPC endpoints can connect to the UI or API of any
    workspace that attaches this private access settings object. `ACCOUNT` level access (the
    default) allows only VPC endpoints that are registered in your Databricks account connect to
    your workspace. `ENDPOINT` level access allows only specified VPC endpoints connect to your
    workspace. For details, see allowed_vpc_endpoint_ids."""

    private_access_settings_id: Optional[str] = None
    """Databricks private access settings ID."""

    private_access_settings_name: Optional[str] = None
    """The human-readable name of the private access settings object."""

    public_access_enabled: Optional[bool] = None
    """Determines if the workspace can be accessed over public internet. For fully private workspaces,
    you can optionally specify false, but only if you implement both the front-end and the back-end
    PrivateLink connections. Otherwise, specify true, which means that public access is enabled."""

    region: Optional[str] = None
    """The AWS region for workspaces attached to this private access settings object."""

    def as_dict(self) -> dict:
        """Serializes the PrivateAccessSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.allowed_vpc_endpoint_ids:
            body["allowed_vpc_endpoint_ids"] = [v for v in self.allowed_vpc_endpoint_ids]
        if self.private_access_level is not None:
            body["private_access_level"] = self.private_access_level.value
        if self.private_access_settings_id is not None:
            body["private_access_settings_id"] = self.private_access_settings_id
        if self.private_access_settings_name is not None:
            body["private_access_settings_name"] = self.private_access_settings_name
        if self.public_access_enabled is not None:
            body["public_access_enabled"] = self.public_access_enabled
        if self.region is not None:
            body["region"] = self.region
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PrivateAccessSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.allowed_vpc_endpoint_ids:
            body["allowed_vpc_endpoint_ids"] = self.allowed_vpc_endpoint_ids
        if self.private_access_level is not None:
            body["private_access_level"] = self.private_access_level
        if self.private_access_settings_id is not None:
            body["private_access_settings_id"] = self.private_access_settings_id
        if self.private_access_settings_name is not None:
            body["private_access_settings_name"] = self.private_access_settings_name
        if self.public_access_enabled is not None:
            body["public_access_enabled"] = self.public_access_enabled
        if self.region is not None:
            body["region"] = self.region
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PrivateAccessSettings:
        """Deserializes the PrivateAccessSettings from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            allowed_vpc_endpoint_ids=d.get("allowed_vpc_endpoint_ids", None),
            private_access_level=_enum(d, "private_access_level", PrivateAccessLevel),
            private_access_settings_id=d.get("private_access_settings_id", None),
            private_access_settings_name=d.get("private_access_settings_name", None),
            public_access_enabled=d.get("public_access_enabled", None),
            region=d.get("region", None),
        )


@dataclass
class RootBucketInfo:
    bucket_name: Optional[str] = None
    """Name of the S3 bucket"""

    def as_dict(self) -> dict:
        """Serializes the RootBucketInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bucket_name is not None:
            body["bucket_name"] = self.bucket_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RootBucketInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bucket_name is not None:
            body["bucket_name"] = self.bucket_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RootBucketInfo:
        """Deserializes the RootBucketInfo from a dictionary."""
        return cls(bucket_name=d.get("bucket_name", None))


@dataclass
class StorageConfiguration:
    account_id: Optional[str] = None
    """The Databricks account ID associated with this storage configuration."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when the storage configuration was created."""

    role_arn: Optional[str] = None
    """Optional IAM role that is used to access the workspace catalog which is created during workspace
    creation for UC by Default. If a storage configuration with this field populated is used to
    create a workspace, then a workspace catalog is created together with the workspace. The
    workspace catalog shares the root bucket with internal workspace storage (including DBFS root)
    but uses a dedicated bucket path prefix."""

    root_bucket_info: Optional[RootBucketInfo] = None
    """The root bucket information for the storage configuration."""

    storage_configuration_id: Optional[str] = None
    """Databricks storage configuration ID."""

    storage_configuration_name: Optional[str] = None
    """The human-readable name of the storage configuration."""

    def as_dict(self) -> dict:
        """Serializes the StorageConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        if self.root_bucket_info:
            body["root_bucket_info"] = self.root_bucket_info.as_dict()
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.storage_configuration_name is not None:
            body["storage_configuration_name"] = self.storage_configuration_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StorageConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        if self.root_bucket_info:
            body["root_bucket_info"] = self.root_bucket_info
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.storage_configuration_name is not None:
            body["storage_configuration_name"] = self.storage_configuration_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StorageConfiguration:
        """Deserializes the StorageConfiguration from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            creation_time=d.get("creation_time", None),
            role_arn=d.get("role_arn", None),
            root_bucket_info=_from_dict(d, "root_bucket_info", RootBucketInfo),
            storage_configuration_id=d.get("storage_configuration_id", None),
            storage_configuration_name=d.get("storage_configuration_name", None),
        )


@dataclass
class StsRole:
    role_arn: Optional[str] = None
    """The Amazon Resource Name (ARN) of the cross account IAM role."""

    def as_dict(self) -> dict:
        """Serializes the StsRole into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StsRole into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.role_arn is not None:
            body["role_arn"] = self.role_arn
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StsRole:
        """Deserializes the StsRole from a dictionary."""
        return cls(role_arn=d.get("role_arn", None))


@dataclass
class VpcEndpoint:
    """*"""

    account_id: Optional[str] = None
    """The Databricks account ID that hosts the VPC endpoint configuration. TODO - This may signal an
    OpenAPI diff; it does not show up in the generated spec"""

    aws_account_id: Optional[str] = None
    """The AWS Account in which the VPC endpoint object exists."""

    aws_endpoint_service_id: Optional[str] = None
    """The ID of the Databricks [endpoint service] that this VPC endpoint is connected to. For a list
    of endpoint service IDs for each supported AWS region, see the [Databricks PrivateLink
    documentation].
    
    [Databricks PrivateLink documentation]: https://docs.databricks.com/administration-guide/cloud-configurations/aws/privatelink.html
    [endpoint service]: https://docs.aws.amazon.com/vpc/latest/privatelink/endpoint-service.html"""

    aws_vpc_endpoint_id: Optional[str] = None
    """The ID of the VPC endpoint object in AWS."""

    gcp_vpc_endpoint_info: Optional[GcpVpcEndpointInfo] = None
    """The cloud info of this vpc endpoint. Info for a GCP vpc endpoint."""

    region: Optional[str] = None
    """The AWS region in which this VPC endpoint object exists."""

    state: Optional[str] = None
    """The current state (such as `available` or `rejected`) of the VPC endpoint. Derived from AWS. For
    the full set of values, see [AWS DescribeVpcEndpoint documentation].
    
    [AWS DescribeVpcEndpoint documentation]: https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-vpc-endpoints.html"""

    use_case: Optional[EndpointUseCase] = None
    """This enumeration represents the type of Databricks VPC endpoint service that was used when
    creating this VPC endpoint. If the VPC endpoint connects to the Databricks control plane for
    either the front-end connection or the back-end REST API connection, the value is
    WORKSPACE_ACCESS. If the VPC endpoint connects to the Databricks workspace for the back-end
    secure cluster connectivity relay, the value is DATAPLANE_RELAY_ACCESS."""

    vpc_endpoint_id: Optional[str] = None
    """Databricks VPC endpoint ID. This is the Databricks-specific name of the VPC endpoint. Do not
    confuse this with the `aws_vpc_endpoint_id`, which is the ID within AWS of the VPC endpoint."""

    vpc_endpoint_name: Optional[str] = None
    """The human-readable name of the storage configuration."""

    def as_dict(self) -> dict:
        """Serializes the VpcEndpoint into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_account_id is not None:
            body["aws_account_id"] = self.aws_account_id
        if self.aws_endpoint_service_id is not None:
            body["aws_endpoint_service_id"] = self.aws_endpoint_service_id
        if self.aws_vpc_endpoint_id is not None:
            body["aws_vpc_endpoint_id"] = self.aws_vpc_endpoint_id
        if self.gcp_vpc_endpoint_info:
            body["gcp_vpc_endpoint_info"] = self.gcp_vpc_endpoint_info.as_dict()
        if self.region is not None:
            body["region"] = self.region
        if self.state is not None:
            body["state"] = self.state
        if self.use_case is not None:
            body["use_case"] = self.use_case.value
        if self.vpc_endpoint_id is not None:
            body["vpc_endpoint_id"] = self.vpc_endpoint_id
        if self.vpc_endpoint_name is not None:
            body["vpc_endpoint_name"] = self.vpc_endpoint_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the VpcEndpoint into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_account_id is not None:
            body["aws_account_id"] = self.aws_account_id
        if self.aws_endpoint_service_id is not None:
            body["aws_endpoint_service_id"] = self.aws_endpoint_service_id
        if self.aws_vpc_endpoint_id is not None:
            body["aws_vpc_endpoint_id"] = self.aws_vpc_endpoint_id
        if self.gcp_vpc_endpoint_info:
            body["gcp_vpc_endpoint_info"] = self.gcp_vpc_endpoint_info
        if self.region is not None:
            body["region"] = self.region
        if self.state is not None:
            body["state"] = self.state
        if self.use_case is not None:
            body["use_case"] = self.use_case
        if self.vpc_endpoint_id is not None:
            body["vpc_endpoint_id"] = self.vpc_endpoint_id
        if self.vpc_endpoint_name is not None:
            body["vpc_endpoint_name"] = self.vpc_endpoint_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> VpcEndpoint:
        """Deserializes the VpcEndpoint from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            aws_account_id=d.get("aws_account_id", None),
            aws_endpoint_service_id=d.get("aws_endpoint_service_id", None),
            aws_vpc_endpoint_id=d.get("aws_vpc_endpoint_id", None),
            gcp_vpc_endpoint_info=_from_dict(d, "gcp_vpc_endpoint_info", GcpVpcEndpointInfo),
            region=d.get("region", None),
            state=d.get("state", None),
            use_case=_enum(d, "use_case", EndpointUseCase),
            vpc_endpoint_id=d.get("vpc_endpoint_id", None),
            vpc_endpoint_name=d.get("vpc_endpoint_name", None),
        )


class VpcStatus(Enum):

    BROKEN = "BROKEN"
    UNATTACHED = "UNATTACHED"
    VALID = "VALID"
    WARNED = "WARNED"


class WarningType(Enum):

    SECURITY_GROUP = "securityGroup"
    SUBNET = "subnet"


@dataclass
class Workspace:
    account_id: Optional[str] = None
    """Databricks account ID."""

    aws_region: Optional[str] = None

    azure_workspace_info: Optional[AzureWorkspaceInfo] = None

    cloud: Optional[str] = None
    """The cloud name. This field can have values like `azure`, `gcp`."""

    cloud_resource_container: Optional[CloudResourceContainer] = None

    compute_mode: Optional[CustomerFacingComputeMode] = None
    """The compute mode of the workspace."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when the workspace was created."""

    credentials_id: Optional[str] = None
    """ID of the workspace's credential configuration object."""

    custom_tags: Optional[Dict[str, str]] = None
    """The custom tags key-value pairing that is attached to this workspace. The key-value pair is a
    string of utf-8 characters. The value can be an empty string, with maximum length of 255
    characters. The key can be of maximum length of 127 characters, and cannot be empty."""

    deployment_name: Optional[str] = None

    expected_workspace_status: Optional[WorkspaceStatus] = None
    """A client owned field used to indicate the workspace status that the client expects to be in. For
    now this is only used to unblock Temporal workflow for GCP least privileged workspace."""

    gcp_managed_network_config: Optional[GcpManagedNetworkConfig] = None

    gke_config: Optional[GkeConfig] = None

    location: Optional[str] = None
    """The Google Cloud region of the workspace data plane in your Google account (for example,
    `us-east4`)."""

    managed_services_customer_managed_key_id: Optional[str] = None
    """ID of the key configuration for encrypting managed services."""

    network: Optional[WorkspaceNetwork] = None
    """The network configuration for the workspace.
    
    DEPRECATED. Use `network_id` instead."""

    network_connectivity_config_id: Optional[str] = None
    """The object ID of network connectivity config."""

    network_id: Optional[str] = None
    """If this workspace is BYO VPC, then the network_id will be populated. If this workspace is not
    BYO VPC, then the network_id will be empty."""

    pricing_tier: Optional[PricingTier] = None

    private_access_settings_id: Optional[str] = None
    """ID of the workspace's private access settings object. Only used for PrivateLink. You must
    specify this ID if you are using [AWS PrivateLink] for either front-end (user-to-workspace
    connection), back-end (data plane to control plane connection), or both connection types.
    
    Before configuring PrivateLink, read the [Databricks article about PrivateLink].",
    
    [AWS PrivateLink]: https://aws.amazon.com/privatelink/
    [Databricks article about PrivateLink]: https://docs.databricks.com/administration-guide/cloud-configurations/aws/privatelink.html"""

    storage_configuration_id: Optional[str] = None
    """ID of the workspace's storage configuration object."""

    storage_customer_managed_key_id: Optional[str] = None
    """ID of the key configuration for encrypting workspace storage."""

    storage_mode: Optional[CustomerFacingStorageMode] = None
    """The storage mode of the workspace."""

    workspace_id: Optional[int] = None
    """A unique integer ID for the workspace"""

    workspace_name: Optional[str] = None
    """The human-readable name of the workspace."""

    workspace_status: Optional[WorkspaceStatus] = None
    """The status of a workspace"""

    workspace_status_message: Optional[str] = None
    """Message describing the current workspace status."""

    def as_dict(self) -> dict:
        """Serializes the Workspace into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_region is not None:
            body["aws_region"] = self.aws_region
        if self.azure_workspace_info:
            body["azure_workspace_info"] = self.azure_workspace_info.as_dict()
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.cloud_resource_container:
            body["cloud_resource_container"] = self.cloud_resource_container.as_dict()
        if self.compute_mode is not None:
            body["compute_mode"] = self.compute_mode.value
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.deployment_name is not None:
            body["deployment_name"] = self.deployment_name
        if self.expected_workspace_status is not None:
            body["expected_workspace_status"] = self.expected_workspace_status.value
        if self.gcp_managed_network_config:
            body["gcp_managed_network_config"] = self.gcp_managed_network_config.as_dict()
        if self.gke_config:
            body["gke_config"] = self.gke_config.as_dict()
        if self.location is not None:
            body["location"] = self.location
        if self.managed_services_customer_managed_key_id is not None:
            body["managed_services_customer_managed_key_id"] = self.managed_services_customer_managed_key_id
        if self.network:
            body["network"] = self.network.as_dict()
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.network_id is not None:
            body["network_id"] = self.network_id
        if self.pricing_tier is not None:
            body["pricing_tier"] = self.pricing_tier.value
        if self.private_access_settings_id is not None:
            body["private_access_settings_id"] = self.private_access_settings_id
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.storage_customer_managed_key_id is not None:
            body["storage_customer_managed_key_id"] = self.storage_customer_managed_key_id
        if self.storage_mode is not None:
            body["storage_mode"] = self.storage_mode.value
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        if self.workspace_name is not None:
            body["workspace_name"] = self.workspace_name
        if self.workspace_status is not None:
            body["workspace_status"] = self.workspace_status.value
        if self.workspace_status_message is not None:
            body["workspace_status_message"] = self.workspace_status_message
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Workspace into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.aws_region is not None:
            body["aws_region"] = self.aws_region
        if self.azure_workspace_info:
            body["azure_workspace_info"] = self.azure_workspace_info
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.cloud_resource_container:
            body["cloud_resource_container"] = self.cloud_resource_container
        if self.compute_mode is not None:
            body["compute_mode"] = self.compute_mode
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.deployment_name is not None:
            body["deployment_name"] = self.deployment_name
        if self.expected_workspace_status is not None:
            body["expected_workspace_status"] = self.expected_workspace_status
        if self.gcp_managed_network_config:
            body["gcp_managed_network_config"] = self.gcp_managed_network_config
        if self.gke_config:
            body["gke_config"] = self.gke_config
        if self.location is not None:
            body["location"] = self.location
        if self.managed_services_customer_managed_key_id is not None:
            body["managed_services_customer_managed_key_id"] = self.managed_services_customer_managed_key_id
        if self.network:
            body["network"] = self.network
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.network_id is not None:
            body["network_id"] = self.network_id
        if self.pricing_tier is not None:
            body["pricing_tier"] = self.pricing_tier
        if self.private_access_settings_id is not None:
            body["private_access_settings_id"] = self.private_access_settings_id
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.storage_customer_managed_key_id is not None:
            body["storage_customer_managed_key_id"] = self.storage_customer_managed_key_id
        if self.storage_mode is not None:
            body["storage_mode"] = self.storage_mode
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        if self.workspace_name is not None:
            body["workspace_name"] = self.workspace_name
        if self.workspace_status is not None:
            body["workspace_status"] = self.workspace_status
        if self.workspace_status_message is not None:
            body["workspace_status_message"] = self.workspace_status_message
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Workspace:
        """Deserializes the Workspace from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            aws_region=d.get("aws_region", None),
            azure_workspace_info=_from_dict(d, "azure_workspace_info", AzureWorkspaceInfo),
            cloud=d.get("cloud", None),
            cloud_resource_container=_from_dict(d, "cloud_resource_container", CloudResourceContainer),
            compute_mode=_enum(d, "compute_mode", CustomerFacingComputeMode),
            creation_time=d.get("creation_time", None),
            credentials_id=d.get("credentials_id", None),
            custom_tags=d.get("custom_tags", None),
            deployment_name=d.get("deployment_name", None),
            expected_workspace_status=_enum(d, "expected_workspace_status", WorkspaceStatus),
            gcp_managed_network_config=_from_dict(d, "gcp_managed_network_config", GcpManagedNetworkConfig),
            gke_config=_from_dict(d, "gke_config", GkeConfig),
            location=d.get("location", None),
            managed_services_customer_managed_key_id=d.get("managed_services_customer_managed_key_id", None),
            network=_from_dict(d, "network", WorkspaceNetwork),
            network_connectivity_config_id=d.get("network_connectivity_config_id", None),
            network_id=d.get("network_id", None),
            pricing_tier=_enum(d, "pricing_tier", PricingTier),
            private_access_settings_id=d.get("private_access_settings_id", None),
            storage_configuration_id=d.get("storage_configuration_id", None),
            storage_customer_managed_key_id=d.get("storage_customer_managed_key_id", None),
            storage_mode=_enum(d, "storage_mode", CustomerFacingStorageMode),
            workspace_id=d.get("workspace_id", None),
            workspace_name=d.get("workspace_name", None),
            workspace_status=_enum(d, "workspace_status", WorkspaceStatus),
            workspace_status_message=d.get("workspace_status_message", None),
        )


@dataclass
class WorkspaceNetwork:
    """The network configuration for workspaces."""

    gcp_common_network_config: Optional[GcpCommonNetworkConfig] = None
    """The shared network config for GCP workspace. This object has common network configurations that
    are network attributions of a workspace. This object is input-only."""

    gcp_managed_network_config: Optional[GcpManagedNetworkConfig] = None
    """The mutually exclusive network deployment modes. The option decides which network mode the
    workspace will use. The network config for GCP workspace with Databricks managed network. This
    object is input-only and will not be provided when listing workspaces. See
    go/gcp-byovpc-alpha-design for interface decisions."""

    network_id: Optional[str] = None
    """The ID of the network object, if the workspace is a BYOVPC workspace. This should apply to
    workspaces on all clouds in internal services. In accounts-rest-api, user will use
    workspace.network_id for input and output instead. Currently (2021-06-19) the network ID is only
    used by GCP."""

    def as_dict(self) -> dict:
        """Serializes the WorkspaceNetwork into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.gcp_common_network_config:
            body["gcp_common_network_config"] = self.gcp_common_network_config.as_dict()
        if self.gcp_managed_network_config:
            body["gcp_managed_network_config"] = self.gcp_managed_network_config.as_dict()
        if self.network_id is not None:
            body["network_id"] = self.network_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceNetwork into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.gcp_common_network_config:
            body["gcp_common_network_config"] = self.gcp_common_network_config
        if self.gcp_managed_network_config:
            body["gcp_managed_network_config"] = self.gcp_managed_network_config
        if self.network_id is not None:
            body["network_id"] = self.network_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceNetwork:
        """Deserializes the WorkspaceNetwork from a dictionary."""
        return cls(
            gcp_common_network_config=_from_dict(d, "gcp_common_network_config", GcpCommonNetworkConfig),
            gcp_managed_network_config=_from_dict(d, "gcp_managed_network_config", GcpManagedNetworkConfig),
            network_id=d.get("network_id", None),
        )


class WorkspaceStatus(Enum):
    """The different statuses of a workspace. The following represents the current set of valid
    transitions from status to status: NOT_PROVISIONED -> PROVISIONING -> CANCELLED PROVISIONING ->
    RUNNING -> FAILED -> CANCELLED (note that this transition is disallowed in the MultiWorkspace
    Project) RUNNING -> PROVISIONING -> BANNED -> CANCELLED FAILED -> PROVISIONING -> CANCELLED
    BANNED -> RUNNING -> CANCELLED Note that a transition from any state to itself is also valid.
    TODO(PLAT-5867): add a transition from CANCELLED to some other value (e.g. RECOVERING)"""

    BANNED = "BANNED"
    CANCELLING = "CANCELLING"
    FAILED = "FAILED"
    NOT_PROVISIONED = "NOT_PROVISIONED"
    PROVISIONING = "PROVISIONING"
    RUNNING = "RUNNING"


class CredentialsAPI:
    """These APIs manage credential configurations for this workspace. Databricks needs access to a cross-account
    service IAM role in your AWS account so that Databricks can deploy clusters in the appropriate VPC for the
    new workspace. A credential configuration encapsulates this role information, and its ID is used when
    creating a new workspace."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, credentials_name: str, aws_credentials: CreateCredentialAwsCredentials) -> Credential:
        """Creates a Databricks credential configuration that represents cloud cross-account credentials for a
        specified account. Databricks uses this to set up network infrastructure properly to host Databricks
        clusters. For your AWS IAM role, you need to trust the External ID (the Databricks Account API account
        ID) in the returned credential object, and configure the required access policy.

        Save the response's `credentials_id` field, which is the ID for your new credential configuration
        object.

        For information about how to create a new workspace with this API, see [Create a new workspace using
        the Account API]

        [Create a new workspace using the Account API]: http://docs.databricks.com/administration-guide/account-api/new-workspace.html

        :param credentials_name: str
          The human-readable name of the credential configuration object.
        :param aws_credentials: :class:`CreateCredentialAwsCredentials`

        :returns: :class:`Credential`
        """

        body = {}
        if aws_credentials is not None:
            body["aws_credentials"] = aws_credentials.as_dict()
        if credentials_name is not None:
            body["credentials_name"] = credentials_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do("POST", f"/api/2.0/accounts/{self._api.account_id}/credentials", body=body, headers=headers)
        return Credential.from_dict(res)

    def delete(self, credentials_id: str) -> Credential:
        """Deletes a Databricks credential configuration object for an account, both specified by ID. You cannot
        delete a credential that is associated with any workspace.

        :param credentials_id: str
          Databricks Account API credential configuration ID

        :returns: :class:`Credential`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE", f"/api/2.0/accounts/{self._api.account_id}/credentials/{credentials_id}", headers=headers
        )
        return Credential.from_dict(res)

    def get(self, credentials_id: str) -> Credential:
        """Gets a Databricks credential configuration object for an account, both specified by ID.

        :param credentials_id: str
          Credential configuration ID

        :returns: :class:`Credential`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/credentials/{credentials_id}", headers=headers
        )
        return Credential.from_dict(res)

    def list(self) -> Iterator[Credential]:
        """List Databricks credential configuration objects for an account, specified by ID.


        :returns: Iterator over :class:`Credential`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/credentials", headers=headers)
        return [Credential.from_dict(v) for v in res]


class EncryptionKeysAPI:
    """These APIs manage encryption key configurations for this workspace (optional). A key configuration
    encapsulates the AWS KMS key information and some information about how the key configuration can be used.
    There are two possible uses for key configurations:

    * Managed services: A key configuration can be used to encrypt a workspace's notebook and secret data in
    the control plane, as well as Databricks SQL queries and query history. * Storage: A key configuration can
    be used to encrypt a workspace's DBFS and EBS data in the data plane.

    In both of these cases, the key configuration's ID is used when creating a new workspace. This Preview
    feature is available if your account is on the E2 version of the platform. Updating a running workspace
    with workspace storage encryption requires that the workspace is on the E2 version of the platform. If you
    have an older workspace, it might not be on the E2 version of the platform. If you are not sure, contact
    your Databricks representative."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        use_cases: List[KeyUseCase],
        *,
        aws_key_info: Optional[CreateAwsKeyInfo] = None,
        gcp_key_info: Optional[CreateGcpKeyInfo] = None,
    ) -> CustomerManagedKey:
        """Creates a customer-managed key configuration object for an account, specified by ID. This operation
        uploads a reference to a customer-managed key to Databricks. If the key is assigned as a workspace's
        customer-managed key for managed services, Databricks uses the key to encrypt the workspaces notebooks
        and secrets in the control plane, in addition to Databricks SQL queries and query history. If it is
        specified as a workspace's customer-managed key for workspace storage, the key encrypts the
        workspace's root S3 bucket (which contains the workspace's root DBFS and system data) and, optionally,
        cluster EBS volume data.

        **Important**: Customer-managed keys are supported only for some deployment types, subscription types,
        and AWS regions that currently support creation of Databricks workspaces.

        This operation is available only if your account is on the E2 version of the platform or on a select
        custom plan that allows multiple workspaces per account.

        :param use_cases: List[:class:`KeyUseCase`]
          The cases that the key can be used for.
        :param aws_key_info: :class:`CreateAwsKeyInfo` (optional)
        :param gcp_key_info: :class:`CreateGcpKeyInfo` (optional)

        :returns: :class:`CustomerManagedKey`
        """

        body = {}
        if aws_key_info is not None:
            body["aws_key_info"] = aws_key_info.as_dict()
        if gcp_key_info is not None:
            body["gcp_key_info"] = gcp_key_info.as_dict()
        if use_cases is not None:
            body["use_cases"] = [v.value for v in use_cases]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/customer-managed-keys", body=body, headers=headers
        )
        return CustomerManagedKey.from_dict(res)

    def delete(self, customer_managed_key_id: str) -> CustomerManagedKey:
        """Deletes a customer-managed key configuration object for an account. You cannot delete a configuration
        that is associated with a running workspace.

        :param customer_managed_key_id: str
          Databricks encryption key configuration ID.

        :returns: :class:`CustomerManagedKey`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/customer-managed-keys/{customer_managed_key_id}",
            headers=headers,
        )
        return CustomerManagedKey.from_dict(res)

    def get(self, customer_managed_key_id: str) -> CustomerManagedKey:
        """Gets a customer-managed key configuration object for an account, specified by ID. This operation
        uploads a reference to a customer-managed key to Databricks. If assigned as a workspace's
        customer-managed key for managed services, Databricks uses the key to encrypt the workspaces notebooks
        and secrets in the control plane, in addition to Databricks SQL queries and query history. If it is
        specified as a workspace's customer-managed key for storage, the key encrypts the workspace's root S3
        bucket (which contains the workspace's root DBFS and system data) and, optionally, cluster EBS volume
        data.

        **Important**: Customer-managed keys are supported only for some deployment types, subscription types,
        and AWS regions.

        This operation is available only if your account is on the E2 version of the platform.",

        :param customer_managed_key_id: str
          Databricks encryption key configuration ID.

        :returns: :class:`CustomerManagedKey`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/customer-managed-keys/{customer_managed_key_id}",
            headers=headers,
        )
        return CustomerManagedKey.from_dict(res)

    def list(self) -> Iterator[CustomerManagedKey]:
        """Lists Databricks customer-managed key configurations for an account.


        :returns: Iterator over :class:`CustomerManagedKey`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/customer-managed-keys", headers=headers)
        return [CustomerManagedKey.from_dict(v) for v in res]


class NetworksAPI:
    """These APIs manage network configurations for customer-managed VPCs (optional). Its ID is used when
    creating a new workspace if you use customer-managed VPCs."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        *,
        gcp_network_info: Optional[GcpNetworkInfo] = None,
        network_name: Optional[str] = None,
        security_group_ids: Optional[List[str]] = None,
        subnet_ids: Optional[List[str]] = None,
        vpc_endpoints: Optional[NetworkVpcEndpoints] = None,
        vpc_id: Optional[str] = None,
    ) -> Network:
        """Creates a Databricks network configuration that represents an VPC and its resources. The VPC will be
        used for new Databricks clusters. This requires a pre-existing VPC and subnets.

        :param gcp_network_info: :class:`GcpNetworkInfo` (optional)
        :param network_name: str (optional)
          The human-readable name of the network configuration.
        :param security_group_ids: List[str] (optional)
          IDs of one to five security groups associated with this network. Security group IDs **cannot** be
          used in multiple network configurations.
        :param subnet_ids: List[str] (optional)
          IDs of at least two subnets associated with this network. Subnet IDs **cannot** be used in multiple
          network configurations.
        :param vpc_endpoints: :class:`NetworkVpcEndpoints` (optional)
        :param vpc_id: str (optional)
          The ID of the VPC associated with this network configuration. VPC IDs can be used in multiple
          networks.

        :returns: :class:`Network`
        """

        body = {}
        if gcp_network_info is not None:
            body["gcp_network_info"] = gcp_network_info.as_dict()
        if network_name is not None:
            body["network_name"] = network_name
        if security_group_ids is not None:
            body["security_group_ids"] = [v for v in security_group_ids]
        if subnet_ids is not None:
            body["subnet_ids"] = [v for v in subnet_ids]
        if vpc_endpoints is not None:
            body["vpc_endpoints"] = vpc_endpoints.as_dict()
        if vpc_id is not None:
            body["vpc_id"] = vpc_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do("POST", f"/api/2.0/accounts/{self._api.account_id}/networks", body=body, headers=headers)
        return Network.from_dict(res)

    def delete(self, network_id: str) -> Network:
        """Deletes a Databricks network configuration, which represents a cloud VPC and its resources. You cannot
        delete a network that is associated with a workspace.

        This operation is available only if your account is on the E2 version of the platform.

        :param network_id: str
          Databricks Account API network configuration ID.

        :returns: :class:`Network`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("DELETE", f"/api/2.0/accounts/{self._api.account_id}/networks/{network_id}", headers=headers)
        return Network.from_dict(res)

    def get(self, network_id: str) -> Network:
        """Gets a Databricks network configuration, which represents a cloud VPC and its resources.

        :param network_id: str
          Databricks Account API network configuration ID.

        :returns: :class:`Network`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/networks/{network_id}", headers=headers)
        return Network.from_dict(res)

    def list(self) -> Iterator[Network]:
        """Lists Databricks network configurations for an account.


        :returns: Iterator over :class:`Network`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/networks", headers=headers)
        return [Network.from_dict(v) for v in res]


class PrivateAccessAPI:
    """These APIs manage private access settings for this account."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        *,
        allowed_vpc_endpoint_ids: Optional[List[str]] = None,
        private_access_level: Optional[PrivateAccessLevel] = None,
        private_access_settings_name: Optional[str] = None,
        public_access_enabled: Optional[bool] = None,
        region: Optional[str] = None,
    ) -> PrivateAccessSettings:
        """Creates a private access settings configuration, which represents network access restrictions for
        workspace resources. Private access settings configure whether workspaces can be accessed from the
        public internet or only from private endpoints.

        :param allowed_vpc_endpoint_ids: List[str] (optional)
          An array of Databricks VPC endpoint IDs. This is the Databricks ID returned when registering the VPC
          endpoint configuration in your Databricks account. This is not the ID of the VPC endpoint in AWS.
          Only used when private_access_level is set to ENDPOINT. This is an allow list of VPC endpoints
          registered in your Databricks account that can connect to your workspace over AWS PrivateLink. Note:
          If hybrid access to your workspace is enabled by setting public_access_enabled to true, this control
          only works for PrivateLink connections. To control how your workspace is accessed via public
          internet, see IP access lists.
        :param private_access_level: :class:`PrivateAccessLevel` (optional)
          The private access level controls which VPC endpoints can connect to the UI or API of any workspace
          that attaches this private access settings object. `ACCOUNT` level access (the default) allows only
          VPC endpoints that are registered in your Databricks account connect to your workspace. `ENDPOINT`
          level access allows only specified VPC endpoints connect to your workspace. For details, see
          allowed_vpc_endpoint_ids.
        :param private_access_settings_name: str (optional)
          The human-readable name of the private access settings object.
        :param public_access_enabled: bool (optional)
          Determines if the workspace can be accessed over public internet. For fully private workspaces, you
          can optionally specify false, but only if you implement both the front-end and the back-end
          PrivateLink connections. Otherwise, specify true, which means that public access is enabled.
        :param region: str (optional)
          The AWS region for workspaces attached to this private access settings object.

        :returns: :class:`PrivateAccessSettings`
        """

        body = {}
        if allowed_vpc_endpoint_ids is not None:
            body["allowed_vpc_endpoint_ids"] = [v for v in allowed_vpc_endpoint_ids]
        if private_access_level is not None:
            body["private_access_level"] = private_access_level.value
        if private_access_settings_name is not None:
            body["private_access_settings_name"] = private_access_settings_name
        if public_access_enabled is not None:
            body["public_access_enabled"] = public_access_enabled
        if region is not None:
            body["region"] = region
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/private-access-settings", body=body, headers=headers
        )
        return PrivateAccessSettings.from_dict(res)

    def delete(self, private_access_settings_id: str) -> PrivateAccessSettings:
        """Deletes a Databricks private access settings configuration, both specified by ID.

        :param private_access_settings_id: str

        :returns: :class:`PrivateAccessSettings`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/private-access-settings/{private_access_settings_id}",
            headers=headers,
        )
        return PrivateAccessSettings.from_dict(res)

    def get(self, private_access_settings_id: str) -> PrivateAccessSettings:
        """Gets a Databricks private access settings configuration, both specified by ID.

        :param private_access_settings_id: str

        :returns: :class:`PrivateAccessSettings`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/private-access-settings/{private_access_settings_id}",
            headers=headers,
        )
        return PrivateAccessSettings.from_dict(res)

    def list(self) -> Iterator[PrivateAccessSettings]:
        """Lists Databricks private access settings for an account.


        :returns: Iterator over :class:`PrivateAccessSettings`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/private-access-settings", headers=headers)
        return [PrivateAccessSettings.from_dict(v) for v in res]

    def replace(
        self, private_access_settings_id: str, customer_facing_private_access_settings: PrivateAccessSettings
    ) -> PrivateAccessSettings:
        """Updates an existing private access settings object, which specifies how your workspace is accessed
        over AWS PrivateLink. To use AWS PrivateLink, a workspace must have a private access settings object
        referenced by ID in the workspace's private_access_settings_id property. This operation completely
        overwrites your existing private access settings object attached to your workspaces. All workspaces
        attached to the private access settings are affected by any change. If public_access_enabled,
        private_access_level, or allowed_vpc_endpoint_ids are updated, effects of these changes might take
        several minutes to propagate to the workspace API. You can share one private access settings object
        with multiple workspaces in a single account. However, private access settings are specific to AWS
        regions, so only workspaces in the same AWS region can use a given private access settings object.
        Before configuring PrivateLink, read the Databricks article about PrivateLink.

        :param private_access_settings_id: str
          Databricks private access settings ID.
        :param customer_facing_private_access_settings: :class:`PrivateAccessSettings`
          Properties of the new private access settings object.

        :returns: :class:`PrivateAccessSettings`
        """

        body = customer_facing_private_access_settings.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PUT",
            f"/api/2.0/accounts/{self._api.account_id}/private-access-settings/{private_access_settings_id}",
            body=body,
            headers=headers,
        )
        return PrivateAccessSettings.from_dict(res)


class StorageAPI:
    """These APIs manage storage configurations for this workspace. A root storage S3 bucket in your account is
    required to store objects like cluster logs, notebook revisions, and job results. You can also use the
    root storage S3 bucket for storage of non-production DBFS data. A storage configuration encapsulates this
    bucket information, and its ID is used when creating a new workspace."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, storage_configuration_name: str, root_bucket_info: RootBucketInfo, *, role_arn: Optional[str] = None
    ) -> StorageConfiguration:
        """Creates a Databricks storage configuration for an account.

        :param storage_configuration_name: str
          The human-readable name of the storage configuration.
        :param root_bucket_info: :class:`RootBucketInfo`
          Root S3 bucket information.
        :param role_arn: str (optional)
          Optional IAM role that is used to access the workspace catalog which is created during workspace
          creation for UC by Default. If a storage configuration with this field populated is used to create a
          workspace, then a workspace catalog is created together with the workspace. The workspace catalog
          shares the root bucket with internal workspace storage (including DBFS root) but uses a dedicated
          bucket path prefix.

        :returns: :class:`StorageConfiguration`
        """

        body = {}
        if role_arn is not None:
            body["role_arn"] = role_arn
        if root_bucket_info is not None:
            body["root_bucket_info"] = root_bucket_info.as_dict()
        if storage_configuration_name is not None:
            body["storage_configuration_name"] = storage_configuration_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/storage-configurations", body=body, headers=headers
        )
        return StorageConfiguration.from_dict(res)

    def delete(self, storage_configuration_id: str) -> StorageConfiguration:
        """Deletes a Databricks storage configuration. You cannot delete a storage configuration that is
        associated with any workspace.

        :param storage_configuration_id: str

        :returns: :class:`StorageConfiguration`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/storage-configurations/{storage_configuration_id}",
            headers=headers,
        )
        return StorageConfiguration.from_dict(res)

    def get(self, storage_configuration_id: str) -> StorageConfiguration:
        """Gets a Databricks storage configuration for an account, both specified by ID.

        :param storage_configuration_id: str

        :returns: :class:`StorageConfiguration`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/storage-configurations/{storage_configuration_id}",
            headers=headers,
        )
        return StorageConfiguration.from_dict(res)

    def list(self) -> Iterator[StorageConfiguration]:
        """Lists Databricks storage configurations for an account, specified by ID.


        :returns: Iterator over :class:`StorageConfiguration`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/storage-configurations", headers=headers)
        return [StorageConfiguration.from_dict(v) for v in res]


class VpcEndpointsAPI:
    """These APIs manage VPC endpoint configurations for this account."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        *,
        aws_vpc_endpoint_id: Optional[str] = None,
        gcp_vpc_endpoint_info: Optional[GcpVpcEndpointInfo] = None,
        region: Optional[str] = None,
        vpc_endpoint_name: Optional[str] = None,
    ) -> VpcEndpoint:
        """Creates a VPC endpoint configuration, which represents a [VPC endpoint] object in AWS used to
        communicate privately with Databricks over [AWS PrivateLink].

        After you create the VPC endpoint configuration, the Databricks [endpoint service] automatically
        accepts the VPC endpoint.

        Before configuring PrivateLink, read the [Databricks article about PrivateLink].

        [AWS PrivateLink]: https://aws.amazon.com/privatelink
        [Databricks article about PrivateLink]: https://docs.databricks.com/administration-guide/cloud-configurations/aws/privatelink.html
        [VPC endpoint]: https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints.html
        [endpoint service]: https://docs.aws.amazon.com/vpc/latest/privatelink/privatelink-share-your-services.html

        :param aws_vpc_endpoint_id: str (optional)
          The ID of the VPC endpoint object in AWS.
        :param gcp_vpc_endpoint_info: :class:`GcpVpcEndpointInfo` (optional)
          The cloud info of this vpc endpoint.
        :param region: str (optional)
          The region in which this VPC endpoint object exists.
        :param vpc_endpoint_name: str (optional)
          The human-readable name of the storage configuration.

        :returns: :class:`VpcEndpoint`
        """

        body = {}
        if aws_vpc_endpoint_id is not None:
            body["aws_vpc_endpoint_id"] = aws_vpc_endpoint_id
        if gcp_vpc_endpoint_info is not None:
            body["gcp_vpc_endpoint_info"] = gcp_vpc_endpoint_info.as_dict()
        if region is not None:
            body["region"] = region
        if vpc_endpoint_name is not None:
            body["vpc_endpoint_name"] = vpc_endpoint_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/vpc-endpoints", body=body, headers=headers
        )
        return VpcEndpoint.from_dict(res)

    def delete(self, vpc_endpoint_id: str) -> VpcEndpoint:
        """Deletes a Databricks VPC endpoint configuration. You cannot delete a VPC endpoint configuration that
        is associated with any workspace.

        :param vpc_endpoint_id: str

        :returns: :class:`VpcEndpoint`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE", f"/api/2.0/accounts/{self._api.account_id}/vpc-endpoints/{vpc_endpoint_id}", headers=headers
        )
        return VpcEndpoint.from_dict(res)

    def get(self, vpc_endpoint_id: str) -> VpcEndpoint:
        """Gets a VPC endpoint configuration, which represents a [VPC endpoint] object in AWS used to communicate
        privately with Databricks over [AWS PrivateLink].

        [AWS PrivateLink]: https://aws.amazon.com/privatelink
        [VPC endpoint]: https://docs.aws.amazon.com/vpc/latest/privatelink/concepts.html

        :param vpc_endpoint_id: str
          Databricks VPC endpoint ID.

        :returns: :class:`VpcEndpoint`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/vpc-endpoints/{vpc_endpoint_id}", headers=headers
        )
        return VpcEndpoint.from_dict(res)

    def list(self) -> Iterator[VpcEndpoint]:
        """Lists Databricks VPC endpoint configurations for an account.


        :returns: Iterator over :class:`VpcEndpoint`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/vpc-endpoints", headers=headers)
        return [VpcEndpoint.from_dict(v) for v in res]


class WorkspacesAPI:
    """These APIs manage workspaces for this account. A Databricks workspace is an environment for accessing all
    of your Databricks assets. The workspace organizes objects (notebooks, libraries, and experiments) into
    folders, and provides access to data and computational resources such as clusters and jobs.

    These endpoints are available if your account is on the E2 version of the platform or on a select custom
    plan that allows multiple workspaces per account."""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_workspace_running(
        self, workspace_id: int, timeout=timedelta(minutes=20), callback: Optional[Callable[[Workspace], None]] = None
    ) -> Workspace:
        deadline = time.time() + timeout.total_seconds()
        target_states = (WorkspaceStatus.RUNNING,)
        failure_states = (
            WorkspaceStatus.BANNED,
            WorkspaceStatus.FAILED,
        )
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get(workspace_id=workspace_id)
            status = poll.workspace_status
            status_message = poll.workspace_status_message
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach RUNNING, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"workspace_id={workspace_id}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def create(
        self,
        *,
        aws_region: Optional[str] = None,
        cloud: Optional[str] = None,
        cloud_resource_container: Optional[CloudResourceContainer] = None,
        compute_mode: Optional[CustomerFacingComputeMode] = None,
        credentials_id: Optional[str] = None,
        custom_tags: Optional[Dict[str, str]] = None,
        deployment_name: Optional[str] = None,
        gcp_managed_network_config: Optional[GcpManagedNetworkConfig] = None,
        gke_config: Optional[GkeConfig] = None,
        location: Optional[str] = None,
        managed_services_customer_managed_key_id: Optional[str] = None,
        network_connectivity_config_id: Optional[str] = None,
        network_id: Optional[str] = None,
        pricing_tier: Optional[PricingTier] = None,
        private_access_settings_id: Optional[str] = None,
        storage_configuration_id: Optional[str] = None,
        storage_customer_managed_key_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
    ) -> Wait[Workspace]:
        """Creates a new workspace using a credential configuration and a storage configuration, an optional
        network configuration (if using a customer-managed VPC), an optional managed services key
        configuration (if using customer-managed keys for managed services), and an optional storage key
        configuration (if using customer-managed keys for storage). The key configurations used for managed
        services and storage encryption can be the same or different.

        Important: This operation is asynchronous. A response with HTTP status code 200 means the request has
        been accepted and is in progress, but does not mean that the workspace deployed successfully and is
        running. The initial workspace status is typically PROVISIONING. Use the workspace ID (workspace_id)
        field in the response to identify the new workspace and make repeated GET requests with the workspace
        ID and check its status. The workspace becomes available when the status changes to RUNNING.

        You can share one customer-managed VPC with multiple workspaces in a single account. It is not
        required to create a new VPC for each workspace. However, you cannot reuse subnets or Security Groups
        between workspaces. If you plan to share one VPC with multiple workspaces, make sure you size your VPC
        and subnets accordingly. Because a Databricks Account API network configuration encapsulates this
        information, you cannot reuse a Databricks Account API network configuration across workspaces.

        For information about how to create a new workspace with this API including error handling, see
        [Create a new workspace using the Account API].

        Important: Customer-managed VPCs, PrivateLink, and customer-managed keys are supported on a limited
        set of deployment and subscription types. If you have questions about availability, contact your
        Databricks representative.

        This operation is available only if your account is on the E2 version of the platform or on a select
        custom plan that allows multiple workspaces per account.

        [Create a new workspace using the Account API]: http://docs.databricks.com/administration-guide/account-api/new-workspace.html

        :param aws_region: str (optional)
        :param cloud: str (optional)
          DEPRECATED: This field is being ignored by the server and will be removed in the future. The cloud
          name. This field always has the value `gcp`.
        :param cloud_resource_container: :class:`CloudResourceContainer` (optional)
        :param compute_mode: :class:`CustomerFacingComputeMode` (optional)
          If the compute mode is `SERVERLESS`, a serverless workspace is created that comes pre-configured
          with serverless compute and default storage, providing a fully-managed, enterprise-ready SaaS
          experience. This means you don't need to provide any resources managed by you, such as credentials,
          storage, or network. If the compute mode is `HYBRID` (which is the default option), a classic
          workspace is created that uses customer-managed resources.
        :param credentials_id: str (optional)
          ID of the workspace's credential configuration object.
        :param custom_tags: Dict[str,str] (optional)
          The custom tags key-value pairing that is attached to this workspace. The key-value pair is a string
          of utf-8 characters. The value can be an empty string, with maximum length of 255 characters. The
          key can be of maximum length of 127 characters, and cannot be empty.
        :param deployment_name: str (optional)
          The deployment name defines part of the subdomain for the workspace. The workspace URL for the web
          application and REST APIs is <workspace-deployment-name>.cloud.databricks.com. For example, if the
          deployment name is abcsales, your workspace URL will be https://abcsales.cloud.databricks.com.
          Hyphens are allowed. This property supports only the set of characters that are allowed in a
          subdomain. To set this value, you must have a deployment name prefix. Contact your Databricks
          account team to add an account deployment name prefix to your account. Workspace deployment names
          follow the account prefix and a hyphen. For example, if your account's deployment prefix is acme and
          the workspace deployment name is workspace-1, the JSON response for the deployment_name field
          becomes acme-workspace-1. The workspace URL would be acme-workspace-1.cloud.databricks.com. You can
          also set the deployment_name to the reserved keyword EMPTY if you want the deployment name to only
          include the deployment prefix. For example, if your account's deployment prefix is acme and the
          workspace deployment name is EMPTY, the deployment_name becomes acme only and the workspace URL is
          acme.cloud.databricks.com. This value must be unique across all non-deleted deployments across all
          AWS regions. If a new workspace omits this property, the server generates a unique deployment name
          for you with the pattern dbc-xxxxxxxx-xxxx.
        :param gcp_managed_network_config: :class:`GcpManagedNetworkConfig` (optional)
        :param gke_config: :class:`GkeConfig` (optional)
        :param location: str (optional)
          The Google Cloud region of the workspace data plane in your Google account (for example,
          `us-east4`).
        :param managed_services_customer_managed_key_id: str (optional)
          The ID of the workspace's managed services encryption key configuration object. This is used to help
          protect and control access to the workspace's notebooks, secrets, Databricks SQL queries, and query
          history. The provided key configuration object property use_cases must contain MANAGED_SERVICES.
        :param network_connectivity_config_id: str (optional)
          The object ID of network connectivity config. Once assigned, the workspace serverless compute
          resources use the same set of stable IP CIDR blocks and optional private link to access your
          resources.
        :param network_id: str (optional)
          The ID of the workspace's network configuration object. To use AWS PrivateLink, this field is
          required.
        :param pricing_tier: :class:`PricingTier` (optional)
        :param private_access_settings_id: str (optional)
          ID of the workspace's private access settings object. Only used for PrivateLink. You must specify
          this ID if you are using [AWS PrivateLink] for either front-end (user-to-workspace connection),
          back-end (data plane to control plane connection), or both connection types. Before configuring
          PrivateLink, read the [Databricks article about PrivateLink].",

          [AWS PrivateLink]: https://aws.amazon.com/privatelink/
          [Databricks article about PrivateLink]: https://docs.databricks.com/administration-guide/cloud-configurations/aws/privatelink.html
        :param storage_configuration_id: str (optional)
          ID of the workspace's storage configuration object.
        :param storage_customer_managed_key_id: str (optional)
          The ID of the workspace's storage encryption key configuration object. This is used to encrypt the
          workspace's root S3 bucket (root DBFS and system data) and, optionally, cluster EBS volumes. The
          provided key configuration object property use_cases must contain STORAGE.
        :param workspace_name: str (optional)
          The human-readable name of the workspace.

        :returns:
          Long-running operation waiter for :class:`Workspace`.
          See :method:wait_get_workspace_running for more details.
        """

        body = {}
        if aws_region is not None:
            body["aws_region"] = aws_region
        if cloud is not None:
            body["cloud"] = cloud
        if cloud_resource_container is not None:
            body["cloud_resource_container"] = cloud_resource_container.as_dict()
        if compute_mode is not None:
            body["compute_mode"] = compute_mode.value
        if credentials_id is not None:
            body["credentials_id"] = credentials_id
        if custom_tags is not None:
            body["custom_tags"] = custom_tags
        if deployment_name is not None:
            body["deployment_name"] = deployment_name
        if gcp_managed_network_config is not None:
            body["gcp_managed_network_config"] = gcp_managed_network_config.as_dict()
        if gke_config is not None:
            body["gke_config"] = gke_config.as_dict()
        if location is not None:
            body["location"] = location
        if managed_services_customer_managed_key_id is not None:
            body["managed_services_customer_managed_key_id"] = managed_services_customer_managed_key_id
        if network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = network_connectivity_config_id
        if network_id is not None:
            body["network_id"] = network_id
        if pricing_tier is not None:
            body["pricing_tier"] = pricing_tier.value
        if private_access_settings_id is not None:
            body["private_access_settings_id"] = private_access_settings_id
        if storage_configuration_id is not None:
            body["storage_configuration_id"] = storage_configuration_id
        if storage_customer_managed_key_id is not None:
            body["storage_customer_managed_key_id"] = storage_customer_managed_key_id
        if workspace_name is not None:
            body["workspace_name"] = workspace_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        op_response = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/workspaces", body=body, headers=headers
        )
        return Wait(
            self.wait_get_workspace_running,
            response=Workspace.from_dict(op_response),
            workspace_id=op_response["workspace_id"],
        )

    def create_and_wait(
        self,
        *,
        aws_region: Optional[str] = None,
        cloud: Optional[str] = None,
        cloud_resource_container: Optional[CloudResourceContainer] = None,
        compute_mode: Optional[CustomerFacingComputeMode] = None,
        credentials_id: Optional[str] = None,
        custom_tags: Optional[Dict[str, str]] = None,
        deployment_name: Optional[str] = None,
        gcp_managed_network_config: Optional[GcpManagedNetworkConfig] = None,
        gke_config: Optional[GkeConfig] = None,
        location: Optional[str] = None,
        managed_services_customer_managed_key_id: Optional[str] = None,
        network_connectivity_config_id: Optional[str] = None,
        network_id: Optional[str] = None,
        pricing_tier: Optional[PricingTier] = None,
        private_access_settings_id: Optional[str] = None,
        storage_configuration_id: Optional[str] = None,
        storage_customer_managed_key_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
        timeout=timedelta(minutes=20),
    ) -> Workspace:
        return self.create(
            aws_region=aws_region,
            cloud=cloud,
            cloud_resource_container=cloud_resource_container,
            compute_mode=compute_mode,
            credentials_id=credentials_id,
            custom_tags=custom_tags,
            deployment_name=deployment_name,
            gcp_managed_network_config=gcp_managed_network_config,
            gke_config=gke_config,
            location=location,
            managed_services_customer_managed_key_id=managed_services_customer_managed_key_id,
            network_connectivity_config_id=network_connectivity_config_id,
            network_id=network_id,
            pricing_tier=pricing_tier,
            private_access_settings_id=private_access_settings_id,
            storage_configuration_id=storage_configuration_id,
            storage_customer_managed_key_id=storage_customer_managed_key_id,
            workspace_name=workspace_name,
        ).result(timeout=timeout)

    def delete(self, workspace_id: int) -> Workspace:
        """Deletes a Databricks workspace, both specified by ID.

        :param workspace_id: int

        :returns: :class:`Workspace`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE", f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}", headers=headers
        )
        return Workspace.from_dict(res)

    def get(self, workspace_id: int) -> Workspace:
        """Gets information including status for a Databricks workspace, specified by ID. In the response, the
        `workspace_status` field indicates the current status. After initial workspace creation (which is
        asynchronous), make repeated `GET` requests with the workspace ID and check its status. The workspace
        becomes available when the status changes to `RUNNING`. For information about how to create a new
        workspace with this API **including error handling**, see [Create a new workspace using the Account
        API].

        [Create a new workspace using the Account API]: http://docs.databricks.com/administration-guide/account-api/new-workspace.html

        :param workspace_id: int

        :returns: :class:`Workspace`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}", headers=headers
        )
        return Workspace.from_dict(res)

    def list(self) -> Iterator[Workspace]:
        """Lists Databricks workspaces for an account.


        :returns: Iterator over :class:`Workspace`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/workspaces", headers=headers)
        return [Workspace.from_dict(v) for v in res]

    def update(
        self, workspace_id: int, customer_facing_workspace: Workspace, *, update_mask: Optional[str] = None
    ) -> Wait[Workspace]:
        """Updates a workspace.

        :param workspace_id: int
          A unique integer ID for the workspace
        :param customer_facing_workspace: :class:`Workspace`
        :param update_mask: str (optional)
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns:
          Long-running operation waiter for :class:`Workspace`.
          See :method:wait_get_workspace_running for more details.
        """

        body = customer_facing_workspace.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        op_response = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}",
            query=query,
            body=body,
            headers=headers,
        )
        return Wait(
            self.wait_get_workspace_running,
            response=Workspace.from_dict(op_response),
            workspace_id=op_response["workspace_id"],
        )

    def update_and_wait(
        self,
        workspace_id: int,
        customer_facing_workspace: Workspace,
        *,
        update_mask: Optional[str] = None,
        timeout=timedelta(minutes=20),
    ) -> Workspace:
        return self.update(
            customer_facing_workspace=customer_facing_workspace, update_mask=update_mask, workspace_id=workspace_id
        ).result(timeout=timeout)
