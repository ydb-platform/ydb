# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service import catalog
from databricks.sdk.service._internal import (_enum, _from_dict,
                                              _repeated_dict, _repeated_enum)

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


class AuthenticationType(Enum):
    """The delta sharing authentication type."""

    DATABRICKS = "DATABRICKS"
    OAUTH_CLIENT_CREDENTIALS = "OAUTH_CLIENT_CREDENTIALS"
    OIDC_FEDERATION = "OIDC_FEDERATION"
    TOKEN = "TOKEN"


class ColumnTypeName(Enum):
    """UC supported column types Copied from
    https://src.dev.databricks.com/databricks/universe@23a85902bb58695ab9293adc9f327b0714b55e72/-/blob/managed-catalog/api/messages/table.proto?L68
    """

    ARRAY = "ARRAY"
    BINARY = "BINARY"
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    CHAR = "CHAR"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    DOUBLE = "DOUBLE"
    FLOAT = "FLOAT"
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
class DeltaSharingDependency:
    """Represents a UC dependency."""

    function: Optional[DeltaSharingFunctionDependency] = None

    table: Optional[DeltaSharingTableDependency] = None

    def as_dict(self) -> dict:
        """Serializes the DeltaSharingDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.function:
            body["function"] = self.function.as_dict()
        if self.table:
            body["table"] = self.table.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaSharingDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.function:
            body["function"] = self.function
        if self.table:
            body["table"] = self.table
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaSharingDependency:
        """Deserializes the DeltaSharingDependency from a dictionary."""
        return cls(
            function=_from_dict(d, "function", DeltaSharingFunctionDependency),
            table=_from_dict(d, "table", DeltaSharingTableDependency),
        )


@dataclass
class DeltaSharingDependencyList:
    """Represents a list of dependencies."""

    dependencies: Optional[List[DeltaSharingDependency]] = None
    """An array of Dependency."""

    def as_dict(self) -> dict:
        """Serializes the DeltaSharingDependencyList into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dependencies:
            body["dependencies"] = [v.as_dict() for v in self.dependencies]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaSharingDependencyList into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dependencies:
            body["dependencies"] = self.dependencies
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaSharingDependencyList:
        """Deserializes the DeltaSharingDependencyList from a dictionary."""
        return cls(dependencies=_repeated_dict(d, "dependencies", DeltaSharingDependency))


@dataclass
class DeltaSharingFunction:
    aliases: Optional[List[RegisteredModelAlias]] = None
    """The aliass of registered model."""

    comment: Optional[str] = None
    """The comment of the function."""

    data_type: Optional[ColumnTypeName] = None
    """The data type of the function."""

    dependency_list: Optional[DeltaSharingDependencyList] = None
    """The dependency list of the function."""

    full_data_type: Optional[str] = None
    """The full data type of the function."""

    id: Optional[str] = None
    """The id of the function."""

    input_params: Optional[FunctionParameterInfos] = None
    """The function parameter information."""

    name: Optional[str] = None
    """The name of the function."""

    properties: Optional[str] = None
    """The properties of the function."""

    routine_definition: Optional[str] = None
    """The routine definition of the function."""

    schema: Optional[str] = None
    """The name of the schema that the function belongs to."""

    securable_kind: Optional[SharedSecurableKind] = None
    """The securable kind of the function."""

    share: Optional[str] = None
    """The name of the share that the function belongs to."""

    share_id: Optional[str] = None
    """The id of the share that the function belongs to."""

    storage_location: Optional[str] = None
    """The storage location of the function."""

    tags: Optional[List[catalog.TagKeyValue]] = None
    """The tags of the function."""

    def as_dict(self) -> dict:
        """Serializes the DeltaSharingFunction into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aliases:
            body["aliases"] = [v.as_dict() for v in self.aliases]
        if self.comment is not None:
            body["comment"] = self.comment
        if self.data_type is not None:
            body["data_type"] = self.data_type.value
        if self.dependency_list:
            body["dependency_list"] = self.dependency_list.as_dict()
        if self.full_data_type is not None:
            body["full_data_type"] = self.full_data_type
        if self.id is not None:
            body["id"] = self.id
        if self.input_params:
            body["input_params"] = self.input_params.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.properties is not None:
            body["properties"] = self.properties
        if self.routine_definition is not None:
            body["routine_definition"] = self.routine_definition
        if self.schema is not None:
            body["schema"] = self.schema
        if self.securable_kind is not None:
            body["securable_kind"] = self.securable_kind.value
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaSharingFunction into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aliases:
            body["aliases"] = self.aliases
        if self.comment is not None:
            body["comment"] = self.comment
        if self.data_type is not None:
            body["data_type"] = self.data_type
        if self.dependency_list:
            body["dependency_list"] = self.dependency_list
        if self.full_data_type is not None:
            body["full_data_type"] = self.full_data_type
        if self.id is not None:
            body["id"] = self.id
        if self.input_params:
            body["input_params"] = self.input_params
        if self.name is not None:
            body["name"] = self.name
        if self.properties is not None:
            body["properties"] = self.properties
        if self.routine_definition is not None:
            body["routine_definition"] = self.routine_definition
        if self.schema is not None:
            body["schema"] = self.schema
        if self.securable_kind is not None:
            body["securable_kind"] = self.securable_kind
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.storage_location is not None:
            body["storage_location"] = self.storage_location
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaSharingFunction:
        """Deserializes the DeltaSharingFunction from a dictionary."""
        return cls(
            aliases=_repeated_dict(d, "aliases", RegisteredModelAlias),
            comment=d.get("comment", None),
            data_type=_enum(d, "data_type", ColumnTypeName),
            dependency_list=_from_dict(d, "dependency_list", DeltaSharingDependencyList),
            full_data_type=d.get("full_data_type", None),
            id=d.get("id", None),
            input_params=_from_dict(d, "input_params", FunctionParameterInfos),
            name=d.get("name", None),
            properties=d.get("properties", None),
            routine_definition=d.get("routine_definition", None),
            schema=d.get("schema", None),
            securable_kind=_enum(d, "securable_kind", SharedSecurableKind),
            share=d.get("share", None),
            share_id=d.get("share_id", None),
            storage_location=d.get("storage_location", None),
            tags=_repeated_dict(d, "tags", catalog.TagKeyValue),
        )


@dataclass
class DeltaSharingFunctionDependency:
    """A Function in UC as a dependency."""

    function_name: Optional[str] = None

    schema_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the DeltaSharingFunctionDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaSharingFunctionDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.function_name is not None:
            body["function_name"] = self.function_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaSharingFunctionDependency:
        """Deserializes the DeltaSharingFunctionDependency from a dictionary."""
        return cls(function_name=d.get("function_name", None), schema_name=d.get("schema_name", None))


@dataclass
class DeltaSharingTableDependency:
    """A Table in UC as a dependency."""

    schema_name: Optional[str] = None

    table_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the DeltaSharingTableDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaSharingTableDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaSharingTableDependency:
        """Deserializes the DeltaSharingTableDependency from a dictionary."""
        return cls(schema_name=d.get("schema_name", None), table_name=d.get("table_name", None))


@dataclass
class FederationPolicy:
    comment: Optional[str] = None
    """Description of the policy. This is a user-provided description."""

    create_time: Optional[str] = None
    """System-generated timestamp indicating when the policy was created."""

    id: Optional[str] = None
    """Unique, immutable system-generated identifier for the federation policy."""

    name: Optional[str] = None
    """Name of the federation policy. A recipient can have multiple policies with different names. The
    name must contain only lowercase alphanumeric characters, numbers, and hyphens."""

    oidc_policy: Optional[OidcFederationPolicy] = None
    """Specifies the policy to use for validating OIDC claims in the federated tokens."""

    update_time: Optional[str] = None
    """System-generated timestamp indicating when the policy was last updated."""

    def as_dict(self) -> dict:
        """Serializes the FederationPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.oidc_policy:
            body["oidc_policy"] = self.oidc_policy.as_dict()
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FederationPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.oidc_policy:
            body["oidc_policy"] = self.oidc_policy
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FederationPolicy:
        """Deserializes the FederationPolicy from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            create_time=d.get("create_time", None),
            id=d.get("id", None),
            name=d.get("name", None),
            oidc_policy=_from_dict(d, "oidc_policy", OidcFederationPolicy),
            update_time=d.get("update_time", None),
        )


@dataclass
class FunctionParameterInfo:
    """Represents a parameter of a function. The same message is used for both input and output
    columns."""

    comment: Optional[str] = None
    """The comment of the parameter."""

    name: Optional[str] = None
    """The name of the parameter."""

    parameter_default: Optional[str] = None
    """The default value of the parameter."""

    parameter_mode: Optional[FunctionParameterMode] = None
    """The mode of the function parameter."""

    parameter_type: Optional[FunctionParameterType] = None
    """The type of the function parameter."""

    position: Optional[int] = None
    """The position of the parameter."""

    type_interval_type: Optional[str] = None
    """The interval type of the parameter type."""

    type_json: Optional[str] = None
    """The type of the parameter in JSON format."""

    type_name: Optional[ColumnTypeName] = None
    """The type of the parameter in Enum format."""

    type_precision: Optional[int] = None
    """The precision of the parameter type."""

    type_scale: Optional[int] = None
    """The scale of the parameter type."""

    type_text: Optional[str] = None
    """The type of the parameter in text format."""

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
    """The list of parameters of the function."""

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
    INOUT = "INOUT"
    OUT = "OUT"


class FunctionParameterType(Enum):

    COLUMN = "COLUMN"
    PARAM = "PARAM"


@dataclass
class GetActivationUrlInfoResponse:
    def as_dict(self) -> dict:
        """Serializes the GetActivationUrlInfoResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetActivationUrlInfoResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetActivationUrlInfoResponse:
        """Deserializes the GetActivationUrlInfoResponse from a dictionary."""
        return cls()


@dataclass
class GetRecipientSharePermissionsResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    permissions_out: Optional[List[ShareToPrivilegeAssignment]] = None
    """An array of data share permissions for a recipient."""

    def as_dict(self) -> dict:
        """Serializes the GetRecipientSharePermissionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.permissions_out:
            body["permissions_out"] = [v.as_dict() for v in self.permissions_out]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetRecipientSharePermissionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.permissions_out:
            body["permissions_out"] = self.permissions_out
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetRecipientSharePermissionsResponse:
        """Deserializes the GetRecipientSharePermissionsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            permissions_out=_repeated_dict(d, "permissions_out", ShareToPrivilegeAssignment),
        )


@dataclass
class GetSharePermissionsResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    privilege_assignments: Optional[List[PrivilegeAssignment]] = None
    """The privileges assigned to each principal"""

    def as_dict(self) -> dict:
        """Serializes the GetSharePermissionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.privilege_assignments:
            body["privilege_assignments"] = [v.as_dict() for v in self.privilege_assignments]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetSharePermissionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.privilege_assignments:
            body["privilege_assignments"] = self.privilege_assignments
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetSharePermissionsResponse:
        """Deserializes the GetSharePermissionsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            privilege_assignments=_repeated_dict(d, "privilege_assignments", PrivilegeAssignment),
        )


@dataclass
class IpAccessList:
    allowed_ip_addresses: Optional[List[str]] = None
    """Allowed IP Addresses in CIDR notation. Limit of 100."""

    def as_dict(self) -> dict:
        """Serializes the IpAccessList into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.allowed_ip_addresses:
            body["allowed_ip_addresses"] = [v for v in self.allowed_ip_addresses]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IpAccessList into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.allowed_ip_addresses:
            body["allowed_ip_addresses"] = self.allowed_ip_addresses
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IpAccessList:
        """Deserializes the IpAccessList from a dictionary."""
        return cls(allowed_ip_addresses=d.get("allowed_ip_addresses", None))


@dataclass
class ListFederationPoliciesResponse:
    next_page_token: Optional[str] = None

    policies: Optional[List[FederationPolicy]] = None

    def as_dict(self) -> dict:
        """Serializes the ListFederationPoliciesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.policies:
            body["policies"] = [v.as_dict() for v in self.policies]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListFederationPoliciesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.policies:
            body["policies"] = self.policies
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListFederationPoliciesResponse:
        """Deserializes the ListFederationPoliciesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), policies=_repeated_dict(d, "policies", FederationPolicy)
        )


@dataclass
class ListProviderShareAssetsResponse:
    """Response to ListProviderShareAssets, which contains the list of assets of a share."""

    functions: Optional[List[DeltaSharingFunction]] = None
    """The list of functions in the share."""

    notebooks: Optional[List[NotebookFile]] = None
    """The list of notebooks in the share."""

    share: Optional[Share] = None
    """The metadata of the share."""

    tables: Optional[List[Table]] = None
    """The list of tables in the share."""

    volumes: Optional[List[Volume]] = None
    """The list of volumes in the share."""

    def as_dict(self) -> dict:
        """Serializes the ListProviderShareAssetsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.functions:
            body["functions"] = [v.as_dict() for v in self.functions]
        if self.notebooks:
            body["notebooks"] = [v.as_dict() for v in self.notebooks]
        if self.share:
            body["share"] = self.share.as_dict()
        if self.tables:
            body["tables"] = [v.as_dict() for v in self.tables]
        if self.volumes:
            body["volumes"] = [v.as_dict() for v in self.volumes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListProviderShareAssetsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.functions:
            body["functions"] = self.functions
        if self.notebooks:
            body["notebooks"] = self.notebooks
        if self.share:
            body["share"] = self.share
        if self.tables:
            body["tables"] = self.tables
        if self.volumes:
            body["volumes"] = self.volumes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListProviderShareAssetsResponse:
        """Deserializes the ListProviderShareAssetsResponse from a dictionary."""
        return cls(
            functions=_repeated_dict(d, "functions", DeltaSharingFunction),
            notebooks=_repeated_dict(d, "notebooks", NotebookFile),
            share=_from_dict(d, "share", Share),
            tables=_repeated_dict(d, "tables", Table),
            volumes=_repeated_dict(d, "volumes", Volume),
        )


@dataclass
class ListProviderSharesResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    shares: Optional[List[ProviderShare]] = None
    """An array of provider shares."""

    def as_dict(self) -> dict:
        """Serializes the ListProviderSharesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.shares:
            body["shares"] = [v.as_dict() for v in self.shares]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListProviderSharesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.shares:
            body["shares"] = self.shares
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListProviderSharesResponse:
        """Deserializes the ListProviderSharesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), shares=_repeated_dict(d, "shares", ProviderShare))


@dataclass
class ListProvidersResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    providers: Optional[List[ProviderInfo]] = None
    """An array of provider information objects."""

    def as_dict(self) -> dict:
        """Serializes the ListProvidersResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.providers:
            body["providers"] = [v.as_dict() for v in self.providers]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListProvidersResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.providers:
            body["providers"] = self.providers
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListProvidersResponse:
        """Deserializes the ListProvidersResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), providers=_repeated_dict(d, "providers", ProviderInfo)
        )


@dataclass
class ListRecipientsResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    recipients: Optional[List[RecipientInfo]] = None
    """An array of recipient information objects."""

    def as_dict(self) -> dict:
        """Serializes the ListRecipientsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.recipients:
            body["recipients"] = [v.as_dict() for v in self.recipients]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListRecipientsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.recipients:
            body["recipients"] = self.recipients
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListRecipientsResponse:
        """Deserializes the ListRecipientsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), recipients=_repeated_dict(d, "recipients", RecipientInfo)
        )


@dataclass
class ListSharesResponse:
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results. Absent if there are no more pages.
    __page_token__ should be set to this value for the next request (for the next page of results)."""

    shares: Optional[List[ShareInfo]] = None
    """An array of data share information objects."""

    def as_dict(self) -> dict:
        """Serializes the ListSharesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.shares:
            body["shares"] = [v.as_dict() for v in self.shares]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSharesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.shares:
            body["shares"] = self.shares
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSharesResponse:
        """Deserializes the ListSharesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), shares=_repeated_dict(d, "shares", ShareInfo))


@dataclass
class NotebookFile:
    comment: Optional[str] = None
    """The comment of the notebook file."""

    id: Optional[str] = None
    """The id of the notebook file."""

    name: Optional[str] = None
    """Name of the notebook file."""

    share: Optional[str] = None
    """The name of the share that the notebook file belongs to."""

    share_id: Optional[str] = None
    """The id of the share that the notebook file belongs to."""

    tags: Optional[List[catalog.TagKeyValue]] = None
    """The tags of the notebook file."""

    def as_dict(self) -> dict:
        """Serializes the NotebookFile into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotebookFile into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotebookFile:
        """Deserializes the NotebookFile from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            id=d.get("id", None),
            name=d.get("name", None),
            share=d.get("share", None),
            share_id=d.get("share_id", None),
            tags=_repeated_dict(d, "tags", catalog.TagKeyValue),
        )


@dataclass
class OidcFederationPolicy:
    """Specifies the policy to use for validating OIDC claims in your federated tokens from Delta
    Sharing Clients. Refer to https://docs.databricks.com/en/delta-sharing/create-recipient-oidc-fed
    for more details."""

    issuer: str
    """The required token issuer, as specified in the 'iss' claim of federated tokens."""

    subject_claim: str
    """The claim that contains the subject of the token. Depending on the identity provider and the use
    case (U2M or M2M), this can vary: - For Entra ID (AAD): * U2M flow (group access): Use `groups`.
    * U2M flow (user access): Use `oid`. * M2M flow (OAuth App access): Use `azp`. - For other IdPs,
    refer to the specific IdP documentation.
    
    Supported `subject_claim` values are: - `oid`: Object ID of the user. - `azp`: Client ID of the
    OAuth app. - `groups`: Object ID of the group. - `sub`: Subject identifier for other use cases."""

    subject: str
    """The required token subject, as specified in the subject claim of federated tokens. The subject
    claim identifies the identity of the user or machine accessing the resource. Examples for Entra
    ID (AAD): - U2M flow (group access): If the subject claim is `groups`, this must be the Object
    ID of the group in Entra ID. - U2M flow (user access): If the subject claim is `oid`, this must
    be the Object ID of the user in Entra ID. - M2M flow (OAuth App access): If the subject claim is
    `azp`, this must be the client ID of the OAuth app registered in Entra ID."""

    audiences: Optional[List[str]] = None
    """The allowed token audiences, as specified in the 'aud' claim of federated tokens. The audience
    identifier is intended to represent the recipient of the token. Can be any non-empty string
    value. As long as the audience in the token matches at least one audience in the policy,"""

    def as_dict(self) -> dict:
        """Serializes the OidcFederationPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.audiences:
            body["audiences"] = [v for v in self.audiences]
        if self.issuer is not None:
            body["issuer"] = self.issuer
        if self.subject is not None:
            body["subject"] = self.subject
        if self.subject_claim is not None:
            body["subject_claim"] = self.subject_claim
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OidcFederationPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.audiences:
            body["audiences"] = self.audiences
        if self.issuer is not None:
            body["issuer"] = self.issuer
        if self.subject is not None:
            body["subject"] = self.subject
        if self.subject_claim is not None:
            body["subject_claim"] = self.subject_claim
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OidcFederationPolicy:
        """Deserializes the OidcFederationPolicy from a dictionary."""
        return cls(
            audiences=d.get("audiences", None),
            issuer=d.get("issuer", None),
            subject=d.get("subject", None),
            subject_claim=d.get("subject_claim", None),
        )


@dataclass
class Partition:
    values: Optional[List[PartitionValue]] = None
    """An array of partition values."""

    def as_dict(self) -> dict:
        """Serializes the Partition into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.values:
            body["values"] = [v.as_dict() for v in self.values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Partition into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.values:
            body["values"] = self.values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Partition:
        """Deserializes the Partition from a dictionary."""
        return cls(values=_repeated_dict(d, "values", PartitionValue))


@dataclass
class PartitionValue:
    name: Optional[str] = None
    """The name of the partition column."""

    op: Optional[PartitionValueOp] = None
    """The operator to apply for the value."""

    recipient_property_key: Optional[str] = None
    """The key of a Delta Sharing recipient's property. For example "databricks-account-id". When this
    field is set, field `value` can not be set."""

    value: Optional[str] = None
    """The value of the partition column. When this value is not set, it means `null` value. When this
    field is set, field `recipient_property_key` can not be set."""

    def as_dict(self) -> dict:
        """Serializes the PartitionValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.op is not None:
            body["op"] = self.op.value
        if self.recipient_property_key is not None:
            body["recipient_property_key"] = self.recipient_property_key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PartitionValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.op is not None:
            body["op"] = self.op
        if self.recipient_property_key is not None:
            body["recipient_property_key"] = self.recipient_property_key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PartitionValue:
        """Deserializes the PartitionValue from a dictionary."""
        return cls(
            name=d.get("name", None),
            op=_enum(d, "op", PartitionValueOp),
            recipient_property_key=d.get("recipient_property_key", None),
            value=d.get("value", None),
        )


class PartitionValueOp(Enum):

    EQUAL = "EQUAL"
    LIKE = "LIKE"


@dataclass
class PermissionsChange:
    add: Optional[List[str]] = None
    """The set of privileges to add."""

    principal: Optional[str] = None
    """The principal whose privileges we are changing. Only one of principal or principal_id should be
    specified, never both at the same time."""

    remove: Optional[List[str]] = None
    """The set of privileges to remove."""

    def as_dict(self) -> dict:
        """Serializes the PermissionsChange into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.add:
            body["add"] = [v for v in self.add]
        if self.principal is not None:
            body["principal"] = self.principal
        if self.remove:
            body["remove"] = [v for v in self.remove]
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
        return cls(add=d.get("add", None), principal=d.get("principal", None), remove=d.get("remove", None))


class Privilege(Enum):

    ACCESS = "ACCESS"
    ALL_PRIVILEGES = "ALL_PRIVILEGES"
    APPLY_TAG = "APPLY_TAG"
    CREATE = "CREATE"
    CREATE_CATALOG = "CREATE_CATALOG"
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
    MANAGE = "MANAGE"
    MANAGE_ALLOWLIST = "MANAGE_ALLOWLIST"
    MODIFY = "MODIFY"
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
class ProviderInfo:
    authentication_type: Optional[AuthenticationType] = None

    cloud: Optional[str] = None
    """Cloud vendor of the provider's UC metastore. This field is only present when the
    __authentication_type__ is **DATABRICKS**."""

    comment: Optional[str] = None
    """Description about the provider."""

    created_at: Optional[int] = None
    """Time at which this Provider was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of Provider creator."""

    data_provider_global_metastore_id: Optional[str] = None
    """The global UC metastore id of the data provider. This field is only present when the
    __authentication_type__ is **DATABRICKS**. The identifier is of format
    __cloud__:__region__:__metastore-uuid__."""

    metastore_id: Optional[str] = None
    """UUID of the provider's UC metastore. This field is only present when the __authentication_type__
    is **DATABRICKS**."""

    name: Optional[str] = None
    """The name of the Provider."""

    owner: Optional[str] = None
    """Username of Provider owner."""

    recipient_profile: Optional[RecipientProfile] = None
    """The recipient profile. This field is only present when the authentication_type is `TOKEN` or
    `OAUTH_CLIENT_CREDENTIALS`."""

    recipient_profile_str: Optional[str] = None
    """This field is required when the __authentication_type__ is **TOKEN**,
    **OAUTH_CLIENT_CREDENTIALS** or not provided."""

    region: Optional[str] = None
    """Cloud region of the provider's UC metastore. This field is only present when the
    __authentication_type__ is **DATABRICKS**."""

    updated_at: Optional[int] = None
    """Time at which this Provider was created, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of user who last modified Provider."""

    def as_dict(self) -> dict:
        """Serializes the ProviderInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.authentication_type is not None:
            body["authentication_type"] = self.authentication_type.value
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_provider_global_metastore_id is not None:
            body["data_provider_global_metastore_id"] = self.data_provider_global_metastore_id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.recipient_profile:
            body["recipient_profile"] = self.recipient_profile.as_dict()
        if self.recipient_profile_str is not None:
            body["recipient_profile_str"] = self.recipient_profile_str
        if self.region is not None:
            body["region"] = self.region
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProviderInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.authentication_type is not None:
            body["authentication_type"] = self.authentication_type
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_provider_global_metastore_id is not None:
            body["data_provider_global_metastore_id"] = self.data_provider_global_metastore_id
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.recipient_profile:
            body["recipient_profile"] = self.recipient_profile
        if self.recipient_profile_str is not None:
            body["recipient_profile_str"] = self.recipient_profile_str
        if self.region is not None:
            body["region"] = self.region
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProviderInfo:
        """Deserializes the ProviderInfo from a dictionary."""
        return cls(
            authentication_type=_enum(d, "authentication_type", AuthenticationType),
            cloud=d.get("cloud", None),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            data_provider_global_metastore_id=d.get("data_provider_global_metastore_id", None),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            recipient_profile=_from_dict(d, "recipient_profile", RecipientProfile),
            recipient_profile_str=d.get("recipient_profile_str", None),
            region=d.get("region", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class ProviderShare:
    name: Optional[str] = None
    """The name of the Provider Share."""

    def as_dict(self) -> dict:
        """Serializes the ProviderShare into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProviderShare into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProviderShare:
        """Deserializes the ProviderShare from a dictionary."""
        return cls(name=d.get("name", None))


@dataclass
class RecipientInfo:
    activated: Optional[bool] = None
    """A boolean status field showing whether the Recipient's activation URL has been exercised or not."""

    activation_url: Optional[str] = None
    """Full activation url to retrieve the access token. It will be empty if the token is already
    retrieved."""

    authentication_type: Optional[AuthenticationType] = None

    cloud: Optional[str] = None
    """Cloud vendor of the recipient's Unity Catalog Metastore. This field is only present when the
    __authentication_type__ is **DATABRICKS**."""

    comment: Optional[str] = None
    """Description about the recipient."""

    created_at: Optional[int] = None
    """Time at which this recipient was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of recipient creator."""

    data_recipient_global_metastore_id: Optional[str] = None
    """The global Unity Catalog metastore id provided by the data recipient. This field is only present
    when the __authentication_type__ is **DATABRICKS**. The identifier is of format
    __cloud__:__region__:__metastore-uuid__."""

    expiration_time: Optional[int] = None
    """Expiration timestamp of the token, in epoch milliseconds."""

    id: Optional[str] = None
    """[Create,Update:IGN] common - id of the recipient"""

    ip_access_list: Optional[IpAccessList] = None
    """IP Access List"""

    metastore_id: Optional[str] = None
    """Unique identifier of recipient's Unity Catalog Metastore. This field is only present when the
    __authentication_type__ is **DATABRICKS**."""

    name: Optional[str] = None
    """Name of Recipient."""

    owner: Optional[str] = None
    """Username of the recipient owner."""

    properties_kvpairs: Optional[SecurablePropertiesKvPairs] = None
    """Recipient properties as map of string key-value pairs. When provided in update request, the
    specified properties will override the existing properties. To add and remove properties, one
    would need to perform a read-modify-write."""

    region: Optional[str] = None
    """Cloud region of the recipient's Unity Catalog Metastore. This field is only present when the
    __authentication_type__ is **DATABRICKS**."""

    sharing_code: Optional[str] = None
    """The one-time sharing code provided by the data recipient. This field is only present when the
    __authentication_type__ is **DATABRICKS**."""

    tokens: Optional[List[RecipientTokenInfo]] = None
    """This field is only present when the __authentication_type__ is **TOKEN**."""

    updated_at: Optional[int] = None
    """Time at which the recipient was updated, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of recipient updater."""

    def as_dict(self) -> dict:
        """Serializes the RecipientInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.activated is not None:
            body["activated"] = self.activated
        if self.activation_url is not None:
            body["activation_url"] = self.activation_url
        if self.authentication_type is not None:
            body["authentication_type"] = self.authentication_type.value
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_recipient_global_metastore_id is not None:
            body["data_recipient_global_metastore_id"] = self.data_recipient_global_metastore_id
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.id is not None:
            body["id"] = self.id
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list.as_dict()
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties_kvpairs:
            body["properties_kvpairs"] = self.properties_kvpairs.as_dict()
        if self.region is not None:
            body["region"] = self.region
        if self.sharing_code is not None:
            body["sharing_code"] = self.sharing_code
        if self.tokens:
            body["tokens"] = [v.as_dict() for v in self.tokens]
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RecipientInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.activated is not None:
            body["activated"] = self.activated
        if self.activation_url is not None:
            body["activation_url"] = self.activation_url
        if self.authentication_type is not None:
            body["authentication_type"] = self.authentication_type
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.data_recipient_global_metastore_id is not None:
            body["data_recipient_global_metastore_id"] = self.data_recipient_global_metastore_id
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.id is not None:
            body["id"] = self.id
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.name is not None:
            body["name"] = self.name
        if self.owner is not None:
            body["owner"] = self.owner
        if self.properties_kvpairs:
            body["properties_kvpairs"] = self.properties_kvpairs
        if self.region is not None:
            body["region"] = self.region
        if self.sharing_code is not None:
            body["sharing_code"] = self.sharing_code
        if self.tokens:
            body["tokens"] = self.tokens
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RecipientInfo:
        """Deserializes the RecipientInfo from a dictionary."""
        return cls(
            activated=d.get("activated", None),
            activation_url=d.get("activation_url", None),
            authentication_type=_enum(d, "authentication_type", AuthenticationType),
            cloud=d.get("cloud", None),
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            data_recipient_global_metastore_id=d.get("data_recipient_global_metastore_id", None),
            expiration_time=d.get("expiration_time", None),
            id=d.get("id", None),
            ip_access_list=_from_dict(d, "ip_access_list", IpAccessList),
            metastore_id=d.get("metastore_id", None),
            name=d.get("name", None),
            owner=d.get("owner", None),
            properties_kvpairs=_from_dict(d, "properties_kvpairs", SecurablePropertiesKvPairs),
            region=d.get("region", None),
            sharing_code=d.get("sharing_code", None),
            tokens=_repeated_dict(d, "tokens", RecipientTokenInfo),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class RecipientProfile:
    bearer_token: Optional[str] = None
    """The token used to authorize the recipient."""

    endpoint: Optional[str] = None
    """The endpoint for the share to be used by the recipient."""

    share_credentials_version: Optional[int] = None
    """The version number of the recipient's credentials on a share."""

    def as_dict(self) -> dict:
        """Serializes the RecipientProfile into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bearer_token is not None:
            body["bearer_token"] = self.bearer_token
        if self.endpoint is not None:
            body["endpoint"] = self.endpoint
        if self.share_credentials_version is not None:
            body["share_credentials_version"] = self.share_credentials_version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RecipientProfile into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bearer_token is not None:
            body["bearer_token"] = self.bearer_token
        if self.endpoint is not None:
            body["endpoint"] = self.endpoint
        if self.share_credentials_version is not None:
            body["share_credentials_version"] = self.share_credentials_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RecipientProfile:
        """Deserializes the RecipientProfile from a dictionary."""
        return cls(
            bearer_token=d.get("bearer_token", None),
            endpoint=d.get("endpoint", None),
            share_credentials_version=d.get("share_credentials_version", None),
        )


@dataclass
class RecipientTokenInfo:
    activation_url: Optional[str] = None
    """Full activation URL to retrieve the access token. It will be empty if the token is already
    retrieved."""

    created_at: Optional[int] = None
    """Time at which this recipient token was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of recipient token creator."""

    expiration_time: Optional[int] = None
    """Expiration timestamp of the token in epoch milliseconds."""

    id: Optional[str] = None
    """Unique ID of the recipient token."""

    updated_at: Optional[int] = None
    """Time at which this recipient token was updated, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of recipient token updater."""

    def as_dict(self) -> dict:
        """Serializes the RecipientTokenInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.activation_url is not None:
            body["activation_url"] = self.activation_url
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.id is not None:
            body["id"] = self.id
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RecipientTokenInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.activation_url is not None:
            body["activation_url"] = self.activation_url
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.id is not None:
            body["id"] = self.id
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RecipientTokenInfo:
        """Deserializes the RecipientTokenInfo from a dictionary."""
        return cls(
            activation_url=d.get("activation_url", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            expiration_time=d.get("expiration_time", None),
            id=d.get("id", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class RegisteredModelAlias:
    alias_name: Optional[str] = None
    """Name of the alias."""

    version_num: Optional[int] = None
    """Numeric model version that alias will reference."""

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelAlias into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alias_name is not None:
            body["alias_name"] = self.alias_name
        if self.version_num is not None:
            body["version_num"] = self.version_num
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelAlias into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alias_name is not None:
            body["alias_name"] = self.alias_name
        if self.version_num is not None:
            body["version_num"] = self.version_num
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelAlias:
        """Deserializes the RegisteredModelAlias from a dictionary."""
        return cls(alias_name=d.get("alias_name", None), version_num=d.get("version_num", None))


@dataclass
class RetrieveTokenResponse:
    bearer_token: Optional[str] = None
    """The token used to authorize the recipient."""

    endpoint: Optional[str] = None
    """The endpoint for the share to be used by the recipient."""

    expiration_time: Optional[str] = None
    """Expiration timestamp of the token in epoch milliseconds."""

    share_credentials_version: Optional[int] = None
    """These field names must follow the delta sharing protocol."""

    def as_dict(self) -> dict:
        """Serializes the RetrieveTokenResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bearer_token is not None:
            body["bearerToken"] = self.bearer_token
        if self.endpoint is not None:
            body["endpoint"] = self.endpoint
        if self.expiration_time is not None:
            body["expirationTime"] = self.expiration_time
        if self.share_credentials_version is not None:
            body["shareCredentialsVersion"] = self.share_credentials_version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RetrieveTokenResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bearer_token is not None:
            body["bearerToken"] = self.bearer_token
        if self.endpoint is not None:
            body["endpoint"] = self.endpoint
        if self.expiration_time is not None:
            body["expirationTime"] = self.expiration_time
        if self.share_credentials_version is not None:
            body["shareCredentialsVersion"] = self.share_credentials_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RetrieveTokenResponse:
        """Deserializes the RetrieveTokenResponse from a dictionary."""
        return cls(
            bearer_token=d.get("bearerToken", None),
            endpoint=d.get("endpoint", None),
            expiration_time=d.get("expirationTime", None),
            share_credentials_version=d.get("shareCredentialsVersion", None),
        )


@dataclass
class SecurablePropertiesKvPairs:
    """An object with __properties__ containing map of key-value properties attached to the securable."""

    properties: Dict[str, str]
    """A map of key-value properties attached to the securable."""

    def as_dict(self) -> dict:
        """Serializes the SecurablePropertiesKvPairs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.properties:
            body["properties"] = self.properties
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SecurablePropertiesKvPairs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.properties:
            body["properties"] = self.properties
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SecurablePropertiesKvPairs:
        """Deserializes the SecurablePropertiesKvPairs from a dictionary."""
        return cls(properties=d.get("properties", None))


@dataclass
class Share:
    id: Optional[str] = None

    name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the Share into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Share into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Share:
        """Deserializes the Share from a dictionary."""
        return cls(id=d.get("id", None), name=d.get("name", None))


@dataclass
class ShareInfo:
    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this share was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of share creator."""

    name: Optional[str] = None
    """Name of the share."""

    objects: Optional[List[SharedDataObject]] = None
    """A list of shared data objects within the share."""

    owner: Optional[str] = None
    """Username of current owner of share."""

    storage_location: Optional[str] = None
    """Storage Location URL (full path) for the share."""

    storage_root: Optional[str] = None
    """Storage root URL for the share."""

    updated_at: Optional[int] = None
    """Time at which this share was updated, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of share updater."""

    def as_dict(self) -> dict:
        """Serializes the ShareInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.name is not None:
            body["name"] = self.name
        if self.objects:
            body["objects"] = [v.as_dict() for v in self.objects]
        if self.owner is not None:
            body["owner"] = self.owner
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
        """Serializes the ShareInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.name is not None:
            body["name"] = self.name
        if self.objects:
            body["objects"] = self.objects
        if self.owner is not None:
            body["owner"] = self.owner
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
    def from_dict(cls, d: Dict[str, Any]) -> ShareInfo:
        """Deserializes the ShareInfo from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            name=d.get("name", None),
            objects=_repeated_dict(d, "objects", SharedDataObject),
            owner=d.get("owner", None),
            storage_location=d.get("storage_location", None),
            storage_root=d.get("storage_root", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class ShareToPrivilegeAssignment:
    privilege_assignments: Optional[List[PrivilegeAssignment]] = None
    """The privileges assigned to the principal."""

    share_name: Optional[str] = None
    """The share name."""

    def as_dict(self) -> dict:
        """Serializes the ShareToPrivilegeAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.privilege_assignments:
            body["privilege_assignments"] = [v.as_dict() for v in self.privilege_assignments]
        if self.share_name is not None:
            body["share_name"] = self.share_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ShareToPrivilegeAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.privilege_assignments:
            body["privilege_assignments"] = self.privilege_assignments
        if self.share_name is not None:
            body["share_name"] = self.share_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ShareToPrivilegeAssignment:
        """Deserializes the ShareToPrivilegeAssignment from a dictionary."""
        return cls(
            privilege_assignments=_repeated_dict(d, "privilege_assignments", PrivilegeAssignment),
            share_name=d.get("share_name", None),
        )


@dataclass
class SharedDataObject:
    name: str
    """A fully qualified name that uniquely identifies a data object. For example, a table's fully
    qualified name is in the format of `<catalog>.<schema>.<table>`,"""

    added_at: Optional[int] = None
    """The time when this data object is added to the share, in epoch milliseconds."""

    added_by: Optional[str] = None
    """Username of the sharer."""

    cdf_enabled: Optional[bool] = None
    """Whether to enable cdf or indicate if cdf is enabled on the shared object."""

    comment: Optional[str] = None
    """A user-provided comment when adding the data object to the share."""

    content: Optional[str] = None
    """The content of the notebook file when the data object type is NOTEBOOK_FILE. This should be
    base64 encoded. Required for adding a NOTEBOOK_FILE, optional for updating, ignored for other
    types."""

    data_object_type: Optional[SharedDataObjectDataObjectType] = None
    """The type of the data object."""

    history_data_sharing_status: Optional[SharedDataObjectHistoryDataSharingStatus] = None
    """Whether to enable or disable sharing of data history. If not specified, the default is
    **DISABLED**."""

    partitions: Optional[List[Partition]] = None
    """Array of partitions for the shared data."""

    shared_as: Optional[str] = None
    """A user-provided alias name for table-like data objects within the share.
    
    Use this field for table-like objects (for example: TABLE, VIEW, MATERIALIZED_VIEW,
    STREAMING_TABLE, FOREIGN_TABLE). For non-table objects (for example: VOLUME, MODEL,
    NOTEBOOK_FILE, FUNCTION), use `string_shared_as` instead.
    
    Important: For non-table objects, this field must be omitted entirely.
    
    Format: Must be a 2-part name `<schema_name>.<table_name>` (e.g., "sales_schema.orders_table") -
    Both schema and table names must contain only alphanumeric characters and underscores - No
    periods, spaces, forward slashes, or control characters are allowed within each part - Do not
    include the catalog name (use 2 parts, not 3)
    
    Behavior: - If not provided, the service automatically generates the alias as `<schema>.<table>`
    from the object's original name - If you don't want to specify this field, omit it entirely from
    the request (do not pass an empty string) - The `shared_as` name must be unique within the share
    
    Examples: - Valid: "analytics_schema.customer_view" - Invalid:
    "catalog.analytics_schema.customer_view" (3 parts not allowed) - Invalid:
    "analytics-schema.customer-view" (hyphens not allowed)"""

    start_version: Optional[int] = None
    """The start version associated with the object. This allows data providers to control the lowest
    object version that is accessible by clients. If specified, clients can query snapshots or
    changes for versions >= start_version. If not specified, clients can only query starting from
    the version of the object at the time it was added to the share.
    
    NOTE: The start_version should be <= the `current` version of the object."""

    status: Optional[SharedDataObjectStatus] = None
    """One of: **ACTIVE**, **PERMISSION_DENIED**."""

    string_shared_as: Optional[str] = None
    """A user-provided alias name for non-table data objects within the share.
    
    Use this field for non-table objects (for example: VOLUME, MODEL, NOTEBOOK_FILE, FUNCTION). For
    table-like objects (for example: TABLE, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE,
    FOREIGN_TABLE), use `shared_as` instead.
    
    Important: For table-like objects, this field must be omitted entirely.
    
    Format: - For VOLUME: Must be a 2-part name `<schema_name>.<volume_name>` (e.g.,
    "data_schema.ml_models") - For FUNCTION: Must be a 2-part name `<schema_name>.<function_name>`
    (e.g., "udf_schema.calculate_tax") - For MODEL: Must be a 2-part name
    `<schema_name>.<model_name>` (e.g., "models.prediction_model") - For NOTEBOOK_FILE: Should be
    the notebook file name (e.g., "analysis_notebook.py") - All names must contain only alphanumeric
    characters and underscores - No periods, spaces, forward slashes, or control characters are
    allowed within each part
    
    Behavior: - If not provided, the service automatically generates the alias from the object's
    original name - If you don't want to specify this field, omit it entirely from the request (do
    not pass an empty string) - The `string_shared_as` name must be unique for objects of the same
    type within the share
    
    Examples: - Valid for VOLUME: "data_schema.training_data" - Valid for FUNCTION:
    "analytics.calculate_revenue" - Invalid: "catalog.data_schema.training_data" (3 parts not
    allowed for volumes) - Invalid: "data-schema.training-data" (hyphens not allowed)"""

    def as_dict(self) -> dict:
        """Serializes the SharedDataObject into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.added_at is not None:
            body["added_at"] = self.added_at
        if self.added_by is not None:
            body["added_by"] = self.added_by
        if self.cdf_enabled is not None:
            body["cdf_enabled"] = self.cdf_enabled
        if self.comment is not None:
            body["comment"] = self.comment
        if self.content is not None:
            body["content"] = self.content
        if self.data_object_type is not None:
            body["data_object_type"] = self.data_object_type.value
        if self.history_data_sharing_status is not None:
            body["history_data_sharing_status"] = self.history_data_sharing_status.value
        if self.name is not None:
            body["name"] = self.name
        if self.partitions:
            body["partitions"] = [v.as_dict() for v in self.partitions]
        if self.shared_as is not None:
            body["shared_as"] = self.shared_as
        if self.start_version is not None:
            body["start_version"] = self.start_version
        if self.status is not None:
            body["status"] = self.status.value
        if self.string_shared_as is not None:
            body["string_shared_as"] = self.string_shared_as
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SharedDataObject into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.added_at is not None:
            body["added_at"] = self.added_at
        if self.added_by is not None:
            body["added_by"] = self.added_by
        if self.cdf_enabled is not None:
            body["cdf_enabled"] = self.cdf_enabled
        if self.comment is not None:
            body["comment"] = self.comment
        if self.content is not None:
            body["content"] = self.content
        if self.data_object_type is not None:
            body["data_object_type"] = self.data_object_type
        if self.history_data_sharing_status is not None:
            body["history_data_sharing_status"] = self.history_data_sharing_status
        if self.name is not None:
            body["name"] = self.name
        if self.partitions:
            body["partitions"] = self.partitions
        if self.shared_as is not None:
            body["shared_as"] = self.shared_as
        if self.start_version is not None:
            body["start_version"] = self.start_version
        if self.status is not None:
            body["status"] = self.status
        if self.string_shared_as is not None:
            body["string_shared_as"] = self.string_shared_as
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SharedDataObject:
        """Deserializes the SharedDataObject from a dictionary."""
        return cls(
            added_at=d.get("added_at", None),
            added_by=d.get("added_by", None),
            cdf_enabled=d.get("cdf_enabled", None),
            comment=d.get("comment", None),
            content=d.get("content", None),
            data_object_type=_enum(d, "data_object_type", SharedDataObjectDataObjectType),
            history_data_sharing_status=_enum(
                d, "history_data_sharing_status", SharedDataObjectHistoryDataSharingStatus
            ),
            name=d.get("name", None),
            partitions=_repeated_dict(d, "partitions", Partition),
            shared_as=d.get("shared_as", None),
            start_version=d.get("start_version", None),
            status=_enum(d, "status", SharedDataObjectStatus),
            string_shared_as=d.get("string_shared_as", None),
        )


class SharedDataObjectDataObjectType(Enum):

    FEATURE_SPEC = "FEATURE_SPEC"
    FOREIGN_TABLE = "FOREIGN_TABLE"
    FUNCTION = "FUNCTION"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    MODEL = "MODEL"
    NOTEBOOK_FILE = "NOTEBOOK_FILE"
    SCHEMA = "SCHEMA"
    STREAMING_TABLE = "STREAMING_TABLE"
    TABLE = "TABLE"
    VIEW = "VIEW"
    VOLUME = "VOLUME"


class SharedDataObjectHistoryDataSharingStatus(Enum):

    DISABLED = "DISABLED"
    ENABLED = "ENABLED"


class SharedDataObjectStatus(Enum):

    ACTIVE = "ACTIVE"
    PERMISSION_DENIED = "PERMISSION_DENIED"


@dataclass
class SharedDataObjectUpdate:
    action: Optional[SharedDataObjectUpdateAction] = None
    """One of: **ADD**, **REMOVE**, **UPDATE**."""

    data_object: Optional[SharedDataObject] = None
    """The data object that is being added, removed, or updated. The maximum number update data objects
    allowed is a 100."""

    def as_dict(self) -> dict:
        """Serializes the SharedDataObjectUpdate into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.action is not None:
            body["action"] = self.action.value
        if self.data_object:
            body["data_object"] = self.data_object.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SharedDataObjectUpdate into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.action is not None:
            body["action"] = self.action
        if self.data_object:
            body["data_object"] = self.data_object
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SharedDataObjectUpdate:
        """Deserializes the SharedDataObjectUpdate from a dictionary."""
        return cls(
            action=_enum(d, "action", SharedDataObjectUpdateAction),
            data_object=_from_dict(d, "data_object", SharedDataObject),
        )


class SharedDataObjectUpdateAction(Enum):

    ADD = "ADD"
    REMOVE = "REMOVE"
    UPDATE = "UPDATE"


class SharedSecurableKind(Enum):
    """The SecurableKind of a delta-shared object."""

    FUNCTION_FEATURE_SPEC = "FUNCTION_FEATURE_SPEC"
    FUNCTION_REGISTERED_MODEL = "FUNCTION_REGISTERED_MODEL"
    FUNCTION_STANDARD = "FUNCTION_STANDARD"


@dataclass
class Table:
    comment: Optional[str] = None
    """The comment of the table."""

    id: Optional[str] = None
    """The id of the table."""

    materialization_namespace: Optional[str] = None
    """The catalog and schema of the materialized table"""

    materialized_table_name: Optional[str] = None
    """The name of a materialized table."""

    name: Optional[str] = None
    """The name of the table."""

    schema: Optional[str] = None
    """The name of the schema that the table belongs to."""

    share: Optional[str] = None
    """The name of the share that the table belongs to."""

    share_id: Optional[str] = None
    """The id of the share that the table belongs to."""

    tags: Optional[List[catalog.TagKeyValue]] = None
    """The Tags of the table."""

    def as_dict(self) -> dict:
        """Serializes the Table into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.id is not None:
            body["id"] = self.id
        if self.materialization_namespace is not None:
            body["materialization_namespace"] = self.materialization_namespace
        if self.materialized_table_name is not None:
            body["materialized_table_name"] = self.materialized_table_name
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Table into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.id is not None:
            body["id"] = self.id
        if self.materialization_namespace is not None:
            body["materialization_namespace"] = self.materialization_namespace
        if self.materialized_table_name is not None:
            body["materialized_table_name"] = self.materialized_table_name
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Table:
        """Deserializes the Table from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            id=d.get("id", None),
            materialization_namespace=d.get("materialization_namespace", None),
            materialized_table_name=d.get("materialized_table_name", None),
            name=d.get("name", None),
            schema=d.get("schema", None),
            share=d.get("share", None),
            share_id=d.get("share_id", None),
            tags=_repeated_dict(d, "tags", catalog.TagKeyValue),
        )


@dataclass
class UpdateSharePermissionsResponse:
    privilege_assignments: Optional[List[PrivilegeAssignment]] = None
    """The privileges assigned to each principal"""

    def as_dict(self) -> dict:
        """Serializes the UpdateSharePermissionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.privilege_assignments:
            body["privilege_assignments"] = [v.as_dict() for v in self.privilege_assignments]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateSharePermissionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.privilege_assignments:
            body["privilege_assignments"] = self.privilege_assignments
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateSharePermissionsResponse:
        """Deserializes the UpdateSharePermissionsResponse from a dictionary."""
        return cls(privilege_assignments=_repeated_dict(d, "privilege_assignments", PrivilegeAssignment))


@dataclass
class Volume:
    comment: Optional[str] = None
    """The comment of the volume."""

    id: Optional[str] = None
    """This id maps to the shared_volume_id in database Recipient needs shared_volume_id for recon to
    check if this volume is already in recipient's DB or not."""

    name: Optional[str] = None
    """The name of the volume."""

    schema: Optional[str] = None
    """The name of the schema that the volume belongs to."""

    share: Optional[str] = None
    """The name of the share that the volume belongs to."""

    share_id: Optional[str] = None
    """/ The id of the share that the volume belongs to."""

    tags: Optional[List[catalog.TagKeyValue]] = None
    """The tags of the volume."""

    def as_dict(self) -> dict:
        """Serializes the Volume into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Volume into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        if self.share is not None:
            body["share"] = self.share
        if self.share_id is not None:
            body["share_id"] = self.share_id
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Volume:
        """Deserializes the Volume from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            id=d.get("id", None),
            name=d.get("name", None),
            schema=d.get("schema", None),
            share=d.get("share", None),
            share_id=d.get("share_id", None),
            tags=_repeated_dict(d, "tags", catalog.TagKeyValue),
        )


class ProvidersAPI:
    """A data provider is an object representing the organization in the real world who shares the data. A
    provider contains shares which further contain the shared data."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        authentication_type: AuthenticationType,
        *,
        comment: Optional[str] = None,
        recipient_profile_str: Optional[str] = None,
    ) -> ProviderInfo:
        """Creates a new authentication provider minimally based on a name and authentication type. The caller
        must be an admin on the metastore.

        :param name: str
          The name of the Provider.
        :param authentication_type: :class:`AuthenticationType`
        :param comment: str (optional)
          Description about the provider.
        :param recipient_profile_str: str (optional)
          This field is required when the __authentication_type__ is **TOKEN**, **OAUTH_CLIENT_CREDENTIALS**
          or not provided.

        :returns: :class:`ProviderInfo`
        """

        body = {}
        if authentication_type is not None:
            body["authentication_type"] = authentication_type.value
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if recipient_profile_str is not None:
            body["recipient_profile_str"] = recipient_profile_str
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/providers", body=body, headers=headers)
        return ProviderInfo.from_dict(res)

    def delete(self, name: str):
        """Deletes an authentication provider, if the caller is a metastore admin or is the owner of the
        provider.

        :param name: str
          Name of the provider.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/providers/{name}", headers=headers)

    def get(self, name: str) -> ProviderInfo:
        """Gets a specific authentication provider. The caller must supply the name of the provider, and must
        either be a metastore admin or the owner of the provider.

        :param name: str
          Name of the provider.

        :returns: :class:`ProviderInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/providers/{name}", headers=headers)
        return ProviderInfo.from_dict(res)

    def list(
        self,
        *,
        data_provider_global_metastore_id: Optional[str] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[ProviderInfo]:
        """Gets an array of available authentication providers. The caller must either be a metastore admin, have
        the **USE_PROVIDER** privilege on the providers, or be the owner of the providers. Providers not owned
        by the caller and for which the caller does not have the **USE_PROVIDER** privilege are not included
        in the response. There is no guarantee of a specific ordering of the elements in the array.

        :param data_provider_global_metastore_id: str (optional)
          If not provided, all providers will be returned. If no providers exist with this ID, no results will
          be returned.
        :param max_results: int (optional)
          Maximum number of providers to return. - when set to 0, the page length is set to a server
          configured value (recommended); - when set to a value greater than 0, the page length is the minimum
          of this value and a server configured value; - when set to a value less than 0, an invalid parameter
          error is returned; - If not set, all valid providers are returned (not recommended). - Note: The
          number of returned providers might be less than the specified max_results size, even zero. The only
          definitive indication that no further providers can be fetched is when the next_page_token is unset
          from the response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ProviderInfo`
        """

        query = {}
        if data_provider_global_metastore_id is not None:
            query["data_provider_global_metastore_id"] = data_provider_global_metastore_id
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
            json = self._api.do("GET", "/api/2.1/unity-catalog/providers", query=query, headers=headers)
            if "providers" in json:
                for v in json["providers"]:
                    yield ProviderInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_provider_share_assets(
        self,
        provider_name: str,
        share_name: str,
        *,
        function_max_results: Optional[int] = None,
        notebook_max_results: Optional[int] = None,
        table_max_results: Optional[int] = None,
        volume_max_results: Optional[int] = None,
    ) -> ListProviderShareAssetsResponse:
        """Get arrays of assets associated with a specified provider's share. The caller is the recipient of the
        share.

        :param provider_name: str
          The name of the provider who owns the share.
        :param share_name: str
          The name of the share.
        :param function_max_results: int (optional)
          Maximum number of functions to return.
        :param notebook_max_results: int (optional)
          Maximum number of notebooks to return.
        :param table_max_results: int (optional)
          Maximum number of tables to return.
        :param volume_max_results: int (optional)
          Maximum number of volumes to return.

        :returns: :class:`ListProviderShareAssetsResponse`
        """

        query = {}
        if function_max_results is not None:
            query["function_max_results"] = function_max_results
        if notebook_max_results is not None:
            query["notebook_max_results"] = notebook_max_results
        if table_max_results is not None:
            query["table_max_results"] = table_max_results
        if volume_max_results is not None:
            query["volume_max_results"] = volume_max_results
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.1/data-sharing/providers/{provider_name}/shares/{share_name}", query=query, headers=headers
        )
        return ListProviderShareAssetsResponse.from_dict(res)

    def list_shares(
        self, name: str, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ProviderShare]:
        """Gets an array of a specified provider's shares within the metastore where:

        * the caller is a metastore admin, or * the caller is the owner.

        :param name: str
          Name of the provider in which to list shares.
        :param max_results: int (optional)
          Maximum number of shares to return. - when set to 0, the page length is set to a server configured
          value (recommended); - when set to a value greater than 0, the page length is the minimum of this
          value and a server configured value; - when set to a value less than 0, an invalid parameter error
          is returned; - If not set, all valid shares are returned (not recommended). - Note: The number of
          returned shares might be less than the specified max_results size, even zero. The only definitive
          indication that no further shares can be fetched is when the next_page_token is unset from the
          response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ProviderShare`
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
            json = self._api.do("GET", f"/api/2.1/unity-catalog/providers/{name}/shares", query=query, headers=headers)
            if "shares" in json:
                for v in json["shares"]:
                    yield ProviderShare.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        name: str,
        *,
        comment: Optional[str] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        recipient_profile_str: Optional[str] = None,
    ) -> ProviderInfo:
        """Updates the information for an authentication provider, if the caller is a metastore admin or is the
        owner of the provider. If the update changes the provider name, the caller must be both a metastore
        admin and the owner of the provider.

        :param name: str
          Name of the provider.
        :param comment: str (optional)
          Description about the provider.
        :param new_name: str (optional)
          New name for the provider.
        :param owner: str (optional)
          Username of Provider owner.
        :param recipient_profile_str: str (optional)
          This field is required when the __authentication_type__ is **TOKEN**, **OAUTH_CLIENT_CREDENTIALS**
          or not provided.

        :returns: :class:`ProviderInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if recipient_profile_str is not None:
            body["recipient_profile_str"] = recipient_profile_str
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/providers/{name}", body=body, headers=headers)
        return ProviderInfo.from_dict(res)


class RecipientActivationAPI:
    """The Recipient Activation API is only applicable in the open sharing model where the recipient object has
    the authentication type of `TOKEN`. The data recipient follows the activation link shared by the data
    provider to download the credential file that includes the access token. The recipient will then use the
    credential file to establish a secure connection with the provider to receive the shared data.

    Note that you can download the credential file only once. Recipients should treat the downloaded
    credential as a secret and must not share it outside of their organization."""

    def __init__(self, api_client):
        self._api = api_client

    def get_activation_url_info(self, activation_url: str):
        """Gets an activation URL for a share.

        :param activation_url: str
          The one time activation url. It also accepts activation token.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "GET", f"/api/2.1/unity-catalog/public/data_sharing_activation_info/{activation_url}", headers=headers
        )

    def retrieve_token(self, activation_url: str) -> RetrieveTokenResponse:
        """Retrieve access token with an activation url. This is a public API without any authentication.

        :param activation_url: str
          The one time activation url. It also accepts activation token.

        :returns: :class:`RetrieveTokenResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.1/unity-catalog/public/data_sharing_activation/{activation_url}", headers=headers
        )
        return RetrieveTokenResponse.from_dict(res)


class RecipientFederationPoliciesAPI:
    """The Recipient Federation Policies APIs are only applicable in the open sharing model where the recipient
    object has the authentication type of `OIDC_RECIPIENT`, enabling data sharing from Databricks to
    non-Databricks recipients. OIDC Token Federation enables secure, secret-less authentication for accessing
    Delta Sharing servers. Users and applications authenticate using short-lived OIDC tokens issued by their
    own Identity Provider (IdP), such as Azure Entra ID or Okta, without the need for managing static
    credentials or client secrets. A federation policy defines how non-Databricks recipients authenticate
    using OIDC tokens. It validates the OIDC claims in federated tokens and is set at the recipient level. The
    caller must be the owner of the recipient to create or manage a federation policy. Federation policies
    support the following scenarios: - User-to-Machine (U2M) flow: A user accesses Delta Shares using their
    own identity, such as connecting through PowerBI Delta Sharing Client. - Machine-to-Machine (M2M) flow: An
    application accesses Delta Shares using its own identity, typically for automation tasks like nightly jobs
    through Python Delta Sharing Client. OIDC Token Federation enables fine-grained access control, supports
    Multi-Factor Authentication (MFA), and enhances security by minimizing the risk of credential leakage
    through the use of short-lived, expiring tokens. It is designed for strong identity governance, secure
    cross-platform data sharing, and reduced operational overhead for credential management.

    For more information, see
    https://www.databricks.com/blog/announcing-oidc-token-federation-enhanced-delta-sharing-security and
    https://docs.databricks.com/en/delta-sharing/create-recipient-oidc-fed"""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, recipient_name: str, policy: FederationPolicy) -> FederationPolicy:
        """Create a federation policy for an OIDC_FEDERATION recipient for sharing data from Databricks to
        non-Databricks recipients. The caller must be the owner of the recipient. When sharing data from
        Databricks to non-Databricks clients, you can define a federation policy to authenticate
        non-Databricks recipients. The federation policy validates OIDC claims in federated tokens and is
        defined at the recipient level. This enables secretless sharing clients to authenticate using OIDC
        tokens.

        Supported scenarios for federation policies: 1. **User-to-Machine (U2M) flow** (e.g., PowerBI): A user
        accesses a resource using their own identity. 2. **Machine-to-Machine (M2M) flow** (e.g., OAuth App):
        An OAuth App accesses a resource using its own identity, typically for tasks like running nightly
        jobs.

        For an overview, refer to: - Blog post: Overview of feature:
        https://www.databricks.com/blog/announcing-oidc-token-federation-enhanced-delta-sharing-security

        For detailed configuration guides based on your use case: - Creating a Federation Policy as a
        provider: https://docs.databricks.com/en/delta-sharing/create-recipient-oidc-fed - Configuration and
        usage for Machine-to-Machine (M2M) applications (e.g., Python Delta Sharing Client):
        https://docs.databricks.com/aws/en/delta-sharing/sharing-over-oidc-m2m - Configuration and usage for
        User-to-Machine (U2M) applications (e.g., PowerBI):
        https://docs.databricks.com/aws/en/delta-sharing/sharing-over-oidc-u2m

        :param recipient_name: str
          Name of the recipient. This is the name of the recipient for which the policy is being created.
        :param policy: :class:`FederationPolicy`
          Name of the policy. This is the name of the policy to be created.

        :returns: :class:`FederationPolicy`
        """

        body = policy.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", f"/api/2.0/data-sharing/recipients/{recipient_name}/federation-policies", body=body, headers=headers
        )
        return FederationPolicy.from_dict(res)

    def delete(self, recipient_name: str, name: str):
        """Deletes an existing federation policy for an OIDC_FEDERATION recipient. The caller must be the owner
        of the recipient.

        :param recipient_name: str
          Name of the recipient. This is the name of the recipient for which the policy is being deleted.
        :param name: str
          Name of the policy. This is the name of the policy to be deleted.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/2.0/data-sharing/recipients/{recipient_name}/federation-policies/{name}", headers=headers
        )

    def get_federation_policy(self, recipient_name: str, name: str) -> FederationPolicy:
        """Reads an existing federation policy for an OIDC_FEDERATION recipient for sharing data from Databricks
        to non-Databricks recipients. The caller must have read access to the recipient.

        :param recipient_name: str
          Name of the recipient. This is the name of the recipient for which the policy is being retrieved.
        :param name: str
          Name of the policy. This is the name of the policy to be retrieved.

        :returns: :class:`FederationPolicy`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/data-sharing/recipients/{recipient_name}/federation-policies/{name}", headers=headers
        )
        return FederationPolicy.from_dict(res)

    def list(
        self, recipient_name: str, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[FederationPolicy]:
        """Lists federation policies for an OIDC_FEDERATION recipient for sharing data from Databricks to
        non-Databricks recipients. The caller must have read access to the recipient.

        :param recipient_name: str
          Name of the recipient. This is the name of the recipient for which the policies are being listed.
        :param max_results: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`FederationPolicy`
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
                f"/api/2.0/data-sharing/recipients/{recipient_name}/federation-policies",
                query=query,
                headers=headers,
            )
            if "policies" in json:
                for v in json["policies"]:
                    yield FederationPolicy.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class RecipientsAPI:
    """A recipient is an object you create using :method:recipients/create to represent an organization which you
    want to allow access shares. The way how sharing works differs depending on whether or not your recipient
    has access to a Databricks workspace that is enabled for Unity Catalog:

    - For recipients with access to a Databricks workspace that is enabled for Unity Catalog, you can create a
    recipient object along with a unique sharing identifier you get from the recipient. The sharing identifier
    is the key identifier that enables the secure connection. This sharing mode is called
    **Databricks-to-Databricks sharing**.

    - For recipients without access to a Databricks workspace that is enabled for Unity Catalog, when you
    create a recipient object, Databricks generates an activation link you can send to the recipient. The
    recipient follows the activation link to download the credential file, and then uses the credential file
    to establish a secure connection to receive the shared data. This sharing mode is called **open sharing**."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        authentication_type: AuthenticationType,
        *,
        comment: Optional[str] = None,
        data_recipient_global_metastore_id: Optional[str] = None,
        expiration_time: Optional[int] = None,
        id: Optional[str] = None,
        ip_access_list: Optional[IpAccessList] = None,
        owner: Optional[str] = None,
        properties_kvpairs: Optional[SecurablePropertiesKvPairs] = None,
        sharing_code: Optional[str] = None,
    ) -> RecipientInfo:
        """Creates a new recipient with the delta sharing authentication type in the metastore. The caller must
        be a metastore admin or have the **CREATE_RECIPIENT** privilege on the metastore.

        :param name: str
          Name of Recipient.
        :param authentication_type: :class:`AuthenticationType`
        :param comment: str (optional)
          Description about the recipient.
        :param data_recipient_global_metastore_id: str (optional)
          The global Unity Catalog metastore id provided by the data recipient. This field is only present
          when the __authentication_type__ is **DATABRICKS**. The identifier is of format
          __cloud__:__region__:__metastore-uuid__.
        :param expiration_time: int (optional)
          Expiration timestamp of the token, in epoch milliseconds.
        :param id: str (optional)
          [Create,Update:IGN] common - id of the recipient
        :param ip_access_list: :class:`IpAccessList` (optional)
          IP Access List
        :param owner: str (optional)
          Username of the recipient owner.
        :param properties_kvpairs: :class:`SecurablePropertiesKvPairs` (optional)
          Recipient properties as map of string key-value pairs. When provided in update request, the
          specified properties will override the existing properties. To add and remove properties, one would
          need to perform a read-modify-write.
        :param sharing_code: str (optional)
          The one-time sharing code provided by the data recipient. This field is only present when the
          __authentication_type__ is **DATABRICKS**.

        :returns: :class:`RecipientInfo`
        """

        body = {}
        if authentication_type is not None:
            body["authentication_type"] = authentication_type.value
        if comment is not None:
            body["comment"] = comment
        if data_recipient_global_metastore_id is not None:
            body["data_recipient_global_metastore_id"] = data_recipient_global_metastore_id
        if expiration_time is not None:
            body["expiration_time"] = expiration_time
        if id is not None:
            body["id"] = id
        if ip_access_list is not None:
            body["ip_access_list"] = ip_access_list.as_dict()
        if name is not None:
            body["name"] = name
        if owner is not None:
            body["owner"] = owner
        if properties_kvpairs is not None:
            body["properties_kvpairs"] = properties_kvpairs.as_dict()
        if sharing_code is not None:
            body["sharing_code"] = sharing_code
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/recipients", body=body, headers=headers)
        return RecipientInfo.from_dict(res)

    def delete(self, name: str):
        """Deletes the specified recipient from the metastore. The caller must be the owner of the recipient.

        :param name: str
          Name of the recipient.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/recipients/{name}", headers=headers)

    def get(self, name: str) -> RecipientInfo:
        """Gets a share recipient from the metastore. The caller must be one of: * A user with **USE_RECIPIENT**
        privilege on the metastore * The owner of the share recipient * A metastore admin

        :param name: str
          Name of the recipient.

        :returns: :class:`RecipientInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/recipients/{name}", headers=headers)
        return RecipientInfo.from_dict(res)

    def list(
        self,
        *,
        data_recipient_global_metastore_id: Optional[str] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[RecipientInfo]:
        """Gets an array of all share recipients within the current metastore where:

        * the caller is a metastore admin, or * the caller is the owner. There is no guarantee of a specific
        ordering of the elements in the array.

        :param data_recipient_global_metastore_id: str (optional)
          If not provided, all recipients will be returned. If no recipients exist with this ID, no results
          will be returned.
        :param max_results: int (optional)
          Maximum number of recipients to return. - when set to 0, the page length is set to a server
          configured value (recommended); - when set to a value greater than 0, the page length is the minimum
          of this value and a server configured value; - when set to a value less than 0, an invalid parameter
          error is returned; - If not set, all valid recipients are returned (not recommended). - Note: The
          number of returned recipients might be less than the specified max_results size, even zero. The only
          definitive indication that no further recipients can be fetched is when the next_page_token is unset
          from the response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`RecipientInfo`
        """

        query = {}
        if data_recipient_global_metastore_id is not None:
            query["data_recipient_global_metastore_id"] = data_recipient_global_metastore_id
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
            json = self._api.do("GET", "/api/2.1/unity-catalog/recipients", query=query, headers=headers)
            if "recipients" in json:
                for v in json["recipients"]:
                    yield RecipientInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def rotate_token(self, name: str, existing_token_expire_in_seconds: int) -> RecipientInfo:
        """Refreshes the specified recipient's delta sharing authentication token with the provided token info.
        The caller must be the owner of the recipient.

        :param name: str
          The name of the Recipient.
        :param existing_token_expire_in_seconds: int
          The expiration time of the bearer token in ISO 8601 format. This will set the expiration_time of
          existing token only to a smaller timestamp, it cannot extend the expiration_time. Use 0 to expire
          the existing token immediately, negative number will return an error.

        :returns: :class:`RecipientInfo`
        """

        body = {}
        if existing_token_expire_in_seconds is not None:
            body["existing_token_expire_in_seconds"] = existing_token_expire_in_seconds
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.1/unity-catalog/recipients/{name}/rotate-token", body=body, headers=headers)
        return RecipientInfo.from_dict(res)

    def share_permissions(
        self, name: str, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> GetRecipientSharePermissionsResponse:
        """Gets the share permissions for the specified Recipient. The caller must have the **USE_RECIPIENT**
        privilege on the metastore or be the owner of the Recipient.

        :param name: str
          The name of the Recipient.
        :param max_results: int (optional)
          Maximum number of permissions to return. - when set to 0, the page length is set to a server
          configured value (recommended); - when set to a value greater than 0, the page length is the minimum
          of this value and a server configured value; - when set to a value less than 0, an invalid parameter
          error is returned; - If not set, all valid permissions are returned (not recommended). - Note: The
          number of returned permissions might be less than the specified max_results size, even zero. The
          only definitive indication that no further permissions can be fetched is when the next_page_token is
          unset from the response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: :class:`GetRecipientSharePermissionsResponse`
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

        res = self._api.do(
            "GET", f"/api/2.1/unity-catalog/recipients/{name}/share-permissions", query=query, headers=headers
        )
        return GetRecipientSharePermissionsResponse.from_dict(res)

    def update(
        self,
        name: str,
        *,
        comment: Optional[str] = None,
        expiration_time: Optional[int] = None,
        id: Optional[str] = None,
        ip_access_list: Optional[IpAccessList] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        properties_kvpairs: Optional[SecurablePropertiesKvPairs] = None,
    ) -> RecipientInfo:
        """Updates an existing recipient in the metastore. The caller must be a metastore admin or the owner of
        the recipient. If the recipient name will be updated, the user must be both a metastore admin and the
        owner of the recipient.

        :param name: str
          Name of the recipient.
        :param comment: str (optional)
          Description about the recipient.
        :param expiration_time: int (optional)
          Expiration timestamp of the token, in epoch milliseconds.
        :param id: str (optional)
          [Create,Update:IGN] common - id of the recipient
        :param ip_access_list: :class:`IpAccessList` (optional)
          IP Access List
        :param new_name: str (optional)
          New name for the recipient. .
        :param owner: str (optional)
          Username of the recipient owner.
        :param properties_kvpairs: :class:`SecurablePropertiesKvPairs` (optional)
          Recipient properties as map of string key-value pairs. When provided in update request, the
          specified properties will override the existing properties. To add and remove properties, one would
          need to perform a read-modify-write.

        :returns: :class:`RecipientInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if expiration_time is not None:
            body["expiration_time"] = expiration_time
        if id is not None:
            body["id"] = id
        if ip_access_list is not None:
            body["ip_access_list"] = ip_access_list.as_dict()
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if properties_kvpairs is not None:
            body["properties_kvpairs"] = properties_kvpairs.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/recipients/{name}", body=body, headers=headers)
        return RecipientInfo.from_dict(res)


class SharesAPI:
    """A share is a container instantiated with :method:shares/create. Once created you can iteratively register
    a collection of existing data assets defined within the metastore using :method:shares/update. You can
    register data assets under their original name, qualified by their original schema, or provide alternate
    exposed names."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, name: str, *, comment: Optional[str] = None, storage_root: Optional[str] = None) -> ShareInfo:
        """Creates a new share for data objects. Data objects can be added after creation with **update**. The
        caller must be a metastore admin or have the **CREATE_SHARE** privilege on the metastore.

        :param name: str
          Name of the share.
        :param comment: str (optional)
          User-provided free-form text description.
        :param storage_root: str (optional)
          Storage root URL for the share.

        :returns: :class:`ShareInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if storage_root is not None:
            body["storage_root"] = storage_root
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/unity-catalog/shares", body=body, headers=headers)
        return ShareInfo.from_dict(res)

    def delete(self, name: str):
        """Deletes a data object share from the metastore. The caller must be an owner of the share.

        :param name: str
          The name of the share.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/unity-catalog/shares/{name}", headers=headers)

    def get(self, name: str, *, include_shared_data: Optional[bool] = None) -> ShareInfo:
        """Gets a data object share from the metastore. The caller must have the USE_SHARE privilege on the
        metastore or be the owner of the share.

        :param name: str
          The name of the share.
        :param include_shared_data: bool (optional)
          Query for data to include in the share.

        :returns: :class:`ShareInfo`
        """

        query = {}
        if include_shared_data is not None:
            query["include_shared_data"] = include_shared_data
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/unity-catalog/shares/{name}", query=query, headers=headers)
        return ShareInfo.from_dict(res)

    def list_shares(
        self, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ShareInfo]:
        """Gets an array of data object shares from the metastore. If the caller has the USE_SHARE privilege on
        the metastore, all shares are returned. Otherwise, only shares owned by the caller are returned. There
        is no guarantee of a specific ordering of the elements in the array.

        :param max_results: int (optional)
          Maximum number of shares to return. - when set to 0, the page length is set to a server configured
          value (recommended); - when set to a value greater than 0, the page length is the minimum of this
          value and a server configured value; - when set to a value less than 0, an invalid parameter error
          is returned; - If not set, all valid shares are returned (not recommended). - Note: The number of
          returned shares might be less than the specified max_results size, even zero. The only definitive
          indication that no further shares can be fetched is when the next_page_token is unset from the
          response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ShareInfo`
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
            json = self._api.do("GET", "/api/2.1/unity-catalog/shares", query=query, headers=headers)
            if "shares" in json:
                for v in json["shares"]:
                    yield ShareInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def share_permissions(
        self, name: str, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> GetSharePermissionsResponse:
        """Gets the permissions for a data share from the metastore. The caller must have the USE_SHARE privilege
        on the metastore or be the owner of the share.

        :param name: str
          The name of the Recipient.
        :param max_results: int (optional)
          Maximum number of permissions to return. - when set to 0, the page length is set to a server
          configured value (recommended); - when set to a value greater than 0, the page length is the minimum
          of this value and a server configured value; - when set to a value less than 0, an invalid parameter
          error is returned; - If not set, all valid permissions are returned (not recommended). - Note: The
          number of returned permissions might be less than the specified max_results size, even zero. The
          only definitive indication that no further permissions can be fetched is when the next_page_token is
          unset from the response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: :class:`GetSharePermissionsResponse`
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

        res = self._api.do("GET", f"/api/2.1/unity-catalog/shares/{name}/permissions", query=query, headers=headers)
        return GetSharePermissionsResponse.from_dict(res)

    def update(
        self,
        name: str,
        *,
        comment: Optional[str] = None,
        new_name: Optional[str] = None,
        owner: Optional[str] = None,
        storage_root: Optional[str] = None,
        updates: Optional[List[SharedDataObjectUpdate]] = None,
    ) -> ShareInfo:
        """Updates the share with the changes and data objects in the request. The caller must be the owner of
        the share or a metastore admin.

        When the caller is a metastore admin, only the __owner__ field can be updated.

        In the case the share name is changed, **updateShare** requires that the caller is the owner of the
        share and has the CREATE_SHARE privilege.

        If there are notebook files in the share, the __storage_root__ field cannot be updated.

        For each table that is added through this method, the share owner must also have **SELECT** privilege
        on the table. This privilege must be maintained indefinitely for recipients to be able to access the
        table. Typically, you should use a group as the share owner.

        Table removals through **update** do not require additional privileges.

        :param name: str
          The name of the share.
        :param comment: str (optional)
          User-provided free-form text description.
        :param new_name: str (optional)
          New name for the share.
        :param owner: str (optional)
          Username of current owner of share.
        :param storage_root: str (optional)
          Storage root URL for the share.
        :param updates: List[:class:`SharedDataObjectUpdate`] (optional)
          Array of shared data object updates.

        :returns: :class:`ShareInfo`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if new_name is not None:
            body["new_name"] = new_name
        if owner is not None:
            body["owner"] = owner
        if storage_root is not None:
            body["storage_root"] = storage_root
        if updates is not None:
            body["updates"] = [v.as_dict() for v in updates]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/shares/{name}", body=body, headers=headers)
        return ShareInfo.from_dict(res)

    def update_permissions(
        self,
        name: str,
        *,
        changes: Optional[List[PermissionsChange]] = None,
        omit_permissions_list: Optional[bool] = None,
    ) -> UpdateSharePermissionsResponse:
        """Updates the permissions for a data share in the metastore. The caller must have both the USE_SHARE and
        SET_SHARE_PERMISSION privileges on the metastore, or be the owner of the share.

        For new recipient grants, the user must also be the owner of the recipients. recipient revocations do
        not require additional privileges.

        :param name: str
          The name of the share.
        :param changes: List[:class:`PermissionsChange`] (optional)
          Array of permissions change objects.
        :param omit_permissions_list: bool (optional)
          Optional. Whether to return the latest permissions list of the share in the response.

        :returns: :class:`UpdateSharePermissionsResponse`
        """

        body = {}
        if changes is not None:
            body["changes"] = [v.as_dict() for v in changes]
        if omit_permissions_list is not None:
            body["omit_permissions_list"] = omit_permissions_list
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.1/unity-catalog/shares/{name}/permissions", body=body, headers=headers)
        return UpdateSharePermissionsResponse.from_dict(res)
