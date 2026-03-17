# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict)

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class CustomTag:
    key: Optional[str] = None
    """The key of the custom tag."""

    value: Optional[str] = None
    """The value of the custom tag."""

    def as_dict(self) -> dict:
        """Serializes the CustomTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CustomTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CustomTag:
        """Deserializes the CustomTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class DatabaseCatalog:
    name: str
    """The name of the catalog in UC."""

    database_instance_name: str
    """The name of the DatabaseInstance housing the database."""

    database_name: str
    """The name of the database (in a instance) associated with the catalog."""

    create_database_if_not_exists: Optional[bool] = None

    uid: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the DatabaseCatalog into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_database_if_not_exists is not None:
            body["create_database_if_not_exists"] = self.create_database_if_not_exists
        if self.database_instance_name is not None:
            body["database_instance_name"] = self.database_instance_name
        if self.database_name is not None:
            body["database_name"] = self.database_name
        if self.name is not None:
            body["name"] = self.name
        if self.uid is not None:
            body["uid"] = self.uid
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseCatalog into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_database_if_not_exists is not None:
            body["create_database_if_not_exists"] = self.create_database_if_not_exists
        if self.database_instance_name is not None:
            body["database_instance_name"] = self.database_instance_name
        if self.database_name is not None:
            body["database_name"] = self.database_name
        if self.name is not None:
            body["name"] = self.name
        if self.uid is not None:
            body["uid"] = self.uid
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseCatalog:
        """Deserializes the DatabaseCatalog from a dictionary."""
        return cls(
            create_database_if_not_exists=d.get("create_database_if_not_exists", None),
            database_instance_name=d.get("database_instance_name", None),
            database_name=d.get("database_name", None),
            name=d.get("name", None),
            uid=d.get("uid", None),
        )


@dataclass
class DatabaseCredential:
    expiration_time: Optional[str] = None

    token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the DatabaseCredential into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.token is not None:
            body["token"] = self.token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseCredential into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.token is not None:
            body["token"] = self.token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseCredential:
        """Deserializes the DatabaseCredential from a dictionary."""
        return cls(expiration_time=d.get("expiration_time", None), token=d.get("token", None))


@dataclass
class DatabaseInstance:
    """A DatabaseInstance represents a logical Postgres instance, comprised of both compute and
    storage."""

    name: str
    """The name of the instance. This is the unique identifier for the instance."""

    capacity: Optional[str] = None
    """The sku of the instance. Valid values are "CU_1", "CU_2", "CU_4", "CU_8"."""

    child_instance_refs: Optional[List[DatabaseInstanceRef]] = None
    """The refs of the child instances. This is only available if the instance is parent instance."""

    creation_time: Optional[str] = None
    """The timestamp when the instance was created."""

    creator: Optional[str] = None
    """The email of the creator of the instance."""

    custom_tags: Optional[List[CustomTag]] = None
    """Custom tags associated with the instance. This field is only included on create and update
    responses."""

    effective_capacity: Optional[str] = None
    """Deprecated. The sku of the instance; this field will always match the value of capacity. This is
    an output only field that contains the value computed from the input field combined with server
    side defaults. Use the field without the effective_ prefix to set the value."""

    effective_custom_tags: Optional[List[CustomTag]] = None
    """The recorded custom tags associated with the instance. This is an output only field that
    contains the value computed from the input field combined with server side defaults. Use the
    field without the effective_ prefix to set the value."""

    effective_enable_pg_native_login: Optional[bool] = None
    """Whether the instance has PG native password login enabled. This is an output only field that
    contains the value computed from the input field combined with server side defaults. Use the
    field without the effective_ prefix to set the value."""

    effective_enable_readable_secondaries: Optional[bool] = None
    """Whether secondaries serving read-only traffic are enabled. Defaults to false. This is an output
    only field that contains the value computed from the input field combined with server side
    defaults. Use the field without the effective_ prefix to set the value."""

    effective_node_count: Optional[int] = None
    """The number of nodes in the instance, composed of 1 primary and 0 or more secondaries. Defaults
    to 1 primary and 0 secondaries. This is an output only field that contains the value computed
    from the input field combined with server side defaults. Use the field without the effective_
    prefix to set the value."""

    effective_retention_window_in_days: Optional[int] = None
    """The retention window for the instance. This is the time window in days for which the historical
    data is retained. This is an output only field that contains the value computed from the input
    field combined with server side defaults. Use the field without the effective_ prefix to set the
    value."""

    effective_stopped: Optional[bool] = None
    """Whether the instance is stopped. This is an output only field that contains the value computed
    from the input field combined with server side defaults. Use the field without the effective_
    prefix to set the value."""

    effective_usage_policy_id: Optional[str] = None
    """The policy that is applied to the instance. This is an output only field that contains the value
    computed from the input field combined with server side defaults. Use the field without the
    effective_ prefix to set the value."""

    enable_pg_native_login: Optional[bool] = None
    """Whether to enable PG native password login on the instance. Defaults to false."""

    enable_readable_secondaries: Optional[bool] = None
    """Whether to enable secondaries to serve read-only traffic. Defaults to false."""

    node_count: Optional[int] = None
    """The number of nodes in the instance, composed of 1 primary and 0 or more secondaries. Defaults
    to 1 primary and 0 secondaries. This field is input only, see effective_node_count for the
    output."""

    parent_instance_ref: Optional[DatabaseInstanceRef] = None
    """The ref of the parent instance. This is only available if the instance is child instance. Input:
    For specifying the parent instance to create a child instance. Optional. Output: Only populated
    if provided as input to create a child instance."""

    pg_version: Optional[str] = None
    """The version of Postgres running on the instance."""

    read_only_dns: Optional[str] = None
    """The DNS endpoint to connect to the instance for read only access. This is only available if
    enable_readable_secondaries is true."""

    read_write_dns: Optional[str] = None
    """The DNS endpoint to connect to the instance for read+write access."""

    retention_window_in_days: Optional[int] = None
    """The retention window for the instance. This is the time window in days for which the historical
    data is retained. The default value is 7 days. Valid values are 2 to 35 days."""

    state: Optional[DatabaseInstanceState] = None
    """The current state of the instance."""

    stopped: Optional[bool] = None
    """Whether to stop the instance. An input only param, see effective_stopped for the output."""

    uid: Optional[str] = None
    """An immutable UUID identifier for the instance."""

    usage_policy_id: Optional[str] = None
    """The desired usage policy to associate with the instance."""

    def as_dict(self) -> dict:
        """Serializes the DatabaseInstance into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.capacity is not None:
            body["capacity"] = self.capacity
        if self.child_instance_refs:
            body["child_instance_refs"] = [v.as_dict() for v in self.child_instance_refs]
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.custom_tags:
            body["custom_tags"] = [v.as_dict() for v in self.custom_tags]
        if self.effective_capacity is not None:
            body["effective_capacity"] = self.effective_capacity
        if self.effective_custom_tags:
            body["effective_custom_tags"] = [v.as_dict() for v in self.effective_custom_tags]
        if self.effective_enable_pg_native_login is not None:
            body["effective_enable_pg_native_login"] = self.effective_enable_pg_native_login
        if self.effective_enable_readable_secondaries is not None:
            body["effective_enable_readable_secondaries"] = self.effective_enable_readable_secondaries
        if self.effective_node_count is not None:
            body["effective_node_count"] = self.effective_node_count
        if self.effective_retention_window_in_days is not None:
            body["effective_retention_window_in_days"] = self.effective_retention_window_in_days
        if self.effective_stopped is not None:
            body["effective_stopped"] = self.effective_stopped
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.enable_pg_native_login is not None:
            body["enable_pg_native_login"] = self.enable_pg_native_login
        if self.enable_readable_secondaries is not None:
            body["enable_readable_secondaries"] = self.enable_readable_secondaries
        if self.name is not None:
            body["name"] = self.name
        if self.node_count is not None:
            body["node_count"] = self.node_count
        if self.parent_instance_ref:
            body["parent_instance_ref"] = self.parent_instance_ref.as_dict()
        if self.pg_version is not None:
            body["pg_version"] = self.pg_version
        if self.read_only_dns is not None:
            body["read_only_dns"] = self.read_only_dns
        if self.read_write_dns is not None:
            body["read_write_dns"] = self.read_write_dns
        if self.retention_window_in_days is not None:
            body["retention_window_in_days"] = self.retention_window_in_days
        if self.state is not None:
            body["state"] = self.state.value
        if self.stopped is not None:
            body["stopped"] = self.stopped
        if self.uid is not None:
            body["uid"] = self.uid
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseInstance into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.capacity is not None:
            body["capacity"] = self.capacity
        if self.child_instance_refs:
            body["child_instance_refs"] = self.child_instance_refs
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.effective_capacity is not None:
            body["effective_capacity"] = self.effective_capacity
        if self.effective_custom_tags:
            body["effective_custom_tags"] = self.effective_custom_tags
        if self.effective_enable_pg_native_login is not None:
            body["effective_enable_pg_native_login"] = self.effective_enable_pg_native_login
        if self.effective_enable_readable_secondaries is not None:
            body["effective_enable_readable_secondaries"] = self.effective_enable_readable_secondaries
        if self.effective_node_count is not None:
            body["effective_node_count"] = self.effective_node_count
        if self.effective_retention_window_in_days is not None:
            body["effective_retention_window_in_days"] = self.effective_retention_window_in_days
        if self.effective_stopped is not None:
            body["effective_stopped"] = self.effective_stopped
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.enable_pg_native_login is not None:
            body["enable_pg_native_login"] = self.enable_pg_native_login
        if self.enable_readable_secondaries is not None:
            body["enable_readable_secondaries"] = self.enable_readable_secondaries
        if self.name is not None:
            body["name"] = self.name
        if self.node_count is not None:
            body["node_count"] = self.node_count
        if self.parent_instance_ref:
            body["parent_instance_ref"] = self.parent_instance_ref
        if self.pg_version is not None:
            body["pg_version"] = self.pg_version
        if self.read_only_dns is not None:
            body["read_only_dns"] = self.read_only_dns
        if self.read_write_dns is not None:
            body["read_write_dns"] = self.read_write_dns
        if self.retention_window_in_days is not None:
            body["retention_window_in_days"] = self.retention_window_in_days
        if self.state is not None:
            body["state"] = self.state
        if self.stopped is not None:
            body["stopped"] = self.stopped
        if self.uid is not None:
            body["uid"] = self.uid
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseInstance:
        """Deserializes the DatabaseInstance from a dictionary."""
        return cls(
            capacity=d.get("capacity", None),
            child_instance_refs=_repeated_dict(d, "child_instance_refs", DatabaseInstanceRef),
            creation_time=d.get("creation_time", None),
            creator=d.get("creator", None),
            custom_tags=_repeated_dict(d, "custom_tags", CustomTag),
            effective_capacity=d.get("effective_capacity", None),
            effective_custom_tags=_repeated_dict(d, "effective_custom_tags", CustomTag),
            effective_enable_pg_native_login=d.get("effective_enable_pg_native_login", None),
            effective_enable_readable_secondaries=d.get("effective_enable_readable_secondaries", None),
            effective_node_count=d.get("effective_node_count", None),
            effective_retention_window_in_days=d.get("effective_retention_window_in_days", None),
            effective_stopped=d.get("effective_stopped", None),
            effective_usage_policy_id=d.get("effective_usage_policy_id", None),
            enable_pg_native_login=d.get("enable_pg_native_login", None),
            enable_readable_secondaries=d.get("enable_readable_secondaries", None),
            name=d.get("name", None),
            node_count=d.get("node_count", None),
            parent_instance_ref=_from_dict(d, "parent_instance_ref", DatabaseInstanceRef),
            pg_version=d.get("pg_version", None),
            read_only_dns=d.get("read_only_dns", None),
            read_write_dns=d.get("read_write_dns", None),
            retention_window_in_days=d.get("retention_window_in_days", None),
            state=_enum(d, "state", DatabaseInstanceState),
            stopped=d.get("stopped", None),
            uid=d.get("uid", None),
            usage_policy_id=d.get("usage_policy_id", None),
        )


@dataclass
class DatabaseInstanceRef:
    """DatabaseInstanceRef is a reference to a database instance. It is used in the DatabaseInstance
    object to refer to the parent instance of an instance and to refer the child instances of an
    instance. To specify as a parent instance during creation of an instance, the lsn and
    branch_time fields are optional. If not specified, the child instance will be created from the
    latest lsn of the parent. If both lsn and branch_time are specified, the lsn will be used to
    create the child instance."""

    branch_time: Optional[str] = None
    """Branch time of the ref database instance. For a parent ref instance, this is the point in time
    on the parent instance from which the instance was created. For a child ref instance, this is
    the point in time on the instance from which the child instance was created. Input: For
    specifying the point in time to create a child instance. Optional. Output: Only populated if
    provided as input to create a child instance."""

    effective_lsn: Optional[str] = None
    """For a parent ref instance, this is the LSN on the parent instance from which the instance was
    created. For a child ref instance, this is the LSN on the instance from which the child instance
    was created. This is an output only field that contains the value computed from the input field
    combined with server side defaults. Use the field without the effective_ prefix to set the
    value."""

    lsn: Optional[str] = None
    """User-specified WAL LSN of the ref database instance.
    
    Input: For specifying the WAL LSN to create a child instance. Optional. Output: Only populated
    if provided as input to create a child instance."""

    name: Optional[str] = None
    """Name of the ref database instance."""

    uid: Optional[str] = None
    """Id of the ref database instance."""

    def as_dict(self) -> dict:
        """Serializes the DatabaseInstanceRef into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.branch_time is not None:
            body["branch_time"] = self.branch_time
        if self.effective_lsn is not None:
            body["effective_lsn"] = self.effective_lsn
        if self.lsn is not None:
            body["lsn"] = self.lsn
        if self.name is not None:
            body["name"] = self.name
        if self.uid is not None:
            body["uid"] = self.uid
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseInstanceRef into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.branch_time is not None:
            body["branch_time"] = self.branch_time
        if self.effective_lsn is not None:
            body["effective_lsn"] = self.effective_lsn
        if self.lsn is not None:
            body["lsn"] = self.lsn
        if self.name is not None:
            body["name"] = self.name
        if self.uid is not None:
            body["uid"] = self.uid
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseInstanceRef:
        """Deserializes the DatabaseInstanceRef from a dictionary."""
        return cls(
            branch_time=d.get("branch_time", None),
            effective_lsn=d.get("effective_lsn", None),
            lsn=d.get("lsn", None),
            name=d.get("name", None),
            uid=d.get("uid", None),
        )


@dataclass
class DatabaseInstanceRole:
    """A DatabaseInstanceRole represents a Postgres role in a database instance."""

    name: str
    """The name of the role. This is the unique identifier for the role in an instance."""

    attributes: Optional[DatabaseInstanceRoleAttributes] = None
    """The desired API-exposed Postgres role attribute to associate with the role. Optional."""

    effective_attributes: Optional[DatabaseInstanceRoleAttributes] = None
    """The attributes that are applied to the role. This is an output only field that contains the
    value computed from the input field combined with server side defaults. Use the field without
    the effective_ prefix to set the value."""

    identity_type: Optional[DatabaseInstanceRoleIdentityType] = None
    """The type of the role."""

    instance_name: Optional[str] = None

    membership_role: Optional[DatabaseInstanceRoleMembershipRole] = None
    """An enum value for a standard role that this role is a member of."""

    def as_dict(self) -> dict:
        """Serializes the DatabaseInstanceRole into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.attributes:
            body["attributes"] = self.attributes.as_dict()
        if self.effective_attributes:
            body["effective_attributes"] = self.effective_attributes.as_dict()
        if self.identity_type is not None:
            body["identity_type"] = self.identity_type.value
        if self.instance_name is not None:
            body["instance_name"] = self.instance_name
        if self.membership_role is not None:
            body["membership_role"] = self.membership_role.value
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseInstanceRole into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.attributes:
            body["attributes"] = self.attributes
        if self.effective_attributes:
            body["effective_attributes"] = self.effective_attributes
        if self.identity_type is not None:
            body["identity_type"] = self.identity_type
        if self.instance_name is not None:
            body["instance_name"] = self.instance_name
        if self.membership_role is not None:
            body["membership_role"] = self.membership_role
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseInstanceRole:
        """Deserializes the DatabaseInstanceRole from a dictionary."""
        return cls(
            attributes=_from_dict(d, "attributes", DatabaseInstanceRoleAttributes),
            effective_attributes=_from_dict(d, "effective_attributes", DatabaseInstanceRoleAttributes),
            identity_type=_enum(d, "identity_type", DatabaseInstanceRoleIdentityType),
            instance_name=d.get("instance_name", None),
            membership_role=_enum(d, "membership_role", DatabaseInstanceRoleMembershipRole),
            name=d.get("name", None),
        )


@dataclass
class DatabaseInstanceRoleAttributes:
    """Attributes that can be granted to a Postgres role. We are only implementing a subset for now,
    see xref: https://www.postgresql.org/docs/16/sql-createrole.html The values follow Postgres
    keyword naming e.g. CREATEDB, BYPASSRLS, etc. which is why they don't include typical
    underscores between words. We were requested to make this a nested object/struct representation
    since these are knobs from an external spec."""

    bypassrls: Optional[bool] = None

    createdb: Optional[bool] = None

    createrole: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the DatabaseInstanceRoleAttributes into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bypassrls is not None:
            body["bypassrls"] = self.bypassrls
        if self.createdb is not None:
            body["createdb"] = self.createdb
        if self.createrole is not None:
            body["createrole"] = self.createrole
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseInstanceRoleAttributes into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bypassrls is not None:
            body["bypassrls"] = self.bypassrls
        if self.createdb is not None:
            body["createdb"] = self.createdb
        if self.createrole is not None:
            body["createrole"] = self.createrole
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseInstanceRoleAttributes:
        """Deserializes the DatabaseInstanceRoleAttributes from a dictionary."""
        return cls(
            bypassrls=d.get("bypassrls", None), createdb=d.get("createdb", None), createrole=d.get("createrole", None)
        )


class DatabaseInstanceRoleIdentityType(Enum):

    GROUP = "GROUP"
    PG_ONLY = "PG_ONLY"
    SERVICE_PRINCIPAL = "SERVICE_PRINCIPAL"
    USER = "USER"


class DatabaseInstanceRoleMembershipRole(Enum):
    """Roles that the DatabaseInstanceRole can be a member of."""

    DATABRICKS_SUPERUSER = "DATABRICKS_SUPERUSER"


class DatabaseInstanceState(Enum):

    AVAILABLE = "AVAILABLE"
    DELETING = "DELETING"
    FAILING_OVER = "FAILING_OVER"
    STARTING = "STARTING"
    STOPPED = "STOPPED"
    UPDATING = "UPDATING"


@dataclass
class DatabaseTable:
    """Next field marker: 13"""

    name: str
    """Full three-part (catalog, schema, table) name of the table."""

    database_instance_name: Optional[str] = None
    """Name of the target database instance. This is required when creating database tables in standard
    catalogs. This is optional when creating database tables in registered catalogs. If this field
    is specified when creating database tables in registered catalogs, the database instance name
    MUST match that of the registered catalog (or the request will be rejected)."""

    logical_database_name: Optional[str] = None
    """Target Postgres database object (logical database) name for this table.
    
    When creating a table in a standard catalog, this field is required. In this scenario,
    specifying this field will allow targeting an arbitrary postgres database.
    
    Registration of database tables via /database/tables is currently only supported in standard
    catalogs."""

    def as_dict(self) -> dict:
        """Serializes the DatabaseTable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.database_instance_name is not None:
            body["database_instance_name"] = self.database_instance_name
        if self.logical_database_name is not None:
            body["logical_database_name"] = self.logical_database_name
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseTable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.database_instance_name is not None:
            body["database_instance_name"] = self.database_instance_name
        if self.logical_database_name is not None:
            body["logical_database_name"] = self.logical_database_name
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseTable:
        """Deserializes the DatabaseTable from a dictionary."""
        return cls(
            database_instance_name=d.get("database_instance_name", None),
            logical_database_name=d.get("logical_database_name", None),
            name=d.get("name", None),
        )


@dataclass
class DeltaTableSyncInfo:
    delta_commit_timestamp: Optional[str] = None
    """The timestamp when the above Delta version was committed in the source Delta table. Note: This
    is the Delta commit time, not the time the data was written to the synced table."""

    delta_commit_version: Optional[int] = None
    """The Delta Lake commit version that was last successfully synced."""

    def as_dict(self) -> dict:
        """Serializes the DeltaTableSyncInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.delta_commit_timestamp is not None:
            body["delta_commit_timestamp"] = self.delta_commit_timestamp
        if self.delta_commit_version is not None:
            body["delta_commit_version"] = self.delta_commit_version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaTableSyncInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.delta_commit_timestamp is not None:
            body["delta_commit_timestamp"] = self.delta_commit_timestamp
        if self.delta_commit_version is not None:
            body["delta_commit_version"] = self.delta_commit_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaTableSyncInfo:
        """Deserializes the DeltaTableSyncInfo from a dictionary."""
        return cls(
            delta_commit_timestamp=d.get("delta_commit_timestamp", None),
            delta_commit_version=d.get("delta_commit_version", None),
        )


@dataclass
class ListDatabaseCatalogsResponse:
    database_catalogs: Optional[List[DatabaseCatalog]] = None

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of database catalogs."""

    def as_dict(self) -> dict:
        """Serializes the ListDatabaseCatalogsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.database_catalogs:
            body["database_catalogs"] = [v.as_dict() for v in self.database_catalogs]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListDatabaseCatalogsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.database_catalogs:
            body["database_catalogs"] = self.database_catalogs
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListDatabaseCatalogsResponse:
        """Deserializes the ListDatabaseCatalogsResponse from a dictionary."""
        return cls(
            database_catalogs=_repeated_dict(d, "database_catalogs", DatabaseCatalog),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListDatabaseInstanceRolesResponse:
    database_instance_roles: Optional[List[DatabaseInstanceRole]] = None
    """List of database instance roles."""

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of instances."""

    def as_dict(self) -> dict:
        """Serializes the ListDatabaseInstanceRolesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.database_instance_roles:
            body["database_instance_roles"] = [v.as_dict() for v in self.database_instance_roles]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListDatabaseInstanceRolesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.database_instance_roles:
            body["database_instance_roles"] = self.database_instance_roles
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListDatabaseInstanceRolesResponse:
        """Deserializes the ListDatabaseInstanceRolesResponse from a dictionary."""
        return cls(
            database_instance_roles=_repeated_dict(d, "database_instance_roles", DatabaseInstanceRole),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListDatabaseInstancesResponse:
    database_instances: Optional[List[DatabaseInstance]] = None
    """List of instances."""

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of instances."""

    def as_dict(self) -> dict:
        """Serializes the ListDatabaseInstancesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.database_instances:
            body["database_instances"] = [v.as_dict() for v in self.database_instances]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListDatabaseInstancesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.database_instances:
            body["database_instances"] = self.database_instances
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListDatabaseInstancesResponse:
        """Deserializes the ListDatabaseInstancesResponse from a dictionary."""
        return cls(
            database_instances=_repeated_dict(d, "database_instances", DatabaseInstance),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListSyncedDatabaseTablesResponse:
    next_page_token: Optional[str] = None
    """Pagination token to request the next page of synced tables."""

    synced_tables: Optional[List[SyncedDatabaseTable]] = None

    def as_dict(self) -> dict:
        """Serializes the ListSyncedDatabaseTablesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.synced_tables:
            body["synced_tables"] = [v.as_dict() for v in self.synced_tables]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSyncedDatabaseTablesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.synced_tables:
            body["synced_tables"] = self.synced_tables
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSyncedDatabaseTablesResponse:
        """Deserializes the ListSyncedDatabaseTablesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            synced_tables=_repeated_dict(d, "synced_tables", SyncedDatabaseTable),
        )


@dataclass
class NewPipelineSpec:
    """Custom fields that user can set for pipeline while creating SyncedDatabaseTable. Note that other
    fields of pipeline are still inferred by table def internally"""

    budget_policy_id: Optional[str] = None
    """Budget policy to set on the newly created pipeline."""

    storage_catalog: Optional[str] = None
    """This field needs to be specified if the destination catalog is a managed postgres catalog.
    
    UC catalog for the pipeline to store intermediate files (checkpoints, event logs etc). This
    needs to be a standard catalog where the user has permissions to create Delta tables."""

    storage_schema: Optional[str] = None
    """This field needs to be specified if the destination catalog is a managed postgres catalog.
    
    UC schema for the pipeline to store intermediate files (checkpoints, event logs etc). This needs
    to be in the standard catalog where the user has permissions to create Delta tables."""

    def as_dict(self) -> dict:
        """Serializes the NewPipelineSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.storage_catalog is not None:
            body["storage_catalog"] = self.storage_catalog
        if self.storage_schema is not None:
            body["storage_schema"] = self.storage_schema
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NewPipelineSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.storage_catalog is not None:
            body["storage_catalog"] = self.storage_catalog
        if self.storage_schema is not None:
            body["storage_schema"] = self.storage_schema
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NewPipelineSpec:
        """Deserializes the NewPipelineSpec from a dictionary."""
        return cls(
            budget_policy_id=d.get("budget_policy_id", None),
            storage_catalog=d.get("storage_catalog", None),
            storage_schema=d.get("storage_schema", None),
        )


class ProvisioningInfoState(Enum):

    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    DELETING = "DELETING"
    FAILED = "FAILED"
    PROVISIONING = "PROVISIONING"
    UPDATING = "UPDATING"


class ProvisioningPhase(Enum):

    PROVISIONING_PHASE_INDEX_SCAN = "PROVISIONING_PHASE_INDEX_SCAN"
    PROVISIONING_PHASE_INDEX_SORT = "PROVISIONING_PHASE_INDEX_SORT"
    PROVISIONING_PHASE_MAIN = "PROVISIONING_PHASE_MAIN"


@dataclass
class RequestedClaims:
    permission_set: Optional[RequestedClaimsPermissionSet] = None

    resources: Optional[List[RequestedResource]] = None

    def as_dict(self) -> dict:
        """Serializes the RequestedClaims into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_set is not None:
            body["permission_set"] = self.permission_set.value
        if self.resources:
            body["resources"] = [v.as_dict() for v in self.resources]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RequestedClaims into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_set is not None:
            body["permission_set"] = self.permission_set
        if self.resources:
            body["resources"] = self.resources
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RequestedClaims:
        """Deserializes the RequestedClaims from a dictionary."""
        return cls(
            permission_set=_enum(d, "permission_set", RequestedClaimsPermissionSet),
            resources=_repeated_dict(d, "resources", RequestedResource),
        )


class RequestedClaimsPermissionSet(Enum):
    """Might add WRITE in the future"""

    READ_ONLY = "READ_ONLY"


@dataclass
class RequestedResource:
    table_name: Optional[str] = None

    unspecified_resource_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the RequestedResource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.table_name is not None:
            body["table_name"] = self.table_name
        if self.unspecified_resource_name is not None:
            body["unspecified_resource_name"] = self.unspecified_resource_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RequestedResource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.table_name is not None:
            body["table_name"] = self.table_name
        if self.unspecified_resource_name is not None:
            body["unspecified_resource_name"] = self.unspecified_resource_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RequestedResource:
        """Deserializes the RequestedResource from a dictionary."""
        return cls(
            table_name=d.get("table_name", None), unspecified_resource_name=d.get("unspecified_resource_name", None)
        )


@dataclass
class SyncedDatabaseTable:
    """Next field marker: 18"""

    name: str
    """Full three-part (catalog, schema, table) name of the table."""

    data_synchronization_status: Optional[SyncedTableStatus] = None
    """Synced Table data synchronization status"""

    database_instance_name: Optional[str] = None
    """Name of the target database instance. This is required when creating synced database tables in
    standard catalogs. This is optional when creating synced database tables in registered catalogs.
    If this field is specified when creating synced database tables in registered catalogs, the
    database instance name MUST match that of the registered catalog (or the request will be
    rejected)."""

    effective_database_instance_name: Optional[str] = None
    """The name of the database instance that this table is registered to. This field is always
    returned, and for tables inside database catalogs is inferred database instance associated with
    the catalog. This is an output only field that contains the value computed from the input field
    combined with server side defaults. Use the field without the effective_ prefix to set the
    value."""

    effective_logical_database_name: Optional[str] = None
    """The name of the logical database that this table is registered to. This is an output only field
    that contains the value computed from the input field combined with server side defaults. Use
    the field without the effective_ prefix to set the value."""

    logical_database_name: Optional[str] = None
    """Target Postgres database object (logical database) name for this table.
    
    When creating a synced table in a registered Postgres catalog, the target Postgres database name
    is inferred to be that of the registered catalog. If this field is specified in this scenario,
    the Postgres database name MUST match that of the registered catalog (or the request will be
    rejected).
    
    When creating a synced table in a standard catalog, this field is required. In this scenario,
    specifying this field will allow targeting an arbitrary postgres database. Note that this has
    implications for the `create_database_objects_is_missing` field in `spec`."""

    spec: Optional[SyncedTableSpec] = None

    unity_catalog_provisioning_state: Optional[ProvisioningInfoState] = None
    """The provisioning state of the synced table entity in Unity Catalog. This is distinct from the
    state of the data synchronization pipeline (i.e. the table may be in "ACTIVE" but the pipeline
    may be in "PROVISIONING" as it runs asynchronously)."""

    def as_dict(self) -> dict:
        """Serializes the SyncedDatabaseTable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.data_synchronization_status:
            body["data_synchronization_status"] = self.data_synchronization_status.as_dict()
        if self.database_instance_name is not None:
            body["database_instance_name"] = self.database_instance_name
        if self.effective_database_instance_name is not None:
            body["effective_database_instance_name"] = self.effective_database_instance_name
        if self.effective_logical_database_name is not None:
            body["effective_logical_database_name"] = self.effective_logical_database_name
        if self.logical_database_name is not None:
            body["logical_database_name"] = self.logical_database_name
        if self.name is not None:
            body["name"] = self.name
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.unity_catalog_provisioning_state is not None:
            body["unity_catalog_provisioning_state"] = self.unity_catalog_provisioning_state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedDatabaseTable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.data_synchronization_status:
            body["data_synchronization_status"] = self.data_synchronization_status
        if self.database_instance_name is not None:
            body["database_instance_name"] = self.database_instance_name
        if self.effective_database_instance_name is not None:
            body["effective_database_instance_name"] = self.effective_database_instance_name
        if self.effective_logical_database_name is not None:
            body["effective_logical_database_name"] = self.effective_logical_database_name
        if self.logical_database_name is not None:
            body["logical_database_name"] = self.logical_database_name
        if self.name is not None:
            body["name"] = self.name
        if self.spec:
            body["spec"] = self.spec
        if self.unity_catalog_provisioning_state is not None:
            body["unity_catalog_provisioning_state"] = self.unity_catalog_provisioning_state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedDatabaseTable:
        """Deserializes the SyncedDatabaseTable from a dictionary."""
        return cls(
            data_synchronization_status=_from_dict(d, "data_synchronization_status", SyncedTableStatus),
            database_instance_name=d.get("database_instance_name", None),
            effective_database_instance_name=d.get("effective_database_instance_name", None),
            effective_logical_database_name=d.get("effective_logical_database_name", None),
            logical_database_name=d.get("logical_database_name", None),
            name=d.get("name", None),
            spec=_from_dict(d, "spec", SyncedTableSpec),
            unity_catalog_provisioning_state=_enum(d, "unity_catalog_provisioning_state", ProvisioningInfoState),
        )


@dataclass
class SyncedTableContinuousUpdateStatus:
    """Detailed status of a synced table. Shown if the synced table is in the SYNCED_CONTINUOUS_UPDATE
    or the SYNCED_UPDATING_PIPELINE_RESOURCES state."""

    initial_pipeline_sync_progress: Optional[SyncedTablePipelineProgress] = None
    """Progress of the initial data synchronization."""

    last_processed_commit_version: Optional[int] = None
    """The last source table Delta version that was successfully synced to the synced table."""

    timestamp: Optional[str] = None
    """The end timestamp of the last time any data was synchronized from the source table to the synced
    table. This is when the data is available in the synced table."""

    def as_dict(self) -> dict:
        """Serializes the SyncedTableContinuousUpdateStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress.as_dict()
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTableContinuousUpdateStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTableContinuousUpdateStatus:
        """Deserializes the SyncedTableContinuousUpdateStatus from a dictionary."""
        return cls(
            initial_pipeline_sync_progress=_from_dict(d, "initial_pipeline_sync_progress", SyncedTablePipelineProgress),
            last_processed_commit_version=d.get("last_processed_commit_version", None),
            timestamp=d.get("timestamp", None),
        )


@dataclass
class SyncedTableFailedStatus:
    """Detailed status of a synced table. Shown if the synced table is in the OFFLINE_FAILED or the
    SYNCED_PIPELINE_FAILED state."""

    last_processed_commit_version: Optional[int] = None
    """The last source table Delta version that was successfully synced to the synced table. The last
    source table Delta version that was synced to the synced table. Only populated if the table is
    still synced and available for serving."""

    timestamp: Optional[str] = None
    """The end timestamp of the last time any data was synchronized from the source table to the synced
    table. Only populated if the table is still synced and available for serving."""

    def as_dict(self) -> dict:
        """Serializes the SyncedTableFailedStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTableFailedStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTableFailedStatus:
        """Deserializes the SyncedTableFailedStatus from a dictionary."""
        return cls(
            last_processed_commit_version=d.get("last_processed_commit_version", None),
            timestamp=d.get("timestamp", None),
        )


@dataclass
class SyncedTablePipelineProgress:
    """Progress information of the Synced Table data synchronization pipeline."""

    estimated_completion_time_seconds: Optional[float] = None
    """The estimated time remaining to complete this update in seconds."""

    latest_version_currently_processing: Optional[int] = None
    """The source table Delta version that was last processed by the pipeline. The pipeline may not
    have completely processed this version yet."""

    provisioning_phase: Optional[ProvisioningPhase] = None
    """The current phase of the data synchronization pipeline."""

    sync_progress_completion: Optional[float] = None
    """The completion ratio of this update. This is a number between 0 and 1."""

    synced_row_count: Optional[int] = None
    """The number of rows that have been synced in this update."""

    total_row_count: Optional[int] = None
    """The total number of rows that need to be synced in this update. This number may be an estimate."""

    def as_dict(self) -> dict:
        """Serializes the SyncedTablePipelineProgress into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.estimated_completion_time_seconds is not None:
            body["estimated_completion_time_seconds"] = self.estimated_completion_time_seconds
        if self.latest_version_currently_processing is not None:
            body["latest_version_currently_processing"] = self.latest_version_currently_processing
        if self.provisioning_phase is not None:
            body["provisioning_phase"] = self.provisioning_phase.value
        if self.sync_progress_completion is not None:
            body["sync_progress_completion"] = self.sync_progress_completion
        if self.synced_row_count is not None:
            body["synced_row_count"] = self.synced_row_count
        if self.total_row_count is not None:
            body["total_row_count"] = self.total_row_count
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTablePipelineProgress into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.estimated_completion_time_seconds is not None:
            body["estimated_completion_time_seconds"] = self.estimated_completion_time_seconds
        if self.latest_version_currently_processing is not None:
            body["latest_version_currently_processing"] = self.latest_version_currently_processing
        if self.provisioning_phase is not None:
            body["provisioning_phase"] = self.provisioning_phase
        if self.sync_progress_completion is not None:
            body["sync_progress_completion"] = self.sync_progress_completion
        if self.synced_row_count is not None:
            body["synced_row_count"] = self.synced_row_count
        if self.total_row_count is not None:
            body["total_row_count"] = self.total_row_count
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTablePipelineProgress:
        """Deserializes the SyncedTablePipelineProgress from a dictionary."""
        return cls(
            estimated_completion_time_seconds=d.get("estimated_completion_time_seconds", None),
            latest_version_currently_processing=d.get("latest_version_currently_processing", None),
            provisioning_phase=_enum(d, "provisioning_phase", ProvisioningPhase),
            sync_progress_completion=d.get("sync_progress_completion", None),
            synced_row_count=d.get("synced_row_count", None),
            total_row_count=d.get("total_row_count", None),
        )


@dataclass
class SyncedTablePosition:
    delta_table_sync_info: Optional[DeltaTableSyncInfo] = None

    sync_end_timestamp: Optional[str] = None
    """The end timestamp of the most recent successful synchronization. This is the time when the data
    is available in the synced table."""

    sync_start_timestamp: Optional[str] = None
    """The starting timestamp of the most recent successful synchronization from the source table to
    the destination (synced) table. Note this is the starting timestamp of the sync operation, not
    the end time. E.g., for a batch, this is the time when the sync operation started."""

    def as_dict(self) -> dict:
        """Serializes the SyncedTablePosition into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.delta_table_sync_info:
            body["delta_table_sync_info"] = self.delta_table_sync_info.as_dict()
        if self.sync_end_timestamp is not None:
            body["sync_end_timestamp"] = self.sync_end_timestamp
        if self.sync_start_timestamp is not None:
            body["sync_start_timestamp"] = self.sync_start_timestamp
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTablePosition into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.delta_table_sync_info:
            body["delta_table_sync_info"] = self.delta_table_sync_info
        if self.sync_end_timestamp is not None:
            body["sync_end_timestamp"] = self.sync_end_timestamp
        if self.sync_start_timestamp is not None:
            body["sync_start_timestamp"] = self.sync_start_timestamp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTablePosition:
        """Deserializes the SyncedTablePosition from a dictionary."""
        return cls(
            delta_table_sync_info=_from_dict(d, "delta_table_sync_info", DeltaTableSyncInfo),
            sync_end_timestamp=d.get("sync_end_timestamp", None),
            sync_start_timestamp=d.get("sync_start_timestamp", None),
        )


@dataclass
class SyncedTableProvisioningStatus:
    """Detailed status of a synced table. Shown if the synced table is in the
    PROVISIONING_PIPELINE_RESOURCES or the PROVISIONING_INITIAL_SNAPSHOT state."""

    initial_pipeline_sync_progress: Optional[SyncedTablePipelineProgress] = None
    """Details about initial data synchronization. Only populated when in the
    PROVISIONING_INITIAL_SNAPSHOT state."""

    def as_dict(self) -> dict:
        """Serializes the SyncedTableProvisioningStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTableProvisioningStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.initial_pipeline_sync_progress:
            body["initial_pipeline_sync_progress"] = self.initial_pipeline_sync_progress
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTableProvisioningStatus:
        """Deserializes the SyncedTableProvisioningStatus from a dictionary."""
        return cls(
            initial_pipeline_sync_progress=_from_dict(d, "initial_pipeline_sync_progress", SyncedTablePipelineProgress)
        )


class SyncedTableSchedulingPolicy(Enum):

    CONTINUOUS = "CONTINUOUS"
    SNAPSHOT = "SNAPSHOT"
    TRIGGERED = "TRIGGERED"


@dataclass
class SyncedTableSpec:
    """Specification of a synced database table."""

    create_database_objects_if_missing: Optional[bool] = None
    """If true, the synced table's logical database and schema resources in PG will be created if they
    do not already exist."""

    existing_pipeline_id: Optional[str] = None
    """At most one of existing_pipeline_id and new_pipeline_spec should be defined.
    
    If existing_pipeline_id is defined, the synced table will be bin packed into the existing
    pipeline referenced. This avoids creating a new pipeline and allows sharing existing compute. In
    this case, the scheduling_policy of this synced table must match the scheduling policy of the
    existing pipeline."""

    new_pipeline_spec: Optional[NewPipelineSpec] = None
    """At most one of existing_pipeline_id and new_pipeline_spec should be defined.
    
    If new_pipeline_spec is defined, a new pipeline is created for this synced table. The location
    pointed to is used to store intermediate files (checkpoints, event logs etc). The caller must
    have write permissions to create Delta tables in the specified catalog and schema. Again, note
    this requires write permissions, whereas the source table only requires read permissions."""

    primary_key_columns: Optional[List[str]] = None
    """Primary Key columns to be used for data insert/update in the destination."""

    scheduling_policy: Optional[SyncedTableSchedulingPolicy] = None
    """Scheduling policy of the underlying pipeline."""

    source_table_full_name: Optional[str] = None
    """Three-part (catalog, schema, table) name of the source Delta table."""

    timeseries_key: Optional[str] = None
    """Time series key to deduplicate (tie-break) rows with the same primary key."""

    def as_dict(self) -> dict:
        """Serializes the SyncedTableSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_database_objects_if_missing is not None:
            body["create_database_objects_if_missing"] = self.create_database_objects_if_missing
        if self.existing_pipeline_id is not None:
            body["existing_pipeline_id"] = self.existing_pipeline_id
        if self.new_pipeline_spec:
            body["new_pipeline_spec"] = self.new_pipeline_spec.as_dict()
        if self.primary_key_columns:
            body["primary_key_columns"] = [v for v in self.primary_key_columns]
        if self.scheduling_policy is not None:
            body["scheduling_policy"] = self.scheduling_policy.value
        if self.source_table_full_name is not None:
            body["source_table_full_name"] = self.source_table_full_name
        if self.timeseries_key is not None:
            body["timeseries_key"] = self.timeseries_key
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTableSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_database_objects_if_missing is not None:
            body["create_database_objects_if_missing"] = self.create_database_objects_if_missing
        if self.existing_pipeline_id is not None:
            body["existing_pipeline_id"] = self.existing_pipeline_id
        if self.new_pipeline_spec:
            body["new_pipeline_spec"] = self.new_pipeline_spec
        if self.primary_key_columns:
            body["primary_key_columns"] = self.primary_key_columns
        if self.scheduling_policy is not None:
            body["scheduling_policy"] = self.scheduling_policy
        if self.source_table_full_name is not None:
            body["source_table_full_name"] = self.source_table_full_name
        if self.timeseries_key is not None:
            body["timeseries_key"] = self.timeseries_key
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTableSpec:
        """Deserializes the SyncedTableSpec from a dictionary."""
        return cls(
            create_database_objects_if_missing=d.get("create_database_objects_if_missing", None),
            existing_pipeline_id=d.get("existing_pipeline_id", None),
            new_pipeline_spec=_from_dict(d, "new_pipeline_spec", NewPipelineSpec),
            primary_key_columns=d.get("primary_key_columns", None),
            scheduling_policy=_enum(d, "scheduling_policy", SyncedTableSchedulingPolicy),
            source_table_full_name=d.get("source_table_full_name", None),
            timeseries_key=d.get("timeseries_key", None),
        )


class SyncedTableState(Enum):
    """The state of a synced table."""

    SYNCED_TABLED_OFFLINE = "SYNCED_TABLED_OFFLINE"
    SYNCED_TABLE_OFFLINE_FAILED = "SYNCED_TABLE_OFFLINE_FAILED"
    SYNCED_TABLE_ONLINE = "SYNCED_TABLE_ONLINE"
    SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE = "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE"
    SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"
    SYNCED_TABLE_ONLINE_PIPELINE_FAILED = "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"
    SYNCED_TABLE_ONLINE_TRIGGERED_UPDATE = "SYNCED_TABLE_ONLINE_TRIGGERED_UPDATE"
    SYNCED_TABLE_ONLINE_UPDATING_PIPELINE_RESOURCES = "SYNCED_TABLE_ONLINE_UPDATING_PIPELINE_RESOURCES"
    SYNCED_TABLE_PROVISIONING = "SYNCED_TABLE_PROVISIONING"
    SYNCED_TABLE_PROVISIONING_INITIAL_SNAPSHOT = "SYNCED_TABLE_PROVISIONING_INITIAL_SNAPSHOT"
    SYNCED_TABLE_PROVISIONING_PIPELINE_RESOURCES = "SYNCED_TABLE_PROVISIONING_PIPELINE_RESOURCES"


@dataclass
class SyncedTableStatus:
    """Status of a synced table."""

    continuous_update_status: Optional[SyncedTableContinuousUpdateStatus] = None

    detailed_state: Optional[SyncedTableState] = None
    """The state of the synced table."""

    failed_status: Optional[SyncedTableFailedStatus] = None

    last_sync: Optional[SyncedTablePosition] = None
    """Summary of the last successful synchronization from source to destination.
    
    Will always be present if there has been a successful sync. Even if the most recent syncs have
    failed.
    
    Limitation: The only exception is if the synced table is doing a FULL REFRESH, then the last
    sync information will not be available until the full refresh is complete. This limitation will
    be addressed in a future version.
    
    This top-level field is a convenience for consumers who want easy access to last sync
    information without having to traverse detailed_status."""

    message: Optional[str] = None
    """A text description of the current state of the synced table."""

    pipeline_id: Optional[str] = None
    """ID of the associated pipeline. The pipeline ID may have been provided by the client (in the case
    of bin packing), or generated by the server (when creating a new pipeline)."""

    provisioning_status: Optional[SyncedTableProvisioningStatus] = None

    triggered_update_status: Optional[SyncedTableTriggeredUpdateStatus] = None

    def as_dict(self) -> dict:
        """Serializes the SyncedTableStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.continuous_update_status:
            body["continuous_update_status"] = self.continuous_update_status.as_dict()
        if self.detailed_state is not None:
            body["detailed_state"] = self.detailed_state.value
        if self.failed_status:
            body["failed_status"] = self.failed_status.as_dict()
        if self.last_sync:
            body["last_sync"] = self.last_sync.as_dict()
        if self.message is not None:
            body["message"] = self.message
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.provisioning_status:
            body["provisioning_status"] = self.provisioning_status.as_dict()
        if self.triggered_update_status:
            body["triggered_update_status"] = self.triggered_update_status.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTableStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.continuous_update_status:
            body["continuous_update_status"] = self.continuous_update_status
        if self.detailed_state is not None:
            body["detailed_state"] = self.detailed_state
        if self.failed_status:
            body["failed_status"] = self.failed_status
        if self.last_sync:
            body["last_sync"] = self.last_sync
        if self.message is not None:
            body["message"] = self.message
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.provisioning_status:
            body["provisioning_status"] = self.provisioning_status
        if self.triggered_update_status:
            body["triggered_update_status"] = self.triggered_update_status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTableStatus:
        """Deserializes the SyncedTableStatus from a dictionary."""
        return cls(
            continuous_update_status=_from_dict(d, "continuous_update_status", SyncedTableContinuousUpdateStatus),
            detailed_state=_enum(d, "detailed_state", SyncedTableState),
            failed_status=_from_dict(d, "failed_status", SyncedTableFailedStatus),
            last_sync=_from_dict(d, "last_sync", SyncedTablePosition),
            message=d.get("message", None),
            pipeline_id=d.get("pipeline_id", None),
            provisioning_status=_from_dict(d, "provisioning_status", SyncedTableProvisioningStatus),
            triggered_update_status=_from_dict(d, "triggered_update_status", SyncedTableTriggeredUpdateStatus),
        )


@dataclass
class SyncedTableTriggeredUpdateStatus:
    """Detailed status of a synced table. Shown if the synced table is in the SYNCED_TRIGGERED_UPDATE
    or the SYNCED_NO_PENDING_UPDATE state."""

    last_processed_commit_version: Optional[int] = None
    """The last source table Delta version that was successfully synced to the synced table."""

    timestamp: Optional[str] = None
    """The end timestamp of the last time any data was synchronized from the source table to the synced
    table. This is when the data is available in the synced table."""

    triggered_update_progress: Optional[SyncedTablePipelineProgress] = None
    """Progress of the active data synchronization pipeline."""

    def as_dict(self) -> dict:
        """Serializes the SyncedTableTriggeredUpdateStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.triggered_update_progress:
            body["triggered_update_progress"] = self.triggered_update_progress.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SyncedTableTriggeredUpdateStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.last_processed_commit_version is not None:
            body["last_processed_commit_version"] = self.last_processed_commit_version
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.triggered_update_progress:
            body["triggered_update_progress"] = self.triggered_update_progress
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SyncedTableTriggeredUpdateStatus:
        """Deserializes the SyncedTableTriggeredUpdateStatus from a dictionary."""
        return cls(
            last_processed_commit_version=d.get("last_processed_commit_version", None),
            timestamp=d.get("timestamp", None),
            triggered_update_progress=_from_dict(d, "triggered_update_progress", SyncedTablePipelineProgress),
        )


class DatabaseAPI:
    """Database Instances provide access to a database via REST API or direct SQL."""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_database_instance_database_available(
        self, name: str, timeout=timedelta(minutes=20), callback: Optional[Callable[[DatabaseInstance], None]] = None
    ) -> DatabaseInstance:
        deadline = time.time() + timeout.total_seconds()
        target_states = (DatabaseInstanceState.AVAILABLE,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get_database_instance(name=name)
            status = poll.state
            status_message = f"current status: {status}"
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            prefix = f"name={name}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def create_database_catalog(self, catalog: DatabaseCatalog) -> DatabaseCatalog:
        """Create a Database Catalog.

        :param catalog: :class:`DatabaseCatalog`

        :returns: :class:`DatabaseCatalog`
        """

        body = catalog.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/database/catalogs", body=body, headers=headers)
        return DatabaseCatalog.from_dict(res)

    def create_database_instance(self, database_instance: DatabaseInstance) -> Wait[DatabaseInstance]:
        """Create a Database Instance.

        :param database_instance: :class:`DatabaseInstance`
          Instance to create.

        :returns:
          Long-running operation waiter for :class:`DatabaseInstance`.
          See :method:wait_get_database_instance_database_available for more details.
        """

        body = database_instance.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.0/database/instances", body=body, headers=headers)
        return Wait(
            self.wait_get_database_instance_database_available,
            response=DatabaseInstance.from_dict(op_response),
            name=op_response["name"],
        )

    def create_database_instance_and_wait(
        self, database_instance: DatabaseInstance, timeout=timedelta(minutes=20)
    ) -> DatabaseInstance:
        return self.create_database_instance(database_instance=database_instance).result(timeout=timeout)

    def create_database_instance_role(
        self,
        instance_name: str,
        database_instance_role: DatabaseInstanceRole,
        *,
        database_instance_name: Optional[str] = None,
    ) -> DatabaseInstanceRole:
        """Create a role for a Database Instance.

        :param instance_name: str
        :param database_instance_role: :class:`DatabaseInstanceRole`
        :param database_instance_name: str (optional)

        :returns: :class:`DatabaseInstanceRole`
        """

        body = database_instance_role.as_dict()
        query = {}
        if database_instance_name is not None:
            query["database_instance_name"] = database_instance_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", f"/api/2.0/database/instances/{instance_name}/roles", query=query, body=body, headers=headers
        )
        return DatabaseInstanceRole.from_dict(res)

    def create_database_table(self, table: DatabaseTable) -> DatabaseTable:
        """Create a Database Table. Useful for registering pre-existing PG tables in UC. See
        CreateSyncedDatabaseTable for creating synced tables in PG from a source table in UC.

        :param table: :class:`DatabaseTable`

        :returns: :class:`DatabaseTable`
        """

        body = table.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/database/tables", body=body, headers=headers)
        return DatabaseTable.from_dict(res)

    def create_synced_database_table(self, synced_table: SyncedDatabaseTable) -> SyncedDatabaseTable:
        """Create a Synced Database Table.

        :param synced_table: :class:`SyncedDatabaseTable`

        :returns: :class:`SyncedDatabaseTable`
        """

        body = synced_table.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/database/synced_tables", body=body, headers=headers)
        return SyncedDatabaseTable.from_dict(res)

    def delete_database_catalog(self, name: str):
        """Delete a Database Catalog.

        :param name: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/database/catalogs/{name}", headers=headers)

    def delete_database_instance(self, name: str, *, force: Optional[bool] = None, purge: Optional[bool] = None):
        """Delete a Database Instance.

        :param name: str
          Name of the instance to delete.
        :param force: bool (optional)
          By default, a instance cannot be deleted if it has descendant instances created via PITR. If this
          flag is specified as true, all descendent instances will be deleted as well.
        :param purge: bool (optional)
          Deprecated. Omitting the field or setting it to true will result in the field being hard deleted.
          Setting a value of false will throw a bad request.


        """

        query = {}
        if force is not None:
            query["force"] = force
        if purge is not None:
            query["purge"] = purge
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/database/instances/{name}", query=query, headers=headers)

    def delete_database_instance_role(
        self,
        instance_name: str,
        name: str,
        *,
        allow_missing: Optional[bool] = None,
        reassign_owned_to: Optional[str] = None,
    ):
        """Deletes a role for a Database Instance.

        :param instance_name: str
        :param name: str
        :param allow_missing: bool (optional)
          This is the AIP standard name for the equivalent of Postgres' `IF EXISTS` option
        :param reassign_owned_to: str (optional)


        """

        query = {}
        if allow_missing is not None:
            query["allow_missing"] = allow_missing
        if reassign_owned_to is not None:
            query["reassign_owned_to"] = reassign_owned_to
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/2.0/database/instances/{instance_name}/roles/{name}", query=query, headers=headers
        )

    def delete_database_table(self, name: str):
        """Delete a Database Table.

        :param name: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/database/tables/{name}", headers=headers)

    def delete_synced_database_table(self, name: str, *, purge_data: Optional[bool] = None):
        """Delete a Synced Database Table.

        :param name: str
        :param purge_data: bool (optional)
          Optional. When set to true, the actual PostgreSQL table will be dropped from the database.


        """

        query = {}
        if purge_data is not None:
            query["purge_data"] = purge_data
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/database/synced_tables/{name}", query=query, headers=headers)

    def find_database_instance_by_uid(self, *, uid: Optional[str] = None) -> DatabaseInstance:
        """Find a Database Instance by uid.

        :param uid: str (optional)
          UID of the cluster to get.

        :returns: :class:`DatabaseInstance`
        """

        query = {}
        if uid is not None:
            query["uid"] = uid
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/database/instances:findByUid", query=query, headers=headers)
        return DatabaseInstance.from_dict(res)

    def generate_database_credential(
        self,
        *,
        claims: Optional[List[RequestedClaims]] = None,
        instance_names: Optional[List[str]] = None,
        request_id: Optional[str] = None,
    ) -> DatabaseCredential:
        """Generates a credential that can be used to access database instances.

        :param claims: List[:class:`RequestedClaims`] (optional)
          The returned token will be scoped to the union of instance_names and instances containing the
          specified UC tables, so instance_names is allowed to be empty.
        :param instance_names: List[str] (optional)
          Instances to which the token will be scoped.
        :param request_id: str (optional)

        :returns: :class:`DatabaseCredential`
        """

        if request_id is None or request_id == "":
            request_id = str(uuid.uuid4())
        body = {}
        if claims is not None:
            body["claims"] = [v.as_dict() for v in claims]
        if instance_names is not None:
            body["instance_names"] = [v for v in instance_names]
        if request_id is not None:
            body["request_id"] = request_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/database/credentials", body=body, headers=headers)
        return DatabaseCredential.from_dict(res)

    def get_database_catalog(self, name: str) -> DatabaseCatalog:
        """Get a Database Catalog.

        :param name: str

        :returns: :class:`DatabaseCatalog`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/database/catalogs/{name}", headers=headers)
        return DatabaseCatalog.from_dict(res)

    def get_database_instance(self, name: str) -> DatabaseInstance:
        """Get a Database Instance.

        :param name: str
          Name of the cluster to get.

        :returns: :class:`DatabaseInstance`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/database/instances/{name}", headers=headers)
        return DatabaseInstance.from_dict(res)

    def get_database_instance_role(self, instance_name: str, name: str) -> DatabaseInstanceRole:
        """Gets a role for a Database Instance.

        :param instance_name: str
        :param name: str

        :returns: :class:`DatabaseInstanceRole`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/database/instances/{instance_name}/roles/{name}", headers=headers)
        return DatabaseInstanceRole.from_dict(res)

    def get_database_table(self, name: str) -> DatabaseTable:
        """Get a Database Table.

        :param name: str

        :returns: :class:`DatabaseTable`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/database/tables/{name}", headers=headers)
        return DatabaseTable.from_dict(res)

    def get_synced_database_table(self, name: str) -> SyncedDatabaseTable:
        """Get a Synced Database Table.

        :param name: str

        :returns: :class:`SyncedDatabaseTable`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/database/synced_tables/{name}", headers=headers)
        return SyncedDatabaseTable.from_dict(res)

    def list_database_catalogs(
        self, instance_name: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[DatabaseCatalog]:
        """This API is currently unimplemented, but exposed for Terraform support.

        :param instance_name: str
          Name of the instance to get database catalogs for.
        :param page_size: int (optional)
          Upper bound for items returned.
        :param page_token: str (optional)
          Pagination token to go to the next page of synced database tables. Requests first page if absent.

        :returns: Iterator over :class:`DatabaseCatalog`
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
            json = self._api.do(
                "GET", f"/api/2.0/database/instances/{instance_name}/catalogs", query=query, headers=headers
            )
            if "database_catalogs" in json:
                for v in json["database_catalogs"]:
                    yield DatabaseCatalog.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_database_instance_roles(
        self, instance_name: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[DatabaseInstanceRole]:
        """START OF PG ROLE APIs Section These APIs are marked a PUBLIC with stage < PUBLIC_PREVIEW. With more
        recent Lakebase V2 plans, we don't plan to ever advance these to PUBLIC_PREVIEW. These APIs will
        remain effectively undocumented/UI-only and we'll aim for a new public roles API as part of V2 PuPr.

        :param instance_name: str
        :param page_size: int (optional)
          Upper bound for items returned.
        :param page_token: str (optional)
          Pagination token to go to the next page of Database Instances. Requests first page if absent.

        :returns: Iterator over :class:`DatabaseInstanceRole`
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
            json = self._api.do(
                "GET", f"/api/2.0/database/instances/{instance_name}/roles", query=query, headers=headers
            )
            if "database_instance_roles" in json:
                for v in json["database_instance_roles"]:
                    yield DatabaseInstanceRole.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_database_instances(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[DatabaseInstance]:
        """List Database Instances.

        :param page_size: int (optional)
          Upper bound for items returned. The maximum value is 100.
        :param page_token: str (optional)
          Pagination token to go to the next page of Database Instances. Requests first page if absent.

        :returns: Iterator over :class:`DatabaseInstance`
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
            json = self._api.do("GET", "/api/2.0/database/instances", query=query, headers=headers)
            if "database_instances" in json:
                for v in json["database_instances"]:
                    yield DatabaseInstance.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_synced_database_tables(
        self, instance_name: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[SyncedDatabaseTable]:
        """This API is currently unimplemented, but exposed for Terraform support.

        :param instance_name: str
          Name of the instance to get synced tables for.
        :param page_size: int (optional)
          Upper bound for items returned.
        :param page_token: str (optional)
          Pagination token to go to the next page of synced database tables. Requests first page if absent.

        :returns: Iterator over :class:`SyncedDatabaseTable`
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
            json = self._api.do(
                "GET", f"/api/2.0/database/instances/{instance_name}/synced_tables", query=query, headers=headers
            )
            if "synced_tables" in json:
                for v in json["synced_tables"]:
                    yield SyncedDatabaseTable.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_database_catalog(
        self, name: str, database_catalog: DatabaseCatalog, update_mask: str
    ) -> DatabaseCatalog:
        """This API is currently unimplemented, but exposed for Terraform support.

        :param name: str
          The name of the catalog in UC.
        :param database_catalog: :class:`DatabaseCatalog`
          Note that updating a database catalog is not yet supported.
        :param update_mask: str
          The list of fields to update. Setting this field is not yet supported.

        :returns: :class:`DatabaseCatalog`
        """

        body = database_catalog.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.0/database/catalogs/{name}", query=query, body=body, headers=headers)
        return DatabaseCatalog.from_dict(res)

    def update_database_instance(
        self, name: str, database_instance: DatabaseInstance, update_mask: str
    ) -> DatabaseInstance:
        """Update a Database Instance.

        :param name: str
          The name of the instance. This is the unique identifier for the instance.
        :param database_instance: :class:`DatabaseInstance`
        :param update_mask: str
          The list of fields to update. If unspecified, all fields will be updated when possible. To wipe out
          custom_tags, specify custom_tags in the update_mask with an empty custom_tags map.

        :returns: :class:`DatabaseInstance`
        """

        body = database_instance.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.0/database/instances/{name}", query=query, body=body, headers=headers)
        return DatabaseInstance.from_dict(res)

    def update_synced_database_table(
        self, name: str, synced_table: SyncedDatabaseTable, update_mask: str
    ) -> SyncedDatabaseTable:
        """This API is currently unimplemented, but exposed for Terraform support.

        :param name: str
          Full three-part (catalog, schema, table) name of the table.
        :param synced_table: :class:`SyncedDatabaseTable`
          Note that updating a synced database table is not yet supported.
        :param update_mask: str
          The list of fields to update. Setting this field is not yet supported.

        :returns: :class:`SyncedDatabaseTable`
        """

        body = synced_table.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.0/database/synced_tables/{name}", query=query, body=body, headers=headers)
        return SyncedDatabaseTable.from_dict(res)
