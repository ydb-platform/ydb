# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from databricks.sdk.client_types import HostType
from databricks.sdk.common import lro
from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.retries import RetryError, poll
from databricks.sdk.service._internal import (_duration, _enum, _from_dict,
                                              _repeated_dict, _repeated_enum,
                                              _timestamp)

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class Branch:
    create_time: Optional[Timestamp] = None
    """A timestamp indicating when the branch was created."""

    name: Optional[str] = None
    """Output only. The full resource path of the branch. Format:
    projects/{project_id}/branches/{branch_id}"""

    parent: Optional[str] = None
    """The project containing this branch (API resource hierarchy). Format: projects/{project_id}
    
    Note: This field indicates where the branch exists in the resource hierarchy. For point-in-time
    branching from another branch, see `status.source_branch`."""

    spec: Optional[BranchSpec] = None
    """The spec contains the branch configuration."""

    status: Optional[BranchStatus] = None
    """The current status of a Branch."""

    uid: Optional[str] = None
    """System-generated unique ID for the branch."""

    update_time: Optional[Timestamp] = None
    """A timestamp indicating when the branch was last updated."""

    def as_dict(self) -> dict:
        """Serializes the Branch into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time.ToJsonString()
        if self.name is not None:
            body["name"] = self.name
        if self.parent is not None:
            body["parent"] = self.parent
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.uid is not None:
            body["uid"] = self.uid
        if self.update_time is not None:
            body["update_time"] = self.update_time.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Branch into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.name is not None:
            body["name"] = self.name
        if self.parent is not None:
            body["parent"] = self.parent
        if self.spec:
            body["spec"] = self.spec
        if self.status:
            body["status"] = self.status
        if self.uid is not None:
            body["uid"] = self.uid
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Branch:
        """Deserializes the Branch from a dictionary."""
        return cls(
            create_time=_timestamp(d, "create_time"),
            name=d.get("name", None),
            parent=d.get("parent", None),
            spec=_from_dict(d, "spec", BranchSpec),
            status=_from_dict(d, "status", BranchStatus),
            uid=d.get("uid", None),
            update_time=_timestamp(d, "update_time"),
        )


@dataclass
class BranchOperationMetadata:
    def as_dict(self) -> dict:
        """Serializes the BranchOperationMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BranchOperationMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BranchOperationMetadata:
        """Deserializes the BranchOperationMetadata from a dictionary."""
        return cls()


@dataclass
class BranchSpec:
    expire_time: Optional[Timestamp] = None
    """Absolute expiration timestamp. When set, the branch will expire at this time."""

    is_protected: Optional[bool] = None
    """When set to true, protects the branch from deletion and reset. Associated compute endpoints and
    the project cannot be deleted while the branch is protected."""

    no_expiry: Optional[bool] = None
    """Explicitly disable expiration. When set to true, the branch will not expire. If set to false,
    the request is invalid; provide either ttl or expire_time instead."""

    source_branch: Optional[str] = None
    """The name of the source branch from which this branch was created (data lineage for point-in-time
    recovery). If not specified, defaults to the project's default branch. Format:
    projects/{project_id}/branches/{branch_id}"""

    source_branch_lsn: Optional[str] = None
    """The Log Sequence Number (LSN) on the source branch from which this branch was created."""

    source_branch_time: Optional[Timestamp] = None
    """The point in time on the source branch from which this branch was created."""

    ttl: Optional[Duration] = None
    """Relative time-to-live duration. When set, the branch will expire at creation_time + ttl."""

    def as_dict(self) -> dict:
        """Serializes the BranchSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.expire_time is not None:
            body["expire_time"] = self.expire_time.ToJsonString()
        if self.is_protected is not None:
            body["is_protected"] = self.is_protected
        if self.no_expiry is not None:
            body["no_expiry"] = self.no_expiry
        if self.source_branch is not None:
            body["source_branch"] = self.source_branch
        if self.source_branch_lsn is not None:
            body["source_branch_lsn"] = self.source_branch_lsn
        if self.source_branch_time is not None:
            body["source_branch_time"] = self.source_branch_time.ToJsonString()
        if self.ttl is not None:
            body["ttl"] = self.ttl.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BranchSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.expire_time is not None:
            body["expire_time"] = self.expire_time
        if self.is_protected is not None:
            body["is_protected"] = self.is_protected
        if self.no_expiry is not None:
            body["no_expiry"] = self.no_expiry
        if self.source_branch is not None:
            body["source_branch"] = self.source_branch
        if self.source_branch_lsn is not None:
            body["source_branch_lsn"] = self.source_branch_lsn
        if self.source_branch_time is not None:
            body["source_branch_time"] = self.source_branch_time
        if self.ttl is not None:
            body["ttl"] = self.ttl
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BranchSpec:
        """Deserializes the BranchSpec from a dictionary."""
        return cls(
            expire_time=_timestamp(d, "expire_time"),
            is_protected=d.get("is_protected", None),
            no_expiry=d.get("no_expiry", None),
            source_branch=d.get("source_branch", None),
            source_branch_lsn=d.get("source_branch_lsn", None),
            source_branch_time=_timestamp(d, "source_branch_time"),
            ttl=_duration(d, "ttl"),
        )


@dataclass
class BranchStatus:
    current_state: Optional[BranchStatusState] = None
    """The branch's state, indicating if it is initializing, ready for use, or archived."""

    default: Optional[bool] = None
    """Whether the branch is the project's default branch."""

    expire_time: Optional[Timestamp] = None
    """Absolute expiration time for the branch. Empty if expiration is disabled."""

    is_protected: Optional[bool] = None
    """Whether the branch is protected."""

    logical_size_bytes: Optional[int] = None
    """The logical size of the branch."""

    pending_state: Optional[BranchStatusState] = None
    """The pending state of the branch, if a state transition is in progress."""

    source_branch: Optional[str] = None
    """The name of the source branch from which this branch was created. Format:
    projects/{project_id}/branches/{branch_id}"""

    source_branch_lsn: Optional[str] = None
    """The Log Sequence Number (LSN) on the source branch from which this branch was created."""

    source_branch_time: Optional[Timestamp] = None
    """The point in time on the source branch from which this branch was created."""

    state_change_time: Optional[Timestamp] = None
    """A timestamp indicating when the `current_state` began."""

    def as_dict(self) -> dict:
        """Serializes the BranchStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.current_state is not None:
            body["current_state"] = self.current_state.value
        if self.default is not None:
            body["default"] = self.default
        if self.expire_time is not None:
            body["expire_time"] = self.expire_time.ToJsonString()
        if self.is_protected is not None:
            body["is_protected"] = self.is_protected
        if self.logical_size_bytes is not None:
            body["logical_size_bytes"] = self.logical_size_bytes
        if self.pending_state is not None:
            body["pending_state"] = self.pending_state.value
        if self.source_branch is not None:
            body["source_branch"] = self.source_branch
        if self.source_branch_lsn is not None:
            body["source_branch_lsn"] = self.source_branch_lsn
        if self.source_branch_time is not None:
            body["source_branch_time"] = self.source_branch_time.ToJsonString()
        if self.state_change_time is not None:
            body["state_change_time"] = self.state_change_time.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BranchStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.current_state is not None:
            body["current_state"] = self.current_state
        if self.default is not None:
            body["default"] = self.default
        if self.expire_time is not None:
            body["expire_time"] = self.expire_time
        if self.is_protected is not None:
            body["is_protected"] = self.is_protected
        if self.logical_size_bytes is not None:
            body["logical_size_bytes"] = self.logical_size_bytes
        if self.pending_state is not None:
            body["pending_state"] = self.pending_state
        if self.source_branch is not None:
            body["source_branch"] = self.source_branch
        if self.source_branch_lsn is not None:
            body["source_branch_lsn"] = self.source_branch_lsn
        if self.source_branch_time is not None:
            body["source_branch_time"] = self.source_branch_time
        if self.state_change_time is not None:
            body["state_change_time"] = self.state_change_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BranchStatus:
        """Deserializes the BranchStatus from a dictionary."""
        return cls(
            current_state=_enum(d, "current_state", BranchStatusState),
            default=d.get("default", None),
            expire_time=_timestamp(d, "expire_time"),
            is_protected=d.get("is_protected", None),
            logical_size_bytes=d.get("logical_size_bytes", None),
            pending_state=_enum(d, "pending_state", BranchStatusState),
            source_branch=d.get("source_branch", None),
            source_branch_lsn=d.get("source_branch_lsn", None),
            source_branch_time=_timestamp(d, "source_branch_time"),
            state_change_time=_timestamp(d, "state_change_time"),
        )


class BranchStatusState(Enum):
    """The state of the branch."""

    ARCHIVED = "ARCHIVED"
    IMPORTING = "IMPORTING"
    INIT = "INIT"
    READY = "READY"
    RESETTING = "RESETTING"


@dataclass
class DatabaseCredential:
    expire_time: Optional[Timestamp] = None
    """Timestamp in UTC of when this credential expires."""

    token: Optional[str] = None
    """The OAuth token that can be used as a password when connecting to a database."""

    def as_dict(self) -> dict:
        """Serializes the DatabaseCredential into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.expire_time is not None:
            body["expire_time"] = self.expire_time.ToJsonString()
        if self.token is not None:
            body["token"] = self.token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatabaseCredential into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.expire_time is not None:
            body["expire_time"] = self.expire_time
        if self.token is not None:
            body["token"] = self.token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatabaseCredential:
        """Deserializes the DatabaseCredential from a dictionary."""
        return cls(expire_time=_timestamp(d, "expire_time"), token=d.get("token", None))


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
class Endpoint:
    create_time: Optional[Timestamp] = None
    """A timestamp indicating when the compute endpoint was created."""

    name: Optional[str] = None
    """Output only. The full resource path of the endpoint. Format:
    projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}"""

    parent: Optional[str] = None
    """The branch containing this endpoint (API resource hierarchy). Format:
    projects/{project_id}/branches/{branch_id}"""

    spec: Optional[EndpointSpec] = None
    """The spec contains the compute endpoint configuration, including autoscaling limits, suspend
    timeout, and disabled state."""

    status: Optional[EndpointStatus] = None
    """Current operational status of the compute endpoint."""

    uid: Optional[str] = None
    """System-generated unique ID for the endpoint."""

    update_time: Optional[Timestamp] = None
    """A timestamp indicating when the compute endpoint was last updated."""

    def as_dict(self) -> dict:
        """Serializes the Endpoint into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time.ToJsonString()
        if self.name is not None:
            body["name"] = self.name
        if self.parent is not None:
            body["parent"] = self.parent
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.uid is not None:
            body["uid"] = self.uid
        if self.update_time is not None:
            body["update_time"] = self.update_time.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Endpoint into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.name is not None:
            body["name"] = self.name
        if self.parent is not None:
            body["parent"] = self.parent
        if self.spec:
            body["spec"] = self.spec
        if self.status:
            body["status"] = self.status
        if self.uid is not None:
            body["uid"] = self.uid
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Endpoint:
        """Deserializes the Endpoint from a dictionary."""
        return cls(
            create_time=_timestamp(d, "create_time"),
            name=d.get("name", None),
            parent=d.get("parent", None),
            spec=_from_dict(d, "spec", EndpointSpec),
            status=_from_dict(d, "status", EndpointStatus),
            uid=d.get("uid", None),
            update_time=_timestamp(d, "update_time"),
        )


@dataclass
class EndpointGroupSpec:
    min: int
    """The minimum number of computes in the endpoint group. Currently, this must be equal to max. This
    must be greater than or equal to 1."""

    max: int
    """The maximum number of computes in the endpoint group. Currently, this must be equal to min. Set
    to 1 for single compute endpoints, to disable HA. To manually suspend all computes in an
    endpoint group, set disabled to true on the EndpointSpec."""

    enable_readable_secondaries: Optional[bool] = None
    """Whether to allow read-only connections to read-write endpoints. Only relevant for read-write
    endpoints where size.max > 1."""

    def as_dict(self) -> dict:
        """Serializes the EndpointGroupSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enable_readable_secondaries is not None:
            body["enable_readable_secondaries"] = self.enable_readable_secondaries
        if self.max is not None:
            body["max"] = self.max
        if self.min is not None:
            body["min"] = self.min
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointGroupSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enable_readable_secondaries is not None:
            body["enable_readable_secondaries"] = self.enable_readable_secondaries
        if self.max is not None:
            body["max"] = self.max
        if self.min is not None:
            body["min"] = self.min
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointGroupSpec:
        """Deserializes the EndpointGroupSpec from a dictionary."""
        return cls(
            enable_readable_secondaries=d.get("enable_readable_secondaries", None),
            max=d.get("max", None),
            min=d.get("min", None),
        )


@dataclass
class EndpointGroupStatus:
    min: int
    """The minimum number of computes in the endpoint group. Currently, this must be equal to max. This
    must be greater than or equal to 1."""

    max: int
    """The maximum number of computes in the endpoint group. Currently, this must be equal to min. Set
    to 1 for single compute endpoints, to disable HA. To manually suspend all computes in an
    endpoint group, set disabled to true on the EndpointSpec."""

    enable_readable_secondaries: Optional[bool] = None
    """Whether read-only connections to read-write endpoints are allowed. Only relevant if read
    replicas are configured by specifying size.max > 1."""

    def as_dict(self) -> dict:
        """Serializes the EndpointGroupStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enable_readable_secondaries is not None:
            body["enable_readable_secondaries"] = self.enable_readable_secondaries
        if self.max is not None:
            body["max"] = self.max
        if self.min is not None:
            body["min"] = self.min
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointGroupStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enable_readable_secondaries is not None:
            body["enable_readable_secondaries"] = self.enable_readable_secondaries
        if self.max is not None:
            body["max"] = self.max
        if self.min is not None:
            body["min"] = self.min
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointGroupStatus:
        """Deserializes the EndpointGroupStatus from a dictionary."""
        return cls(
            enable_readable_secondaries=d.get("enable_readable_secondaries", None),
            max=d.get("max", None),
            min=d.get("min", None),
        )


@dataclass
class EndpointHosts:
    """Encapsulates various hostnames (r/w or r/o, pooled or not) for an endpoint."""

    host: Optional[str] = None
    """The hostname to connect to this endpoint. For read-write endpoints, this is a read-write
    hostname which connects to the primary compute. For read-only endpoints, this is a read-only
    hostname which allows read-only operations."""

    read_only_host: Optional[str] = None
    """An optionally defined read-only host for the endpoint, without pooling. For read-only endpoints,
    this attribute is always defined and is equivalent to host. For read-write endpoints, this
    attribute is defined if the enclosing endpoint is a group with greater than 1 computes
    configured, and has readable secondaries enabled."""

    def as_dict(self) -> dict:
        """Serializes the EndpointHosts into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.host is not None:
            body["host"] = self.host
        if self.read_only_host is not None:
            body["read_only_host"] = self.read_only_host
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointHosts into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.host is not None:
            body["host"] = self.host
        if self.read_only_host is not None:
            body["read_only_host"] = self.read_only_host
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointHosts:
        """Deserializes the EndpointHosts from a dictionary."""
        return cls(host=d.get("host", None), read_only_host=d.get("read_only_host", None))


@dataclass
class EndpointOperationMetadata:
    def as_dict(self) -> dict:
        """Serializes the EndpointOperationMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointOperationMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointOperationMetadata:
        """Deserializes the EndpointOperationMetadata from a dictionary."""
        return cls()


@dataclass
class EndpointSettings:
    """A collection of settings for a compute endpoint."""

    pg_settings: Optional[Dict[str, str]] = None
    """A raw representation of Postgres settings."""

    def as_dict(self) -> dict:
        """Serializes the EndpointSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.pg_settings:
            body["pg_settings"] = self.pg_settings
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.pg_settings:
            body["pg_settings"] = self.pg_settings
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointSettings:
        """Deserializes the EndpointSettings from a dictionary."""
        return cls(pg_settings=d.get("pg_settings", None))


@dataclass
class EndpointSpec:
    endpoint_type: EndpointType
    """The endpoint type. A branch can only have one READ_WRITE endpoint."""

    autoscaling_limit_max_cu: Optional[float] = None
    """The maximum number of Compute Units. Minimum value is 0.5."""

    autoscaling_limit_min_cu: Optional[float] = None
    """The minimum number of Compute Units. Minimum value is 0.5."""

    disabled: Optional[bool] = None
    """Whether to restrict connections to the compute endpoint. Enabling this option schedules a
    suspend compute operation. A disabled compute endpoint cannot be enabled by a connection or
    console action."""

    group: Optional[EndpointGroupSpec] = None
    """Settings for optional HA configuration of the endpoint. If unspecified, the endpoint defaults to
    non HA settings, with a single compute backing the endpoint (and no readable secondaries for
    Read/Write endpoints)."""

    no_suspension: Optional[bool] = None
    """When set to true, explicitly disables automatic suspension (never suspend). Should be set to
    true when provided."""

    settings: Optional[EndpointSettings] = None

    suspend_timeout_duration: Optional[Duration] = None
    """Duration of inactivity after which the compute endpoint is automatically suspended. If specified
    should be between 60s and 604800s (1 minute to 1 week)."""

    def as_dict(self) -> dict:
        """Serializes the EndpointSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.autoscaling_limit_max_cu is not None:
            body["autoscaling_limit_max_cu"] = self.autoscaling_limit_max_cu
        if self.autoscaling_limit_min_cu is not None:
            body["autoscaling_limit_min_cu"] = self.autoscaling_limit_min_cu
        if self.disabled is not None:
            body["disabled"] = self.disabled
        if self.endpoint_type is not None:
            body["endpoint_type"] = self.endpoint_type.value
        if self.group:
            body["group"] = self.group.as_dict()
        if self.no_suspension is not None:
            body["no_suspension"] = self.no_suspension
        if self.settings:
            body["settings"] = self.settings.as_dict()
        if self.suspend_timeout_duration is not None:
            body["suspend_timeout_duration"] = self.suspend_timeout_duration.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.autoscaling_limit_max_cu is not None:
            body["autoscaling_limit_max_cu"] = self.autoscaling_limit_max_cu
        if self.autoscaling_limit_min_cu is not None:
            body["autoscaling_limit_min_cu"] = self.autoscaling_limit_min_cu
        if self.disabled is not None:
            body["disabled"] = self.disabled
        if self.endpoint_type is not None:
            body["endpoint_type"] = self.endpoint_type
        if self.group:
            body["group"] = self.group
        if self.no_suspension is not None:
            body["no_suspension"] = self.no_suspension
        if self.settings:
            body["settings"] = self.settings
        if self.suspend_timeout_duration is not None:
            body["suspend_timeout_duration"] = self.suspend_timeout_duration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointSpec:
        """Deserializes the EndpointSpec from a dictionary."""
        return cls(
            autoscaling_limit_max_cu=d.get("autoscaling_limit_max_cu", None),
            autoscaling_limit_min_cu=d.get("autoscaling_limit_min_cu", None),
            disabled=d.get("disabled", None),
            endpoint_type=_enum(d, "endpoint_type", EndpointType),
            group=_from_dict(d, "group", EndpointGroupSpec),
            no_suspension=d.get("no_suspension", None),
            settings=_from_dict(d, "settings", EndpointSettings),
            suspend_timeout_duration=_duration(d, "suspend_timeout_duration"),
        )


@dataclass
class EndpointStatus:
    autoscaling_limit_max_cu: Optional[float] = None
    """The maximum number of Compute Units."""

    autoscaling_limit_min_cu: Optional[float] = None
    """The minimum number of Compute Units."""

    current_state: Optional[EndpointStatusState] = None

    disabled: Optional[bool] = None
    """Whether to restrict connections to the compute endpoint. Enabling this option schedules a
    suspend compute operation. A disabled compute endpoint cannot be enabled by a connection or
    console action."""

    endpoint_type: Optional[EndpointType] = None
    """The endpoint type. A branch can only have one READ_WRITE endpoint."""

    group: Optional[EndpointGroupStatus] = None
    """Details on the HA configuration of the endpoint."""

    hosts: Optional[EndpointHosts] = None
    """Contains host information for connecting to the endpoint."""

    pending_state: Optional[EndpointStatusState] = None

    settings: Optional[EndpointSettings] = None

    suspend_timeout_duration: Optional[Duration] = None
    """Duration of inactivity after which the compute endpoint is automatically suspended."""

    def as_dict(self) -> dict:
        """Serializes the EndpointStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.autoscaling_limit_max_cu is not None:
            body["autoscaling_limit_max_cu"] = self.autoscaling_limit_max_cu
        if self.autoscaling_limit_min_cu is not None:
            body["autoscaling_limit_min_cu"] = self.autoscaling_limit_min_cu
        if self.current_state is not None:
            body["current_state"] = self.current_state.value
        if self.disabled is not None:
            body["disabled"] = self.disabled
        if self.endpoint_type is not None:
            body["endpoint_type"] = self.endpoint_type.value
        if self.group:
            body["group"] = self.group.as_dict()
        if self.hosts:
            body["hosts"] = self.hosts.as_dict()
        if self.pending_state is not None:
            body["pending_state"] = self.pending_state.value
        if self.settings:
            body["settings"] = self.settings.as_dict()
        if self.suspend_timeout_duration is not None:
            body["suspend_timeout_duration"] = self.suspend_timeout_duration.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.autoscaling_limit_max_cu is not None:
            body["autoscaling_limit_max_cu"] = self.autoscaling_limit_max_cu
        if self.autoscaling_limit_min_cu is not None:
            body["autoscaling_limit_min_cu"] = self.autoscaling_limit_min_cu
        if self.current_state is not None:
            body["current_state"] = self.current_state
        if self.disabled is not None:
            body["disabled"] = self.disabled
        if self.endpoint_type is not None:
            body["endpoint_type"] = self.endpoint_type
        if self.group:
            body["group"] = self.group
        if self.hosts:
            body["hosts"] = self.hosts
        if self.pending_state is not None:
            body["pending_state"] = self.pending_state
        if self.settings:
            body["settings"] = self.settings
        if self.suspend_timeout_duration is not None:
            body["suspend_timeout_duration"] = self.suspend_timeout_duration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointStatus:
        """Deserializes the EndpointStatus from a dictionary."""
        return cls(
            autoscaling_limit_max_cu=d.get("autoscaling_limit_max_cu", None),
            autoscaling_limit_min_cu=d.get("autoscaling_limit_min_cu", None),
            current_state=_enum(d, "current_state", EndpointStatusState),
            disabled=d.get("disabled", None),
            endpoint_type=_enum(d, "endpoint_type", EndpointType),
            group=_from_dict(d, "group", EndpointGroupStatus),
            hosts=_from_dict(d, "hosts", EndpointHosts),
            pending_state=_enum(d, "pending_state", EndpointStatusState),
            settings=_from_dict(d, "settings", EndpointSettings),
            suspend_timeout_duration=_duration(d, "suspend_timeout_duration"),
        )


class EndpointStatusState(Enum):
    """The state of the compute endpoint."""

    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    IDLE = "IDLE"
    INIT = "INIT"


class EndpointType(Enum):
    """The compute endpoint type. Either `read_write` or `read_only`."""

    ENDPOINT_TYPE_READ_ONLY = "ENDPOINT_TYPE_READ_ONLY"
    ENDPOINT_TYPE_READ_WRITE = "ENDPOINT_TYPE_READ_WRITE"


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
class InitialEndpointSpec:
    group: Optional[EndpointGroupSpec] = None
    """Settings for HA configuration of the endpoint"""

    def as_dict(self) -> dict:
        """Serializes the InitialEndpointSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group:
            body["group"] = self.group.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the InitialEndpointSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group:
            body["group"] = self.group
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> InitialEndpointSpec:
        """Deserializes the InitialEndpointSpec from a dictionary."""
        return cls(group=_from_dict(d, "group", EndpointGroupSpec))


@dataclass
class ListBranchesResponse:
    branches: Optional[List[Branch]] = None
    """List of branches in the project."""

    next_page_token: Optional[str] = None
    """Token to request the next page of branches."""

    def as_dict(self) -> dict:
        """Serializes the ListBranchesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.branches:
            body["branches"] = [v.as_dict() for v in self.branches]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListBranchesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.branches:
            body["branches"] = self.branches
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListBranchesResponse:
        """Deserializes the ListBranchesResponse from a dictionary."""
        return cls(branches=_repeated_dict(d, "branches", Branch), next_page_token=d.get("next_page_token", None))


@dataclass
class ListEndpointsResponse:
    endpoints: Optional[List[Endpoint]] = None
    """List of compute endpoints in the branch."""

    next_page_token: Optional[str] = None
    """Token to request the next page of compute endpoints."""

    def as_dict(self) -> dict:
        """Serializes the ListEndpointsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.endpoints:
            body["endpoints"] = [v.as_dict() for v in self.endpoints]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListEndpointsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.endpoints:
            body["endpoints"] = self.endpoints
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListEndpointsResponse:
        """Deserializes the ListEndpointsResponse from a dictionary."""
        return cls(endpoints=_repeated_dict(d, "endpoints", Endpoint), next_page_token=d.get("next_page_token", None))


@dataclass
class ListProjectsResponse:
    next_page_token: Optional[str] = None
    """Token to request the next page of projects."""

    projects: Optional[List[Project]] = None
    """List of all projects in the workspace that the user has permission to access."""

    def as_dict(self) -> dict:
        """Serializes the ListProjectsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.projects:
            body["projects"] = [v.as_dict() for v in self.projects]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListProjectsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.projects:
            body["projects"] = self.projects
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListProjectsResponse:
        """Deserializes the ListProjectsResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), projects=_repeated_dict(d, "projects", Project))


@dataclass
class ListRolesResponse:
    next_page_token: Optional[str] = None
    """Token to request the next page of Postgres roles."""

    roles: Optional[List[Role]] = None
    """List of Postgres roles in the branch."""

    def as_dict(self) -> dict:
        """Serializes the ListRolesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.roles:
            body["roles"] = [v.as_dict() for v in self.roles]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListRolesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.roles:
            body["roles"] = self.roles
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListRolesResponse:
        """Deserializes the ListRolesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), roles=_repeated_dict(d, "roles", Role))


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
class Project:
    create_time: Optional[Timestamp] = None
    """A timestamp indicating when the project was created."""

    initial_endpoint_spec: Optional[InitialEndpointSpec] = None
    """Configuration settings for the initial Read/Write endpoint created inside the default branch for
    a newly created project. If omitted, the initial endpoint created will have default settings,
    without high availability configured. This field does not apply to any endpoints created after
    project creation. Use spec.default_endpoint_settings to configure default settings for endpoints
    created after project creation."""

    name: Optional[str] = None
    """Output only. The full resource path of the project. Format: projects/{project_id}"""

    spec: Optional[ProjectSpec] = None
    """The spec contains the project configuration, including display_name, pg_version (Postgres
    version), history_retention_duration, and default_endpoint_settings."""

    status: Optional[ProjectStatus] = None
    """The current status of a Project."""

    uid: Optional[str] = None
    """System-generated unique ID for the project."""

    update_time: Optional[Timestamp] = None
    """A timestamp indicating when the project was last updated."""

    def as_dict(self) -> dict:
        """Serializes the Project into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time.ToJsonString()
        if self.initial_endpoint_spec:
            body["initial_endpoint_spec"] = self.initial_endpoint_spec.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.uid is not None:
            body["uid"] = self.uid
        if self.update_time is not None:
            body["update_time"] = self.update_time.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Project into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.initial_endpoint_spec:
            body["initial_endpoint_spec"] = self.initial_endpoint_spec
        if self.name is not None:
            body["name"] = self.name
        if self.spec:
            body["spec"] = self.spec
        if self.status:
            body["status"] = self.status
        if self.uid is not None:
            body["uid"] = self.uid
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Project:
        """Deserializes the Project from a dictionary."""
        return cls(
            create_time=_timestamp(d, "create_time"),
            initial_endpoint_spec=_from_dict(d, "initial_endpoint_spec", InitialEndpointSpec),
            name=d.get("name", None),
            spec=_from_dict(d, "spec", ProjectSpec),
            status=_from_dict(d, "status", ProjectStatus),
            uid=d.get("uid", None),
            update_time=_timestamp(d, "update_time"),
        )


@dataclass
class ProjectCustomTag:
    key: Optional[str] = None
    """The key of the custom tag."""

    value: Optional[str] = None
    """The value of the custom tag."""

    def as_dict(self) -> dict:
        """Serializes the ProjectCustomTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProjectCustomTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProjectCustomTag:
        """Deserializes the ProjectCustomTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class ProjectDefaultEndpointSettings:
    """A collection of settings for a compute endpoint."""

    autoscaling_limit_max_cu: Optional[float] = None
    """The maximum number of Compute Units. Minimum value is 0.5."""

    autoscaling_limit_min_cu: Optional[float] = None
    """The minimum number of Compute Units. Minimum value is 0.5."""

    no_suspension: Optional[bool] = None
    """When set to true, explicitly disables automatic suspension (never suspend). Should be set to
    true when provided."""

    pg_settings: Optional[Dict[str, str]] = None
    """A raw representation of Postgres settings."""

    suspend_timeout_duration: Optional[Duration] = None
    """Duration of inactivity after which the compute endpoint is automatically suspended. If specified
    should be between 60s and 604800s (1 minute to 1 week)."""

    def as_dict(self) -> dict:
        """Serializes the ProjectDefaultEndpointSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.autoscaling_limit_max_cu is not None:
            body["autoscaling_limit_max_cu"] = self.autoscaling_limit_max_cu
        if self.autoscaling_limit_min_cu is not None:
            body["autoscaling_limit_min_cu"] = self.autoscaling_limit_min_cu
        if self.no_suspension is not None:
            body["no_suspension"] = self.no_suspension
        if self.pg_settings:
            body["pg_settings"] = self.pg_settings
        if self.suspend_timeout_duration is not None:
            body["suspend_timeout_duration"] = self.suspend_timeout_duration.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProjectDefaultEndpointSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.autoscaling_limit_max_cu is not None:
            body["autoscaling_limit_max_cu"] = self.autoscaling_limit_max_cu
        if self.autoscaling_limit_min_cu is not None:
            body["autoscaling_limit_min_cu"] = self.autoscaling_limit_min_cu
        if self.no_suspension is not None:
            body["no_suspension"] = self.no_suspension
        if self.pg_settings:
            body["pg_settings"] = self.pg_settings
        if self.suspend_timeout_duration is not None:
            body["suspend_timeout_duration"] = self.suspend_timeout_duration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProjectDefaultEndpointSettings:
        """Deserializes the ProjectDefaultEndpointSettings from a dictionary."""
        return cls(
            autoscaling_limit_max_cu=d.get("autoscaling_limit_max_cu", None),
            autoscaling_limit_min_cu=d.get("autoscaling_limit_min_cu", None),
            no_suspension=d.get("no_suspension", None),
            pg_settings=d.get("pg_settings", None),
            suspend_timeout_duration=_duration(d, "suspend_timeout_duration"),
        )


@dataclass
class ProjectOperationMetadata:
    def as_dict(self) -> dict:
        """Serializes the ProjectOperationMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProjectOperationMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProjectOperationMetadata:
        """Deserializes the ProjectOperationMetadata from a dictionary."""
        return cls()


@dataclass
class ProjectSpec:
    budget_policy_id: Optional[str] = None
    """The desired budget policy to associate with the project. See status.budget_policy_id for the
    policy that is actually applied to the project."""

    custom_tags: Optional[List[ProjectCustomTag]] = None
    """Custom tags to associate with the project. Forwarded to LBM for billing and cost tracking. To
    update tags, provide the new tag list and include "spec.custom_tags" in the update_mask. To
    clear all tags, provide an empty list and include "spec.custom_tags" in the update_mask. To
    preserve existing tags, omit this field from the update_mask (or use wildcard "*" which
    auto-excludes empty tags)."""

    default_endpoint_settings: Optional[ProjectDefaultEndpointSettings] = None

    display_name: Optional[str] = None
    """Human-readable project name. Length should be between 1 and 256 characters."""

    history_retention_duration: Optional[Duration] = None
    """The number of seconds to retain the shared history for point in time recovery for all branches
    in this project. Value should be between 0s and 2592000s (up to 30 days)."""

    pg_version: Optional[int] = None
    """The major Postgres version number. Supported versions are 16 and 17."""

    def as_dict(self) -> dict:
        """Serializes the ProjectSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.custom_tags:
            body["custom_tags"] = [v.as_dict() for v in self.custom_tags]
        if self.default_endpoint_settings:
            body["default_endpoint_settings"] = self.default_endpoint_settings.as_dict()
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.history_retention_duration is not None:
            body["history_retention_duration"] = self.history_retention_duration.ToJsonString()
        if self.pg_version is not None:
            body["pg_version"] = self.pg_version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProjectSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.default_endpoint_settings:
            body["default_endpoint_settings"] = self.default_endpoint_settings
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.history_retention_duration is not None:
            body["history_retention_duration"] = self.history_retention_duration
        if self.pg_version is not None:
            body["pg_version"] = self.pg_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProjectSpec:
        """Deserializes the ProjectSpec from a dictionary."""
        return cls(
            budget_policy_id=d.get("budget_policy_id", None),
            custom_tags=_repeated_dict(d, "custom_tags", ProjectCustomTag),
            default_endpoint_settings=_from_dict(d, "default_endpoint_settings", ProjectDefaultEndpointSettings),
            display_name=d.get("display_name", None),
            history_retention_duration=_duration(d, "history_retention_duration"),
            pg_version=d.get("pg_version", None),
        )


@dataclass
class ProjectStatus:
    branch_logical_size_limit_bytes: Optional[int] = None
    """The logical size limit for a branch."""

    budget_policy_id: Optional[str] = None
    """The budget policy that is applied to the project."""

    custom_tags: Optional[List[ProjectCustomTag]] = None
    """The effective custom tags associated with the project."""

    default_endpoint_settings: Optional[ProjectDefaultEndpointSettings] = None
    """The effective default endpoint settings."""

    display_name: Optional[str] = None
    """The effective human-readable project name."""

    history_retention_duration: Optional[Duration] = None
    """The effective number of seconds to retain the shared history for point in time recovery."""

    owner: Optional[str] = None
    """The email of the project owner."""

    pg_version: Optional[int] = None
    """The effective major Postgres version number."""

    synthetic_storage_size_bytes: Optional[int] = None
    """The current space occupied by the project in storage."""

    def as_dict(self) -> dict:
        """Serializes the ProjectStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.branch_logical_size_limit_bytes is not None:
            body["branch_logical_size_limit_bytes"] = self.branch_logical_size_limit_bytes
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.custom_tags:
            body["custom_tags"] = [v.as_dict() for v in self.custom_tags]
        if self.default_endpoint_settings:
            body["default_endpoint_settings"] = self.default_endpoint_settings.as_dict()
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.history_retention_duration is not None:
            body["history_retention_duration"] = self.history_retention_duration.ToJsonString()
        if self.owner is not None:
            body["owner"] = self.owner
        if self.pg_version is not None:
            body["pg_version"] = self.pg_version
        if self.synthetic_storage_size_bytes is not None:
            body["synthetic_storage_size_bytes"] = self.synthetic_storage_size_bytes
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProjectStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.branch_logical_size_limit_bytes is not None:
            body["branch_logical_size_limit_bytes"] = self.branch_logical_size_limit_bytes
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.default_endpoint_settings:
            body["default_endpoint_settings"] = self.default_endpoint_settings
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.history_retention_duration is not None:
            body["history_retention_duration"] = self.history_retention_duration
        if self.owner is not None:
            body["owner"] = self.owner
        if self.pg_version is not None:
            body["pg_version"] = self.pg_version
        if self.synthetic_storage_size_bytes is not None:
            body["synthetic_storage_size_bytes"] = self.synthetic_storage_size_bytes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProjectStatus:
        """Deserializes the ProjectStatus from a dictionary."""
        return cls(
            branch_logical_size_limit_bytes=d.get("branch_logical_size_limit_bytes", None),
            budget_policy_id=d.get("budget_policy_id", None),
            custom_tags=_repeated_dict(d, "custom_tags", ProjectCustomTag),
            default_endpoint_settings=_from_dict(d, "default_endpoint_settings", ProjectDefaultEndpointSettings),
            display_name=d.get("display_name", None),
            history_retention_duration=_duration(d, "history_retention_duration"),
            owner=d.get("owner", None),
            pg_version=d.get("pg_version", None),
            synthetic_storage_size_bytes=d.get("synthetic_storage_size_bytes", None),
        )


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
class Role:
    """Role represents a Postgres role within a Branch."""

    create_time: Optional[Timestamp] = None

    name: Optional[str] = None
    """Output only. The full resource path of the role. Format:
    projects/{project_id}/branches/{branch_id}/roles/{role_id}"""

    parent: Optional[str] = None
    """The Branch where this Role exists. Format: projects/{project_id}/branches/{branch_id}"""

    spec: Optional[RoleRoleSpec] = None
    """The spec contains the role configuration, including identity type, authentication method, and
    role attributes."""

    status: Optional[RoleRoleStatus] = None
    """Current status of the role, including its identity type, authentication method, and role
    attributes."""

    update_time: Optional[Timestamp] = None

    def as_dict(self) -> dict:
        """Serializes the Role into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time.ToJsonString()
        if self.name is not None:
            body["name"] = self.name
        if self.parent is not None:
            body["parent"] = self.parent
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.update_time is not None:
            body["update_time"] = self.update_time.ToJsonString()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Role into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.name is not None:
            body["name"] = self.name
        if self.parent is not None:
            body["parent"] = self.parent
        if self.spec:
            body["spec"] = self.spec
        if self.status:
            body["status"] = self.status
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Role:
        """Deserializes the Role from a dictionary."""
        return cls(
            create_time=_timestamp(d, "create_time"),
            name=d.get("name", None),
            parent=d.get("parent", None),
            spec=_from_dict(d, "spec", RoleRoleSpec),
            status=_from_dict(d, "status", RoleRoleStatus),
            update_time=_timestamp(d, "update_time"),
        )


@dataclass
class RoleAttributes:
    """Attributes that can be granted to a Postgres role. We are only implementing a subset for now,
    see xref: https://www.postgresql.org/docs/16/sql-createrole.html The values follow Postgres
    keyword naming e.g. CREATEDB, BYPASSRLS, etc. which is why they don't include typical
    underscores between words."""

    bypassrls: Optional[bool] = None

    createdb: Optional[bool] = None

    createrole: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the RoleAttributes into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bypassrls is not None:
            body["bypassrls"] = self.bypassrls
        if self.createdb is not None:
            body["createdb"] = self.createdb
        if self.createrole is not None:
            body["createrole"] = self.createrole
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RoleAttributes into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bypassrls is not None:
            body["bypassrls"] = self.bypassrls
        if self.createdb is not None:
            body["createdb"] = self.createdb
        if self.createrole is not None:
            body["createrole"] = self.createrole
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RoleAttributes:
        """Deserializes the RoleAttributes from a dictionary."""
        return cls(
            bypassrls=d.get("bypassrls", None), createdb=d.get("createdb", None), createrole=d.get("createrole", None)
        )


class RoleAuthMethod(Enum):
    """How the role is authenticated when connecting to Postgres."""

    LAKEBASE_OAUTH_V1 = "LAKEBASE_OAUTH_V1"
    NO_LOGIN = "NO_LOGIN"
    PG_PASSWORD_SCRAM_SHA_256 = "PG_PASSWORD_SCRAM_SHA_256"


class RoleIdentityType(Enum):
    """The type of the Databricks managed identity that this Role represents. Leave empty if you wish
    to create a regular Postgres role not associated with a Databricks identity."""

    GROUP = "GROUP"
    SERVICE_PRINCIPAL = "SERVICE_PRINCIPAL"
    USER = "USER"


class RoleMembershipRole(Enum):
    """Roles that the DatabaseInstanceRole can be a member of."""

    DATABRICKS_SUPERUSER = "DATABRICKS_SUPERUSER"


@dataclass
class RoleOperationMetadata:
    def as_dict(self) -> dict:
        """Serializes the RoleOperationMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RoleOperationMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RoleOperationMetadata:
        """Deserializes the RoleOperationMetadata from a dictionary."""
        return cls()


@dataclass
class RoleRoleSpec:
    attributes: Optional[RoleAttributes] = None
    """The desired API-exposed Postgres role attribute to associate with the role. Optional."""

    auth_method: Optional[RoleAuthMethod] = None
    """If auth_method is left unspecified, a meaningful authentication method is derived from the
    identity_type: * For the managed identities, OAUTH is used. * For the regular postgres roles,
    authentication based on postgres passwords is used.
    
    NOTE: this is ignored for the Databricks identity type GROUP, and NO_LOGIN is implicitly assumed
    instead for the GROUP identity type."""

    identity_type: Optional[RoleIdentityType] = None
    """The type of role. When specifying a managed-identity, the chosen role_id must be a valid:
    
    * application ID for SERVICE_PRINCIPAL * user email for USER * group name for GROUP"""

    membership_roles: Optional[List[RoleMembershipRole]] = None
    """An enum value for a standard role that this role is a member of."""

    postgres_role: Optional[str] = None
    """The name of the Postgres role.
    
    This expects a valid Postgres identifier as specified in the link below.
    https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    
    Required when creating the Role.
    
    If you wish to create a Postgres Role backed by a managed Databricks identity, then
    postgres_role must be one of the following:
    
    1. user email for IdentityType.USER 2. app ID for IdentityType.SERVICE_PRINCIPAL 2. group name
    for IdentityType.GROUP"""

    def as_dict(self) -> dict:
        """Serializes the RoleRoleSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.attributes:
            body["attributes"] = self.attributes.as_dict()
        if self.auth_method is not None:
            body["auth_method"] = self.auth_method.value
        if self.identity_type is not None:
            body["identity_type"] = self.identity_type.value
        if self.membership_roles:
            body["membership_roles"] = [v.value for v in self.membership_roles]
        if self.postgres_role is not None:
            body["postgres_role"] = self.postgres_role
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RoleRoleSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.attributes:
            body["attributes"] = self.attributes
        if self.auth_method is not None:
            body["auth_method"] = self.auth_method
        if self.identity_type is not None:
            body["identity_type"] = self.identity_type
        if self.membership_roles:
            body["membership_roles"] = self.membership_roles
        if self.postgres_role is not None:
            body["postgres_role"] = self.postgres_role
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RoleRoleSpec:
        """Deserializes the RoleRoleSpec from a dictionary."""
        return cls(
            attributes=_from_dict(d, "attributes", RoleAttributes),
            auth_method=_enum(d, "auth_method", RoleAuthMethod),
            identity_type=_enum(d, "identity_type", RoleIdentityType),
            membership_roles=_repeated_enum(d, "membership_roles", RoleMembershipRole),
            postgres_role=d.get("postgres_role", None),
        )


@dataclass
class RoleRoleStatus:
    auth_method: Optional[RoleAuthMethod] = None

    identity_type: Optional[RoleIdentityType] = None
    """The type of the role."""

    membership_roles: Optional[List[RoleMembershipRole]] = None
    """An enum value for a standard role that this role is a member of."""

    postgres_role: Optional[str] = None
    """The name of the Postgres role."""

    def as_dict(self) -> dict:
        """Serializes the RoleRoleStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.auth_method is not None:
            body["auth_method"] = self.auth_method.value
        if self.identity_type is not None:
            body["identity_type"] = self.identity_type.value
        if self.membership_roles:
            body["membership_roles"] = [v.value for v in self.membership_roles]
        if self.postgres_role is not None:
            body["postgres_role"] = self.postgres_role
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RoleRoleStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.auth_method is not None:
            body["auth_method"] = self.auth_method
        if self.identity_type is not None:
            body["identity_type"] = self.identity_type
        if self.membership_roles:
            body["membership_roles"] = self.membership_roles
        if self.postgres_role is not None:
            body["postgres_role"] = self.postgres_role
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RoleRoleStatus:
        """Deserializes the RoleRoleStatus from a dictionary."""
        return cls(
            auth_method=_enum(d, "auth_method", RoleAuthMethod),
            identity_type=_enum(d, "identity_type", RoleIdentityType),
            membership_roles=_repeated_enum(d, "membership_roles", RoleMembershipRole),
            postgres_role=d.get("postgres_role", None),
        )


class PostgresAPI:
    """Use the Postgres API to create and manage Lakebase Autoscaling Postgres infrastructure, including
    projects, branches, compute endpoints, and roles.

    This API manages database infrastructure only. To query or modify data, use the Data API or direct SQL
    connections.

    **About resource IDs and names**

    Resources are identified by hierarchical resource names like
    `projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}`. The `name` field on each resource
    contains this full path and is output-only. Note that `name` refers to this resource path, not the
    user-visible `display_name`."""

    def __init__(self, api_client):
        self._api = api_client

    def create_branch(self, parent: str, branch: Branch, branch_id: str) -> CreateBranchOperation:
        """Creates a new database branch in the project.

        :param parent: str
          The Project where this Branch will be created. Format: projects/{project_id}
        :param branch: :class:`Branch`
          The Branch to create.
        :param branch_id: str
          The ID to use for the Branch. This becomes the final component of the branch's resource name. The ID
          is required and must be 1-63 characters long, start with a lowercase letter, and contain only
          lowercase letters, numbers, and hyphens. For example, `development` becomes
          `projects/my-app/branches/development`.

        :returns: :class:`Operation`
        """

        body = branch.as_dict()
        query = {}
        if branch_id is not None:
            query["branch_id"] = branch_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/postgres/{parent}/branches", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return CreateBranchOperation(self, operation)

    def create_endpoint(self, parent: str, endpoint: Endpoint, endpoint_id: str) -> CreateEndpointOperation:
        """Creates a new compute endpoint in the branch.

        :param parent: str
          The Branch where this Endpoint will be created. Format: projects/{project_id}/branches/{branch_id}
        :param endpoint: :class:`Endpoint`
          The Endpoint to create.
        :param endpoint_id: str
          The ID to use for the Endpoint. This becomes the final component of the endpoint's resource name.
          The ID is required and must be 1-63 characters long, start with a lowercase letter, and contain only
          lowercase letters, numbers, and hyphens. For example, `primary` becomes
          `projects/my-app/branches/development/endpoints/primary`.

        :returns: :class:`Operation`
        """

        body = endpoint.as_dict()
        query = {}
        if endpoint_id is not None:
            query["endpoint_id"] = endpoint_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/postgres/{parent}/endpoints", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return CreateEndpointOperation(self, operation)

    def create_project(self, project: Project, project_id: str) -> CreateProjectOperation:
        """Creates a new Lakebase Autoscaling Postgres database project, which contains branches and compute
        endpoints.

        :param project: :class:`Project`
          The Project to create.
        :param project_id: str
          The ID to use for the Project. This becomes the final component of the project's resource name. The
          ID is required and must be 1-63 characters long, start with a lowercase letter, and contain only
          lowercase letters, numbers, and hyphens. For example, `my-app` becomes `projects/my-app`.

        :returns: :class:`Operation`
        """

        body = project.as_dict()
        query = {}
        if project_id is not None:
            query["project_id"] = project_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/postgres/projects", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return CreateProjectOperation(self, operation)

    def create_role(self, parent: str, role: Role, *, role_id: Optional[str] = None) -> CreateRoleOperation:
        """Creates a new Postgres role in the branch.

        :param parent: str
          The Branch where this Role is created. Format: projects/{project_id}/branches/{branch_id}
        :param role: :class:`Role`
          The desired specification of a Role.
        :param role_id: str (optional)
          The ID to use for the Role, which will become the final component of the role's resource name. This
          ID becomes the role in Postgres.

          This value should be 4-63 characters, and valid characters are lowercase letters, numbers, and
          hyphens, as defined by RFC 1123.

          If role_id is not specified in the request, it is generated automatically.

        :returns: :class:`Operation`
        """

        body = role.as_dict()
        query = {}
        if role_id is not None:
            query["role_id"] = role_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/postgres/{parent}/roles", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return CreateRoleOperation(self, operation)

    def delete_branch(self, name: str) -> DeleteBranchOperation:
        """Deletes the specified database branch.

        :param name: str
          The full resource path of the branch to delete. Format: projects/{project_id}/branches/{branch_id}

        :returns: :class:`Operation`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.0/postgres/{name}", headers=headers)
        operation = Operation.from_dict(res)
        return DeleteBranchOperation(self, operation)

    def delete_endpoint(self, name: str) -> DeleteEndpointOperation:
        """Deletes the specified compute endpoint.

        :param name: str
          The full resource path of the endpoint to delete. Format:
          projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}

        :returns: :class:`Operation`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.0/postgres/{name}", headers=headers)
        operation = Operation.from_dict(res)
        return DeleteEndpointOperation(self, operation)

    def delete_project(self, name: str) -> DeleteProjectOperation:
        """Deletes the specified database project.

        :param name: str
          The full resource path of the project to delete. Format: projects/{project_id}

        :returns: :class:`Operation`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.0/postgres/{name}", headers=headers)
        operation = Operation.from_dict(res)
        return DeleteProjectOperation(self, operation)

    def delete_role(self, name: str, *, reassign_owned_to: Optional[str] = None) -> DeleteRoleOperation:
        """Deletes the specified Postgres role.

        :param name: str
          The full resource path of the role to delete. Format:
          projects/{project_id}/branches/{branch_id}/roles/{role_id}
        :param reassign_owned_to: str (optional)
          Reassign objects. If this is set, all objects owned by the role are reassigned to the role specified
          in this parameter.

          NOTE: setting this requires spinning up a compute to succeed, since it involves running SQL queries.

          TODO: #LKB-7187 implement reassign_owned_to on LBM side. This might end-up being a synchronous query
          when this parameter is used.

        :returns: :class:`Operation`
        """

        query = {}
        if reassign_owned_to is not None:
            query["reassign_owned_to"] = reassign_owned_to
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", f"/api/2.0/postgres/{name}", query=query, headers=headers)
        operation = Operation.from_dict(res)
        return DeleteRoleOperation(self, operation)

    def generate_database_credential(
        self, endpoint: str, *, claims: Optional[List[RequestedClaims]] = None
    ) -> DatabaseCredential:
        """Generate OAuth credentials for a Postgres database.

        :param endpoint: str
          This field is not yet supported. The endpoint for which this credential will be generated. Format:
          projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}
        :param claims: List[:class:`RequestedClaims`] (optional)
          The returned token will be scoped to UC tables with the specified permissions.

        :returns: :class:`DatabaseCredential`
        """

        body = {}
        if claims is not None:
            body["claims"] = [v.as_dict() for v in claims]
        if endpoint is not None:
            body["endpoint"] = endpoint
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/postgres/credentials", body=body, headers=headers)
        return DatabaseCredential.from_dict(res)

    def get_branch(self, name: str) -> Branch:
        """Retrieves information about the specified database branch.

        :param name: str
          The full resource path of the branch to retrieve. Format: projects/{project_id}/branches/{branch_id}

        :returns: :class:`Branch`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/postgres/{name}", headers=headers)
        return Branch.from_dict(res)

    def get_endpoint(self, name: str) -> Endpoint:
        """Retrieves information about the specified compute endpoint, including its connection details and
        operational state.

        :param name: str
          The full resource path of the endpoint to retrieve. Format:
          projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}

        :returns: :class:`Endpoint`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/postgres/{name}", headers=headers)
        return Endpoint.from_dict(res)

    def get_operation(self, name: str) -> Operation:
        """Retrieves the status of a long-running operation.

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

        res = self._api.do("GET", f"/api/2.0/postgres/{name}", headers=headers)
        return Operation.from_dict(res)

    def get_project(self, name: str) -> Project:
        """Retrieves information about the specified database project.

        :param name: str
          The full resource path of the project to retrieve. Format: projects/{project_id}

        :returns: :class:`Project`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/postgres/{name}", headers=headers)
        return Project.from_dict(res)

    def get_role(self, name: str) -> Role:
        """Retrieves information about the specified Postgres role, including its authentication method and
        permissions.

        :param name: str
          The full resource path of the role to retrieve. Format:
          projects/{project_id}/branches/{branch_id}/roles/{role_id}

        :returns: :class:`Role`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/postgres/{name}", headers=headers)
        return Role.from_dict(res)

    def list_branches(
        self, parent: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[Branch]:
        """Returns a paginated list of database branches in the project.

        :param parent: str
          The Project that owns this collection of branches. Format: projects/{project_id}
        :param page_size: int (optional)
          Upper bound for items returned. Cannot be negative.
        :param page_token: str (optional)
          Page token from a previous response. If not provided, returns the first page.

        :returns: Iterator over :class:`Branch`
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
            json = self._api.do("GET", f"/api/2.0/postgres/{parent}/branches", query=query, headers=headers)
            if "branches" in json:
                for v in json["branches"]:
                    yield Branch.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_endpoints(
        self, parent: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[Endpoint]:
        """Returns a paginated list of compute endpoints in the branch.

        :param parent: str
          The Branch that owns this collection of endpoints. Format:
          projects/{project_id}/branches/{branch_id}
        :param page_size: int (optional)
          Upper bound for items returned. Cannot be negative.
        :param page_token: str (optional)
          Page token from a previous response. If not provided, returns the first page.

        :returns: Iterator over :class:`Endpoint`
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
            json = self._api.do("GET", f"/api/2.0/postgres/{parent}/endpoints", query=query, headers=headers)
            if "endpoints" in json:
                for v in json["endpoints"]:
                    yield Endpoint.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_projects(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[Project]:
        """Returns a paginated list of database projects in the workspace that the user has permission to access.

        :param page_size: int (optional)
          Upper bound for items returned. Cannot be negative. The maximum value is 100.
        :param page_token: str (optional)
          Page token from a previous response. If not provided, returns the first page.

        :returns: Iterator over :class:`Project`
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
            json = self._api.do("GET", "/api/2.0/postgres/projects", query=query, headers=headers)
            if "projects" in json:
                for v in json["projects"]:
                    yield Project.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_roles(
        self, parent: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[Role]:
        """Returns a paginated list of Postgres roles in the branch.

        :param parent: str
          The Branch that owns this collection of roles. Format: projects/{project_id}/branches/{branch_id}
        :param page_size: int (optional)
          Upper bound for items returned. Cannot be negative.
        :param page_token: str (optional)
          Page token from a previous response. If not provided, returns the first page.

        :returns: Iterator over :class:`Role`
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
            json = self._api.do("GET", f"/api/2.0/postgres/{parent}/roles", query=query, headers=headers)
            if "roles" in json:
                for v in json["roles"]:
                    yield Role.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_branch(self, name: str, branch: Branch, update_mask: FieldMask) -> UpdateBranchOperation:
        """Updates the specified database branch. You can set this branch as the project's default branch, or
        protect/unprotect it.

        :param name: str
          Output only. The full resource path of the branch. Format:
          projects/{project_id}/branches/{branch_id}
        :param branch: :class:`Branch`
          The Branch to update.

          The branch's `name` field is used to identify the branch to update. Format:
          projects/{project_id}/branches/{branch_id}
        :param update_mask: FieldMask
          The list of fields to update. If unspecified, all fields will be updated when possible.

        :returns: :class:`Operation`
        """

        body = branch.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.0/postgres/{name}", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return UpdateBranchOperation(self, operation)

    def update_endpoint(self, name: str, endpoint: Endpoint, update_mask: FieldMask) -> UpdateEndpointOperation:
        """Updates the specified compute endpoint. You can update autoscaling limits, suspend timeout, or
        enable/disable the compute endpoint.

        :param name: str
          Output only. The full resource path of the endpoint. Format:
          projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}
        :param endpoint: :class:`Endpoint`
          The Endpoint to update.

          The endpoint's `name` field is used to identify the endpoint to update. Format:
          projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}
        :param update_mask: FieldMask
          The list of fields to update. If unspecified, all fields will be updated when possible.

        :returns: :class:`Operation`
        """

        body = endpoint.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.0/postgres/{name}", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return UpdateEndpointOperation(self, operation)

    def update_project(self, name: str, project: Project, update_mask: FieldMask) -> UpdateProjectOperation:
        """Updates the specified database project.

        :param name: str
          Output only. The full resource path of the project. Format: projects/{project_id}
        :param project: :class:`Project`
          The Project to update.

          The project's `name` field is used to identify the project to update. Format: projects/{project_id}
        :param update_mask: FieldMask
          The list of fields to update. If unspecified, all fields will be updated when possible.

        :returns: :class:`Operation`
        """

        body = project.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.0/postgres/{name}", query=query, body=body, headers=headers)
        operation = Operation.from_dict(res)
        return UpdateProjectOperation(self, operation)


class CreateBranchOperation:
    """Long-running operation for create_branch"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Branch:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Branch`
        """

        def poll_operation():
            operation = self._impl.get_operation(name=self._operation.name)

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

            branch = Branch.from_dict(operation.response)

            return branch, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> BranchOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`BranchOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return BranchOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class CreateEndpointOperation:
    """Long-running operation for create_endpoint"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Endpoint:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Endpoint`
        """

        def poll_operation():
            operation = self._impl.get_operation(name=self._operation.name)

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

            endpoint = Endpoint.from_dict(operation.response)

            return endpoint, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> EndpointOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`EndpointOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return EndpointOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class CreateProjectOperation:
    """Long-running operation for create_project"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Project:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Project`
        """

        def poll_operation():
            operation = self._impl.get_operation(name=self._operation.name)

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

            project = Project.from_dict(operation.response)

            return project, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> ProjectOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`ProjectOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return ProjectOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class CreateRoleOperation:
    """Long-running operation for create_role"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Role:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Role`
        """

        def poll_operation():
            operation = self._impl.get_operation(name=self._operation.name)

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

            role = Role.from_dict(operation.response)

            return role, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> RoleOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`RoleOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return RoleOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class DeleteBranchOperation:
    """Long-running operation for delete_branch"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
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
            operation = self._impl.get_operation(name=self._operation.name)

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

    def metadata(self) -> BranchOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`BranchOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return BranchOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class DeleteEndpointOperation:
    """Long-running operation for delete_endpoint"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
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
            operation = self._impl.get_operation(name=self._operation.name)

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

    def metadata(self) -> EndpointOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`EndpointOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return EndpointOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class DeleteProjectOperation:
    """Long-running operation for delete_project"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
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
            operation = self._impl.get_operation(name=self._operation.name)

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

    def metadata(self) -> ProjectOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`ProjectOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return ProjectOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class DeleteRoleOperation:
    """Long-running operation for delete_role"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
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
            operation = self._impl.get_operation(name=self._operation.name)

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

    def metadata(self) -> RoleOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`RoleOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return RoleOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class UpdateBranchOperation:
    """Long-running operation for update_branch"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Branch:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Branch`
        """

        def poll_operation():
            operation = self._impl.get_operation(name=self._operation.name)

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

            branch = Branch.from_dict(operation.response)

            return branch, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> BranchOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`BranchOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return BranchOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class UpdateEndpointOperation:
    """Long-running operation for update_endpoint"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Endpoint:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Endpoint`
        """

        def poll_operation():
            operation = self._impl.get_operation(name=self._operation.name)

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

            endpoint = Endpoint.from_dict(operation.response)

            return endpoint, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> EndpointOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`EndpointOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return EndpointOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done


class UpdateProjectOperation:
    """Long-running operation for update_project"""

    def __init__(self, impl: PostgresAPI, operation: Operation):
        self._impl = impl
        self._operation = operation

    def wait(self, opts: Optional[lro.LroOptions] = None) -> Project:
        """Wait blocks until the long-running operation is completed. If no timeout is
        specified, this will poll indefinitely. If a timeout is provided and the operation
        didn't finish within the timeout, this function will raise an error of type
        TimeoutError, otherwise returns successful response and any errors encountered.

        :param opts: :class:`LroOptions`
          Timeout options (default: polls indefinitely)

        :returns: :class:`Project`
        """

        def poll_operation():
            operation = self._impl.get_operation(name=self._operation.name)

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

            project = Project.from_dict(operation.response)

            return project, None

        return poll(poll_operation, timeout=opts.timeout if opts is not None else None)

    def name(self) -> str:
        """Name returns the name of the long-running operation. The name is assigned
        by the server and is unique within the service from which the operation is created.

        :returns: str
        """
        return self._operation.name

    def metadata(self) -> ProjectOperationMetadata:
        """Metadata returns metadata associated with the long-running operation.
        If the metadata is not available, the returned metadata is None.

        :returns: :class:`ProjectOperationMetadata` or None
        """
        if self._operation.metadata is None:
            return None

        return ProjectOperationMetadata.from_dict(self._operation.metadata)

    def done(self) -> bool:
        """Done reports whether the long-running operation has completed.

        :returns: bool
        """
        # Refresh the operation state first
        operation = self._impl.get_operation(name=self._operation.name)

        # Update local operation state
        self._operation = operation

        return operation.done
