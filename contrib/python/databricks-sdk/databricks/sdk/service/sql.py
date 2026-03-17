# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict, _repeated_enum)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AccessControl:
    group_name: Optional[str] = None

    permission_level: Optional[PermissionLevel] = None
    """* `CAN_VIEW`: Can view the query * `CAN_RUN`: Can run the query * `CAN_EDIT`: Can edit the query
    * `CAN_MANAGE`: Can manage the query"""

    user_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the AccessControl into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccessControl into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccessControl:
        """Deserializes the AccessControl from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", PermissionLevel),
            user_name=d.get("user_name", None),
        )


class Aggregation(Enum):

    AVG = "AVG"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    MAX = "MAX"
    MEDIAN = "MEDIAN"
    MIN = "MIN"
    STDDEV = "STDDEV"
    SUM = "SUM"


@dataclass
class Alert:
    condition: Optional[AlertCondition] = None
    """Trigger conditions of the alert."""

    create_time: Optional[str] = None
    """The timestamp indicating when the alert was created."""

    custom_body: Optional[str] = None
    """Custom body of alert notification, if it exists. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    custom_subject: Optional[str] = None
    """Custom subject of alert notification, if it exists. This can include email subject entries and
    Slack notification headers, for example. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    display_name: Optional[str] = None
    """The display name of the alert."""

    id: Optional[str] = None
    """UUID identifying the alert."""

    lifecycle_state: Optional[LifecycleState] = None
    """The workspace state of the alert. Used for tracking trashed status."""

    notify_on_ok: Optional[bool] = None
    """Whether to notify alert subscribers when alert returns back to normal."""

    owner_user_name: Optional[str] = None
    """The owner's username. This field is set to "Unavailable" if the user has been deleted."""

    parent_path: Optional[str] = None
    """The workspace path of the folder containing the alert."""

    query_id: Optional[str] = None
    """UUID of the query attached to the alert."""

    seconds_to_retrigger: Optional[int] = None
    """Number of seconds an alert must wait after being triggered to rearm itself. After rearming, it
    can be triggered again. If 0 or not specified, the alert will not be triggered again."""

    state: Optional[AlertState] = None
    """Current state of the alert's trigger status. This field is set to UNKNOWN if the alert has not
    yet been evaluated or ran into an error during the last evaluation."""

    trigger_time: Optional[str] = None
    """Timestamp when the alert was last triggered, if the alert has been triggered before."""

    update_time: Optional[str] = None
    """The timestamp indicating when the alert was updated."""

    def as_dict(self) -> dict:
        """Serializes the Alert into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.condition:
            body["condition"] = self.condition.as_dict()
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state.value
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        if self.state is not None:
            body["state"] = self.state.value
        if self.trigger_time is not None:
            body["trigger_time"] = self.trigger_time
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Alert into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.condition:
            body["condition"] = self.condition
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        if self.state is not None:
            body["state"] = self.state
        if self.trigger_time is not None:
            body["trigger_time"] = self.trigger_time
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Alert:
        """Deserializes the Alert from a dictionary."""
        return cls(
            condition=_from_dict(d, "condition", AlertCondition),
            create_time=d.get("create_time", None),
            custom_body=d.get("custom_body", None),
            custom_subject=d.get("custom_subject", None),
            display_name=d.get("display_name", None),
            id=d.get("id", None),
            lifecycle_state=_enum(d, "lifecycle_state", LifecycleState),
            notify_on_ok=d.get("notify_on_ok", None),
            owner_user_name=d.get("owner_user_name", None),
            parent_path=d.get("parent_path", None),
            query_id=d.get("query_id", None),
            seconds_to_retrigger=d.get("seconds_to_retrigger", None),
            state=_enum(d, "state", AlertState),
            trigger_time=d.get("trigger_time", None),
            update_time=d.get("update_time", None),
        )


@dataclass
class AlertCondition:
    empty_result_state: Optional[AlertState] = None
    """Alert state if result is empty."""

    op: Optional[AlertOperator] = None
    """Operator used for comparison in alert evaluation."""

    operand: Optional[AlertConditionOperand] = None
    """Name of the column from the query result to use for comparison in alert evaluation."""

    threshold: Optional[AlertConditionThreshold] = None
    """Threshold value used for comparison in alert evaluation."""

    def as_dict(self) -> dict:
        """Serializes the AlertCondition into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.empty_result_state is not None:
            body["empty_result_state"] = self.empty_result_state.value
        if self.op is not None:
            body["op"] = self.op.value
        if self.operand:
            body["operand"] = self.operand.as_dict()
        if self.threshold:
            body["threshold"] = self.threshold.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertCondition into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.empty_result_state is not None:
            body["empty_result_state"] = self.empty_result_state
        if self.op is not None:
            body["op"] = self.op
        if self.operand:
            body["operand"] = self.operand
        if self.threshold:
            body["threshold"] = self.threshold
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertCondition:
        """Deserializes the AlertCondition from a dictionary."""
        return cls(
            empty_result_state=_enum(d, "empty_result_state", AlertState),
            op=_enum(d, "op", AlertOperator),
            operand=_from_dict(d, "operand", AlertConditionOperand),
            threshold=_from_dict(d, "threshold", AlertConditionThreshold),
        )


@dataclass
class AlertConditionOperand:
    column: Optional[AlertOperandColumn] = None

    def as_dict(self) -> dict:
        """Serializes the AlertConditionOperand into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.column:
            body["column"] = self.column.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertConditionOperand into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.column:
            body["column"] = self.column
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertConditionOperand:
        """Deserializes the AlertConditionOperand from a dictionary."""
        return cls(column=_from_dict(d, "column", AlertOperandColumn))


@dataclass
class AlertConditionThreshold:
    value: Optional[AlertOperandValue] = None

    def as_dict(self) -> dict:
        """Serializes the AlertConditionThreshold into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.value:
            body["value"] = self.value.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertConditionThreshold into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.value:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertConditionThreshold:
        """Deserializes the AlertConditionThreshold from a dictionary."""
        return cls(value=_from_dict(d, "value", AlertOperandValue))


class AlertEvaluationState(Enum):
    """UNSPECIFIED - default unspecify value for proto enum, do not use it in the code UNKNOWN - alert
    not yet evaluated TRIGGERED - alert is triggered OK - alert is not triggered ERROR - alert
    evaluation failed"""

    ERROR = "ERROR"
    OK = "OK"
    TRIGGERED = "TRIGGERED"
    UNKNOWN = "UNKNOWN"


class AlertLifecycleState(Enum):

    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


@dataclass
class AlertOperandColumn:
    name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the AlertOperandColumn into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertOperandColumn into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertOperandColumn:
        """Deserializes the AlertOperandColumn from a dictionary."""
        return cls(name=d.get("name", None))


@dataclass
class AlertOperandValue:
    bool_value: Optional[bool] = None

    double_value: Optional[float] = None

    string_value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the AlertOperandValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bool_value is not None:
            body["bool_value"] = self.bool_value
        if self.double_value is not None:
            body["double_value"] = self.double_value
        if self.string_value is not None:
            body["string_value"] = self.string_value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertOperandValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bool_value is not None:
            body["bool_value"] = self.bool_value
        if self.double_value is not None:
            body["double_value"] = self.double_value
        if self.string_value is not None:
            body["string_value"] = self.string_value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertOperandValue:
        """Deserializes the AlertOperandValue from a dictionary."""
        return cls(
            bool_value=d.get("bool_value", None),
            double_value=d.get("double_value", None),
            string_value=d.get("string_value", None),
        )


class AlertOperator(Enum):

    EQUAL = "EQUAL"
    GREATER_THAN = "GREATER_THAN"
    GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL"
    IS_NULL = "IS_NULL"
    LESS_THAN = "LESS_THAN"
    LESS_THAN_OR_EQUAL = "LESS_THAN_OR_EQUAL"
    NOT_EQUAL = "NOT_EQUAL"


@dataclass
class AlertOptions:
    """Alert configuration options."""

    column: str
    """Name of column in the query result to compare in alert evaluation."""

    op: str
    """Operator used to compare in alert evaluation: `>`, `>=`, `<`, `<=`, `==`, `!=`"""

    value: Any
    """Value used to compare in alert evaluation. Supported types include strings (eg. 'foobar'),
    floats (eg. 123.4), and booleans (true)."""

    custom_body: Optional[str] = None
    """Custom body of alert notification, if it exists. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    custom_subject: Optional[str] = None
    """Custom subject of alert notification, if it exists. This includes email subject, Slack
    notification header, etc. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    empty_result_state: Optional[AlertOptionsEmptyResultState] = None
    """State that alert evaluates to when query result is empty."""

    muted: Optional[bool] = None
    """Whether or not the alert is muted. If an alert is muted, it will not notify users and
    notification destinations when triggered."""

    def as_dict(self) -> dict:
        """Serializes the AlertOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.column is not None:
            body["column"] = self.column
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.empty_result_state is not None:
            body["empty_result_state"] = self.empty_result_state.value
        if self.muted is not None:
            body["muted"] = self.muted
        if self.op is not None:
            body["op"] = self.op
        if self.value:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.column is not None:
            body["column"] = self.column
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.empty_result_state is not None:
            body["empty_result_state"] = self.empty_result_state
        if self.muted is not None:
            body["muted"] = self.muted
        if self.op is not None:
            body["op"] = self.op
        if self.value:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertOptions:
        """Deserializes the AlertOptions from a dictionary."""
        return cls(
            column=d.get("column", None),
            custom_body=d.get("custom_body", None),
            custom_subject=d.get("custom_subject", None),
            empty_result_state=_enum(d, "empty_result_state", AlertOptionsEmptyResultState),
            muted=d.get("muted", None),
            op=d.get("op", None),
            value=d.get("value", None),
        )


class AlertOptionsEmptyResultState(Enum):
    """State that alert evaluates to when query result is empty."""

    OK = "ok"
    TRIGGERED = "triggered"
    UNKNOWN = "unknown"


@dataclass
class AlertQuery:
    created_at: Optional[str] = None
    """The timestamp when this query was created."""

    data_source_id: Optional[str] = None
    """Data source ID maps to the ID of the data source used by the resource and is distinct from the
    warehouse ID. [Learn more]
    
    [Learn more]: https://docs.databricks.com/api/workspace/datasources/list"""

    description: Optional[str] = None
    """General description that conveys additional information about this query such as usage notes."""

    id: Optional[str] = None
    """Query ID."""

    is_archived: Optional[bool] = None
    """Indicates whether the query is trashed. Trashed queries can't be used in dashboards, or appear
    in search results. If this boolean is `true`, the `options` property for this query includes a
    `moved_to_trash_at` timestamp. Trashed queries are permanently deleted after 30 days."""

    is_draft: Optional[bool] = None
    """Whether the query is a draft. Draft queries only appear in list views for their owners.
    Visualizations from draft queries cannot appear on dashboards."""

    is_safe: Optional[bool] = None
    """Text parameter types are not safe from SQL injection for all types of data source. Set this
    Boolean parameter to `true` if a query either does not use any text type parameters or uses a
    data source type where text type parameters are handled safely."""

    name: Optional[str] = None
    """The title of this query that appears in list views, widget headings, and on the query page."""

    options: Optional[QueryOptions] = None

    query: Optional[str] = None
    """The text of the query to be run."""

    tags: Optional[List[str]] = None

    updated_at: Optional[str] = None
    """The timestamp at which this query was last updated."""

    user_id: Optional[int] = None
    """The ID of the user who owns the query."""

    def as_dict(self) -> dict:
        """Serializes the AlertQuery into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.data_source_id is not None:
            body["data_source_id"] = self.data_source_id
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.is_archived is not None:
            body["is_archived"] = self.is_archived
        if self.is_draft is not None:
            body["is_draft"] = self.is_draft
        if self.is_safe is not None:
            body["is_safe"] = self.is_safe
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options.as_dict()
        if self.query is not None:
            body["query"] = self.query
        if self.tags:
            body["tags"] = [v for v in self.tags]
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertQuery into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.data_source_id is not None:
            body["data_source_id"] = self.data_source_id
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.is_archived is not None:
            body["is_archived"] = self.is_archived
        if self.is_draft is not None:
            body["is_draft"] = self.is_draft
        if self.is_safe is not None:
            body["is_safe"] = self.is_safe
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.query is not None:
            body["query"] = self.query
        if self.tags:
            body["tags"] = self.tags
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertQuery:
        """Deserializes the AlertQuery from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            data_source_id=d.get("data_source_id", None),
            description=d.get("description", None),
            id=d.get("id", None),
            is_archived=d.get("is_archived", None),
            is_draft=d.get("is_draft", None),
            is_safe=d.get("is_safe", None),
            name=d.get("name", None),
            options=_from_dict(d, "options", QueryOptions),
            query=d.get("query", None),
            tags=d.get("tags", None),
            updated_at=d.get("updated_at", None),
            user_id=d.get("user_id", None),
        )


class AlertState(Enum):

    OK = "OK"
    TRIGGERED = "TRIGGERED"
    UNKNOWN = "UNKNOWN"


@dataclass
class AlertV2:
    display_name: str
    """The display name of the alert."""

    query_text: str
    """Text of the query to be run."""

    warehouse_id: str
    """ID of the SQL warehouse attached to the alert."""

    evaluation: AlertV2Evaluation

    schedule: CronSchedule

    create_time: Optional[str] = None
    """The timestamp indicating when the alert was created."""

    custom_description: Optional[str] = None
    """Custom description for the alert. support mustache template."""

    custom_summary: Optional[str] = None
    """Custom summary for the alert. support mustache template."""

    effective_run_as: Optional[AlertV2RunAs] = None
    """The actual identity that will be used to execute the alert. This is an output-only field that
    shows the resolved run-as identity after applying permissions and defaults."""

    id: Optional[str] = None
    """UUID identifying the alert."""

    lifecycle_state: Optional[AlertLifecycleState] = None
    """Indicates whether the query is trashed."""

    owner_user_name: Optional[str] = None
    """The owner's username. This field is set to "Unavailable" if the user has been deleted."""

    parent_path: Optional[str] = None
    """The workspace path of the folder containing the alert. Can only be set on create, and cannot be
    updated."""

    run_as: Optional[AlertV2RunAs] = None
    """Specifies the identity that will be used to run the alert. This field allows you to configure
    alerts to run as a specific user or service principal. - For user identity: Set `user_name` to
    the email of an active workspace user. Users can only set this to their own email. - For service
    principal: Set `service_principal_name` to the application ID. Requires the
    `servicePrincipal/user` role. If not specified, the alert will run as the request user."""

    run_as_user_name: Optional[str] = None
    """The run as username or application ID of service principal. On Create and Update, this field can
    be set to application ID of an active service principal. Setting this field requires the
    servicePrincipal/user role. Deprecated: Use `run_as` field instead. This field will be removed
    in a future release."""

    update_time: Optional[str] = None
    """The timestamp indicating when the alert was updated."""

    def as_dict(self) -> dict:
        """Serializes the AlertV2 into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.custom_description is not None:
            body["custom_description"] = self.custom_description
        if self.custom_summary is not None:
            body["custom_summary"] = self.custom_summary
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.effective_run_as:
            body["effective_run_as"] = self.effective_run_as.as_dict()
        if self.evaluation:
            body["evaluation"] = self.evaluation.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state.value
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as:
            body["run_as"] = self.run_as.as_dict()
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.schedule:
            body["schedule"] = self.schedule.as_dict()
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2 into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.custom_description is not None:
            body["custom_description"] = self.custom_description
        if self.custom_summary is not None:
            body["custom_summary"] = self.custom_summary
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.effective_run_as:
            body["effective_run_as"] = self.effective_run_as
        if self.evaluation:
            body["evaluation"] = self.evaluation
        if self.id is not None:
            body["id"] = self.id
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as:
            body["run_as"] = self.run_as
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.schedule:
            body["schedule"] = self.schedule
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2:
        """Deserializes the AlertV2 from a dictionary."""
        return cls(
            create_time=d.get("create_time", None),
            custom_description=d.get("custom_description", None),
            custom_summary=d.get("custom_summary", None),
            display_name=d.get("display_name", None),
            effective_run_as=_from_dict(d, "effective_run_as", AlertV2RunAs),
            evaluation=_from_dict(d, "evaluation", AlertV2Evaluation),
            id=d.get("id", None),
            lifecycle_state=_enum(d, "lifecycle_state", AlertLifecycleState),
            owner_user_name=d.get("owner_user_name", None),
            parent_path=d.get("parent_path", None),
            query_text=d.get("query_text", None),
            run_as=_from_dict(d, "run_as", AlertV2RunAs),
            run_as_user_name=d.get("run_as_user_name", None),
            schedule=_from_dict(d, "schedule", CronSchedule),
            update_time=d.get("update_time", None),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class AlertV2Evaluation:
    source: AlertV2OperandColumn
    """Source column from result to use to evaluate alert"""

    comparison_operator: ComparisonOperator
    """Operator used for comparison in alert evaluation."""

    empty_result_state: Optional[AlertEvaluationState] = None
    """Alert state if result is empty. Please avoid setting this field to be `UNKNOWN` because
    `UNKNOWN` state is planned to be deprecated."""

    last_evaluated_at: Optional[str] = None
    """Timestamp of the last evaluation."""

    notification: Optional[AlertV2Notification] = None
    """User or Notification Destination to notify when alert is triggered."""

    state: Optional[AlertEvaluationState] = None
    """Latest state of alert evaluation."""

    threshold: Optional[AlertV2Operand] = None
    """Threshold to user for alert evaluation, can be a column or a value."""

    def as_dict(self) -> dict:
        """Serializes the AlertV2Evaluation into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comparison_operator is not None:
            body["comparison_operator"] = self.comparison_operator.value
        if self.empty_result_state is not None:
            body["empty_result_state"] = self.empty_result_state.value
        if self.last_evaluated_at is not None:
            body["last_evaluated_at"] = self.last_evaluated_at
        if self.notification:
            body["notification"] = self.notification.as_dict()
        if self.source:
            body["source"] = self.source.as_dict()
        if self.state is not None:
            body["state"] = self.state.value
        if self.threshold:
            body["threshold"] = self.threshold.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2Evaluation into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comparison_operator is not None:
            body["comparison_operator"] = self.comparison_operator
        if self.empty_result_state is not None:
            body["empty_result_state"] = self.empty_result_state
        if self.last_evaluated_at is not None:
            body["last_evaluated_at"] = self.last_evaluated_at
        if self.notification:
            body["notification"] = self.notification
        if self.source:
            body["source"] = self.source
        if self.state is not None:
            body["state"] = self.state
        if self.threshold:
            body["threshold"] = self.threshold
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2Evaluation:
        """Deserializes the AlertV2Evaluation from a dictionary."""
        return cls(
            comparison_operator=_enum(d, "comparison_operator", ComparisonOperator),
            empty_result_state=_enum(d, "empty_result_state", AlertEvaluationState),
            last_evaluated_at=d.get("last_evaluated_at", None),
            notification=_from_dict(d, "notification", AlertV2Notification),
            source=_from_dict(d, "source", AlertV2OperandColumn),
            state=_enum(d, "state", AlertEvaluationState),
            threshold=_from_dict(d, "threshold", AlertV2Operand),
        )


@dataclass
class AlertV2Notification:
    notify_on_ok: Optional[bool] = None
    """Whether to notify alert subscribers when alert returns back to normal."""

    retrigger_seconds: Optional[int] = None
    """Number of seconds an alert waits after being triggered before it is allowed to send another
    notification. If set to 0 or omitted, the alert will not send any further notifications after
    the first trigger Setting this value to 1 allows the alert to send a notification on every
    evaluation where the condition is met, effectively making it always retrigger for notification
    purposes."""

    subscriptions: Optional[List[AlertV2Subscription]] = None

    def as_dict(self) -> dict:
        """Serializes the AlertV2Notification into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.retrigger_seconds is not None:
            body["retrigger_seconds"] = self.retrigger_seconds
        if self.subscriptions:
            body["subscriptions"] = [v.as_dict() for v in self.subscriptions]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2Notification into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.retrigger_seconds is not None:
            body["retrigger_seconds"] = self.retrigger_seconds
        if self.subscriptions:
            body["subscriptions"] = self.subscriptions
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2Notification:
        """Deserializes the AlertV2Notification from a dictionary."""
        return cls(
            notify_on_ok=d.get("notify_on_ok", None),
            retrigger_seconds=d.get("retrigger_seconds", None),
            subscriptions=_repeated_dict(d, "subscriptions", AlertV2Subscription),
        )


@dataclass
class AlertV2Operand:
    column: Optional[AlertV2OperandColumn] = None

    value: Optional[AlertV2OperandValue] = None

    def as_dict(self) -> dict:
        """Serializes the AlertV2Operand into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.column:
            body["column"] = self.column.as_dict()
        if self.value:
            body["value"] = self.value.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2Operand into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.column:
            body["column"] = self.column
        if self.value:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2Operand:
        """Deserializes the AlertV2Operand from a dictionary."""
        return cls(
            column=_from_dict(d, "column", AlertV2OperandColumn), value=_from_dict(d, "value", AlertV2OperandValue)
        )


@dataclass
class AlertV2OperandColumn:
    name: str

    aggregation: Optional[Aggregation] = None
    """If not set, the behavior is equivalent to using `First row` in the UI."""

    display: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the AlertV2OperandColumn into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aggregation is not None:
            body["aggregation"] = self.aggregation.value
        if self.display is not None:
            body["display"] = self.display
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2OperandColumn into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aggregation is not None:
            body["aggregation"] = self.aggregation
        if self.display is not None:
            body["display"] = self.display
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2OperandColumn:
        """Deserializes the AlertV2OperandColumn from a dictionary."""
        return cls(
            aggregation=_enum(d, "aggregation", Aggregation), display=d.get("display", None), name=d.get("name", None)
        )


@dataclass
class AlertV2OperandValue:
    bool_value: Optional[bool] = None

    double_value: Optional[float] = None

    string_value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the AlertV2OperandValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bool_value is not None:
            body["bool_value"] = self.bool_value
        if self.double_value is not None:
            body["double_value"] = self.double_value
        if self.string_value is not None:
            body["string_value"] = self.string_value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2OperandValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bool_value is not None:
            body["bool_value"] = self.bool_value
        if self.double_value is not None:
            body["double_value"] = self.double_value
        if self.string_value is not None:
            body["string_value"] = self.string_value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2OperandValue:
        """Deserializes the AlertV2OperandValue from a dictionary."""
        return cls(
            bool_value=d.get("bool_value", None),
            double_value=d.get("double_value", None),
            string_value=d.get("string_value", None),
        )


@dataclass
class AlertV2RunAs:
    service_principal_name: Optional[str] = None
    """Application ID of an active service principal. Setting this field requires the
    `servicePrincipal/user` role."""

    user_name: Optional[str] = None
    """The email of an active workspace user. Can only set this field to their own email."""

    def as_dict(self) -> dict:
        """Serializes the AlertV2RunAs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2RunAs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2RunAs:
        """Deserializes the AlertV2RunAs from a dictionary."""
        return cls(service_principal_name=d.get("service_principal_name", None), user_name=d.get("user_name", None))


@dataclass
class AlertV2Subscription:
    destination_id: Optional[str] = None

    user_email: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the AlertV2Subscription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.user_email is not None:
            body["user_email"] = self.user_email
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertV2Subscription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.user_email is not None:
            body["user_email"] = self.user_email
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertV2Subscription:
        """Deserializes the AlertV2Subscription from a dictionary."""
        return cls(destination_id=d.get("destination_id", None), user_email=d.get("user_email", None))


@dataclass
class BaseChunkInfo:
    byte_count: Optional[int] = None
    """The number of bytes in the result chunk. This field is not available when using `INLINE`
    disposition."""

    chunk_index: Optional[int] = None
    """The position within the sequence of result set chunks."""

    row_count: Optional[int] = None
    """The number of rows within the result chunk."""

    row_offset: Optional[int] = None
    """The starting row offset within the result set."""

    def as_dict(self) -> dict:
        """Serializes the BaseChunkInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.byte_count is not None:
            body["byte_count"] = self.byte_count
        if self.chunk_index is not None:
            body["chunk_index"] = self.chunk_index
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.row_offset is not None:
            body["row_offset"] = self.row_offset
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BaseChunkInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.byte_count is not None:
            body["byte_count"] = self.byte_count
        if self.chunk_index is not None:
            body["chunk_index"] = self.chunk_index
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.row_offset is not None:
            body["row_offset"] = self.row_offset
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BaseChunkInfo:
        """Deserializes the BaseChunkInfo from a dictionary."""
        return cls(
            byte_count=d.get("byte_count", None),
            chunk_index=d.get("chunk_index", None),
            row_count=d.get("row_count", None),
            row_offset=d.get("row_offset", None),
        )


@dataclass
class Channel:
    """Configures the channel name and DBSQL version of the warehouse. CHANNEL_NAME_CUSTOM should be
    chosen only when `dbsql_version` is specified."""

    dbsql_version: Optional[str] = None

    name: Optional[ChannelName] = None

    def as_dict(self) -> dict:
        """Serializes the Channel into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dbsql_version is not None:
            body["dbsql_version"] = self.dbsql_version
        if self.name is not None:
            body["name"] = self.name.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Channel into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dbsql_version is not None:
            body["dbsql_version"] = self.dbsql_version
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Channel:
        """Deserializes the Channel from a dictionary."""
        return cls(dbsql_version=d.get("dbsql_version", None), name=_enum(d, "name", ChannelName))


@dataclass
class ChannelInfo:
    """Details about a Channel."""

    dbsql_version: Optional[str] = None
    """DB SQL Version the Channel is mapped to."""

    name: Optional[ChannelName] = None
    """Name of the channel"""

    def as_dict(self) -> dict:
        """Serializes the ChannelInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dbsql_version is not None:
            body["dbsql_version"] = self.dbsql_version
        if self.name is not None:
            body["name"] = self.name.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ChannelInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dbsql_version is not None:
            body["dbsql_version"] = self.dbsql_version
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ChannelInfo:
        """Deserializes the ChannelInfo from a dictionary."""
        return cls(dbsql_version=d.get("dbsql_version", None), name=_enum(d, "name", ChannelName))


class ChannelName(Enum):

    CHANNEL_NAME_CURRENT = "CHANNEL_NAME_CURRENT"
    CHANNEL_NAME_CUSTOM = "CHANNEL_NAME_CUSTOM"
    CHANNEL_NAME_PREVIEW = "CHANNEL_NAME_PREVIEW"
    CHANNEL_NAME_PREVIOUS = "CHANNEL_NAME_PREVIOUS"


@dataclass
class ClientConfig:
    allow_custom_js_visualizations: Optional[bool] = None
    """allow_custom_js_visualizations is not supported/implemneted."""

    allow_downloads: Optional[bool] = None

    allow_external_shares: Optional[bool] = None

    allow_subscriptions: Optional[bool] = None

    date_format: Optional[str] = None

    date_time_format: Optional[str] = None

    disable_publish: Optional[bool] = None

    enable_legacy_autodetect_types: Optional[bool] = None

    feature_show_permissions_control: Optional[bool] = None

    hide_plotly_mode_bar: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the ClientConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.allow_custom_js_visualizations is not None:
            body["allow_custom_js_visualizations"] = self.allow_custom_js_visualizations
        if self.allow_downloads is not None:
            body["allow_downloads"] = self.allow_downloads
        if self.allow_external_shares is not None:
            body["allow_external_shares"] = self.allow_external_shares
        if self.allow_subscriptions is not None:
            body["allow_subscriptions"] = self.allow_subscriptions
        if self.date_format is not None:
            body["date_format"] = self.date_format
        if self.date_time_format is not None:
            body["date_time_format"] = self.date_time_format
        if self.disable_publish is not None:
            body["disable_publish"] = self.disable_publish
        if self.enable_legacy_autodetect_types is not None:
            body["enable_legacy_autodetect_types"] = self.enable_legacy_autodetect_types
        if self.feature_show_permissions_control is not None:
            body["feature_show_permissions_control"] = self.feature_show_permissions_control
        if self.hide_plotly_mode_bar is not None:
            body["hide_plotly_mode_bar"] = self.hide_plotly_mode_bar
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClientConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.allow_custom_js_visualizations is not None:
            body["allow_custom_js_visualizations"] = self.allow_custom_js_visualizations
        if self.allow_downloads is not None:
            body["allow_downloads"] = self.allow_downloads
        if self.allow_external_shares is not None:
            body["allow_external_shares"] = self.allow_external_shares
        if self.allow_subscriptions is not None:
            body["allow_subscriptions"] = self.allow_subscriptions
        if self.date_format is not None:
            body["date_format"] = self.date_format
        if self.date_time_format is not None:
            body["date_time_format"] = self.date_time_format
        if self.disable_publish is not None:
            body["disable_publish"] = self.disable_publish
        if self.enable_legacy_autodetect_types is not None:
            body["enable_legacy_autodetect_types"] = self.enable_legacy_autodetect_types
        if self.feature_show_permissions_control is not None:
            body["feature_show_permissions_control"] = self.feature_show_permissions_control
        if self.hide_plotly_mode_bar is not None:
            body["hide_plotly_mode_bar"] = self.hide_plotly_mode_bar
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClientConfig:
        """Deserializes the ClientConfig from a dictionary."""
        return cls(
            allow_custom_js_visualizations=d.get("allow_custom_js_visualizations", None),
            allow_downloads=d.get("allow_downloads", None),
            allow_external_shares=d.get("allow_external_shares", None),
            allow_subscriptions=d.get("allow_subscriptions", None),
            date_format=d.get("date_format", None),
            date_time_format=d.get("date_time_format", None),
            disable_publish=d.get("disable_publish", None),
            enable_legacy_autodetect_types=d.get("enable_legacy_autodetect_types", None),
            feature_show_permissions_control=d.get("feature_show_permissions_control", None),
            hide_plotly_mode_bar=d.get("hide_plotly_mode_bar", None),
        )


@dataclass
class ColumnInfo:
    name: Optional[str] = None
    """The name of the column."""

    position: Optional[int] = None
    """The ordinal position of the column (starting at position 0)."""

    type_interval_type: Optional[str] = None
    """The format of the interval type."""

    type_name: Optional[ColumnInfoTypeName] = None
    """The name of the base data type. This doesn't include details for complex types such as STRUCT,
    MAP or ARRAY."""

    type_precision: Optional[int] = None
    """Specifies the number of digits in a number. This applies to the DECIMAL type."""

    type_scale: Optional[int] = None
    """Specifies the number of digits to the right of the decimal point in a number. This applies to
    the DECIMAL type."""

    type_text: Optional[str] = None
    """The full SQL type specification."""

    def as_dict(self) -> dict:
        """Serializes the ColumnInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.position is not None:
            body["position"] = self.position
        if self.type_interval_type is not None:
            body["type_interval_type"] = self.type_interval_type
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
        if self.name is not None:
            body["name"] = self.name
        if self.position is not None:
            body["position"] = self.position
        if self.type_interval_type is not None:
            body["type_interval_type"] = self.type_interval_type
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
            name=d.get("name", None),
            position=d.get("position", None),
            type_interval_type=d.get("type_interval_type", None),
            type_name=_enum(d, "type_name", ColumnInfoTypeName),
            type_precision=d.get("type_precision", None),
            type_scale=d.get("type_scale", None),
            type_text=d.get("type_text", None),
        )


class ColumnInfoTypeName(Enum):
    """The name of the base data type. This doesn't include details for complex types such as STRUCT,
    MAP or ARRAY."""

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
    TIMESTAMP = "TIMESTAMP"
    USER_DEFINED_TYPE = "USER_DEFINED_TYPE"


class ComparisonOperator(Enum):

    EQUAL = "EQUAL"
    GREATER_THAN = "GREATER_THAN"
    GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL"
    IS_NOT_NULL = "IS_NOT_NULL"
    IS_NULL = "IS_NULL"
    LESS_THAN = "LESS_THAN"
    LESS_THAN_OR_EQUAL = "LESS_THAN_OR_EQUAL"
    NOT_EQUAL = "NOT_EQUAL"


@dataclass
class CreateAlertRequestAlert:
    condition: Optional[AlertCondition] = None
    """Trigger conditions of the alert."""

    custom_body: Optional[str] = None
    """Custom body of alert notification, if it exists. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    custom_subject: Optional[str] = None
    """Custom subject of alert notification, if it exists. This can include email subject entries and
    Slack notification headers, for example. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    display_name: Optional[str] = None
    """The display name of the alert."""

    notify_on_ok: Optional[bool] = None
    """Whether to notify alert subscribers when alert returns back to normal."""

    parent_path: Optional[str] = None
    """The workspace path of the folder containing the alert."""

    query_id: Optional[str] = None
    """UUID of the query attached to the alert."""

    seconds_to_retrigger: Optional[int] = None
    """Number of seconds an alert must wait after being triggered to rearm itself. After rearming, it
    can be triggered again. If 0 or not specified, the alert will not be triggered again."""

    def as_dict(self) -> dict:
        """Serializes the CreateAlertRequestAlert into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.condition:
            body["condition"] = self.condition.as_dict()
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateAlertRequestAlert into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.condition:
            body["condition"] = self.condition
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateAlertRequestAlert:
        """Deserializes the CreateAlertRequestAlert from a dictionary."""
        return cls(
            condition=_from_dict(d, "condition", AlertCondition),
            custom_body=d.get("custom_body", None),
            custom_subject=d.get("custom_subject", None),
            display_name=d.get("display_name", None),
            notify_on_ok=d.get("notify_on_ok", None),
            parent_path=d.get("parent_path", None),
            query_id=d.get("query_id", None),
            seconds_to_retrigger=d.get("seconds_to_retrigger", None),
        )


@dataclass
class CreateQueryRequestQuery:
    apply_auto_limit: Optional[bool] = None
    """Whether to apply a 1000 row limit to the query result."""

    catalog: Optional[str] = None
    """Name of the catalog where this query will be executed."""

    description: Optional[str] = None
    """General description that conveys additional information about this query such as usage notes."""

    display_name: Optional[str] = None
    """Display name of the query that appears in list views, widget headings, and on the query page."""

    parameters: Optional[List[QueryParameter]] = None
    """List of query parameter definitions."""

    parent_path: Optional[str] = None
    """Workspace path of the workspace folder containing the object."""

    query_text: Optional[str] = None
    """Text of the query to be run."""

    run_as_mode: Optional[RunAsMode] = None
    """Sets the "Run as" role for the object."""

    schema: Optional[str] = None
    """Name of the schema where this query will be executed."""

    tags: Optional[List[str]] = None

    warehouse_id: Optional[str] = None
    """ID of the SQL warehouse attached to the query."""

    def as_dict(self) -> dict:
        """Serializes the CreateQueryRequestQuery into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode.value
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = [v for v in self.tags]
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateQueryRequestQuery into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.parameters:
            body["parameters"] = self.parameters
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = self.tags
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateQueryRequestQuery:
        """Deserializes the CreateQueryRequestQuery from a dictionary."""
        return cls(
            apply_auto_limit=d.get("apply_auto_limit", None),
            catalog=d.get("catalog", None),
            description=d.get("description", None),
            display_name=d.get("display_name", None),
            parameters=_repeated_dict(d, "parameters", QueryParameter),
            parent_path=d.get("parent_path", None),
            query_text=d.get("query_text", None),
            run_as_mode=_enum(d, "run_as_mode", RunAsMode),
            schema=d.get("schema", None),
            tags=d.get("tags", None),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class CreateVisualizationRequestVisualization:
    display_name: Optional[str] = None
    """The display name of the visualization."""

    query_id: Optional[str] = None
    """UUID of the query that the visualization is attached to."""

    serialized_options: Optional[str] = None
    """The visualization options varies widely from one visualization type to the next and is
    unsupported. Databricks does not recommend modifying visualization options directly."""

    serialized_query_plan: Optional[str] = None
    """The visualization query plan varies widely from one visualization type to the next and is
    unsupported. Databricks does not recommend modifying the visualization query plan directly."""

    type: Optional[str] = None
    """The type of visualization: counter, table, funnel, and so on."""

    def as_dict(self) -> dict:
        """Serializes the CreateVisualizationRequestVisualization into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.serialized_options is not None:
            body["serialized_options"] = self.serialized_options
        if self.serialized_query_plan is not None:
            body["serialized_query_plan"] = self.serialized_query_plan
        if self.type is not None:
            body["type"] = self.type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateVisualizationRequestVisualization into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.serialized_options is not None:
            body["serialized_options"] = self.serialized_options
        if self.serialized_query_plan is not None:
            body["serialized_query_plan"] = self.serialized_query_plan
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateVisualizationRequestVisualization:
        """Deserializes the CreateVisualizationRequestVisualization from a dictionary."""
        return cls(
            display_name=d.get("display_name", None),
            query_id=d.get("query_id", None),
            serialized_options=d.get("serialized_options", None),
            serialized_query_plan=d.get("serialized_query_plan", None),
            type=d.get("type", None),
        )


class CreateWarehouseRequestWarehouseType(Enum):

    CLASSIC = "CLASSIC"
    PRO = "PRO"
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"


@dataclass
class CreateWarehouseResponse:
    id: Optional[str] = None
    """Id for the SQL warehouse. This value is unique across all SQL warehouses."""

    def as_dict(self) -> dict:
        """Serializes the CreateWarehouseResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateWarehouseResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateWarehouseResponse:
        """Deserializes the CreateWarehouseResponse from a dictionary."""
        return cls(id=d.get("id", None))


@dataclass
class CronSchedule:
    quartz_cron_schedule: str
    """A cron expression using quartz syntax that specifies the schedule for this pipeline. Should use
    the quartz format described here:
    http://www.quartz-scheduler.org/documentation/quartz-2.1.7/tutorials/tutorial-lesson-06.html"""

    timezone_id: str
    """A Java timezone id. The schedule will be resolved using this timezone. This will be combined
    with the quartz_cron_schedule to determine the schedule. See
    https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-conf-mgmt-set-timezone.html
    for details."""

    pause_status: Optional[SchedulePauseStatus] = None
    """Indicate whether this schedule is paused or not."""

    def as_dict(self) -> dict:
        """Serializes the CronSchedule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status.value
        if self.quartz_cron_schedule is not None:
            body["quartz_cron_schedule"] = self.quartz_cron_schedule
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CronSchedule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status
        if self.quartz_cron_schedule is not None:
            body["quartz_cron_schedule"] = self.quartz_cron_schedule
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CronSchedule:
        """Deserializes the CronSchedule from a dictionary."""
        return cls(
            pause_status=_enum(d, "pause_status", SchedulePauseStatus),
            quartz_cron_schedule=d.get("quartz_cron_schedule", None),
            timezone_id=d.get("timezone_id", None),
        )


@dataclass
class Dashboard:
    """A JSON representing a dashboard containing widgets of visualizations and text boxes."""

    can_edit: Optional[bool] = None
    """Whether the authenticated user can edit the query definition."""

    created_at: Optional[str] = None
    """Timestamp when this dashboard was created."""

    dashboard_filters_enabled: Optional[bool] = None
    """In the web application, query filters that share a name are coupled to a single selection box if
    this value is `true`."""

    id: Optional[str] = None
    """The ID for this dashboard."""

    is_archived: Optional[bool] = None
    """Indicates whether a dashboard is trashed. Trashed dashboards won't appear in list views. If this
    boolean is `true`, the `options` property for this dashboard includes a `moved_to_trash_at`
    timestamp. Items in trash are permanently deleted after 30 days."""

    is_draft: Optional[bool] = None
    """Whether a dashboard is a draft. Draft dashboards only appear in list views for their owners."""

    is_favorite: Optional[bool] = None
    """Indicates whether this query object appears in the current user's favorites list. This flag
    determines whether the star icon for favorites is selected."""

    name: Optional[str] = None
    """The title of the dashboard that appears in list views and at the top of the dashboard page."""

    options: Optional[DashboardOptions] = None

    parent: Optional[str] = None
    """The identifier of the workspace folder containing the object."""

    permission_tier: Optional[PermissionLevel] = None
    """* `CAN_VIEW`: Can view the query * `CAN_RUN`: Can run the query * `CAN_EDIT`: Can edit the query
    * `CAN_MANAGE`: Can manage the query"""

    slug: Optional[str] = None
    """URL slug. Usually mirrors the query name with dashes (`-`) instead of spaces. Appears in the URL
    for this query."""

    tags: Optional[List[str]] = None

    updated_at: Optional[str] = None
    """Timestamp when this dashboard was last updated."""

    user: Optional[User] = None

    user_id: Optional[int] = None
    """The ID of the user who owns the dashboard."""

    widgets: Optional[List[Widget]] = None

    def as_dict(self) -> dict:
        """Serializes the Dashboard into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.can_edit is not None:
            body["can_edit"] = self.can_edit
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.dashboard_filters_enabled is not None:
            body["dashboard_filters_enabled"] = self.dashboard_filters_enabled
        if self.id is not None:
            body["id"] = self.id
        if self.is_archived is not None:
            body["is_archived"] = self.is_archived
        if self.is_draft is not None:
            body["is_draft"] = self.is_draft
        if self.is_favorite is not None:
            body["is_favorite"] = self.is_favorite
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options.as_dict()
        if self.parent is not None:
            body["parent"] = self.parent
        if self.permission_tier is not None:
            body["permission_tier"] = self.permission_tier.value
        if self.slug is not None:
            body["slug"] = self.slug
        if self.tags:
            body["tags"] = [v for v in self.tags]
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user:
            body["user"] = self.user.as_dict()
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.widgets:
            body["widgets"] = [v.as_dict() for v in self.widgets]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Dashboard into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.can_edit is not None:
            body["can_edit"] = self.can_edit
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.dashboard_filters_enabled is not None:
            body["dashboard_filters_enabled"] = self.dashboard_filters_enabled
        if self.id is not None:
            body["id"] = self.id
        if self.is_archived is not None:
            body["is_archived"] = self.is_archived
        if self.is_draft is not None:
            body["is_draft"] = self.is_draft
        if self.is_favorite is not None:
            body["is_favorite"] = self.is_favorite
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.parent is not None:
            body["parent"] = self.parent
        if self.permission_tier is not None:
            body["permission_tier"] = self.permission_tier
        if self.slug is not None:
            body["slug"] = self.slug
        if self.tags:
            body["tags"] = self.tags
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user:
            body["user"] = self.user
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.widgets:
            body["widgets"] = self.widgets
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Dashboard:
        """Deserializes the Dashboard from a dictionary."""
        return cls(
            can_edit=d.get("can_edit", None),
            created_at=d.get("created_at", None),
            dashboard_filters_enabled=d.get("dashboard_filters_enabled", None),
            id=d.get("id", None),
            is_archived=d.get("is_archived", None),
            is_draft=d.get("is_draft", None),
            is_favorite=d.get("is_favorite", None),
            name=d.get("name", None),
            options=_from_dict(d, "options", DashboardOptions),
            parent=d.get("parent", None),
            permission_tier=_enum(d, "permission_tier", PermissionLevel),
            slug=d.get("slug", None),
            tags=d.get("tags", None),
            updated_at=d.get("updated_at", None),
            user=_from_dict(d, "user", User),
            user_id=d.get("user_id", None),
            widgets=_repeated_dict(d, "widgets", Widget),
        )


@dataclass
class DashboardOptions:
    moved_to_trash_at: Optional[str] = None
    """The timestamp when this dashboard was moved to trash. Only present when the `is_archived`
    property is `true`. Trashed items are deleted after thirty days."""

    def as_dict(self) -> dict:
        """Serializes the DashboardOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.moved_to_trash_at is not None:
            body["moved_to_trash_at"] = self.moved_to_trash_at
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DashboardOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.moved_to_trash_at is not None:
            body["moved_to_trash_at"] = self.moved_to_trash_at
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DashboardOptions:
        """Deserializes the DashboardOptions from a dictionary."""
        return cls(moved_to_trash_at=d.get("moved_to_trash_at", None))


@dataclass
class DataSource:
    """A JSON object representing a DBSQL data source / SQL warehouse."""

    id: Optional[str] = None
    """Data source ID maps to the ID of the data source used by the resource and is distinct from the
    warehouse ID. [Learn more]
    
    [Learn more]: https://docs.databricks.com/api/workspace/datasources/list"""

    name: Optional[str] = None
    """The string name of this data source / SQL warehouse as it appears in the Databricks SQL web
    application."""

    pause_reason: Optional[str] = None
    """Reserved for internal use."""

    paused: Optional[int] = None
    """Reserved for internal use."""

    supports_auto_limit: Optional[bool] = None
    """Reserved for internal use."""

    syntax: Optional[str] = None
    """Reserved for internal use."""

    type: Optional[str] = None
    """The type of data source. For SQL warehouses, this will be `databricks_internal`."""

    view_only: Optional[bool] = None
    """Reserved for internal use."""

    warehouse_id: Optional[str] = None
    """The ID of the associated SQL warehouse, if this data source is backed by a SQL warehouse."""

    def as_dict(self) -> dict:
        """Serializes the DataSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.pause_reason is not None:
            body["pause_reason"] = self.pause_reason
        if self.paused is not None:
            body["paused"] = self.paused
        if self.supports_auto_limit is not None:
            body["supports_auto_limit"] = self.supports_auto_limit
        if self.syntax is not None:
            body["syntax"] = self.syntax
        if self.type is not None:
            body["type"] = self.type
        if self.view_only is not None:
            body["view_only"] = self.view_only
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DataSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.pause_reason is not None:
            body["pause_reason"] = self.pause_reason
        if self.paused is not None:
            body["paused"] = self.paused
        if self.supports_auto_limit is not None:
            body["supports_auto_limit"] = self.supports_auto_limit
        if self.syntax is not None:
            body["syntax"] = self.syntax
        if self.type is not None:
            body["type"] = self.type
        if self.view_only is not None:
            body["view_only"] = self.view_only
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DataSource:
        """Deserializes the DataSource from a dictionary."""
        return cls(
            id=d.get("id", None),
            name=d.get("name", None),
            pause_reason=d.get("pause_reason", None),
            paused=d.get("paused", None),
            supports_auto_limit=d.get("supports_auto_limit", None),
            syntax=d.get("syntax", None),
            type=d.get("type", None),
            view_only=d.get("view_only", None),
            warehouse_id=d.get("warehouse_id", None),
        )


class DatePrecision(Enum):

    DAY_PRECISION = "DAY_PRECISION"
    MINUTE_PRECISION = "MINUTE_PRECISION"
    SECOND_PRECISION = "SECOND_PRECISION"


@dataclass
class DateRange:
    start: str

    end: str

    def as_dict(self) -> dict:
        """Serializes the DateRange into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.end is not None:
            body["end"] = self.end
        if self.start is not None:
            body["start"] = self.start
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DateRange into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.end is not None:
            body["end"] = self.end
        if self.start is not None:
            body["start"] = self.start
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DateRange:
        """Deserializes the DateRange from a dictionary."""
        return cls(end=d.get("end", None), start=d.get("start", None))


@dataclass
class DateRangeValue:
    date_range_value: Optional[DateRange] = None
    """Manually specified date-time range value."""

    dynamic_date_range_value: Optional[DateRangeValueDynamicDateRange] = None
    """Dynamic date-time range value based on current date-time."""

    precision: Optional[DatePrecision] = None
    """Date-time precision to format the value into when the query is run. Defaults to DAY_PRECISION
    (YYYY-MM-DD)."""

    start_day_of_week: Optional[int] = None

    def as_dict(self) -> dict:
        """Serializes the DateRangeValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.date_range_value:
            body["date_range_value"] = self.date_range_value.as_dict()
        if self.dynamic_date_range_value is not None:
            body["dynamic_date_range_value"] = self.dynamic_date_range_value.value
        if self.precision is not None:
            body["precision"] = self.precision.value
        if self.start_day_of_week is not None:
            body["start_day_of_week"] = self.start_day_of_week
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DateRangeValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.date_range_value:
            body["date_range_value"] = self.date_range_value
        if self.dynamic_date_range_value is not None:
            body["dynamic_date_range_value"] = self.dynamic_date_range_value
        if self.precision is not None:
            body["precision"] = self.precision
        if self.start_day_of_week is not None:
            body["start_day_of_week"] = self.start_day_of_week
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DateRangeValue:
        """Deserializes the DateRangeValue from a dictionary."""
        return cls(
            date_range_value=_from_dict(d, "date_range_value", DateRange),
            dynamic_date_range_value=_enum(d, "dynamic_date_range_value", DateRangeValueDynamicDateRange),
            precision=_enum(d, "precision", DatePrecision),
            start_day_of_week=d.get("start_day_of_week", None),
        )


class DateRangeValueDynamicDateRange(Enum):

    LAST_12_MONTHS = "LAST_12_MONTHS"
    LAST_14_DAYS = "LAST_14_DAYS"
    LAST_24_HOURS = "LAST_24_HOURS"
    LAST_30_DAYS = "LAST_30_DAYS"
    LAST_60_DAYS = "LAST_60_DAYS"
    LAST_7_DAYS = "LAST_7_DAYS"
    LAST_8_HOURS = "LAST_8_HOURS"
    LAST_90_DAYS = "LAST_90_DAYS"
    LAST_HOUR = "LAST_HOUR"
    LAST_MONTH = "LAST_MONTH"
    LAST_WEEK = "LAST_WEEK"
    LAST_YEAR = "LAST_YEAR"
    THIS_MONTH = "THIS_MONTH"
    THIS_WEEK = "THIS_WEEK"
    THIS_YEAR = "THIS_YEAR"
    TODAY = "TODAY"
    YESTERDAY = "YESTERDAY"


@dataclass
class DateValue:
    date_value: Optional[str] = None
    """Manually specified date-time value."""

    dynamic_date_value: Optional[DateValueDynamicDate] = None
    """Dynamic date-time value based on current date-time."""

    precision: Optional[DatePrecision] = None
    """Date-time precision to format the value into when the query is run. Defaults to DAY_PRECISION
    (YYYY-MM-DD)."""

    def as_dict(self) -> dict:
        """Serializes the DateValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.date_value is not None:
            body["date_value"] = self.date_value
        if self.dynamic_date_value is not None:
            body["dynamic_date_value"] = self.dynamic_date_value.value
        if self.precision is not None:
            body["precision"] = self.precision.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DateValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.date_value is not None:
            body["date_value"] = self.date_value
        if self.dynamic_date_value is not None:
            body["dynamic_date_value"] = self.dynamic_date_value
        if self.precision is not None:
            body["precision"] = self.precision
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DateValue:
        """Deserializes the DateValue from a dictionary."""
        return cls(
            date_value=d.get("date_value", None),
            dynamic_date_value=_enum(d, "dynamic_date_value", DateValueDynamicDate),
            precision=_enum(d, "precision", DatePrecision),
        )


class DateValueDynamicDate(Enum):

    NOW = "NOW"
    YESTERDAY = "YESTERDAY"


@dataclass
class DefaultWarehouseOverride:
    """Represents a per-user default warehouse override configuration. This resource allows users or
    administrators to customize how a user's default warehouse is selected for SQL operations. If no
    override exists for a user, the workspace default warehouse will be used."""

    type: DefaultWarehouseOverrideType
    """The type of override behavior."""

    default_warehouse_override_id: Optional[str] = None
    """The ID component of the resource name (user ID)."""

    name: Optional[str] = None
    """The resource name of the default warehouse override. Format:
    default-warehouse-overrides/{default_warehouse_override_id}"""

    warehouse_id: Optional[str] = None
    """The specific warehouse ID when type is CUSTOM. Not set for LAST_SELECTED type."""

    def as_dict(self) -> dict:
        """Serializes the DefaultWarehouseOverride into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default_warehouse_override_id is not None:
            body["default_warehouse_override_id"] = self.default_warehouse_override_id
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type.value
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DefaultWarehouseOverride into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default_warehouse_override_id is not None:
            body["default_warehouse_override_id"] = self.default_warehouse_override_id
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DefaultWarehouseOverride:
        """Deserializes the DefaultWarehouseOverride from a dictionary."""
        return cls(
            default_warehouse_override_id=d.get("default_warehouse_override_id", None),
            name=d.get("name", None),
            type=_enum(d, "type", DefaultWarehouseOverrideType),
            warehouse_id=d.get("warehouse_id", None),
        )


class DefaultWarehouseOverrideType(Enum):
    """Type of default warehouse override behavior."""

    CUSTOM = "CUSTOM"
    LAST_SELECTED = "LAST_SELECTED"


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
class DeleteWarehouseResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteWarehouseResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteWarehouseResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteWarehouseResponse:
        """Deserializes the DeleteWarehouseResponse from a dictionary."""
        return cls()


class Disposition(Enum):

    EXTERNAL_LINKS = "EXTERNAL_LINKS"
    INLINE = "INLINE"


class EditWarehouseRequestWarehouseType(Enum):

    CLASSIC = "CLASSIC"
    PRO = "PRO"
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"


@dataclass
class EditWarehouseResponse:
    def as_dict(self) -> dict:
        """Serializes the EditWarehouseResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EditWarehouseResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EditWarehouseResponse:
        """Deserializes the EditWarehouseResponse from a dictionary."""
        return cls()


@dataclass
class Empty:
    """Represents an empty message, similar to google.protobuf.Empty, which is not available in the
    firm right now."""

    def as_dict(self) -> dict:
        """Serializes the Empty into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Empty into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Empty:
        """Deserializes the Empty from a dictionary."""
        return cls()


@dataclass
class EndpointConfPair:
    key: Optional[str] = None

    value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the EndpointConfPair into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointConfPair into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointConfPair:
        """Deserializes the EndpointConfPair from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class EndpointHealth:
    details: Optional[str] = None
    """Details about errors that are causing current degraded/failed status."""

    failure_reason: Optional[TerminationReason] = None
    """The reason for failure to bring up clusters for this warehouse. This is available when status is
    'FAILED' and sometimes when it is DEGRADED."""

    message: Optional[str] = None
    """Deprecated. split into summary and details for security"""

    status: Optional[Status] = None
    """Health status of the endpoint."""

    summary: Optional[str] = None
    """A short summary of the health status in case of degraded/failed warehouses."""

    def as_dict(self) -> dict:
        """Serializes the EndpointHealth into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.details is not None:
            body["details"] = self.details
        if self.failure_reason:
            body["failure_reason"] = self.failure_reason.as_dict()
        if self.message is not None:
            body["message"] = self.message
        if self.status is not None:
            body["status"] = self.status.value
        if self.summary is not None:
            body["summary"] = self.summary
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointHealth into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.details is not None:
            body["details"] = self.details
        if self.failure_reason:
            body["failure_reason"] = self.failure_reason
        if self.message is not None:
            body["message"] = self.message
        if self.status is not None:
            body["status"] = self.status
        if self.summary is not None:
            body["summary"] = self.summary
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointHealth:
        """Deserializes the EndpointHealth from a dictionary."""
        return cls(
            details=d.get("details", None),
            failure_reason=_from_dict(d, "failure_reason", TerminationReason),
            message=d.get("message", None),
            status=_enum(d, "status", Status),
            summary=d.get("summary", None),
        )


@dataclass
class EndpointInfo:
    auto_stop_mins: Optional[int] = None
    """The amount of time in minutes that a SQL warehouse must be idle (i.e., no RUNNING queries)
    before it is automatically stopped.
    
    Supported values: - Must be == 0 or >= 10 mins - 0 indicates no autostop.
    
    Defaults to 120 mins"""

    channel: Optional[Channel] = None
    """Channel Details"""

    cluster_size: Optional[str] = None
    """Size of the clusters allocated for this warehouse. Increasing the size of a spark cluster allows
    you to run larger queries on it. If you want to increase the number of concurrent queries,
    please tune max_num_clusters.
    
    Supported values: - 2X-Small - X-Small - Small - Medium - Large - X-Large - 2X-Large - 3X-Large
    - 4X-Large - 5X-Large"""

    creator_name: Optional[str] = None
    """warehouse creator name"""

    enable_photon: Optional[bool] = None
    """Configures whether the warehouse should use Photon optimized clusters.
    
    Defaults to true."""

    enable_serverless_compute: Optional[bool] = None
    """Configures whether the warehouse should use serverless compute"""

    health: Optional[EndpointHealth] = None
    """Optional health status. Assume the warehouse is healthy if this field is not set."""

    id: Optional[str] = None
    """unique identifier for warehouse"""

    instance_profile_arn: Optional[str] = None
    """Deprecated. Instance profile used to pass IAM role to the cluster"""

    jdbc_url: Optional[str] = None
    """the jdbc connection string for this warehouse"""

    max_num_clusters: Optional[int] = None
    """Maximum number of clusters that the autoscaler will create to handle concurrent queries.
    
    Supported values: - Must be >= min_num_clusters - Must be <= 40.
    
    Defaults to min_clusters if unset."""

    min_num_clusters: Optional[int] = None
    """Minimum number of available clusters that will be maintained for this SQL warehouse. Increasing
    this will ensure that a larger number of clusters are always running and therefore may reduce
    the cold start time for new queries. This is similar to reserved vs. revocable cores in a
    resource manager.
    
    Supported values: - Must be > 0 - Must be <= min(max_num_clusters, 30)
    
    Defaults to 1"""

    name: Optional[str] = None
    """Logical name for the cluster.
    
    Supported values: - Must be unique within an org. - Must be less than 100 characters."""

    num_active_sessions: Optional[int] = None
    """Deprecated. current number of active sessions for the warehouse"""

    num_clusters: Optional[int] = None
    """current number of clusters running for the service"""

    odbc_params: Optional[OdbcParams] = None
    """ODBC parameters for the SQL warehouse"""

    spot_instance_policy: Optional[SpotInstancePolicy] = None
    """Configurations whether the endpoint should use spot instances."""

    state: Optional[State] = None
    """state of the endpoint"""

    tags: Optional[EndpointTags] = None
    """A set of key-value pairs that will be tagged on all resources (e.g., AWS instances and EBS
    volumes) associated with this SQL warehouse.
    
    Supported values: - Number of tags < 45."""

    warehouse_type: Optional[EndpointInfoWarehouseType] = None
    """Warehouse type: `PRO` or `CLASSIC`. If you want to use serverless compute, you must set to `PRO`
    and also set the field `enable_serverless_compute` to `true`."""

    def as_dict(self) -> dict:
        """Serializes the EndpointInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.auto_stop_mins is not None:
            body["auto_stop_mins"] = self.auto_stop_mins
        if self.channel:
            body["channel"] = self.channel.as_dict()
        if self.cluster_size is not None:
            body["cluster_size"] = self.cluster_size
        if self.creator_name is not None:
            body["creator_name"] = self.creator_name
        if self.enable_photon is not None:
            body["enable_photon"] = self.enable_photon
        if self.enable_serverless_compute is not None:
            body["enable_serverless_compute"] = self.enable_serverless_compute
        if self.health:
            body["health"] = self.health.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.instance_profile_arn is not None:
            body["instance_profile_arn"] = self.instance_profile_arn
        if self.jdbc_url is not None:
            body["jdbc_url"] = self.jdbc_url
        if self.max_num_clusters is not None:
            body["max_num_clusters"] = self.max_num_clusters
        if self.min_num_clusters is not None:
            body["min_num_clusters"] = self.min_num_clusters
        if self.name is not None:
            body["name"] = self.name
        if self.num_active_sessions is not None:
            body["num_active_sessions"] = self.num_active_sessions
        if self.num_clusters is not None:
            body["num_clusters"] = self.num_clusters
        if self.odbc_params:
            body["odbc_params"] = self.odbc_params.as_dict()
        if self.spot_instance_policy is not None:
            body["spot_instance_policy"] = self.spot_instance_policy.value
        if self.state is not None:
            body["state"] = self.state.value
        if self.tags:
            body["tags"] = self.tags.as_dict()
        if self.warehouse_type is not None:
            body["warehouse_type"] = self.warehouse_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.auto_stop_mins is not None:
            body["auto_stop_mins"] = self.auto_stop_mins
        if self.channel:
            body["channel"] = self.channel
        if self.cluster_size is not None:
            body["cluster_size"] = self.cluster_size
        if self.creator_name is not None:
            body["creator_name"] = self.creator_name
        if self.enable_photon is not None:
            body["enable_photon"] = self.enable_photon
        if self.enable_serverless_compute is not None:
            body["enable_serverless_compute"] = self.enable_serverless_compute
        if self.health:
            body["health"] = self.health
        if self.id is not None:
            body["id"] = self.id
        if self.instance_profile_arn is not None:
            body["instance_profile_arn"] = self.instance_profile_arn
        if self.jdbc_url is not None:
            body["jdbc_url"] = self.jdbc_url
        if self.max_num_clusters is not None:
            body["max_num_clusters"] = self.max_num_clusters
        if self.min_num_clusters is not None:
            body["min_num_clusters"] = self.min_num_clusters
        if self.name is not None:
            body["name"] = self.name
        if self.num_active_sessions is not None:
            body["num_active_sessions"] = self.num_active_sessions
        if self.num_clusters is not None:
            body["num_clusters"] = self.num_clusters
        if self.odbc_params:
            body["odbc_params"] = self.odbc_params
        if self.spot_instance_policy is not None:
            body["spot_instance_policy"] = self.spot_instance_policy
        if self.state is not None:
            body["state"] = self.state
        if self.tags:
            body["tags"] = self.tags
        if self.warehouse_type is not None:
            body["warehouse_type"] = self.warehouse_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointInfo:
        """Deserializes the EndpointInfo from a dictionary."""
        return cls(
            auto_stop_mins=d.get("auto_stop_mins", None),
            channel=_from_dict(d, "channel", Channel),
            cluster_size=d.get("cluster_size", None),
            creator_name=d.get("creator_name", None),
            enable_photon=d.get("enable_photon", None),
            enable_serverless_compute=d.get("enable_serverless_compute", None),
            health=_from_dict(d, "health", EndpointHealth),
            id=d.get("id", None),
            instance_profile_arn=d.get("instance_profile_arn", None),
            jdbc_url=d.get("jdbc_url", None),
            max_num_clusters=d.get("max_num_clusters", None),
            min_num_clusters=d.get("min_num_clusters", None),
            name=d.get("name", None),
            num_active_sessions=d.get("num_active_sessions", None),
            num_clusters=d.get("num_clusters", None),
            odbc_params=_from_dict(d, "odbc_params", OdbcParams),
            spot_instance_policy=_enum(d, "spot_instance_policy", SpotInstancePolicy),
            state=_enum(d, "state", State),
            tags=_from_dict(d, "tags", EndpointTags),
            warehouse_type=_enum(d, "warehouse_type", EndpointInfoWarehouseType),
        )


class EndpointInfoWarehouseType(Enum):

    CLASSIC = "CLASSIC"
    PRO = "PRO"
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"


@dataclass
class EndpointTagPair:
    key: Optional[str] = None

    value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the EndpointTagPair into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointTagPair into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointTagPair:
        """Deserializes the EndpointTagPair from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class EndpointTags:
    custom_tags: Optional[List[EndpointTagPair]] = None

    def as_dict(self) -> dict:
        """Serializes the EndpointTags into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.custom_tags:
            body["custom_tags"] = [v.as_dict() for v in self.custom_tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EndpointTags into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EndpointTags:
        """Deserializes the EndpointTags from a dictionary."""
        return cls(custom_tags=_repeated_dict(d, "custom_tags", EndpointTagPair))


@dataclass
class EnumValue:
    enum_options: Optional[str] = None
    """List of valid query parameter values, newline delimited."""

    multi_values_options: Optional[MultiValuesOptions] = None
    """If specified, allows multiple values to be selected for this parameter."""

    values: Optional[List[str]] = None
    """List of selected query parameter values."""

    def as_dict(self) -> dict:
        """Serializes the EnumValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enum_options is not None:
            body["enum_options"] = self.enum_options
        if self.multi_values_options:
            body["multi_values_options"] = self.multi_values_options.as_dict()
        if self.values:
            body["values"] = [v for v in self.values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnumValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enum_options is not None:
            body["enum_options"] = self.enum_options
        if self.multi_values_options:
            body["multi_values_options"] = self.multi_values_options
        if self.values:
            body["values"] = self.values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnumValue:
        """Deserializes the EnumValue from a dictionary."""
        return cls(
            enum_options=d.get("enum_options", None),
            multi_values_options=_from_dict(d, "multi_values_options", MultiValuesOptions),
            values=d.get("values", None),
        )


class ExecuteStatementRequestOnWaitTimeout(Enum):
    """When `wait_timeout > 0s`, the call will block up to the specified time. If the statement
    execution doesn't finish within this time, `on_wait_timeout` determines whether the execution
    should continue or be canceled. When set to `CONTINUE`, the statement execution continues
    asynchronously and the call returns a statement ID which can be used for polling with
    :method:statementexecution/getStatement. When set to `CANCEL`, the statement execution is
    canceled and the call returns with a `CANCELED` state."""

    CANCEL = "CANCEL"
    CONTINUE = "CONTINUE"


@dataclass
class ExternalLink:
    byte_count: Optional[int] = None
    """The number of bytes in the result chunk. This field is not available when using `INLINE`
    disposition."""

    chunk_index: Optional[int] = None
    """The position within the sequence of result set chunks."""

    expiration: Optional[str] = None
    """Indicates the date-time that the given external link will expire and becomes invalid, after
    which point a new `external_link` must be requested."""

    external_link: Optional[str] = None
    """A URL pointing to a chunk of result data, hosted by an external service, with a short expiration
    time (<= 15 minutes). As this URL contains a temporary credential, it should be considered
    sensitive and the client should not expose this URL in a log."""

    http_headers: Optional[Dict[str, str]] = None
    """HTTP headers that must be included with a GET request to the `external_link`. Each header is
    provided as a key-value pair. Headers are typically used to pass a decryption key to the
    external service. The values of these headers should be considered sensitive and the client
    should not expose these values in a log."""

    next_chunk_index: Optional[int] = None
    """When fetching, provides the `chunk_index` for the _next_ chunk. If absent, indicates there are
    no more chunks. The next chunk can be fetched with a
    :method:statementexecution/getstatementresultchunkn request."""

    next_chunk_internal_link: Optional[str] = None
    """When fetching, provides a link to fetch the _next_ chunk. If absent, indicates there are no more
    chunks. This link is an absolute `path` to be joined with your `$DATABRICKS_HOST`, and should be
    treated as an opaque link. This is an alternative to using `next_chunk_index`."""

    row_count: Optional[int] = None
    """The number of rows within the result chunk."""

    row_offset: Optional[int] = None
    """The starting row offset within the result set."""

    def as_dict(self) -> dict:
        """Serializes the ExternalLink into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.byte_count is not None:
            body["byte_count"] = self.byte_count
        if self.chunk_index is not None:
            body["chunk_index"] = self.chunk_index
        if self.expiration is not None:
            body["expiration"] = self.expiration
        if self.external_link is not None:
            body["external_link"] = self.external_link
        if self.http_headers:
            body["http_headers"] = self.http_headers
        if self.next_chunk_index is not None:
            body["next_chunk_index"] = self.next_chunk_index
        if self.next_chunk_internal_link is not None:
            body["next_chunk_internal_link"] = self.next_chunk_internal_link
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.row_offset is not None:
            body["row_offset"] = self.row_offset
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalLink into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.byte_count is not None:
            body["byte_count"] = self.byte_count
        if self.chunk_index is not None:
            body["chunk_index"] = self.chunk_index
        if self.expiration is not None:
            body["expiration"] = self.expiration
        if self.external_link is not None:
            body["external_link"] = self.external_link
        if self.http_headers:
            body["http_headers"] = self.http_headers
        if self.next_chunk_index is not None:
            body["next_chunk_index"] = self.next_chunk_index
        if self.next_chunk_internal_link is not None:
            body["next_chunk_internal_link"] = self.next_chunk_internal_link
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.row_offset is not None:
            body["row_offset"] = self.row_offset
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalLink:
        """Deserializes the ExternalLink from a dictionary."""
        return cls(
            byte_count=d.get("byte_count", None),
            chunk_index=d.get("chunk_index", None),
            expiration=d.get("expiration", None),
            external_link=d.get("external_link", None),
            http_headers=d.get("http_headers", None),
            next_chunk_index=d.get("next_chunk_index", None),
            next_chunk_internal_link=d.get("next_chunk_internal_link", None),
            row_count=d.get("row_count", None),
            row_offset=d.get("row_offset", None),
        )


@dataclass
class ExternalQuerySource:
    alert_id: Optional[str] = None
    """The canonical identifier for this SQL alert"""

    dashboard_id: Optional[str] = None
    """The canonical identifier for this Lakeview dashboard"""

    genie_space_id: Optional[str] = None
    """The canonical identifier for this Genie space"""

    job_info: Optional[ExternalQuerySourceJobInfo] = None

    legacy_dashboard_id: Optional[str] = None
    """The canonical identifier for this legacy dashboard"""

    notebook_id: Optional[str] = None
    """The canonical identifier for this notebook"""

    sql_query_id: Optional[str] = None
    """The canonical identifier for this SQL query"""

    def as_dict(self) -> dict:
        """Serializes the ExternalQuerySource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alert_id is not None:
            body["alert_id"] = self.alert_id
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.genie_space_id is not None:
            body["genie_space_id"] = self.genie_space_id
        if self.job_info:
            body["job_info"] = self.job_info.as_dict()
        if self.legacy_dashboard_id is not None:
            body["legacy_dashboard_id"] = self.legacy_dashboard_id
        if self.notebook_id is not None:
            body["notebook_id"] = self.notebook_id
        if self.sql_query_id is not None:
            body["sql_query_id"] = self.sql_query_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalQuerySource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alert_id is not None:
            body["alert_id"] = self.alert_id
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.genie_space_id is not None:
            body["genie_space_id"] = self.genie_space_id
        if self.job_info:
            body["job_info"] = self.job_info
        if self.legacy_dashboard_id is not None:
            body["legacy_dashboard_id"] = self.legacy_dashboard_id
        if self.notebook_id is not None:
            body["notebook_id"] = self.notebook_id
        if self.sql_query_id is not None:
            body["sql_query_id"] = self.sql_query_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalQuerySource:
        """Deserializes the ExternalQuerySource from a dictionary."""
        return cls(
            alert_id=d.get("alert_id", None),
            dashboard_id=d.get("dashboard_id", None),
            genie_space_id=d.get("genie_space_id", None),
            job_info=_from_dict(d, "job_info", ExternalQuerySourceJobInfo),
            legacy_dashboard_id=d.get("legacy_dashboard_id", None),
            notebook_id=d.get("notebook_id", None),
            sql_query_id=d.get("sql_query_id", None),
        )


@dataclass
class ExternalQuerySourceJobInfo:
    job_id: Optional[str] = None
    """The canonical identifier for this job."""

    job_run_id: Optional[str] = None
    """The canonical identifier of the run. This ID is unique across all runs of all jobs."""

    job_task_run_id: Optional[str] = None
    """The canonical identifier of the task run."""

    def as_dict(self) -> dict:
        """Serializes the ExternalQuerySourceJobInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        if self.job_task_run_id is not None:
            body["job_task_run_id"] = self.job_task_run_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExternalQuerySourceJobInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        if self.job_task_run_id is not None:
            body["job_task_run_id"] = self.job_task_run_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExternalQuerySourceJobInfo:
        """Deserializes the ExternalQuerySourceJobInfo from a dictionary."""
        return cls(
            job_id=d.get("job_id", None),
            job_run_id=d.get("job_run_id", None),
            job_task_run_id=d.get("job_task_run_id", None),
        )


class Format(Enum):

    ARROW_STREAM = "ARROW_STREAM"
    CSV = "CSV"
    JSON_ARRAY = "JSON_ARRAY"


@dataclass
class GetResponse:
    access_control_list: Optional[List[AccessControl]] = None

    object_id: Optional[str] = None
    """An object's type and UUID, separated by a forward slash (/) character."""

    object_type: Optional[ObjectType] = None
    """A singular noun object type."""

    def as_dict(self) -> dict:
        """Serializes the GetResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetResponse:
        """Deserializes the GetResponse from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", AccessControl),
            object_id=d.get("object_id", None),
            object_type=_enum(d, "object_type", ObjectType),
        )


@dataclass
class GetWarehousePermissionLevelsResponse:
    permission_levels: Optional[List[WarehousePermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetWarehousePermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetWarehousePermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetWarehousePermissionLevelsResponse:
        """Deserializes the GetWarehousePermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", WarehousePermissionsDescription))


@dataclass
class GetWarehouseResponse:
    auto_stop_mins: Optional[int] = None
    """The amount of time in minutes that a SQL warehouse must be idle (i.e., no RUNNING queries)
    before it is automatically stopped.
    
    Supported values: - Must be == 0 or >= 10 mins - 0 indicates no autostop.
    
    Defaults to 120 mins"""

    channel: Optional[Channel] = None
    """Channel Details"""

    cluster_size: Optional[str] = None
    """Size of the clusters allocated for this warehouse. Increasing the size of a spark cluster allows
    you to run larger queries on it. If you want to increase the number of concurrent queries,
    please tune max_num_clusters.
    
    Supported values: - 2X-Small - X-Small - Small - Medium - Large - X-Large - 2X-Large - 3X-Large
    - 4X-Large - 5X-Large"""

    creator_name: Optional[str] = None
    """warehouse creator name"""

    enable_photon: Optional[bool] = None
    """Configures whether the warehouse should use Photon optimized clusters.
    
    Defaults to true."""

    enable_serverless_compute: Optional[bool] = None
    """Configures whether the warehouse should use serverless compute"""

    health: Optional[EndpointHealth] = None
    """Optional health status. Assume the warehouse is healthy if this field is not set."""

    id: Optional[str] = None
    """unique identifier for warehouse"""

    instance_profile_arn: Optional[str] = None
    """Deprecated. Instance profile used to pass IAM role to the cluster"""

    jdbc_url: Optional[str] = None
    """the jdbc connection string for this warehouse"""

    max_num_clusters: Optional[int] = None
    """Maximum number of clusters that the autoscaler will create to handle concurrent queries.
    
    Supported values: - Must be >= min_num_clusters - Must be <= 40.
    
    Defaults to min_clusters if unset."""

    min_num_clusters: Optional[int] = None
    """Minimum number of available clusters that will be maintained for this SQL warehouse. Increasing
    this will ensure that a larger number of clusters are always running and therefore may reduce
    the cold start time for new queries. This is similar to reserved vs. revocable cores in a
    resource manager.
    
    Supported values: - Must be > 0 - Must be <= min(max_num_clusters, 30)
    
    Defaults to 1"""

    name: Optional[str] = None
    """Logical name for the cluster.
    
    Supported values: - Must be unique within an org. - Must be less than 100 characters."""

    num_active_sessions: Optional[int] = None
    """Deprecated. current number of active sessions for the warehouse"""

    num_clusters: Optional[int] = None
    """current number of clusters running for the service"""

    odbc_params: Optional[OdbcParams] = None
    """ODBC parameters for the SQL warehouse"""

    spot_instance_policy: Optional[SpotInstancePolicy] = None
    """Configurations whether the endpoint should use spot instances."""

    state: Optional[State] = None
    """state of the endpoint"""

    tags: Optional[EndpointTags] = None
    """A set of key-value pairs that will be tagged on all resources (e.g., AWS instances and EBS
    volumes) associated with this SQL warehouse.
    
    Supported values: - Number of tags < 45."""

    warehouse_type: Optional[GetWarehouseResponseWarehouseType] = None
    """Warehouse type: `PRO` or `CLASSIC`. If you want to use serverless compute, you must set to `PRO`
    and also set the field `enable_serverless_compute` to `true`."""

    def as_dict(self) -> dict:
        """Serializes the GetWarehouseResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.auto_stop_mins is not None:
            body["auto_stop_mins"] = self.auto_stop_mins
        if self.channel:
            body["channel"] = self.channel.as_dict()
        if self.cluster_size is not None:
            body["cluster_size"] = self.cluster_size
        if self.creator_name is not None:
            body["creator_name"] = self.creator_name
        if self.enable_photon is not None:
            body["enable_photon"] = self.enable_photon
        if self.enable_serverless_compute is not None:
            body["enable_serverless_compute"] = self.enable_serverless_compute
        if self.health:
            body["health"] = self.health.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.instance_profile_arn is not None:
            body["instance_profile_arn"] = self.instance_profile_arn
        if self.jdbc_url is not None:
            body["jdbc_url"] = self.jdbc_url
        if self.max_num_clusters is not None:
            body["max_num_clusters"] = self.max_num_clusters
        if self.min_num_clusters is not None:
            body["min_num_clusters"] = self.min_num_clusters
        if self.name is not None:
            body["name"] = self.name
        if self.num_active_sessions is not None:
            body["num_active_sessions"] = self.num_active_sessions
        if self.num_clusters is not None:
            body["num_clusters"] = self.num_clusters
        if self.odbc_params:
            body["odbc_params"] = self.odbc_params.as_dict()
        if self.spot_instance_policy is not None:
            body["spot_instance_policy"] = self.spot_instance_policy.value
        if self.state is not None:
            body["state"] = self.state.value
        if self.tags:
            body["tags"] = self.tags.as_dict()
        if self.warehouse_type is not None:
            body["warehouse_type"] = self.warehouse_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetWarehouseResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.auto_stop_mins is not None:
            body["auto_stop_mins"] = self.auto_stop_mins
        if self.channel:
            body["channel"] = self.channel
        if self.cluster_size is not None:
            body["cluster_size"] = self.cluster_size
        if self.creator_name is not None:
            body["creator_name"] = self.creator_name
        if self.enable_photon is not None:
            body["enable_photon"] = self.enable_photon
        if self.enable_serverless_compute is not None:
            body["enable_serverless_compute"] = self.enable_serverless_compute
        if self.health:
            body["health"] = self.health
        if self.id is not None:
            body["id"] = self.id
        if self.instance_profile_arn is not None:
            body["instance_profile_arn"] = self.instance_profile_arn
        if self.jdbc_url is not None:
            body["jdbc_url"] = self.jdbc_url
        if self.max_num_clusters is not None:
            body["max_num_clusters"] = self.max_num_clusters
        if self.min_num_clusters is not None:
            body["min_num_clusters"] = self.min_num_clusters
        if self.name is not None:
            body["name"] = self.name
        if self.num_active_sessions is not None:
            body["num_active_sessions"] = self.num_active_sessions
        if self.num_clusters is not None:
            body["num_clusters"] = self.num_clusters
        if self.odbc_params:
            body["odbc_params"] = self.odbc_params
        if self.spot_instance_policy is not None:
            body["spot_instance_policy"] = self.spot_instance_policy
        if self.state is not None:
            body["state"] = self.state
        if self.tags:
            body["tags"] = self.tags
        if self.warehouse_type is not None:
            body["warehouse_type"] = self.warehouse_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetWarehouseResponse:
        """Deserializes the GetWarehouseResponse from a dictionary."""
        return cls(
            auto_stop_mins=d.get("auto_stop_mins", None),
            channel=_from_dict(d, "channel", Channel),
            cluster_size=d.get("cluster_size", None),
            creator_name=d.get("creator_name", None),
            enable_photon=d.get("enable_photon", None),
            enable_serverless_compute=d.get("enable_serverless_compute", None),
            health=_from_dict(d, "health", EndpointHealth),
            id=d.get("id", None),
            instance_profile_arn=d.get("instance_profile_arn", None),
            jdbc_url=d.get("jdbc_url", None),
            max_num_clusters=d.get("max_num_clusters", None),
            min_num_clusters=d.get("min_num_clusters", None),
            name=d.get("name", None),
            num_active_sessions=d.get("num_active_sessions", None),
            num_clusters=d.get("num_clusters", None),
            odbc_params=_from_dict(d, "odbc_params", OdbcParams),
            spot_instance_policy=_enum(d, "spot_instance_policy", SpotInstancePolicy),
            state=_enum(d, "state", State),
            tags=_from_dict(d, "tags", EndpointTags),
            warehouse_type=_enum(d, "warehouse_type", GetWarehouseResponseWarehouseType),
        )


class GetWarehouseResponseWarehouseType(Enum):

    CLASSIC = "CLASSIC"
    PRO = "PRO"
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"


@dataclass
class GetWorkspaceWarehouseConfigResponse:
    channel: Optional[Channel] = None
    """Optional: Channel selection details"""

    config_param: Optional[RepeatedEndpointConfPairs] = None
    """Deprecated: Use sql_configuration_parameters"""

    data_access_config: Optional[List[EndpointConfPair]] = None
    """Spark confs for external hive metastore configuration JSON serialized size must be less than <=
    512K"""

    enable_serverless_compute: Optional[bool] = None
    """Enable Serverless compute for SQL warehouses"""

    enabled_warehouse_types: Optional[List[WarehouseTypePair]] = None
    """List of Warehouse Types allowed in this workspace (limits allowed value of the type field in
    CreateWarehouse and EditWarehouse). Note: Some types cannot be disabled, they don't need to be
    specified in SetWorkspaceWarehouseConfig. Note: Disabling a type may cause existing warehouses
    to be converted to another type. Used by frontend to save specific type availability in the
    warehouse create and edit form UI."""

    global_param: Optional[RepeatedEndpointConfPairs] = None
    """Deprecated: Use sql_configuration_parameters"""

    google_service_account: Optional[str] = None
    """GCP only: Google Service Account used to pass to cluster to access Google Cloud Storage"""

    instance_profile_arn: Optional[str] = None
    """AWS Only: The instance profile used to pass an IAM role to the SQL warehouses. This
    configuration is also applied to the workspace's serverless compute for notebooks and jobs."""

    security_policy: Optional[GetWorkspaceWarehouseConfigResponseSecurityPolicy] = None
    """Security policy for warehouses"""

    sql_configuration_parameters: Optional[RepeatedEndpointConfPairs] = None
    """SQL configuration parameters"""

    def as_dict(self) -> dict:
        """Serializes the GetWorkspaceWarehouseConfigResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.channel:
            body["channel"] = self.channel.as_dict()
        if self.config_param:
            body["config_param"] = self.config_param.as_dict()
        if self.data_access_config:
            body["data_access_config"] = [v.as_dict() for v in self.data_access_config]
        if self.enable_serverless_compute is not None:
            body["enable_serverless_compute"] = self.enable_serverless_compute
        if self.enabled_warehouse_types:
            body["enabled_warehouse_types"] = [v.as_dict() for v in self.enabled_warehouse_types]
        if self.global_param:
            body["global_param"] = self.global_param.as_dict()
        if self.google_service_account is not None:
            body["google_service_account"] = self.google_service_account
        if self.instance_profile_arn is not None:
            body["instance_profile_arn"] = self.instance_profile_arn
        if self.security_policy is not None:
            body["security_policy"] = self.security_policy.value
        if self.sql_configuration_parameters:
            body["sql_configuration_parameters"] = self.sql_configuration_parameters.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetWorkspaceWarehouseConfigResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.channel:
            body["channel"] = self.channel
        if self.config_param:
            body["config_param"] = self.config_param
        if self.data_access_config:
            body["data_access_config"] = self.data_access_config
        if self.enable_serverless_compute is not None:
            body["enable_serverless_compute"] = self.enable_serverless_compute
        if self.enabled_warehouse_types:
            body["enabled_warehouse_types"] = self.enabled_warehouse_types
        if self.global_param:
            body["global_param"] = self.global_param
        if self.google_service_account is not None:
            body["google_service_account"] = self.google_service_account
        if self.instance_profile_arn is not None:
            body["instance_profile_arn"] = self.instance_profile_arn
        if self.security_policy is not None:
            body["security_policy"] = self.security_policy
        if self.sql_configuration_parameters:
            body["sql_configuration_parameters"] = self.sql_configuration_parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetWorkspaceWarehouseConfigResponse:
        """Deserializes the GetWorkspaceWarehouseConfigResponse from a dictionary."""
        return cls(
            channel=_from_dict(d, "channel", Channel),
            config_param=_from_dict(d, "config_param", RepeatedEndpointConfPairs),
            data_access_config=_repeated_dict(d, "data_access_config", EndpointConfPair),
            enable_serverless_compute=d.get("enable_serverless_compute", None),
            enabled_warehouse_types=_repeated_dict(d, "enabled_warehouse_types", WarehouseTypePair),
            global_param=_from_dict(d, "global_param", RepeatedEndpointConfPairs),
            google_service_account=d.get("google_service_account", None),
            instance_profile_arn=d.get("instance_profile_arn", None),
            security_policy=_enum(d, "security_policy", GetWorkspaceWarehouseConfigResponseSecurityPolicy),
            sql_configuration_parameters=_from_dict(d, "sql_configuration_parameters", RepeatedEndpointConfPairs),
        )


class GetWorkspaceWarehouseConfigResponseSecurityPolicy(Enum):
    """Security policy to be used for warehouses"""

    DATA_ACCESS_CONTROL = "DATA_ACCESS_CONTROL"
    NONE = "NONE"
    PASSTHROUGH = "PASSTHROUGH"


@dataclass
class LegacyAlert:
    created_at: Optional[str] = None
    """Timestamp when the alert was created."""

    id: Optional[str] = None
    """Alert ID."""

    last_triggered_at: Optional[str] = None
    """Timestamp when the alert was last triggered."""

    name: Optional[str] = None
    """Name of the alert."""

    options: Optional[AlertOptions] = None
    """Alert configuration options."""

    parent: Optional[str] = None
    """The identifier of the workspace folder containing the object."""

    query: Optional[AlertQuery] = None

    rearm: Optional[int] = None
    """Number of seconds after being triggered before the alert rearms itself and can be triggered
    again. If `null`, alert will never be triggered again."""

    state: Optional[LegacyAlertState] = None
    """State of the alert. Possible values are: `unknown` (yet to be evaluated), `triggered` (evaluated
    and fulfilled trigger conditions), or `ok` (evaluated and did not fulfill trigger conditions)."""

    updated_at: Optional[str] = None
    """Timestamp when the alert was last updated."""

    user: Optional[User] = None

    def as_dict(self) -> dict:
        """Serializes the LegacyAlert into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.id is not None:
            body["id"] = self.id
        if self.last_triggered_at is not None:
            body["last_triggered_at"] = self.last_triggered_at
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options.as_dict()
        if self.parent is not None:
            body["parent"] = self.parent
        if self.query:
            body["query"] = self.query.as_dict()
        if self.rearm is not None:
            body["rearm"] = self.rearm
        if self.state is not None:
            body["state"] = self.state.value
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user:
            body["user"] = self.user.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LegacyAlert into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.id is not None:
            body["id"] = self.id
        if self.last_triggered_at is not None:
            body["last_triggered_at"] = self.last_triggered_at
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.parent is not None:
            body["parent"] = self.parent
        if self.query:
            body["query"] = self.query
        if self.rearm is not None:
            body["rearm"] = self.rearm
        if self.state is not None:
            body["state"] = self.state
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user:
            body["user"] = self.user
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LegacyAlert:
        """Deserializes the LegacyAlert from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            id=d.get("id", None),
            last_triggered_at=d.get("last_triggered_at", None),
            name=d.get("name", None),
            options=_from_dict(d, "options", AlertOptions),
            parent=d.get("parent", None),
            query=_from_dict(d, "query", AlertQuery),
            rearm=d.get("rearm", None),
            state=_enum(d, "state", LegacyAlertState),
            updated_at=d.get("updated_at", None),
            user=_from_dict(d, "user", User),
        )


class LegacyAlertState(Enum):

    OK = "ok"
    TRIGGERED = "triggered"
    UNKNOWN = "unknown"


@dataclass
class LegacyQuery:
    can_edit: Optional[bool] = None
    """Describes whether the authenticated user is allowed to edit the definition of this query."""

    created_at: Optional[str] = None
    """The timestamp when this query was created."""

    data_source_id: Optional[str] = None
    """Data source ID maps to the ID of the data source used by the resource and is distinct from the
    warehouse ID. [Learn more]
    
    [Learn more]: https://docs.databricks.com/api/workspace/datasources/list"""

    description: Optional[str] = None
    """General description that conveys additional information about this query such as usage notes."""

    id: Optional[str] = None
    """Query ID."""

    is_archived: Optional[bool] = None
    """Indicates whether the query is trashed. Trashed queries can't be used in dashboards, or appear
    in search results. If this boolean is `true`, the `options` property for this query includes a
    `moved_to_trash_at` timestamp. Trashed queries are permanently deleted after 30 days."""

    is_draft: Optional[bool] = None
    """Whether the query is a draft. Draft queries only appear in list views for their owners.
    Visualizations from draft queries cannot appear on dashboards."""

    is_favorite: Optional[bool] = None
    """Whether this query object appears in the current user's favorites list. This flag determines
    whether the star icon for favorites is selected."""

    is_safe: Optional[bool] = None
    """Text parameter types are not safe from SQL injection for all types of data source. Set this
    Boolean parameter to `true` if a query either does not use any text type parameters or uses a
    data source type where text type parameters are handled safely."""

    last_modified_by: Optional[User] = None

    last_modified_by_id: Optional[int] = None
    """The ID of the user who last saved changes to this query."""

    latest_query_data_id: Optional[str] = None
    """If there is a cached result for this query and user, this field includes the query result ID. If
    this query uses parameters, this field is always null."""

    name: Optional[str] = None
    """The title of this query that appears in list views, widget headings, and on the query page."""

    options: Optional[QueryOptions] = None

    parent: Optional[str] = None
    """The identifier of the workspace folder containing the object."""

    permission_tier: Optional[PermissionLevel] = None
    """* `CAN_VIEW`: Can view the query * `CAN_RUN`: Can run the query * `CAN_EDIT`: Can edit the query
    * `CAN_MANAGE`: Can manage the query"""

    query: Optional[str] = None
    """The text of the query to be run."""

    query_hash: Optional[str] = None
    """A SHA-256 hash of the query text along with the authenticated user ID."""

    run_as_role: Optional[RunAsRole] = None
    """Sets the **Run as** role for the object. Must be set to one of `"viewer"` (signifying "run as
    viewer" behavior) or `"owner"` (signifying "run as owner" behavior)"""

    tags: Optional[List[str]] = None

    updated_at: Optional[str] = None
    """The timestamp at which this query was last updated."""

    user: Optional[User] = None

    user_id: Optional[int] = None
    """The ID of the user who owns the query."""

    visualizations: Optional[List[LegacyVisualization]] = None

    def as_dict(self) -> dict:
        """Serializes the LegacyQuery into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.can_edit is not None:
            body["can_edit"] = self.can_edit
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.data_source_id is not None:
            body["data_source_id"] = self.data_source_id
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.is_archived is not None:
            body["is_archived"] = self.is_archived
        if self.is_draft is not None:
            body["is_draft"] = self.is_draft
        if self.is_favorite is not None:
            body["is_favorite"] = self.is_favorite
        if self.is_safe is not None:
            body["is_safe"] = self.is_safe
        if self.last_modified_by:
            body["last_modified_by"] = self.last_modified_by.as_dict()
        if self.last_modified_by_id is not None:
            body["last_modified_by_id"] = self.last_modified_by_id
        if self.latest_query_data_id is not None:
            body["latest_query_data_id"] = self.latest_query_data_id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options.as_dict()
        if self.parent is not None:
            body["parent"] = self.parent
        if self.permission_tier is not None:
            body["permission_tier"] = self.permission_tier.value
        if self.query is not None:
            body["query"] = self.query
        if self.query_hash is not None:
            body["query_hash"] = self.query_hash
        if self.run_as_role is not None:
            body["run_as_role"] = self.run_as_role.value
        if self.tags:
            body["tags"] = [v for v in self.tags]
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user:
            body["user"] = self.user.as_dict()
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.visualizations:
            body["visualizations"] = [v.as_dict() for v in self.visualizations]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LegacyQuery into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.can_edit is not None:
            body["can_edit"] = self.can_edit
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.data_source_id is not None:
            body["data_source_id"] = self.data_source_id
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.is_archived is not None:
            body["is_archived"] = self.is_archived
        if self.is_draft is not None:
            body["is_draft"] = self.is_draft
        if self.is_favorite is not None:
            body["is_favorite"] = self.is_favorite
        if self.is_safe is not None:
            body["is_safe"] = self.is_safe
        if self.last_modified_by:
            body["last_modified_by"] = self.last_modified_by
        if self.last_modified_by_id is not None:
            body["last_modified_by_id"] = self.last_modified_by_id
        if self.latest_query_data_id is not None:
            body["latest_query_data_id"] = self.latest_query_data_id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.parent is not None:
            body["parent"] = self.parent
        if self.permission_tier is not None:
            body["permission_tier"] = self.permission_tier
        if self.query is not None:
            body["query"] = self.query
        if self.query_hash is not None:
            body["query_hash"] = self.query_hash
        if self.run_as_role is not None:
            body["run_as_role"] = self.run_as_role
        if self.tags:
            body["tags"] = self.tags
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.user:
            body["user"] = self.user
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.visualizations:
            body["visualizations"] = self.visualizations
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LegacyQuery:
        """Deserializes the LegacyQuery from a dictionary."""
        return cls(
            can_edit=d.get("can_edit", None),
            created_at=d.get("created_at", None),
            data_source_id=d.get("data_source_id", None),
            description=d.get("description", None),
            id=d.get("id", None),
            is_archived=d.get("is_archived", None),
            is_draft=d.get("is_draft", None),
            is_favorite=d.get("is_favorite", None),
            is_safe=d.get("is_safe", None),
            last_modified_by=_from_dict(d, "last_modified_by", User),
            last_modified_by_id=d.get("last_modified_by_id", None),
            latest_query_data_id=d.get("latest_query_data_id", None),
            name=d.get("name", None),
            options=_from_dict(d, "options", QueryOptions),
            parent=d.get("parent", None),
            permission_tier=_enum(d, "permission_tier", PermissionLevel),
            query=d.get("query", None),
            query_hash=d.get("query_hash", None),
            run_as_role=_enum(d, "run_as_role", RunAsRole),
            tags=d.get("tags", None),
            updated_at=d.get("updated_at", None),
            user=_from_dict(d, "user", User),
            user_id=d.get("user_id", None),
            visualizations=_repeated_dict(d, "visualizations", LegacyVisualization),
        )


@dataclass
class LegacyVisualization:
    """The visualization description API changes frequently and is unsupported. You can duplicate a
    visualization by copying description objects received _from the API_ and then using them to
    create a new one with a POST request to the same endpoint. Databricks does not recommend
    constructing ad-hoc visualizations entirely in JSON."""

    created_at: Optional[str] = None

    description: Optional[str] = None
    """A short description of this visualization. This is not displayed in the UI."""

    id: Optional[str] = None
    """The UUID for this visualization."""

    name: Optional[str] = None
    """The name of the visualization that appears on dashboards and the query screen."""

    options: Optional[Any] = None
    """The options object varies widely from one visualization type to the next and is unsupported.
    Databricks does not recommend modifying visualization settings in JSON."""

    query: Optional[LegacyQuery] = None

    type: Optional[str] = None
    """The type of visualization: chart, table, pivot table, and so on."""

    updated_at: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the LegacyVisualization into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.query:
            body["query"] = self.query.as_dict()
        if self.type is not None:
            body["type"] = self.type
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LegacyVisualization into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.options:
            body["options"] = self.options
        if self.query:
            body["query"] = self.query
        if self.type is not None:
            body["type"] = self.type
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LegacyVisualization:
        """Deserializes the LegacyVisualization from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            description=d.get("description", None),
            id=d.get("id", None),
            name=d.get("name", None),
            options=d.get("options", None),
            query=_from_dict(d, "query", LegacyQuery),
            type=d.get("type", None),
            updated_at=d.get("updated_at", None),
        )


class LifecycleState(Enum):

    ACTIVE = "ACTIVE"
    TRASHED = "TRASHED"


@dataclass
class ListAlertsResponse:
    next_page_token: Optional[str] = None

    results: Optional[List[ListAlertsResponseAlert]] = None

    def as_dict(self) -> dict:
        """Serializes the ListAlertsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAlertsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAlertsResponse:
        """Deserializes the ListAlertsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            results=_repeated_dict(d, "results", ListAlertsResponseAlert),
        )


@dataclass
class ListAlertsResponseAlert:
    condition: Optional[AlertCondition] = None
    """Trigger conditions of the alert."""

    create_time: Optional[str] = None
    """The timestamp indicating when the alert was created."""

    custom_body: Optional[str] = None
    """Custom body of alert notification, if it exists. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    custom_subject: Optional[str] = None
    """Custom subject of alert notification, if it exists. This can include email subject entries and
    Slack notification headers, for example. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    display_name: Optional[str] = None
    """The display name of the alert."""

    id: Optional[str] = None
    """UUID identifying the alert."""

    lifecycle_state: Optional[LifecycleState] = None
    """The workspace state of the alert. Used for tracking trashed status."""

    notify_on_ok: Optional[bool] = None
    """Whether to notify alert subscribers when alert returns back to normal."""

    owner_user_name: Optional[str] = None
    """The owner's username. This field is set to "Unavailable" if the user has been deleted."""

    query_id: Optional[str] = None
    """UUID of the query attached to the alert."""

    seconds_to_retrigger: Optional[int] = None
    """Number of seconds an alert must wait after being triggered to rearm itself. After rearming, it
    can be triggered again. If 0 or not specified, the alert will not be triggered again."""

    state: Optional[AlertState] = None
    """Current state of the alert's trigger status. This field is set to UNKNOWN if the alert has not
    yet been evaluated or ran into an error during the last evaluation."""

    trigger_time: Optional[str] = None
    """Timestamp when the alert was last triggered, if the alert has been triggered before."""

    update_time: Optional[str] = None
    """The timestamp indicating when the alert was updated."""

    def as_dict(self) -> dict:
        """Serializes the ListAlertsResponseAlert into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.condition:
            body["condition"] = self.condition.as_dict()
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state.value
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        if self.state is not None:
            body["state"] = self.state.value
        if self.trigger_time is not None:
            body["trigger_time"] = self.trigger_time
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAlertsResponseAlert into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.condition:
            body["condition"] = self.condition
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        if self.state is not None:
            body["state"] = self.state
        if self.trigger_time is not None:
            body["trigger_time"] = self.trigger_time
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAlertsResponseAlert:
        """Deserializes the ListAlertsResponseAlert from a dictionary."""
        return cls(
            condition=_from_dict(d, "condition", AlertCondition),
            create_time=d.get("create_time", None),
            custom_body=d.get("custom_body", None),
            custom_subject=d.get("custom_subject", None),
            display_name=d.get("display_name", None),
            id=d.get("id", None),
            lifecycle_state=_enum(d, "lifecycle_state", LifecycleState),
            notify_on_ok=d.get("notify_on_ok", None),
            owner_user_name=d.get("owner_user_name", None),
            query_id=d.get("query_id", None),
            seconds_to_retrigger=d.get("seconds_to_retrigger", None),
            state=_enum(d, "state", AlertState),
            trigger_time=d.get("trigger_time", None),
            update_time=d.get("update_time", None),
        )


@dataclass
class ListAlertsV2Response:
    alerts: Optional[List[AlertV2]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListAlertsV2Response into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alerts:
            body["alerts"] = [v.as_dict() for v in self.alerts]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAlertsV2Response into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alerts:
            body["alerts"] = self.alerts
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAlertsV2Response:
        """Deserializes the ListAlertsV2Response from a dictionary."""
        return cls(alerts=_repeated_dict(d, "alerts", AlertV2), next_page_token=d.get("next_page_token", None))


@dataclass
class ListDefaultWarehouseOverridesResponse:
    """Response message for ListDefaultWarehouseOverrides."""

    default_warehouse_overrides: Optional[List[DefaultWarehouseOverride]] = None
    """The default warehouse overrides in the workspace."""

    next_page_token: Optional[str] = None
    """A token, which can be sent as `page_token` to retrieve the next page. If this field is omitted,
    there are no subsequent pages."""

    def as_dict(self) -> dict:
        """Serializes the ListDefaultWarehouseOverridesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default_warehouse_overrides:
            body["default_warehouse_overrides"] = [v.as_dict() for v in self.default_warehouse_overrides]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListDefaultWarehouseOverridesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default_warehouse_overrides:
            body["default_warehouse_overrides"] = self.default_warehouse_overrides
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListDefaultWarehouseOverridesResponse:
        """Deserializes the ListDefaultWarehouseOverridesResponse from a dictionary."""
        return cls(
            default_warehouse_overrides=_repeated_dict(d, "default_warehouse_overrides", DefaultWarehouseOverride),
            next_page_token=d.get("next_page_token", None),
        )


class ListOrder(Enum):

    CREATED_AT = "created_at"
    NAME = "name"


@dataclass
class ListQueriesResponse:
    has_next_page: Optional[bool] = None
    """Whether there is another page of results."""

    next_page_token: Optional[str] = None
    """A token that can be used to get the next page of results."""

    res: Optional[List[QueryInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ListQueriesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.has_next_page is not None:
            body["has_next_page"] = self.has_next_page
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.res:
            body["res"] = [v.as_dict() for v in self.res]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListQueriesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.has_next_page is not None:
            body["has_next_page"] = self.has_next_page
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.res:
            body["res"] = self.res
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListQueriesResponse:
        """Deserializes the ListQueriesResponse from a dictionary."""
        return cls(
            has_next_page=d.get("has_next_page", None),
            next_page_token=d.get("next_page_token", None),
            res=_repeated_dict(d, "res", QueryInfo),
        )


@dataclass
class ListQueryObjectsResponse:
    next_page_token: Optional[str] = None

    results: Optional[List[ListQueryObjectsResponseQuery]] = None

    def as_dict(self) -> dict:
        """Serializes the ListQueryObjectsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListQueryObjectsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListQueryObjectsResponse:
        """Deserializes the ListQueryObjectsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            results=_repeated_dict(d, "results", ListQueryObjectsResponseQuery),
        )


@dataclass
class ListQueryObjectsResponseQuery:
    apply_auto_limit: Optional[bool] = None
    """Whether to apply a 1000 row limit to the query result."""

    catalog: Optional[str] = None
    """Name of the catalog where this query will be executed."""

    create_time: Optional[str] = None
    """Timestamp when this query was created."""

    description: Optional[str] = None
    """General description that conveys additional information about this query such as usage notes."""

    display_name: Optional[str] = None
    """Display name of the query that appears in list views, widget headings, and on the query page."""

    id: Optional[str] = None
    """UUID identifying the query."""

    last_modifier_user_name: Optional[str] = None
    """Username of the user who last saved changes to this query."""

    lifecycle_state: Optional[LifecycleState] = None
    """Indicates whether the query is trashed."""

    owner_user_name: Optional[str] = None
    """Username of the user that owns the query."""

    parameters: Optional[List[QueryParameter]] = None
    """List of query parameter definitions."""

    query_text: Optional[str] = None
    """Text of the query to be run."""

    run_as_mode: Optional[RunAsMode] = None
    """Sets the "Run as" role for the object."""

    schema: Optional[str] = None
    """Name of the schema where this query will be executed."""

    tags: Optional[List[str]] = None

    update_time: Optional[str] = None
    """Timestamp when this query was last updated."""

    warehouse_id: Optional[str] = None
    """ID of the SQL warehouse attached to the query."""

    def as_dict(self) -> dict:
        """Serializes the ListQueryObjectsResponseQuery into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.last_modifier_user_name is not None:
            body["last_modifier_user_name"] = self.last_modifier_user_name
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state.value
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode.value
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = [v for v in self.tags]
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListQueryObjectsResponseQuery into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.last_modifier_user_name is not None:
            body["last_modifier_user_name"] = self.last_modifier_user_name
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parameters:
            body["parameters"] = self.parameters
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = self.tags
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListQueryObjectsResponseQuery:
        """Deserializes the ListQueryObjectsResponseQuery from a dictionary."""
        return cls(
            apply_auto_limit=d.get("apply_auto_limit", None),
            catalog=d.get("catalog", None),
            create_time=d.get("create_time", None),
            description=d.get("description", None),
            display_name=d.get("display_name", None),
            id=d.get("id", None),
            last_modifier_user_name=d.get("last_modifier_user_name", None),
            lifecycle_state=_enum(d, "lifecycle_state", LifecycleState),
            owner_user_name=d.get("owner_user_name", None),
            parameters=_repeated_dict(d, "parameters", QueryParameter),
            query_text=d.get("query_text", None),
            run_as_mode=_enum(d, "run_as_mode", RunAsMode),
            schema=d.get("schema", None),
            tags=d.get("tags", None),
            update_time=d.get("update_time", None),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class ListResponse:
    count: Optional[int] = None
    """The total number of dashboards."""

    page: Optional[int] = None
    """The current page being displayed."""

    page_size: Optional[int] = None
    """The number of dashboards per page."""

    results: Optional[List[Dashboard]] = None
    """List of dashboards returned."""

    def as_dict(self) -> dict:
        """Serializes the ListResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.count is not None:
            body["count"] = self.count
        if self.page is not None:
            body["page"] = self.page
        if self.page_size is not None:
            body["page_size"] = self.page_size
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.count is not None:
            body["count"] = self.count
        if self.page is not None:
            body["page"] = self.page
        if self.page_size is not None:
            body["page_size"] = self.page_size
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListResponse:
        """Deserializes the ListResponse from a dictionary."""
        return cls(
            count=d.get("count", None),
            page=d.get("page", None),
            page_size=d.get("page_size", None),
            results=_repeated_dict(d, "results", Dashboard),
        )


@dataclass
class ListVisualizationsForQueryResponse:
    next_page_token: Optional[str] = None

    results: Optional[List[Visualization]] = None

    def as_dict(self) -> dict:
        """Serializes the ListVisualizationsForQueryResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListVisualizationsForQueryResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListVisualizationsForQueryResponse:
        """Deserializes the ListVisualizationsForQueryResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), results=_repeated_dict(d, "results", Visualization))


@dataclass
class ListWarehousesResponse:
    next_page_token: Optional[str] = None
    """A token, which can be sent as `page_token` to retrieve the next page. If this field is omitted,
    there are no subsequent pages."""

    warehouses: Optional[List[EndpointInfo]] = None
    """A list of warehouses and their configurations."""

    def as_dict(self) -> dict:
        """Serializes the ListWarehousesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.warehouses:
            body["warehouses"] = [v.as_dict() for v in self.warehouses]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListWarehousesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.warehouses:
            body["warehouses"] = self.warehouses
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListWarehousesResponse:
        """Deserializes the ListWarehousesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), warehouses=_repeated_dict(d, "warehouses", EndpointInfo)
        )


@dataclass
class MultiValuesOptions:
    prefix: Optional[str] = None
    """Character that prefixes each selected parameter value."""

    separator: Optional[str] = None
    """Character that separates each selected parameter value. Defaults to a comma."""

    suffix: Optional[str] = None
    """Character that suffixes each selected parameter value."""

    def as_dict(self) -> dict:
        """Serializes the MultiValuesOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.prefix is not None:
            body["prefix"] = self.prefix
        if self.separator is not None:
            body["separator"] = self.separator
        if self.suffix is not None:
            body["suffix"] = self.suffix
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MultiValuesOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.prefix is not None:
            body["prefix"] = self.prefix
        if self.separator is not None:
            body["separator"] = self.separator
        if self.suffix is not None:
            body["suffix"] = self.suffix
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MultiValuesOptions:
        """Deserializes the MultiValuesOptions from a dictionary."""
        return cls(prefix=d.get("prefix", None), separator=d.get("separator", None), suffix=d.get("suffix", None))


@dataclass
class NumericValue:
    value: Optional[float] = None

    def as_dict(self) -> dict:
        """Serializes the NumericValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NumericValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NumericValue:
        """Deserializes the NumericValue from a dictionary."""
        return cls(value=d.get("value", None))


class ObjectType(Enum):
    """A singular noun object type."""

    ALERT = "alert"
    DASHBOARD = "dashboard"
    DATA_SOURCE = "data_source"
    QUERY = "query"


class ObjectTypePlural(Enum):
    """Always a plural of the object type."""

    ALERTS = "alerts"
    DASHBOARDS = "dashboards"
    DATA_SOURCES = "data_sources"
    QUERIES = "queries"


@dataclass
class OdbcParams:
    hostname: Optional[str] = None

    path: Optional[str] = None

    port: Optional[int] = None

    protocol: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the OdbcParams into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.hostname is not None:
            body["hostname"] = self.hostname
        if self.path is not None:
            body["path"] = self.path
        if self.port is not None:
            body["port"] = self.port
        if self.protocol is not None:
            body["protocol"] = self.protocol
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OdbcParams into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.hostname is not None:
            body["hostname"] = self.hostname
        if self.path is not None:
            body["path"] = self.path
        if self.port is not None:
            body["port"] = self.port
        if self.protocol is not None:
            body["protocol"] = self.protocol
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OdbcParams:
        """Deserializes the OdbcParams from a dictionary."""
        return cls(
            hostname=d.get("hostname", None),
            path=d.get("path", None),
            port=d.get("port", None),
            protocol=d.get("protocol", None),
        )


class OwnableObjectType(Enum):

    ALERT = "alert"
    DASHBOARD = "dashboard"
    QUERY = "query"


@dataclass
class Parameter:
    enum_options: Optional[str] = None
    """List of valid parameter values, newline delimited. Only applies for dropdown list parameters."""

    multi_values_options: Optional[MultiValuesOptions] = None
    """If specified, allows multiple values to be selected for this parameter. Only applies to dropdown
    list and query-based dropdown list parameters."""

    name: Optional[str] = None
    """The literal parameter marker that appears between double curly braces in the query text."""

    query_id: Optional[str] = None
    """The UUID of the query that provides the parameter values. Only applies for query-based dropdown
    list parameters."""

    title: Optional[str] = None
    """The text displayed in a parameter picking widget."""

    type: Optional[ParameterType] = None
    """Parameters can have several different types."""

    value: Optional[Any] = None
    """The default value for this parameter."""

    def as_dict(self) -> dict:
        """Serializes the Parameter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enum_options is not None:
            body["enumOptions"] = self.enum_options
        if self.multi_values_options:
            body["multiValuesOptions"] = self.multi_values_options.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.query_id is not None:
            body["queryId"] = self.query_id
        if self.title is not None:
            body["title"] = self.title
        if self.type is not None:
            body["type"] = self.type.value
        if self.value:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Parameter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enum_options is not None:
            body["enumOptions"] = self.enum_options
        if self.multi_values_options:
            body["multiValuesOptions"] = self.multi_values_options
        if self.name is not None:
            body["name"] = self.name
        if self.query_id is not None:
            body["queryId"] = self.query_id
        if self.title is not None:
            body["title"] = self.title
        if self.type is not None:
            body["type"] = self.type
        if self.value:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Parameter:
        """Deserializes the Parameter from a dictionary."""
        return cls(
            enum_options=d.get("enumOptions", None),
            multi_values_options=_from_dict(d, "multiValuesOptions", MultiValuesOptions),
            name=d.get("name", None),
            query_id=d.get("queryId", None),
            title=d.get("title", None),
            type=_enum(d, "type", ParameterType),
            value=d.get("value", None),
        )


class ParameterType(Enum):

    DATETIME = "datetime"
    ENUM = "enum"
    NUMBER = "number"
    QUERY = "query"
    TEXT = "text"


class PermissionLevel(Enum):
    """* `CAN_VIEW`: Can view the query * `CAN_RUN`: Can run the query * `CAN_EDIT`: Can edit the query
    * `CAN_MANAGE`: Can manage the query"""

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_RUN = "CAN_RUN"
    CAN_VIEW = "CAN_VIEW"


class PlansState(Enum):
    """Possible Reasons for which we have not saved plans in the database"""

    EMPTY = "EMPTY"
    EXISTS = "EXISTS"
    IGNORED_LARGE_PLANS_SIZE = "IGNORED_LARGE_PLANS_SIZE"
    IGNORED_SMALL_DURATION = "IGNORED_SMALL_DURATION"
    IGNORED_SPARK_PLAN_TYPE = "IGNORED_SPARK_PLAN_TYPE"
    UNKNOWN = "UNKNOWN"


@dataclass
class Query:
    apply_auto_limit: Optional[bool] = None
    """Whether to apply a 1000 row limit to the query result."""

    catalog: Optional[str] = None
    """Name of the catalog where this query will be executed."""

    create_time: Optional[str] = None
    """Timestamp when this query was created."""

    description: Optional[str] = None
    """General description that conveys additional information about this query such as usage notes."""

    display_name: Optional[str] = None
    """Display name of the query that appears in list views, widget headings, and on the query page."""

    id: Optional[str] = None
    """UUID identifying the query."""

    last_modifier_user_name: Optional[str] = None
    """Username of the user who last saved changes to this query."""

    lifecycle_state: Optional[LifecycleState] = None
    """Indicates whether the query is trashed."""

    owner_user_name: Optional[str] = None
    """Username of the user that owns the query."""

    parameters: Optional[List[QueryParameter]] = None
    """List of query parameter definitions."""

    parent_path: Optional[str] = None
    """Workspace path of the workspace folder containing the object."""

    query_text: Optional[str] = None
    """Text of the query to be run."""

    run_as_mode: Optional[RunAsMode] = None
    """Sets the "Run as" role for the object."""

    schema: Optional[str] = None
    """Name of the schema where this query will be executed."""

    tags: Optional[List[str]] = None

    update_time: Optional[str] = None
    """Timestamp when this query was last updated."""

    warehouse_id: Optional[str] = None
    """ID of the SQL warehouse attached to the query."""

    def as_dict(self) -> dict:
        """Serializes the Query into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.last_modifier_user_name is not None:
            body["last_modifier_user_name"] = self.last_modifier_user_name
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state.value
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode.value
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = [v for v in self.tags]
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Query into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.last_modifier_user_name is not None:
            body["last_modifier_user_name"] = self.last_modifier_user_name
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parameters:
            body["parameters"] = self.parameters
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = self.tags
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Query:
        """Deserializes the Query from a dictionary."""
        return cls(
            apply_auto_limit=d.get("apply_auto_limit", None),
            catalog=d.get("catalog", None),
            create_time=d.get("create_time", None),
            description=d.get("description", None),
            display_name=d.get("display_name", None),
            id=d.get("id", None),
            last_modifier_user_name=d.get("last_modifier_user_name", None),
            lifecycle_state=_enum(d, "lifecycle_state", LifecycleState),
            owner_user_name=d.get("owner_user_name", None),
            parameters=_repeated_dict(d, "parameters", QueryParameter),
            parent_path=d.get("parent_path", None),
            query_text=d.get("query_text", None),
            run_as_mode=_enum(d, "run_as_mode", RunAsMode),
            schema=d.get("schema", None),
            tags=d.get("tags", None),
            update_time=d.get("update_time", None),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class QueryBackedValue:
    multi_values_options: Optional[MultiValuesOptions] = None
    """If specified, allows multiple values to be selected for this parameter."""

    query_id: Optional[str] = None
    """UUID of the query that provides the parameter values."""

    values: Optional[List[str]] = None
    """List of selected query parameter values."""

    def as_dict(self) -> dict:
        """Serializes the QueryBackedValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.multi_values_options:
            body["multi_values_options"] = self.multi_values_options.as_dict()
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.values:
            body["values"] = [v for v in self.values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryBackedValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.multi_values_options:
            body["multi_values_options"] = self.multi_values_options
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.values:
            body["values"] = self.values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryBackedValue:
        """Deserializes the QueryBackedValue from a dictionary."""
        return cls(
            multi_values_options=_from_dict(d, "multi_values_options", MultiValuesOptions),
            query_id=d.get("query_id", None),
            values=d.get("values", None),
        )


@dataclass
class QueryFilter:
    query_start_time_range: Optional[TimeRange] = None
    """A range filter for query submitted time. The time range must be less than or equal to 30 days."""

    statement_ids: Optional[List[str]] = None
    """A list of statement IDs."""

    statuses: Optional[List[QueryStatus]] = None
    """A list of statuses (QUEUED, RUNNING, CANCELED, FAILED, FINISHED) to match query results.
    Corresponds to the `status` field in the response. Filtering for multiple statuses is not
    recommended. Instead, opt to filter by a single status multiple times and then combine the
    results."""

    user_ids: Optional[List[int]] = None
    """A list of user IDs who ran the queries."""

    warehouse_ids: Optional[List[str]] = None
    """A list of warehouse IDs."""

    def as_dict(self) -> dict:
        """Serializes the QueryFilter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.query_start_time_range:
            body["query_start_time_range"] = self.query_start_time_range.as_dict()
        if self.statement_ids:
            body["statement_ids"] = [v for v in self.statement_ids]
        if self.statuses:
            body["statuses"] = [v.value for v in self.statuses]
        if self.user_ids:
            body["user_ids"] = [v for v in self.user_ids]
        if self.warehouse_ids:
            body["warehouse_ids"] = [v for v in self.warehouse_ids]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryFilter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.query_start_time_range:
            body["query_start_time_range"] = self.query_start_time_range
        if self.statement_ids:
            body["statement_ids"] = self.statement_ids
        if self.statuses:
            body["statuses"] = self.statuses
        if self.user_ids:
            body["user_ids"] = self.user_ids
        if self.warehouse_ids:
            body["warehouse_ids"] = self.warehouse_ids
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryFilter:
        """Deserializes the QueryFilter from a dictionary."""
        return cls(
            query_start_time_range=_from_dict(d, "query_start_time_range", TimeRange),
            statement_ids=d.get("statement_ids", None),
            statuses=_repeated_enum(d, "statuses", QueryStatus),
            user_ids=d.get("user_ids", None),
            warehouse_ids=d.get("warehouse_ids", None),
        )


@dataclass
class QueryInfo:
    cache_query_id: Optional[str] = None
    """The ID of the cached query if this result retrieved from cache"""

    channel_used: Optional[ChannelInfo] = None
    """SQL Warehouse channel information at the time of query execution"""

    client_application: Optional[str] = None
    """Client application that ran the statement. For example: Databricks SQL Editor, Tableau, and
    Power BI. This field is derived from information provided by client applications. While values
    are expected to remain static over time, this cannot be guaranteed."""

    duration: Optional[int] = None
    """Total time of the statement execution. This value does not include the time taken to retrieve
    the results, which can result in a discrepancy between this value and the start-to-finish
    wall-clock time."""

    endpoint_id: Optional[str] = None
    """Alias for `warehouse_id`."""

    error_message: Optional[str] = None
    """Message describing why the query could not complete."""

    executed_as_user_id: Optional[int] = None
    """The ID of the user whose credentials were used to run the query."""

    executed_as_user_name: Optional[str] = None
    """The email address or username of the user whose credentials were used to run the query."""

    execution_end_time_ms: Optional[int] = None
    """The time execution of the query ended."""

    is_final: Optional[bool] = None
    """Whether more updates for the query are expected."""

    lookup_key: Optional[str] = None
    """A key that can be used to look up query details."""

    metrics: Optional[QueryMetrics] = None
    """Metrics about query execution."""

    plans_state: Optional[PlansState] = None
    """Whether plans exist for the execution, or the reason why they are missing"""

    query_end_time_ms: Optional[int] = None
    """The time the query ended."""

    query_id: Optional[str] = None
    """The query ID."""

    query_source: Optional[ExternalQuerySource] = None
    """A struct that contains key-value pairs representing Databricks entities that were involved in
    the execution of this statement, such as jobs, notebooks, or dashboards. This field only records
    Databricks entities."""

    query_start_time_ms: Optional[int] = None
    """The time the query started."""

    query_tags: Optional[List[QueryTag]] = None
    """A query execution can be optionally annotated with query tags"""

    query_text: Optional[str] = None
    """The text of the query."""

    rows_produced: Optional[int] = None
    """The number of results returned by the query."""

    session_id: Optional[str] = None
    """The spark session UUID that query ran on. This is either the Spark Connect, DBSQL, or SDP
    session ID."""

    spark_ui_url: Optional[str] = None
    """URL to the Spark UI query plan."""

    statement_type: Optional[QueryStatementType] = None
    """Type of statement for this query"""

    status: Optional[QueryStatus] = None
    """Query status with one the following values:
    
    - `QUEUED`: Query has been received and queued. - `RUNNING`: Query has started. - `CANCELED`:
    Query has been cancelled by the user. - `FAILED`: Query has failed. - `FINISHED`: Query has
    completed."""

    user_id: Optional[int] = None
    """The ID of the user who ran the query."""

    user_name: Optional[str] = None
    """The email address or username of the user who ran the query."""

    warehouse_id: Optional[str] = None
    """Warehouse ID."""

    def as_dict(self) -> dict:
        """Serializes the QueryInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cache_query_id is not None:
            body["cache_query_id"] = self.cache_query_id
        if self.channel_used:
            body["channel_used"] = self.channel_used.as_dict()
        if self.client_application is not None:
            body["client_application"] = self.client_application
        if self.duration is not None:
            body["duration"] = self.duration
        if self.endpoint_id is not None:
            body["endpoint_id"] = self.endpoint_id
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.executed_as_user_id is not None:
            body["executed_as_user_id"] = self.executed_as_user_id
        if self.executed_as_user_name is not None:
            body["executed_as_user_name"] = self.executed_as_user_name
        if self.execution_end_time_ms is not None:
            body["execution_end_time_ms"] = self.execution_end_time_ms
        if self.is_final is not None:
            body["is_final"] = self.is_final
        if self.lookup_key is not None:
            body["lookup_key"] = self.lookup_key
        if self.metrics:
            body["metrics"] = self.metrics.as_dict()
        if self.plans_state is not None:
            body["plans_state"] = self.plans_state.value
        if self.query_end_time_ms is not None:
            body["query_end_time_ms"] = self.query_end_time_ms
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.query_source:
            body["query_source"] = self.query_source.as_dict()
        if self.query_start_time_ms is not None:
            body["query_start_time_ms"] = self.query_start_time_ms
        if self.query_tags:
            body["query_tags"] = [v.as_dict() for v in self.query_tags]
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.rows_produced is not None:
            body["rows_produced"] = self.rows_produced
        if self.session_id is not None:
            body["session_id"] = self.session_id
        if self.spark_ui_url is not None:
            body["spark_ui_url"] = self.spark_ui_url
        if self.statement_type is not None:
            body["statement_type"] = self.statement_type.value
        if self.status is not None:
            body["status"] = self.status.value
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.user_name is not None:
            body["user_name"] = self.user_name
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cache_query_id is not None:
            body["cache_query_id"] = self.cache_query_id
        if self.channel_used:
            body["channel_used"] = self.channel_used
        if self.client_application is not None:
            body["client_application"] = self.client_application
        if self.duration is not None:
            body["duration"] = self.duration
        if self.endpoint_id is not None:
            body["endpoint_id"] = self.endpoint_id
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.executed_as_user_id is not None:
            body["executed_as_user_id"] = self.executed_as_user_id
        if self.executed_as_user_name is not None:
            body["executed_as_user_name"] = self.executed_as_user_name
        if self.execution_end_time_ms is not None:
            body["execution_end_time_ms"] = self.execution_end_time_ms
        if self.is_final is not None:
            body["is_final"] = self.is_final
        if self.lookup_key is not None:
            body["lookup_key"] = self.lookup_key
        if self.metrics:
            body["metrics"] = self.metrics
        if self.plans_state is not None:
            body["plans_state"] = self.plans_state
        if self.query_end_time_ms is not None:
            body["query_end_time_ms"] = self.query_end_time_ms
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.query_source:
            body["query_source"] = self.query_source
        if self.query_start_time_ms is not None:
            body["query_start_time_ms"] = self.query_start_time_ms
        if self.query_tags:
            body["query_tags"] = self.query_tags
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.rows_produced is not None:
            body["rows_produced"] = self.rows_produced
        if self.session_id is not None:
            body["session_id"] = self.session_id
        if self.spark_ui_url is not None:
            body["spark_ui_url"] = self.spark_ui_url
        if self.statement_type is not None:
            body["statement_type"] = self.statement_type
        if self.status is not None:
            body["status"] = self.status
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.user_name is not None:
            body["user_name"] = self.user_name
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryInfo:
        """Deserializes the QueryInfo from a dictionary."""
        return cls(
            cache_query_id=d.get("cache_query_id", None),
            channel_used=_from_dict(d, "channel_used", ChannelInfo),
            client_application=d.get("client_application", None),
            duration=d.get("duration", None),
            endpoint_id=d.get("endpoint_id", None),
            error_message=d.get("error_message", None),
            executed_as_user_id=d.get("executed_as_user_id", None),
            executed_as_user_name=d.get("executed_as_user_name", None),
            execution_end_time_ms=d.get("execution_end_time_ms", None),
            is_final=d.get("is_final", None),
            lookup_key=d.get("lookup_key", None),
            metrics=_from_dict(d, "metrics", QueryMetrics),
            plans_state=_enum(d, "plans_state", PlansState),
            query_end_time_ms=d.get("query_end_time_ms", None),
            query_id=d.get("query_id", None),
            query_source=_from_dict(d, "query_source", ExternalQuerySource),
            query_start_time_ms=d.get("query_start_time_ms", None),
            query_tags=_repeated_dict(d, "query_tags", QueryTag),
            query_text=d.get("query_text", None),
            rows_produced=d.get("rows_produced", None),
            session_id=d.get("session_id", None),
            spark_ui_url=d.get("spark_ui_url", None),
            statement_type=_enum(d, "statement_type", QueryStatementType),
            status=_enum(d, "status", QueryStatus),
            user_id=d.get("user_id", None),
            user_name=d.get("user_name", None),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class QueryList:
    count: Optional[int] = None
    """The total number of queries."""

    page: Optional[int] = None
    """The page number that is currently displayed."""

    page_size: Optional[int] = None
    """The number of queries per page."""

    results: Optional[List[LegacyQuery]] = None
    """List of queries returned."""

    def as_dict(self) -> dict:
        """Serializes the QueryList into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.count is not None:
            body["count"] = self.count
        if self.page is not None:
            body["page"] = self.page
        if self.page_size is not None:
            body["page_size"] = self.page_size
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryList into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.count is not None:
            body["count"] = self.count
        if self.page is not None:
            body["page"] = self.page
        if self.page_size is not None:
            body["page_size"] = self.page_size
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryList:
        """Deserializes the QueryList from a dictionary."""
        return cls(
            count=d.get("count", None),
            page=d.get("page", None),
            page_size=d.get("page_size", None),
            results=_repeated_dict(d, "results", LegacyQuery),
        )


@dataclass
class QueryMetrics:
    """A query metric that encapsulates a set of measurements for a single query. Metrics come from the
    driver and are stored in the history service database."""

    compilation_time_ms: Optional[int] = None
    """Time spent loading metadata and optimizing the query, in milliseconds."""

    execution_time_ms: Optional[int] = None
    """Time spent executing the query, in milliseconds."""

    network_sent_bytes: Optional[int] = None
    """Total amount of data sent over the network between executor nodes during shuffle, in bytes."""

    overloading_queue_start_timestamp: Optional[int] = None
    """Timestamp of when the query was enqueued waiting while the warehouse was at max load. This field
    is optional and will not appear if the query skipped the overloading queue."""

    photon_total_time_ms: Optional[int] = None
    """Total execution time for all individual Photon query engine tasks in the query, in milliseconds."""

    projected_remaining_task_total_time_ms: Optional[int] = None
    """projected remaining work to be done aggregated across all stages in the query, in milliseconds"""

    projected_remaining_wallclock_time_ms: Optional[int] = None
    """projected lower bound on remaining total task time based on
    projected_remaining_task_total_time_ms / maximum concurrency"""

    provisioning_queue_start_timestamp: Optional[int] = None
    """Timestamp of when the query was enqueued waiting for a cluster to be provisioned for the
    warehouse. This field is optional and will not appear if the query skipped the provisioning
    queue."""

    pruned_bytes: Optional[int] = None
    """Total number of file bytes in all tables not read due to pruning"""

    pruned_files_count: Optional[int] = None
    """Total number of files from all tables not read due to pruning"""

    query_compilation_start_timestamp: Optional[int] = None
    """Timestamp of when the underlying compute started compilation of the query."""

    read_bytes: Optional[int] = None
    """Total size of data read by the query, in bytes."""

    read_cache_bytes: Optional[int] = None
    """Size of persistent data read from the cache, in bytes."""

    read_files_bytes: Optional[int] = None
    """Total number of file bytes in all tables read"""

    read_files_count: Optional[int] = None
    """Number of files read after pruning"""

    read_partitions_count: Optional[int] = None
    """Number of partitions read after pruning."""

    read_remote_bytes: Optional[int] = None
    """Size of persistent data read from cloud object storage on your cloud tenant, in bytes."""

    remaining_task_count: Optional[int] = None
    """number of remaining tasks to complete this is based on the current status and could be bigger or
    smaller in the future based on future updates"""

    result_fetch_time_ms: Optional[int] = None
    """Time spent fetching the query results after the execution finished, in milliseconds."""

    result_from_cache: Optional[bool] = None
    """`true` if the query result was fetched from cache, `false` otherwise."""

    rows_produced_count: Optional[int] = None
    """Total number of rows returned by the query."""

    rows_read_count: Optional[int] = None
    """Total number of rows read by the query."""

    runnable_tasks: Optional[int] = None
    """number of remaining tasks to complete, calculated by autoscaler StatementAnalysis.scala
    deprecated: use remaining_task_count instead"""

    spill_to_disk_bytes: Optional[int] = None
    """Size of data temporarily written to disk while executing the query, in bytes."""

    task_time_over_time_range: Optional[TaskTimeOverRange] = None
    """sum of task times completed in a range of wall clock time, approximated to a configurable number
    of points aggregated over all stages and jobs in the query (based on task_total_time_ms)"""

    task_total_time_ms: Optional[int] = None
    """Sum of execution time for all of the query’s tasks, in milliseconds."""

    total_time_ms: Optional[int] = None
    """Total execution time of the query from the client’s point of view, in milliseconds."""

    work_to_be_done: Optional[int] = None
    """remaining work to be done across all stages in the query, calculated by autoscaler
    StatementAnalysis.scala, in milliseconds deprecated: using
    projected_remaining_task_total_time_ms instead"""

    write_remote_bytes: Optional[int] = None
    """Size pf persistent data written to cloud object storage in your cloud tenant, in bytes."""

    def as_dict(self) -> dict:
        """Serializes the QueryMetrics into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.compilation_time_ms is not None:
            body["compilation_time_ms"] = self.compilation_time_ms
        if self.execution_time_ms is not None:
            body["execution_time_ms"] = self.execution_time_ms
        if self.network_sent_bytes is not None:
            body["network_sent_bytes"] = self.network_sent_bytes
        if self.overloading_queue_start_timestamp is not None:
            body["overloading_queue_start_timestamp"] = self.overloading_queue_start_timestamp
        if self.photon_total_time_ms is not None:
            body["photon_total_time_ms"] = self.photon_total_time_ms
        if self.projected_remaining_task_total_time_ms is not None:
            body["projected_remaining_task_total_time_ms"] = self.projected_remaining_task_total_time_ms
        if self.projected_remaining_wallclock_time_ms is not None:
            body["projected_remaining_wallclock_time_ms"] = self.projected_remaining_wallclock_time_ms
        if self.provisioning_queue_start_timestamp is not None:
            body["provisioning_queue_start_timestamp"] = self.provisioning_queue_start_timestamp
        if self.pruned_bytes is not None:
            body["pruned_bytes"] = self.pruned_bytes
        if self.pruned_files_count is not None:
            body["pruned_files_count"] = self.pruned_files_count
        if self.query_compilation_start_timestamp is not None:
            body["query_compilation_start_timestamp"] = self.query_compilation_start_timestamp
        if self.read_bytes is not None:
            body["read_bytes"] = self.read_bytes
        if self.read_cache_bytes is not None:
            body["read_cache_bytes"] = self.read_cache_bytes
        if self.read_files_bytes is not None:
            body["read_files_bytes"] = self.read_files_bytes
        if self.read_files_count is not None:
            body["read_files_count"] = self.read_files_count
        if self.read_partitions_count is not None:
            body["read_partitions_count"] = self.read_partitions_count
        if self.read_remote_bytes is not None:
            body["read_remote_bytes"] = self.read_remote_bytes
        if self.remaining_task_count is not None:
            body["remaining_task_count"] = self.remaining_task_count
        if self.result_fetch_time_ms is not None:
            body["result_fetch_time_ms"] = self.result_fetch_time_ms
        if self.result_from_cache is not None:
            body["result_from_cache"] = self.result_from_cache
        if self.rows_produced_count is not None:
            body["rows_produced_count"] = self.rows_produced_count
        if self.rows_read_count is not None:
            body["rows_read_count"] = self.rows_read_count
        if self.runnable_tasks is not None:
            body["runnable_tasks"] = self.runnable_tasks
        if self.spill_to_disk_bytes is not None:
            body["spill_to_disk_bytes"] = self.spill_to_disk_bytes
        if self.task_time_over_time_range:
            body["task_time_over_time_range"] = self.task_time_over_time_range.as_dict()
        if self.task_total_time_ms is not None:
            body["task_total_time_ms"] = self.task_total_time_ms
        if self.total_time_ms is not None:
            body["total_time_ms"] = self.total_time_ms
        if self.work_to_be_done is not None:
            body["work_to_be_done"] = self.work_to_be_done
        if self.write_remote_bytes is not None:
            body["write_remote_bytes"] = self.write_remote_bytes
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryMetrics into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.compilation_time_ms is not None:
            body["compilation_time_ms"] = self.compilation_time_ms
        if self.execution_time_ms is not None:
            body["execution_time_ms"] = self.execution_time_ms
        if self.network_sent_bytes is not None:
            body["network_sent_bytes"] = self.network_sent_bytes
        if self.overloading_queue_start_timestamp is not None:
            body["overloading_queue_start_timestamp"] = self.overloading_queue_start_timestamp
        if self.photon_total_time_ms is not None:
            body["photon_total_time_ms"] = self.photon_total_time_ms
        if self.projected_remaining_task_total_time_ms is not None:
            body["projected_remaining_task_total_time_ms"] = self.projected_remaining_task_total_time_ms
        if self.projected_remaining_wallclock_time_ms is not None:
            body["projected_remaining_wallclock_time_ms"] = self.projected_remaining_wallclock_time_ms
        if self.provisioning_queue_start_timestamp is not None:
            body["provisioning_queue_start_timestamp"] = self.provisioning_queue_start_timestamp
        if self.pruned_bytes is not None:
            body["pruned_bytes"] = self.pruned_bytes
        if self.pruned_files_count is not None:
            body["pruned_files_count"] = self.pruned_files_count
        if self.query_compilation_start_timestamp is not None:
            body["query_compilation_start_timestamp"] = self.query_compilation_start_timestamp
        if self.read_bytes is not None:
            body["read_bytes"] = self.read_bytes
        if self.read_cache_bytes is not None:
            body["read_cache_bytes"] = self.read_cache_bytes
        if self.read_files_bytes is not None:
            body["read_files_bytes"] = self.read_files_bytes
        if self.read_files_count is not None:
            body["read_files_count"] = self.read_files_count
        if self.read_partitions_count is not None:
            body["read_partitions_count"] = self.read_partitions_count
        if self.read_remote_bytes is not None:
            body["read_remote_bytes"] = self.read_remote_bytes
        if self.remaining_task_count is not None:
            body["remaining_task_count"] = self.remaining_task_count
        if self.result_fetch_time_ms is not None:
            body["result_fetch_time_ms"] = self.result_fetch_time_ms
        if self.result_from_cache is not None:
            body["result_from_cache"] = self.result_from_cache
        if self.rows_produced_count is not None:
            body["rows_produced_count"] = self.rows_produced_count
        if self.rows_read_count is not None:
            body["rows_read_count"] = self.rows_read_count
        if self.runnable_tasks is not None:
            body["runnable_tasks"] = self.runnable_tasks
        if self.spill_to_disk_bytes is not None:
            body["spill_to_disk_bytes"] = self.spill_to_disk_bytes
        if self.task_time_over_time_range:
            body["task_time_over_time_range"] = self.task_time_over_time_range
        if self.task_total_time_ms is not None:
            body["task_total_time_ms"] = self.task_total_time_ms
        if self.total_time_ms is not None:
            body["total_time_ms"] = self.total_time_ms
        if self.work_to_be_done is not None:
            body["work_to_be_done"] = self.work_to_be_done
        if self.write_remote_bytes is not None:
            body["write_remote_bytes"] = self.write_remote_bytes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryMetrics:
        """Deserializes the QueryMetrics from a dictionary."""
        return cls(
            compilation_time_ms=d.get("compilation_time_ms", None),
            execution_time_ms=d.get("execution_time_ms", None),
            network_sent_bytes=d.get("network_sent_bytes", None),
            overloading_queue_start_timestamp=d.get("overloading_queue_start_timestamp", None),
            photon_total_time_ms=d.get("photon_total_time_ms", None),
            projected_remaining_task_total_time_ms=d.get("projected_remaining_task_total_time_ms", None),
            projected_remaining_wallclock_time_ms=d.get("projected_remaining_wallclock_time_ms", None),
            provisioning_queue_start_timestamp=d.get("provisioning_queue_start_timestamp", None),
            pruned_bytes=d.get("pruned_bytes", None),
            pruned_files_count=d.get("pruned_files_count", None),
            query_compilation_start_timestamp=d.get("query_compilation_start_timestamp", None),
            read_bytes=d.get("read_bytes", None),
            read_cache_bytes=d.get("read_cache_bytes", None),
            read_files_bytes=d.get("read_files_bytes", None),
            read_files_count=d.get("read_files_count", None),
            read_partitions_count=d.get("read_partitions_count", None),
            read_remote_bytes=d.get("read_remote_bytes", None),
            remaining_task_count=d.get("remaining_task_count", None),
            result_fetch_time_ms=d.get("result_fetch_time_ms", None),
            result_from_cache=d.get("result_from_cache", None),
            rows_produced_count=d.get("rows_produced_count", None),
            rows_read_count=d.get("rows_read_count", None),
            runnable_tasks=d.get("runnable_tasks", None),
            spill_to_disk_bytes=d.get("spill_to_disk_bytes", None),
            task_time_over_time_range=_from_dict(d, "task_time_over_time_range", TaskTimeOverRange),
            task_total_time_ms=d.get("task_total_time_ms", None),
            total_time_ms=d.get("total_time_ms", None),
            work_to_be_done=d.get("work_to_be_done", None),
            write_remote_bytes=d.get("write_remote_bytes", None),
        )


@dataclass
class QueryOptions:
    catalog: Optional[str] = None
    """The name of the catalog to execute this query in."""

    moved_to_trash_at: Optional[str] = None
    """The timestamp when this query was moved to trash. Only present when the `is_archived` property
    is `true`. Trashed items are deleted after thirty days."""

    parameters: Optional[List[Parameter]] = None

    schema: Optional[str] = None
    """The name of the schema to execute this query in."""

    def as_dict(self) -> dict:
        """Serializes the QueryOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.moved_to_trash_at is not None:
            body["moved_to_trash_at"] = self.moved_to_trash_at
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.schema is not None:
            body["schema"] = self.schema
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.moved_to_trash_at is not None:
            body["moved_to_trash_at"] = self.moved_to_trash_at
        if self.parameters:
            body["parameters"] = self.parameters
        if self.schema is not None:
            body["schema"] = self.schema
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryOptions:
        """Deserializes the QueryOptions from a dictionary."""
        return cls(
            catalog=d.get("catalog", None),
            moved_to_trash_at=d.get("moved_to_trash_at", None),
            parameters=_repeated_dict(d, "parameters", Parameter),
            schema=d.get("schema", None),
        )


@dataclass
class QueryParameter:
    date_range_value: Optional[DateRangeValue] = None
    """Date-range query parameter value. Can only specify one of `dynamic_date_range_value` or
    `date_range_value`."""

    date_value: Optional[DateValue] = None
    """Date query parameter value. Can only specify one of `dynamic_date_value` or `date_value`."""

    enum_value: Optional[EnumValue] = None
    """Dropdown query parameter value."""

    name: Optional[str] = None
    """Literal parameter marker that appears between double curly braces in the query text."""

    numeric_value: Optional[NumericValue] = None
    """Numeric query parameter value."""

    query_backed_value: Optional[QueryBackedValue] = None
    """Query-based dropdown query parameter value."""

    text_value: Optional[TextValue] = None
    """Text query parameter value."""

    title: Optional[str] = None
    """Text displayed in the user-facing parameter widget in the UI."""

    def as_dict(self) -> dict:
        """Serializes the QueryParameter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.date_range_value:
            body["date_range_value"] = self.date_range_value.as_dict()
        if self.date_value:
            body["date_value"] = self.date_value.as_dict()
        if self.enum_value:
            body["enum_value"] = self.enum_value.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.numeric_value:
            body["numeric_value"] = self.numeric_value.as_dict()
        if self.query_backed_value:
            body["query_backed_value"] = self.query_backed_value.as_dict()
        if self.text_value:
            body["text_value"] = self.text_value.as_dict()
        if self.title is not None:
            body["title"] = self.title
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryParameter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.date_range_value:
            body["date_range_value"] = self.date_range_value
        if self.date_value:
            body["date_value"] = self.date_value
        if self.enum_value:
            body["enum_value"] = self.enum_value
        if self.name is not None:
            body["name"] = self.name
        if self.numeric_value:
            body["numeric_value"] = self.numeric_value
        if self.query_backed_value:
            body["query_backed_value"] = self.query_backed_value
        if self.text_value:
            body["text_value"] = self.text_value
        if self.title is not None:
            body["title"] = self.title
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryParameter:
        """Deserializes the QueryParameter from a dictionary."""
        return cls(
            date_range_value=_from_dict(d, "date_range_value", DateRangeValue),
            date_value=_from_dict(d, "date_value", DateValue),
            enum_value=_from_dict(d, "enum_value", EnumValue),
            name=d.get("name", None),
            numeric_value=_from_dict(d, "numeric_value", NumericValue),
            query_backed_value=_from_dict(d, "query_backed_value", QueryBackedValue),
            text_value=_from_dict(d, "text_value", TextValue),
            title=d.get("title", None),
        )


class QueryStatementType(Enum):

    ALTER = "ALTER"
    ANALYZE = "ANALYZE"
    COPY = "COPY"
    CREATE = "CREATE"
    DELETE = "DELETE"
    DESCRIBE = "DESCRIBE"
    DROP = "DROP"
    EXPLAIN = "EXPLAIN"
    GRANT = "GRANT"
    INSERT = "INSERT"
    MERGE = "MERGE"
    OPTIMIZE = "OPTIMIZE"
    OTHER = "OTHER"
    REFRESH = "REFRESH"
    REPLACE = "REPLACE"
    REVOKE = "REVOKE"
    SELECT = "SELECT"
    SET = "SET"
    SHOW = "SHOW"
    TRUNCATE = "TRUNCATE"
    UPDATE = "UPDATE"
    USE = "USE"


class QueryStatus(Enum):
    """Statuses which are also used by OperationStatus in runtime"""

    CANCELED = "CANCELED"
    COMPILED = "COMPILED"
    COMPILING = "COMPILING"
    FAILED = "FAILED"
    FINISHED = "FINISHED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    STARTED = "STARTED"


@dataclass
class QueryTag:
    """* A query execution can be annotated with an optional key-value pair to allow users to attribute
    the executions by key and optional value to filter by. QueryTag is the user-facing
    representation."""

    key: str

    value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the QueryTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryTag:
        """Deserializes the QueryTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class RepeatedEndpointConfPairs:
    config_pair: Optional[List[EndpointConfPair]] = None
    """Deprecated: Use configuration_pairs"""

    configuration_pairs: Optional[List[EndpointConfPair]] = None

    def as_dict(self) -> dict:
        """Serializes the RepeatedEndpointConfPairs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.config_pair:
            body["config_pair"] = [v.as_dict() for v in self.config_pair]
        if self.configuration_pairs:
            body["configuration_pairs"] = [v.as_dict() for v in self.configuration_pairs]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepeatedEndpointConfPairs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.config_pair:
            body["config_pair"] = self.config_pair
        if self.configuration_pairs:
            body["configuration_pairs"] = self.configuration_pairs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepeatedEndpointConfPairs:
        """Deserializes the RepeatedEndpointConfPairs from a dictionary."""
        return cls(
            config_pair=_repeated_dict(d, "config_pair", EndpointConfPair),
            configuration_pairs=_repeated_dict(d, "configuration_pairs", EndpointConfPair),
        )


@dataclass
class RestoreResponse:
    def as_dict(self) -> dict:
        """Serializes the RestoreResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RestoreResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RestoreResponse:
        """Deserializes the RestoreResponse from a dictionary."""
        return cls()


@dataclass
class ResultData:
    """Contains the result data of a single chunk when using `INLINE` disposition. When using
    `EXTERNAL_LINKS` disposition, the array `external_links` is used instead to provide URLs to the
    result data in cloud storage. Exactly one of these alternatives is used. (While the
    `external_links` array prepares the API to return multiple links in a single response. Currently
    only a single link is returned.)"""

    byte_count: Optional[int] = None
    """The number of bytes in the result chunk. This field is not available when using `INLINE`
    disposition."""

    chunk_index: Optional[int] = None
    """The position within the sequence of result set chunks."""

    data_array: Optional[List[List[str]]] = None
    """The `JSON_ARRAY` format is an array of arrays of values, where each non-null value is formatted
    as a string. Null values are encoded as JSON `null`."""

    external_links: Optional[List[ExternalLink]] = None

    next_chunk_index: Optional[int] = None
    """When fetching, provides the `chunk_index` for the _next_ chunk. If absent, indicates there are
    no more chunks. The next chunk can be fetched with a
    :method:statementexecution/getstatementresultchunkn request."""

    next_chunk_internal_link: Optional[str] = None
    """When fetching, provides a link to fetch the _next_ chunk. If absent, indicates there are no more
    chunks. This link is an absolute `path` to be joined with your `$DATABRICKS_HOST`, and should be
    treated as an opaque link. This is an alternative to using `next_chunk_index`."""

    row_count: Optional[int] = None
    """The number of rows within the result chunk."""

    row_offset: Optional[int] = None
    """The starting row offset within the result set."""

    def as_dict(self) -> dict:
        """Serializes the ResultData into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.byte_count is not None:
            body["byte_count"] = self.byte_count
        if self.chunk_index is not None:
            body["chunk_index"] = self.chunk_index
        if self.data_array:
            body["data_array"] = [v for v in self.data_array]
        if self.external_links:
            body["external_links"] = [v.as_dict() for v in self.external_links]
        if self.next_chunk_index is not None:
            body["next_chunk_index"] = self.next_chunk_index
        if self.next_chunk_internal_link is not None:
            body["next_chunk_internal_link"] = self.next_chunk_internal_link
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.row_offset is not None:
            body["row_offset"] = self.row_offset
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResultData into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.byte_count is not None:
            body["byte_count"] = self.byte_count
        if self.chunk_index is not None:
            body["chunk_index"] = self.chunk_index
        if self.data_array:
            body["data_array"] = self.data_array
        if self.external_links:
            body["external_links"] = self.external_links
        if self.next_chunk_index is not None:
            body["next_chunk_index"] = self.next_chunk_index
        if self.next_chunk_internal_link is not None:
            body["next_chunk_internal_link"] = self.next_chunk_internal_link
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.row_offset is not None:
            body["row_offset"] = self.row_offset
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResultData:
        """Deserializes the ResultData from a dictionary."""
        return cls(
            byte_count=d.get("byte_count", None),
            chunk_index=d.get("chunk_index", None),
            data_array=d.get("data_array", None),
            external_links=_repeated_dict(d, "external_links", ExternalLink),
            next_chunk_index=d.get("next_chunk_index", None),
            next_chunk_internal_link=d.get("next_chunk_internal_link", None),
            row_count=d.get("row_count", None),
            row_offset=d.get("row_offset", None),
        )


@dataclass
class ResultManifest:
    """The result manifest provides schema and metadata for the result set."""

    chunks: Optional[List[BaseChunkInfo]] = None
    """Array of result set chunk metadata."""

    format: Optional[Format] = None

    schema: Optional[ResultSchema] = None

    total_byte_count: Optional[int] = None
    """The total number of bytes in the result set. This field is not available when using `INLINE`
    disposition."""

    total_chunk_count: Optional[int] = None
    """The total number of chunks that the result set has been divided into."""

    total_row_count: Optional[int] = None
    """The total number of rows in the result set."""

    truncated: Optional[bool] = None
    """Indicates whether the result is truncated due to `row_limit` or `byte_limit`."""

    def as_dict(self) -> dict:
        """Serializes the ResultManifest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.chunks:
            body["chunks"] = [v.as_dict() for v in self.chunks]
        if self.format is not None:
            body["format"] = self.format.value
        if self.schema:
            body["schema"] = self.schema.as_dict()
        if self.total_byte_count is not None:
            body["total_byte_count"] = self.total_byte_count
        if self.total_chunk_count is not None:
            body["total_chunk_count"] = self.total_chunk_count
        if self.total_row_count is not None:
            body["total_row_count"] = self.total_row_count
        if self.truncated is not None:
            body["truncated"] = self.truncated
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResultManifest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.chunks:
            body["chunks"] = self.chunks
        if self.format is not None:
            body["format"] = self.format
        if self.schema:
            body["schema"] = self.schema
        if self.total_byte_count is not None:
            body["total_byte_count"] = self.total_byte_count
        if self.total_chunk_count is not None:
            body["total_chunk_count"] = self.total_chunk_count
        if self.total_row_count is not None:
            body["total_row_count"] = self.total_row_count
        if self.truncated is not None:
            body["truncated"] = self.truncated
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResultManifest:
        """Deserializes the ResultManifest from a dictionary."""
        return cls(
            chunks=_repeated_dict(d, "chunks", BaseChunkInfo),
            format=_enum(d, "format", Format),
            schema=_from_dict(d, "schema", ResultSchema),
            total_byte_count=d.get("total_byte_count", None),
            total_chunk_count=d.get("total_chunk_count", None),
            total_row_count=d.get("total_row_count", None),
            truncated=d.get("truncated", None),
        )


@dataclass
class ResultSchema:
    """The schema is an ordered list of column descriptions."""

    column_count: Optional[int] = None

    columns: Optional[List[ColumnInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ResultSchema into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.column_count is not None:
            body["column_count"] = self.column_count
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResultSchema into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.column_count is not None:
            body["column_count"] = self.column_count
        if self.columns:
            body["columns"] = self.columns
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResultSchema:
        """Deserializes the ResultSchema from a dictionary."""
        return cls(column_count=d.get("column_count", None), columns=_repeated_dict(d, "columns", ColumnInfo))


class RunAsMode(Enum):

    OWNER = "OWNER"
    VIEWER = "VIEWER"


class RunAsRole(Enum):

    OWNER = "owner"
    VIEWER = "viewer"


class SchedulePauseStatus(Enum):

    PAUSED = "PAUSED"
    UNPAUSED = "UNPAUSED"


@dataclass
class ServiceError:
    error_code: Optional[ServiceErrorCode] = None

    message: Optional[str] = None
    """A brief summary of the error condition."""

    def as_dict(self) -> dict:
        """Serializes the ServiceError into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.error_code is not None:
            body["error_code"] = self.error_code.value
        if self.message is not None:
            body["message"] = self.message
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ServiceError into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.error_code is not None:
            body["error_code"] = self.error_code
        if self.message is not None:
            body["message"] = self.message
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ServiceError:
        """Deserializes the ServiceError from a dictionary."""
        return cls(error_code=_enum(d, "error_code", ServiceErrorCode), message=d.get("message", None))


class ServiceErrorCode(Enum):

    ABORTED = "ABORTED"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    BAD_REQUEST = "BAD_REQUEST"
    CANCELLED = "CANCELLED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    IO_ERROR = "IO_ERROR"
    NOT_FOUND = "NOT_FOUND"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    SERVICE_UNDER_MAINTENANCE = "SERVICE_UNDER_MAINTENANCE"
    TEMPORARILY_UNAVAILABLE = "TEMPORARILY_UNAVAILABLE"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    UNKNOWN = "UNKNOWN"
    WORKSPACE_TEMPORARILY_UNAVAILABLE = "WORKSPACE_TEMPORARILY_UNAVAILABLE"


@dataclass
class SetResponse:
    access_control_list: Optional[List[AccessControl]] = None

    object_id: Optional[str] = None
    """An object's type and UUID, separated by a forward slash (/) character."""

    object_type: Optional[ObjectType] = None
    """A singular noun object type."""

    def as_dict(self) -> dict:
        """Serializes the SetResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SetResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SetResponse:
        """Deserializes the SetResponse from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", AccessControl),
            object_id=d.get("object_id", None),
            object_type=_enum(d, "object_type", ObjectType),
        )


class SetWorkspaceWarehouseConfigRequestSecurityPolicy(Enum):
    """Security policy to be used for warehouses"""

    DATA_ACCESS_CONTROL = "DATA_ACCESS_CONTROL"
    NONE = "NONE"
    PASSTHROUGH = "PASSTHROUGH"


@dataclass
class SetWorkspaceWarehouseConfigResponse:
    def as_dict(self) -> dict:
        """Serializes the SetWorkspaceWarehouseConfigResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SetWorkspaceWarehouseConfigResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SetWorkspaceWarehouseConfigResponse:
        """Deserializes the SetWorkspaceWarehouseConfigResponse from a dictionary."""
        return cls()


class SpotInstancePolicy(Enum):
    """EndpointSpotInstancePolicy configures whether the endpoint should use spot instances.

    The breakdown of how the EndpointSpotInstancePolicy converts to per cloud configurations is:

    +-------+--------------------------------------+--------------------------------+ | Cloud |
    COST_OPTIMIZED | RELIABILITY_OPTIMIZED |
    +-------+--------------------------------------+--------------------------------+ | AWS | On
    Demand Driver with Spot Executors | On Demand Driver and Executors | | AZURE | On Demand Driver
    and Executors | On Demand Driver and Executors |
    +-------+--------------------------------------+--------------------------------+

    While including "spot" in the enum name may limit the the future extensibility of this field
    because it limits this enum to denoting "spot or not", this is the field that PM recommends
    after discussion with customers per SC-48783."""

    COST_OPTIMIZED = "COST_OPTIMIZED"
    POLICY_UNSPECIFIED = "POLICY_UNSPECIFIED"
    RELIABILITY_OPTIMIZED = "RELIABILITY_OPTIMIZED"


@dataclass
class StartWarehouseResponse:
    def as_dict(self) -> dict:
        """Serializes the StartWarehouseResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StartWarehouseResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StartWarehouseResponse:
        """Deserializes the StartWarehouseResponse from a dictionary."""
        return cls()


class State(Enum):
    """* State of a warehouse."""

    DELETED = "DELETED"
    DELETING = "DELETING"
    RUNNING = "RUNNING"
    STARTING = "STARTING"
    STOPPED = "STOPPED"
    STOPPING = "STOPPING"


@dataclass
class StatementParameterListItem:
    name: str
    """The name of a parameter marker to be substituted in the statement."""

    type: Optional[str] = None
    """The data type, given as a string. For example: `INT`, `STRING`, `DECIMAL(10,2)`. If no type is
    given the type is assumed to be `STRING`. Complex types, such as `ARRAY`, `MAP`, and `STRUCT`
    are not supported. For valid types, refer to the section [Data types] of the SQL language
    reference.
    
    [Data types]: https://docs.databricks.com/sql/language-manual/functions/cast.html"""

    value: Optional[str] = None
    """The value to substitute, represented as a string. If omitted, the value is interpreted as NULL."""

    def as_dict(self) -> dict:
        """Serializes the StatementParameterListItem into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StatementParameterListItem into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StatementParameterListItem:
        """Deserializes the StatementParameterListItem from a dictionary."""
        return cls(name=d.get("name", None), type=d.get("type", None), value=d.get("value", None))


@dataclass
class StatementResponse:
    manifest: Optional[ResultManifest] = None

    result: Optional[ResultData] = None

    statement_id: Optional[str] = None
    """The statement ID is returned upon successfully submitting a SQL statement, and is a required
    reference for all subsequent calls."""

    status: Optional[StatementStatus] = None

    def as_dict(self) -> dict:
        """Serializes the StatementResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.manifest:
            body["manifest"] = self.manifest.as_dict()
        if self.result:
            body["result"] = self.result.as_dict()
        if self.statement_id is not None:
            body["statement_id"] = self.statement_id
        if self.status:
            body["status"] = self.status.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StatementResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.manifest:
            body["manifest"] = self.manifest
        if self.result:
            body["result"] = self.result
        if self.statement_id is not None:
            body["statement_id"] = self.statement_id
        if self.status:
            body["status"] = self.status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StatementResponse:
        """Deserializes the StatementResponse from a dictionary."""
        return cls(
            manifest=_from_dict(d, "manifest", ResultManifest),
            result=_from_dict(d, "result", ResultData),
            statement_id=d.get("statement_id", None),
            status=_from_dict(d, "status", StatementStatus),
        )


class StatementState(Enum):

    CANCELED = "CANCELED"
    CLOSED = "CLOSED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"


@dataclass
class StatementStatus:
    """The status response includes execution state and if relevant, error information."""

    error: Optional[ServiceError] = None

    state: Optional[StatementState] = None
    """Statement execution state: - `PENDING`: waiting for warehouse - `RUNNING`: running -
    `SUCCEEDED`: execution was successful, result data available for fetch - `FAILED`: execution
    failed; reason for failure described in accompanying error message - `CANCELED`: user canceled;
    can come from explicit cancel call, or timeout with `on_wait_timeout=CANCEL` - `CLOSED`:
    execution successful, and statement closed; result no longer available for fetch"""

    def as_dict(self) -> dict:
        """Serializes the StatementStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.error:
            body["error"] = self.error.as_dict()
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StatementStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.error:
            body["error"] = self.error
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StatementStatus:
        """Deserializes the StatementStatus from a dictionary."""
        return cls(error=_from_dict(d, "error", ServiceError), state=_enum(d, "state", StatementState))


class Status(Enum):

    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    HEALTHY = "HEALTHY"


@dataclass
class StopWarehouseResponse:
    def as_dict(self) -> dict:
        """Serializes the StopWarehouseResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StopWarehouseResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StopWarehouseResponse:
        """Deserializes the StopWarehouseResponse from a dictionary."""
        return cls()


@dataclass
class Success:
    message: Optional[SuccessMessage] = None

    def as_dict(self) -> dict:
        """Serializes the Success into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Success into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Success:
        """Deserializes the Success from a dictionary."""
        return cls(message=_enum(d, "message", SuccessMessage))


class SuccessMessage(Enum):

    SUCCESS = "Success"


@dataclass
class TaskTimeOverRange:
    entries: Optional[List[TaskTimeOverRangeEntry]] = None

    interval: Optional[int] = None
    """interval length for all entries (difference in start time and end time of an entry range) the
    same for all entries start time of first interval is query_start_time_ms"""

    def as_dict(self) -> dict:
        """Serializes the TaskTimeOverRange into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.entries:
            body["entries"] = [v.as_dict() for v in self.entries]
        if self.interval is not None:
            body["interval"] = self.interval
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TaskTimeOverRange into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.entries:
            body["entries"] = self.entries
        if self.interval is not None:
            body["interval"] = self.interval
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TaskTimeOverRange:
        """Deserializes the TaskTimeOverRange from a dictionary."""
        return cls(entries=_repeated_dict(d, "entries", TaskTimeOverRangeEntry), interval=d.get("interval", None))


@dataclass
class TaskTimeOverRangeEntry:
    task_completed_time_ms: Optional[int] = None
    """total task completion time in this time range, aggregated over all stages and jobs in the query"""

    def as_dict(self) -> dict:
        """Serializes the TaskTimeOverRangeEntry into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.task_completed_time_ms is not None:
            body["task_completed_time_ms"] = self.task_completed_time_ms
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TaskTimeOverRangeEntry into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.task_completed_time_ms is not None:
            body["task_completed_time_ms"] = self.task_completed_time_ms
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TaskTimeOverRangeEntry:
        """Deserializes the TaskTimeOverRangeEntry from a dictionary."""
        return cls(task_completed_time_ms=d.get("task_completed_time_ms", None))


@dataclass
class TerminationReason:
    code: Optional[TerminationReasonCode] = None
    """status code indicating why the cluster was terminated"""

    parameters: Optional[Dict[str, str]] = None
    """list of parameters that provide additional information about why the cluster was terminated"""

    type: Optional[TerminationReasonType] = None
    """type of the termination"""

    def as_dict(self) -> dict:
        """Serializes the TerminationReason into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.code is not None:
            body["code"] = self.code.value
        if self.parameters:
            body["parameters"] = self.parameters
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TerminationReason into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.code is not None:
            body["code"] = self.code
        if self.parameters:
            body["parameters"] = self.parameters
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TerminationReason:
        """Deserializes the TerminationReason from a dictionary."""
        return cls(
            code=_enum(d, "code", TerminationReasonCode),
            parameters=d.get("parameters", None),
            type=_enum(d, "type", TerminationReasonType),
        )


class TerminationReasonCode(Enum):
    """The status code indicating why the cluster was terminated"""

    ABUSE_DETECTED = "ABUSE_DETECTED"
    ACCESS_TOKEN_FAILURE = "ACCESS_TOKEN_FAILURE"
    ALLOCATION_TIMEOUT = "ALLOCATION_TIMEOUT"
    ALLOCATION_TIMEOUT_NODE_DAEMON_NOT_READY = "ALLOCATION_TIMEOUT_NODE_DAEMON_NOT_READY"
    ALLOCATION_TIMEOUT_NO_HEALTHY_AND_WARMED_UP_CLUSTERS = "ALLOCATION_TIMEOUT_NO_HEALTHY_AND_WARMED_UP_CLUSTERS"
    ALLOCATION_TIMEOUT_NO_HEALTHY_CLUSTERS = "ALLOCATION_TIMEOUT_NO_HEALTHY_CLUSTERS"
    ALLOCATION_TIMEOUT_NO_MATCHED_CLUSTERS = "ALLOCATION_TIMEOUT_NO_MATCHED_CLUSTERS"
    ALLOCATION_TIMEOUT_NO_READY_CLUSTERS = "ALLOCATION_TIMEOUT_NO_READY_CLUSTERS"
    ALLOCATION_TIMEOUT_NO_UNALLOCATED_CLUSTERS = "ALLOCATION_TIMEOUT_NO_UNALLOCATED_CLUSTERS"
    ALLOCATION_TIMEOUT_NO_WARMED_UP_CLUSTERS = "ALLOCATION_TIMEOUT_NO_WARMED_UP_CLUSTERS"
    ATTACH_PROJECT_FAILURE = "ATTACH_PROJECT_FAILURE"
    AWS_AUTHORIZATION_FAILURE = "AWS_AUTHORIZATION_FAILURE"
    AWS_INACCESSIBLE_KMS_KEY_FAILURE = "AWS_INACCESSIBLE_KMS_KEY_FAILURE"
    AWS_INSTANCE_PROFILE_UPDATE_FAILURE = "AWS_INSTANCE_PROFILE_UPDATE_FAILURE"
    AWS_INSUFFICIENT_FREE_ADDRESSES_IN_SUBNET_FAILURE = "AWS_INSUFFICIENT_FREE_ADDRESSES_IN_SUBNET_FAILURE"
    AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE = "AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE"
    AWS_INVALID_KEY_PAIR = "AWS_INVALID_KEY_PAIR"
    AWS_INVALID_KMS_KEY_STATE = "AWS_INVALID_KMS_KEY_STATE"
    AWS_MAX_SPOT_INSTANCE_COUNT_EXCEEDED_FAILURE = "AWS_MAX_SPOT_INSTANCE_COUNT_EXCEEDED_FAILURE"
    AWS_REQUEST_LIMIT_EXCEEDED = "AWS_REQUEST_LIMIT_EXCEEDED"
    AWS_RESOURCE_QUOTA_EXCEEDED = "AWS_RESOURCE_QUOTA_EXCEEDED"
    AWS_UNSUPPORTED_FAILURE = "AWS_UNSUPPORTED_FAILURE"
    AZURE_BYOK_KEY_PERMISSION_FAILURE = "AZURE_BYOK_KEY_PERMISSION_FAILURE"
    AZURE_EPHEMERAL_DISK_FAILURE = "AZURE_EPHEMERAL_DISK_FAILURE"
    AZURE_INVALID_DEPLOYMENT_TEMPLATE = "AZURE_INVALID_DEPLOYMENT_TEMPLATE"
    AZURE_OPERATION_NOT_ALLOWED_EXCEPTION = "AZURE_OPERATION_NOT_ALLOWED_EXCEPTION"
    AZURE_PACKED_DEPLOYMENT_PARTIAL_FAILURE = "AZURE_PACKED_DEPLOYMENT_PARTIAL_FAILURE"
    AZURE_QUOTA_EXCEEDED_EXCEPTION = "AZURE_QUOTA_EXCEEDED_EXCEPTION"
    AZURE_RESOURCE_MANAGER_THROTTLING = "AZURE_RESOURCE_MANAGER_THROTTLING"
    AZURE_RESOURCE_PROVIDER_THROTTLING = "AZURE_RESOURCE_PROVIDER_THROTTLING"
    AZURE_UNEXPECTED_DEPLOYMENT_TEMPLATE_FAILURE = "AZURE_UNEXPECTED_DEPLOYMENT_TEMPLATE_FAILURE"
    AZURE_VM_EXTENSION_FAILURE = "AZURE_VM_EXTENSION_FAILURE"
    AZURE_VNET_CONFIGURATION_FAILURE = "AZURE_VNET_CONFIGURATION_FAILURE"
    BOOTSTRAP_TIMEOUT = "BOOTSTRAP_TIMEOUT"
    BOOTSTRAP_TIMEOUT_CLOUD_PROVIDER_EXCEPTION = "BOOTSTRAP_TIMEOUT_CLOUD_PROVIDER_EXCEPTION"
    BOOTSTRAP_TIMEOUT_DUE_TO_MISCONFIG = "BOOTSTRAP_TIMEOUT_DUE_TO_MISCONFIG"
    BUDGET_POLICY_LIMIT_ENFORCEMENT_ACTIVATED = "BUDGET_POLICY_LIMIT_ENFORCEMENT_ACTIVATED"
    BUDGET_POLICY_RESOLUTION_FAILURE = "BUDGET_POLICY_RESOLUTION_FAILURE"
    CLOUD_ACCOUNT_POD_QUOTA_EXCEEDED = "CLOUD_ACCOUNT_POD_QUOTA_EXCEEDED"
    CLOUD_ACCOUNT_SETUP_FAILURE = "CLOUD_ACCOUNT_SETUP_FAILURE"
    CLOUD_OPERATION_CANCELLED = "CLOUD_OPERATION_CANCELLED"
    CLOUD_PROVIDER_DISK_SETUP_FAILURE = "CLOUD_PROVIDER_DISK_SETUP_FAILURE"
    CLOUD_PROVIDER_INSTANCE_NOT_LAUNCHED = "CLOUD_PROVIDER_INSTANCE_NOT_LAUNCHED"
    CLOUD_PROVIDER_LAUNCH_FAILURE = "CLOUD_PROVIDER_LAUNCH_FAILURE"
    CLOUD_PROVIDER_LAUNCH_FAILURE_DUE_TO_MISCONFIG = "CLOUD_PROVIDER_LAUNCH_FAILURE_DUE_TO_MISCONFIG"
    CLOUD_PROVIDER_RESOURCE_STOCKOUT = "CLOUD_PROVIDER_RESOURCE_STOCKOUT"
    CLOUD_PROVIDER_RESOURCE_STOCKOUT_DUE_TO_MISCONFIG = "CLOUD_PROVIDER_RESOURCE_STOCKOUT_DUE_TO_MISCONFIG"
    CLOUD_PROVIDER_SHUTDOWN = "CLOUD_PROVIDER_SHUTDOWN"
    CLUSTER_OPERATION_THROTTLED = "CLUSTER_OPERATION_THROTTLED"
    CLUSTER_OPERATION_TIMEOUT = "CLUSTER_OPERATION_TIMEOUT"
    COMMUNICATION_LOST = "COMMUNICATION_LOST"
    CONTAINER_LAUNCH_FAILURE = "CONTAINER_LAUNCH_FAILURE"
    CONTROL_PLANE_CONNECTION_FAILURE = "CONTROL_PLANE_CONNECTION_FAILURE"
    CONTROL_PLANE_CONNECTION_FAILURE_DUE_TO_MISCONFIG = "CONTROL_PLANE_CONNECTION_FAILURE_DUE_TO_MISCONFIG"
    CONTROL_PLANE_REQUEST_FAILURE = "CONTROL_PLANE_REQUEST_FAILURE"
    CONTROL_PLANE_REQUEST_FAILURE_DUE_TO_MISCONFIG = "CONTROL_PLANE_REQUEST_FAILURE_DUE_TO_MISCONFIG"
    DATABASE_CONNECTION_FAILURE = "DATABASE_CONNECTION_FAILURE"
    DATA_ACCESS_CONFIG_CHANGED = "DATA_ACCESS_CONFIG_CHANGED"
    DBFS_COMPONENT_UNHEALTHY = "DBFS_COMPONENT_UNHEALTHY"
    DBR_IMAGE_RESOLUTION_FAILURE = "DBR_IMAGE_RESOLUTION_FAILURE"
    DISASTER_RECOVERY_REPLICATION = "DISASTER_RECOVERY_REPLICATION"
    DNS_RESOLUTION_ERROR = "DNS_RESOLUTION_ERROR"
    DOCKER_CONTAINER_CREATION_EXCEPTION = "DOCKER_CONTAINER_CREATION_EXCEPTION"
    DOCKER_IMAGE_PULL_FAILURE = "DOCKER_IMAGE_PULL_FAILURE"
    DOCKER_IMAGE_TOO_LARGE_FOR_INSTANCE_EXCEPTION = "DOCKER_IMAGE_TOO_LARGE_FOR_INSTANCE_EXCEPTION"
    DOCKER_INVALID_OS_EXCEPTION = "DOCKER_INVALID_OS_EXCEPTION"
    DRIVER_EVICTION = "DRIVER_EVICTION"
    DRIVER_LAUNCH_TIMEOUT = "DRIVER_LAUNCH_TIMEOUT"
    DRIVER_NODE_UNREACHABLE = "DRIVER_NODE_UNREACHABLE"
    DRIVER_OUT_OF_DISK = "DRIVER_OUT_OF_DISK"
    DRIVER_OUT_OF_MEMORY = "DRIVER_OUT_OF_MEMORY"
    DRIVER_POD_CREATION_FAILURE = "DRIVER_POD_CREATION_FAILURE"
    DRIVER_UNEXPECTED_FAILURE = "DRIVER_UNEXPECTED_FAILURE"
    DRIVER_UNHEALTHY = "DRIVER_UNHEALTHY"
    DRIVER_UNREACHABLE = "DRIVER_UNREACHABLE"
    DRIVER_UNRESPONSIVE = "DRIVER_UNRESPONSIVE"
    DYNAMIC_SPARK_CONF_SIZE_EXCEEDED = "DYNAMIC_SPARK_CONF_SIZE_EXCEEDED"
    EOS_SPARK_IMAGE = "EOS_SPARK_IMAGE"
    EXECUTION_COMPONENT_UNHEALTHY = "EXECUTION_COMPONENT_UNHEALTHY"
    EXECUTOR_POD_UNSCHEDULED = "EXECUTOR_POD_UNSCHEDULED"
    GCP_API_RATE_QUOTA_EXCEEDED = "GCP_API_RATE_QUOTA_EXCEEDED"
    GCP_DENIED_BY_ORG_POLICY = "GCP_DENIED_BY_ORG_POLICY"
    GCP_FORBIDDEN = "GCP_FORBIDDEN"
    GCP_IAM_TIMEOUT = "GCP_IAM_TIMEOUT"
    GCP_INACCESSIBLE_KMS_KEY_FAILURE = "GCP_INACCESSIBLE_KMS_KEY_FAILURE"
    GCP_INSUFFICIENT_CAPACITY = "GCP_INSUFFICIENT_CAPACITY"
    GCP_IP_SPACE_EXHAUSTED = "GCP_IP_SPACE_EXHAUSTED"
    GCP_KMS_KEY_PERMISSION_DENIED = "GCP_KMS_KEY_PERMISSION_DENIED"
    GCP_NOT_FOUND = "GCP_NOT_FOUND"
    GCP_QUOTA_EXCEEDED = "GCP_QUOTA_EXCEEDED"
    GCP_RESOURCE_QUOTA_EXCEEDED = "GCP_RESOURCE_QUOTA_EXCEEDED"
    GCP_SERVICE_ACCOUNT_ACCESS_DENIED = "GCP_SERVICE_ACCOUNT_ACCESS_DENIED"
    GCP_SERVICE_ACCOUNT_DELETED = "GCP_SERVICE_ACCOUNT_DELETED"
    GCP_SERVICE_ACCOUNT_NOT_FOUND = "GCP_SERVICE_ACCOUNT_NOT_FOUND"
    GCP_SUBNET_NOT_READY = "GCP_SUBNET_NOT_READY"
    GCP_TRUSTED_IMAGE_PROJECTS_VIOLATED = "GCP_TRUSTED_IMAGE_PROJECTS_VIOLATED"
    GKE_BASED_CLUSTER_TERMINATION = "GKE_BASED_CLUSTER_TERMINATION"
    GLOBAL_INIT_SCRIPT_FAILURE = "GLOBAL_INIT_SCRIPT_FAILURE"
    HIVEMETASTORE_CONNECTIVITY_FAILURE = "HIVEMETASTORE_CONNECTIVITY_FAILURE"
    HIVE_METASTORE_PROVISIONING_FAILURE = "HIVE_METASTORE_PROVISIONING_FAILURE"
    IMAGE_PULL_PERMISSION_DENIED = "IMAGE_PULL_PERMISSION_DENIED"
    INACTIVITY = "INACTIVITY"
    INIT_CONTAINER_NOT_FINISHED = "INIT_CONTAINER_NOT_FINISHED"
    INIT_SCRIPT_FAILURE = "INIT_SCRIPT_FAILURE"
    INSTANCE_POOL_CLUSTER_FAILURE = "INSTANCE_POOL_CLUSTER_FAILURE"
    INSTANCE_POOL_MAX_CAPACITY_REACHED = "INSTANCE_POOL_MAX_CAPACITY_REACHED"
    INSTANCE_POOL_NOT_FOUND = "INSTANCE_POOL_NOT_FOUND"
    INSTANCE_UNREACHABLE = "INSTANCE_UNREACHABLE"
    INSTANCE_UNREACHABLE_DUE_TO_MISCONFIG = "INSTANCE_UNREACHABLE_DUE_TO_MISCONFIG"
    INTERNAL_CAPACITY_FAILURE = "INTERNAL_CAPACITY_FAILURE"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    INVALID_AWS_PARAMETER = "INVALID_AWS_PARAMETER"
    INVALID_INSTANCE_PLACEMENT_PROTOCOL = "INVALID_INSTANCE_PLACEMENT_PROTOCOL"
    INVALID_SPARK_IMAGE = "INVALID_SPARK_IMAGE"
    INVALID_WORKER_IMAGE_FAILURE = "INVALID_WORKER_IMAGE_FAILURE"
    IN_PENALTY_BOX = "IN_PENALTY_BOX"
    IP_EXHAUSTION_FAILURE = "IP_EXHAUSTION_FAILURE"
    JOB_FINISHED = "JOB_FINISHED"
    K8S_ACTIVE_POD_QUOTA_EXCEEDED = "K8S_ACTIVE_POD_QUOTA_EXCEEDED"
    K8S_AUTOSCALING_FAILURE = "K8S_AUTOSCALING_FAILURE"
    K8S_DBR_CLUSTER_LAUNCH_TIMEOUT = "K8S_DBR_CLUSTER_LAUNCH_TIMEOUT"
    LAZY_ALLOCATION_TIMEOUT = "LAZY_ALLOCATION_TIMEOUT"
    MAINTENANCE_MODE = "MAINTENANCE_MODE"
    METASTORE_COMPONENT_UNHEALTHY = "METASTORE_COMPONENT_UNHEALTHY"
    MTLS_PORT_CONNECTIVITY_FAILURE = "MTLS_PORT_CONNECTIVITY_FAILURE"
    NEPHOS_RESOURCE_MANAGEMENT = "NEPHOS_RESOURCE_MANAGEMENT"
    NETVISOR_SETUP_TIMEOUT = "NETVISOR_SETUP_TIMEOUT"
    NETWORK_CHECK_CONTROL_PLANE_FAILURE = "NETWORK_CHECK_CONTROL_PLANE_FAILURE"
    NETWORK_CHECK_CONTROL_PLANE_FAILURE_DUE_TO_MISCONFIG = "NETWORK_CHECK_CONTROL_PLANE_FAILURE_DUE_TO_MISCONFIG"
    NETWORK_CHECK_DNS_SERVER_FAILURE = "NETWORK_CHECK_DNS_SERVER_FAILURE"
    NETWORK_CHECK_DNS_SERVER_FAILURE_DUE_TO_MISCONFIG = "NETWORK_CHECK_DNS_SERVER_FAILURE_DUE_TO_MISCONFIG"
    NETWORK_CHECK_METADATA_ENDPOINT_FAILURE = "NETWORK_CHECK_METADATA_ENDPOINT_FAILURE"
    NETWORK_CHECK_METADATA_ENDPOINT_FAILURE_DUE_TO_MISCONFIG = (
        "NETWORK_CHECK_METADATA_ENDPOINT_FAILURE_DUE_TO_MISCONFIG"
    )
    NETWORK_CHECK_MULTIPLE_COMPONENTS_FAILURE = "NETWORK_CHECK_MULTIPLE_COMPONENTS_FAILURE"
    NETWORK_CHECK_MULTIPLE_COMPONENTS_FAILURE_DUE_TO_MISCONFIG = (
        "NETWORK_CHECK_MULTIPLE_COMPONENTS_FAILURE_DUE_TO_MISCONFIG"
    )
    NETWORK_CHECK_NIC_FAILURE = "NETWORK_CHECK_NIC_FAILURE"
    NETWORK_CHECK_NIC_FAILURE_DUE_TO_MISCONFIG = "NETWORK_CHECK_NIC_FAILURE_DUE_TO_MISCONFIG"
    NETWORK_CHECK_STORAGE_FAILURE = "NETWORK_CHECK_STORAGE_FAILURE"
    NETWORK_CHECK_STORAGE_FAILURE_DUE_TO_MISCONFIG = "NETWORK_CHECK_STORAGE_FAILURE_DUE_TO_MISCONFIG"
    NETWORK_CONFIGURATION_FAILURE = "NETWORK_CONFIGURATION_FAILURE"
    NFS_MOUNT_FAILURE = "NFS_MOUNT_FAILURE"
    NO_MATCHED_K8S = "NO_MATCHED_K8S"
    NO_MATCHED_K8S_TESTING_TAG = "NO_MATCHED_K8S_TESTING_TAG"
    NPIP_TUNNEL_SETUP_FAILURE = "NPIP_TUNNEL_SETUP_FAILURE"
    NPIP_TUNNEL_TOKEN_FAILURE = "NPIP_TUNNEL_TOKEN_FAILURE"
    POD_ASSIGNMENT_FAILURE = "POD_ASSIGNMENT_FAILURE"
    POD_SCHEDULING_FAILURE = "POD_SCHEDULING_FAILURE"
    RATE_LIMITED = "RATE_LIMITED"
    REQUEST_REJECTED = "REQUEST_REJECTED"
    REQUEST_THROTTLED = "REQUEST_THROTTLED"
    RESOURCE_USAGE_BLOCKED = "RESOURCE_USAGE_BLOCKED"
    SECRET_CREATION_FAILURE = "SECRET_CREATION_FAILURE"
    SECRET_PERMISSION_DENIED = "SECRET_PERMISSION_DENIED"
    SECRET_RESOLUTION_ERROR = "SECRET_RESOLUTION_ERROR"
    SECURITY_DAEMON_REGISTRATION_EXCEPTION = "SECURITY_DAEMON_REGISTRATION_EXCEPTION"
    SELF_BOOTSTRAP_FAILURE = "SELF_BOOTSTRAP_FAILURE"
    SERVERLESS_LONG_RUNNING_TERMINATED = "SERVERLESS_LONG_RUNNING_TERMINATED"
    SKIPPED_SLOW_NODES = "SKIPPED_SLOW_NODES"
    SLOW_IMAGE_DOWNLOAD = "SLOW_IMAGE_DOWNLOAD"
    SPARK_ERROR = "SPARK_ERROR"
    SPARK_IMAGE_DOWNLOAD_FAILURE = "SPARK_IMAGE_DOWNLOAD_FAILURE"
    SPARK_IMAGE_DOWNLOAD_THROTTLED = "SPARK_IMAGE_DOWNLOAD_THROTTLED"
    SPARK_IMAGE_NOT_FOUND = "SPARK_IMAGE_NOT_FOUND"
    SPARK_STARTUP_FAILURE = "SPARK_STARTUP_FAILURE"
    SPOT_INSTANCE_TERMINATION = "SPOT_INSTANCE_TERMINATION"
    SSH_BOOTSTRAP_FAILURE = "SSH_BOOTSTRAP_FAILURE"
    STORAGE_DOWNLOAD_FAILURE = "STORAGE_DOWNLOAD_FAILURE"
    STORAGE_DOWNLOAD_FAILURE_DUE_TO_MISCONFIG = "STORAGE_DOWNLOAD_FAILURE_DUE_TO_MISCONFIG"
    STORAGE_DOWNLOAD_FAILURE_SLOW = "STORAGE_DOWNLOAD_FAILURE_SLOW"
    STORAGE_DOWNLOAD_FAILURE_THROTTLED = "STORAGE_DOWNLOAD_FAILURE_THROTTLED"
    STS_CLIENT_SETUP_FAILURE = "STS_CLIENT_SETUP_FAILURE"
    SUBNET_EXHAUSTED_FAILURE = "SUBNET_EXHAUSTED_FAILURE"
    TEMPORARILY_UNAVAILABLE = "TEMPORARILY_UNAVAILABLE"
    TRIAL_EXPIRED = "TRIAL_EXPIRED"
    UNEXPECTED_LAUNCH_FAILURE = "UNEXPECTED_LAUNCH_FAILURE"
    UNEXPECTED_POD_RECREATION = "UNEXPECTED_POD_RECREATION"
    UNKNOWN = "UNKNOWN"
    UNSUPPORTED_INSTANCE_TYPE = "UNSUPPORTED_INSTANCE_TYPE"
    UPDATE_INSTANCE_PROFILE_FAILURE = "UPDATE_INSTANCE_PROFILE_FAILURE"
    USAGE_POLICY_ENTITLEMENT_DENIED = "USAGE_POLICY_ENTITLEMENT_DENIED"
    USER_INITIATED_VM_TERMINATION = "USER_INITIATED_VM_TERMINATION"
    USER_REQUEST = "USER_REQUEST"
    WORKER_SETUP_FAILURE = "WORKER_SETUP_FAILURE"
    WORKSPACE_CANCELLED_ERROR = "WORKSPACE_CANCELLED_ERROR"
    WORKSPACE_CONFIGURATION_ERROR = "WORKSPACE_CONFIGURATION_ERROR"
    WORKSPACE_UPDATE = "WORKSPACE_UPDATE"


class TerminationReasonType(Enum):
    """type of the termination"""

    CLIENT_ERROR = "CLIENT_ERROR"
    CLOUD_FAILURE = "CLOUD_FAILURE"
    SERVICE_FAULT = "SERVICE_FAULT"
    SUCCESS = "SUCCESS"


@dataclass
class TextValue:
    value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the TextValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TextValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TextValue:
        """Deserializes the TextValue from a dictionary."""
        return cls(value=d.get("value", None))


@dataclass
class TimeRange:
    end_time_ms: Optional[int] = None
    """The end time in milliseconds."""

    start_time_ms: Optional[int] = None
    """The start time in milliseconds."""

    def as_dict(self) -> dict:
        """Serializes the TimeRange into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.end_time_ms is not None:
            body["end_time_ms"] = self.end_time_ms
        if self.start_time_ms is not None:
            body["start_time_ms"] = self.start_time_ms
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TimeRange into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.end_time_ms is not None:
            body["end_time_ms"] = self.end_time_ms
        if self.start_time_ms is not None:
            body["start_time_ms"] = self.start_time_ms
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TimeRange:
        """Deserializes the TimeRange from a dictionary."""
        return cls(end_time_ms=d.get("end_time_ms", None), start_time_ms=d.get("start_time_ms", None))


@dataclass
class TransferOwnershipObjectId:
    new_owner: Optional[str] = None
    """Email address for the new owner, who must exist in the workspace."""

    def as_dict(self) -> dict:
        """Serializes the TransferOwnershipObjectId into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.new_owner is not None:
            body["new_owner"] = self.new_owner
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TransferOwnershipObjectId into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.new_owner is not None:
            body["new_owner"] = self.new_owner
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TransferOwnershipObjectId:
        """Deserializes the TransferOwnershipObjectId from a dictionary."""
        return cls(new_owner=d.get("new_owner", None))


@dataclass
class UpdateAlertRequestAlert:
    condition: Optional[AlertCondition] = None
    """Trigger conditions of the alert."""

    custom_body: Optional[str] = None
    """Custom body of alert notification, if it exists. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    custom_subject: Optional[str] = None
    """Custom subject of alert notification, if it exists. This can include email subject entries and
    Slack notification headers, for example. See [here] for custom templating instructions.
    
    [here]: https://docs.databricks.com/sql/user/alerts/index.html"""

    display_name: Optional[str] = None
    """The display name of the alert."""

    notify_on_ok: Optional[bool] = None
    """Whether to notify alert subscribers when alert returns back to normal."""

    owner_user_name: Optional[str] = None
    """The owner's username. This field is set to "Unavailable" if the user has been deleted."""

    query_id: Optional[str] = None
    """UUID of the query attached to the alert."""

    seconds_to_retrigger: Optional[int] = None
    """Number of seconds an alert must wait after being triggered to rearm itself. After rearming, it
    can be triggered again. If 0 or not specified, the alert will not be triggered again."""

    def as_dict(self) -> dict:
        """Serializes the UpdateAlertRequestAlert into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.condition:
            body["condition"] = self.condition.as_dict()
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateAlertRequestAlert into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.condition:
            body["condition"] = self.condition
        if self.custom_body is not None:
            body["custom_body"] = self.custom_body
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.notify_on_ok is not None:
            body["notify_on_ok"] = self.notify_on_ok
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.seconds_to_retrigger is not None:
            body["seconds_to_retrigger"] = self.seconds_to_retrigger
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateAlertRequestAlert:
        """Deserializes the UpdateAlertRequestAlert from a dictionary."""
        return cls(
            condition=_from_dict(d, "condition", AlertCondition),
            custom_body=d.get("custom_body", None),
            custom_subject=d.get("custom_subject", None),
            display_name=d.get("display_name", None),
            notify_on_ok=d.get("notify_on_ok", None),
            owner_user_name=d.get("owner_user_name", None),
            query_id=d.get("query_id", None),
            seconds_to_retrigger=d.get("seconds_to_retrigger", None),
        )


@dataclass
class UpdateQueryRequestQuery:
    apply_auto_limit: Optional[bool] = None
    """Whether to apply a 1000 row limit to the query result."""

    catalog: Optional[str] = None
    """Name of the catalog where this query will be executed."""

    description: Optional[str] = None
    """General description that conveys additional information about this query such as usage notes."""

    display_name: Optional[str] = None
    """Display name of the query that appears in list views, widget headings, and on the query page."""

    owner_user_name: Optional[str] = None
    """Username of the user that owns the query."""

    parameters: Optional[List[QueryParameter]] = None
    """List of query parameter definitions."""

    query_text: Optional[str] = None
    """Text of the query to be run."""

    run_as_mode: Optional[RunAsMode] = None
    """Sets the "Run as" role for the object."""

    schema: Optional[str] = None
    """Name of the schema where this query will be executed."""

    tags: Optional[List[str]] = None

    warehouse_id: Optional[str] = None
    """ID of the SQL warehouse attached to the query."""

    def as_dict(self) -> dict:
        """Serializes the UpdateQueryRequestQuery into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode.value
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = [v for v in self.tags]
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateQueryRequestQuery into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.apply_auto_limit is not None:
            body["apply_auto_limit"] = self.apply_auto_limit
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.description is not None:
            body["description"] = self.description
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.owner_user_name is not None:
            body["owner_user_name"] = self.owner_user_name
        if self.parameters:
            body["parameters"] = self.parameters
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.run_as_mode is not None:
            body["run_as_mode"] = self.run_as_mode
        if self.schema is not None:
            body["schema"] = self.schema
        if self.tags:
            body["tags"] = self.tags
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateQueryRequestQuery:
        """Deserializes the UpdateQueryRequestQuery from a dictionary."""
        return cls(
            apply_auto_limit=d.get("apply_auto_limit", None),
            catalog=d.get("catalog", None),
            description=d.get("description", None),
            display_name=d.get("display_name", None),
            owner_user_name=d.get("owner_user_name", None),
            parameters=_repeated_dict(d, "parameters", QueryParameter),
            query_text=d.get("query_text", None),
            run_as_mode=_enum(d, "run_as_mode", RunAsMode),
            schema=d.get("schema", None),
            tags=d.get("tags", None),
            warehouse_id=d.get("warehouse_id", None),
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
class UpdateVisualizationRequestVisualization:
    display_name: Optional[str] = None
    """The display name of the visualization."""

    serialized_options: Optional[str] = None
    """The visualization options varies widely from one visualization type to the next and is
    unsupported. Databricks does not recommend modifying visualization options directly."""

    serialized_query_plan: Optional[str] = None
    """The visualization query plan varies widely from one visualization type to the next and is
    unsupported. Databricks does not recommend modifying the visualization query plan directly."""

    type: Optional[str] = None
    """The type of visualization: counter, table, funnel, and so on."""

    def as_dict(self) -> dict:
        """Serializes the UpdateVisualizationRequestVisualization into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.serialized_options is not None:
            body["serialized_options"] = self.serialized_options
        if self.serialized_query_plan is not None:
            body["serialized_query_plan"] = self.serialized_query_plan
        if self.type is not None:
            body["type"] = self.type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateVisualizationRequestVisualization into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.serialized_options is not None:
            body["serialized_options"] = self.serialized_options
        if self.serialized_query_plan is not None:
            body["serialized_query_plan"] = self.serialized_query_plan
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateVisualizationRequestVisualization:
        """Deserializes the UpdateVisualizationRequestVisualization from a dictionary."""
        return cls(
            display_name=d.get("display_name", None),
            serialized_options=d.get("serialized_options", None),
            serialized_query_plan=d.get("serialized_query_plan", None),
            type=d.get("type", None),
        )


@dataclass
class User:
    email: Optional[str] = None

    id: Optional[int] = None

    name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the User into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.email is not None:
            body["email"] = self.email
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the User into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.email is not None:
            body["email"] = self.email
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> User:
        """Deserializes the User from a dictionary."""
        return cls(email=d.get("email", None), id=d.get("id", None), name=d.get("name", None))


@dataclass
class Visualization:
    create_time: Optional[str] = None
    """The timestamp indicating when the visualization was created."""

    display_name: Optional[str] = None
    """The display name of the visualization."""

    id: Optional[str] = None
    """UUID identifying the visualization."""

    query_id: Optional[str] = None
    """UUID of the query that the visualization is attached to."""

    serialized_options: Optional[str] = None
    """The visualization options varies widely from one visualization type to the next and is
    unsupported. Databricks does not recommend modifying visualization options directly."""

    serialized_query_plan: Optional[str] = None
    """The visualization query plan varies widely from one visualization type to the next and is
    unsupported. Databricks does not recommend modifying the visualization query plan directly."""

    type: Optional[str] = None
    """The type of visualization: counter, table, funnel, and so on."""

    update_time: Optional[str] = None
    """The timestamp indicating when the visualization was updated."""

    def as_dict(self) -> dict:
        """Serializes the Visualization into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.serialized_options is not None:
            body["serialized_options"] = self.serialized_options
        if self.serialized_query_plan is not None:
            body["serialized_query_plan"] = self.serialized_query_plan
        if self.type is not None:
            body["type"] = self.type
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Visualization into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        if self.query_id is not None:
            body["query_id"] = self.query_id
        if self.serialized_options is not None:
            body["serialized_options"] = self.serialized_options
        if self.serialized_query_plan is not None:
            body["serialized_query_plan"] = self.serialized_query_plan
        if self.type is not None:
            body["type"] = self.type
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Visualization:
        """Deserializes the Visualization from a dictionary."""
        return cls(
            create_time=d.get("create_time", None),
            display_name=d.get("display_name", None),
            id=d.get("id", None),
            query_id=d.get("query_id", None),
            serialized_options=d.get("serialized_options", None),
            serialized_query_plan=d.get("serialized_query_plan", None),
            type=d.get("type", None),
            update_time=d.get("update_time", None),
        )


@dataclass
class WarehouseAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[WarehousePermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the WarehouseAccessControlRequest into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the WarehouseAccessControlRequest into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> WarehouseAccessControlRequest:
        """Deserializes the WarehouseAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", WarehousePermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class WarehouseAccessControlResponse:
    all_permissions: Optional[List[WarehousePermission]] = None
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
        """Serializes the WarehouseAccessControlResponse into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the WarehouseAccessControlResponse into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> WarehouseAccessControlResponse:
        """Deserializes the WarehouseAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", WarehousePermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class WarehousePermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[WarehousePermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the WarehousePermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WarehousePermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WarehousePermission:
        """Deserializes the WarehousePermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", WarehousePermissionLevel),
        )


class WarehousePermissionLevel(Enum):
    """Permission level"""

    CAN_MANAGE = "CAN_MANAGE"
    CAN_MONITOR = "CAN_MONITOR"
    CAN_USE = "CAN_USE"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"


@dataclass
class WarehousePermissions:
    access_control_list: Optional[List[WarehouseAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the WarehousePermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WarehousePermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WarehousePermissions:
        """Deserializes the WarehousePermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", WarehouseAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class WarehousePermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[WarehousePermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the WarehousePermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WarehousePermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WarehousePermissionsDescription:
        """Deserializes the WarehousePermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None),
            permission_level=_enum(d, "permission_level", WarehousePermissionLevel),
        )


@dataclass
class WarehouseTypePair:
    """* Configuration values to enable or disable the access to specific warehouse types in the
    workspace."""

    enabled: Optional[bool] = None
    """If set to false the specific warehouse type will not be be allowed as a value for warehouse_type
    in CreateWarehouse and EditWarehouse"""

    warehouse_type: Optional[WarehouseTypePairWarehouseType] = None

    def as_dict(self) -> dict:
        """Serializes the WarehouseTypePair into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.warehouse_type is not None:
            body["warehouse_type"] = self.warehouse_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WarehouseTypePair into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.warehouse_type is not None:
            body["warehouse_type"] = self.warehouse_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WarehouseTypePair:
        """Deserializes the WarehouseTypePair from a dictionary."""
        return cls(
            enabled=d.get("enabled", None), warehouse_type=_enum(d, "warehouse_type", WarehouseTypePairWarehouseType)
        )


class WarehouseTypePairWarehouseType(Enum):

    CLASSIC = "CLASSIC"
    PRO = "PRO"
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"


@dataclass
class Widget:
    id: Optional[str] = None
    """The unique ID for this widget."""

    options: Optional[WidgetOptions] = None

    visualization: Optional[LegacyVisualization] = None
    """The visualization description API changes frequently and is unsupported. You can duplicate a
    visualization by copying description objects received _from the API_ and then using them to
    create a new one with a POST request to the same endpoint. Databricks does not recommend
    constructing ad-hoc visualizations entirely in JSON."""

    width: Optional[int] = None
    """Unused field."""

    def as_dict(self) -> dict:
        """Serializes the Widget into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.options:
            body["options"] = self.options.as_dict()
        if self.visualization:
            body["visualization"] = self.visualization.as_dict()
        if self.width is not None:
            body["width"] = self.width
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Widget into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        if self.options:
            body["options"] = self.options
        if self.visualization:
            body["visualization"] = self.visualization
        if self.width is not None:
            body["width"] = self.width
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Widget:
        """Deserializes the Widget from a dictionary."""
        return cls(
            id=d.get("id", None),
            options=_from_dict(d, "options", WidgetOptions),
            visualization=_from_dict(d, "visualization", LegacyVisualization),
            width=d.get("width", None),
        )


@dataclass
class WidgetOptions:
    created_at: Optional[str] = None
    """Timestamp when this object was created"""

    description: Optional[str] = None
    """Custom description of the widget"""

    is_hidden: Optional[bool] = None
    """Whether this widget is hidden on the dashboard."""

    parameter_mappings: Optional[Any] = None
    """How parameters used by the visualization in this widget relate to other widgets on the
    dashboard. Databricks does not recommend modifying this definition in JSON."""

    position: Optional[WidgetPosition] = None
    """Coordinates of this widget on a dashboard. This portion of the API changes frequently and is
    unsupported."""

    title: Optional[str] = None
    """Custom title of the widget"""

    updated_at: Optional[str] = None
    """Timestamp of the last time this object was updated."""

    def as_dict(self) -> dict:
        """Serializes the WidgetOptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.description is not None:
            body["description"] = self.description
        if self.is_hidden is not None:
            body["isHidden"] = self.is_hidden
        if self.parameter_mappings:
            body["parameterMappings"] = self.parameter_mappings
        if self.position:
            body["position"] = self.position.as_dict()
        if self.title is not None:
            body["title"] = self.title
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WidgetOptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.description is not None:
            body["description"] = self.description
        if self.is_hidden is not None:
            body["isHidden"] = self.is_hidden
        if self.parameter_mappings:
            body["parameterMappings"] = self.parameter_mappings
        if self.position:
            body["position"] = self.position
        if self.title is not None:
            body["title"] = self.title
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WidgetOptions:
        """Deserializes the WidgetOptions from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            description=d.get("description", None),
            is_hidden=d.get("isHidden", None),
            parameter_mappings=d.get("parameterMappings", None),
            position=_from_dict(d, "position", WidgetPosition),
            title=d.get("title", None),
            updated_at=d.get("updated_at", None),
        )


@dataclass
class WidgetPosition:
    """Coordinates of this widget on a dashboard. This portion of the API changes frequently and is
    unsupported."""

    auto_height: Optional[bool] = None
    """reserved for internal use"""

    col: Optional[int] = None
    """column in the dashboard grid. Values start with 0"""

    row: Optional[int] = None
    """row in the dashboard grid. Values start with 0"""

    size_x: Optional[int] = None
    """width of the widget measured in dashboard grid cells"""

    size_y: Optional[int] = None
    """height of the widget measured in dashboard grid cells"""

    def as_dict(self) -> dict:
        """Serializes the WidgetPosition into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.auto_height is not None:
            body["autoHeight"] = self.auto_height
        if self.col is not None:
            body["col"] = self.col
        if self.row is not None:
            body["row"] = self.row
        if self.size_x is not None:
            body["sizeX"] = self.size_x
        if self.size_y is not None:
            body["sizeY"] = self.size_y
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WidgetPosition into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.auto_height is not None:
            body["autoHeight"] = self.auto_height
        if self.col is not None:
            body["col"] = self.col
        if self.row is not None:
            body["row"] = self.row
        if self.size_x is not None:
            body["sizeX"] = self.size_x
        if self.size_y is not None:
            body["sizeY"] = self.size_y
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WidgetPosition:
        """Deserializes the WidgetPosition from a dictionary."""
        return cls(
            auto_height=d.get("autoHeight", None),
            col=d.get("col", None),
            row=d.get("row", None),
            size_x=d.get("sizeX", None),
            size_y=d.get("sizeY", None),
        )


class AlertsAPI:
    """The alerts API can be used to perform CRUD operations on alerts. An alert is a Databricks SQL object that
    periodically runs a query, evaluates a condition of its result, and notifies one or more users and/or
    notification destinations if the condition was met. Alerts can be scheduled using the `sql_task` type of
    the Jobs API, e.g. :method:jobs/create."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, *, alert: Optional[CreateAlertRequestAlert] = None, auto_resolve_display_name: Optional[bool] = None
    ) -> Alert:
        """Creates an alert.

        :param alert: :class:`CreateAlertRequestAlert` (optional)
        :param auto_resolve_display_name: bool (optional)
          If true, automatically resolve alert display name conflicts. Otherwise, fail the request if the
          alert's display name conflicts with an existing alert's display name.

        :returns: :class:`Alert`
        """

        body = {}
        if alert is not None:
            body["alert"] = alert.as_dict()
        if auto_resolve_display_name is not None:
            body["auto_resolve_display_name"] = auto_resolve_display_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/sql/alerts", body=body, headers=headers)
        return Alert.from_dict(res)

    def delete(self, id: str):
        """Moves an alert to the trash. Trashed alerts immediately disappear from searches and list views, and
        can no longer trigger. You can restore a trashed alert through the UI. A trashed alert is permanently
        deleted after 30 days.

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/sql/alerts/{id}", headers=headers)

    def get(self, id: str) -> Alert:
        """Gets an alert.

        :param id: str

        :returns: :class:`Alert`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/sql/alerts/{id}", headers=headers)
        return Alert.from_dict(res)

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ListAlertsResponseAlert]:
        """Gets a list of alerts accessible to the user, ordered by creation time. **Warning:** Calling this API
        concurrently 10 or more times could result in throttling, service degradation, or a temporary ban.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ListAlertsResponseAlert`
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
            json = self._api.do("GET", "/api/2.0/sql/alerts", query=query, headers=headers)
            if "results" in json:
                for v in json["results"]:
                    yield ListAlertsResponseAlert.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        id: str,
        update_mask: str,
        *,
        alert: Optional[UpdateAlertRequestAlert] = None,
        auto_resolve_display_name: Optional[bool] = None,
    ) -> Alert:
        """Updates an alert.

        :param id: str
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.
        :param alert: :class:`UpdateAlertRequestAlert` (optional)
        :param auto_resolve_display_name: bool (optional)
          If true, automatically resolve alert display name conflicts. Otherwise, fail the request if the
          alert's display name conflicts with an existing alert's display name.

        :returns: :class:`Alert`
        """

        body = {}
        if alert is not None:
            body["alert"] = alert.as_dict()
        if auto_resolve_display_name is not None:
            body["auto_resolve_display_name"] = auto_resolve_display_name
        if update_mask is not None:
            body["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/sql/alerts/{id}", body=body, headers=headers)
        return Alert.from_dict(res)


class AlertsLegacyAPI:
    """The alerts API can be used to perform CRUD operations on alerts. An alert is a Databricks SQL object that
    periodically runs a query, evaluates a condition of its result, and notifies one or more users and/or
    notification destinations if the condition was met. Alerts can be scheduled using the `sql_task` type of
    the Jobs API, e.g. :method:jobs/create.

    **Warning**: This API is deprecated. Please see the latest version of the Databricks SQL API. [Learn more]

    [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html"""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        name: str,
        options: AlertOptions,
        query_id: str,
        *,
        parent: Optional[str] = None,
        rearm: Optional[int] = None,
    ) -> LegacyAlert:
        """Creates an alert. An alert is a Databricks SQL object that periodically runs a query, evaluates a
        condition of its result, and notifies users or notification destinations if the condition was met.

        **Warning**: This API is deprecated. Please use :method:alerts/create instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param name: str
          Name of the alert.
        :param options: :class:`AlertOptions`
          Alert configuration options.
        :param query_id: str
          Query ID.
        :param parent: str (optional)
          The identifier of the workspace folder containing the object.
        :param rearm: int (optional)
          Number of seconds after being triggered before the alert rearms itself and can be triggered again.
          If `null`, alert will never be triggered again.

        :returns: :class:`LegacyAlert`
        """

        body = {}
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options.as_dict()
        if parent is not None:
            body["parent"] = parent
        if query_id is not None:
            body["query_id"] = query_id
        if rearm is not None:
            body["rearm"] = rearm
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/preview/sql/alerts", body=body, headers=headers)
        return LegacyAlert.from_dict(res)

    def delete(self, alert_id: str):
        """Deletes an alert. Deleted alerts are no longer accessible and cannot be restored. **Note**: Unlike
        queries and dashboards, alerts cannot be moved to the trash.

        **Warning**: This API is deprecated. Please use :method:alerts/delete instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param alert_id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/preview/sql/alerts/{alert_id}", headers=headers)

    def get(self, alert_id: str) -> LegacyAlert:
        """Gets an alert.

        **Warning**: This API is deprecated. Please use :method:alerts/get instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param alert_id: str

        :returns: :class:`LegacyAlert`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/preview/sql/alerts/{alert_id}", headers=headers)
        return LegacyAlert.from_dict(res)

    def list(self) -> Iterator[LegacyAlert]:
        """Gets a list of alerts.

        **Warning**: This API is deprecated. Please use :method:alerts/list instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html


        :returns: Iterator over :class:`LegacyAlert`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/preview/sql/alerts", headers=headers)
        return [LegacyAlert.from_dict(v) for v in res]

    def update(self, alert_id: str, name: str, options: AlertOptions, query_id: str, *, rearm: Optional[int] = None):
        """Updates an alert.

        **Warning**: This API is deprecated. Please use :method:alerts/update instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param alert_id: str
        :param name: str
          Name of the alert.
        :param options: :class:`AlertOptions`
          Alert configuration options.
        :param query_id: str
          Query ID.
        :param rearm: int (optional)
          Number of seconds after being triggered before the alert rearms itself and can be triggered again.
          If `null`, alert will never be triggered again.


        """

        body = {}
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options.as_dict()
        if query_id is not None:
            body["query_id"] = query_id
        if rearm is not None:
            body["rearm"] = rearm
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PUT", f"/api/2.0/preview/sql/alerts/{alert_id}", body=body, headers=headers)


class AlertsV2API:
    """New version of SQL Alerts"""

    def __init__(self, api_client):
        self._api = api_client

    def create_alert(self, alert: AlertV2) -> AlertV2:
        """Create Alert

        :param alert: :class:`AlertV2`

        :returns: :class:`AlertV2`
        """

        body = alert.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/alerts", body=body, headers=headers)
        return AlertV2.from_dict(res)

    def get_alert(self, id: str) -> AlertV2:
        """Gets an alert.

        :param id: str

        :returns: :class:`AlertV2`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/alerts/{id}", headers=headers)
        return AlertV2.from_dict(res)

    def list_alerts(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[AlertV2]:
        """Gets a list of alerts accessible to the user, ordered by creation time.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`AlertV2`
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
            json = self._api.do("GET", "/api/2.0/alerts", query=query, headers=headers)
            if "alerts" in json:
                for v in json["alerts"]:
                    yield AlertV2.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def trash_alert(self, id: str, *, purge: Optional[bool] = None):
        """Moves an alert to the trash. Trashed alerts immediately disappear from list views, and can no longer
        trigger. You can restore a trashed alert through the UI. A trashed alert is permanently deleted after
        30 days.

        :param id: str
        :param purge: bool (optional)
          Whether to permanently delete the alert. If not set, the alert will only be soft deleted.


        """

        query = {}
        if purge is not None:
            query["purge"] = purge
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/alerts/{id}", query=query, headers=headers)

    def update_alert(self, id: str, alert: AlertV2, update_mask: str) -> AlertV2:
        """Update alert

        :param id: str
          UUID identifying the alert.
        :param alert: :class:`AlertV2`
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`AlertV2`
        """

        body = alert.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.0/alerts/{id}", query=query, body=body, headers=headers)
        return AlertV2.from_dict(res)


class DashboardWidgetsAPI:
    """This is an evolving API that facilitates the addition and removal of widgets from existing dashboards
    within the Databricks Workspace. Data structures may change over time."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        dashboard_id: str,
        options: WidgetOptions,
        width: int,
        *,
        text: Optional[str] = None,
        visualization_id: Optional[str] = None,
    ) -> Widget:
        """Adds a widget to a dashboard

        :param dashboard_id: str
          Dashboard ID returned by :method:dashboards/create.
        :param options: :class:`WidgetOptions`
        :param width: int
          Width of a widget
        :param text: str (optional)
          If this is a textbox widget, the application displays this text. This field is ignored if the widget
          contains a visualization in the `visualization` field.
        :param visualization_id: str (optional)
          Query Vizualization ID returned by :method:queryvisualizations/create.

        :returns: :class:`Widget`
        """

        body = {}
        if dashboard_id is not None:
            body["dashboard_id"] = dashboard_id
        if options is not None:
            body["options"] = options.as_dict()
        if text is not None:
            body["text"] = text
        if visualization_id is not None:
            body["visualization_id"] = visualization_id
        if width is not None:
            body["width"] = width
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/preview/sql/widgets", body=body, headers=headers)
        return Widget.from_dict(res)

    def delete(self, id: str):
        """Removes a widget from a dashboard

        :param id: str
          Widget ID returned by :method:dashboardwidgets/create


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/preview/sql/widgets/{id}", headers=headers)

    def update(
        self,
        id: str,
        dashboard_id: str,
        options: WidgetOptions,
        width: int,
        *,
        text: Optional[str] = None,
        visualization_id: Optional[str] = None,
    ) -> Widget:
        """Updates an existing widget

        :param id: str
          Widget ID returned by :method:dashboardwidgets/create
        :param dashboard_id: str
          Dashboard ID returned by :method:dashboards/create.
        :param options: :class:`WidgetOptions`
        :param width: int
          Width of a widget
        :param text: str (optional)
          If this is a textbox widget, the application displays this text. This field is ignored if the widget
          contains a visualization in the `visualization` field.
        :param visualization_id: str (optional)
          Query Vizualization ID returned by :method:queryvisualizations/create.

        :returns: :class:`Widget`
        """

        body = {}
        if dashboard_id is not None:
            body["dashboard_id"] = dashboard_id
        if options is not None:
            body["options"] = options.as_dict()
        if text is not None:
            body["text"] = text
        if visualization_id is not None:
            body["visualization_id"] = visualization_id
        if width is not None:
            body["width"] = width
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/preview/sql/widgets/{id}", body=body, headers=headers)
        return Widget.from_dict(res)


class DashboardsAPI:
    """In general, there is little need to modify dashboards using the API. However, it can be useful to use
    dashboard objects to look-up a collection of related query IDs. The API can also be used to duplicate
    multiple dashboards at once since you can get a dashboard definition with a GET request and then POST it
    to create a new one. Dashboards can be scheduled using the `sql_task` type of the Jobs API, e.g.
    :method:jobs/create.

    **Warning**: This API is deprecated. Please use the AI/BI Dashboards API instead. [Learn more]

    [Learn more]: https://docs.databricks.com/en/dashboards/"""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, dashboard_id: str):
        """Moves a dashboard to the trash. Trashed dashboards do not appear in list views or searches, and cannot
        be shared.

        **Warning**: This API is deprecated. Please use the AI/BI Dashboards API instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/dashboards/

        :param dashboard_id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/preview/sql/dashboards/{dashboard_id}", headers=headers)

    def get(self, dashboard_id: str) -> Dashboard:
        """Returns a JSON representation of a dashboard object, including its visualization and query objects.

        **Warning**: This API is deprecated. Please use the AI/BI Dashboards API instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/dashboards/

        :param dashboard_id: str

        :returns: :class:`Dashboard`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/preview/sql/dashboards/{dashboard_id}", headers=headers)
        return Dashboard.from_dict(res)

    def list(
        self,
        *,
        order: Optional[ListOrder] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        q: Optional[str] = None,
    ) -> Iterator[Dashboard]:
        """Fetch a paginated list of dashboard objects.

        **Warning**: Calling this API concurrently 10 or more times could result in throttling, service
        degradation, or a temporary ban.

        **Warning**: This API is deprecated. Please use the AI/BI Dashboards API instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/dashboards/

        :param order: :class:`ListOrder` (optional)
          Name of dashboard attribute to order by.
        :param page: int (optional)
          Page number to retrieve.
        :param page_size: int (optional)
          Number of dashboards to return per page.
        :param q: str (optional)
          Full text search term.

        :returns: Iterator over :class:`Dashboard`
        """

        query = {}
        if order is not None:
            query["order"] = order.value
        if page is not None:
            query["page"] = page
        if page_size is not None:
            query["page_size"] = page_size
        if q is not None:
            query["q"] = q
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        query["page"] = 1
        while True:
            json = self._api.do("GET", "/api/2.0/preview/sql/dashboards", query=query, headers=headers)
            if "results" in json:
                for v in json["results"]:
                    yield Dashboard.from_dict(v)
            if "results" not in json or not json["results"]:
                return
            query["page"] += 1

    def restore(self, dashboard_id: str):
        """A restored dashboard appears in list views and searches and can be shared.

        **Warning**: This API is deprecated. Please use the AI/BI Dashboards API instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/dashboards/

        :param dashboard_id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", f"/api/2.0/preview/sql/dashboards/trash/{dashboard_id}", headers=headers)

    def update(
        self,
        dashboard_id: str,
        *,
        name: Optional[str] = None,
        run_as_role: Optional[RunAsRole] = None,
        tags: Optional[List[str]] = None,
    ) -> Dashboard:
        """Modify this dashboard definition. This operation only affects attributes of the dashboard object. It
        does not add, modify, or remove widgets.

        **Note**: You cannot undo this operation.

        **Warning**: This API is deprecated. Please use the AI/BI Dashboards API instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/dashboards/

        :param dashboard_id: str
        :param name: str (optional)
          The title of this dashboard that appears in list views and at the top of the dashboard page.
        :param run_as_role: :class:`RunAsRole` (optional)
          Sets the **Run as** role for the object. Must be set to one of `"viewer"` (signifying "run as
          viewer" behavior) or `"owner"` (signifying "run as owner" behavior)
        :param tags: List[str] (optional)

        :returns: :class:`Dashboard`
        """

        body = {}
        if name is not None:
            body["name"] = name
        if run_as_role is not None:
            body["run_as_role"] = run_as_role.value
        if tags is not None:
            body["tags"] = [v for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/preview/sql/dashboards/{dashboard_id}", body=body, headers=headers)
        return Dashboard.from_dict(res)


class DataSourcesAPI:
    """This API is provided to assist you in making new query objects. When creating a query object, you may
    optionally specify a `data_source_id` for the SQL warehouse against which it will run. If you don't
    already know the `data_source_id` for your desired SQL warehouse, this API will help you find it.

    This API does not support searches. It returns the full list of SQL warehouses in your workspace. We
    advise you to use any text editor, REST client, or `grep` to search the response from this API for the
    name of your SQL warehouse as it appears in Databricks SQL.

    **Warning**: This API is deprecated. Please see the latest version of the Databricks SQL API. [Learn more]

    [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html"""

    def __init__(self, api_client):
        self._api = api_client

    def list(self) -> Iterator[DataSource]:
        """Retrieves a full list of SQL warehouses available in this workspace. All fields that appear in this
        API response are enumerated for clarity. However, you need only a SQL warehouse's `id` to create new
        queries against it.

        **Warning**: This API is deprecated. Please use :method:warehouses/list instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html


        :returns: Iterator over :class:`DataSource`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/preview/sql/data_sources", headers=headers)
        return [DataSource.from_dict(v) for v in res]


class DbsqlPermissionsAPI:
    """The SQL Permissions API is similar to the endpoints of the :method:permissions/set. However, this exposes
    only one endpoint, which gets the Access Control List for a given object. You cannot modify any
    permissions using this API.

    There are three levels of permission:

    - `CAN_VIEW`: Allows read-only access

    - `CAN_RUN`: Allows read access and run access (superset of `CAN_VIEW`)

    - `CAN_MANAGE`: Allows all actions: read, run, edit, delete, modify permissions (superset of `CAN_RUN`)

    **Warning**: This API is deprecated. Please see the latest version of the Databricks SQL API. [Learn more]

    [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html"""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, object_type: ObjectTypePlural, object_id: str) -> GetResponse:
        """Gets a JSON representation of the access control list (ACL) for a specified object.

        **Warning**: This API is deprecated. Please use :method:workspace/getpermissions instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param object_type: :class:`ObjectTypePlural`
          The type of object permissions to check.
        :param object_id: str
          Object ID. An ACL is returned for the object with this UUID.

        :returns: :class:`GetResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/preview/sql/permissions/{object_type.value}/{object_id}", headers=headers)
        return GetResponse.from_dict(res)

    def set(
        self,
        object_type: ObjectTypePlural,
        object_id: str,
        *,
        access_control_list: Optional[List[AccessControl]] = None,
    ) -> SetResponse:
        """Sets the access control list (ACL) for a specified object. This operation will complete rewrite the
        ACL.

        **Warning**: This API is deprecated. Please use :method:workspace/setpermissions instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param object_type: :class:`ObjectTypePlural`
          The type of object permission to set.
        :param object_id: str
          Object ID. The ACL for the object with this UUID is overwritten by this request's POST content.
        :param access_control_list: List[:class:`AccessControl`] (optional)

        :returns: :class:`SetResponse`
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
            "POST", f"/api/2.0/preview/sql/permissions/{object_type.value}/{object_id}", body=body, headers=headers
        )
        return SetResponse.from_dict(res)

    def transfer_ownership(
        self, object_type: OwnableObjectType, object_id: TransferOwnershipObjectId, *, new_owner: Optional[str] = None
    ) -> Success:
        """Transfers ownership of a dashboard, query, or alert to an active user. Requires an admin API key.

        **Warning**: This API is deprecated. For queries and alerts, please use :method:queries/update and
        :method:alerts/update respectively instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param object_type: :class:`OwnableObjectType`
          The type of object on which to change ownership.
        :param object_id: :class:`TransferOwnershipObjectId`
          The ID of the object on which to change ownership.
        :param new_owner: str (optional)
          Email address for the new owner, who must exist in the workspace.

        :returns: :class:`Success`
        """

        body = {}
        if new_owner is not None:
            body["new_owner"] = new_owner
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/2.0/preview/sql/permissions/{object_type.value}/{object_id}/transfer",
            body=body,
            headers=headers,
        )
        return Success.from_dict(res)


class QueriesAPI:
    """The queries API can be used to perform CRUD operations on queries. A query is a Databricks SQL object that
    includes the target SQL warehouse, query text, name, description, tags, and parameters. Queries can be
    scheduled using the `sql_task` type of the Jobs API, e.g. :method:jobs/create."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, *, auto_resolve_display_name: Optional[bool] = None, query: Optional[CreateQueryRequestQuery] = None
    ) -> Query:
        """Creates a query.

        :param auto_resolve_display_name: bool (optional)
          If true, automatically resolve query display name conflicts. Otherwise, fail the request if the
          query's display name conflicts with an existing query's display name.
        :param query: :class:`CreateQueryRequestQuery` (optional)

        :returns: :class:`Query`
        """

        body = {}
        if auto_resolve_display_name is not None:
            body["auto_resolve_display_name"] = auto_resolve_display_name
        if query is not None:
            body["query"] = query.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/sql/queries", body=body, headers=headers)
        return Query.from_dict(res)

    def delete(self, id: str):
        """Moves a query to the trash. Trashed queries immediately disappear from searches and list views, and
        cannot be used for alerts. You can restore a trashed query through the UI. A trashed query is
        permanently deleted after 30 days.

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/sql/queries/{id}", headers=headers)

    def get(self, id: str) -> Query:
        """Gets a query.

        :param id: str

        :returns: :class:`Query`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/sql/queries/{id}", headers=headers)
        return Query.from_dict(res)

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ListQueryObjectsResponseQuery]:
        """Gets a list of queries accessible to the user, ordered by creation time. **Warning:** Calling this API
        concurrently 10 or more times could result in throttling, service degradation, or a temporary ban.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ListQueryObjectsResponseQuery`
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
            json = self._api.do("GET", "/api/2.0/sql/queries", query=query, headers=headers)
            if "results" in json:
                for v in json["results"]:
                    yield ListQueryObjectsResponseQuery.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_visualizations(
        self, id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[Visualization]:
        """Gets a list of visualizations on a query.

        :param id: str
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`Visualization`
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
            json = self._api.do("GET", f"/api/2.0/sql/queries/{id}/visualizations", query=query, headers=headers)
            if "results" in json:
                for v in json["results"]:
                    yield Visualization.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        id: str,
        update_mask: str,
        *,
        auto_resolve_display_name: Optional[bool] = None,
        query: Optional[UpdateQueryRequestQuery] = None,
    ) -> Query:
        """Updates a query.

        :param id: str
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.
        :param auto_resolve_display_name: bool (optional)
          If true, automatically resolve alert display name conflicts. Otherwise, fail the request if the
          alert's display name conflicts with an existing alert's display name.
        :param query: :class:`UpdateQueryRequestQuery` (optional)

        :returns: :class:`Query`
        """

        body = {}
        if auto_resolve_display_name is not None:
            body["auto_resolve_display_name"] = auto_resolve_display_name
        if query is not None:
            body["query"] = query.as_dict()
        if update_mask is not None:
            body["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/sql/queries/{id}", body=body, headers=headers)
        return Query.from_dict(res)


class QueriesLegacyAPI:
    """These endpoints are used for CRUD operations on query definitions. Query definitions include the target
    SQL warehouse, query text, name, description, tags, parameters, and visualizations. Queries can be
    scheduled using the `sql_task` type of the Jobs API, e.g. :method:jobs/create.

    **Warning**: This API is deprecated. Please see the latest version of the Databricks SQL API. [Learn more]

    [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html"""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        *,
        data_source_id: Optional[str] = None,
        description: Optional[str] = None,
        name: Optional[str] = None,
        options: Optional[Any] = None,
        parent: Optional[str] = None,
        query: Optional[str] = None,
        run_as_role: Optional[RunAsRole] = None,
        tags: Optional[List[str]] = None,
    ) -> LegacyQuery:
        """Creates a new query definition. Queries created with this endpoint belong to the authenticated user
        making the request.

        The `data_source_id` field specifies the ID of the SQL warehouse to run this query against. You can
        use the Data Sources API to see a complete list of available SQL warehouses. Or you can copy the
        `data_source_id` from an existing query.

        **Note**: You cannot add a visualization until you create the query.

        **Warning**: This API is deprecated. Please use :method:queries/create instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param data_source_id: str (optional)
          Data source ID maps to the ID of the data source used by the resource and is distinct from the
          warehouse ID. [Learn more]

          [Learn more]: https://docs.databricks.com/api/workspace/datasources/list
        :param description: str (optional)
          General description that conveys additional information about this query such as usage notes.
        :param name: str (optional)
          The title of this query that appears in list views, widget headings, and on the query page.
        :param options: Any (optional)
          Exclusively used for storing a list parameter definitions. A parameter is an object with `title`,
          `name`, `type`, and `value` properties. The `value` field here is the default value. It can be
          overridden at runtime.
        :param parent: str (optional)
          The identifier of the workspace folder containing the object.
        :param query: str (optional)
          The text of the query to be run.
        :param run_as_role: :class:`RunAsRole` (optional)
          Sets the **Run as** role for the object. Must be set to one of `"viewer"` (signifying "run as
          viewer" behavior) or `"owner"` (signifying "run as owner" behavior)
        :param tags: List[str] (optional)

        :returns: :class:`LegacyQuery`
        """

        body = {}
        if data_source_id is not None:
            body["data_source_id"] = data_source_id
        if description is not None:
            body["description"] = description
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options
        if parent is not None:
            body["parent"] = parent
        if query is not None:
            body["query"] = query
        if run_as_role is not None:
            body["run_as_role"] = run_as_role.value
        if tags is not None:
            body["tags"] = [v for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/preview/sql/queries", body=body, headers=headers)
        return LegacyQuery.from_dict(res)

    def delete(self, query_id: str):
        """Moves a query to the trash. Trashed queries immediately disappear from searches and list views, and
        they cannot be used for alerts. The trash is deleted after 30 days.

        **Warning**: This API is deprecated. Please use :method:queries/delete instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param query_id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/preview/sql/queries/{query_id}", headers=headers)

    def get(self, query_id: str) -> LegacyQuery:
        """Retrieve a query object definition along with contextual permissions information about the currently
        authenticated user.

        **Warning**: This API is deprecated. Please use :method:queries/get instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param query_id: str

        :returns: :class:`LegacyQuery`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/preview/sql/queries/{query_id}", headers=headers)
        return LegacyQuery.from_dict(res)

    def list(
        self,
        *,
        order: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        q: Optional[str] = None,
    ) -> Iterator[LegacyQuery]:
        """Gets a list of queries. Optionally, this list can be filtered by a search term.

        **Warning**: Calling this API concurrently 10 or more times could result in throttling, service
        degradation, or a temporary ban.

        **Warning**: This API is deprecated. Please use :method:queries/list instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param order: str (optional)
          Name of query attribute to order by. Default sort order is ascending. Append a dash (`-`) to order
          descending instead.

          - `name`: The name of the query.

          - `created_at`: The timestamp the query was created.

          - `runtime`: The time it took to run this query. This is blank for parameterized queries. A blank
          value is treated as the highest value for sorting.

          - `executed_at`: The timestamp when the query was last run.

          - `created_by`: The user name of the user that created the query.
        :param page: int (optional)
          Page number to retrieve.
        :param page_size: int (optional)
          Number of queries to return per page.
        :param q: str (optional)
          Full text search term

        :returns: Iterator over :class:`LegacyQuery`
        """

        query = {}
        if order is not None:
            query["order"] = order
        if page is not None:
            query["page"] = page
        if page_size is not None:
            query["page_size"] = page_size
        if q is not None:
            query["q"] = q
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        query["page"] = 1
        while True:
            json = self._api.do("GET", "/api/2.0/preview/sql/queries", query=query, headers=headers)
            if "results" in json:
                for v in json["results"]:
                    yield LegacyQuery.from_dict(v)
            if "results" not in json or not json["results"]:
                return
            query["page"] += 1

    def restore(self, query_id: str):
        """Restore a query that has been moved to the trash. A restored query appears in list views and searches.
        You can use restored queries for alerts.

        **Warning**: This API is deprecated. Please see the latest version. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param query_id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", f"/api/2.0/preview/sql/queries/trash/{query_id}", headers=headers)

    def update(
        self,
        query_id: str,
        *,
        data_source_id: Optional[str] = None,
        description: Optional[str] = None,
        name: Optional[str] = None,
        options: Optional[Any] = None,
        query: Optional[str] = None,
        run_as_role: Optional[RunAsRole] = None,
        tags: Optional[List[str]] = None,
    ) -> LegacyQuery:
        """Modify this query definition.

        **Note**: You cannot undo this operation.

        **Warning**: This API is deprecated. Please use :method:queries/update instead. [Learn more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param query_id: str
        :param data_source_id: str (optional)
          Data source ID maps to the ID of the data source used by the resource and is distinct from the
          warehouse ID. [Learn more]

          [Learn more]: https://docs.databricks.com/api/workspace/datasources/list
        :param description: str (optional)
          General description that conveys additional information about this query such as usage notes.
        :param name: str (optional)
          The title of this query that appears in list views, widget headings, and on the query page.
        :param options: Any (optional)
          Exclusively used for storing a list parameter definitions. A parameter is an object with `title`,
          `name`, `type`, and `value` properties. The `value` field here is the default value. It can be
          overridden at runtime.
        :param query: str (optional)
          The text of the query to be run.
        :param run_as_role: :class:`RunAsRole` (optional)
          Sets the **Run as** role for the object. Must be set to one of `"viewer"` (signifying "run as
          viewer" behavior) or `"owner"` (signifying "run as owner" behavior)
        :param tags: List[str] (optional)

        :returns: :class:`LegacyQuery`
        """

        body = {}
        if data_source_id is not None:
            body["data_source_id"] = data_source_id
        if description is not None:
            body["description"] = description
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options
        if query is not None:
            body["query"] = query
        if run_as_role is not None:
            body["run_as_role"] = run_as_role.value
        if tags is not None:
            body["tags"] = [v for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/preview/sql/queries/{query_id}", body=body, headers=headers)
        return LegacyQuery.from_dict(res)


class QueryHistoryAPI:
    """A service responsible for storing and retrieving the list of queries run against SQL endpoints and
    serverless compute."""

    def __init__(self, api_client):
        self._api = api_client

    def list(
        self,
        *,
        filter_by: Optional[QueryFilter] = None,
        include_metrics: Optional[bool] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> ListQueriesResponse:
        """List the history of queries through SQL warehouses, and serverless compute.

        You can filter by user ID, warehouse ID, status, and time range. Most recently started queries are
        returned first (up to max_results in request). The pagination token returned in response can be used
        to list subsequent query statuses.

        :param filter_by: :class:`QueryFilter` (optional)
          An optional filter object to limit query history results. Accepts parameters such as user IDs,
          endpoint IDs, and statuses to narrow the returned data. In a URL, the parameters of this filter are
          specified with dot notation. For example: `filter_by.statement_ids`.
        :param include_metrics: bool (optional)
          Whether to include the query metrics with each query. Only use this for a small subset of queries
          (max_results). Defaults to false.
        :param max_results: int (optional)
          Limit the number of results returned in one page. Must be less than 1000 and the default is 100.
        :param page_token: str (optional)
          A token that can be used to get the next page of results. The token can contains characters that
          need to be encoded before using it in a URL. For example, the character '+' needs to be replaced by
          %2B. This field is optional.

        :returns: :class:`ListQueriesResponse`
        """

        query = {}
        if filter_by is not None:
            query["filter_by"] = filter_by.as_dict()
        if include_metrics is not None:
            query["include_metrics"] = include_metrics
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

        res = self._api.do("GET", "/api/2.0/sql/history/queries", query=query, headers=headers)
        return ListQueriesResponse.from_dict(res)


class QueryVisualizationsAPI:
    """This is an evolving API that facilitates the addition and removal of visualizations from existing queries
    in the Databricks Workspace. Data structures can change over time."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, *, visualization: Optional[CreateVisualizationRequestVisualization] = None) -> Visualization:
        """Adds a visualization to a query.

        :param visualization: :class:`CreateVisualizationRequestVisualization` (optional)

        :returns: :class:`Visualization`
        """

        body = {}
        if visualization is not None:
            body["visualization"] = visualization.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/sql/visualizations", body=body, headers=headers)
        return Visualization.from_dict(res)

    def delete(self, id: str):
        """Removes a visualization.

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/sql/visualizations/{id}", headers=headers)

    def update(
        self, id: str, update_mask: str, *, visualization: Optional[UpdateVisualizationRequestVisualization] = None
    ) -> Visualization:
        """Updates a visualization.

        :param id: str
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.
        :param visualization: :class:`UpdateVisualizationRequestVisualization` (optional)

        :returns: :class:`Visualization`
        """

        body = {}
        if update_mask is not None:
            body["update_mask"] = update_mask
        if visualization is not None:
            body["visualization"] = visualization.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/sql/visualizations/{id}", body=body, headers=headers)
        return Visualization.from_dict(res)


class QueryVisualizationsLegacyAPI:
    """This is an evolving API that facilitates the addition and removal of vizualisations from existing queries
    within the Databricks Workspace. Data structures may change over time.

    **Warning**: This API is deprecated. Please see the latest version of the Databricks SQL API. [Learn more]

    [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html"""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, options: Any, query_id: str, type: str, *, description: Optional[str] = None, name: Optional[str] = None
    ) -> LegacyVisualization:
        """Creates visualization in the query.

        **Warning**: This API is deprecated. Please use :method:queryvisualizations/create instead. [Learn
        more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param options: Any
          The options object varies widely from one visualization type to the next and is unsupported.
          Databricks does not recommend modifying visualization settings in JSON.
        :param query_id: str
          The identifier returned by :method:queries/create
        :param type: str
          The type of visualization: chart, table, pivot table, and so on.
        :param description: str (optional)
          A short description of this visualization. This is not displayed in the UI.
        :param name: str (optional)
          The name of the visualization that appears on dashboards and the query screen.

        :returns: :class:`LegacyVisualization`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options
        if query_id is not None:
            body["query_id"] = query_id
        if type is not None:
            body["type"] = type
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/preview/sql/visualizations", body=body, headers=headers)
        return LegacyVisualization.from_dict(res)

    def delete(self, id: str):
        """Removes a visualization from the query.

        **Warning**: This API is deprecated. Please use :method:queryvisualizations/delete instead. [Learn
        more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param id: str
          Widget ID returned by :method:queryvisualizations/create


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/preview/sql/visualizations/{id}", headers=headers)

    def update(
        self,
        *,
        created_at: Optional[str] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
        options: Optional[Any] = None,
        query: Optional[LegacyQuery] = None,
        type: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> LegacyVisualization:
        """Updates visualization in the query.

        **Warning**: This API is deprecated. Please use :method:queryvisualizations/update instead. [Learn
        more]

        [Learn more]: https://docs.databricks.com/en/sql/dbsql-api-latest.html

        :param created_at: str (optional)
        :param description: str (optional)
          A short description of this visualization. This is not displayed in the UI.
        :param id: str (optional)
          The UUID for this visualization.
        :param name: str (optional)
          The name of the visualization that appears on dashboards and the query screen.
        :param options: Any (optional)
          The options object varies widely from one visualization type to the next and is unsupported.
          Databricks does not recommend modifying visualization settings in JSON.
        :param query: :class:`LegacyQuery` (optional)
        :param type: str (optional)
          The type of visualization: chart, table, pivot table, and so on.
        :param updated_at: str (optional)

        :returns: :class:`LegacyVisualization`
        """

        body = {}
        if created_at is not None:
            body["created_at"] = created_at
        if description is not None:
            body["description"] = description
        if id is not None:
            body["id"] = id
        if name is not None:
            body["name"] = name
        if options is not None:
            body["options"] = options
        if query is not None:
            body["query"] = query.as_dict()
        if type is not None:
            body["type"] = type
        if updated_at is not None:
            body["updated_at"] = updated_at
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/preview/sql/visualizations/{id}", body=body, headers=headers)
        return LegacyVisualization.from_dict(res)


class RedashConfigAPI:
    """Redash V2 service for workspace configurations (internal)"""

    def __init__(self, api_client):
        self._api = api_client

    def get_config(self) -> ClientConfig:
        """Read workspace configuration for Redash-v2.


        :returns: :class:`ClientConfig`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/redash-v2/config", headers=headers)
        return ClientConfig.from_dict(res)


class StatementExecutionAPI:
    """The Databricks SQL Statement Execution API can be used to execute SQL statements on a SQL warehouse and
    fetch the result.

    **Getting started**

    We suggest beginning with the [Databricks SQL Statement Execution API tutorial].

    **Overview of statement execution and result fetching**

    Statement execution begins by issuing a :method:statementexecution/executeStatement request with a valid
    SQL statement and warehouse ID, along with optional parameters such as the data catalog and output format.
    If no other parameters are specified, the server will wait for up to 10s before returning a response. If
    the statement has completed within this timespan, the response will include the result data as a JSON
    array and metadata. Otherwise, if no result is available after the 10s timeout expired, the response will
    provide the statement ID that can be used to poll for results by using a
    :method:statementexecution/getStatement request.

    You can specify whether the call should behave synchronously, asynchronously or start synchronously with a
    fallback to asynchronous execution. This is controlled with the `wait_timeout` and `on_wait_timeout`
    settings. If `wait_timeout` is set between 5-50 seconds (default: 10s), the call waits for results up to
    the specified timeout; when set to `0s`, the call is asynchronous and responds immediately with a
    statement ID. The `on_wait_timeout` setting specifies what should happen when the timeout is reached while
    the statement execution has not yet finished. This can be set to either `CONTINUE`, to fallback to
    asynchronous mode, or it can be set to `CANCEL`, which cancels the statement.

    In summary: - **Synchronous mode** (`wait_timeout=30s` and `on_wait_timeout=CANCEL`): The call waits up to
    30 seconds; if the statement execution finishes within this time, the result data is returned directly in
    the response. If the execution takes longer than 30 seconds, the execution is canceled and the call
    returns with a `CANCELED` state. - **Asynchronous mode** (`wait_timeout=0s` and `on_wait_timeout` is
    ignored): The call doesn't wait for the statement to finish but returns directly with a statement ID. The
    status of the statement execution can be polled by issuing :method:statementexecution/getStatement with
    the statement ID. Once the execution has succeeded, this call also returns the result and metadata in the
    response. - **[Default] Hybrid mode** (`wait_timeout=10s` and `on_wait_timeout=CONTINUE`): The call waits
    for up to 10 seconds; if the statement execution finishes within this time, the result data is returned
    directly in the response. If the execution takes longer than 10 seconds, a statement ID is returned. The
    statement ID can be used to fetch status and results in the same way as in the asynchronous mode.

    Depending on the size, the result can be split into multiple chunks. If the statement execution is
    successful, the statement response contains a manifest and the first chunk of the result. The manifest
    contains schema information and provides metadata for each chunk in the result. Result chunks can be
    retrieved by index with :method:statementexecution/getStatementResultChunkN which may be called in any
    order and in parallel. For sequential fetching, each chunk, apart from the last, also contains a
    `next_chunk_index` and `next_chunk_internal_link` that point to the next chunk.

    A statement can be canceled with :method:statementexecution/cancelExecution.

    **Fetching result data: format and disposition**

    To specify the format of the result data, use the `format` field, which can be set to one of the following
    options: `JSON_ARRAY` (JSON), `ARROW_STREAM` ([Apache Arrow Columnar]), or `CSV`.

    There are two ways to receive statement results, controlled by the `disposition` setting, which can be
    either `INLINE` or `EXTERNAL_LINKS`:

    - `INLINE`: In this mode, the result data is directly included in the response. It's best suited for
    smaller results. This mode can only be used with the `JSON_ARRAY` format.

    - `EXTERNAL_LINKS`: In this mode, the response provides links that can be used to download the result data
    in chunks separately. This approach is ideal for larger results and offers higher throughput. This mode
    can be used with all the formats: `JSON_ARRAY`, `ARROW_STREAM`, and `CSV`.

    By default, the API uses `format=JSON_ARRAY` and `disposition=INLINE`.

    **Limits and limitations**

    Note: The byte limit for INLINE disposition is based on internal storage metrics and will not exactly
    match the byte count of the actual payload.

    - Statements with `disposition=INLINE` are limited to 25 MiB and will fail when this limit is exceeded. -
    Statements with `disposition=EXTERNAL_LINKS` are limited to 100 GiB. Result sets larger than this limit
    will be truncated. Truncation is indicated by the `truncated` field in the result manifest. - The maximum
    query text size is 16 MiB. - Cancelation might silently fail. A successful response from a cancel request
    indicates that the cancel request was successfully received and sent to the processing engine. However, an
    outstanding statement might have already completed execution when the cancel request arrives. Polling for
    status until a terminal state is reached is a reliable way to determine the final state. - Wait timeouts
    are approximate, occur server-side, and cannot account for things such as caller delays and network
    latency from caller to service. - To guarantee that the statement is kept alive, you must poll at least
    once every 15 minutes. - The results are only available for one hour after success; polling does not
    extend this. - The SQL Execution API must be used for the entire lifecycle of the statement. For example,
    you cannot use the Jobs API to execute the command, and then the SQL Execution API to cancel it.

    [Apache Arrow Columnar]: https://arrow.apache.org/overview/
    [Databricks SQL Statement Execution API tutorial]: https://docs.databricks.com/sql/api/sql-execution-tutorial.html
    """

    def __init__(self, api_client):
        self._api = api_client

    def cancel_execution(self, statement_id: str):
        """Requests that an executing statement be canceled. Callers must poll for status to see the terminal
        state. Cancel response is empty; receiving response indicates successful receipt.

        :param statement_id: str
          The statement ID is returned upon successfully submitting a SQL statement, and is a required
          reference for all subsequent calls.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", f"/api/2.0/sql/statements/{statement_id}/cancel", headers=headers)

    def execute_statement(
        self,
        statement: str,
        warehouse_id: str,
        *,
        byte_limit: Optional[int] = None,
        catalog: Optional[str] = None,
        disposition: Optional[Disposition] = None,
        format: Optional[Format] = None,
        on_wait_timeout: Optional[ExecuteStatementRequestOnWaitTimeout] = None,
        parameters: Optional[List[StatementParameterListItem]] = None,
        query_tags: Optional[List[QueryTag]] = None,
        row_limit: Optional[int] = None,
        schema: Optional[str] = None,
        wait_timeout: Optional[str] = None,
    ) -> StatementResponse:
        """Execute a SQL statement and optionally await its results for a specified time.

        **Use case: small result sets with INLINE + JSON_ARRAY**

        For flows that generate small and predictable result sets (<= 25 MiB), `INLINE` responses of
        `JSON_ARRAY` result data are typically the simplest way to execute and fetch result data.

        **Use case: large result sets with EXTERNAL_LINKS**

        Using `EXTERNAL_LINKS` to fetch result data allows you to fetch large result sets efficiently. The
        main differences from using `INLINE` disposition are that the result data is accessed with URLs, and
        that there are 3 supported formats: `JSON_ARRAY`, `ARROW_STREAM` and `CSV` compared to only
        `JSON_ARRAY` with `INLINE`.

        ** URLs**

        External links point to data stored within your workspace's internal storage, in the form of a URL.
        The URLs are valid for only a short period, <= 15 minutes. Alongside each `external_link` is an
        expiration field indicating the time at which the URL is no longer valid. In `EXTERNAL_LINKS` mode,
        chunks can be resolved and fetched multiple times and in parallel.

        ----

        ### **Warning: Databricks strongly recommends that you protect the URLs that are returned by the
        `EXTERNAL_LINKS` disposition.**

        When you use the `EXTERNAL_LINKS` disposition, a short-lived, URL is generated, which can be used to
        download the results directly from . As a short-lived is embedded in this URL, you should protect the
        URL.

        Because URLs are already generated with embedded temporary s, you must not set an `Authorization`
        header in the download requests.

        The `EXTERNAL_LINKS` disposition can be disabled upon request by creating a support case.

        See also [Security best practices].

        ----

        StatementResponse contains `statement_id` and `status`; other fields might be absent or present
        depending on context. If the SQL warehouse fails to execute the provided statement, a 200 response is
        returned with `status.state` set to `FAILED` (in contrast to a failure when accepting the request,
        which results in a non-200 response). Details of the error can be found at `status.error` in case of
        execution failures.

        [Security best practices]: https://docs.databricks.com/sql/admin/sql-execution-tutorial.html#security-best-practices

        :param statement: str
          The SQL statement to execute. The statement can optionally be parameterized, see `parameters`. The
          maximum query text size is 16 MiB.
        :param warehouse_id: str
          Warehouse upon which to execute a statement. See also [What are SQL warehouses?]

          [What are SQL warehouses?]: https://docs.databricks.com/sql/admin/warehouse-type.html
        :param byte_limit: int (optional)
          Applies the given byte limit to the statement's result size. Byte counts are based on internal data
          representations and might not match the final size in the requested `format`. If the result was
          truncated due to the byte limit, then `truncated` in the response is set to `true`. When using
          `EXTERNAL_LINKS` disposition, a default `byte_limit` of 100 GiB is applied if `byte_limit` is not
          explicitly set.
        :param catalog: str (optional)
          Sets default catalog for statement execution, similar to [`USE CATALOG`] in SQL.

          [`USE CATALOG`]: https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-use-catalog.html
        :param disposition: :class:`Disposition` (optional)
          The fetch disposition provides two modes of fetching results: `INLINE` and `EXTERNAL_LINKS`.

          Statements executed with `INLINE` disposition will return result data inline, in `JSON_ARRAY`
          format, in a series of chunks. If a given statement produces a result set with a size larger than 25
          MiB, that statement execution is aborted, and no result set will be available.

          **NOTE** Byte limits are computed based upon internal representations of the result set data, and
          might not match the sizes visible in JSON responses.

          Statements executed with `EXTERNAL_LINKS` disposition will return result data as external links:
          URLs that point to cloud storage internal to the workspace. Using `EXTERNAL_LINKS` disposition
          allows statements to generate arbitrarily sized result sets for fetching up to 100 GiB. The
          resulting links have two important properties:

          1. They point to resources _external_ to the Databricks compute; therefore any associated
          authentication information (typically a personal access token, OAuth token, or similar) _must be
          removed_ when fetching from these links.

          2. These are URLs with a specific expiration, indicated in the response. The behavior when
          attempting to use an expired link is cloud specific.
        :param format: :class:`Format` (optional)
          Statement execution supports three result formats: `JSON_ARRAY` (default), `ARROW_STREAM`, and
          `CSV`.

          Important: The formats `ARROW_STREAM` and `CSV` are supported only with `EXTERNAL_LINKS`
          disposition. `JSON_ARRAY` is supported in `INLINE` and `EXTERNAL_LINKS` disposition.

          When specifying `format=JSON_ARRAY`, result data will be formatted as an array of arrays of values,
          where each value is either the *string representation* of a value, or `null`. For example, the
          output of `SELECT concat('id-', id) AS strCol, id AS intCol, null AS nullCol FROM range(3)` would
          look like this:

          ``` [ [ "id-1", "1", null ], [ "id-2", "2", null ], [ "id-3", "3", null ], ] ```

          When specifying `format=JSON_ARRAY` and `disposition=EXTERNAL_LINKS`, each chunk in the result
          contains compact JSON with no indentation or extra whitespace.

          When specifying `format=ARROW_STREAM` and `disposition=EXTERNAL_LINKS`, each chunk in the result
          will be formatted as Apache Arrow Stream. See the [Apache Arrow streaming format].

          When specifying `format=CSV` and `disposition=EXTERNAL_LINKS`, each chunk in the result will be a
          CSV according to [RFC 4180] standard. All the columns values will have *string representation*
          similar to the `JSON_ARRAY` format, and `null` values will be encoded as “null”. Only the first
          chunk in the result would contain a header row with column names. For example, the output of `SELECT
          concat('id-', id) AS strCol, id AS intCol, null as nullCol FROM range(3)` would look like this:

          ``` strCol,intCol,nullCol id-1,1,null id-2,2,null id-3,3,null ```

          [Apache Arrow streaming format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
          [RFC 4180]: https://www.rfc-editor.org/rfc/rfc4180
        :param on_wait_timeout: :class:`ExecuteStatementRequestOnWaitTimeout` (optional)
          When `wait_timeout > 0s`, the call will block up to the specified time. If the statement execution
          doesn't finish within this time, `on_wait_timeout` determines whether the execution should continue
          or be canceled. When set to `CONTINUE`, the statement execution continues asynchronously and the
          call returns a statement ID which can be used for polling with
          :method:statementexecution/getStatement. When set to `CANCEL`, the statement execution is canceled
          and the call returns with a `CANCELED` state.
        :param parameters: List[:class:`StatementParameterListItem`] (optional)
          A list of parameters to pass into a SQL statement containing parameter markers. A parameter consists
          of a name, a value, and optionally a type. To represent a NULL value, the `value` field may be
          omitted or set to `null` explicitly. If the `type` field is omitted, the value is interpreted as a
          string.

          If the type is given, parameters will be checked for type correctness according to the given type. A
          value is correct if the provided string can be converted to the requested type using the `cast`
          function. The exact semantics are described in the section [`cast` function] of the SQL language
          reference.

          For example, the following statement contains two parameters, `my_name` and `my_date`:

          ``` SELECT * FROM my_table WHERE name = :my_name AND date = :my_date ```

          The parameters can be passed in the request body as follows:

          ` { ..., "statement": "SELECT * FROM my_table WHERE name = :my_name AND date = :my_date",
          "parameters": [ { "name": "my_name", "value": "the name" }, { "name": "my_date", "value":
          "2020-01-01", "type": "DATE" } ] } `

          Currently, positional parameters denoted by a `?` marker are not supported by the Databricks SQL
          Statement Execution API.

          Also see the section [Parameter markers] of the SQL language reference.

          [Parameter markers]: https://docs.databricks.com/sql/language-manual/sql-ref-parameter-marker.html
          [`cast` function]: https://docs.databricks.com/sql/language-manual/functions/cast.html
        :param query_tags: List[:class:`QueryTag`] (optional)
          An array of query tags to annotate a SQL statement. A query tag consists of a non-empty key and,
          optionally, a value. To represent a NULL value, either omit the `value` field or manually set it to
          `null` or white space. Refer to the SQL language reference for the format specification of query
          tags. There's no significance to the order of tags. Only one value per key will be recorded. A
          sequence in excess of 20 query tags will be coerced to 20. Example:

          { ..., "query_tags": [ { "key": "team", "value": "eng" }, { "key": "some key only tag" } ] }
        :param row_limit: int (optional)
          Applies the given row limit to the statement's result set, but unlike the `LIMIT` clause in SQL, it
          also sets the `truncated` field in the response to indicate whether the result was trimmed due to
          the limit or not.
        :param schema: str (optional)
          Sets default schema for statement execution, similar to [`USE SCHEMA`] in SQL.

          [`USE SCHEMA`]: https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-use-schema.html
        :param wait_timeout: str (optional)
          The time in seconds the call will wait for the statement's result set as `Ns`, where `N` can be set
          to 0 or to a value between 5 and 50.

          When set to `0s`, the statement will execute in asynchronous mode and the call will not wait for the
          execution to finish. In this case, the call returns directly with `PENDING` state and a statement ID
          which can be used for polling with :method:statementexecution/getStatement.

          When set between 5 and 50 seconds, the call will behave synchronously up to this timeout and wait
          for the statement execution to finish. If the execution finishes within this time, the call returns
          immediately with a manifest and result data (or a `FAILED` state in case of an execution error). If
          the statement takes longer to execute, `on_wait_timeout` determines what should happen after the
          timeout is reached.

        :returns: :class:`StatementResponse`
        """

        body = {}
        if byte_limit is not None:
            body["byte_limit"] = byte_limit
        if catalog is not None:
            body["catalog"] = catalog
        if disposition is not None:
            body["disposition"] = disposition.value
        if format is not None:
            body["format"] = format.value
        if on_wait_timeout is not None:
            body["on_wait_timeout"] = on_wait_timeout.value
        if parameters is not None:
            body["parameters"] = [v.as_dict() for v in parameters]
        if query_tags is not None:
            body["query_tags"] = [v.as_dict() for v in query_tags]
        if row_limit is not None:
            body["row_limit"] = row_limit
        if schema is not None:
            body["schema"] = schema
        if statement is not None:
            body["statement"] = statement
        if wait_timeout is not None:
            body["wait_timeout"] = wait_timeout
        if warehouse_id is not None:
            body["warehouse_id"] = warehouse_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/sql/statements", body=body, headers=headers)
        return StatementResponse.from_dict(res)

    def get_statement(self, statement_id: str) -> StatementResponse:
        """This request can be used to poll for the statement's status. StatementResponse contains `statement_id`
        and `status`; other fields might be absent or present depending on context. When the `status.state`
        field is `SUCCEEDED` it will also return the result manifest and the first chunk of the result data.
        When the statement is in the terminal states `CANCELED`, `CLOSED` or `FAILED`, it returns HTTP 200
        with the state set. After at least 12 hours in terminal state, the statement is removed from the
        warehouse and further calls will receive an HTTP 404 response.

        **NOTE** This call currently might take up to 5 seconds to get the latest status and result.

        :param statement_id: str
          The statement ID is returned upon successfully submitting a SQL statement, and is a required
          reference for all subsequent calls.

        :returns: :class:`StatementResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/sql/statements/{statement_id}", headers=headers)
        return StatementResponse.from_dict(res)

    def get_statement_result_chunk_n(self, statement_id: str, chunk_index: int) -> ResultData:
        """After the statement execution has `SUCCEEDED`, this request can be used to fetch any chunk by index.
        Whereas the first chunk with `chunk_index=0` is typically fetched with
        :method:statementexecution/executeStatement or :method:statementexecution/getStatement, this request
        can be used to fetch subsequent chunks. The response structure is identical to the nested `result`
        element described in the :method:statementexecution/getStatement request, and similarly includes the
        `next_chunk_index` and `next_chunk_internal_link` fields for simple iteration through the result set.
        Depending on `disposition`, the response returns chunks of data either inline, or as links.

        :param statement_id: str
          The statement ID is returned upon successfully submitting a SQL statement, and is a required
          reference for all subsequent calls.
        :param chunk_index: int

        :returns: :class:`ResultData`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}", headers=headers
        )
        return ResultData.from_dict(res)


class WarehousesAPI:
    """A SQL warehouse is a compute resource that lets you run SQL commands on data objects within Databricks
    SQL. Compute resources are infrastructure resources that provide processing capabilities in the cloud."""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_warehouse_running(
        self, id: str, timeout=timedelta(minutes=20), callback: Optional[Callable[[GetWarehouseResponse], None]] = None
    ) -> GetWarehouseResponse:
        deadline = time.time() + timeout.total_seconds()
        target_states = (State.RUNNING,)
        failure_states = (
            State.STOPPED,
            State.DELETED,
        )
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get(id=id)
            status = poll.state
            status_message = f"current status: {status}"
            if poll.health:
                status_message = poll.health.summary
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach RUNNING, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"id={id}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def wait_get_warehouse_stopped(
        self, id: str, timeout=timedelta(minutes=20), callback: Optional[Callable[[GetWarehouseResponse], None]] = None
    ) -> GetWarehouseResponse:
        deadline = time.time() + timeout.total_seconds()
        target_states = (State.STOPPED,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get(id=id)
            status = poll.state
            status_message = f"current status: {status}"
            if poll.health:
                status_message = poll.health.summary
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            prefix = f"id={id}"
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
        auto_stop_mins: Optional[int] = None,
        channel: Optional[Channel] = None,
        cluster_size: Optional[str] = None,
        creator_name: Optional[str] = None,
        enable_photon: Optional[bool] = None,
        enable_serverless_compute: Optional[bool] = None,
        instance_profile_arn: Optional[str] = None,
        max_num_clusters: Optional[int] = None,
        min_num_clusters: Optional[int] = None,
        name: Optional[str] = None,
        spot_instance_policy: Optional[SpotInstancePolicy] = None,
        tags: Optional[EndpointTags] = None,
        warehouse_type: Optional[CreateWarehouseRequestWarehouseType] = None,
    ) -> Wait[GetWarehouseResponse]:
        """Creates a new SQL warehouse.

        :param auto_stop_mins: int (optional)
          The amount of time in minutes that a SQL warehouse must be idle (i.e., no RUNNING queries) before it
          is automatically stopped.

          Supported values: - Must be == 0 or >= 10 mins - 0 indicates no autostop.

          Defaults to 120 mins
        :param channel: :class:`Channel` (optional)
          Channel Details
        :param cluster_size: str (optional)
          Size of the clusters allocated for this warehouse. Increasing the size of a spark cluster allows you
          to run larger queries on it. If you want to increase the number of concurrent queries, please tune
          max_num_clusters.

          Supported values: - 2X-Small - X-Small - Small - Medium - Large - X-Large - 2X-Large - 3X-Large -
          4X-Large - 5X-Large
        :param creator_name: str (optional)
          warehouse creator name
        :param enable_photon: bool (optional)
          Configures whether the warehouse should use Photon optimized clusters.

          Defaults to true.
        :param enable_serverless_compute: bool (optional)
          Configures whether the warehouse should use serverless compute
        :param instance_profile_arn: str (optional)
          Deprecated. Instance profile used to pass IAM role to the cluster
        :param max_num_clusters: int (optional)
          Maximum number of clusters that the autoscaler will create to handle concurrent queries.

          Supported values: - Must be >= min_num_clusters - Must be <= 40.

          Defaults to min_clusters if unset.
        :param min_num_clusters: int (optional)
          Minimum number of available clusters that will be maintained for this SQL warehouse. Increasing this
          will ensure that a larger number of clusters are always running and therefore may reduce the cold
          start time for new queries. This is similar to reserved vs. revocable cores in a resource manager.

          Supported values: - Must be > 0 - Must be <= min(max_num_clusters, 30)

          Defaults to 1
        :param name: str (optional)
          Logical name for the cluster.

          Supported values: - Must be unique within an org. - Must be less than 100 characters.
        :param spot_instance_policy: :class:`SpotInstancePolicy` (optional)
          Configurations whether the endpoint should use spot instances.
        :param tags: :class:`EndpointTags` (optional)
          A set of key-value pairs that will be tagged on all resources (e.g., AWS instances and EBS volumes)
          associated with this SQL warehouse.

          Supported values: - Number of tags < 45.
        :param warehouse_type: :class:`CreateWarehouseRequestWarehouseType` (optional)
          Warehouse type: `PRO` or `CLASSIC`. If you want to use serverless compute, you must set to `PRO` and
          also set the field `enable_serverless_compute` to `true`.

        :returns:
          Long-running operation waiter for :class:`GetWarehouseResponse`.
          See :method:wait_get_warehouse_running for more details.
        """

        body = {}
        if auto_stop_mins is not None:
            body["auto_stop_mins"] = auto_stop_mins
        if channel is not None:
            body["channel"] = channel.as_dict()
        if cluster_size is not None:
            body["cluster_size"] = cluster_size
        if creator_name is not None:
            body["creator_name"] = creator_name
        if enable_photon is not None:
            body["enable_photon"] = enable_photon
        if enable_serverless_compute is not None:
            body["enable_serverless_compute"] = enable_serverless_compute
        if instance_profile_arn is not None:
            body["instance_profile_arn"] = instance_profile_arn
        if max_num_clusters is not None:
            body["max_num_clusters"] = max_num_clusters
        if min_num_clusters is not None:
            body["min_num_clusters"] = min_num_clusters
        if name is not None:
            body["name"] = name
        if spot_instance_policy is not None:
            body["spot_instance_policy"] = spot_instance_policy.value
        if tags is not None:
            body["tags"] = tags.as_dict()
        if warehouse_type is not None:
            body["warehouse_type"] = warehouse_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.0/sql/warehouses", body=body, headers=headers)
        return Wait(
            self.wait_get_warehouse_running,
            response=CreateWarehouseResponse.from_dict(op_response),
            id=op_response["id"],
        )

    def create_and_wait(
        self,
        *,
        auto_stop_mins: Optional[int] = None,
        channel: Optional[Channel] = None,
        cluster_size: Optional[str] = None,
        creator_name: Optional[str] = None,
        enable_photon: Optional[bool] = None,
        enable_serverless_compute: Optional[bool] = None,
        instance_profile_arn: Optional[str] = None,
        max_num_clusters: Optional[int] = None,
        min_num_clusters: Optional[int] = None,
        name: Optional[str] = None,
        spot_instance_policy: Optional[SpotInstancePolicy] = None,
        tags: Optional[EndpointTags] = None,
        warehouse_type: Optional[CreateWarehouseRequestWarehouseType] = None,
        timeout=timedelta(minutes=20),
    ) -> GetWarehouseResponse:
        return self.create(
            auto_stop_mins=auto_stop_mins,
            channel=channel,
            cluster_size=cluster_size,
            creator_name=creator_name,
            enable_photon=enable_photon,
            enable_serverless_compute=enable_serverless_compute,
            instance_profile_arn=instance_profile_arn,
            max_num_clusters=max_num_clusters,
            min_num_clusters=min_num_clusters,
            name=name,
            spot_instance_policy=spot_instance_policy,
            tags=tags,
            warehouse_type=warehouse_type,
        ).result(timeout=timeout)

    def create_default_warehouse_override(
        self, default_warehouse_override: DefaultWarehouseOverride, default_warehouse_override_id: str
    ) -> DefaultWarehouseOverride:
        """Creates a new default warehouse override for a user. Users can create their own override. Admins can
        create overrides for any user.

        :param default_warehouse_override: :class:`DefaultWarehouseOverride`
          Required. The default warehouse override to create.
        :param default_warehouse_override_id: str
          Required. The ID to use for the override, which will become the final component of the override's
          resource name. Can be a numeric user ID or the literal string "me" for the current user.

        :returns: :class:`DefaultWarehouseOverride`
        """

        body = default_warehouse_override.as_dict()
        query = {}
        if default_warehouse_override_id is not None:
            query["default_warehouse_override_id"] = default_warehouse_override_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", "/api/warehouses/v1/default-warehouse-overrides", query=query, body=body, headers=headers
        )
        return DefaultWarehouseOverride.from_dict(res)

    def delete(self, id: str):
        """Deletes a SQL warehouse.

        :param id: str
          Required. Id of the SQL warehouse.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/sql/warehouses/{id}", headers=headers)

    def delete_default_warehouse_override(self, name: str):
        """Deletes the default warehouse override for a user. Users can delete their own override. Admins can
        delete overrides for any user. After deletion, the workspace default warehouse will be used.

        :param name: str
          Required. The resource name of the default warehouse override to delete. Format:
          default-warehouse-overrides/{default_warehouse_override_id} The default_warehouse_override_id can be
          a numeric user ID or the literal string "me" for the current user.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/warehouses/v1/{name}", headers=headers)

    def edit(
        self,
        id: str,
        *,
        auto_stop_mins: Optional[int] = None,
        channel: Optional[Channel] = None,
        cluster_size: Optional[str] = None,
        creator_name: Optional[str] = None,
        enable_photon: Optional[bool] = None,
        enable_serverless_compute: Optional[bool] = None,
        instance_profile_arn: Optional[str] = None,
        max_num_clusters: Optional[int] = None,
        min_num_clusters: Optional[int] = None,
        name: Optional[str] = None,
        spot_instance_policy: Optional[SpotInstancePolicy] = None,
        tags: Optional[EndpointTags] = None,
        warehouse_type: Optional[EditWarehouseRequestWarehouseType] = None,
    ) -> Wait[GetWarehouseResponse]:
        """Updates the configuration for a SQL warehouse.

        :param id: str
          Required. Id of the warehouse to configure.
        :param auto_stop_mins: int (optional)
          The amount of time in minutes that a SQL warehouse must be idle (i.e., no RUNNING queries) before it
          is automatically stopped.

          Supported values: - Must be == 0 or >= 10 mins - 0 indicates no autostop.

          Defaults to 120 mins
        :param channel: :class:`Channel` (optional)
          Channel Details
        :param cluster_size: str (optional)
          Size of the clusters allocated for this warehouse. Increasing the size of a spark cluster allows you
          to run larger queries on it. If you want to increase the number of concurrent queries, please tune
          max_num_clusters.

          Supported values: - 2X-Small - X-Small - Small - Medium - Large - X-Large - 2X-Large - 3X-Large -
          4X-Large - 5X-Large
        :param creator_name: str (optional)
          warehouse creator name
        :param enable_photon: bool (optional)
          Configures whether the warehouse should use Photon optimized clusters.

          Defaults to true.
        :param enable_serverless_compute: bool (optional)
          Configures whether the warehouse should use serverless compute
        :param instance_profile_arn: str (optional)
          Deprecated. Instance profile used to pass IAM role to the cluster
        :param max_num_clusters: int (optional)
          Maximum number of clusters that the autoscaler will create to handle concurrent queries.

          Supported values: - Must be >= min_num_clusters - Must be <= 40.

          Defaults to min_clusters if unset.
        :param min_num_clusters: int (optional)
          Minimum number of available clusters that will be maintained for this SQL warehouse. Increasing this
          will ensure that a larger number of clusters are always running and therefore may reduce the cold
          start time for new queries. This is similar to reserved vs. revocable cores in a resource manager.

          Supported values: - Must be > 0 - Must be <= min(max_num_clusters, 30)

          Defaults to 1
        :param name: str (optional)
          Logical name for the cluster.

          Supported values: - Must be unique within an org. - Must be less than 100 characters.
        :param spot_instance_policy: :class:`SpotInstancePolicy` (optional)
          Configurations whether the endpoint should use spot instances.
        :param tags: :class:`EndpointTags` (optional)
          A set of key-value pairs that will be tagged on all resources (e.g., AWS instances and EBS volumes)
          associated with this SQL warehouse.

          Supported values: - Number of tags < 45.
        :param warehouse_type: :class:`EditWarehouseRequestWarehouseType` (optional)
          Warehouse type: `PRO` or `CLASSIC`. If you want to use serverless compute, you must set to `PRO` and
          also set the field `enable_serverless_compute` to `true`.

        :returns:
          Long-running operation waiter for :class:`GetWarehouseResponse`.
          See :method:wait_get_warehouse_running for more details.
        """

        body = {}
        if auto_stop_mins is not None:
            body["auto_stop_mins"] = auto_stop_mins
        if channel is not None:
            body["channel"] = channel.as_dict()
        if cluster_size is not None:
            body["cluster_size"] = cluster_size
        if creator_name is not None:
            body["creator_name"] = creator_name
        if enable_photon is not None:
            body["enable_photon"] = enable_photon
        if enable_serverless_compute is not None:
            body["enable_serverless_compute"] = enable_serverless_compute
        if instance_profile_arn is not None:
            body["instance_profile_arn"] = instance_profile_arn
        if max_num_clusters is not None:
            body["max_num_clusters"] = max_num_clusters
        if min_num_clusters is not None:
            body["min_num_clusters"] = min_num_clusters
        if name is not None:
            body["name"] = name
        if spot_instance_policy is not None:
            body["spot_instance_policy"] = spot_instance_policy.value
        if tags is not None:
            body["tags"] = tags.as_dict()
        if warehouse_type is not None:
            body["warehouse_type"] = warehouse_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/sql/warehouses/{id}/edit", body=body, headers=headers)
        return Wait(self.wait_get_warehouse_running, id=id)

    def edit_and_wait(
        self,
        id: str,
        *,
        auto_stop_mins: Optional[int] = None,
        channel: Optional[Channel] = None,
        cluster_size: Optional[str] = None,
        creator_name: Optional[str] = None,
        enable_photon: Optional[bool] = None,
        enable_serverless_compute: Optional[bool] = None,
        instance_profile_arn: Optional[str] = None,
        max_num_clusters: Optional[int] = None,
        min_num_clusters: Optional[int] = None,
        name: Optional[str] = None,
        spot_instance_policy: Optional[SpotInstancePolicy] = None,
        tags: Optional[EndpointTags] = None,
        warehouse_type: Optional[EditWarehouseRequestWarehouseType] = None,
        timeout=timedelta(minutes=20),
    ) -> GetWarehouseResponse:
        return self.edit(
            auto_stop_mins=auto_stop_mins,
            channel=channel,
            cluster_size=cluster_size,
            creator_name=creator_name,
            enable_photon=enable_photon,
            enable_serverless_compute=enable_serverless_compute,
            id=id,
            instance_profile_arn=instance_profile_arn,
            max_num_clusters=max_num_clusters,
            min_num_clusters=min_num_clusters,
            name=name,
            spot_instance_policy=spot_instance_policy,
            tags=tags,
            warehouse_type=warehouse_type,
        ).result(timeout=timeout)

    def get(self, id: str) -> GetWarehouseResponse:
        """Gets the information for a single SQL warehouse.

        :param id: str
          Required. Id of the SQL warehouse.

        :returns: :class:`GetWarehouseResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/sql/warehouses/{id}", headers=headers)
        return GetWarehouseResponse.from_dict(res)

    def get_default_warehouse_override(self, name: str) -> DefaultWarehouseOverride:
        """Returns the default warehouse override for a user. Users can fetch their own override. Admins can
        fetch overrides for any user. If no override exists, the UI will fallback to the workspace default
        warehouse.

        :param name: str
          Required. The resource name of the default warehouse override to retrieve. Format:
          default-warehouse-overrides/{default_warehouse_override_id} The default_warehouse_override_id can be
          a numeric user ID or the literal string "me" for the current user.

        :returns: :class:`DefaultWarehouseOverride`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/warehouses/v1/{name}", headers=headers)
        return DefaultWarehouseOverride.from_dict(res)

    def get_permission_levels(self, warehouse_id: str) -> GetWarehousePermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param warehouse_id: str
          The SQL warehouse for which to get or manage permissions.

        :returns: :class:`GetWarehousePermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/warehouses/{warehouse_id}/permissionLevels", headers=headers)
        return GetWarehousePermissionLevelsResponse.from_dict(res)

    def get_permissions(self, warehouse_id: str) -> WarehousePermissions:
        """Gets the permissions of a SQL warehouse. SQL warehouses can inherit permissions from their root
        object.

        :param warehouse_id: str
          The SQL warehouse for which to get or manage permissions.

        :returns: :class:`WarehousePermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/warehouses/{warehouse_id}", headers=headers)
        return WarehousePermissions.from_dict(res)

    def get_workspace_warehouse_config(self) -> GetWorkspaceWarehouseConfigResponse:
        """Gets the workspace level configuration that is shared by all SQL warehouses in a workspace.


        :returns: :class:`GetWorkspaceWarehouseConfigResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/sql/config/warehouses", headers=headers)
        return GetWorkspaceWarehouseConfigResponse.from_dict(res)

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None, run_as_user_id: Optional[int] = None
    ) -> Iterator[EndpointInfo]:
        """Lists all SQL warehouses that a user has access to.

        :param page_size: int (optional)
          The max number of warehouses to return.
        :param page_token: str (optional)
          A page token, received from a previous `ListWarehouses` call. Provide this to retrieve the
          subsequent page; otherwise the first will be retrieved.

          When paginating, all other parameters provided to `ListWarehouses` must match the call that provided
          the page token.
        :param run_as_user_id: int (optional)
          Service Principal which will be used to fetch the list of endpoints. If not specified, SQL Gateway
          will use the user from the session header.

        :returns: Iterator over :class:`EndpointInfo`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        if run_as_user_id is not None:
            query["run_as_user_id"] = run_as_user_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/sql/warehouses", query=query, headers=headers)
            if "warehouses" in json:
                for v in json["warehouses"]:
                    yield EndpointInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_default_warehouse_overrides(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[DefaultWarehouseOverride]:
        """Lists all default warehouse overrides in the workspace. Only workspace administrators can list all
        overrides.

        :param page_size: int (optional)
          The maximum number of overrides to return. The service may return fewer than this value. If
          unspecified, at most 100 overrides will be returned. The maximum value is 1000; values above 1000
          will be coerced to 1000.
        :param page_token: str (optional)
          A page token, received from a previous `ListDefaultWarehouseOverrides` call. Provide this to
          retrieve the subsequent page.

          When paginating, all other parameters provided to `ListDefaultWarehouseOverrides` must match the
          call that provided the page token.

        :returns: Iterator over :class:`DefaultWarehouseOverride`
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
            json = self._api.do("GET", "/api/warehouses/v1/default-warehouse-overrides", query=query, headers=headers)
            if "default_warehouse_overrides" in json:
                for v in json["default_warehouse_overrides"]:
                    yield DefaultWarehouseOverride.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def set_permissions(
        self, warehouse_id: str, *, access_control_list: Optional[List[WarehouseAccessControlRequest]] = None
    ) -> WarehousePermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param warehouse_id: str
          The SQL warehouse for which to get or manage permissions.
        :param access_control_list: List[:class:`WarehouseAccessControlRequest`] (optional)

        :returns: :class:`WarehousePermissions`
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

        res = self._api.do("PUT", f"/api/2.0/permissions/warehouses/{warehouse_id}", body=body, headers=headers)
        return WarehousePermissions.from_dict(res)

    def set_workspace_warehouse_config(
        self,
        *,
        channel: Optional[Channel] = None,
        config_param: Optional[RepeatedEndpointConfPairs] = None,
        data_access_config: Optional[List[EndpointConfPair]] = None,
        enable_serverless_compute: Optional[bool] = None,
        enabled_warehouse_types: Optional[List[WarehouseTypePair]] = None,
        global_param: Optional[RepeatedEndpointConfPairs] = None,
        google_service_account: Optional[str] = None,
        instance_profile_arn: Optional[str] = None,
        security_policy: Optional[SetWorkspaceWarehouseConfigRequestSecurityPolicy] = None,
        sql_configuration_parameters: Optional[RepeatedEndpointConfPairs] = None,
    ):
        """Sets the workspace level configuration that is shared by all SQL warehouses in a workspace.

        :param channel: :class:`Channel` (optional)
          Optional: Channel selection details
        :param config_param: :class:`RepeatedEndpointConfPairs` (optional)
          Deprecated: Use sql_configuration_parameters
        :param data_access_config: List[:class:`EndpointConfPair`] (optional)
          Spark confs for external hive metastore configuration JSON serialized size must be less than <= 512K
        :param enable_serverless_compute: bool (optional)
          Enable Serverless compute for SQL warehouses
        :param enabled_warehouse_types: List[:class:`WarehouseTypePair`] (optional)
          List of Warehouse Types allowed in this workspace (limits allowed value of the type field in
          CreateWarehouse and EditWarehouse). Note: Some types cannot be disabled, they don't need to be
          specified in SetWorkspaceWarehouseConfig. Note: Disabling a type may cause existing warehouses to be
          converted to another type. Used by frontend to save specific type availability in the warehouse
          create and edit form UI.
        :param global_param: :class:`RepeatedEndpointConfPairs` (optional)
          Deprecated: Use sql_configuration_parameters
        :param google_service_account: str (optional)
          GCP only: Google Service Account used to pass to cluster to access Google Cloud Storage
        :param instance_profile_arn: str (optional)
          AWS Only: The instance profile used to pass an IAM role to the SQL warehouses. This configuration is
          also applied to the workspace's serverless compute for notebooks and jobs.
        :param security_policy: :class:`SetWorkspaceWarehouseConfigRequestSecurityPolicy` (optional)
          Security policy for warehouses
        :param sql_configuration_parameters: :class:`RepeatedEndpointConfPairs` (optional)
          SQL configuration parameters


        """

        body = {}
        if channel is not None:
            body["channel"] = channel.as_dict()
        if config_param is not None:
            body["config_param"] = config_param.as_dict()
        if data_access_config is not None:
            body["data_access_config"] = [v.as_dict() for v in data_access_config]
        if enable_serverless_compute is not None:
            body["enable_serverless_compute"] = enable_serverless_compute
        if enabled_warehouse_types is not None:
            body["enabled_warehouse_types"] = [v.as_dict() for v in enabled_warehouse_types]
        if global_param is not None:
            body["global_param"] = global_param.as_dict()
        if google_service_account is not None:
            body["google_service_account"] = google_service_account
        if instance_profile_arn is not None:
            body["instance_profile_arn"] = instance_profile_arn
        if security_policy is not None:
            body["security_policy"] = security_policy.value
        if sql_configuration_parameters is not None:
            body["sql_configuration_parameters"] = sql_configuration_parameters.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PUT", "/api/2.0/sql/config/warehouses", body=body, headers=headers)

    def start(self, id: str) -> Wait[GetWarehouseResponse]:
        """Starts a SQL warehouse.

        :param id: str
          Required. Id of the SQL warehouse.

        :returns:
          Long-running operation waiter for :class:`GetWarehouseResponse`.
          See :method:wait_get_warehouse_running for more details.
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/sql/warehouses/{id}/start", headers=headers)
        return Wait(self.wait_get_warehouse_running, id=id)

    def start_and_wait(self, id: str, timeout=timedelta(minutes=20)) -> GetWarehouseResponse:
        return self.start(id=id).result(timeout=timeout)

    def stop(self, id: str) -> Wait[GetWarehouseResponse]:
        """Stops a SQL warehouse.

        :param id: str
          Required. Id of the SQL warehouse.

        :returns:
          Long-running operation waiter for :class:`GetWarehouseResponse`.
          See :method:wait_get_warehouse_stopped for more details.
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/sql/warehouses/{id}/stop", headers=headers)
        return Wait(self.wait_get_warehouse_stopped, id=id)

    def stop_and_wait(self, id: str, timeout=timedelta(minutes=20)) -> GetWarehouseResponse:
        return self.stop(id=id).result(timeout=timeout)

    def update_default_warehouse_override(
        self,
        name: str,
        default_warehouse_override: DefaultWarehouseOverride,
        update_mask: FieldMask,
        *,
        allow_missing: Optional[bool] = None,
    ) -> DefaultWarehouseOverride:
        """Updates an existing default warehouse override for a user. Users can update their own override. Admins
        can update overrides for any user.

        :param name: str
          The resource name of the default warehouse override. Format:
          default-warehouse-overrides/{default_warehouse_override_id}
        :param default_warehouse_override: :class:`DefaultWarehouseOverride`
          Required. The default warehouse override to update. The name field must be set in the format:
          default-warehouse-overrides/{default_warehouse_override_id} The default_warehouse_override_id can be
          a numeric user ID or the literal string "me" for the current user.
        :param update_mask: FieldMask
          Required. Field mask specifying which fields to update. Only the fields specified in the mask will
          be updated. Use "*" to update all fields. When allow_missing is true, this field is ignored and all
          fields are applied.
        :param allow_missing: bool (optional)
          If set to true, and the override is not found, a new override will be created. In this situation,
          `update_mask` is ignored and all fields are applied. Defaults to false.

        :returns: :class:`DefaultWarehouseOverride`
        """

        body = default_warehouse_override.as_dict()
        query = {}
        if allow_missing is not None:
            query["allow_missing"] = allow_missing
        if update_mask is not None:
            query["update_mask"] = update_mask.ToJsonString()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/warehouses/v1/{name}", query=query, body=body, headers=headers)
        return DefaultWarehouseOverride.from_dict(res)

    def update_permissions(
        self, warehouse_id: str, *, access_control_list: Optional[List[WarehouseAccessControlRequest]] = None
    ) -> WarehousePermissions:
        """Updates the permissions on a SQL warehouse. SQL warehouses can inherit permissions from their root
        object.

        :param warehouse_id: str
          The SQL warehouse for which to get or manage permissions.
        :param access_control_list: List[:class:`WarehouseAccessControlRequest`] (optional)

        :returns: :class:`WarehousePermissions`
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

        res = self._api.do("PATCH", f"/api/2.0/permissions/warehouses/{warehouse_id}", body=body, headers=headers)
        return WarehousePermissions.from_dict(res)
