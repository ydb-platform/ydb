# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, BinaryIO, Dict, Iterator, List, Optional

from databricks.sdk.service import compute
from databricks.sdk.service._internal import _enum, _from_dict, _repeated_dict

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class ActionConfiguration:
    action_configuration_id: Optional[str] = None
    """Databricks action configuration ID."""

    action_type: Optional[ActionConfigurationType] = None
    """The type of the action."""

    target: Optional[str] = None
    """Target for the action. For example, an email address."""

    def as_dict(self) -> dict:
        """Serializes the ActionConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.action_configuration_id is not None:
            body["action_configuration_id"] = self.action_configuration_id
        if self.action_type is not None:
            body["action_type"] = self.action_type.value
        if self.target is not None:
            body["target"] = self.target
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ActionConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.action_configuration_id is not None:
            body["action_configuration_id"] = self.action_configuration_id
        if self.action_type is not None:
            body["action_type"] = self.action_type
        if self.target is not None:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ActionConfiguration:
        """Deserializes the ActionConfiguration from a dictionary."""
        return cls(
            action_configuration_id=d.get("action_configuration_id", None),
            action_type=_enum(d, "action_type", ActionConfigurationType),
            target=d.get("target", None),
        )


class ActionConfigurationType(Enum):

    EMAIL_NOTIFICATION = "EMAIL_NOTIFICATION"


@dataclass
class AlertConfiguration:
    action_configurations: Optional[List[ActionConfiguration]] = None
    """Configured actions for this alert. These define what happens when an alert enters a triggered
    state."""

    alert_configuration_id: Optional[str] = None
    """Databricks alert configuration ID."""

    quantity_threshold: Optional[str] = None
    """The threshold for the budget alert to determine if it is in a triggered state. The number is
    evaluated based on `quantity_type`."""

    quantity_type: Optional[AlertConfigurationQuantityType] = None
    """The way to calculate cost for this budget alert. This is what `quantity_threshold` is measured
    in."""

    time_period: Optional[AlertConfigurationTimePeriod] = None
    """The time window of usage data for the budget."""

    trigger_type: Optional[AlertConfigurationTriggerType] = None
    """The evaluation method to determine when this budget alert is in a triggered state."""

    def as_dict(self) -> dict:
        """Serializes the AlertConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.action_configurations:
            body["action_configurations"] = [v.as_dict() for v in self.action_configurations]
        if self.alert_configuration_id is not None:
            body["alert_configuration_id"] = self.alert_configuration_id
        if self.quantity_threshold is not None:
            body["quantity_threshold"] = self.quantity_threshold
        if self.quantity_type is not None:
            body["quantity_type"] = self.quantity_type.value
        if self.time_period is not None:
            body["time_period"] = self.time_period.value
        if self.trigger_type is not None:
            body["trigger_type"] = self.trigger_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AlertConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.action_configurations:
            body["action_configurations"] = self.action_configurations
        if self.alert_configuration_id is not None:
            body["alert_configuration_id"] = self.alert_configuration_id
        if self.quantity_threshold is not None:
            body["quantity_threshold"] = self.quantity_threshold
        if self.quantity_type is not None:
            body["quantity_type"] = self.quantity_type
        if self.time_period is not None:
            body["time_period"] = self.time_period
        if self.trigger_type is not None:
            body["trigger_type"] = self.trigger_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertConfiguration:
        """Deserializes the AlertConfiguration from a dictionary."""
        return cls(
            action_configurations=_repeated_dict(d, "action_configurations", ActionConfiguration),
            alert_configuration_id=d.get("alert_configuration_id", None),
            quantity_threshold=d.get("quantity_threshold", None),
            quantity_type=_enum(d, "quantity_type", AlertConfigurationQuantityType),
            time_period=_enum(d, "time_period", AlertConfigurationTimePeriod),
            trigger_type=_enum(d, "trigger_type", AlertConfigurationTriggerType),
        )


class AlertConfigurationQuantityType(Enum):

    LIST_PRICE_DOLLARS_USD = "LIST_PRICE_DOLLARS_USD"


class AlertConfigurationTimePeriod(Enum):

    MONTH = "MONTH"


class AlertConfigurationTriggerType(Enum):

    CUMULATIVE_SPENDING_EXCEEDED = "CUMULATIVE_SPENDING_EXCEEDED"


@dataclass
class BudgetConfiguration:
    account_id: Optional[str] = None
    """Databricks account ID."""

    alert_configurations: Optional[List[AlertConfiguration]] = None
    """Alerts to configure when this budget is in a triggered state. Budgets must have exactly one
    alert configuration."""

    budget_configuration_id: Optional[str] = None
    """Databricks budget configuration ID."""

    create_time: Optional[int] = None
    """Creation time of this budget configuration."""

    display_name: Optional[str] = None
    """Human-readable name of budget configuration. Max Length: 128"""

    filter: Optional[BudgetConfigurationFilter] = None
    """Configured filters for this budget. These are applied to your account's usage to limit the scope
    of what is considered for this budget. Leave empty to include all usage for this account. All
    provided filters must be matched for usage to be included."""

    update_time: Optional[int] = None
    """Update time of this budget configuration."""

    def as_dict(self) -> dict:
        """Serializes the BudgetConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.alert_configurations:
            body["alert_configurations"] = [v.as_dict() for v in self.alert_configurations]
        if self.budget_configuration_id is not None:
            body["budget_configuration_id"] = self.budget_configuration_id
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.filter:
            body["filter"] = self.filter.as_dict()
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BudgetConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.alert_configurations:
            body["alert_configurations"] = self.alert_configurations
        if self.budget_configuration_id is not None:
            body["budget_configuration_id"] = self.budget_configuration_id
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.filter:
            body["filter"] = self.filter
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BudgetConfiguration:
        """Deserializes the BudgetConfiguration from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            alert_configurations=_repeated_dict(d, "alert_configurations", AlertConfiguration),
            budget_configuration_id=d.get("budget_configuration_id", None),
            create_time=d.get("create_time", None),
            display_name=d.get("display_name", None),
            filter=_from_dict(d, "filter", BudgetConfigurationFilter),
            update_time=d.get("update_time", None),
        )


@dataclass
class BudgetConfigurationFilter:
    tags: Optional[List[BudgetConfigurationFilterTagClause]] = None
    """A list of tag keys and values that will limit the budget to usage that includes those specific
    custom tags. Tags are case-sensitive and should be entered exactly as they appear in your usage
    data."""

    workspace_id: Optional[BudgetConfigurationFilterWorkspaceIdClause] = None
    """If provided, usage must match with the provided Databricks workspace IDs."""

    def as_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        if self.workspace_id:
            body["workspace_id"] = self.workspace_id.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.tags:
            body["tags"] = self.tags
        if self.workspace_id:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BudgetConfigurationFilter:
        """Deserializes the BudgetConfigurationFilter from a dictionary."""
        return cls(
            tags=_repeated_dict(d, "tags", BudgetConfigurationFilterTagClause),
            workspace_id=_from_dict(d, "workspace_id", BudgetConfigurationFilterWorkspaceIdClause),
        )


@dataclass
class BudgetConfigurationFilterClause:
    operator: Optional[BudgetConfigurationFilterOperator] = None

    values: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilterClause into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.operator is not None:
            body["operator"] = self.operator.value
        if self.values:
            body["values"] = [v for v in self.values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilterClause into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.operator is not None:
            body["operator"] = self.operator
        if self.values:
            body["values"] = self.values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BudgetConfigurationFilterClause:
        """Deserializes the BudgetConfigurationFilterClause from a dictionary."""
        return cls(operator=_enum(d, "operator", BudgetConfigurationFilterOperator), values=d.get("values", None))


class BudgetConfigurationFilterOperator(Enum):

    IN = "IN"


@dataclass
class BudgetConfigurationFilterTagClause:
    key: Optional[str] = None

    value: Optional[BudgetConfigurationFilterClause] = None

    def as_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilterTagClause into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value:
            body["value"] = self.value.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilterTagClause into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BudgetConfigurationFilterTagClause:
        """Deserializes the BudgetConfigurationFilterTagClause from a dictionary."""
        return cls(key=d.get("key", None), value=_from_dict(d, "value", BudgetConfigurationFilterClause))


@dataclass
class BudgetConfigurationFilterWorkspaceIdClause:
    operator: Optional[BudgetConfigurationFilterOperator] = None

    values: Optional[List[int]] = None

    def as_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilterWorkspaceIdClause into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.operator is not None:
            body["operator"] = self.operator.value
        if self.values:
            body["values"] = [v for v in self.values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BudgetConfigurationFilterWorkspaceIdClause into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.operator is not None:
            body["operator"] = self.operator
        if self.values:
            body["values"] = self.values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BudgetConfigurationFilterWorkspaceIdClause:
        """Deserializes the BudgetConfigurationFilterWorkspaceIdClause from a dictionary."""
        return cls(operator=_enum(d, "operator", BudgetConfigurationFilterOperator), values=d.get("values", None))


@dataclass
class BudgetPolicy:
    """Contains the BudgetPolicy details."""

    binding_workspace_ids: Optional[List[int]] = None
    """List of workspaces that this budget policy will be exclusively bound to. An empty binding
    implies that this budget policy is open to any workspace in the account."""

    custom_tags: Optional[List[compute.CustomPolicyTag]] = None
    """A list of tags defined by the customer. At most 20 entries are allowed per policy."""

    policy_id: Optional[str] = None
    """The Id of the policy. This field is generated by Databricks and globally unique."""

    policy_name: Optional[str] = None
    """The name of the policy. - Must be unique among active policies. - Can contain only characters
    from the ISO 8859-1 (latin1) set. - Can't start with reserved keywords such as
    `databricks:default-policy`."""

    def as_dict(self) -> dict:
        """Serializes the BudgetPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.binding_workspace_ids:
            body["binding_workspace_ids"] = [v for v in self.binding_workspace_ids]
        if self.custom_tags:
            body["custom_tags"] = [v.as_dict() for v in self.custom_tags]
        if self.policy_id is not None:
            body["policy_id"] = self.policy_id
        if self.policy_name is not None:
            body["policy_name"] = self.policy_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BudgetPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.binding_workspace_ids:
            body["binding_workspace_ids"] = self.binding_workspace_ids
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.policy_id is not None:
            body["policy_id"] = self.policy_id
        if self.policy_name is not None:
            body["policy_name"] = self.policy_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BudgetPolicy:
        """Deserializes the BudgetPolicy from a dictionary."""
        return cls(
            binding_workspace_ids=d.get("binding_workspace_ids", None),
            custom_tags=_repeated_dict(d, "custom_tags", compute.CustomPolicyTag),
            policy_id=d.get("policy_id", None),
            policy_name=d.get("policy_name", None),
        )


@dataclass
class CreateBillingUsageDashboardResponse:
    dashboard_id: Optional[str] = None
    """The unique id of the usage dashboard."""

    def as_dict(self) -> dict:
        """Serializes the CreateBillingUsageDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateBillingUsageDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateBillingUsageDashboardResponse:
        """Deserializes the CreateBillingUsageDashboardResponse from a dictionary."""
        return cls(dashboard_id=d.get("dashboard_id", None))


@dataclass
class CreateBudgetConfigurationBudget:
    account_id: Optional[str] = None
    """Databricks account ID."""

    alert_configurations: Optional[List[CreateBudgetConfigurationBudgetAlertConfigurations]] = None
    """Alerts to configure when this budget is in a triggered state. Budgets must have exactly one
    alert configuration."""

    display_name: Optional[str] = None
    """Human-readable name of budget configuration. Max Length: 128"""

    filter: Optional[BudgetConfigurationFilter] = None
    """Configured filters for this budget. These are applied to your account's usage to limit the scope
    of what is considered for this budget. Leave empty to include all usage for this account. All
    provided filters must be matched for usage to be included."""

    def as_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationBudget into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.alert_configurations:
            body["alert_configurations"] = [v.as_dict() for v in self.alert_configurations]
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.filter:
            body["filter"] = self.filter.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationBudget into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.alert_configurations:
            body["alert_configurations"] = self.alert_configurations
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.filter:
            body["filter"] = self.filter
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateBudgetConfigurationBudget:
        """Deserializes the CreateBudgetConfigurationBudget from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            alert_configurations=_repeated_dict(
                d, "alert_configurations", CreateBudgetConfigurationBudgetAlertConfigurations
            ),
            display_name=d.get("display_name", None),
            filter=_from_dict(d, "filter", BudgetConfigurationFilter),
        )


@dataclass
class CreateBudgetConfigurationBudgetActionConfigurations:
    action_type: Optional[ActionConfigurationType] = None
    """The type of the action."""

    target: Optional[str] = None
    """Target for the action. For example, an email address."""

    def as_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationBudgetActionConfigurations into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.action_type is not None:
            body["action_type"] = self.action_type.value
        if self.target is not None:
            body["target"] = self.target
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationBudgetActionConfigurations into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.action_type is not None:
            body["action_type"] = self.action_type
        if self.target is not None:
            body["target"] = self.target
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateBudgetConfigurationBudgetActionConfigurations:
        """Deserializes the CreateBudgetConfigurationBudgetActionConfigurations from a dictionary."""
        return cls(action_type=_enum(d, "action_type", ActionConfigurationType), target=d.get("target", None))


@dataclass
class CreateBudgetConfigurationBudgetAlertConfigurations:
    action_configurations: Optional[List[CreateBudgetConfigurationBudgetActionConfigurations]] = None
    """Configured actions for this alert. These define what happens when an alert enters a triggered
    state."""

    quantity_threshold: Optional[str] = None
    """The threshold for the budget alert to determine if it is in a triggered state. The number is
    evaluated based on `quantity_type`."""

    quantity_type: Optional[AlertConfigurationQuantityType] = None
    """The way to calculate cost for this budget alert. This is what `quantity_threshold` is measured
    in."""

    time_period: Optional[AlertConfigurationTimePeriod] = None
    """The time window of usage data for the budget."""

    trigger_type: Optional[AlertConfigurationTriggerType] = None
    """The evaluation method to determine when this budget alert is in a triggered state."""

    def as_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationBudgetAlertConfigurations into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.action_configurations:
            body["action_configurations"] = [v.as_dict() for v in self.action_configurations]
        if self.quantity_threshold is not None:
            body["quantity_threshold"] = self.quantity_threshold
        if self.quantity_type is not None:
            body["quantity_type"] = self.quantity_type.value
        if self.time_period is not None:
            body["time_period"] = self.time_period.value
        if self.trigger_type is not None:
            body["trigger_type"] = self.trigger_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationBudgetAlertConfigurations into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.action_configurations:
            body["action_configurations"] = self.action_configurations
        if self.quantity_threshold is not None:
            body["quantity_threshold"] = self.quantity_threshold
        if self.quantity_type is not None:
            body["quantity_type"] = self.quantity_type
        if self.time_period is not None:
            body["time_period"] = self.time_period
        if self.trigger_type is not None:
            body["trigger_type"] = self.trigger_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateBudgetConfigurationBudgetAlertConfigurations:
        """Deserializes the CreateBudgetConfigurationBudgetAlertConfigurations from a dictionary."""
        return cls(
            action_configurations=_repeated_dict(
                d, "action_configurations", CreateBudgetConfigurationBudgetActionConfigurations
            ),
            quantity_threshold=d.get("quantity_threshold", None),
            quantity_type=_enum(d, "quantity_type", AlertConfigurationQuantityType),
            time_period=_enum(d, "time_period", AlertConfigurationTimePeriod),
            trigger_type=_enum(d, "trigger_type", AlertConfigurationTriggerType),
        )


@dataclass
class CreateBudgetConfigurationResponse:
    budget: Optional[BudgetConfiguration] = None
    """The created budget configuration."""

    def as_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget:
            body["budget"] = self.budget.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateBudgetConfigurationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget:
            body["budget"] = self.budget
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateBudgetConfigurationResponse:
        """Deserializes the CreateBudgetConfigurationResponse from a dictionary."""
        return cls(budget=_from_dict(d, "budget", BudgetConfiguration))


@dataclass
class CreateLogDeliveryConfigurationParams:
    """* Log Delivery Configuration"""

    log_type: LogType
    """Log delivery type. Supported values are: * `BILLABLE_USAGE` — Configure [billable usage log
    delivery]. For the CSV schema, see the [View billable usage]. * `AUDIT_LOGS` — Configure
    [audit log delivery]. For the JSON schema, see [Configure audit logging]
    
    [Configure audit logging]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
    [View billable usage]: https://docs.databricks.com/administration-guide/account-settings/usage.html
    [audit log delivery]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
    [billable usage log delivery]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html"""

    output_format: OutputFormat
    """The file type of log delivery. * If `log_type` is `BILLABLE_USAGE`, this value must be `CSV`.
    Only the CSV (comma-separated values) format is supported. For the schema, see the [View
    billable usage] * If `log_type` is `AUDIT_LOGS`, this value must be `JSON`. Only the JSON
    (JavaScript Object Notation) format is supported. For the schema, see the [Configuring audit
    logs].
    
    [Configuring audit logs]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
    [View billable usage]: https://docs.databricks.com/administration-guide/account-settings/usage.html"""

    credentials_id: str
    """The ID for a method:credentials/create that represents the AWS IAM role with policy and trust
    relationship as described in the main billable usage documentation page. See [Configure billable
    usage delivery].
    
    [Configure billable usage delivery]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html"""

    storage_configuration_id: str
    """The ID for a method:storage/create that represents the S3 bucket with bucket policy as described
    in the main billable usage documentation page. See [Configure billable usage delivery].
    
    [Configure billable usage delivery]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html"""

    config_name: Optional[str] = None
    """The optional human-readable name of the log delivery configuration. Defaults to empty."""

    delivery_path_prefix: Optional[str] = None
    """The optional delivery path prefix within Amazon S3 storage. Defaults to empty, which means that
    logs are delivered to the root of the bucket. This must be a valid S3 object key. This must not
    start or end with a slash character."""

    delivery_start_time: Optional[str] = None
    """This field applies only if log_type is BILLABLE_USAGE. This is the optional start month and year
    for delivery, specified in YYYY-MM format. Defaults to current year and month. BILLABLE_USAGE
    logs are not available for usage before March 2019 (2019-03)."""

    status: Optional[LogDeliveryConfigStatus] = None
    """Status of log delivery configuration. Set to `ENABLED` (enabled) or `DISABLED` (disabled).
    Defaults to `ENABLED`. You can [enable or disable the
    configuration](#operation/patch-log-delivery-config-status) later. Deletion of a configuration
    is not supported, so disable a log delivery configuration that is no longer needed."""

    workspace_ids_filter: Optional[List[int]] = None
    """Optional filter that specifies workspace IDs to deliver logs for. By default the workspace
    filter is empty and log delivery applies at the account level, delivering workspace-level logs
    for all workspaces in your account, plus account level logs. You can optionally set this field
    to an array of workspace IDs (each one is an `int64`) to which log delivery should apply, in
    which case only workspace-level logs relating to the specified workspaces are delivered. If you
    plan to use different log delivery configurations for different workspaces, set this field
    explicitly. Be aware that delivery configurations mentioning specific workspaces won't apply to
    new workspaces created in the future, and delivery won't include account level logs. For some
    types of Databricks deployments there is only one workspace per account ID, so this field is
    unnecessary."""

    def as_dict(self) -> dict:
        """Serializes the CreateLogDeliveryConfigurationParams into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.config_name is not None:
            body["config_name"] = self.config_name
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.delivery_path_prefix is not None:
            body["delivery_path_prefix"] = self.delivery_path_prefix
        if self.delivery_start_time is not None:
            body["delivery_start_time"] = self.delivery_start_time
        if self.log_type is not None:
            body["log_type"] = self.log_type.value
        if self.output_format is not None:
            body["output_format"] = self.output_format.value
        if self.status is not None:
            body["status"] = self.status.value
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.workspace_ids_filter:
            body["workspace_ids_filter"] = [v for v in self.workspace_ids_filter]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateLogDeliveryConfigurationParams into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.config_name is not None:
            body["config_name"] = self.config_name
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.delivery_path_prefix is not None:
            body["delivery_path_prefix"] = self.delivery_path_prefix
        if self.delivery_start_time is not None:
            body["delivery_start_time"] = self.delivery_start_time
        if self.log_type is not None:
            body["log_type"] = self.log_type
        if self.output_format is not None:
            body["output_format"] = self.output_format
        if self.status is not None:
            body["status"] = self.status
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.workspace_ids_filter:
            body["workspace_ids_filter"] = self.workspace_ids_filter
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateLogDeliveryConfigurationParams:
        """Deserializes the CreateLogDeliveryConfigurationParams from a dictionary."""
        return cls(
            config_name=d.get("config_name", None),
            credentials_id=d.get("credentials_id", None),
            delivery_path_prefix=d.get("delivery_path_prefix", None),
            delivery_start_time=d.get("delivery_start_time", None),
            log_type=_enum(d, "log_type", LogType),
            output_format=_enum(d, "output_format", OutputFormat),
            status=_enum(d, "status", LogDeliveryConfigStatus),
            storage_configuration_id=d.get("storage_configuration_id", None),
            workspace_ids_filter=d.get("workspace_ids_filter", None),
        )


@dataclass
class DeleteBudgetConfigurationResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteBudgetConfigurationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteBudgetConfigurationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteBudgetConfigurationResponse:
        """Deserializes the DeleteBudgetConfigurationResponse from a dictionary."""
        return cls()


class DeliveryStatus(Enum):
    """* The status string for log delivery. Possible values are: `CREATED`: There were no log delivery
    attempts since the config was created. `SUCCEEDED`: The latest attempt of log delivery has
    succeeded completely. `USER_FAILURE`: The latest attempt of log delivery failed because of
    misconfiguration of customer provided permissions on role or storage. `SYSTEM_FAILURE`: The
    latest attempt of log delivery failed because of an Databricks internal error. Contact support
    if it doesn't go away soon. `NOT_FOUND`: The log delivery status as the configuration has been
    disabled since the release of this feature or there are no workspaces in the account."""

    CREATED = "CREATED"
    NOT_FOUND = "NOT_FOUND"
    SUCCEEDED = "SUCCEEDED"
    SYSTEM_FAILURE = "SYSTEM_FAILURE"
    USER_FAILURE = "USER_FAILURE"


@dataclass
class DownloadResponse:
    contents: Optional[BinaryIO] = None

    def as_dict(self) -> dict:
        """Serializes the DownloadResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.contents:
            body["contents"] = self.contents
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DownloadResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.contents:
            body["contents"] = self.contents
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DownloadResponse:
        """Deserializes the DownloadResponse from a dictionary."""
        return cls(contents=d.get("contents", None))


@dataclass
class Filter:
    """Structured representation of a filter to be applied to a list of policies. All specified filters
    will be applied in conjunction."""

    creator_user_id: Optional[int] = None
    """The policy creator user id to be filtered on. If unspecified, all policies will be returned."""

    creator_user_name: Optional[str] = None
    """The policy creator user name to be filtered on. If unspecified, all policies will be returned."""

    policy_name: Optional[str] = None
    """The partial name of policies to be filtered on. If unspecified, all policies will be returned."""

    def as_dict(self) -> dict:
        """Serializes the Filter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creator_user_id is not None:
            body["creator_user_id"] = self.creator_user_id
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.policy_name is not None:
            body["policy_name"] = self.policy_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Filter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creator_user_id is not None:
            body["creator_user_id"] = self.creator_user_id
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.policy_name is not None:
            body["policy_name"] = self.policy_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Filter:
        """Deserializes the Filter from a dictionary."""
        return cls(
            creator_user_id=d.get("creator_user_id", None),
            creator_user_name=d.get("creator_user_name", None),
            policy_name=d.get("policy_name", None),
        )


@dataclass
class GetBillingUsageDashboardResponse:
    dashboard_id: Optional[str] = None
    """The unique id of the usage dashboard."""

    dashboard_url: Optional[str] = None
    """The URL of the usage dashboard."""

    def as_dict(self) -> dict:
        """Serializes the GetBillingUsageDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.dashboard_url is not None:
            body["dashboard_url"] = self.dashboard_url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetBillingUsageDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.dashboard_url is not None:
            body["dashboard_url"] = self.dashboard_url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetBillingUsageDashboardResponse:
        """Deserializes the GetBillingUsageDashboardResponse from a dictionary."""
        return cls(dashboard_id=d.get("dashboard_id", None), dashboard_url=d.get("dashboard_url", None))


@dataclass
class GetBudgetConfigurationResponse:
    budget: Optional[BudgetConfiguration] = None

    def as_dict(self) -> dict:
        """Serializes the GetBudgetConfigurationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget:
            body["budget"] = self.budget.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetBudgetConfigurationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget:
            body["budget"] = self.budget
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetBudgetConfigurationResponse:
        """Deserializes the GetBudgetConfigurationResponse from a dictionary."""
        return cls(budget=_from_dict(d, "budget", BudgetConfiguration))


@dataclass
class GetLogDeliveryConfigurationResponse:
    log_delivery_configuration: Optional[LogDeliveryConfiguration] = None
    """The fetched log delivery configuration"""

    def as_dict(self) -> dict:
        """Serializes the GetLogDeliveryConfigurationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.log_delivery_configuration:
            body["log_delivery_configuration"] = self.log_delivery_configuration.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetLogDeliveryConfigurationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.log_delivery_configuration:
            body["log_delivery_configuration"] = self.log_delivery_configuration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetLogDeliveryConfigurationResponse:
        """Deserializes the GetLogDeliveryConfigurationResponse from a dictionary."""
        return cls(log_delivery_configuration=_from_dict(d, "log_delivery_configuration", LogDeliveryConfiguration))


@dataclass
class LimitConfig:
    """The limit configuration of the policy. Limit configuration provide a budget policy level cost
    control by enforcing the limit."""

    def as_dict(self) -> dict:
        """Serializes the LimitConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LimitConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LimitConfig:
        """Deserializes the LimitConfig from a dictionary."""
        return cls()


@dataclass
class ListBudgetConfigurationsResponse:
    budgets: Optional[List[BudgetConfiguration]] = None

    next_page_token: Optional[str] = None
    """Token which can be sent as `page_token` to retrieve the next page of results. If this field is
    omitted, there are no subsequent budgets."""

    def as_dict(self) -> dict:
        """Serializes the ListBudgetConfigurationsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budgets:
            body["budgets"] = [v.as_dict() for v in self.budgets]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListBudgetConfigurationsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budgets:
            body["budgets"] = self.budgets
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListBudgetConfigurationsResponse:
        """Deserializes the ListBudgetConfigurationsResponse from a dictionary."""
        return cls(
            budgets=_repeated_dict(d, "budgets", BudgetConfiguration), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListBudgetPoliciesResponse:
    """A list of policies."""

    next_page_token: Optional[str] = None
    """A token that can be sent as `page_token` to retrieve the next page. If this field is omitted,
    there are no subsequent pages."""

    policies: Optional[List[BudgetPolicy]] = None

    previous_page_token: Optional[str] = None
    """A token that can be sent as `page_token` to retrieve the previous page. In this field is
    omitted, there are no previous pages."""

    def as_dict(self) -> dict:
        """Serializes the ListBudgetPoliciesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.policies:
            body["policies"] = [v.as_dict() for v in self.policies]
        if self.previous_page_token is not None:
            body["previous_page_token"] = self.previous_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListBudgetPoliciesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.policies:
            body["policies"] = self.policies
        if self.previous_page_token is not None:
            body["previous_page_token"] = self.previous_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListBudgetPoliciesResponse:
        """Deserializes the ListBudgetPoliciesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            policies=_repeated_dict(d, "policies", BudgetPolicy),
            previous_page_token=d.get("previous_page_token", None),
        )


class LogDeliveryConfigStatus(Enum):
    """* Log Delivery Status

    `ENABLED`: All dependencies have executed and succeeded `DISABLED`: At least one dependency has
    succeeded"""

    DISABLED = "DISABLED"
    ENABLED = "ENABLED"


@dataclass
class LogDeliveryConfiguration:
    """* Log Delivery Configuration"""

    log_type: LogType
    """Log delivery type. Supported values are: * `BILLABLE_USAGE` — Configure [billable usage log
    delivery]. For the CSV schema, see the [View billable usage]. * `AUDIT_LOGS` — Configure
    [audit log delivery]. For the JSON schema, see [Configure audit logging]
    
    [Configure audit logging]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
    [View billable usage]: https://docs.databricks.com/administration-guide/account-settings/usage.html
    [audit log delivery]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
    [billable usage log delivery]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html"""

    output_format: OutputFormat
    """The file type of log delivery. * If `log_type` is `BILLABLE_USAGE`, this value must be `CSV`.
    Only the CSV (comma-separated values) format is supported. For the schema, see the [View
    billable usage] * If `log_type` is `AUDIT_LOGS`, this value must be `JSON`. Only the JSON
    (JavaScript Object Notation) format is supported. For the schema, see the [Configuring audit
    logs].
    
    [Configuring audit logs]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
    [View billable usage]: https://docs.databricks.com/administration-guide/account-settings/usage.html"""

    credentials_id: str
    """The ID for a method:credentials/create that represents the AWS IAM role with policy and trust
    relationship as described in the main billable usage documentation page. See [Configure billable
    usage delivery].
    
    [Configure billable usage delivery]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html"""

    storage_configuration_id: str
    """The ID for a method:storage/create that represents the S3 bucket with bucket policy as described
    in the main billable usage documentation page. See [Configure billable usage delivery].
    
    [Configure billable usage delivery]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html"""

    account_id: Optional[str] = None
    """Databricks account ID."""

    config_id: Optional[str] = None
    """The unique UUID of log delivery configuration"""

    config_name: Optional[str] = None
    """The optional human-readable name of the log delivery configuration. Defaults to empty."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when the log delivery configuration was created."""

    delivery_path_prefix: Optional[str] = None
    """The optional delivery path prefix within Amazon S3 storage. Defaults to empty, which means that
    logs are delivered to the root of the bucket. This must be a valid S3 object key. This must not
    start or end with a slash character."""

    delivery_start_time: Optional[str] = None
    """This field applies only if log_type is BILLABLE_USAGE. This is the optional start month and year
    for delivery, specified in YYYY-MM format. Defaults to current year and month. BILLABLE_USAGE
    logs are not available for usage before March 2019 (2019-03)."""

    log_delivery_status: Optional[LogDeliveryStatus] = None
    """The LogDeliveryStatus of this log delivery configuration"""

    status: Optional[LogDeliveryConfigStatus] = None
    """Status of log delivery configuration. Set to `ENABLED` (enabled) or `DISABLED` (disabled).
    Defaults to `ENABLED`. You can [enable or disable the
    configuration](#operation/patch-log-delivery-config-status) later. Deletion of a configuration
    is not supported, so disable a log delivery configuration that is no longer needed."""

    update_time: Optional[int] = None
    """Time in epoch milliseconds when the log delivery configuration was updated."""

    workspace_ids_filter: Optional[List[int]] = None
    """Optional filter that specifies workspace IDs to deliver logs for. By default the workspace
    filter is empty and log delivery applies at the account level, delivering workspace-level logs
    for all workspaces in your account, plus account level logs. You can optionally set this field
    to an array of workspace IDs (each one is an `int64`) to which log delivery should apply, in
    which case only workspace-level logs relating to the specified workspaces are delivered. If you
    plan to use different log delivery configurations for different workspaces, set this field
    explicitly. Be aware that delivery configurations mentioning specific workspaces won't apply to
    new workspaces created in the future, and delivery won't include account level logs. For some
    types of Databricks deployments there is only one workspace per account ID, so this field is
    unnecessary."""

    def as_dict(self) -> dict:
        """Serializes the LogDeliveryConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.config_id is not None:
            body["config_id"] = self.config_id
        if self.config_name is not None:
            body["config_name"] = self.config_name
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.delivery_path_prefix is not None:
            body["delivery_path_prefix"] = self.delivery_path_prefix
        if self.delivery_start_time is not None:
            body["delivery_start_time"] = self.delivery_start_time
        if self.log_delivery_status:
            body["log_delivery_status"] = self.log_delivery_status.as_dict()
        if self.log_type is not None:
            body["log_type"] = self.log_type.value
        if self.output_format is not None:
            body["output_format"] = self.output_format.value
        if self.status is not None:
            body["status"] = self.status.value
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.workspace_ids_filter:
            body["workspace_ids_filter"] = [v for v in self.workspace_ids_filter]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogDeliveryConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.config_id is not None:
            body["config_id"] = self.config_id
        if self.config_name is not None:
            body["config_name"] = self.config_name
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.credentials_id is not None:
            body["credentials_id"] = self.credentials_id
        if self.delivery_path_prefix is not None:
            body["delivery_path_prefix"] = self.delivery_path_prefix
        if self.delivery_start_time is not None:
            body["delivery_start_time"] = self.delivery_start_time
        if self.log_delivery_status:
            body["log_delivery_status"] = self.log_delivery_status
        if self.log_type is not None:
            body["log_type"] = self.log_type
        if self.output_format is not None:
            body["output_format"] = self.output_format
        if self.status is not None:
            body["status"] = self.status
        if self.storage_configuration_id is not None:
            body["storage_configuration_id"] = self.storage_configuration_id
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.workspace_ids_filter:
            body["workspace_ids_filter"] = self.workspace_ids_filter
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogDeliveryConfiguration:
        """Deserializes the LogDeliveryConfiguration from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            config_id=d.get("config_id", None),
            config_name=d.get("config_name", None),
            creation_time=d.get("creation_time", None),
            credentials_id=d.get("credentials_id", None),
            delivery_path_prefix=d.get("delivery_path_prefix", None),
            delivery_start_time=d.get("delivery_start_time", None),
            log_delivery_status=_from_dict(d, "log_delivery_status", LogDeliveryStatus),
            log_type=_enum(d, "log_type", LogType),
            output_format=_enum(d, "output_format", OutputFormat),
            status=_enum(d, "status", LogDeliveryConfigStatus),
            storage_configuration_id=d.get("storage_configuration_id", None),
            update_time=d.get("update_time", None),
            workspace_ids_filter=d.get("workspace_ids_filter", None),
        )


@dataclass
class LogDeliveryStatus:
    status: DeliveryStatus
    """Enum that describes the status. Possible values are: * `CREATED`: There were no log delivery
    attempts since the config was created. * `SUCCEEDED`: The latest attempt of log delivery has
    succeeded completely. * `USER_FAILURE`: The latest attempt of log delivery failed because of
    misconfiguration of customer provided permissions on role or storage. * `SYSTEM_FAILURE`: The
    latest attempt of log delivery failed because of an Databricks internal error. Contact support
    if it doesn't go away soon. * `NOT_FOUND`: The log delivery status as the configuration has been
    disabled since the release of this feature or there are no workspaces in the account."""

    message: str
    """Informative message about the latest log delivery attempt. If the log delivery fails with
    USER_FAILURE, error details will be provided for fixing misconfigurations in cloud permissions."""

    last_attempt_time: Optional[str] = None
    """The UTC time for the latest log delivery attempt."""

    last_successful_attempt_time: Optional[str] = None
    """The UTC time for the latest successful log delivery."""

    def as_dict(self) -> dict:
        """Serializes the LogDeliveryStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.last_attempt_time is not None:
            body["last_attempt_time"] = self.last_attempt_time
        if self.last_successful_attempt_time is not None:
            body["last_successful_attempt_time"] = self.last_successful_attempt_time
        if self.message is not None:
            body["message"] = self.message
        if self.status is not None:
            body["status"] = self.status.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogDeliveryStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.last_attempt_time is not None:
            body["last_attempt_time"] = self.last_attempt_time
        if self.last_successful_attempt_time is not None:
            body["last_successful_attempt_time"] = self.last_successful_attempt_time
        if self.message is not None:
            body["message"] = self.message
        if self.status is not None:
            body["status"] = self.status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogDeliveryStatus:
        """Deserializes the LogDeliveryStatus from a dictionary."""
        return cls(
            last_attempt_time=d.get("last_attempt_time", None),
            last_successful_attempt_time=d.get("last_successful_attempt_time", None),
            message=d.get("message", None),
            status=_enum(d, "status", DeliveryStatus),
        )


class LogType(Enum):
    """* Log Delivery Type"""

    AUDIT_LOGS = "AUDIT_LOGS"
    BILLABLE_USAGE = "BILLABLE_USAGE"


class OutputFormat(Enum):
    """* Log Delivery Output Format"""

    CSV = "CSV"
    JSON = "JSON"


@dataclass
class PatchStatusResponse:
    def as_dict(self) -> dict:
        """Serializes the PatchStatusResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PatchStatusResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PatchStatusResponse:
        """Deserializes the PatchStatusResponse from a dictionary."""
        return cls()


@dataclass
class SortSpec:
    descending: Optional[bool] = None
    """Whether to sort in descending order."""

    field: Optional[SortSpecField] = None
    """The filed to sort by"""

    def as_dict(self) -> dict:
        """Serializes the SortSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.descending is not None:
            body["descending"] = self.descending
        if self.field is not None:
            body["field"] = self.field.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SortSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.descending is not None:
            body["descending"] = self.descending
        if self.field is not None:
            body["field"] = self.field
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SortSpec:
        """Deserializes the SortSpec from a dictionary."""
        return cls(descending=d.get("descending", None), field=_enum(d, "field", SortSpecField))


class SortSpecField(Enum):

    POLICY_NAME = "POLICY_NAME"


@dataclass
class UpdateBudgetConfigurationBudget:
    account_id: Optional[str] = None
    """Databricks account ID."""

    alert_configurations: Optional[List[AlertConfiguration]] = None
    """Alerts to configure when this budget is in a triggered state. Budgets must have exactly one
    alert configuration."""

    budget_configuration_id: Optional[str] = None
    """Databricks budget configuration ID."""

    display_name: Optional[str] = None
    """Human-readable name of budget configuration. Max Length: 128"""

    filter: Optional[BudgetConfigurationFilter] = None
    """Configured filters for this budget. These are applied to your account's usage to limit the scope
    of what is considered for this budget. Leave empty to include all usage for this account. All
    provided filters must be matched for usage to be included."""

    def as_dict(self) -> dict:
        """Serializes the UpdateBudgetConfigurationBudget into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.alert_configurations:
            body["alert_configurations"] = [v.as_dict() for v in self.alert_configurations]
        if self.budget_configuration_id is not None:
            body["budget_configuration_id"] = self.budget_configuration_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.filter:
            body["filter"] = self.filter.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateBudgetConfigurationBudget into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.alert_configurations:
            body["alert_configurations"] = self.alert_configurations
        if self.budget_configuration_id is not None:
            body["budget_configuration_id"] = self.budget_configuration_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.filter:
            body["filter"] = self.filter
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateBudgetConfigurationBudget:
        """Deserializes the UpdateBudgetConfigurationBudget from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            alert_configurations=_repeated_dict(d, "alert_configurations", AlertConfiguration),
            budget_configuration_id=d.get("budget_configuration_id", None),
            display_name=d.get("display_name", None),
            filter=_from_dict(d, "filter", BudgetConfigurationFilter),
        )


@dataclass
class UpdateBudgetConfigurationResponse:
    budget: Optional[BudgetConfiguration] = None
    """The updated budget."""

    def as_dict(self) -> dict:
        """Serializes the UpdateBudgetConfigurationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget:
            body["budget"] = self.budget.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateBudgetConfigurationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget:
            body["budget"] = self.budget
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateBudgetConfigurationResponse:
        """Deserializes the UpdateBudgetConfigurationResponse from a dictionary."""
        return cls(budget=_from_dict(d, "budget", BudgetConfiguration))


class UsageDashboardMajorVersion(Enum):

    USAGE_DASHBOARD_MAJOR_VERSION_1 = "USAGE_DASHBOARD_MAJOR_VERSION_1"
    USAGE_DASHBOARD_MAJOR_VERSION_2 = "USAGE_DASHBOARD_MAJOR_VERSION_2"


class UsageDashboardType(Enum):

    USAGE_DASHBOARD_TYPE_GLOBAL = "USAGE_DASHBOARD_TYPE_GLOBAL"
    USAGE_DASHBOARD_TYPE_WORKSPACE = "USAGE_DASHBOARD_TYPE_WORKSPACE"


@dataclass
class WrappedLogDeliveryConfiguration:
    log_delivery_configuration: Optional[LogDeliveryConfiguration] = None
    """The created log delivery configuration"""

    def as_dict(self) -> dict:
        """Serializes the WrappedLogDeliveryConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.log_delivery_configuration:
            body["log_delivery_configuration"] = self.log_delivery_configuration.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WrappedLogDeliveryConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.log_delivery_configuration:
            body["log_delivery_configuration"] = self.log_delivery_configuration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WrappedLogDeliveryConfiguration:
        """Deserializes the WrappedLogDeliveryConfiguration from a dictionary."""
        return cls(log_delivery_configuration=_from_dict(d, "log_delivery_configuration", LogDeliveryConfiguration))


@dataclass
class WrappedLogDeliveryConfigurations:
    log_delivery_configurations: Optional[List[LogDeliveryConfiguration]] = None
    """Log delivery configurations were returned successfully."""

    next_page_token: Optional[str] = None
    """Token which can be sent as `page_token` to retrieve the next page of results. If this field is
    omitted, there are no subsequent budgets."""

    def as_dict(self) -> dict:
        """Serializes the WrappedLogDeliveryConfigurations into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.log_delivery_configurations:
            body["log_delivery_configurations"] = [v.as_dict() for v in self.log_delivery_configurations]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WrappedLogDeliveryConfigurations into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.log_delivery_configurations:
            body["log_delivery_configurations"] = self.log_delivery_configurations
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WrappedLogDeliveryConfigurations:
        """Deserializes the WrappedLogDeliveryConfigurations from a dictionary."""
        return cls(
            log_delivery_configurations=_repeated_dict(d, "log_delivery_configurations", LogDeliveryConfiguration),
            next_page_token=d.get("next_page_token", None),
        )


class BillableUsageAPI:
    """This API allows you to download billable usage logs for the specified account and date range. This feature
    works with all account types."""

    def __init__(self, api_client):
        self._api = api_client

    def download(self, start_month: str, end_month: str, *, personal_data: Optional[bool] = None) -> DownloadResponse:
        """Returns billable usage logs in CSV format for the specified account and date range. For the data
        schema, see:

        - AWS: [CSV file schema]. - GCP: [CSV file schema].

        Note that this method might take multiple minutes to complete.

        **Warning**: Depending on the queried date range, the number of workspaces in the account, the size of
        the response and the internet speed of the caller, this API may hit a timeout after a few minutes. If
        you experience this, try to mitigate by calling the API with narrower date ranges.

        [CSV file schema]: https://docs.gcp.databricks.com/administration-guide/account-settings/usage-analysis.html#csv-file-schema

        :param start_month: str
          Format specification for month in the format `YYYY-MM`. This is used to specify billable usage
          `start_month` and `end_month` properties. **Note**: Billable usage logs are unavailable before March
          2019 (`2019-03`).
        :param end_month: str
          Format: `YYYY-MM`. Last month to return billable usage logs for. This field is required.
        :param personal_data: bool (optional)
          Specify whether to include personally identifiable information in the billable usage logs, for
          example the email addresses of cluster creators. Handle this information with care. Defaults to
          false.

        :returns: :class:`DownloadResponse`
        """

        query = {}
        if end_month is not None:
            query["end_month"] = end_month
        if personal_data is not None:
            query["personal_data"] = personal_data
        if start_month is not None:
            query["start_month"] = start_month
        headers = {
            "Accept": "text/plain",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/usage/download", query=query, headers=headers, raw=True
        )
        return DownloadResponse.from_dict(res)


class BudgetPolicyAPI:
    """A service serves REST API about Budget policies"""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, *, policy: Optional[BudgetPolicy] = None, request_id: Optional[str] = None) -> BudgetPolicy:
        """Creates a new policy.

        :param policy: :class:`BudgetPolicy` (optional)
          The policy to create. `policy_id` needs to be empty as it will be generated `policy_name` must be
          provided, custom_tags may need to be provided depending on the cloud provider. All other fields are
          optional.
        :param request_id: str (optional)
          A unique identifier for this request. Restricted to 36 ASCII characters. A random UUID is
          recommended. This request is only idempotent if a `request_id` is provided.

        :returns: :class:`BudgetPolicy`
        """

        if request_id is None or request_id == "":
            request_id = str(uuid.uuid4())
        body = {}
        if policy is not None:
            body["policy"] = policy.as_dict()
        if request_id is not None:
            body["request_id"] = request_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.1/accounts/{self._api.account_id}/budget-policies", body=body, headers=headers
        )
        return BudgetPolicy.from_dict(res)

    def delete(self, policy_id: str):
        """Deletes a policy

        :param policy_id: str
          The Id of the policy.


        """

        headers = {
            "Accept": "application/json",
        }

        self._api.do("DELETE", f"/api/2.1/accounts/{self._api.account_id}/budget-policies/{policy_id}", headers=headers)

    def get(self, policy_id: str) -> BudgetPolicy:
        """Retrieves a policy by it's ID.

        :param policy_id: str
          The Id of the policy.

        :returns: :class:`BudgetPolicy`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.1/accounts/{self._api.account_id}/budget-policies/{policy_id}", headers=headers
        )
        return BudgetPolicy.from_dict(res)

    def list(
        self,
        *,
        filter_by: Optional[Filter] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        sort_spec: Optional[SortSpec] = None,
    ) -> Iterator[BudgetPolicy]:
        """Lists all policies. Policies are returned in the alphabetically ascending order of their names.

        :param filter_by: :class:`Filter` (optional)
          A filter to apply to the list of policies.
        :param page_size: int (optional)
          The maximum number of budget policies to return. If unspecified, at most 100 budget policies will be
          returned. The maximum value is 1000; values above 1000 will be coerced to 1000.
        :param page_token: str (optional)
          A page token, received from a previous `ListServerlessPolicies` call. Provide this to retrieve the
          subsequent page. If unspecified, the first page will be returned.

          When paginating, all other parameters provided to `ListServerlessPoliciesRequest` must match the
          call that provided the page token.
        :param sort_spec: :class:`SortSpec` (optional)
          The sort specification.

        :returns: Iterator over :class:`BudgetPolicy`
        """

        query = {}
        if filter_by is not None:
            query["filter_by"] = filter_by.as_dict()
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        if sort_spec is not None:
            query["sort_spec"] = sort_spec.as_dict()
        headers = {
            "Accept": "application/json",
        }

        while True:
            json = self._api.do(
                "GET", f"/api/2.1/accounts/{self._api.account_id}/budget-policies", query=query, headers=headers
            )
            if "policies" in json:
                for v in json["policies"]:
                    yield BudgetPolicy.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self, policy_id: str, policy: BudgetPolicy, *, limit_config: Optional[LimitConfig] = None
    ) -> BudgetPolicy:
        """Updates a policy

        :param policy_id: str
          The Id of the policy. This field is generated by Databricks and globally unique.
        :param policy: :class:`BudgetPolicy`
          The policy to update. `creator_user_id` cannot be specified in the request. All other fields must be
          specified even if not changed. The `policy_id` is used to identify the policy to update.
        :param limit_config: :class:`LimitConfig` (optional)
          DEPRECATED. This is redundant field as LimitConfig is part of the BudgetPolicy

        :returns: :class:`BudgetPolicy`
        """

        body = policy.as_dict()
        query = {}
        if limit_config is not None:
            query["limit_config"] = limit_config.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.1/accounts/{self._api.account_id}/budget-policies/{policy_id}",
            query=query,
            body=body,
            headers=headers,
        )
        return BudgetPolicy.from_dict(res)


class BudgetsAPI:
    """These APIs manage budget configurations for this account. Budgets enable you to monitor usage across your
    account. You can set up budgets to either track account-wide spending, or apply filters to track the
    spending of specific teams, projects, or workspaces."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, budget: CreateBudgetConfigurationBudget) -> CreateBudgetConfigurationResponse:
        """Create a new budget configuration for an account. For full details, see
        https://docs.databricks.com/en/admin/account-settings/budgets.html.

        :param budget: :class:`CreateBudgetConfigurationBudget`
          Properties of the new budget configuration.

        :returns: :class:`CreateBudgetConfigurationResponse`
        """

        body = {}
        if budget is not None:
            body["budget"] = budget.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do("POST", f"/api/2.1/accounts/{self._api.account_id}/budgets", body=body, headers=headers)
        return CreateBudgetConfigurationResponse.from_dict(res)

    def delete(self, budget_id: str):
        """Deletes a budget configuration for an account. Both account and budget configuration are specified by
        ID. This cannot be undone.

        :param budget_id: str
          The Databricks budget configuration ID.


        """

        headers = {
            "Accept": "application/json",
        }

        self._api.do("DELETE", f"/api/2.1/accounts/{self._api.account_id}/budgets/{budget_id}", headers=headers)

    def get(self, budget_id: str) -> GetBudgetConfigurationResponse:
        """Gets a budget configuration for an account. Both account and budget configuration are specified by ID.

        :param budget_id: str
          The budget configuration ID

        :returns: :class:`GetBudgetConfigurationResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.1/accounts/{self._api.account_id}/budgets/{budget_id}", headers=headers)
        return GetBudgetConfigurationResponse.from_dict(res)

    def list(self, *, page_token: Optional[str] = None) -> Iterator[BudgetConfiguration]:
        """Gets all budgets associated with this account.

        :param page_token: str (optional)
          A page token received from a previous get all budget configurations call. This token can be used to
          retrieve the subsequent page. Requests first page if absent.

        :returns: Iterator over :class:`BudgetConfiguration`
        """

        query = {}
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        while True:
            json = self._api.do(
                "GET", f"/api/2.1/accounts/{self._api.account_id}/budgets", query=query, headers=headers
            )
            if "budgets" in json:
                for v in json["budgets"]:
                    yield BudgetConfiguration.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(self, budget_id: str, budget: UpdateBudgetConfigurationBudget) -> UpdateBudgetConfigurationResponse:
        """Updates a budget configuration for an account. Both account and budget configuration are specified by
        ID.

        :param budget_id: str
          The Databricks budget configuration ID.
        :param budget: :class:`UpdateBudgetConfigurationBudget`
          The updated budget. This will overwrite the budget specified by the budget ID.

        :returns: :class:`UpdateBudgetConfigurationResponse`
        """

        body = {}
        if budget is not None:
            body["budget"] = budget.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PUT", f"/api/2.1/accounts/{self._api.account_id}/budgets/{budget_id}", body=body, headers=headers
        )
        return UpdateBudgetConfigurationResponse.from_dict(res)


class LogDeliveryAPI:
    """These APIs manage log delivery configurations for this account. The two supported log types for this API
    are _billable usage logs_ and _audit logs_. This feature is in Public Preview. This feature works with all
    account ID types.

    Log delivery works with all account types. However, if your account is on the E2 version of the platform
    or on a select custom plan that allows multiple workspaces per account, you can optionally configure
    different storage destinations for each workspace. Log delivery status is also provided to know the latest
    status of log delivery attempts.

    The high-level flow of billable usage delivery:

    1. **Create storage**: In AWS, [create a new AWS S3 bucket] with a specific bucket policy. Using
    Databricks APIs, call the Account API to create a [storage configuration object](:method:Storage/Create)
    that uses the bucket name.

    2. **Create credentials**: In AWS, create the appropriate AWS IAM role. For full details, including the
    required IAM role policies and trust relationship, see [Billable usage log delivery]. Using Databricks
    APIs, call the Account API to create a [credential configuration object](:method:Credentials/Create) that
    uses the IAM role's ARN.

    3. **Create log delivery configuration**: Using Databricks APIs, call the Account API to [create a log
    delivery configuration](:method:LogDelivery/Create) that uses the credential and storage configuration
    objects from previous steps. You can specify if the logs should include all events of that log type in
    your account (_Account level_ delivery) or only events for a specific set of workspaces (_workspace level_
    delivery). Account level log delivery applies to all current and future workspaces plus account level
    logs, while workspace level log delivery solely delivers logs related to the specified workspaces. You can
    create multiple types of delivery configurations per account.

    For billable usage delivery: * For more information about billable usage logs, see [Billable usage log
    delivery]. For the CSV schema, see the [Usage page]. * The delivery location is
    `<bucket-name>/<prefix>/billable-usage/csv/`, where `<prefix>` is the name of the optional delivery path
    prefix you set up during log delivery configuration. Files are named
    `workspaceId=<workspace-id>-usageMonth=<month>.csv`. * All billable usage logs apply to specific
    workspaces (_workspace level_ logs). You can aggregate usage for your entire account by creating an
    _account level_ delivery configuration that delivers logs for all current and future workspaces in your
    account. * The files are delivered daily by overwriting the month's CSV file for each workspace.

    For audit log delivery: * For more information about about audit log delivery, see [Audit log delivery],
    which includes information about the used JSON schema. * The delivery location is
    `<bucket-name>/<delivery-path-prefix>/workspaceId=<workspaceId>/date=<yyyy-mm-dd>/auditlogs_<internal-id>.json`.
    Files may get overwritten with the same content multiple times to achieve exactly-once delivery. * If the
    audit log delivery configuration included specific workspace IDs, only _workspace-level_ audit logs for
    those workspaces are delivered. If the log delivery configuration applies to the entire account (_account
    level_ delivery configuration), the audit log delivery includes workspace-level audit logs for all
    workspaces in the account as well as account-level audit logs. See [Audit log delivery] for details. *
    Auditable events are typically available in logs within 15 minutes.

    [Audit log delivery]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
    [Billable usage log delivery]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html
    [Usage page]: https://docs.databricks.com/administration-guide/account-settings/usage.html
    [create a new AWS S3 bucket]: https://docs.databricks.com/administration-guide/account-api/aws-storage.html"""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, log_delivery_configuration: CreateLogDeliveryConfigurationParams
    ) -> WrappedLogDeliveryConfiguration:
        """Creates a new Databricks log delivery configuration to enable delivery of the specified type of logs
        to your storage location. This requires that you already created a [credential
        object](:method:Credentials/Create) (which encapsulates a cross-account service IAM role) and a
        [storage configuration object](:method:Storage/Create) (which encapsulates an S3 bucket).

        For full details, including the required IAM role policies and bucket policies, see [Deliver and
        access billable usage logs] or [Configure audit logging].

        **Note**: There is a limit on the number of log delivery configurations available per account (each
        limit applies separately to each log type including billable usage and audit logs). You can create a
        maximum of two enabled account-level delivery configurations (configurations without a workspace
        filter) per type. Additionally, you can create two enabled workspace-level delivery configurations per
        workspace for each log type, which means that the same workspace ID can occur in the workspace filter
        for no more than two delivery configurations per log type.

        You cannot delete a log delivery configuration, but you can disable it (see [Enable or disable log
        delivery configuration](:method:LogDelivery/PatchStatus)).

        [Configure audit logging]: https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
        [Deliver and access billable usage logs]: https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html

        :param log_delivery_configuration: :class:`CreateLogDeliveryConfigurationParams`

        :returns: :class:`WrappedLogDeliveryConfiguration`
        """

        body = {}
        if log_delivery_configuration is not None:
            body["log_delivery_configuration"] = log_delivery_configuration.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do("POST", f"/api/2.0/accounts/{self._api.account_id}/log-delivery", body=body, headers=headers)
        return WrappedLogDeliveryConfiguration.from_dict(res)

    def get(self, log_delivery_configuration_id: str) -> GetLogDeliveryConfigurationResponse:
        """Gets a Databricks log delivery configuration object for an account, both specified by ID.

        :param log_delivery_configuration_id: str
          The log delivery configuration id of customer

        :returns: :class:`GetLogDeliveryConfigurationResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/log-delivery/{log_delivery_configuration_id}",
            headers=headers,
        )
        return GetLogDeliveryConfigurationResponse.from_dict(res)

    def list(
        self,
        *,
        credentials_id: Optional[str] = None,
        page_token: Optional[str] = None,
        status: Optional[LogDeliveryConfigStatus] = None,
        storage_configuration_id: Optional[str] = None,
    ) -> Iterator[LogDeliveryConfiguration]:
        """Gets all Databricks log delivery configurations associated with an account specified by ID.

        :param credentials_id: str (optional)
          The Credentials id to filter the search results with
        :param page_token: str (optional)
          A page token received from a previous get all budget configurations call. This token can be used to
          retrieve the subsequent page. Requests first page if absent.
        :param status: :class:`LogDeliveryConfigStatus` (optional)
          The log delivery status to filter the search results with
        :param storage_configuration_id: str (optional)
          The Storage Configuration id to filter the search results with

        :returns: Iterator over :class:`LogDeliveryConfiguration`
        """

        query = {}
        if credentials_id is not None:
            query["credentials_id"] = credentials_id
        if page_token is not None:
            query["page_token"] = page_token
        if status is not None:
            query["status"] = status.value
        if storage_configuration_id is not None:
            query["storage_configuration_id"] = storage_configuration_id
        headers = {
            "Accept": "application/json",
        }

        while True:
            json = self._api.do(
                "GET", f"/api/2.0/accounts/{self._api.account_id}/log-delivery", query=query, headers=headers
            )
            if "log_delivery_configurations" in json:
                for v in json["log_delivery_configurations"]:
                    yield LogDeliveryConfiguration.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def patch_status(self, log_delivery_configuration_id: str, status: LogDeliveryConfigStatus):
        """Enables or disables a log delivery configuration. Deletion of delivery configurations is not
        supported, so disable log delivery configurations that are no longer needed. Note that you can't
        re-enable a delivery configuration if this would violate the delivery configuration limits described
        under [Create log delivery](:method:LogDelivery/Create).

        :param log_delivery_configuration_id: str
          The log delivery configuration id of customer
        :param status: :class:`LogDeliveryConfigStatus`
          Status of log delivery configuration. Set to `ENABLED` (enabled) or `DISABLED` (disabled). Defaults
          to `ENABLED`. You can [enable or disable the
          configuration](#operation/patch-log-delivery-config-status) later. Deletion of a configuration is
          not supported, so disable a log delivery configuration that is no longer needed.


        """

        body = {}
        if status is not None:
            body["status"] = status.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/log-delivery/{log_delivery_configuration_id}",
            body=body,
            headers=headers,
        )


class UsageDashboardsAPI:
    """These APIs manage usage dashboards for this account. Usage dashboards enable you to gain insights into
    your usage with pre-built dashboards: visualize breakdowns, analyze tag attributions, and identify cost
    drivers."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        *,
        dashboard_type: Optional[UsageDashboardType] = None,
        major_version: Optional[UsageDashboardMajorVersion] = None,
        workspace_id: Optional[int] = None,
    ) -> CreateBillingUsageDashboardResponse:
        """Create a usage dashboard specified by workspaceId, accountId, and dashboard type.

        :param dashboard_type: :class:`UsageDashboardType` (optional)
          Workspace level usage dashboard shows usage data for the specified workspace ID. Global level usage
          dashboard shows usage data for all workspaces in the account.
        :param major_version: :class:`UsageDashboardMajorVersion` (optional)
          The major version of the usage dashboard template to use. Defaults to VERSION_1.
        :param workspace_id: int (optional)
          The workspace ID of the workspace in which the usage dashboard is created.

        :returns: :class:`CreateBillingUsageDashboardResponse`
        """

        body = {}
        if dashboard_type is not None:
            body["dashboard_type"] = dashboard_type.value
        if major_version is not None:
            body["major_version"] = major_version.value
        if workspace_id is not None:
            body["workspace_id"] = workspace_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do("POST", f"/api/2.0/accounts/{self._api.account_id}/dashboard", body=body, headers=headers)
        return CreateBillingUsageDashboardResponse.from_dict(res)

    def get(
        self, *, dashboard_type: Optional[UsageDashboardType] = None, workspace_id: Optional[int] = None
    ) -> GetBillingUsageDashboardResponse:
        """Get a usage dashboard specified by workspaceId, accountId, and dashboard type.

        :param dashboard_type: :class:`UsageDashboardType` (optional)
          Workspace level usage dashboard shows usage data for the specified workspace ID. Global level usage
          dashboard shows usage data for all workspaces in the account.
        :param workspace_id: int (optional)
          The workspace ID of the workspace in which the usage dashboard is created.

        :returns: :class:`GetBillingUsageDashboardResponse`
        """

        query = {}
        if dashboard_type is not None:
            query["dashboard_type"] = dashboard_type.value
        if workspace_id is not None:
            query["workspace_id"] = workspace_id
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/dashboard", query=query, headers=headers)
        return GetBillingUsageDashboardResponse.from_dict(res)
