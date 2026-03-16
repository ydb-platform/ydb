# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service._internal import (_enum, _from_dict,
                                              _repeated_dict, _repeated_enum)

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AccountIpAccessEnable:
    acct_ip_acl_enable: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the AccountIpAccessEnable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.acct_ip_acl_enable:
            body["acct_ip_acl_enable"] = self.acct_ip_acl_enable.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountIpAccessEnable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.acct_ip_acl_enable:
            body["acct_ip_acl_enable"] = self.acct_ip_acl_enable
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountIpAccessEnable:
        """Deserializes the AccountIpAccessEnable from a dictionary."""
        return cls(
            acct_ip_acl_enable=_from_dict(d, "acct_ip_acl_enable", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class AccountNetworkPolicy:
    account_id: Optional[str] = None
    """The associated account ID for this Network Policy object."""

    egress: Optional[NetworkPolicyEgress] = None
    """The network policies applying for egress traffic."""

    network_policy_id: Optional[str] = None
    """The unique identifier for the network policy."""

    def as_dict(self) -> dict:
        """Serializes the AccountNetworkPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.egress:
            body["egress"] = self.egress.as_dict()
        if self.network_policy_id is not None:
            body["network_policy_id"] = self.network_policy_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AccountNetworkPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.egress:
            body["egress"] = self.egress
        if self.network_policy_id is not None:
            body["network_policy_id"] = self.network_policy_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AccountNetworkPolicy:
        """Deserializes the AccountNetworkPolicy from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            egress=_from_dict(d, "egress", NetworkPolicyEgress),
            network_policy_id=d.get("network_policy_id", None),
        )


@dataclass
class AibiDashboardEmbeddingAccessPolicy:
    access_policy_type: AibiDashboardEmbeddingAccessPolicyAccessPolicyType

    def as_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingAccessPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_policy_type is not None:
            body["access_policy_type"] = self.access_policy_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingAccessPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_policy_type is not None:
            body["access_policy_type"] = self.access_policy_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AibiDashboardEmbeddingAccessPolicy:
        """Deserializes the AibiDashboardEmbeddingAccessPolicy from a dictionary."""
        return cls(
            access_policy_type=_enum(d, "access_policy_type", AibiDashboardEmbeddingAccessPolicyAccessPolicyType)
        )


class AibiDashboardEmbeddingAccessPolicyAccessPolicyType(Enum):

    ALLOW_ALL_DOMAINS = "ALLOW_ALL_DOMAINS"
    ALLOW_APPROVED_DOMAINS = "ALLOW_APPROVED_DOMAINS"
    DENY_ALL_DOMAINS = "DENY_ALL_DOMAINS"


@dataclass
class AibiDashboardEmbeddingAccessPolicySetting:
    aibi_dashboard_embedding_access_policy: AibiDashboardEmbeddingAccessPolicy

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingAccessPolicySetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aibi_dashboard_embedding_access_policy:
            body["aibi_dashboard_embedding_access_policy"] = self.aibi_dashboard_embedding_access_policy.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingAccessPolicySetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aibi_dashboard_embedding_access_policy:
            body["aibi_dashboard_embedding_access_policy"] = self.aibi_dashboard_embedding_access_policy
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AibiDashboardEmbeddingAccessPolicySetting:
        """Deserializes the AibiDashboardEmbeddingAccessPolicySetting from a dictionary."""
        return cls(
            aibi_dashboard_embedding_access_policy=_from_dict(
                d, "aibi_dashboard_embedding_access_policy", AibiDashboardEmbeddingAccessPolicy
            ),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class AibiDashboardEmbeddingApprovedDomains:
    approved_domains: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingApprovedDomains into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.approved_domains:
            body["approved_domains"] = [v for v in self.approved_domains]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingApprovedDomains into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.approved_domains:
            body["approved_domains"] = self.approved_domains
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AibiDashboardEmbeddingApprovedDomains:
        """Deserializes the AibiDashboardEmbeddingApprovedDomains from a dictionary."""
        return cls(approved_domains=d.get("approved_domains", None))


@dataclass
class AibiDashboardEmbeddingApprovedDomainsSetting:
    aibi_dashboard_embedding_approved_domains: AibiDashboardEmbeddingApprovedDomains

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingApprovedDomainsSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aibi_dashboard_embedding_approved_domains:
            body["aibi_dashboard_embedding_approved_domains"] = self.aibi_dashboard_embedding_approved_domains.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AibiDashboardEmbeddingApprovedDomainsSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aibi_dashboard_embedding_approved_domains:
            body["aibi_dashboard_embedding_approved_domains"] = self.aibi_dashboard_embedding_approved_domains
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AibiDashboardEmbeddingApprovedDomainsSetting:
        """Deserializes the AibiDashboardEmbeddingApprovedDomainsSetting from a dictionary."""
        return cls(
            aibi_dashboard_embedding_approved_domains=_from_dict(
                d, "aibi_dashboard_embedding_approved_domains", AibiDashboardEmbeddingApprovedDomains
            ),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class AutomaticClusterUpdateSetting:
    automatic_cluster_update_workspace: ClusterAutoRestartMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the AutomaticClusterUpdateSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.automatic_cluster_update_workspace:
            body["automatic_cluster_update_workspace"] = self.automatic_cluster_update_workspace.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AutomaticClusterUpdateSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.automatic_cluster_update_workspace:
            body["automatic_cluster_update_workspace"] = self.automatic_cluster_update_workspace
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AutomaticClusterUpdateSetting:
        """Deserializes the AutomaticClusterUpdateSetting from a dictionary."""
        return cls(
            automatic_cluster_update_workspace=_from_dict(
                d, "automatic_cluster_update_workspace", ClusterAutoRestartMessage
            ),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class BooleanMessage:
    value: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the BooleanMessage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BooleanMessage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BooleanMessage:
        """Deserializes the BooleanMessage from a dictionary."""
        return cls(value=d.get("value", None))


@dataclass
class ClusterAutoRestartMessage:
    can_toggle: Optional[bool] = None

    enabled: Optional[bool] = None

    enablement_details: Optional[ClusterAutoRestartMessageEnablementDetails] = None

    maintenance_window: Optional[ClusterAutoRestartMessageMaintenanceWindow] = None

    restart_even_if_no_updates_available: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.can_toggle is not None:
            body["can_toggle"] = self.can_toggle
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.enablement_details:
            body["enablement_details"] = self.enablement_details.as_dict()
        if self.maintenance_window:
            body["maintenance_window"] = self.maintenance_window.as_dict()
        if self.restart_even_if_no_updates_available is not None:
            body["restart_even_if_no_updates_available"] = self.restart_even_if_no_updates_available
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.can_toggle is not None:
            body["can_toggle"] = self.can_toggle
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.enablement_details:
            body["enablement_details"] = self.enablement_details
        if self.maintenance_window:
            body["maintenance_window"] = self.maintenance_window
        if self.restart_even_if_no_updates_available is not None:
            body["restart_even_if_no_updates_available"] = self.restart_even_if_no_updates_available
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClusterAutoRestartMessage:
        """Deserializes the ClusterAutoRestartMessage from a dictionary."""
        return cls(
            can_toggle=d.get("can_toggle", None),
            enabled=d.get("enabled", None),
            enablement_details=_from_dict(d, "enablement_details", ClusterAutoRestartMessageEnablementDetails),
            maintenance_window=_from_dict(d, "maintenance_window", ClusterAutoRestartMessageMaintenanceWindow),
            restart_even_if_no_updates_available=d.get("restart_even_if_no_updates_available", None),
        )


@dataclass
class ClusterAutoRestartMessageEnablementDetails:
    """Contains an information about the enablement status judging (e.g. whether the enterprise tier is
    enabled) This is only additional information that MUST NOT be used to decide whether the setting
    is enabled or not. This is intended to use only for purposes like showing an error message to
    the customer with the additional details. For example, using these details we can check why
    exactly the feature is disabled for this customer."""

    forced_for_compliance_mode: Optional[bool] = None
    """The feature is force enabled if compliance mode is active"""

    unavailable_for_disabled_entitlement: Optional[bool] = None
    """The feature is unavailable if the corresponding entitlement disabled (see
    getShieldEntitlementEnable)"""

    unavailable_for_non_enterprise_tier: Optional[bool] = None
    """The feature is unavailable if the customer doesn't have enterprise tier"""

    def as_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageEnablementDetails into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.forced_for_compliance_mode is not None:
            body["forced_for_compliance_mode"] = self.forced_for_compliance_mode
        if self.unavailable_for_disabled_entitlement is not None:
            body["unavailable_for_disabled_entitlement"] = self.unavailable_for_disabled_entitlement
        if self.unavailable_for_non_enterprise_tier is not None:
            body["unavailable_for_non_enterprise_tier"] = self.unavailable_for_non_enterprise_tier
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageEnablementDetails into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.forced_for_compliance_mode is not None:
            body["forced_for_compliance_mode"] = self.forced_for_compliance_mode
        if self.unavailable_for_disabled_entitlement is not None:
            body["unavailable_for_disabled_entitlement"] = self.unavailable_for_disabled_entitlement
        if self.unavailable_for_non_enterprise_tier is not None:
            body["unavailable_for_non_enterprise_tier"] = self.unavailable_for_non_enterprise_tier
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClusterAutoRestartMessageEnablementDetails:
        """Deserializes the ClusterAutoRestartMessageEnablementDetails from a dictionary."""
        return cls(
            forced_for_compliance_mode=d.get("forced_for_compliance_mode", None),
            unavailable_for_disabled_entitlement=d.get("unavailable_for_disabled_entitlement", None),
            unavailable_for_non_enterprise_tier=d.get("unavailable_for_non_enterprise_tier", None),
        )


@dataclass
class ClusterAutoRestartMessageMaintenanceWindow:
    week_day_based_schedule: Optional[ClusterAutoRestartMessageMaintenanceWindowWeekDayBasedSchedule] = None

    def as_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageMaintenanceWindow into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.week_day_based_schedule:
            body["week_day_based_schedule"] = self.week_day_based_schedule.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageMaintenanceWindow into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.week_day_based_schedule:
            body["week_day_based_schedule"] = self.week_day_based_schedule
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClusterAutoRestartMessageMaintenanceWindow:
        """Deserializes the ClusterAutoRestartMessageMaintenanceWindow from a dictionary."""
        return cls(
            week_day_based_schedule=_from_dict(
                d, "week_day_based_schedule", ClusterAutoRestartMessageMaintenanceWindowWeekDayBasedSchedule
            )
        )


class ClusterAutoRestartMessageMaintenanceWindowDayOfWeek(Enum):

    FRIDAY = "FRIDAY"
    MONDAY = "MONDAY"
    SATURDAY = "SATURDAY"
    SUNDAY = "SUNDAY"
    THURSDAY = "THURSDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"


@dataclass
class ClusterAutoRestartMessageMaintenanceWindowWeekDayBasedSchedule:
    day_of_week: Optional[ClusterAutoRestartMessageMaintenanceWindowDayOfWeek] = None

    frequency: Optional[ClusterAutoRestartMessageMaintenanceWindowWeekDayFrequency] = None

    window_start_time: Optional[ClusterAutoRestartMessageMaintenanceWindowWindowStartTime] = None

    def as_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageMaintenanceWindowWeekDayBasedSchedule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.day_of_week is not None:
            body["day_of_week"] = self.day_of_week.value
        if self.frequency is not None:
            body["frequency"] = self.frequency.value
        if self.window_start_time:
            body["window_start_time"] = self.window_start_time.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageMaintenanceWindowWeekDayBasedSchedule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.day_of_week is not None:
            body["day_of_week"] = self.day_of_week
        if self.frequency is not None:
            body["frequency"] = self.frequency
        if self.window_start_time:
            body["window_start_time"] = self.window_start_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClusterAutoRestartMessageMaintenanceWindowWeekDayBasedSchedule:
        """Deserializes the ClusterAutoRestartMessageMaintenanceWindowWeekDayBasedSchedule from a dictionary."""
        return cls(
            day_of_week=_enum(d, "day_of_week", ClusterAutoRestartMessageMaintenanceWindowDayOfWeek),
            frequency=_enum(d, "frequency", ClusterAutoRestartMessageMaintenanceWindowWeekDayFrequency),
            window_start_time=_from_dict(
                d, "window_start_time", ClusterAutoRestartMessageMaintenanceWindowWindowStartTime
            ),
        )


class ClusterAutoRestartMessageMaintenanceWindowWeekDayFrequency(Enum):

    EVERY_WEEK = "EVERY_WEEK"
    FIRST_AND_THIRD_OF_MONTH = "FIRST_AND_THIRD_OF_MONTH"
    FIRST_OF_MONTH = "FIRST_OF_MONTH"
    FOURTH_OF_MONTH = "FOURTH_OF_MONTH"
    SECOND_AND_FOURTH_OF_MONTH = "SECOND_AND_FOURTH_OF_MONTH"
    SECOND_OF_MONTH = "SECOND_OF_MONTH"
    THIRD_OF_MONTH = "THIRD_OF_MONTH"


@dataclass
class ClusterAutoRestartMessageMaintenanceWindowWindowStartTime:
    hours: Optional[int] = None

    minutes: Optional[int] = None

    def as_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageMaintenanceWindowWindowStartTime into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.hours is not None:
            body["hours"] = self.hours
        if self.minutes is not None:
            body["minutes"] = self.minutes
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClusterAutoRestartMessageMaintenanceWindowWindowStartTime into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.hours is not None:
            body["hours"] = self.hours
        if self.minutes is not None:
            body["minutes"] = self.minutes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClusterAutoRestartMessageMaintenanceWindowWindowStartTime:
        """Deserializes the ClusterAutoRestartMessageMaintenanceWindowWindowStartTime from a dictionary."""
        return cls(hours=d.get("hours", None), minutes=d.get("minutes", None))


@dataclass
class ComplianceSecurityProfile:
    """SHIELD feature: CSP"""

    compliance_standards: Optional[List[ComplianceStandard]] = None
    """Set by customers when they request Compliance Security Profile (CSP)"""

    is_enabled: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the ComplianceSecurityProfile into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.compliance_standards:
            body["compliance_standards"] = [v.value for v in self.compliance_standards]
        if self.is_enabled is not None:
            body["is_enabled"] = self.is_enabled
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ComplianceSecurityProfile into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.compliance_standards:
            body["compliance_standards"] = self.compliance_standards
        if self.is_enabled is not None:
            body["is_enabled"] = self.is_enabled
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ComplianceSecurityProfile:
        """Deserializes the ComplianceSecurityProfile from a dictionary."""
        return cls(
            compliance_standards=_repeated_enum(d, "compliance_standards", ComplianceStandard),
            is_enabled=d.get("is_enabled", None),
        )


@dataclass
class ComplianceSecurityProfileSetting:
    compliance_security_profile_workspace: ComplianceSecurityProfile

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the ComplianceSecurityProfileSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.compliance_security_profile_workspace:
            body["compliance_security_profile_workspace"] = self.compliance_security_profile_workspace.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ComplianceSecurityProfileSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.compliance_security_profile_workspace:
            body["compliance_security_profile_workspace"] = self.compliance_security_profile_workspace
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ComplianceSecurityProfileSetting:
        """Deserializes the ComplianceSecurityProfileSetting from a dictionary."""
        return cls(
            compliance_security_profile_workspace=_from_dict(
                d, "compliance_security_profile_workspace", ComplianceSecurityProfile
            ),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


class ComplianceStandard(Enum):
    """Compliance standard for SHIELD customers. See README.md for how instructions of how to add new
    standards."""

    CANADA_PROTECTED_B = "CANADA_PROTECTED_B"
    CYBER_ESSENTIAL_PLUS = "CYBER_ESSENTIAL_PLUS"
    FEDRAMP_HIGH = "FEDRAMP_HIGH"
    FEDRAMP_IL5 = "FEDRAMP_IL5"
    FEDRAMP_MODERATE = "FEDRAMP_MODERATE"
    GERMANY_C5 = "GERMANY_C5"
    GERMANY_TISAX = "GERMANY_TISAX"
    HIPAA = "HIPAA"
    HITRUST = "HITRUST"
    IRAP_PROTECTED = "IRAP_PROTECTED"
    ISMAP = "ISMAP"
    ITAR_EAR = "ITAR_EAR"
    K_FSI = "K_FSI"
    NONE = "NONE"
    PCI_DSS = "PCI_DSS"


@dataclass
class Config:
    email: Optional[EmailConfig] = None

    generic_webhook: Optional[GenericWebhookConfig] = None

    microsoft_teams: Optional[MicrosoftTeamsConfig] = None

    pagerduty: Optional[PagerdutyConfig] = None

    slack: Optional[SlackConfig] = None

    def as_dict(self) -> dict:
        """Serializes the Config into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.email:
            body["email"] = self.email.as_dict()
        if self.generic_webhook:
            body["generic_webhook"] = self.generic_webhook.as_dict()
        if self.microsoft_teams:
            body["microsoft_teams"] = self.microsoft_teams.as_dict()
        if self.pagerduty:
            body["pagerduty"] = self.pagerduty.as_dict()
        if self.slack:
            body["slack"] = self.slack.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Config into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.email:
            body["email"] = self.email
        if self.generic_webhook:
            body["generic_webhook"] = self.generic_webhook
        if self.microsoft_teams:
            body["microsoft_teams"] = self.microsoft_teams
        if self.pagerduty:
            body["pagerduty"] = self.pagerduty
        if self.slack:
            body["slack"] = self.slack
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Config:
        """Deserializes the Config from a dictionary."""
        return cls(
            email=_from_dict(d, "email", EmailConfig),
            generic_webhook=_from_dict(d, "generic_webhook", GenericWebhookConfig),
            microsoft_teams=_from_dict(d, "microsoft_teams", MicrosoftTeamsConfig),
            pagerduty=_from_dict(d, "pagerduty", PagerdutyConfig),
            slack=_from_dict(d, "slack", SlackConfig),
        )


@dataclass
class CreateIpAccessListResponse:
    """An IP access list was successfully created."""

    ip_access_list: Optional[IpAccessListInfo] = None

    def as_dict(self) -> dict:
        """Serializes the CreateIpAccessListResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateIpAccessListResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateIpAccessListResponse:
        """Deserializes the CreateIpAccessListResponse from a dictionary."""
        return cls(ip_access_list=_from_dict(d, "ip_access_list", IpAccessListInfo))


@dataclass
class CreateNetworkConnectivityConfiguration:
    """Properties of the new network connectivity configuration."""

    name: str
    """The name of the network connectivity configuration. The name can contain alphanumeric
    characters, hyphens, and underscores. The length must be between 3 and 30 characters. The name
    must match the regular expression ^[0-9a-zA-Z-_]{3,30}$"""

    region: str
    """The region for the network connectivity configuration. Only workspaces in the same region can be
    attached to the network connectivity configuration."""

    def as_dict(self) -> dict:
        """Serializes the CreateNetworkConnectivityConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.region is not None:
            body["region"] = self.region
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateNetworkConnectivityConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.region is not None:
            body["region"] = self.region
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateNetworkConnectivityConfiguration:
        """Deserializes the CreateNetworkConnectivityConfiguration from a dictionary."""
        return cls(name=d.get("name", None), region=d.get("region", None))


@dataclass
class CreateOboTokenResponse:
    """An on-behalf token was successfully created for the service principal."""

    token_info: Optional[TokenInfo] = None

    token_value: Optional[str] = None
    """Value of the token."""

    def as_dict(self) -> dict:
        """Serializes the CreateOboTokenResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.token_info:
            body["token_info"] = self.token_info.as_dict()
        if self.token_value is not None:
            body["token_value"] = self.token_value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateOboTokenResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.token_info:
            body["token_info"] = self.token_info
        if self.token_value is not None:
            body["token_value"] = self.token_value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateOboTokenResponse:
        """Deserializes the CreateOboTokenResponse from a dictionary."""
        return cls(token_info=_from_dict(d, "token_info", TokenInfo), token_value=d.get("token_value", None))


@dataclass
class CreatePrivateEndpointRule:
    """Properties of the new private endpoint rule. Note that you must approve the endpoint in Azure
    portal after initialization."""

    domain_names: Optional[List[str]] = None
    """Only used by private endpoints to customer-managed private endpoint services.
    
    Domain names of target private link service. When updating this field, the full list of target
    domain_names must be specified."""

    endpoint_service: Optional[str] = None
    """The full target AWS endpoint service name that connects to the destination resources of the
    private endpoint."""

    error_message: Optional[str] = None

    group_id: Optional[str] = None
    """Not used by customer-managed private endpoint services.
    
    The sub-resource type (group ID) of the target resource. Note that to connect to workspace root
    storage (root DBFS), you need two endpoints, one for blob and one for dfs."""

    resource_id: Optional[str] = None
    """The Azure resource ID of the target resource."""

    resource_names: Optional[List[str]] = None
    """Only used by private endpoints towards AWS S3 service.
    
    The globally unique S3 bucket names that will be accessed via the VPC endpoint. The bucket names
    must be in the same region as the NCC/endpoint service. When updating this field, we perform
    full update on this field. Please ensure a full list of desired resource_names is provided."""

    def as_dict(self) -> dict:
        """Serializes the CreatePrivateEndpointRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.domain_names:
            body["domain_names"] = [v for v in self.domain_names]
        if self.endpoint_service is not None:
            body["endpoint_service"] = self.endpoint_service
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.group_id is not None:
            body["group_id"] = self.group_id
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.resource_names:
            body["resource_names"] = [v for v in self.resource_names]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreatePrivateEndpointRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.domain_names:
            body["domain_names"] = self.domain_names
        if self.endpoint_service is not None:
            body["endpoint_service"] = self.endpoint_service
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.group_id is not None:
            body["group_id"] = self.group_id
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.resource_names:
            body["resource_names"] = self.resource_names
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreatePrivateEndpointRule:
        """Deserializes the CreatePrivateEndpointRule from a dictionary."""
        return cls(
            domain_names=d.get("domain_names", None),
            endpoint_service=d.get("endpoint_service", None),
            error_message=d.get("error_message", None),
            group_id=d.get("group_id", None),
            resource_id=d.get("resource_id", None),
            resource_names=d.get("resource_names", None),
        )


@dataclass
class CreateTokenResponse:
    token_info: Optional[PublicTokenInfo] = None
    """The information for the new token."""

    token_value: Optional[str] = None
    """The value of the new token."""

    def as_dict(self) -> dict:
        """Serializes the CreateTokenResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.token_info:
            body["token_info"] = self.token_info.as_dict()
        if self.token_value is not None:
            body["token_value"] = self.token_value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateTokenResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.token_info:
            body["token_info"] = self.token_info
        if self.token_value is not None:
            body["token_value"] = self.token_value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateTokenResponse:
        """Deserializes the CreateTokenResponse from a dictionary."""
        return cls(token_info=_from_dict(d, "token_info", PublicTokenInfo), token_value=d.get("token_value", None))


@dataclass
class CspEnablementAccount:
    """Account level policy for CSP"""

    compliance_standards: Optional[List[ComplianceStandard]] = None
    """Set by customers when they request Compliance Security Profile (CSP) Invariants are enforced in
    Settings policy."""

    is_enforced: Optional[bool] = None
    """Enforced = it cannot be overriden at workspace level."""

    def as_dict(self) -> dict:
        """Serializes the CspEnablementAccount into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.compliance_standards:
            body["compliance_standards"] = [v.value for v in self.compliance_standards]
        if self.is_enforced is not None:
            body["is_enforced"] = self.is_enforced
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CspEnablementAccount into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.compliance_standards:
            body["compliance_standards"] = self.compliance_standards
        if self.is_enforced is not None:
            body["is_enforced"] = self.is_enforced
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CspEnablementAccount:
        """Deserializes the CspEnablementAccount from a dictionary."""
        return cls(
            compliance_standards=_repeated_enum(d, "compliance_standards", ComplianceStandard),
            is_enforced=d.get("is_enforced", None),
        )


@dataclass
class CspEnablementAccountSetting:
    csp_enablement_account: CspEnablementAccount

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the CspEnablementAccountSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.csp_enablement_account:
            body["csp_enablement_account"] = self.csp_enablement_account.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CspEnablementAccountSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.csp_enablement_account:
            body["csp_enablement_account"] = self.csp_enablement_account
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CspEnablementAccountSetting:
        """Deserializes the CspEnablementAccountSetting from a dictionary."""
        return cls(
            csp_enablement_account=_from_dict(d, "csp_enablement_account", CspEnablementAccount),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRule:
    """Properties of the new private endpoint rule. Note that for private endpoints towards a VPC
    endpoint service behind a customer-managed NLB, you must approve the endpoint in AWS console
    after initialization."""

    account_id: Optional[str] = None
    """Databricks account ID. You can find your account ID from the Accounts Console."""

    connection_state: Optional[
        CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRulePrivateLinkConnectionState
    ] = None
    """The current status of this private endpoint. The private endpoint rules are effective only if
    the connection state is ESTABLISHED. Remember that you must approve new endpoints on your
    resources in the AWS console before they take effect. The possible values are: - PENDING: The
    endpoint has been created and pending approval. - ESTABLISHED: The endpoint has been approved
    and is ready to use in your serverless compute resources. - REJECTED: Connection was rejected by
    the private link resource owner. - DISCONNECTED: Connection was removed by the private link
    resource owner, the private endpoint becomes informative and should be deleted for clean-up. -
    EXPIRED: If the endpoint is created but not approved in 14 days, it is EXPIRED."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when this object was created."""

    deactivated: Optional[bool] = None
    """Whether this private endpoint is deactivated."""

    deactivated_at: Optional[int] = None
    """Time in epoch milliseconds when this object was deactivated."""

    domain_names: Optional[List[str]] = None
    """Only used by private endpoints towards a VPC endpoint service for customer-managed VPC endpoint
    service.
    
    The target AWS resource FQDNs accessible via the VPC endpoint service. When updating this field,
    we perform full update on this field. Please ensure a full list of desired domain_names is
    provided."""

    enabled: Optional[bool] = None
    """Only used by private endpoints towards an AWS S3 service.
    
    Update this field to activate/deactivate this private endpoint to allow egress access from
    serverless compute resources."""

    endpoint_service: Optional[str] = None
    """The full target AWS endpoint service name that connects to the destination resources of the
    private endpoint."""

    error_message: Optional[str] = None

    network_connectivity_config_id: Optional[str] = None
    """The ID of a network connectivity configuration, which is the parent resource of this private
    endpoint rule object."""

    resource_names: Optional[List[str]] = None
    """Only used by private endpoints towards AWS S3 service.
    
    The globally unique S3 bucket names that will be accessed via the VPC endpoint. The bucket names
    must be in the same region as the NCC/endpoint service. When updating this field, we perform
    full update on this field. Please ensure a full list of desired resource_names is provided."""

    rule_id: Optional[str] = None
    """The ID of a private endpoint rule."""

    updated_time: Optional[int] = None
    """Time in epoch milliseconds when this object was updated."""

    vpc_endpoint_id: Optional[str] = None
    """The AWS VPC endpoint ID. You can use this ID to identify VPC endpoint created by Databricks."""

    def as_dict(self) -> dict:
        """Serializes the CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.connection_state is not None:
            body["connection_state"] = self.connection_state.value
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.deactivated is not None:
            body["deactivated"] = self.deactivated
        if self.deactivated_at is not None:
            body["deactivated_at"] = self.deactivated_at
        if self.domain_names:
            body["domain_names"] = [v for v in self.domain_names]
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.endpoint_service is not None:
            body["endpoint_service"] = self.endpoint_service
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.resource_names:
            body["resource_names"] = [v for v in self.resource_names]
        if self.rule_id is not None:
            body["rule_id"] = self.rule_id
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        if self.vpc_endpoint_id is not None:
            body["vpc_endpoint_id"] = self.vpc_endpoint_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.connection_state is not None:
            body["connection_state"] = self.connection_state
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.deactivated is not None:
            body["deactivated"] = self.deactivated
        if self.deactivated_at is not None:
            body["deactivated_at"] = self.deactivated_at
        if self.domain_names:
            body["domain_names"] = self.domain_names
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.endpoint_service is not None:
            body["endpoint_service"] = self.endpoint_service
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.resource_names:
            body["resource_names"] = self.resource_names
        if self.rule_id is not None:
            body["rule_id"] = self.rule_id
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        if self.vpc_endpoint_id is not None:
            body["vpc_endpoint_id"] = self.vpc_endpoint_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRule:
        """Deserializes the CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRule from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            connection_state=_enum(
                d,
                "connection_state",
                CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRulePrivateLinkConnectionState,
            ),
            creation_time=d.get("creation_time", None),
            deactivated=d.get("deactivated", None),
            deactivated_at=d.get("deactivated_at", None),
            domain_names=d.get("domain_names", None),
            enabled=d.get("enabled", None),
            endpoint_service=d.get("endpoint_service", None),
            error_message=d.get("error_message", None),
            network_connectivity_config_id=d.get("network_connectivity_config_id", None),
            resource_names=d.get("resource_names", None),
            rule_id=d.get("rule_id", None),
            updated_time=d.get("updated_time", None),
            vpc_endpoint_id=d.get("vpc_endpoint_id", None),
        )


class CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRulePrivateLinkConnectionState(Enum):

    CREATE_FAILED = "CREATE_FAILED"
    CREATING = "CREATING"
    DISCONNECTED = "DISCONNECTED"
    ESTABLISHED = "ESTABLISHED"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"
    REJECTED = "REJECTED"


@dataclass
class DashboardEmailSubscriptions:
    boolean_val: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the DashboardEmailSubscriptions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DashboardEmailSubscriptions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DashboardEmailSubscriptions:
        """Deserializes the DashboardEmailSubscriptions from a dictionary."""
        return cls(
            boolean_val=_from_dict(d, "boolean_val", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class DefaultNamespaceSetting:
    """This represents the setting configuration for the default namespace in the Databricks workspace.
    Setting the default catalog for the workspace determines the catalog that is used when queries
    do not reference a fully qualified 3 level name. For example, if the default catalog is set to
    'retail_prod' then a query 'SELECT * FROM myTable' would reference the object
    'retail_prod.default.myTable' (the schema 'default' is always assumed). This setting requires a
    restart of clusters and SQL warehouses to take effect. Additionally, the default namespace only
    applies when using Unity Catalog-enabled compute."""

    namespace: StringMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the DefaultNamespaceSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.namespace:
            body["namespace"] = self.namespace.as_dict()
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DefaultNamespaceSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.namespace:
            body["namespace"] = self.namespace
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DefaultNamespaceSetting:
        """Deserializes the DefaultNamespaceSetting from a dictionary."""
        return cls(
            etag=d.get("etag", None),
            namespace=_from_dict(d, "namespace", StringMessage),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class DefaultWarehouseId:
    string_val: StringMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the DefaultWarehouseId into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        if self.string_val:
            body["string_val"] = self.string_val.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DefaultWarehouseId into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        if self.string_val:
            body["string_val"] = self.string_val
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DefaultWarehouseId:
        """Deserializes the DefaultWarehouseId from a dictionary."""
        return cls(
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
            string_val=_from_dict(d, "string_val", StringMessage),
        )


@dataclass
class DeleteAccountIpAccessEnableResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteAccountIpAccessEnableResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteAccountIpAccessEnableResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteAccountIpAccessEnableResponse:
        """Deserializes the DeleteAccountIpAccessEnableResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteAibiDashboardEmbeddingAccessPolicySettingResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteAibiDashboardEmbeddingAccessPolicySettingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteAibiDashboardEmbeddingAccessPolicySettingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteAibiDashboardEmbeddingAccessPolicySettingResponse:
        """Deserializes the DeleteAibiDashboardEmbeddingAccessPolicySettingResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse:
        """Deserializes the DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteDashboardEmailSubscriptionsResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteDashboardEmailSubscriptionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteDashboardEmailSubscriptionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteDashboardEmailSubscriptionsResponse:
        """Deserializes the DeleteDashboardEmailSubscriptionsResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteDefaultNamespaceSettingResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteDefaultNamespaceSettingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteDefaultNamespaceSettingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteDefaultNamespaceSettingResponse:
        """Deserializes the DeleteDefaultNamespaceSettingResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteDefaultWarehouseIdResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteDefaultWarehouseIdResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteDefaultWarehouseIdResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteDefaultWarehouseIdResponse:
        """Deserializes the DeleteDefaultWarehouseIdResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteDisableLegacyAccessResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteDisableLegacyAccessResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteDisableLegacyAccessResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteDisableLegacyAccessResponse:
        """Deserializes the DeleteDisableLegacyAccessResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteDisableLegacyDbfsResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteDisableLegacyDbfsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteDisableLegacyDbfsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteDisableLegacyDbfsResponse:
        """Deserializes the DeleteDisableLegacyDbfsResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteDisableLegacyFeaturesResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteDisableLegacyFeaturesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteDisableLegacyFeaturesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteDisableLegacyFeaturesResponse:
        """Deserializes the DeleteDisableLegacyFeaturesResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteLlmProxyPartnerPoweredWorkspaceResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteLlmProxyPartnerPoweredWorkspaceResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteLlmProxyPartnerPoweredWorkspaceResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteLlmProxyPartnerPoweredWorkspaceResponse:
        """Deserializes the DeleteLlmProxyPartnerPoweredWorkspaceResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeletePersonalComputeSettingResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeletePersonalComputeSettingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeletePersonalComputeSettingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeletePersonalComputeSettingResponse:
        """Deserializes the DeletePersonalComputeSettingResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteRestrictWorkspaceAdminsSettingResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteRestrictWorkspaceAdminsSettingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteRestrictWorkspaceAdminsSettingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteRestrictWorkspaceAdminsSettingResponse:
        """Deserializes the DeleteRestrictWorkspaceAdminsSettingResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


@dataclass
class DeleteSqlResultsDownloadResponse:
    """The etag is returned."""

    etag: str
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> delete pattern to perform setting deletions in order to avoid race conditions. That is, get
    an etag from a GET request, and pass it with the DELETE request to identify the rule set version
    you are deleting."""

    def as_dict(self) -> dict:
        """Serializes the DeleteSqlResultsDownloadResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteSqlResultsDownloadResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteSqlResultsDownloadResponse:
        """Deserializes the DeleteSqlResultsDownloadResponse from a dictionary."""
        return cls(etag=d.get("etag", None))


class DestinationType(Enum):

    EMAIL = "EMAIL"
    MICROSOFT_TEAMS = "MICROSOFT_TEAMS"
    PAGERDUTY = "PAGERDUTY"
    SLACK = "SLACK"
    WEBHOOK = "WEBHOOK"


@dataclass
class DisableLegacyAccess:
    disable_legacy_access: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the DisableLegacyAccess into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.disable_legacy_access:
            body["disable_legacy_access"] = self.disable_legacy_access.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DisableLegacyAccess into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.disable_legacy_access:
            body["disable_legacy_access"] = self.disable_legacy_access
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DisableLegacyAccess:
        """Deserializes the DisableLegacyAccess from a dictionary."""
        return cls(
            disable_legacy_access=_from_dict(d, "disable_legacy_access", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class DisableLegacyDbfs:
    disable_legacy_dbfs: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the DisableLegacyDbfs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.disable_legacy_dbfs:
            body["disable_legacy_dbfs"] = self.disable_legacy_dbfs.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DisableLegacyDbfs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.disable_legacy_dbfs:
            body["disable_legacy_dbfs"] = self.disable_legacy_dbfs
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DisableLegacyDbfs:
        """Deserializes the DisableLegacyDbfs from a dictionary."""
        return cls(
            disable_legacy_dbfs=_from_dict(d, "disable_legacy_dbfs", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class DisableLegacyFeatures:
    disable_legacy_features: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the DisableLegacyFeatures into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.disable_legacy_features:
            body["disable_legacy_features"] = self.disable_legacy_features.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DisableLegacyFeatures into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.disable_legacy_features:
            body["disable_legacy_features"] = self.disable_legacy_features
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DisableLegacyFeatures:
        """Deserializes the DisableLegacyFeatures from a dictionary."""
        return cls(
            disable_legacy_features=_from_dict(d, "disable_legacy_features", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class EgressNetworkPolicy:
    """The network policies applying for egress traffic. This message is used by the UI/REST API. We
    translate this message to the format expected by the dataplane in Lakehouse Network Manager (for
    the format expected by the dataplane, see networkconfig.textproto)."""

    internet_access: Optional[EgressNetworkPolicyInternetAccessPolicy] = None
    """The access policy enforced for egress traffic to the internet."""

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.internet_access:
            body["internet_access"] = self.internet_access.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.internet_access:
            body["internet_access"] = self.internet_access
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicy:
        """Deserializes the EgressNetworkPolicy from a dictionary."""
        return cls(internet_access=_from_dict(d, "internet_access", EgressNetworkPolicyInternetAccessPolicy))


@dataclass
class EgressNetworkPolicyInternetAccessPolicy:
    allowed_internet_destinations: Optional[List[EgressNetworkPolicyInternetAccessPolicyInternetDestination]] = None

    allowed_storage_destinations: Optional[List[EgressNetworkPolicyInternetAccessPolicyStorageDestination]] = None

    log_only_mode: Optional[EgressNetworkPolicyInternetAccessPolicyLogOnlyMode] = None
    """Optional. If not specified, assume the policy is enforced for all workloads."""

    restriction_mode: Optional[EgressNetworkPolicyInternetAccessPolicyRestrictionMode] = None

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.allowed_internet_destinations:
            body["allowed_internet_destinations"] = [v.as_dict() for v in self.allowed_internet_destinations]
        if self.allowed_storage_destinations:
            body["allowed_storage_destinations"] = [v.as_dict() for v in self.allowed_storage_destinations]
        if self.log_only_mode:
            body["log_only_mode"] = self.log_only_mode.as_dict()
        if self.restriction_mode is not None:
            body["restriction_mode"] = self.restriction_mode.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.allowed_internet_destinations:
            body["allowed_internet_destinations"] = self.allowed_internet_destinations
        if self.allowed_storage_destinations:
            body["allowed_storage_destinations"] = self.allowed_storage_destinations
        if self.log_only_mode:
            body["log_only_mode"] = self.log_only_mode
        if self.restriction_mode is not None:
            body["restriction_mode"] = self.restriction_mode
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyInternetAccessPolicy:
        """Deserializes the EgressNetworkPolicyInternetAccessPolicy from a dictionary."""
        return cls(
            allowed_internet_destinations=_repeated_dict(
                d, "allowed_internet_destinations", EgressNetworkPolicyInternetAccessPolicyInternetDestination
            ),
            allowed_storage_destinations=_repeated_dict(
                d, "allowed_storage_destinations", EgressNetworkPolicyInternetAccessPolicyStorageDestination
            ),
            log_only_mode=_from_dict(d, "log_only_mode", EgressNetworkPolicyInternetAccessPolicyLogOnlyMode),
            restriction_mode=_enum(d, "restriction_mode", EgressNetworkPolicyInternetAccessPolicyRestrictionMode),
        )


@dataclass
class EgressNetworkPolicyInternetAccessPolicyInternetDestination:
    """Users can specify accessible internet destinations when outbound access is restricted. We only
    support domain name (FQDN) destinations for the time being, though going forwards we want to
    support host names and IP addresses."""

    destination: Optional[str] = None

    protocol: Optional[
        EgressNetworkPolicyInternetAccessPolicyInternetDestinationInternetDestinationFilteringProtocol
    ] = None

    type: Optional[EgressNetworkPolicyInternetAccessPolicyInternetDestinationInternetDestinationType] = None

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicyInternetDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination is not None:
            body["destination"] = self.destination
        if self.protocol is not None:
            body["protocol"] = self.protocol.value
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicyInternetDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination is not None:
            body["destination"] = self.destination
        if self.protocol is not None:
            body["protocol"] = self.protocol
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyInternetAccessPolicyInternetDestination:
        """Deserializes the EgressNetworkPolicyInternetAccessPolicyInternetDestination from a dictionary."""
        return cls(
            destination=d.get("destination", None),
            protocol=_enum(
                d,
                "protocol",
                EgressNetworkPolicyInternetAccessPolicyInternetDestinationInternetDestinationFilteringProtocol,
            ),
            type=_enum(d, "type", EgressNetworkPolicyInternetAccessPolicyInternetDestinationInternetDestinationType),
        )


class EgressNetworkPolicyInternetAccessPolicyInternetDestinationInternetDestinationFilteringProtocol(Enum):
    """The filtering protocol used by the DP. For private and public preview, SEG will only support TCP
    filtering (i.e. DNS based filtering, filtering by destination IP address), so protocol will be
    set to TCP by default and hidden from the user. In the future, users may be able to select HTTP
    filtering (i.e. SNI based filtering, filtering by FQDN)."""

    TCP = "TCP"


class EgressNetworkPolicyInternetAccessPolicyInternetDestinationInternetDestinationType(Enum):

    FQDN = "FQDN"


@dataclass
class EgressNetworkPolicyInternetAccessPolicyLogOnlyMode:
    log_only_mode_type: Optional[EgressNetworkPolicyInternetAccessPolicyLogOnlyModeLogOnlyModeType] = None

    workloads: Optional[List[EgressNetworkPolicyInternetAccessPolicyLogOnlyModeWorkloadType]] = None

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicyLogOnlyMode into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.log_only_mode_type is not None:
            body["log_only_mode_type"] = self.log_only_mode_type.value
        if self.workloads:
            body["workloads"] = [v.value for v in self.workloads]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicyLogOnlyMode into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.log_only_mode_type is not None:
            body["log_only_mode_type"] = self.log_only_mode_type
        if self.workloads:
            body["workloads"] = self.workloads
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyInternetAccessPolicyLogOnlyMode:
        """Deserializes the EgressNetworkPolicyInternetAccessPolicyLogOnlyMode from a dictionary."""
        return cls(
            log_only_mode_type=_enum(
                d, "log_only_mode_type", EgressNetworkPolicyInternetAccessPolicyLogOnlyModeLogOnlyModeType
            ),
            workloads=_repeated_enum(d, "workloads", EgressNetworkPolicyInternetAccessPolicyLogOnlyModeWorkloadType),
        )


class EgressNetworkPolicyInternetAccessPolicyLogOnlyModeLogOnlyModeType(Enum):

    ALL_SERVICES = "ALL_SERVICES"
    SELECTED_SERVICES = "SELECTED_SERVICES"


class EgressNetworkPolicyInternetAccessPolicyLogOnlyModeWorkloadType(Enum):
    """The values should match the list of workloads used in networkconfig.proto"""

    DBSQL = "DBSQL"
    ML_SERVING = "ML_SERVING"


class EgressNetworkPolicyInternetAccessPolicyRestrictionMode(Enum):
    """At which level can Databricks and Databricks managed compute access Internet. FULL_ACCESS:
    Databricks can access Internet. No blocking rules will apply. RESTRICTED_ACCESS: Databricks can
    only access explicitly allowed internet and storage destinations, as well as UC connections and
    external locations. PRIVATE_ACCESS_ONLY (not used): Databricks can only access destinations via
    private link."""

    FULL_ACCESS = "FULL_ACCESS"
    PRIVATE_ACCESS_ONLY = "PRIVATE_ACCESS_ONLY"
    RESTRICTED_ACCESS = "RESTRICTED_ACCESS"


@dataclass
class EgressNetworkPolicyInternetAccessPolicyStorageDestination:
    """Users can specify accessible storage destinations."""

    allowed_paths: Optional[List[str]] = None

    azure_container: Optional[str] = None

    azure_dns_zone: Optional[str] = None

    azure_storage_account: Optional[str] = None

    azure_storage_service: Optional[str] = None

    bucket_name: Optional[str] = None

    region: Optional[str] = None

    type: Optional[EgressNetworkPolicyInternetAccessPolicyStorageDestinationStorageDestinationType] = None

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicyStorageDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.allowed_paths:
            body["allowed_paths"] = [v for v in self.allowed_paths]
        if self.azure_container is not None:
            body["azure_container"] = self.azure_container
        if self.azure_dns_zone is not None:
            body["azure_dns_zone"] = self.azure_dns_zone
        if self.azure_storage_account is not None:
            body["azure_storage_account"] = self.azure_storage_account
        if self.azure_storage_service is not None:
            body["azure_storage_service"] = self.azure_storage_service
        if self.bucket_name is not None:
            body["bucket_name"] = self.bucket_name
        if self.region is not None:
            body["region"] = self.region
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyInternetAccessPolicyStorageDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.allowed_paths:
            body["allowed_paths"] = self.allowed_paths
        if self.azure_container is not None:
            body["azure_container"] = self.azure_container
        if self.azure_dns_zone is not None:
            body["azure_dns_zone"] = self.azure_dns_zone
        if self.azure_storage_account is not None:
            body["azure_storage_account"] = self.azure_storage_account
        if self.azure_storage_service is not None:
            body["azure_storage_service"] = self.azure_storage_service
        if self.bucket_name is not None:
            body["bucket_name"] = self.bucket_name
        if self.region is not None:
            body["region"] = self.region
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyInternetAccessPolicyStorageDestination:
        """Deserializes the EgressNetworkPolicyInternetAccessPolicyStorageDestination from a dictionary."""
        return cls(
            allowed_paths=d.get("allowed_paths", None),
            azure_container=d.get("azure_container", None),
            azure_dns_zone=d.get("azure_dns_zone", None),
            azure_storage_account=d.get("azure_storage_account", None),
            azure_storage_service=d.get("azure_storage_service", None),
            bucket_name=d.get("bucket_name", None),
            region=d.get("region", None),
            type=_enum(d, "type", EgressNetworkPolicyInternetAccessPolicyStorageDestinationStorageDestinationType),
        )


class EgressNetworkPolicyInternetAccessPolicyStorageDestinationStorageDestinationType(Enum):

    AWS_S3 = "AWS_S3"
    AZURE_STORAGE = "AZURE_STORAGE"
    CLOUDFLARE_R2 = "CLOUDFLARE_R2"
    GOOGLE_CLOUD_STORAGE = "GOOGLE_CLOUD_STORAGE"


@dataclass
class EgressNetworkPolicyNetworkAccessPolicy:
    restriction_mode: EgressNetworkPolicyNetworkAccessPolicyRestrictionMode
    """The restriction mode that controls how serverless workloads can access the internet."""

    allowed_internet_destinations: Optional[List[EgressNetworkPolicyNetworkAccessPolicyInternetDestination]] = None
    """List of internet destinations that serverless workloads are allowed to access when in
    RESTRICTED_ACCESS mode."""

    allowed_storage_destinations: Optional[List[EgressNetworkPolicyNetworkAccessPolicyStorageDestination]] = None
    """List of storage destinations that serverless workloads are allowed to access when in
    RESTRICTED_ACCESS mode."""

    policy_enforcement: Optional[EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcement] = None
    """Optional. When policy_enforcement is not provided, we default to ENFORCE_MODE_ALL_SERVICES"""

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.allowed_internet_destinations:
            body["allowed_internet_destinations"] = [v.as_dict() for v in self.allowed_internet_destinations]
        if self.allowed_storage_destinations:
            body["allowed_storage_destinations"] = [v.as_dict() for v in self.allowed_storage_destinations]
        if self.policy_enforcement:
            body["policy_enforcement"] = self.policy_enforcement.as_dict()
        if self.restriction_mode is not None:
            body["restriction_mode"] = self.restriction_mode.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.allowed_internet_destinations:
            body["allowed_internet_destinations"] = self.allowed_internet_destinations
        if self.allowed_storage_destinations:
            body["allowed_storage_destinations"] = self.allowed_storage_destinations
        if self.policy_enforcement:
            body["policy_enforcement"] = self.policy_enforcement
        if self.restriction_mode is not None:
            body["restriction_mode"] = self.restriction_mode
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyNetworkAccessPolicy:
        """Deserializes the EgressNetworkPolicyNetworkAccessPolicy from a dictionary."""
        return cls(
            allowed_internet_destinations=_repeated_dict(
                d, "allowed_internet_destinations", EgressNetworkPolicyNetworkAccessPolicyInternetDestination
            ),
            allowed_storage_destinations=_repeated_dict(
                d, "allowed_storage_destinations", EgressNetworkPolicyNetworkAccessPolicyStorageDestination
            ),
            policy_enforcement=_from_dict(
                d, "policy_enforcement", EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcement
            ),
            restriction_mode=_enum(d, "restriction_mode", EgressNetworkPolicyNetworkAccessPolicyRestrictionMode),
        )


@dataclass
class EgressNetworkPolicyNetworkAccessPolicyInternetDestination:
    """Users can specify accessible internet destinations when outbound access is restricted. We only
    support DNS_NAME (FQDN format) destinations for the time being. Going forward we may extend
    support to host names and IP addresses."""

    destination: Optional[str] = None
    """The internet destination to which access will be allowed. Format dependent on the destination
    type."""

    internet_destination_type: Optional[
        EgressNetworkPolicyNetworkAccessPolicyInternetDestinationInternetDestinationType
    ] = None
    """The type of internet destination. Currently only DNS_NAME is supported."""

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicyInternetDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination is not None:
            body["destination"] = self.destination
        if self.internet_destination_type is not None:
            body["internet_destination_type"] = self.internet_destination_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicyInternetDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination is not None:
            body["destination"] = self.destination
        if self.internet_destination_type is not None:
            body["internet_destination_type"] = self.internet_destination_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyNetworkAccessPolicyInternetDestination:
        """Deserializes the EgressNetworkPolicyNetworkAccessPolicyInternetDestination from a dictionary."""
        return cls(
            destination=d.get("destination", None),
            internet_destination_type=_enum(
                d,
                "internet_destination_type",
                EgressNetworkPolicyNetworkAccessPolicyInternetDestinationInternetDestinationType,
            ),
        )


class EgressNetworkPolicyNetworkAccessPolicyInternetDestinationInternetDestinationType(Enum):

    DNS_NAME = "DNS_NAME"


@dataclass
class EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcement:
    dry_run_mode_product_filter: Optional[
        List[EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcementDryRunModeProductFilter]
    ] = None
    """When empty, it means dry run for all products. When non-empty, it means dry run for specific
    products and for the other products, they will run in enforced mode."""

    enforcement_mode: Optional[EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcementEnforcementMode] = None
    """The mode of policy enforcement. ENFORCED blocks traffic that violates policy, while DRY_RUN only
    logs violations without blocking. When not specified, defaults to ENFORCED."""

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcement into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dry_run_mode_product_filter:
            body["dry_run_mode_product_filter"] = [v.value for v in self.dry_run_mode_product_filter]
        if self.enforcement_mode is not None:
            body["enforcement_mode"] = self.enforcement_mode.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcement into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dry_run_mode_product_filter:
            body["dry_run_mode_product_filter"] = self.dry_run_mode_product_filter
        if self.enforcement_mode is not None:
            body["enforcement_mode"] = self.enforcement_mode
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcement:
        """Deserializes the EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcement from a dictionary."""
        return cls(
            dry_run_mode_product_filter=_repeated_enum(
                d,
                "dry_run_mode_product_filter",
                EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcementDryRunModeProductFilter,
            ),
            enforcement_mode=_enum(
                d, "enforcement_mode", EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcementEnforcementMode
            ),
        )


class EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcementDryRunModeProductFilter(Enum):
    """The values should match the list of workloads used in networkconfig.proto"""

    DBSQL = "DBSQL"
    ML_SERVING = "ML_SERVING"


class EgressNetworkPolicyNetworkAccessPolicyPolicyEnforcementEnforcementMode(Enum):

    DRY_RUN = "DRY_RUN"
    ENFORCED = "ENFORCED"


class EgressNetworkPolicyNetworkAccessPolicyRestrictionMode(Enum):
    """At which level can Databricks and Databricks managed compute access Internet. FULL_ACCESS:
    Databricks can access Internet. No blocking rules will apply. RESTRICTED_ACCESS: Databricks can
    only access explicitly allowed internet and storage destinations, as well as UC connections and
    external locations."""

    FULL_ACCESS = "FULL_ACCESS"
    RESTRICTED_ACCESS = "RESTRICTED_ACCESS"


@dataclass
class EgressNetworkPolicyNetworkAccessPolicyStorageDestination:
    """Users can specify accessible storage destinations."""

    azure_storage_account: Optional[str] = None
    """The Azure storage account name."""

    azure_storage_service: Optional[str] = None
    """The Azure storage service type (blob, dfs, etc.)."""

    bucket_name: Optional[str] = None

    region: Optional[str] = None

    storage_destination_type: Optional[
        EgressNetworkPolicyNetworkAccessPolicyStorageDestinationStorageDestinationType
    ] = None
    """The type of storage destination."""

    def as_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicyStorageDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.azure_storage_account is not None:
            body["azure_storage_account"] = self.azure_storage_account
        if self.azure_storage_service is not None:
            body["azure_storage_service"] = self.azure_storage_service
        if self.bucket_name is not None:
            body["bucket_name"] = self.bucket_name
        if self.region is not None:
            body["region"] = self.region
        if self.storage_destination_type is not None:
            body["storage_destination_type"] = self.storage_destination_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EgressNetworkPolicyNetworkAccessPolicyStorageDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.azure_storage_account is not None:
            body["azure_storage_account"] = self.azure_storage_account
        if self.azure_storage_service is not None:
            body["azure_storage_service"] = self.azure_storage_service
        if self.bucket_name is not None:
            body["bucket_name"] = self.bucket_name
        if self.region is not None:
            body["region"] = self.region
        if self.storage_destination_type is not None:
            body["storage_destination_type"] = self.storage_destination_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EgressNetworkPolicyNetworkAccessPolicyStorageDestination:
        """Deserializes the EgressNetworkPolicyNetworkAccessPolicyStorageDestination from a dictionary."""
        return cls(
            azure_storage_account=d.get("azure_storage_account", None),
            azure_storage_service=d.get("azure_storage_service", None),
            bucket_name=d.get("bucket_name", None),
            region=d.get("region", None),
            storage_destination_type=_enum(
                d,
                "storage_destination_type",
                EgressNetworkPolicyNetworkAccessPolicyStorageDestinationStorageDestinationType,
            ),
        )


class EgressNetworkPolicyNetworkAccessPolicyStorageDestinationStorageDestinationType(Enum):

    AWS_S3 = "AWS_S3"
    AZURE_STORAGE = "AZURE_STORAGE"
    GOOGLE_CLOUD_STORAGE = "GOOGLE_CLOUD_STORAGE"


class EgressResourceType(Enum):
    """The target resources that are supported by Network Connectivity Config. Note: some egress types
    can support general types that are not defined in EgressResourceType. E.g.: Azure private
    endpoint supports private link enabled Azure services."""

    AZURE_BLOB_STORAGE = "AZURE_BLOB_STORAGE"


@dataclass
class EmailConfig:
    addresses: Optional[List[str]] = None
    """Email addresses to notify."""

    def as_dict(self) -> dict:
        """Serializes the EmailConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.addresses:
            body["addresses"] = [v for v in self.addresses]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EmailConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.addresses:
            body["addresses"] = self.addresses
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EmailConfig:
        """Deserializes the EmailConfig from a dictionary."""
        return cls(addresses=d.get("addresses", None))


@dataclass
class Empty:
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
class EnableExportNotebook:
    boolean_val: Optional[BooleanMessage] = None

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the EnableExportNotebook into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnableExportNotebook into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnableExportNotebook:
        """Deserializes the EnableExportNotebook from a dictionary."""
        return cls(boolean_val=_from_dict(d, "boolean_val", BooleanMessage), setting_name=d.get("setting_name", None))


@dataclass
class EnableNotebookTableClipboard:
    boolean_val: Optional[BooleanMessage] = None

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the EnableNotebookTableClipboard into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnableNotebookTableClipboard into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnableNotebookTableClipboard:
        """Deserializes the EnableNotebookTableClipboard from a dictionary."""
        return cls(boolean_val=_from_dict(d, "boolean_val", BooleanMessage), setting_name=d.get("setting_name", None))


@dataclass
class EnableResultsDownloading:
    boolean_val: Optional[BooleanMessage] = None

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the EnableResultsDownloading into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnableResultsDownloading into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnableResultsDownloading:
        """Deserializes the EnableResultsDownloading from a dictionary."""
        return cls(boolean_val=_from_dict(d, "boolean_val", BooleanMessage), setting_name=d.get("setting_name", None))


@dataclass
class EnhancedSecurityMonitoring:
    """SHIELD feature: ESM"""

    is_enabled: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the EnhancedSecurityMonitoring into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_enabled is not None:
            body["is_enabled"] = self.is_enabled
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnhancedSecurityMonitoring into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_enabled is not None:
            body["is_enabled"] = self.is_enabled
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnhancedSecurityMonitoring:
        """Deserializes the EnhancedSecurityMonitoring from a dictionary."""
        return cls(is_enabled=d.get("is_enabled", None))


@dataclass
class EnhancedSecurityMonitoringSetting:
    enhanced_security_monitoring_workspace: EnhancedSecurityMonitoring

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the EnhancedSecurityMonitoringSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enhanced_security_monitoring_workspace:
            body["enhanced_security_monitoring_workspace"] = self.enhanced_security_monitoring_workspace.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnhancedSecurityMonitoringSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enhanced_security_monitoring_workspace:
            body["enhanced_security_monitoring_workspace"] = self.enhanced_security_monitoring_workspace
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnhancedSecurityMonitoringSetting:
        """Deserializes the EnhancedSecurityMonitoringSetting from a dictionary."""
        return cls(
            enhanced_security_monitoring_workspace=_from_dict(
                d, "enhanced_security_monitoring_workspace", EnhancedSecurityMonitoring
            ),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class EsmEnablementAccount:
    """Account level policy for ESM"""

    is_enforced: Optional[bool] = None

    def as_dict(self) -> dict:
        """Serializes the EsmEnablementAccount into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_enforced is not None:
            body["is_enforced"] = self.is_enforced
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EsmEnablementAccount into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_enforced is not None:
            body["is_enforced"] = self.is_enforced
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EsmEnablementAccount:
        """Deserializes the EsmEnablementAccount from a dictionary."""
        return cls(is_enforced=d.get("is_enforced", None))


@dataclass
class EsmEnablementAccountSetting:
    esm_enablement_account: EsmEnablementAccount

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the EsmEnablementAccountSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.esm_enablement_account:
            body["esm_enablement_account"] = self.esm_enablement_account.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EsmEnablementAccountSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.esm_enablement_account:
            body["esm_enablement_account"] = self.esm_enablement_account
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EsmEnablementAccountSetting:
        """Deserializes the EsmEnablementAccountSetting from a dictionary."""
        return cls(
            esm_enablement_account=_from_dict(d, "esm_enablement_account", EsmEnablementAccount),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class ExchangeToken:
    """The exchange token is the result of the token exchange with the IdP"""

    credential: Optional[str] = None
    """The requested token."""

    credential_eol_time: Optional[int] = None
    """The end-of-life timestamp of the token. The value is in milliseconds since the Unix epoch."""

    owner_id: Optional[int] = None
    """User ID of the user that owns this token."""

    scopes: Optional[List[str]] = None
    """The scopes of access granted in the token."""

    token_type: Optional[TokenType] = None
    """The type of this exchange token"""

    def as_dict(self) -> dict:
        """Serializes the ExchangeToken into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.credential is not None:
            body["credential"] = self.credential
        if self.credential_eol_time is not None:
            body["credentialEolTime"] = self.credential_eol_time
        if self.owner_id is not None:
            body["ownerId"] = self.owner_id
        if self.scopes:
            body["scopes"] = [v for v in self.scopes]
        if self.token_type is not None:
            body["tokenType"] = self.token_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExchangeToken into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.credential is not None:
            body["credential"] = self.credential
        if self.credential_eol_time is not None:
            body["credentialEolTime"] = self.credential_eol_time
        if self.owner_id is not None:
            body["ownerId"] = self.owner_id
        if self.scopes:
            body["scopes"] = self.scopes
        if self.token_type is not None:
            body["tokenType"] = self.token_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExchangeToken:
        """Deserializes the ExchangeToken from a dictionary."""
        return cls(
            credential=d.get("credential", None),
            credential_eol_time=d.get("credentialEolTime", None),
            owner_id=d.get("ownerId", None),
            scopes=d.get("scopes", None),
            token_type=_enum(d, "tokenType", TokenType),
        )


@dataclass
class ExchangeTokenResponse:
    """Exhanged tokens were successfully returned."""

    values: Optional[List[ExchangeToken]] = None

    def as_dict(self) -> dict:
        """Serializes the ExchangeTokenResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.values:
            body["values"] = [v.as_dict() for v in self.values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExchangeTokenResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.values:
            body["values"] = self.values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExchangeTokenResponse:
        """Deserializes the ExchangeTokenResponse from a dictionary."""
        return cls(values=_repeated_dict(d, "values", ExchangeToken))


@dataclass
class FetchIpAccessListResponse:
    """An IP access list was successfully returned."""

    ip_access_list: Optional[IpAccessListInfo] = None

    def as_dict(self) -> dict:
        """Serializes the FetchIpAccessListResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FetchIpAccessListResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FetchIpAccessListResponse:
        """Deserializes the FetchIpAccessListResponse from a dictionary."""
        return cls(ip_access_list=_from_dict(d, "ip_access_list", IpAccessListInfo))


@dataclass
class GenericWebhookConfig:
    password: Optional[str] = None
    """[Input-Only][Optional] Password for webhook."""

    password_set: Optional[bool] = None
    """[Output-Only] Whether password is set."""

    url: Optional[str] = None
    """[Input-Only] URL for webhook."""

    url_set: Optional[bool] = None
    """[Output-Only] Whether URL is set."""

    username: Optional[str] = None
    """[Input-Only][Optional] Username for webhook."""

    username_set: Optional[bool] = None
    """[Output-Only] Whether username is set."""

    def as_dict(self) -> dict:
        """Serializes the GenericWebhookConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.password is not None:
            body["password"] = self.password
        if self.password_set is not None:
            body["password_set"] = self.password_set
        if self.url is not None:
            body["url"] = self.url
        if self.url_set is not None:
            body["url_set"] = self.url_set
        if self.username is not None:
            body["username"] = self.username
        if self.username_set is not None:
            body["username_set"] = self.username_set
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenericWebhookConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.password is not None:
            body["password"] = self.password
        if self.password_set is not None:
            body["password_set"] = self.password_set
        if self.url is not None:
            body["url"] = self.url
        if self.url_set is not None:
            body["url_set"] = self.url_set
        if self.username is not None:
            body["username"] = self.username
        if self.username_set is not None:
            body["username_set"] = self.username_set
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenericWebhookConfig:
        """Deserializes the GenericWebhookConfig from a dictionary."""
        return cls(
            password=d.get("password", None),
            password_set=d.get("password_set", None),
            url=d.get("url", None),
            url_set=d.get("url_set", None),
            username=d.get("username", None),
            username_set=d.get("username_set", None),
        )


@dataclass
class GetIpAccessListResponse:
    ip_access_list: Optional[IpAccessListInfo] = None

    def as_dict(self) -> dict:
        """Serializes the GetIpAccessListResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetIpAccessListResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.ip_access_list:
            body["ip_access_list"] = self.ip_access_list
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetIpAccessListResponse:
        """Deserializes the GetIpAccessListResponse from a dictionary."""
        return cls(ip_access_list=_from_dict(d, "ip_access_list", IpAccessListInfo))


@dataclass
class GetIpAccessListsResponse:
    """IP access lists were successfully returned."""

    ip_access_lists: Optional[List[IpAccessListInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the GetIpAccessListsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.ip_access_lists:
            body["ip_access_lists"] = [v.as_dict() for v in self.ip_access_lists]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetIpAccessListsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.ip_access_lists:
            body["ip_access_lists"] = self.ip_access_lists
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetIpAccessListsResponse:
        """Deserializes the GetIpAccessListsResponse from a dictionary."""
        return cls(ip_access_lists=_repeated_dict(d, "ip_access_lists", IpAccessListInfo))


@dataclass
class GetTokenPermissionLevelsResponse:
    permission_levels: Optional[List[TokenPermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetTokenPermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetTokenPermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetTokenPermissionLevelsResponse:
        """Deserializes the GetTokenPermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", TokenPermissionsDescription))


@dataclass
class GetTokenResponse:
    """Token with specified Token ID was successfully returned."""

    token_info: Optional[TokenInfo] = None

    def as_dict(self) -> dict:
        """Serializes the GetTokenResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.token_info:
            body["token_info"] = self.token_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetTokenResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.token_info:
            body["token_info"] = self.token_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetTokenResponse:
        """Deserializes the GetTokenResponse from a dictionary."""
        return cls(token_info=_from_dict(d, "token_info", TokenInfo))


@dataclass
class IpAccessListInfo:
    """Definition of an IP Access list"""

    address_count: Optional[int] = None
    """Total number of IP or CIDR values."""

    created_at: Optional[int] = None
    """Creation timestamp in milliseconds."""

    created_by: Optional[int] = None
    """User ID of the user who created this list."""

    enabled: Optional[bool] = None
    """Specifies whether this IP access list is enabled."""

    ip_addresses: Optional[List[str]] = None

    label: Optional[str] = None
    """Label for the IP access list. This **cannot** be empty."""

    list_id: Optional[str] = None
    """Universally unique identifier (UUID) of the IP access list."""

    list_type: Optional[ListType] = None

    updated_at: Optional[int] = None
    """Update timestamp in milliseconds."""

    updated_by: Optional[int] = None
    """User ID of the user who updated this list."""

    def as_dict(self) -> dict:
        """Serializes the IpAccessListInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.address_count is not None:
            body["address_count"] = self.address_count
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.ip_addresses:
            body["ip_addresses"] = [v for v in self.ip_addresses]
        if self.label is not None:
            body["label"] = self.label
        if self.list_id is not None:
            body["list_id"] = self.list_id
        if self.list_type is not None:
            body["list_type"] = self.list_type.value
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IpAccessListInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.address_count is not None:
            body["address_count"] = self.address_count
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.ip_addresses:
            body["ip_addresses"] = self.ip_addresses
        if self.label is not None:
            body["label"] = self.label
        if self.list_id is not None:
            body["list_id"] = self.list_id
        if self.list_type is not None:
            body["list_type"] = self.list_type
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IpAccessListInfo:
        """Deserializes the IpAccessListInfo from a dictionary."""
        return cls(
            address_count=d.get("address_count", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            enabled=d.get("enabled", None),
            ip_addresses=d.get("ip_addresses", None),
            label=d.get("label", None),
            list_id=d.get("list_id", None),
            list_type=_enum(d, "list_type", ListType),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class ListIpAccessListResponse:
    """IP access lists were successfully returned."""

    ip_access_lists: Optional[List[IpAccessListInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ListIpAccessListResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.ip_access_lists:
            body["ip_access_lists"] = [v.as_dict() for v in self.ip_access_lists]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListIpAccessListResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.ip_access_lists:
            body["ip_access_lists"] = self.ip_access_lists
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListIpAccessListResponse:
        """Deserializes the ListIpAccessListResponse from a dictionary."""
        return cls(ip_access_lists=_repeated_dict(d, "ip_access_lists", IpAccessListInfo))


@dataclass
class ListNetworkConnectivityConfigurationsResponse:
    """The network connectivity configuration list was successfully retrieved."""

    items: Optional[List[NetworkConnectivityConfiguration]] = None

    next_page_token: Optional[str] = None
    """A token that can be used to get the next page of results. If null, there are no more results to
    show."""

    def as_dict(self) -> dict:
        """Serializes the ListNetworkConnectivityConfigurationsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.items:
            body["items"] = [v.as_dict() for v in self.items]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListNetworkConnectivityConfigurationsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.items:
            body["items"] = self.items
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListNetworkConnectivityConfigurationsResponse:
        """Deserializes the ListNetworkConnectivityConfigurationsResponse from a dictionary."""
        return cls(
            items=_repeated_dict(d, "items", NetworkConnectivityConfiguration),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListNetworkPoliciesResponse:
    items: Optional[List[AccountNetworkPolicy]] = None
    """List of network policies."""

    next_page_token: Optional[str] = None
    """A token that can be used to get the next page of results. If null, there are no more results to
    show."""

    def as_dict(self) -> dict:
        """Serializes the ListNetworkPoliciesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.items:
            body["items"] = [v.as_dict() for v in self.items]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListNetworkPoliciesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.items:
            body["items"] = self.items
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListNetworkPoliciesResponse:
        """Deserializes the ListNetworkPoliciesResponse from a dictionary."""
        return cls(
            items=_repeated_dict(d, "items", AccountNetworkPolicy), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListNotificationDestinationsResponse:
    next_page_token: Optional[str] = None
    """Page token for next of results."""

    results: Optional[List[ListNotificationDestinationsResult]] = None

    def as_dict(self) -> dict:
        """Serializes the ListNotificationDestinationsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = [v.as_dict() for v in self.results]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListNotificationDestinationsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.results:
            body["results"] = self.results
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListNotificationDestinationsResponse:
        """Deserializes the ListNotificationDestinationsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            results=_repeated_dict(d, "results", ListNotificationDestinationsResult),
        )


@dataclass
class ListNotificationDestinationsResult:
    destination_type: Optional[DestinationType] = None
    """[Output-only] The type of the notification destination. The type can not be changed once set."""

    display_name: Optional[str] = None
    """The display name for the notification destination."""

    id: Optional[str] = None
    """UUID identifying notification destination."""

    def as_dict(self) -> dict:
        """Serializes the ListNotificationDestinationsResult into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_type is not None:
            body["destination_type"] = self.destination_type.value
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListNotificationDestinationsResult into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_type is not None:
            body["destination_type"] = self.destination_type
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListNotificationDestinationsResult:
        """Deserializes the ListNotificationDestinationsResult from a dictionary."""
        return cls(
            destination_type=_enum(d, "destination_type", DestinationType),
            display_name=d.get("display_name", None),
            id=d.get("id", None),
        )


@dataclass
class ListPrivateEndpointRulesResponse:
    """The private endpoint rule list was successfully retrieved."""

    items: Optional[List[NccPrivateEndpointRule]] = None

    next_page_token: Optional[str] = None
    """A token that can be used to get the next page of results. If null, there are no more results to
    show."""

    def as_dict(self) -> dict:
        """Serializes the ListPrivateEndpointRulesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.items:
            body["items"] = [v.as_dict() for v in self.items]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListPrivateEndpointRulesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.items:
            body["items"] = self.items
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListPrivateEndpointRulesResponse:
        """Deserializes the ListPrivateEndpointRulesResponse from a dictionary."""
        return cls(
            items=_repeated_dict(d, "items", NccPrivateEndpointRule), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListPublicTokensResponse:
    token_infos: Optional[List[PublicTokenInfo]] = None
    """The information for each token."""

    def as_dict(self) -> dict:
        """Serializes the ListPublicTokensResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.token_infos:
            body["token_infos"] = [v.as_dict() for v in self.token_infos]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListPublicTokensResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.token_infos:
            body["token_infos"] = self.token_infos
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListPublicTokensResponse:
        """Deserializes the ListPublicTokensResponse from a dictionary."""
        return cls(token_infos=_repeated_dict(d, "token_infos", PublicTokenInfo))


@dataclass
class ListTokensResponse:
    """Tokens were successfully returned."""

    token_infos: Optional[List[TokenInfo]] = None
    """Token metadata of each user-created token in the workspace"""

    def as_dict(self) -> dict:
        """Serializes the ListTokensResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.token_infos:
            body["token_infos"] = [v.as_dict() for v in self.token_infos]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListTokensResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.token_infos:
            body["token_infos"] = self.token_infos
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListTokensResponse:
        """Deserializes the ListTokensResponse from a dictionary."""
        return cls(token_infos=_repeated_dict(d, "token_infos", TokenInfo))


class ListType(Enum):
    """Type of IP access list. Valid values are as follows and are case-sensitive:

    * `ALLOW`: An allow list. Include this IP or range. * `BLOCK`: A block list. Exclude this IP or
    range. IP addresses in the block list are excluded even if they are included in an allow list."""

    ALLOW = "ALLOW"
    BLOCK = "BLOCK"


@dataclass
class LlmProxyPartnerPoweredAccount:
    boolean_val: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the LlmProxyPartnerPoweredAccount into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LlmProxyPartnerPoweredAccount into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LlmProxyPartnerPoweredAccount:
        """Deserializes the LlmProxyPartnerPoweredAccount from a dictionary."""
        return cls(
            boolean_val=_from_dict(d, "boolean_val", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class LlmProxyPartnerPoweredEnforce:
    boolean_val: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the LlmProxyPartnerPoweredEnforce into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LlmProxyPartnerPoweredEnforce into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LlmProxyPartnerPoweredEnforce:
        """Deserializes the LlmProxyPartnerPoweredEnforce from a dictionary."""
        return cls(
            boolean_val=_from_dict(d, "boolean_val", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class LlmProxyPartnerPoweredWorkspace:
    boolean_val: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the LlmProxyPartnerPoweredWorkspace into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LlmProxyPartnerPoweredWorkspace into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LlmProxyPartnerPoweredWorkspace:
        """Deserializes the LlmProxyPartnerPoweredWorkspace from a dictionary."""
        return cls(
            boolean_val=_from_dict(d, "boolean_val", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class MicrosoftTeamsConfig:
    app_id: Optional[str] = None
    """[Input-Only] App ID for Microsoft Teams App."""

    app_id_set: Optional[bool] = None
    """[Output-Only] Whether App ID is set."""

    auth_secret: Optional[str] = None
    """[Input-Only] Secret for Microsoft Teams App authentication."""

    auth_secret_set: Optional[bool] = None
    """[Output-Only] Whether secret is set."""

    channel_url: Optional[str] = None
    """[Input-Only] Channel URL for Microsoft Teams App."""

    channel_url_set: Optional[bool] = None
    """[Output-Only] Whether Channel URL is set."""

    tenant_id: Optional[str] = None
    """[Input-Only] Tenant ID for Microsoft Teams App."""

    tenant_id_set: Optional[bool] = None
    """[Output-Only] Whether Tenant ID is set."""

    url: Optional[str] = None
    """[Input-Only] URL for Microsoft Teams webhook."""

    url_set: Optional[bool] = None
    """[Output-Only] Whether URL is set."""

    def as_dict(self) -> dict:
        """Serializes the MicrosoftTeamsConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.app_id is not None:
            body["app_id"] = self.app_id
        if self.app_id_set is not None:
            body["app_id_set"] = self.app_id_set
        if self.auth_secret is not None:
            body["auth_secret"] = self.auth_secret
        if self.auth_secret_set is not None:
            body["auth_secret_set"] = self.auth_secret_set
        if self.channel_url is not None:
            body["channel_url"] = self.channel_url
        if self.channel_url_set is not None:
            body["channel_url_set"] = self.channel_url_set
        if self.tenant_id is not None:
            body["tenant_id"] = self.tenant_id
        if self.tenant_id_set is not None:
            body["tenant_id_set"] = self.tenant_id_set
        if self.url is not None:
            body["url"] = self.url
        if self.url_set is not None:
            body["url_set"] = self.url_set
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MicrosoftTeamsConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.app_id is not None:
            body["app_id"] = self.app_id
        if self.app_id_set is not None:
            body["app_id_set"] = self.app_id_set
        if self.auth_secret is not None:
            body["auth_secret"] = self.auth_secret
        if self.auth_secret_set is not None:
            body["auth_secret_set"] = self.auth_secret_set
        if self.channel_url is not None:
            body["channel_url"] = self.channel_url
        if self.channel_url_set is not None:
            body["channel_url_set"] = self.channel_url_set
        if self.tenant_id is not None:
            body["tenant_id"] = self.tenant_id
        if self.tenant_id_set is not None:
            body["tenant_id_set"] = self.tenant_id_set
        if self.url is not None:
            body["url"] = self.url
        if self.url_set is not None:
            body["url_set"] = self.url_set
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MicrosoftTeamsConfig:
        """Deserializes the MicrosoftTeamsConfig from a dictionary."""
        return cls(
            app_id=d.get("app_id", None),
            app_id_set=d.get("app_id_set", None),
            auth_secret=d.get("auth_secret", None),
            auth_secret_set=d.get("auth_secret_set", None),
            channel_url=d.get("channel_url", None),
            channel_url_set=d.get("channel_url_set", None),
            tenant_id=d.get("tenant_id", None),
            tenant_id_set=d.get("tenant_id_set", None),
            url=d.get("url", None),
            url_set=d.get("url_set", None),
        )


@dataclass
class NccAwsStableIpRule:
    """The stable AWS IP CIDR blocks. You can use these to configure the firewall of your resources to
    allow traffic from your Databricks workspace."""

    cidr_blocks: Optional[List[str]] = None
    """The list of stable IP CIDR blocks from which Databricks network traffic originates when
    accessing your resources."""

    def as_dict(self) -> dict:
        """Serializes the NccAwsStableIpRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cidr_blocks:
            body["cidr_blocks"] = [v for v in self.cidr_blocks]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NccAwsStableIpRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cidr_blocks:
            body["cidr_blocks"] = self.cidr_blocks
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NccAwsStableIpRule:
        """Deserializes the NccAwsStableIpRule from a dictionary."""
        return cls(cidr_blocks=d.get("cidr_blocks", None))


@dataclass
class NccAzurePrivateEndpointRule:
    """Properties of the new private endpoint rule. Note that you must approve the endpoint in Azure
    portal after initialization."""

    connection_state: Optional[NccAzurePrivateEndpointRuleConnectionState] = None
    """The current status of this private endpoint. The private endpoint rules are effective only if
    the connection state is ESTABLISHED. Remember that you must approve new endpoints on your
    resources in the Azure portal before they take effect. The possible values are: - INIT:
    (deprecated) The endpoint has been created and pending approval. - PENDING: The endpoint has
    been created and pending approval. - ESTABLISHED: The endpoint has been approved and is ready to
    use in your serverless compute resources. - REJECTED: Connection was rejected by the private
    link resource owner. - DISCONNECTED: Connection was removed by the private link resource owner,
    the private endpoint becomes informative and should be deleted for clean-up. - EXPIRED: If the
    endpoint was created but not approved in 14 days, it will be EXPIRED."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when this object was created."""

    deactivated: Optional[bool] = None
    """Whether this private endpoint is deactivated."""

    deactivated_at: Optional[int] = None
    """Time in epoch milliseconds when this object was deactivated."""

    domain_names: Optional[List[str]] = None
    """Not used by customer-managed private endpoint services.
    
    Domain names of target private link service. When updating this field, the full list of target
    domain_names must be specified."""

    endpoint_name: Optional[str] = None
    """The name of the Azure private endpoint resource."""

    error_message: Optional[str] = None

    group_id: Optional[str] = None
    """Only used by private endpoints to Azure first-party services.
    
    The sub-resource type (group ID) of the target resource. Note that to connect to workspace root
    storage (root DBFS), you need two endpoints, one for blob and one for dfs."""

    network_connectivity_config_id: Optional[str] = None
    """The ID of a network connectivity configuration, which is the parent resource of this private
    endpoint rule object."""

    resource_id: Optional[str] = None
    """The Azure resource ID of the target resource."""

    rule_id: Optional[str] = None
    """The ID of a private endpoint rule."""

    updated_time: Optional[int] = None
    """Time in epoch milliseconds when this object was updated."""

    def as_dict(self) -> dict:
        """Serializes the NccAzurePrivateEndpointRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection_state is not None:
            body["connection_state"] = self.connection_state.value
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.deactivated is not None:
            body["deactivated"] = self.deactivated
        if self.deactivated_at is not None:
            body["deactivated_at"] = self.deactivated_at
        if self.domain_names:
            body["domain_names"] = [v for v in self.domain_names]
        if self.endpoint_name is not None:
            body["endpoint_name"] = self.endpoint_name
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.group_id is not None:
            body["group_id"] = self.group_id
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.rule_id is not None:
            body["rule_id"] = self.rule_id
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NccAzurePrivateEndpointRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection_state is not None:
            body["connection_state"] = self.connection_state
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.deactivated is not None:
            body["deactivated"] = self.deactivated
        if self.deactivated_at is not None:
            body["deactivated_at"] = self.deactivated_at
        if self.domain_names:
            body["domain_names"] = self.domain_names
        if self.endpoint_name is not None:
            body["endpoint_name"] = self.endpoint_name
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.group_id is not None:
            body["group_id"] = self.group_id
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.rule_id is not None:
            body["rule_id"] = self.rule_id
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NccAzurePrivateEndpointRule:
        """Deserializes the NccAzurePrivateEndpointRule from a dictionary."""
        return cls(
            connection_state=_enum(d, "connection_state", NccAzurePrivateEndpointRuleConnectionState),
            creation_time=d.get("creation_time", None),
            deactivated=d.get("deactivated", None),
            deactivated_at=d.get("deactivated_at", None),
            domain_names=d.get("domain_names", None),
            endpoint_name=d.get("endpoint_name", None),
            error_message=d.get("error_message", None),
            group_id=d.get("group_id", None),
            network_connectivity_config_id=d.get("network_connectivity_config_id", None),
            resource_id=d.get("resource_id", None),
            rule_id=d.get("rule_id", None),
            updated_time=d.get("updated_time", None),
        )


class NccAzurePrivateEndpointRuleConnectionState(Enum):

    CREATE_FAILED = "CREATE_FAILED"
    CREATING = "CREATING"
    DISCONNECTED = "DISCONNECTED"
    ESTABLISHED = "ESTABLISHED"
    EXPIRED = "EXPIRED"
    INIT = "INIT"
    PENDING = "PENDING"
    REJECTED = "REJECTED"


@dataclass
class NccAzureServiceEndpointRule:
    """The stable Azure service endpoints. You can configure the firewall of your Azure resources to
    allow traffic from your Databricks serverless compute resources."""

    subnets: Optional[List[str]] = None
    """The list of subnets from which Databricks network traffic originates when accessing your Azure
    resources."""

    target_region: Optional[str] = None
    """The Azure region in which this service endpoint rule applies.."""

    target_services: Optional[List[EgressResourceType]] = None
    """The Azure services to which this service endpoint rule applies to."""

    def as_dict(self) -> dict:
        """Serializes the NccAzureServiceEndpointRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.subnets:
            body["subnets"] = [v for v in self.subnets]
        if self.target_region is not None:
            body["target_region"] = self.target_region
        if self.target_services:
            body["target_services"] = [v.value for v in self.target_services]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NccAzureServiceEndpointRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.subnets:
            body["subnets"] = self.subnets
        if self.target_region is not None:
            body["target_region"] = self.target_region
        if self.target_services:
            body["target_services"] = self.target_services
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NccAzureServiceEndpointRule:
        """Deserializes the NccAzureServiceEndpointRule from a dictionary."""
        return cls(
            subnets=d.get("subnets", None),
            target_region=d.get("target_region", None),
            target_services=_repeated_enum(d, "target_services", EgressResourceType),
        )


@dataclass
class NccEgressConfig:
    default_rules: Optional[NccEgressDefaultRules] = None
    """The network connectivity rules that are applied by default without resource specific
    configurations. You can find the stable network information of your serverless compute resources
    here."""

    target_rules: Optional[NccEgressTargetRules] = None
    """The network connectivity rules that configured for each destinations. These rules override
    default rules."""

    def as_dict(self) -> dict:
        """Serializes the NccEgressConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default_rules:
            body["default_rules"] = self.default_rules.as_dict()
        if self.target_rules:
            body["target_rules"] = self.target_rules.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NccEgressConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default_rules:
            body["default_rules"] = self.default_rules
        if self.target_rules:
            body["target_rules"] = self.target_rules
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NccEgressConfig:
        """Deserializes the NccEgressConfig from a dictionary."""
        return cls(
            default_rules=_from_dict(d, "default_rules", NccEgressDefaultRules),
            target_rules=_from_dict(d, "target_rules", NccEgressTargetRules),
        )


@dataclass
class NccEgressDefaultRules:
    """Default rules don't have specific targets."""

    aws_stable_ip_rule: Optional[NccAwsStableIpRule] = None

    azure_service_endpoint_rule: Optional[NccAzureServiceEndpointRule] = None

    def as_dict(self) -> dict:
        """Serializes the NccEgressDefaultRules into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_stable_ip_rule:
            body["aws_stable_ip_rule"] = self.aws_stable_ip_rule.as_dict()
        if self.azure_service_endpoint_rule:
            body["azure_service_endpoint_rule"] = self.azure_service_endpoint_rule.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NccEgressDefaultRules into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_stable_ip_rule:
            body["aws_stable_ip_rule"] = self.aws_stable_ip_rule
        if self.azure_service_endpoint_rule:
            body["azure_service_endpoint_rule"] = self.azure_service_endpoint_rule
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NccEgressDefaultRules:
        """Deserializes the NccEgressDefaultRules from a dictionary."""
        return cls(
            aws_stable_ip_rule=_from_dict(d, "aws_stable_ip_rule", NccAwsStableIpRule),
            azure_service_endpoint_rule=_from_dict(d, "azure_service_endpoint_rule", NccAzureServiceEndpointRule),
        )


@dataclass
class NccEgressTargetRules:
    """Target rule controls the egress rules that are dedicated to specific resources."""

    aws_private_endpoint_rules: Optional[List[CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRule]] = None
    """AWS private endpoint rule controls the AWS private endpoint based egress rules."""

    azure_private_endpoint_rules: Optional[List[NccAzurePrivateEndpointRule]] = None

    def as_dict(self) -> dict:
        """Serializes the NccEgressTargetRules into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aws_private_endpoint_rules:
            body["aws_private_endpoint_rules"] = [v.as_dict() for v in self.aws_private_endpoint_rules]
        if self.azure_private_endpoint_rules:
            body["azure_private_endpoint_rules"] = [v.as_dict() for v in self.azure_private_endpoint_rules]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NccEgressTargetRules into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aws_private_endpoint_rules:
            body["aws_private_endpoint_rules"] = self.aws_private_endpoint_rules
        if self.azure_private_endpoint_rules:
            body["azure_private_endpoint_rules"] = self.azure_private_endpoint_rules
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NccEgressTargetRules:
        """Deserializes the NccEgressTargetRules from a dictionary."""
        return cls(
            aws_private_endpoint_rules=_repeated_dict(
                d, "aws_private_endpoint_rules", CustomerFacingNetworkConnectivityConfigAwsPrivateEndpointRule
            ),
            azure_private_endpoint_rules=_repeated_dict(d, "azure_private_endpoint_rules", NccAzurePrivateEndpointRule),
        )


@dataclass
class NccPrivateEndpointRule:
    """Properties of the new private endpoint rule. Note that you must approve the endpoint in Azure
    portal after initialization."""

    account_id: Optional[str] = None
    """Databricks account ID. You can find your account ID from the Accounts Console."""

    connection_state: Optional[NccPrivateEndpointRulePrivateLinkConnectionState] = None
    """The current status of this private endpoint. The private endpoint rules are effective only if
    the connection state is ESTABLISHED. Remember that you must approve new endpoints on your
    resources in the Cloud console before they take effect. The possible values are: - PENDING: The
    endpoint has been created and pending approval. - ESTABLISHED: The endpoint has been approved
    and is ready to use in your serverless compute resources. - REJECTED: Connection was rejected by
    the private link resource owner. - DISCONNECTED: Connection was removed by the private link
    resource owner, the private endpoint becomes informative and should be deleted for clean-up. -
    EXPIRED: If the endpoint was created but not approved in 14 days, it will be EXPIRED. -
    CREATING: The endpoint creation is in progress. Once successfully created, the state will
    transition to PENDING. - CREATE_FAILED: The endpoint creation failed. You can check the
    error_message field for more details."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when this object was created."""

    deactivated: Optional[bool] = None
    """Whether this private endpoint is deactivated."""

    deactivated_at: Optional[int] = None
    """Time in epoch milliseconds when this object was deactivated."""

    domain_names: Optional[List[str]] = None
    """Only used by private endpoints to customer-managed private endpoint services.
    
    Domain names of target private link service. When updating this field, the full list of target
    domain_names must be specified."""

    enabled: Optional[bool] = None
    """Only used by private endpoints towards an AWS S3 service.
    
    Update this field to activate/deactivate this private endpoint to allow egress access from
    serverless compute resources."""

    endpoint_name: Optional[str] = None
    """The name of the Azure private endpoint resource."""

    endpoint_service: Optional[str] = None
    """The full target AWS endpoint service name that connects to the destination resources of the
    private endpoint."""

    error_message: Optional[str] = None

    group_id: Optional[str] = None
    """Not used by customer-managed private endpoint services.
    
    The sub-resource type (group ID) of the target resource. Note that to connect to workspace root
    storage (root DBFS), you need two endpoints, one for blob and one for dfs."""

    network_connectivity_config_id: Optional[str] = None
    """The ID of a network connectivity configuration, which is the parent resource of this private
    endpoint rule object."""

    resource_id: Optional[str] = None
    """The Azure resource ID of the target resource."""

    resource_names: Optional[List[str]] = None
    """Only used by private endpoints towards AWS S3 service.
    
    The globally unique S3 bucket names that will be accessed via the VPC endpoint. The bucket names
    must be in the same region as the NCC/endpoint service. When updating this field, we perform
    full update on this field. Please ensure a full list of desired resource_names is provided."""

    rule_id: Optional[str] = None
    """The ID of a private endpoint rule."""

    updated_time: Optional[int] = None
    """Time in epoch milliseconds when this object was updated."""

    vpc_endpoint_id: Optional[str] = None
    """The AWS VPC endpoint ID. You can use this ID to identify the VPC endpoint created by Databricks."""

    def as_dict(self) -> dict:
        """Serializes the NccPrivateEndpointRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.connection_state is not None:
            body["connection_state"] = self.connection_state.value
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.deactivated is not None:
            body["deactivated"] = self.deactivated
        if self.deactivated_at is not None:
            body["deactivated_at"] = self.deactivated_at
        if self.domain_names:
            body["domain_names"] = [v for v in self.domain_names]
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.endpoint_name is not None:
            body["endpoint_name"] = self.endpoint_name
        if self.endpoint_service is not None:
            body["endpoint_service"] = self.endpoint_service
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.group_id is not None:
            body["group_id"] = self.group_id
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.resource_names:
            body["resource_names"] = [v for v in self.resource_names]
        if self.rule_id is not None:
            body["rule_id"] = self.rule_id
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        if self.vpc_endpoint_id is not None:
            body["vpc_endpoint_id"] = self.vpc_endpoint_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NccPrivateEndpointRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.connection_state is not None:
            body["connection_state"] = self.connection_state
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.deactivated is not None:
            body["deactivated"] = self.deactivated
        if self.deactivated_at is not None:
            body["deactivated_at"] = self.deactivated_at
        if self.domain_names:
            body["domain_names"] = self.domain_names
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.endpoint_name is not None:
            body["endpoint_name"] = self.endpoint_name
        if self.endpoint_service is not None:
            body["endpoint_service"] = self.endpoint_service
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.group_id is not None:
            body["group_id"] = self.group_id
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.resource_id is not None:
            body["resource_id"] = self.resource_id
        if self.resource_names:
            body["resource_names"] = self.resource_names
        if self.rule_id is not None:
            body["rule_id"] = self.rule_id
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        if self.vpc_endpoint_id is not None:
            body["vpc_endpoint_id"] = self.vpc_endpoint_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NccPrivateEndpointRule:
        """Deserializes the NccPrivateEndpointRule from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            connection_state=_enum(d, "connection_state", NccPrivateEndpointRulePrivateLinkConnectionState),
            creation_time=d.get("creation_time", None),
            deactivated=d.get("deactivated", None),
            deactivated_at=d.get("deactivated_at", None),
            domain_names=d.get("domain_names", None),
            enabled=d.get("enabled", None),
            endpoint_name=d.get("endpoint_name", None),
            endpoint_service=d.get("endpoint_service", None),
            error_message=d.get("error_message", None),
            group_id=d.get("group_id", None),
            network_connectivity_config_id=d.get("network_connectivity_config_id", None),
            resource_id=d.get("resource_id", None),
            resource_names=d.get("resource_names", None),
            rule_id=d.get("rule_id", None),
            updated_time=d.get("updated_time", None),
            vpc_endpoint_id=d.get("vpc_endpoint_id", None),
        )


class NccPrivateEndpointRulePrivateLinkConnectionState(Enum):

    CREATE_FAILED = "CREATE_FAILED"
    CREATING = "CREATING"
    DISCONNECTED = "DISCONNECTED"
    ESTABLISHED = "ESTABLISHED"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"
    REJECTED = "REJECTED"


@dataclass
class NetworkConnectivityConfiguration:
    """Properties of the new network connectivity configuration."""

    account_id: Optional[str] = None
    """Your Databricks account ID. You can find your account ID in your Databricks accounts console."""

    creation_time: Optional[int] = None
    """Time in epoch milliseconds when this object was created."""

    egress_config: Optional[NccEgressConfig] = None
    """The network connectivity rules that apply to network traffic from your serverless compute
    resources."""

    name: Optional[str] = None
    """The name of the network connectivity configuration. The name can contain alphanumeric
    characters, hyphens, and underscores. The length must be between 3 and 30 characters. The name
    must match the regular expression ^[0-9a-zA-Z-_]{3,30}$"""

    network_connectivity_config_id: Optional[str] = None
    """Databricks network connectivity configuration ID."""

    region: Optional[str] = None
    """The region for the network connectivity configuration. Only workspaces in the same region can be
    attached to the network connectivity configuration."""

    updated_time: Optional[int] = None
    """Time in epoch milliseconds when this object was updated."""

    def as_dict(self) -> dict:
        """Serializes the NetworkConnectivityConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.egress_config:
            body["egress_config"] = self.egress_config.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.region is not None:
            body["region"] = self.region
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NetworkConnectivityConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.account_id is not None:
            body["account_id"] = self.account_id
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.egress_config:
            body["egress_config"] = self.egress_config
        if self.name is not None:
            body["name"] = self.name
        if self.network_connectivity_config_id is not None:
            body["network_connectivity_config_id"] = self.network_connectivity_config_id
        if self.region is not None:
            body["region"] = self.region
        if self.updated_time is not None:
            body["updated_time"] = self.updated_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NetworkConnectivityConfiguration:
        """Deserializes the NetworkConnectivityConfiguration from a dictionary."""
        return cls(
            account_id=d.get("account_id", None),
            creation_time=d.get("creation_time", None),
            egress_config=_from_dict(d, "egress_config", NccEgressConfig),
            name=d.get("name", None),
            network_connectivity_config_id=d.get("network_connectivity_config_id", None),
            region=d.get("region", None),
            updated_time=d.get("updated_time", None),
        )


@dataclass
class NetworkPolicyEgress:
    """The network policies applying for egress traffic. This message is used by the UI/REST API. We
    translate this message to the format expected by the dataplane in Lakehouse Network Manager (for
    the format expected by the dataplane, see networkconfig.textproto). This policy should be
    consistent with [[com.databricks.api.proto.settingspolicy.EgressNetworkPolicy]]. Details see
    API-design: https://docs.google.com/document/d/1DKWO_FpZMCY4cF2O62LpwII1lx8gsnDGG-qgE3t3TOA/"""

    network_access: Optional[EgressNetworkPolicyNetworkAccessPolicy] = None
    """The access policy enforced for egress traffic to the internet."""

    def as_dict(self) -> dict:
        """Serializes the NetworkPolicyEgress into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.network_access:
            body["network_access"] = self.network_access.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NetworkPolicyEgress into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.network_access:
            body["network_access"] = self.network_access
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NetworkPolicyEgress:
        """Deserializes the NetworkPolicyEgress from a dictionary."""
        return cls(network_access=_from_dict(d, "network_access", EgressNetworkPolicyNetworkAccessPolicy))


@dataclass
class NotificationDestination:
    config: Optional[Config] = None
    """The configuration for the notification destination. Will be exactly one of the nested configs.
    Only returns for users with workspace admin permissions."""

    destination_type: Optional[DestinationType] = None
    """[Output-only] The type of the notification destination. The type can not be changed once set."""

    display_name: Optional[str] = None
    """The display name for the notification destination."""

    id: Optional[str] = None
    """UUID identifying notification destination."""

    def as_dict(self) -> dict:
        """Serializes the NotificationDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.config:
            body["config"] = self.config.as_dict()
        if self.destination_type is not None:
            body["destination_type"] = self.destination_type.value
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotificationDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.config:
            body["config"] = self.config
        if self.destination_type is not None:
            body["destination_type"] = self.destination_type
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.id is not None:
            body["id"] = self.id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotificationDestination:
        """Deserializes the NotificationDestination from a dictionary."""
        return cls(
            config=_from_dict(d, "config", Config),
            destination_type=_enum(d, "destination_type", DestinationType),
            display_name=d.get("display_name", None),
            id=d.get("id", None),
        )


@dataclass
class PagerdutyConfig:
    integration_key: Optional[str] = None
    """[Input-Only] Integration key for PagerDuty."""

    integration_key_set: Optional[bool] = None
    """[Output-Only] Whether integration key is set."""

    def as_dict(self) -> dict:
        """Serializes the PagerdutyConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.integration_key is not None:
            body["integration_key"] = self.integration_key
        if self.integration_key_set is not None:
            body["integration_key_set"] = self.integration_key_set
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PagerdutyConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.integration_key is not None:
            body["integration_key"] = self.integration_key
        if self.integration_key_set is not None:
            body["integration_key_set"] = self.integration_key_set
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PagerdutyConfig:
        """Deserializes the PagerdutyConfig from a dictionary."""
        return cls(
            integration_key=d.get("integration_key", None), integration_key_set=d.get("integration_key_set", None)
        )


@dataclass
class PartitionId:
    """Partition by workspace or account"""

    workspace_id: Optional[int] = None
    """The ID of the workspace."""

    def as_dict(self) -> dict:
        """Serializes the PartitionId into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.workspace_id is not None:
            body["workspaceId"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PartitionId into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.workspace_id is not None:
            body["workspaceId"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PartitionId:
        """Deserializes the PartitionId from a dictionary."""
        return cls(workspace_id=d.get("workspaceId", None))


@dataclass
class PersonalComputeMessage:
    value: PersonalComputeMessageEnum

    def as_dict(self) -> dict:
        """Serializes the PersonalComputeMessage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.value is not None:
            body["value"] = self.value.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PersonalComputeMessage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PersonalComputeMessage:
        """Deserializes the PersonalComputeMessage from a dictionary."""
        return cls(value=_enum(d, "value", PersonalComputeMessageEnum))


class PersonalComputeMessageEnum(Enum):
    """ON: Grants all users in all workspaces access to the Personal Compute default policy, allowing
    all users to create single-machine compute resources. DELEGATE: Moves access control for the
    Personal Compute default policy to individual workspaces and requires a workspace’s users or
    groups to be added to the ACLs of that workspace’s Personal Compute default policy before they
    will be able to create compute resources through that policy."""

    DELEGATE = "DELEGATE"
    ON = "ON"


@dataclass
class PersonalComputeSetting:
    personal_compute: PersonalComputeMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the PersonalComputeSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.personal_compute:
            body["personal_compute"] = self.personal_compute.as_dict()
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PersonalComputeSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.personal_compute:
            body["personal_compute"] = self.personal_compute
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PersonalComputeSetting:
        """Deserializes the PersonalComputeSetting from a dictionary."""
        return cls(
            etag=d.get("etag", None),
            personal_compute=_from_dict(d, "personal_compute", PersonalComputeMessage),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class PublicTokenInfo:
    comment: Optional[str] = None
    """Comment the token was created with, if applicable."""

    creation_time: Optional[int] = None
    """Server time (in epoch milliseconds) when the token was created."""

    expiry_time: Optional[int] = None
    """Server time (in epoch milliseconds) when the token will expire, or -1 if not applicable."""

    token_id: Optional[str] = None
    """The ID of this token."""

    def as_dict(self) -> dict:
        """Serializes the PublicTokenInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.expiry_time is not None:
            body["expiry_time"] = self.expiry_time
        if self.token_id is not None:
            body["token_id"] = self.token_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PublicTokenInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.expiry_time is not None:
            body["expiry_time"] = self.expiry_time
        if self.token_id is not None:
            body["token_id"] = self.token_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PublicTokenInfo:
        """Deserializes the PublicTokenInfo from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            creation_time=d.get("creation_time", None),
            expiry_time=d.get("expiry_time", None),
            token_id=d.get("token_id", None),
        )


@dataclass
class RestrictWorkspaceAdminsMessage:
    status: RestrictWorkspaceAdminsMessageStatus

    def as_dict(self) -> dict:
        """Serializes the RestrictWorkspaceAdminsMessage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.status is not None:
            body["status"] = self.status.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RestrictWorkspaceAdminsMessage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.status is not None:
            body["status"] = self.status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RestrictWorkspaceAdminsMessage:
        """Deserializes the RestrictWorkspaceAdminsMessage from a dictionary."""
        return cls(status=_enum(d, "status", RestrictWorkspaceAdminsMessageStatus))


class RestrictWorkspaceAdminsMessageStatus(Enum):

    ALLOW_ALL = "ALLOW_ALL"
    RESTRICT_TOKENS_AND_JOB_RUN_AS = "RESTRICT_TOKENS_AND_JOB_RUN_AS"


@dataclass
class RestrictWorkspaceAdminsSetting:
    restrict_workspace_admins: RestrictWorkspaceAdminsMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the RestrictWorkspaceAdminsSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.restrict_workspace_admins:
            body["restrict_workspace_admins"] = self.restrict_workspace_admins.as_dict()
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RestrictWorkspaceAdminsSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.etag is not None:
            body["etag"] = self.etag
        if self.restrict_workspace_admins:
            body["restrict_workspace_admins"] = self.restrict_workspace_admins
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RestrictWorkspaceAdminsSetting:
        """Deserializes the RestrictWorkspaceAdminsSetting from a dictionary."""
        return cls(
            etag=d.get("etag", None),
            restrict_workspace_admins=_from_dict(d, "restrict_workspace_admins", RestrictWorkspaceAdminsMessage),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class RevokeTokenResponse:
    def as_dict(self) -> dict:
        """Serializes the RevokeTokenResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RevokeTokenResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RevokeTokenResponse:
        """Deserializes the RevokeTokenResponse from a dictionary."""
        return cls()


@dataclass
class SlackConfig:
    channel_id: Optional[str] = None
    """[Input-Only] Slack channel ID for notifications."""

    channel_id_set: Optional[bool] = None
    """[Output-Only] Whether channel ID is set."""

    oauth_token: Optional[str] = None
    """[Input-Only] OAuth token for Slack authentication."""

    oauth_token_set: Optional[bool] = None
    """[Output-Only] Whether OAuth token is set."""

    url: Optional[str] = None
    """[Input-Only] URL for Slack destination."""

    url_set: Optional[bool] = None
    """[Output-Only] Whether URL is set."""

    def as_dict(self) -> dict:
        """Serializes the SlackConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.channel_id is not None:
            body["channel_id"] = self.channel_id
        if self.channel_id_set is not None:
            body["channel_id_set"] = self.channel_id_set
        if self.oauth_token is not None:
            body["oauth_token"] = self.oauth_token
        if self.oauth_token_set is not None:
            body["oauth_token_set"] = self.oauth_token_set
        if self.url is not None:
            body["url"] = self.url
        if self.url_set is not None:
            body["url_set"] = self.url_set
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SlackConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.channel_id is not None:
            body["channel_id"] = self.channel_id
        if self.channel_id_set is not None:
            body["channel_id_set"] = self.channel_id_set
        if self.oauth_token is not None:
            body["oauth_token"] = self.oauth_token
        if self.oauth_token_set is not None:
            body["oauth_token_set"] = self.oauth_token_set
        if self.url is not None:
            body["url"] = self.url
        if self.url_set is not None:
            body["url_set"] = self.url_set
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SlackConfig:
        """Deserializes the SlackConfig from a dictionary."""
        return cls(
            channel_id=d.get("channel_id", None),
            channel_id_set=d.get("channel_id_set", None),
            oauth_token=d.get("oauth_token", None),
            oauth_token_set=d.get("oauth_token_set", None),
            url=d.get("url", None),
            url_set=d.get("url_set", None),
        )


@dataclass
class SqlResultsDownload:
    boolean_val: BooleanMessage

    etag: Optional[str] = None
    """etag used for versioning. The response is at least as fresh as the eTag provided. This is used
    for optimistic concurrency control as a way to help prevent simultaneous writes of a setting
    overwriting each other. It is strongly suggested that systems make use of the etag in the read
    -> update pattern to perform setting updates in order to avoid race conditions. That is, get an
    etag from a GET request, and pass it with the PATCH request to identify the setting version you
    are updating."""

    setting_name: Optional[str] = None
    """Name of the corresponding setting. This field is populated in the response, but it will not be
    respected even if it's set in the request body. The setting name in the path parameter will be
    respected instead. Setting name is required to be 'default' if the setting only has one instance
    per workspace."""

    def as_dict(self) -> dict:
        """Serializes the SqlResultsDownload into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val.as_dict()
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlResultsDownload into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.boolean_val:
            body["boolean_val"] = self.boolean_val
        if self.etag is not None:
            body["etag"] = self.etag
        if self.setting_name is not None:
            body["setting_name"] = self.setting_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlResultsDownload:
        """Deserializes the SqlResultsDownload from a dictionary."""
        return cls(
            boolean_val=_from_dict(d, "boolean_val", BooleanMessage),
            etag=d.get("etag", None),
            setting_name=d.get("setting_name", None),
        )


@dataclass
class StringMessage:
    value: Optional[str] = None
    """Represents a generic string value."""

    def as_dict(self) -> dict:
        """Serializes the StringMessage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StringMessage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StringMessage:
        """Deserializes the StringMessage from a dictionary."""
        return cls(value=d.get("value", None))


@dataclass
class TokenAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[TokenPermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the TokenAccessControlRequest into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the TokenAccessControlRequest into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> TokenAccessControlRequest:
        """Deserializes the TokenAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", TokenPermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class TokenAccessControlResponse:
    all_permissions: Optional[List[TokenPermission]] = None
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
        """Serializes the TokenAccessControlResponse into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the TokenAccessControlResponse into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> TokenAccessControlResponse:
        """Deserializes the TokenAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", TokenPermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class TokenInfo:
    comment: Optional[str] = None
    """Comment that describes the purpose of the token, specified by the token creator."""

    created_by_id: Optional[int] = None
    """User ID of the user that created the token."""

    created_by_username: Optional[str] = None
    """Username of the user that created the token."""

    creation_time: Optional[int] = None
    """Timestamp when the token was created."""

    expiry_time: Optional[int] = None
    """Timestamp when the token expires."""

    last_used_day: Optional[int] = None
    """Approximate timestamp for the day the token was last used. Accurate up to 1 day."""

    owner_id: Optional[int] = None
    """User ID of the user that owns the token."""

    token_id: Optional[str] = None
    """ID of the token."""

    workspace_id: Optional[int] = None
    """If applicable, the ID of the workspace that the token was created in."""

    def as_dict(self) -> dict:
        """Serializes the TokenInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_by_id is not None:
            body["created_by_id"] = self.created_by_id
        if self.created_by_username is not None:
            body["created_by_username"] = self.created_by_username
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.expiry_time is not None:
            body["expiry_time"] = self.expiry_time
        if self.last_used_day is not None:
            body["last_used_day"] = self.last_used_day
        if self.owner_id is not None:
            body["owner_id"] = self.owner_id
        if self.token_id is not None:
            body["token_id"] = self.token_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TokenInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_by_id is not None:
            body["created_by_id"] = self.created_by_id
        if self.created_by_username is not None:
            body["created_by_username"] = self.created_by_username
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.expiry_time is not None:
            body["expiry_time"] = self.expiry_time
        if self.last_used_day is not None:
            body["last_used_day"] = self.last_used_day
        if self.owner_id is not None:
            body["owner_id"] = self.owner_id
        if self.token_id is not None:
            body["token_id"] = self.token_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TokenInfo:
        """Deserializes the TokenInfo from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            created_by_id=d.get("created_by_id", None),
            created_by_username=d.get("created_by_username", None),
            creation_time=d.get("creation_time", None),
            expiry_time=d.get("expiry_time", None),
            last_used_day=d.get("last_used_day", None),
            owner_id=d.get("owner_id", None),
            token_id=d.get("token_id", None),
            workspace_id=d.get("workspace_id", None),
        )


@dataclass
class TokenPermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[TokenPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the TokenPermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TokenPermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TokenPermission:
        """Deserializes the TokenPermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", TokenPermissionLevel),
        )


class TokenPermissionLevel(Enum):
    """Permission level"""

    CAN_USE = "CAN_USE"


@dataclass
class TokenPermissions:
    access_control_list: Optional[List[TokenAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the TokenPermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TokenPermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TokenPermissions:
        """Deserializes the TokenPermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", TokenAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class TokenPermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[TokenPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the TokenPermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TokenPermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TokenPermissionsDescription:
        """Deserializes the TokenPermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None), permission_level=_enum(d, "permission_level", TokenPermissionLevel)
        )


class TokenType(Enum):
    """The type of token request. As of now, only `AZURE_ACTIVE_DIRECTORY_TOKEN` is supported."""

    ARCLIGHT_AZURE_EXCHANGE_TOKEN = "ARCLIGHT_AZURE_EXCHANGE_TOKEN"
    ARCLIGHT_AZURE_EXCHANGE_TOKEN_WITH_USER_DELEGATION_KEY = "ARCLIGHT_AZURE_EXCHANGE_TOKEN_WITH_USER_DELEGATION_KEY"
    ARCLIGHT_MULTI_TENANT_AZURE_EXCHANGE_TOKEN = "ARCLIGHT_MULTI_TENANT_AZURE_EXCHANGE_TOKEN"
    ARCLIGHT_MULTI_TENANT_AZURE_EXCHANGE_TOKEN_WITH_USER_DELEGATION_KEY = (
        "ARCLIGHT_MULTI_TENANT_AZURE_EXCHANGE_TOKEN_WITH_USER_DELEGATION_KEY"
    )
    AZURE_ACTIVE_DIRECTORY_TOKEN = "AZURE_ACTIVE_DIRECTORY_TOKEN"


@dataclass
class UpdatePrivateEndpointRule:
    """Properties of the new private endpoint rule. Note that you must approve the endpoint in Azure
    portal after initialization."""

    domain_names: Optional[List[str]] = None
    """Only used by private endpoints to customer-managed private endpoint services.
    
    Domain names of target private link service. When updating this field, the full list of target
    domain_names must be specified."""

    enabled: Optional[bool] = None
    """Only used by private endpoints towards an AWS S3 service.
    
    Update this field to activate/deactivate this private endpoint to allow egress access from
    serverless compute resources."""

    error_message: Optional[str] = None

    resource_names: Optional[List[str]] = None
    """Only used by private endpoints towards AWS S3 service.
    
    The globally unique S3 bucket names that will be accessed via the VPC endpoint. The bucket names
    must be in the same region as the NCC/endpoint service. When updating this field, we perform
    full update on this field. Please ensure a full list of desired resource_names is provided."""

    def as_dict(self) -> dict:
        """Serializes the UpdatePrivateEndpointRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.domain_names:
            body["domain_names"] = [v for v in self.domain_names]
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.resource_names:
            body["resource_names"] = [v for v in self.resource_names]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdatePrivateEndpointRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.domain_names:
            body["domain_names"] = self.domain_names
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.resource_names:
            body["resource_names"] = self.resource_names
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdatePrivateEndpointRule:
        """Deserializes the UpdatePrivateEndpointRule from a dictionary."""
        return cls(
            domain_names=d.get("domain_names", None),
            enabled=d.get("enabled", None),
            error_message=d.get("error_message", None),
            resource_names=d.get("resource_names", None),
        )


WorkspaceConf = Dict[str, str]


@dataclass
class WorkspaceNetworkOption:
    network_policy_id: Optional[str] = None
    """The network policy ID to apply to the workspace. This controls the network access rules for all
    serverless compute resources in the workspace. Each workspace can only be linked to one policy
    at a time. If no policy is explicitly assigned, the workspace will use 'default-policy'."""

    workspace_id: Optional[int] = None
    """The workspace ID."""

    def as_dict(self) -> dict:
        """Serializes the WorkspaceNetworkOption into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.network_policy_id is not None:
            body["network_policy_id"] = self.network_policy_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WorkspaceNetworkOption into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.network_policy_id is not None:
            body["network_policy_id"] = self.network_policy_id
        if self.workspace_id is not None:
            body["workspace_id"] = self.workspace_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WorkspaceNetworkOption:
        """Deserializes the WorkspaceNetworkOption from a dictionary."""
        return cls(network_policy_id=d.get("network_policy_id", None), workspace_id=d.get("workspace_id", None))


class AccountIpAccessListsAPI:
    """The Accounts IP Access List API enables account admins to configure IP access lists for access to the
    account console.

    Account IP Access Lists affect web application access and REST API access to the account console and
    account APIs. If the feature is disabled for the account, all access is allowed for this account. There is
    support for allow lists (inclusion) and block lists (exclusion).

    When a connection is attempted: 1. **First, all block lists are checked.** If the connection IP address
    matches any block list, the connection is rejected. 2. **If the connection was not rejected by block
    lists**, the IP address is compared with the allow lists.

    If there is at least one allow list for the account, the connection is allowed only if the IP address
    matches an allow list. If there are no allow lists for the account, all IP addresses are allowed.

    For all allow lists and block lists combined, the account supports a maximum of 1000 IP/CIDR values, where
    one CIDR counts as a single value.

    After changes to the account-level IP access lists, it can take a few minutes for changes to take effect."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, label: str, list_type: ListType, *, ip_addresses: Optional[List[str]] = None
    ) -> CreateIpAccessListResponse:
        """Creates an IP access list for the account.

        A list can be an allow list or a block list. See the top of this file for a description of how the
        server treats allow lists and block lists at runtime.

        When creating or updating an IP access list:

        * For all allow lists and block lists combined, the API supports a maximum of 1000 IP/CIDR values,
        where one CIDR counts as a single value. Attempts to exceed that number return error 400 with
        `error_code` value `QUOTA_EXCEEDED`. * If the new list would block the calling user's current IP,
        error 400 is returned with `error_code` value `INVALID_STATE`.

        It can take a few minutes for the changes to take effect.

        :param label: str
          Label for the IP access list. This **cannot** be empty.
        :param list_type: :class:`ListType`
        :param ip_addresses: List[str] (optional)

        :returns: :class:`CreateIpAccessListResponse`
        """

        body = {}
        if ip_addresses is not None:
            body["ip_addresses"] = [v for v in ip_addresses]
        if label is not None:
            body["label"] = label
        if list_type is not None:
            body["list_type"] = list_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/ip-access-lists", body=body, headers=headers
        )
        return CreateIpAccessListResponse.from_dict(res)

    def delete(self, ip_access_list_id: str):
        """Deletes an IP access list, specified by its list ID.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list


        """

        headers = {}

        self._api.do(
            "DELETE", f"/api/2.0/accounts/{self._api.account_id}/ip-access-lists/{ip_access_list_id}", headers=headers
        )

    def get(self, ip_access_list_id: str) -> GetIpAccessListResponse:
        """Gets an IP access list, specified by its list ID.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list

        :returns: :class:`GetIpAccessListResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/ip-access-lists/{ip_access_list_id}", headers=headers
        )
        return GetIpAccessListResponse.from_dict(res)

    def list(self) -> Iterator[IpAccessListInfo]:
        """Gets all IP access lists for the specified account.


        :returns: Iterator over :class:`IpAccessListInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        json = self._api.do("GET", f"/api/2.0/accounts/{self._api.account_id}/ip-access-lists", headers=headers)
        parsed = GetIpAccessListsResponse.from_dict(json).ip_access_lists
        return parsed if parsed is not None else []

    def replace(
        self,
        ip_access_list_id: str,
        label: str,
        list_type: ListType,
        enabled: bool,
        *,
        ip_addresses: Optional[List[str]] = None,
    ):
        """Replaces an IP access list, specified by its ID.

        A list can include allow lists and block lists. See the top of this file for a description of how the
        server treats allow lists and block lists at run time. When replacing an IP access list: * For all
        allow lists and block lists combined, the API supports a maximum of 1000 IP/CIDR values, where one
        CIDR counts as a single value. Attempts to exceed that number return error 400 with `error_code` value
        `QUOTA_EXCEEDED`. * If the resulting list would block the calling user's current IP, error 400 is
        returned with `error_code` value `INVALID_STATE`. It can take a few minutes for the changes to take
        effect.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list
        :param label: str
          Label for the IP access list. This **cannot** be empty.
        :param list_type: :class:`ListType`
        :param enabled: bool
          Specifies whether this IP access list is enabled.
        :param ip_addresses: List[str] (optional)


        """

        body = {}
        if enabled is not None:
            body["enabled"] = enabled
        if ip_addresses is not None:
            body["ip_addresses"] = [v for v in ip_addresses]
        if label is not None:
            body["label"] = label
        if list_type is not None:
            body["list_type"] = list_type.value
        headers = {
            "Content-Type": "application/json",
        }

        self._api.do(
            "PUT",
            f"/api/2.0/accounts/{self._api.account_id}/ip-access-lists/{ip_access_list_id}",
            body=body,
            headers=headers,
        )

    def update(
        self,
        ip_access_list_id: str,
        *,
        enabled: Optional[bool] = None,
        ip_addresses: Optional[List[str]] = None,
        label: Optional[str] = None,
        list_type: Optional[ListType] = None,
    ):
        """Updates an existing IP access list, specified by its ID.

        A list can include allow lists and block lists. See the top of this file for a description of how the
        server treats allow lists and block lists at run time.

        When updating an IP access list:

        * For all allow lists and block lists combined, the API supports a maximum of 1000 IP/CIDR values,
        where one CIDR counts as a single value. Attempts to exceed that number return error 400 with
        `error_code` value `QUOTA_EXCEEDED`. * If the updated list would block the calling user's current IP,
        error 400 is returned with `error_code` value `INVALID_STATE`.

        It can take a few minutes for the changes to take effect.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list
        :param enabled: bool (optional)
          Specifies whether this IP access list is enabled.
        :param ip_addresses: List[str] (optional)
        :param label: str (optional)
          Label for the IP access list. This **cannot** be empty.
        :param list_type: :class:`ListType` (optional)


        """

        body = {}
        if enabled is not None:
            body["enabled"] = enabled
        if ip_addresses is not None:
            body["ip_addresses"] = [v for v in ip_addresses]
        if label is not None:
            body["label"] = label
        if list_type is not None:
            body["list_type"] = list_type.value
        headers = {
            "Content-Type": "application/json",
        }

        self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/ip-access-lists/{ip_access_list_id}",
            body=body,
            headers=headers,
        )


class AccountSettingsAPI:
    """Accounts Settings API allows users to manage settings at the account level."""

    def __init__(self, api_client):
        self._api = api_client

        self._csp_enablement_account = CspEnablementAccountAPI(self._api)
        self._disable_legacy_features = DisableLegacyFeaturesAPI(self._api)
        self._enable_ip_access_lists = EnableIpAccessListsAPI(self._api)
        self._esm_enablement_account = EsmEnablementAccountAPI(self._api)
        self._llm_proxy_partner_powered_account = LlmProxyPartnerPoweredAccountAPI(self._api)
        self._llm_proxy_partner_powered_enforce = LlmProxyPartnerPoweredEnforceAPI(self._api)
        self._personal_compute = PersonalComputeAPI(self._api)

    @property
    def csp_enablement_account(self) -> CspEnablementAccountAPI:
        """The compliance security profile settings at the account level control whether to enable it for new workspaces."""
        return self._csp_enablement_account

    @property
    def disable_legacy_features(self) -> DisableLegacyFeaturesAPI:
        """Disable legacy features for new Databricks workspaces."""
        return self._disable_legacy_features

    @property
    def enable_ip_access_lists(self) -> EnableIpAccessListsAPI:
        """Controls the enforcement of IP access lists for accessing the account console."""
        return self._enable_ip_access_lists

    @property
    def esm_enablement_account(self) -> EsmEnablementAccountAPI:
        """The enhanced security monitoring setting at the account level controls whether to enable the feature on new workspaces."""
        return self._esm_enablement_account

    @property
    def llm_proxy_partner_powered_account(self) -> LlmProxyPartnerPoweredAccountAPI:
        """Determines if partner powered models are enabled or not for a specific account."""
        return self._llm_proxy_partner_powered_account

    @property
    def llm_proxy_partner_powered_enforce(self) -> LlmProxyPartnerPoweredEnforceAPI:
        """Determines if the account-level partner-powered setting value is enforced upon the workspace-level partner-powered setting."""
        return self._llm_proxy_partner_powered_enforce

    @property
    def personal_compute(self) -> PersonalComputeAPI:
        """The Personal Compute enablement setting lets you control which users can use the Personal Compute default policy to create compute resources."""
        return self._personal_compute


class AibiDashboardEmbeddingAccessPolicyAPI:
    """Controls whether AI/BI published dashboard embedding is enabled, conditionally enabled, or disabled at the
    workspace level. By default, this setting is conditionally enabled (ALLOW_APPROVED_DOMAINS)."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteAibiDashboardEmbeddingAccessPolicySettingResponse:
        """Delete the AI/BI dashboard embedding access policy, reverting back to the default.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteAibiDashboardEmbeddingAccessPolicySettingResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE",
            "/api/2.0/settings/types/aibi_dash_embed_ws_acc_policy/names/default",
            query=query,
            headers=headers,
        )
        return DeleteAibiDashboardEmbeddingAccessPolicySettingResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> AibiDashboardEmbeddingAccessPolicySetting:
        """Retrieves the AI/BI dashboard embedding access policy. The default setting is ALLOW_APPROVED_DOMAINS,
        permitting AI/BI dashboards to be embedded on approved domains.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`AibiDashboardEmbeddingAccessPolicySetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/aibi_dash_embed_ws_acc_policy/names/default", query=query, headers=headers
        )
        return AibiDashboardEmbeddingAccessPolicySetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: AibiDashboardEmbeddingAccessPolicySetting, field_mask: str
    ) -> AibiDashboardEmbeddingAccessPolicySetting:
        """Updates the AI/BI dashboard embedding access policy at the workspace level.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`AibiDashboardEmbeddingAccessPolicySetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`AibiDashboardEmbeddingAccessPolicySetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/aibi_dash_embed_ws_acc_policy/names/default", body=body, headers=headers
        )
        return AibiDashboardEmbeddingAccessPolicySetting.from_dict(res)


class AibiDashboardEmbeddingApprovedDomainsAPI:
    """Controls the list of domains approved to host the embedded AI/BI dashboards. The approved domains list
    can't be mutated when the current access policy is not set to ALLOW_APPROVED_DOMAINS."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse:
        """Delete the list of domains approved to host embedded AI/BI dashboards, reverting back to the default
        empty list.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE",
            "/api/2.0/settings/types/aibi_dash_embed_ws_apprvd_domains/names/default",
            query=query,
            headers=headers,
        )
        return DeleteAibiDashboardEmbeddingApprovedDomainsSettingResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> AibiDashboardEmbeddingApprovedDomainsSetting:
        """Retrieves the list of domains approved to host embedded AI/BI dashboards.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`AibiDashboardEmbeddingApprovedDomainsSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            "/api/2.0/settings/types/aibi_dash_embed_ws_apprvd_domains/names/default",
            query=query,
            headers=headers,
        )
        return AibiDashboardEmbeddingApprovedDomainsSetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: AibiDashboardEmbeddingApprovedDomainsSetting, field_mask: str
    ) -> AibiDashboardEmbeddingApprovedDomainsSetting:
        """Updates the list of domains approved to host embedded AI/BI dashboards. This update will fail if the
        current workspace access policy is not ALLOW_APPROVED_DOMAINS.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`AibiDashboardEmbeddingApprovedDomainsSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`AibiDashboardEmbeddingApprovedDomainsSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH",
            "/api/2.0/settings/types/aibi_dash_embed_ws_apprvd_domains/names/default",
            body=body,
            headers=headers,
        )
        return AibiDashboardEmbeddingApprovedDomainsSetting.from_dict(res)


class AutomaticClusterUpdateAPI:
    """Controls whether automatic cluster update is enabled for the current workspace. By default, it is turned
    off."""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, *, etag: Optional[str] = None) -> AutomaticClusterUpdateSetting:
        """Gets the automatic cluster update setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`AutomaticClusterUpdateSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/automatic_cluster_update/names/default", query=query, headers=headers
        )
        return AutomaticClusterUpdateSetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: AutomaticClusterUpdateSetting, field_mask: str
    ) -> AutomaticClusterUpdateSetting:
        """Updates the automatic cluster update setting for the workspace. A fresh etag needs to be provided in
        `PATCH` requests (as part of the setting field). The etag can be retrieved by making a `GET` request
        before the `PATCH` request. If the setting is updated concurrently, `PATCH` fails with 409 and the
        request must be retried by using the fresh etag in the 409 response.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`AutomaticClusterUpdateSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`AutomaticClusterUpdateSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/automatic_cluster_update/names/default", body=body, headers=headers
        )
        return AutomaticClusterUpdateSetting.from_dict(res)


class ComplianceSecurityProfileAPI:
    """Controls whether to enable the compliance security profile for the current workspace. Enabling it on a
    workspace is permanent. By default, it is turned off.

    This settings can NOT be disabled once it is enabled."""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, *, etag: Optional[str] = None) -> ComplianceSecurityProfileSetting:
        """Gets the compliance security profile setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`ComplianceSecurityProfileSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/shield_csp_enablement_ws_db/names/default", query=query, headers=headers
        )
        return ComplianceSecurityProfileSetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: ComplianceSecurityProfileSetting, field_mask: str
    ) -> ComplianceSecurityProfileSetting:
        """Updates the compliance security profile setting for the workspace. A fresh etag needs to be provided
        in `PATCH` requests (as part of the setting field). The etag can be retrieved by making a `GET`
        request before the `PATCH` request. If the setting is updated concurrently, `PATCH` fails with 409 and
        the request must be retried by using the fresh etag in the 409 response.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`ComplianceSecurityProfileSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`ComplianceSecurityProfileSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/shield_csp_enablement_ws_db/names/default", body=body, headers=headers
        )
        return ComplianceSecurityProfileSetting.from_dict(res)


class CredentialsManagerAPI:
    """Credentials manager interacts with with Identity Providers to to perform token exchanges using stored
    credentials and refresh tokens."""

    def __init__(self, api_client):
        self._api = api_client

    def exchange_token(
        self, partition_id: PartitionId, token_type: List[TokenType], scopes: List[str]
    ) -> ExchangeTokenResponse:
        """Exchange tokens with an Identity Provider to get a new access token. It allows specifying scopes to
        determine token permissions.

        :param partition_id: :class:`PartitionId`
          The partition of Credentials store
        :param token_type: List[:class:`TokenType`]
          A list of token types being requested
        :param scopes: List[str]
          Array of scopes for the token request.

        :returns: :class:`ExchangeTokenResponse`
        """

        body = {}
        if partition_id is not None:
            body["partitionId"] = partition_id.as_dict()
        if scopes is not None:
            body["scopes"] = [v for v in scopes]
        if token_type is not None:
            body["tokenType"] = [v.value for v in token_type]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/credentials-manager/exchange-tokens/token", body=body, headers=headers)
        return ExchangeTokenResponse.from_dict(res)


class CspEnablementAccountAPI:
    """The compliance security profile settings at the account level control whether to enable it for new
    workspaces. By default, this account-level setting is disabled for new workspaces. After workspace
    creation, account admins can enable the compliance security profile individually for each workspace.

    This settings can be disabled so that new workspaces do not have compliance security profile enabled by
    default."""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, *, etag: Optional[str] = None) -> CspEnablementAccountSetting:
        """Gets the compliance security profile setting for new workspaces.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`CspEnablementAccountSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/shield_csp_enablement_ac/names/default",
            query=query,
            headers=headers,
        )
        return CspEnablementAccountSetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: CspEnablementAccountSetting, field_mask: str
    ) -> CspEnablementAccountSetting:
        """Updates the value of the compliance security profile setting for new workspaces.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`CspEnablementAccountSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`CspEnablementAccountSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/shield_csp_enablement_ac/names/default",
            body=body,
            headers=headers,
        )
        return CspEnablementAccountSetting.from_dict(res)


class DashboardEmailSubscriptionsAPI:
    """Controls whether schedules or workload tasks for refreshing AI/BI Dashboards in the workspace can send
    subscription emails containing PDFs and/or images of the dashboard. By default, this setting is enabled
    (set to `true`)"""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteDashboardEmailSubscriptionsResponse:
        """Reverts the Dashboard Email Subscriptions setting to its default value.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteDashboardEmailSubscriptionsResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE",
            "/api/2.0/settings/types/dashboard_email_subscriptions/names/default",
            query=query,
            headers=headers,
        )
        return DeleteDashboardEmailSubscriptionsResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> DashboardEmailSubscriptions:
        """Gets the Dashboard Email Subscriptions setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DashboardEmailSubscriptions`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/dashboard_email_subscriptions/names/default", query=query, headers=headers
        )
        return DashboardEmailSubscriptions.from_dict(res)

    def update(
        self, allow_missing: bool, setting: DashboardEmailSubscriptions, field_mask: str
    ) -> DashboardEmailSubscriptions:
        """Updates the Dashboard Email Subscriptions setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`DashboardEmailSubscriptions`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`DashboardEmailSubscriptions`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/dashboard_email_subscriptions/names/default", body=body, headers=headers
        )
        return DashboardEmailSubscriptions.from_dict(res)


class DefaultNamespaceAPI:
    """The default namespace setting API allows users to configure the default namespace for a Databricks
    workspace.

    Through this API, users can retrieve, set, or modify the default namespace used when queries do not
    reference a fully qualified three-level name. For example, if you use the API to set 'retail_prod' as the
    default catalog, then a query 'SELECT * FROM myTable' would reference the object
    'retail_prod.default.myTable' (the schema 'default' is always assumed).

    This setting requires a restart of clusters and SQL warehouses to take effect. Additionally, the default
    namespace only applies when using Unity Catalog-enabled compute."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteDefaultNamespaceSettingResponse:
        """Deletes the default namespace setting for the workspace. A fresh etag needs to be provided in `DELETE`
        requests (as a query parameter). The etag can be retrieved by making a `GET` request before the
        `DELETE` request. If the setting is updated/deleted concurrently, `DELETE` fails with 409 and the
        request must be retried by using the fresh etag in the 409 response.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteDefaultNamespaceSettingResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE", "/api/2.0/settings/types/default_namespace_ws/names/default", query=query, headers=headers
        )
        return DeleteDefaultNamespaceSettingResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> DefaultNamespaceSetting:
        """Gets the default namespace setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DefaultNamespaceSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/default_namespace_ws/names/default", query=query, headers=headers
        )
        return DefaultNamespaceSetting.from_dict(res)

    def update(self, allow_missing: bool, setting: DefaultNamespaceSetting, field_mask: str) -> DefaultNamespaceSetting:
        """Updates the default namespace setting for the workspace. A fresh etag needs to be provided in `PATCH`
        requests (as part of the setting field). The etag can be retrieved by making a `GET` request before
        the `PATCH` request. Note that if the setting does not exist, `GET` returns a NOT_FOUND error and the
        etag is present in the error response, which should be set in the `PATCH` request. If the setting is
        updated concurrently, `PATCH` fails with 409 and the request must be retried by using the fresh etag
        in the 409 response.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`DefaultNamespaceSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`DefaultNamespaceSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/default_namespace_ws/names/default", body=body, headers=headers
        )
        return DefaultNamespaceSetting.from_dict(res)


class DefaultWarehouseIdAPI:
    """Warehouse to be selected by default for users in this workspace. Covers SQL workloads only and can be
    overridden by users."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteDefaultWarehouseIdResponse:
        """Reverts the Default Warehouse Id setting to its default value.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteDefaultWarehouseIdResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE", "/api/2.0/settings/types/default_warehouse_id/names/default", query=query, headers=headers
        )
        return DeleteDefaultWarehouseIdResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> DefaultWarehouseId:
        """Gets the Default Warehouse Id setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DefaultWarehouseId`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/default_warehouse_id/names/default", query=query, headers=headers
        )
        return DefaultWarehouseId.from_dict(res)

    def update(self, allow_missing: bool, setting: DefaultWarehouseId, field_mask: str) -> DefaultWarehouseId:
        """Updates the Default Warehouse Id setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`DefaultWarehouseId`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`DefaultWarehouseId`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/default_warehouse_id/names/default", body=body, headers=headers
        )
        return DefaultWarehouseId.from_dict(res)


class DisableLegacyAccessAPI:
    """'Disabling legacy access' has the following impacts:

    1. Disables direct access to Hive Metastores from the workspace. However, you can still access a Hive
    Metastore through Hive Metastore federation. 2. Disables fallback mode on external location access from
    the workspace. 3. Disables Databricks Runtime versions prior to 13.3LTS."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteDisableLegacyAccessResponse:
        """Deletes legacy access disablement status.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteDisableLegacyAccessResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE", "/api/2.0/settings/types/disable_legacy_access/names/default", query=query, headers=headers
        )
        return DeleteDisableLegacyAccessResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> DisableLegacyAccess:
        """Retrieves legacy access disablement Status.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DisableLegacyAccess`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/disable_legacy_access/names/default", query=query, headers=headers
        )
        return DisableLegacyAccess.from_dict(res)

    def update(self, allow_missing: bool, setting: DisableLegacyAccess, field_mask: str) -> DisableLegacyAccess:
        """Updates legacy access disablement status.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`DisableLegacyAccess`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`DisableLegacyAccess`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/disable_legacy_access/names/default", body=body, headers=headers
        )
        return DisableLegacyAccess.from_dict(res)


class DisableLegacyDbfsAPI:
    """Disabling legacy DBFS has the following implications:

    1. Access to DBFS root and DBFS mounts is disallowed (as well as the creation of new mounts). 2. Disables
    Databricks Runtime versions prior to 13.3LTS.

    When the setting is off, all DBFS functionality is enabled and no restrictions are imposed on Databricks
    Runtime versions. This setting can take up to 20 minutes to take effect and requires a manual restart of
    all-purpose compute clusters and SQL warehouses."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteDisableLegacyDbfsResponse:
        """Deletes the disable legacy DBFS setting for a workspace, reverting back to the default.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteDisableLegacyDbfsResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE", "/api/2.0/settings/types/disable_legacy_dbfs/names/default", query=query, headers=headers
        )
        return DeleteDisableLegacyDbfsResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> DisableLegacyDbfs:
        """Gets the disable legacy DBFS setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DisableLegacyDbfs`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/disable_legacy_dbfs/names/default", query=query, headers=headers
        )
        return DisableLegacyDbfs.from_dict(res)

    def update(self, allow_missing: bool, setting: DisableLegacyDbfs, field_mask: str) -> DisableLegacyDbfs:
        """Updates the disable legacy DBFS setting for the workspace.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`DisableLegacyDbfs`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`DisableLegacyDbfs`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/disable_legacy_dbfs/names/default", body=body, headers=headers
        )
        return DisableLegacyDbfs.from_dict(res)


class DisableLegacyFeaturesAPI:
    """Disable legacy features for new Databricks workspaces.

    For newly created workspaces: 1. Disables the use of DBFS root and mounts. 2. Hive Metastore will not be
    provisioned. 3. Disables the use of ‘No-isolation clusters’. 4. Disables Databricks Runtime versions
    prior to 13.3LTS."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteDisableLegacyFeaturesResponse:
        """Deletes the disable legacy features setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteDisableLegacyFeaturesResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/disable_legacy_features/names/default",
            query=query,
            headers=headers,
        )
        return DeleteDisableLegacyFeaturesResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> DisableLegacyFeatures:
        """Gets the value of the disable legacy features setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DisableLegacyFeatures`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/disable_legacy_features/names/default",
            query=query,
            headers=headers,
        )
        return DisableLegacyFeatures.from_dict(res)

    def update(self, allow_missing: bool, setting: DisableLegacyFeatures, field_mask: str) -> DisableLegacyFeatures:
        """Updates the value of the disable legacy features setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`DisableLegacyFeatures`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`DisableLegacyFeatures`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/disable_legacy_features/names/default",
            body=body,
            headers=headers,
        )
        return DisableLegacyFeatures.from_dict(res)


class EnableExportNotebookAPI:
    """Controls whether users can export notebooks and files from the Workspace UI. By default, this setting is
    enabled."""

    def __init__(self, api_client):
        self._api = api_client

    def get_enable_export_notebook(self) -> EnableExportNotebook:
        """Gets the Notebook and File exporting setting.


        :returns: :class:`EnableExportNotebook`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/settings/types/enable-export-notebook/names/default", headers=headers)
        return EnableExportNotebook.from_dict(res)

    def patch_enable_export_notebook(
        self, allow_missing: bool, setting: EnableExportNotebook, field_mask: str
    ) -> EnableExportNotebook:
        """Updates the Notebook and File exporting setting. The model follows eventual consistency, which means
        the get after the update operation might receive stale values for some time.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`EnableExportNotebook`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`EnableExportNotebook`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/enable-export-notebook/names/default", body=body, headers=headers
        )
        return EnableExportNotebook.from_dict(res)


class EnableIpAccessListsAPI:
    """Controls the enforcement of IP access lists for accessing the account console. Allowing you to enable or
    disable restricted access based on IP addresses."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteAccountIpAccessEnableResponse:
        """Reverts the value of the account IP access toggle setting to default (ON)

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteAccountIpAccessEnableResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/acct_ip_acl_enable/names/default",
            query=query,
            headers=headers,
        )
        return DeleteAccountIpAccessEnableResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> AccountIpAccessEnable:
        """Gets the value of the account IP access toggle setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`AccountIpAccessEnable`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/acct_ip_acl_enable/names/default",
            query=query,
            headers=headers,
        )
        return AccountIpAccessEnable.from_dict(res)

    def update(self, allow_missing: bool, setting: AccountIpAccessEnable, field_mask: str) -> AccountIpAccessEnable:
        """Updates the value of the account IP access toggle setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`AccountIpAccessEnable`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`AccountIpAccessEnable`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/acct_ip_acl_enable/names/default",
            body=body,
            headers=headers,
        )
        return AccountIpAccessEnable.from_dict(res)


class EnableNotebookTableClipboardAPI:
    """Controls whether users can copy tabular data to the clipboard via the UI. By default, this setting is
    enabled."""

    def __init__(self, api_client):
        self._api = api_client

    def get_enable_notebook_table_clipboard(self) -> EnableNotebookTableClipboard:
        """Gets the Results Table Clipboard features setting.


        :returns: :class:`EnableNotebookTableClipboard`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/enable-notebook-table-clipboard/names/default", headers=headers
        )
        return EnableNotebookTableClipboard.from_dict(res)

    def patch_enable_notebook_table_clipboard(
        self, allow_missing: bool, setting: EnableNotebookTableClipboard, field_mask: str
    ) -> EnableNotebookTableClipboard:
        """Updates the Results Table Clipboard features setting. The model follows eventual consistency, which
        means the get after the update operation might receive stale values for some time.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`EnableNotebookTableClipboard`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`EnableNotebookTableClipboard`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/enable-notebook-table-clipboard/names/default", body=body, headers=headers
        )
        return EnableNotebookTableClipboard.from_dict(res)


class EnableResultsDownloadingAPI:
    """Controls whether users can download notebook results. By default, this setting is enabled."""

    def __init__(self, api_client):
        self._api = api_client

    def get_enable_results_downloading(self) -> EnableResultsDownloading:
        """Gets the Notebook results download setting.


        :returns: :class:`EnableResultsDownloading`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/settings/types/enable-results-downloading/names/default", headers=headers)
        return EnableResultsDownloading.from_dict(res)

    def patch_enable_results_downloading(
        self, allow_missing: bool, setting: EnableResultsDownloading, field_mask: str
    ) -> EnableResultsDownloading:
        """Updates the Notebook results download setting. The model follows eventual consistency, which means the
        get after the update operation might receive stale values for some time.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`EnableResultsDownloading`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`EnableResultsDownloading`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/enable-results-downloading/names/default", body=body, headers=headers
        )
        return EnableResultsDownloading.from_dict(res)


class EnhancedSecurityMonitoringAPI:
    """Controls whether enhanced security monitoring is enabled for the current workspace. If the compliance
    security profile is enabled, this is automatically enabled. By default, it is disabled. However, if the
    compliance security profile is enabled, this is automatically enabled.

    If the compliance security profile is disabled, you can enable or disable this setting and it is not
    permanent."""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, *, etag: Optional[str] = None) -> EnhancedSecurityMonitoringSetting:
        """Gets the enhanced security monitoring setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`EnhancedSecurityMonitoringSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/shield_esm_enablement_ws_db/names/default", query=query, headers=headers
        )
        return EnhancedSecurityMonitoringSetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: EnhancedSecurityMonitoringSetting, field_mask: str
    ) -> EnhancedSecurityMonitoringSetting:
        """Updates the enhanced security monitoring setting for the workspace. A fresh etag needs to be provided
        in `PATCH` requests (as part of the setting field). The etag can be retrieved by making a `GET`
        request before the `PATCH` request. If the setting is updated concurrently, `PATCH` fails with 409 and
        the request must be retried by using the fresh etag in the 409 response.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`EnhancedSecurityMonitoringSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`EnhancedSecurityMonitoringSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/shield_esm_enablement_ws_db/names/default", body=body, headers=headers
        )
        return EnhancedSecurityMonitoringSetting.from_dict(res)


class EsmEnablementAccountAPI:
    """The enhanced security monitoring setting at the account level controls whether to enable the feature on
    new workspaces. By default, this account-level setting is disabled for new workspaces. After workspace
    creation, account admins can enable enhanced security monitoring individually for each workspace."""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, *, etag: Optional[str] = None) -> EsmEnablementAccountSetting:
        """Gets the enhanced security monitoring setting for new workspaces.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`EsmEnablementAccountSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/shield_esm_enablement_ac/names/default",
            query=query,
            headers=headers,
        )
        return EsmEnablementAccountSetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: EsmEnablementAccountSetting, field_mask: str
    ) -> EsmEnablementAccountSetting:
        """Updates the value of the enhanced security monitoring setting for new workspaces.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`EsmEnablementAccountSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`EsmEnablementAccountSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/shield_esm_enablement_ac/names/default",
            body=body,
            headers=headers,
        )
        return EsmEnablementAccountSetting.from_dict(res)


class IpAccessListsAPI:
    """IP Access List enables admins to configure IP access lists.

    IP access lists affect web application access and REST API access to this workspace only. If the feature
    is disabled for a workspace, all access is allowed for this workspace. There is support for allow lists
    (inclusion) and block lists (exclusion).

    When a connection is attempted: 1. **First, all block lists are checked.** If the connection IP address
    matches any block list, the connection is rejected. 2. **If the connection was not rejected by block
    lists**, the IP address is compared with the allow lists.

    If there is at least one allow list for the workspace, the connection is allowed only if the IP address
    matches an allow list. If there are no allow lists for the workspace, all IP addresses are allowed.

    For all allow lists and block lists combined, the workspace supports a maximum of 1000 IP/CIDR values,
    where one CIDR counts as a single value.

    After changes to the IP access list feature, it can take a few minutes for changes to take effect."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, label: str, list_type: ListType, *, ip_addresses: Optional[List[str]] = None
    ) -> CreateIpAccessListResponse:
        """Creates an IP access list for this workspace.

        A list can be an allow list or a block list. See the top of this file for a description of how the
        server treats allow lists and block lists at runtime.

        When creating or updating an IP access list:

        * For all allow lists and block lists combined, the API supports a maximum of 1000 IP/CIDR values,
        where one CIDR counts as a single value. Attempts to exceed that number return error 400 with
        `error_code` value `QUOTA_EXCEEDED`. * If the new list would block the calling user's current IP,
        error 400 is returned with `error_code` value `INVALID_STATE`.

        It can take a few minutes for the changes to take effect. **Note**: Your new IP access list has no
        effect until you enable the feature. See :method:workspaceconf/setStatus

        :param label: str
          Label for the IP access list. This **cannot** be empty.
        :param list_type: :class:`ListType`
        :param ip_addresses: List[str] (optional)

        :returns: :class:`CreateIpAccessListResponse`
        """

        body = {}
        if ip_addresses is not None:
            body["ip_addresses"] = [v for v in ip_addresses]
        if label is not None:
            body["label"] = label
        if list_type is not None:
            body["list_type"] = list_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/ip-access-lists", body=body, headers=headers)
        return CreateIpAccessListResponse.from_dict(res)

    def delete(self, ip_access_list_id: str):
        """Deletes an IP access list, specified by its list ID.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/ip-access-lists/{ip_access_list_id}", headers=headers)

    def get(self, ip_access_list_id: str) -> FetchIpAccessListResponse:
        """Gets an IP access list, specified by its list ID.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list

        :returns: :class:`FetchIpAccessListResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/ip-access-lists/{ip_access_list_id}", headers=headers)
        return FetchIpAccessListResponse.from_dict(res)

    def list(self) -> Iterator[IpAccessListInfo]:
        """Gets all IP access lists for the specified workspace.


        :returns: Iterator over :class:`IpAccessListInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/ip-access-lists", headers=headers)
        parsed = ListIpAccessListResponse.from_dict(json).ip_access_lists
        return parsed if parsed is not None else []

    def replace(
        self,
        ip_access_list_id: str,
        label: str,
        list_type: ListType,
        enabled: bool,
        *,
        ip_addresses: Optional[List[str]] = None,
    ):
        """Replaces an IP access list, specified by its ID.

        A list can include allow lists and block lists. See the top of this file for a description of how the
        server treats allow lists and block lists at run time. When replacing an IP access list: * For all
        allow lists and block lists combined, the API supports a maximum of 1000 IP/CIDR values, where one
        CIDR counts as a single value. Attempts to exceed that number return error 400 with `error_code` value
        `QUOTA_EXCEEDED`. * If the resulting list would block the calling user's current IP, error 400 is
        returned with `error_code` value `INVALID_STATE`. It can take a few minutes for the changes to take
        effect. Note that your resulting IP access list has no effect until you enable the feature. See
        :method:workspaceconf/setStatus.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list
        :param label: str
          Label for the IP access list. This **cannot** be empty.
        :param list_type: :class:`ListType`
        :param enabled: bool
          Specifies whether this IP access list is enabled.
        :param ip_addresses: List[str] (optional)


        """

        body = {}
        if enabled is not None:
            body["enabled"] = enabled
        if ip_addresses is not None:
            body["ip_addresses"] = [v for v in ip_addresses]
        if label is not None:
            body["label"] = label
        if list_type is not None:
            body["list_type"] = list_type.value
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PUT", f"/api/2.0/ip-access-lists/{ip_access_list_id}", body=body, headers=headers)

    def update(
        self,
        ip_access_list_id: str,
        *,
        enabled: Optional[bool] = None,
        ip_addresses: Optional[List[str]] = None,
        label: Optional[str] = None,
        list_type: Optional[ListType] = None,
    ):
        """Updates an existing IP access list, specified by its ID.

        A list can include allow lists and block lists. See the top of this file for a description of how the
        server treats allow lists and block lists at run time.

        When updating an IP access list:

        * For all allow lists and block lists combined, the API supports a maximum of 1000 IP/CIDR values,
        where one CIDR counts as a single value. Attempts to exceed that number return error 400 with
        `error_code` value `QUOTA_EXCEEDED`. * If the updated list would block the calling user's current IP,
        error 400 is returned with `error_code` value `INVALID_STATE`.

        It can take a few minutes for the changes to take effect. Note that your resulting IP access list has
        no effect until you enable the feature. See :method:workspaceconf/setStatus.

        :param ip_access_list_id: str
          The ID for the corresponding IP access list
        :param enabled: bool (optional)
          Specifies whether this IP access list is enabled.
        :param ip_addresses: List[str] (optional)
        :param label: str (optional)
          Label for the IP access list. This **cannot** be empty.
        :param list_type: :class:`ListType` (optional)


        """

        body = {}
        if enabled is not None:
            body["enabled"] = enabled
        if ip_addresses is not None:
            body["ip_addresses"] = [v for v in ip_addresses]
        if label is not None:
            body["label"] = label
        if list_type is not None:
            body["list_type"] = list_type.value
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PATCH", f"/api/2.0/ip-access-lists/{ip_access_list_id}", body=body, headers=headers)


class LlmProxyPartnerPoweredAccountAPI:
    """Determines if partner powered models are enabled or not for a specific account"""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, *, etag: Optional[str] = None) -> LlmProxyPartnerPoweredAccount:
        """Gets the enable partner powered AI features account setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`LlmProxyPartnerPoweredAccount`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/llm_proxy_partner_powered/names/default",
            query=query,
            headers=headers,
        )
        return LlmProxyPartnerPoweredAccount.from_dict(res)

    def update(
        self, allow_missing: bool, setting: LlmProxyPartnerPoweredAccount, field_mask: str
    ) -> LlmProxyPartnerPoweredAccount:
        """Updates the enable partner powered AI features account setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`LlmProxyPartnerPoweredAccount`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`LlmProxyPartnerPoweredAccount`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/llm_proxy_partner_powered/names/default",
            body=body,
            headers=headers,
        )
        return LlmProxyPartnerPoweredAccount.from_dict(res)


class LlmProxyPartnerPoweredEnforceAPI:
    """Determines if the account-level partner-powered setting value is enforced upon the workspace-level
    partner-powered setting"""

    def __init__(self, api_client):
        self._api = api_client

    def get(self, *, etag: Optional[str] = None) -> LlmProxyPartnerPoweredEnforce:
        """Gets the enforcement status of partner powered AI features account setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`LlmProxyPartnerPoweredEnforce`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/llm_proxy_partner_powered_enforce/names/default",
            query=query,
            headers=headers,
        )
        return LlmProxyPartnerPoweredEnforce.from_dict(res)

    def update(
        self, allow_missing: bool, setting: LlmProxyPartnerPoweredEnforce, field_mask: str
    ) -> LlmProxyPartnerPoweredEnforce:
        """Updates the enable enforcement status of partner powered AI features account setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`LlmProxyPartnerPoweredEnforce`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`LlmProxyPartnerPoweredEnforce`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/llm_proxy_partner_powered_enforce/names/default",
            body=body,
            headers=headers,
        )
        return LlmProxyPartnerPoweredEnforce.from_dict(res)


class LlmProxyPartnerPoweredWorkspaceAPI:
    """Determines if partner powered models are enabled or not for a specific workspace"""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteLlmProxyPartnerPoweredWorkspaceResponse:
        """Reverts the enable partner powered AI features workspace setting to its default value.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteLlmProxyPartnerPoweredWorkspaceResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE", "/api/2.0/settings/types/llm_proxy_partner_powered/names/default", query=query, headers=headers
        )
        return DeleteLlmProxyPartnerPoweredWorkspaceResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> LlmProxyPartnerPoweredWorkspace:
        """Gets the enable partner powered AI features workspace setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`LlmProxyPartnerPoweredWorkspace`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/llm_proxy_partner_powered/names/default", query=query, headers=headers
        )
        return LlmProxyPartnerPoweredWorkspace.from_dict(res)

    def update(
        self, allow_missing: bool, setting: LlmProxyPartnerPoweredWorkspace, field_mask: str
    ) -> LlmProxyPartnerPoweredWorkspace:
        """Updates the enable partner powered AI features workspace setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`LlmProxyPartnerPoweredWorkspace`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`LlmProxyPartnerPoweredWorkspace`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/llm_proxy_partner_powered/names/default", body=body, headers=headers
        )
        return LlmProxyPartnerPoweredWorkspace.from_dict(res)


class NetworkConnectivityAPI:
    """These APIs provide configurations for the network connectivity of your workspaces for serverless compute
    resources. This API provides stable subnets for your workspace so that you can configure your firewalls on
    your Azure Storage accounts to allow access from Databricks. You can also use the API to provision private
    endpoints for Databricks to privately connect serverless compute resources to your Azure resources using
    Azure Private Link. See [configure serverless secure connectivity].

    [configure serverless secure connectivity]: https://learn.microsoft.com/azure/databricks/security/network/serverless-network-security
    """

    def __init__(self, api_client):
        self._api = api_client

    def create_network_connectivity_configuration(
        self, network_connectivity_config: CreateNetworkConnectivityConfiguration
    ) -> NetworkConnectivityConfiguration:
        """Creates a network connectivity configuration (NCC), which provides stable Azure service subnets when
        accessing your Azure Storage accounts. You can also use a network connectivity configuration to create
        Databricks managed private endpoints so that Databricks serverless compute resources privately access
        your resources.

        **IMPORTANT**: After you create the network connectivity configuration, you must assign one or more
        workspaces to the new network connectivity configuration. You can share one network connectivity
        configuration with multiple workspaces from the same Azure region within the same Databricks account.
        See [configure serverless secure connectivity].

        [configure serverless secure connectivity]: https://learn.microsoft.com/azure/databricks/security/network/serverless-network-security

        :param network_connectivity_config: :class:`CreateNetworkConnectivityConfiguration`

        :returns: :class:`NetworkConnectivityConfiguration`
        """

        body = network_connectivity_config.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs", body=body, headers=headers
        )
        return NetworkConnectivityConfiguration.from_dict(res)

    def create_private_endpoint_rule(
        self, network_connectivity_config_id: str, private_endpoint_rule: CreatePrivateEndpointRule
    ) -> NccPrivateEndpointRule:
        """Create a private endpoint rule for the specified network connectivity config object. Once the object
        is created, Databricks asynchronously provisions a new Azure private endpoint to your specified Azure
        resource.

        **IMPORTANT**: You must use Azure portal or other Azure tools to approve the private endpoint to
        complete the connection. To get the information of the private endpoint created, make a `GET` request
        on the new private endpoint rule. See [serverless private link].

        [serverless private link]: https://learn.microsoft.com/azure/databricks/security/network/serverless-network-security/serverless-private-link

        :param network_connectivity_config_id: str
          Your Network Connectivity Configuration ID.
        :param private_endpoint_rule: :class:`CreatePrivateEndpointRule`

        :returns: :class:`NccPrivateEndpointRule`
        """

        body = private_endpoint_rule.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST",
            f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs/{network_connectivity_config_id}/private-endpoint-rules",
            body=body,
            headers=headers,
        )
        return NccPrivateEndpointRule.from_dict(res)

    def delete_network_connectivity_configuration(self, network_connectivity_config_id: str):
        """Deletes a network connectivity configuration.

        :param network_connectivity_config_id: str
          Your Network Connectivity Configuration ID.


        """

        headers = {
            "Accept": "application/json",
        }

        self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs/{network_connectivity_config_id}",
            headers=headers,
        )

    def delete_private_endpoint_rule(
        self, network_connectivity_config_id: str, private_endpoint_rule_id: str
    ) -> NccPrivateEndpointRule:
        """Initiates deleting a private endpoint rule. If the connection state is PENDING or EXPIRED, the private
        endpoint is immediately deleted. Otherwise, the private endpoint is deactivated and will be deleted
        after seven days of deactivation. When a private endpoint is deactivated, the `deactivated` field is
        set to `true` and the private endpoint is not available to your serverless compute resources.

        :param network_connectivity_config_id: str
          Your Network Connectvity Configuration ID.
        :param private_endpoint_rule_id: str
          Your private endpoint rule ID.

        :returns: :class:`NccPrivateEndpointRule`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs/{network_connectivity_config_id}/private-endpoint-rules/{private_endpoint_rule_id}",
            headers=headers,
        )
        return NccPrivateEndpointRule.from_dict(res)

    def get_network_connectivity_configuration(
        self, network_connectivity_config_id: str
    ) -> NetworkConnectivityConfiguration:
        """Gets a network connectivity configuration.

        :param network_connectivity_config_id: str
          Your Network Connectivity Configuration ID.

        :returns: :class:`NetworkConnectivityConfiguration`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs/{network_connectivity_config_id}",
            headers=headers,
        )
        return NetworkConnectivityConfiguration.from_dict(res)

    def get_private_endpoint_rule(
        self, network_connectivity_config_id: str, private_endpoint_rule_id: str
    ) -> NccPrivateEndpointRule:
        """Gets the private endpoint rule.

        :param network_connectivity_config_id: str
          Your Network Connectvity Configuration ID.
        :param private_endpoint_rule_id: str
          Your private endpoint rule ID.

        :returns: :class:`NccPrivateEndpointRule`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs/{network_connectivity_config_id}/private-endpoint-rules/{private_endpoint_rule_id}",
            headers=headers,
        )
        return NccPrivateEndpointRule.from_dict(res)

    def list_network_connectivity_configurations(
        self, *, page_token: Optional[str] = None
    ) -> Iterator[NetworkConnectivityConfiguration]:
        """Gets an array of network connectivity configurations.

        :param page_token: str (optional)
          Pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`NetworkConnectivityConfiguration`
        """

        query = {}
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        while True:
            json = self._api.do(
                "GET",
                f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs",
                query=query,
                headers=headers,
            )
            if "items" in json:
                for v in json["items"]:
                    yield NetworkConnectivityConfiguration.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_private_endpoint_rules(
        self, network_connectivity_config_id: str, *, page_token: Optional[str] = None
    ) -> Iterator[NccPrivateEndpointRule]:
        """Gets an array of private endpoint rules.

        :param network_connectivity_config_id: str
          Your Network Connectvity Configuration ID.
        :param page_token: str (optional)
          Pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`NccPrivateEndpointRule`
        """

        query = {}
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        while True:
            json = self._api.do(
                "GET",
                f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs/{network_connectivity_config_id}/private-endpoint-rules",
                query=query,
                headers=headers,
            )
            if "items" in json:
                for v in json["items"]:
                    yield NccPrivateEndpointRule.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_private_endpoint_rule(
        self,
        network_connectivity_config_id: str,
        private_endpoint_rule_id: str,
        private_endpoint_rule: UpdatePrivateEndpointRule,
        update_mask: str,
    ) -> NccPrivateEndpointRule:
        """Updates a private endpoint rule. Currently only a private endpoint rule to customer-managed resources
        is allowed to be updated.

        :param network_connectivity_config_id: str
          The ID of a network connectivity configuration, which is the parent resource of this private
          endpoint rule object.
        :param private_endpoint_rule_id: str
          Your private endpoint rule ID.
        :param private_endpoint_rule: :class:`UpdatePrivateEndpointRule`
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

        :returns: :class:`NccPrivateEndpointRule`
        """

        body = private_endpoint_rule.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/network-connectivity-configs/{network_connectivity_config_id}/private-endpoint-rules/{private_endpoint_rule_id}",
            query=query,
            body=body,
            headers=headers,
        )
        return NccPrivateEndpointRule.from_dict(res)


class NetworkPoliciesAPI:
    """These APIs manage network policies for this account. Network policies control which network destinations
    can be accessed from the Databricks environment. Each Databricks account includes a default policy named
    'default-policy'. 'default-policy' is associated with any workspace lacking an explicit network policy
    assignment, and is automatically associated with each newly created workspace. 'default-policy' is
    reserved and cannot be deleted, but it can be updated to customize the default network access rules for
    your account."""

    def __init__(self, api_client):
        self._api = api_client

    def create_network_policy_rpc(self, network_policy: AccountNetworkPolicy) -> AccountNetworkPolicy:
        """Creates a new network policy to manage which network destinations can be accessed from the Databricks
        environment.

        :param network_policy: :class:`AccountNetworkPolicy`
          Network policy configuration details.

        :returns: :class:`AccountNetworkPolicy`
        """

        body = network_policy.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "POST", f"/api/2.0/accounts/{self._api.account_id}/network-policies", body=body, headers=headers
        )
        return AccountNetworkPolicy.from_dict(res)

    def delete_network_policy_rpc(self, network_policy_id: str):
        """Deletes a network policy. Cannot be called on 'default-policy'.

        :param network_policy_id: str
          The unique identifier of the network policy to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        self._api.do(
            "DELETE", f"/api/2.0/accounts/{self._api.account_id}/network-policies/{network_policy_id}", headers=headers
        )

    def get_network_policy_rpc(self, network_policy_id: str) -> AccountNetworkPolicy:
        """Gets a network policy.

        :param network_policy_id: str
          The unique identifier of the network policy to retrieve.

        :returns: :class:`AccountNetworkPolicy`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/network-policies/{network_policy_id}", headers=headers
        )
        return AccountNetworkPolicy.from_dict(res)

    def list_network_policies_rpc(self, *, page_token: Optional[str] = None) -> Iterator[AccountNetworkPolicy]:
        """Gets an array of network policies.

        :param page_token: str (optional)
          Pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`AccountNetworkPolicy`
        """

        query = {}
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        while True:
            json = self._api.do(
                "GET", f"/api/2.0/accounts/{self._api.account_id}/network-policies", query=query, headers=headers
            )
            if "items" in json:
                for v in json["items"]:
                    yield AccountNetworkPolicy.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_network_policy_rpc(
        self, network_policy_id: str, network_policy: AccountNetworkPolicy
    ) -> AccountNetworkPolicy:
        """Updates a network policy. This allows you to modify the configuration of a network policy.

        :param network_policy_id: str
          The unique identifier for the network policy.
        :param network_policy: :class:`AccountNetworkPolicy`
          Updated network policy configuration details.

        :returns: :class:`AccountNetworkPolicy`
        """

        body = network_policy.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PUT",
            f"/api/2.0/accounts/{self._api.account_id}/network-policies/{network_policy_id}",
            body=body,
            headers=headers,
        )
        return AccountNetworkPolicy.from_dict(res)


class NotificationDestinationsAPI:
    """The notification destinations API lets you programmatically manage a workspace's notification
    destinations. Notification destinations are used to send notifications for query alerts and jobs to
    destinations outside of Databricks. Only workspace admins can create, update, and delete notification
    destinations."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, *, config: Optional[Config] = None, display_name: Optional[str] = None) -> NotificationDestination:
        """Creates a notification destination. Requires workspace admin permissions.

        :param config: :class:`Config` (optional)
          The configuration for the notification destination. Must wrap EXACTLY one of the nested configs.
        :param display_name: str (optional)
          The display name for the notification destination.

        :returns: :class:`NotificationDestination`
        """

        body = {}
        if config is not None:
            body["config"] = config.as_dict()
        if display_name is not None:
            body["display_name"] = display_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/notification-destinations", body=body, headers=headers)
        return NotificationDestination.from_dict(res)

    def delete(self, id: str):
        """Deletes a notification destination. Requires workspace admin permissions.

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/notification-destinations/{id}", headers=headers)

    def get(self, id: str) -> NotificationDestination:
        """Gets a notification destination.

        :param id: str

        :returns: :class:`NotificationDestination`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/notification-destinations/{id}", headers=headers)
        return NotificationDestination.from_dict(res)

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ListNotificationDestinationsResult]:
        """Lists notification destinations.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ListNotificationDestinationsResult`
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
            json = self._api.do("GET", "/api/2.0/notification-destinations", query=query, headers=headers)
            if "results" in json:
                for v in json["results"]:
                    yield ListNotificationDestinationsResult.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self, id: str, *, config: Optional[Config] = None, display_name: Optional[str] = None
    ) -> NotificationDestination:
        """Updates a notification destination. Requires workspace admin permissions. At least one field is
        required in the request body.

        :param id: str
          UUID identifying notification destination.
        :param config: :class:`Config` (optional)
          The configuration for the notification destination. Must wrap EXACTLY one of the nested configs.
        :param display_name: str (optional)
          The display name for the notification destination.

        :returns: :class:`NotificationDestination`
        """

        body = {}
        if config is not None:
            body["config"] = config.as_dict()
        if display_name is not None:
            body["display_name"] = display_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/notification-destinations/{id}", body=body, headers=headers)
        return NotificationDestination.from_dict(res)


class PersonalComputeAPI:
    """The Personal Compute enablement setting lets you control which users can use the Personal Compute default
    policy to create compute resources. By default all users in all workspaces have access (ON), but you can
    change the setting to instead let individual workspaces configure access control (DELEGATE).

    There is only one instance of this setting per account. Since this setting has a default value, this
    setting is present on all accounts even though it's never set on a given account. Deletion reverts the
    value of the setting back to the default value."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeletePersonalComputeSettingResponse:
        """Reverts back the Personal Compute setting value to default (ON)

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeletePersonalComputeSettingResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "DELETE",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/dcp_acct_enable/names/default",
            query=query,
            headers=headers,
        )
        return DeletePersonalComputeSettingResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> PersonalComputeSetting:
        """Gets the value of the Personal Compute setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`PersonalComputeSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/dcp_acct_enable/names/default",
            query=query,
            headers=headers,
        )
        return PersonalComputeSetting.from_dict(res)

    def update(self, allow_missing: bool, setting: PersonalComputeSetting, field_mask: str) -> PersonalComputeSetting:
        """Updates the value of the Personal Compute setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`PersonalComputeSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`PersonalComputeSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PATCH",
            f"/api/2.0/accounts/{self._api.account_id}/settings/types/dcp_acct_enable/names/default",
            body=body,
            headers=headers,
        )
        return PersonalComputeSetting.from_dict(res)


class RestrictWorkspaceAdminsAPI:
    """The Restrict Workspace Admins setting lets you control the capabilities of workspace admins. With the
    setting status set to ALLOW_ALL, workspace admins can create service principal personal access tokens on
    behalf of any service principal in their workspace. Workspace admins can also change a job owner to any
    user in their workspace. And they can change the job run_as setting to any user in their workspace or to a
    service principal on which they have the Service Principal User role. With the setting status set to
    RESTRICT_TOKENS_AND_JOB_RUN_AS, workspace admins can only create personal access tokens on behalf of
    service principals they have the Service Principal User role on. They can also only change a job owner to
    themselves. And they can change the job run_as setting to themselves or to a service principal on which
    they have the Service Principal User role."""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteRestrictWorkspaceAdminsSettingResponse:
        """Reverts the restrict workspace admins setting status for the workspace. A fresh etag needs to be
        provided in `DELETE` requests (as a query parameter). The etag can be retrieved by making a `GET`
        request before the DELETE request. If the setting is updated/deleted concurrently, `DELETE` fails with
        409 and the request must be retried by using the fresh etag in the 409 response.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteRestrictWorkspaceAdminsSettingResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE", "/api/2.0/settings/types/restrict_workspace_admins/names/default", query=query, headers=headers
        )
        return DeleteRestrictWorkspaceAdminsSettingResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> RestrictWorkspaceAdminsSetting:
        """Gets the restrict workspace admins setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`RestrictWorkspaceAdminsSetting`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/restrict_workspace_admins/names/default", query=query, headers=headers
        )
        return RestrictWorkspaceAdminsSetting.from_dict(res)

    def update(
        self, allow_missing: bool, setting: RestrictWorkspaceAdminsSetting, field_mask: str
    ) -> RestrictWorkspaceAdminsSetting:
        """Updates the restrict workspace admins setting for the workspace. A fresh etag needs to be provided in
        `PATCH` requests (as part of the setting field). The etag can be retrieved by making a GET request
        before the `PATCH` request. If the setting is updated concurrently, `PATCH` fails with 409 and the
        request must be retried by using the fresh etag in the 409 response.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`RestrictWorkspaceAdminsSetting`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`RestrictWorkspaceAdminsSetting`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/restrict_workspace_admins/names/default", body=body, headers=headers
        )
        return RestrictWorkspaceAdminsSetting.from_dict(res)


class SettingsAPI:
    """Workspace Settings API allows users to manage settings at the workspace level."""

    def __init__(self, api_client):
        self._api = api_client

        self._aibi_dashboard_embedding_access_policy = AibiDashboardEmbeddingAccessPolicyAPI(self._api)
        self._aibi_dashboard_embedding_approved_domains = AibiDashboardEmbeddingApprovedDomainsAPI(self._api)
        self._automatic_cluster_update = AutomaticClusterUpdateAPI(self._api)
        self._compliance_security_profile = ComplianceSecurityProfileAPI(self._api)
        self._dashboard_email_subscriptions = DashboardEmailSubscriptionsAPI(self._api)
        self._default_namespace = DefaultNamespaceAPI(self._api)
        self._default_warehouse_id = DefaultWarehouseIdAPI(self._api)
        self._disable_legacy_access = DisableLegacyAccessAPI(self._api)
        self._disable_legacy_dbfs = DisableLegacyDbfsAPI(self._api)
        self._enable_export_notebook = EnableExportNotebookAPI(self._api)
        self._enable_notebook_table_clipboard = EnableNotebookTableClipboardAPI(self._api)
        self._enable_results_downloading = EnableResultsDownloadingAPI(self._api)
        self._enhanced_security_monitoring = EnhancedSecurityMonitoringAPI(self._api)
        self._llm_proxy_partner_powered_workspace = LlmProxyPartnerPoweredWorkspaceAPI(self._api)
        self._restrict_workspace_admins = RestrictWorkspaceAdminsAPI(self._api)
        self._sql_results_download = SqlResultsDownloadAPI(self._api)

    @property
    def aibi_dashboard_embedding_access_policy(self) -> AibiDashboardEmbeddingAccessPolicyAPI:
        """Controls whether AI/BI published dashboard embedding is enabled, conditionally enabled, or disabled at the workspace level."""
        return self._aibi_dashboard_embedding_access_policy

    @property
    def aibi_dashboard_embedding_approved_domains(self) -> AibiDashboardEmbeddingApprovedDomainsAPI:
        """Controls the list of domains approved to host the embedded AI/BI dashboards."""
        return self._aibi_dashboard_embedding_approved_domains

    @property
    def automatic_cluster_update(self) -> AutomaticClusterUpdateAPI:
        """Controls whether automatic cluster update is enabled for the current workspace."""
        return self._automatic_cluster_update

    @property
    def compliance_security_profile(self) -> ComplianceSecurityProfileAPI:
        """Controls whether to enable the compliance security profile for the current workspace."""
        return self._compliance_security_profile

    @property
    def dashboard_email_subscriptions(self) -> DashboardEmailSubscriptionsAPI:
        """Controls whether schedules or workload tasks for refreshing AI/BI Dashboards in the workspace can send subscription emails containing PDFs and/or images of the dashboard."""
        return self._dashboard_email_subscriptions

    @property
    def default_namespace(self) -> DefaultNamespaceAPI:
        """The default namespace setting API allows users to configure the default namespace for a Databricks workspace."""
        return self._default_namespace

    @property
    def default_warehouse_id(self) -> DefaultWarehouseIdAPI:
        """Warehouse to be selected by default for users in this workspace."""
        return self._default_warehouse_id

    @property
    def disable_legacy_access(self) -> DisableLegacyAccessAPI:
        """'Disabling legacy access' has the following impacts: 1."""
        return self._disable_legacy_access

    @property
    def disable_legacy_dbfs(self) -> DisableLegacyDbfsAPI:
        """Disabling legacy DBFS has the following implications: 1."""
        return self._disable_legacy_dbfs

    @property
    def enable_export_notebook(self) -> EnableExportNotebookAPI:
        """Controls whether users can export notebooks and files from the Workspace UI."""
        return self._enable_export_notebook

    @property
    def enable_notebook_table_clipboard(self) -> EnableNotebookTableClipboardAPI:
        """Controls whether users can copy tabular data to the clipboard via the UI."""
        return self._enable_notebook_table_clipboard

    @property
    def enable_results_downloading(self) -> EnableResultsDownloadingAPI:
        """Controls whether users can download notebook results."""
        return self._enable_results_downloading

    @property
    def enhanced_security_monitoring(self) -> EnhancedSecurityMonitoringAPI:
        """Controls whether enhanced security monitoring is enabled for the current workspace."""
        return self._enhanced_security_monitoring

    @property
    def llm_proxy_partner_powered_workspace(self) -> LlmProxyPartnerPoweredWorkspaceAPI:
        """Determines if partner powered models are enabled or not for a specific workspace."""
        return self._llm_proxy_partner_powered_workspace

    @property
    def restrict_workspace_admins(self) -> RestrictWorkspaceAdminsAPI:
        """The Restrict Workspace Admins setting lets you control the capabilities of workspace admins."""
        return self._restrict_workspace_admins

    @property
    def sql_results_download(self) -> SqlResultsDownloadAPI:
        """Controls whether users within the workspace are allowed to download results from the SQL Editor and AI/BI Dashboards UIs."""
        return self._sql_results_download


class SqlResultsDownloadAPI:
    """Controls whether users within the workspace are allowed to download results from the SQL Editor and AI/BI
    Dashboards UIs. By default, this setting is enabled (set to `true`)"""

    def __init__(self, api_client):
        self._api = api_client

    def delete(self, *, etag: Optional[str] = None) -> DeleteSqlResultsDownloadResponse:
        """Reverts the SQL Results Download setting to its default value.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`DeleteSqlResultsDownloadResponse`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "DELETE", "/api/2.0/settings/types/sql_results_download/names/default", query=query, headers=headers
        )
        return DeleteSqlResultsDownloadResponse.from_dict(res)

    def get(self, *, etag: Optional[str] = None) -> SqlResultsDownload:
        """Gets the SQL Results Download setting.

        :param etag: str (optional)
          etag used for versioning. The response is at least as fresh as the eTag provided. This is used for
          optimistic concurrency control as a way to help prevent simultaneous writes of a setting overwriting
          each other. It is strongly suggested that systems make use of the etag in the read -> delete pattern
          to perform setting deletions in order to avoid race conditions. That is, get an etag from a GET
          request, and pass it with the DELETE request to identify the rule set version you are deleting.

        :returns: :class:`SqlResultsDownload`
        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", "/api/2.0/settings/types/sql_results_download/names/default", query=query, headers=headers
        )
        return SqlResultsDownload.from_dict(res)

    def update(self, allow_missing: bool, setting: SqlResultsDownload, field_mask: str) -> SqlResultsDownload:
        """Updates the SQL Results Download setting.

        :param allow_missing: bool
          This should always be set to true for Settings API. Added for AIP compliance.
        :param setting: :class:`SqlResultsDownload`
        :param field_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`SqlResultsDownload`
        """

        body = {}
        if allow_missing is not None:
            body["allow_missing"] = allow_missing
        if field_mask is not None:
            body["field_mask"] = field_mask
        if setting is not None:
            body["setting"] = setting.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", "/api/2.0/settings/types/sql_results_download/names/default", body=body, headers=headers
        )
        return SqlResultsDownload.from_dict(res)


class TokenManagementAPI:
    """Enables administrators to get all tokens and delete tokens for other users. Admins can either get every
    token, get a specific token by ID, or get all tokens for a particular user."""

    def __init__(self, api_client):
        self._api = api_client

    def create_obo_token(
        self, application_id: str, *, comment: Optional[str] = None, lifetime_seconds: Optional[int] = None
    ) -> CreateOboTokenResponse:
        """Creates a token on behalf of a service principal.

        :param application_id: str
          Application ID of the service principal.
        :param comment: str (optional)
          Comment that describes the purpose of the token.
        :param lifetime_seconds: int (optional)
          The number of seconds before the token expires.

        :returns: :class:`CreateOboTokenResponse`
        """

        body = {}
        if application_id is not None:
            body["application_id"] = application_id
        if comment is not None:
            body["comment"] = comment
        if lifetime_seconds is not None:
            body["lifetime_seconds"] = lifetime_seconds
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/token-management/on-behalf-of/tokens", body=body, headers=headers)
        return CreateOboTokenResponse.from_dict(res)

    def delete(self, token_id: str):
        """Deletes a token, specified by its ID.

        :param token_id: str
          The ID of the token to revoke.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/token-management/tokens/{token_id}", headers=headers)

    def get(self, token_id: str) -> GetTokenResponse:
        """Gets information about a token, specified by its ID.

        :param token_id: str
          The ID of the token to get.

        :returns: :class:`GetTokenResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/token-management/tokens/{token_id}", headers=headers)
        return GetTokenResponse.from_dict(res)

    def get_permission_levels(self) -> GetTokenPermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.


        :returns: :class:`GetTokenPermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/permissions/authorization/tokens/permissionLevels", headers=headers)
        return GetTokenPermissionLevelsResponse.from_dict(res)

    def get_permissions(self) -> TokenPermissions:
        """Gets the permissions of all tokens. Tokens can inherit permissions from their root object.


        :returns: :class:`TokenPermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/permissions/authorization/tokens", headers=headers)
        return TokenPermissions.from_dict(res)

    def list(
        self, *, created_by_id: Optional[int] = None, created_by_username: Optional[str] = None
    ) -> Iterator[TokenInfo]:
        """Lists all tokens associated with the specified workspace or user.

        :param created_by_id: int (optional)
          User ID of the user that created the token.
        :param created_by_username: str (optional)
          Username of the user that created the token.

        :returns: Iterator over :class:`TokenInfo`
        """

        query = {}
        if created_by_id is not None:
            query["created_by_id"] = created_by_id
        if created_by_username is not None:
            query["created_by_username"] = created_by_username
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/token-management/tokens", query=query, headers=headers)
        parsed = ListTokensResponse.from_dict(json).token_infos
        return parsed if parsed is not None else []

    def set_permissions(
        self, *, access_control_list: Optional[List[TokenAccessControlRequest]] = None
    ) -> TokenPermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param access_control_list: List[:class:`TokenAccessControlRequest`] (optional)

        :returns: :class:`TokenPermissions`
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

        res = self._api.do("PUT", "/api/2.0/permissions/authorization/tokens", body=body, headers=headers)
        return TokenPermissions.from_dict(res)

    def update_permissions(
        self, *, access_control_list: Optional[List[TokenAccessControlRequest]] = None
    ) -> TokenPermissions:
        """Updates the permissions on all tokens. Tokens can inherit permissions from their root object.

        :param access_control_list: List[:class:`TokenAccessControlRequest`] (optional)

        :returns: :class:`TokenPermissions`
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

        res = self._api.do("PATCH", "/api/2.0/permissions/authorization/tokens", body=body, headers=headers)
        return TokenPermissions.from_dict(res)


class TokensAPI:
    """The Token API allows you to create, list, and revoke tokens that can be used to authenticate and access
    Databricks REST APIs."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, *, comment: Optional[str] = None, lifetime_seconds: Optional[int] = None) -> CreateTokenResponse:
        """Creates and returns a token for a user. If this call is made through token authentication, it creates
        a token with the same client ID as the authenticated token. If the user's token quota is exceeded,
        this call returns an error **QUOTA_EXCEEDED**.

        :param comment: str (optional)
          Optional description to attach to the token.
        :param lifetime_seconds: int (optional)
          The lifetime of the token, in seconds.

          If the lifetime is not specified, this token remains valid for 2 years.

        :returns: :class:`CreateTokenResponse`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if lifetime_seconds is not None:
            body["lifetime_seconds"] = lifetime_seconds
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/token/create", body=body, headers=headers)
        return CreateTokenResponse.from_dict(res)

    def delete(self, token_id: str):
        """Revokes an access token.

        If a token with the specified ID is not valid, this call returns an error **RESOURCE_DOES_NOT_EXIST**.

        :param token_id: str
          The ID of the token to be revoked.


        """

        body = {}
        if token_id is not None:
            body["token_id"] = token_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/token/delete", body=body, headers=headers)

    def list(self) -> Iterator[PublicTokenInfo]:
        """Lists all the valid tokens for a user-workspace pair.


        :returns: Iterator over :class:`PublicTokenInfo`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/token/list", headers=headers)
        parsed = ListPublicTokensResponse.from_dict(json).token_infos
        return parsed if parsed is not None else []


class WorkspaceConfAPI:
    """This API allows updating known workspace settings for advanced users."""

    def __init__(self, api_client):
        self._api = api_client

    def get_status(self, keys: str) -> WorkspaceConf:
        """Gets the configuration status for a workspace.

        :param keys: str

        :returns: Dict[str,str]
        """

        query = {}
        if keys is not None:
            query["keys"] = keys
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/workspace-conf", query=query, headers=headers)
        return res

    def set_status(self, contents: Dict[str, str]):
        """Sets the configuration status for a workspace, including enabling or disabling it."""

        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PATCH", "/api/2.0/workspace-conf", body=contents, headers=headers)


class WorkspaceNetworkConfigurationAPI:
    """These APIs allow configuration of network settings for Databricks workspaces by selecting which network
    policy to associate with the workspace. Each workspace is always associated with exactly one network
    policy that controls which network destinations can be accessed from the Databricks environment. By
    default, workspaces are associated with the 'default-policy' network policy. You cannot create or delete a
    workspace's network option, only update it to associate the workspace with a different policy"""

    def __init__(self, api_client):
        self._api = api_client

    def get_workspace_network_option_rpc(self, workspace_id: int) -> WorkspaceNetworkOption:
        """Gets the network option for a workspace. Every workspace has exactly one network policy binding, with
        'default-policy' used if no explicit assignment exists.

        :param workspace_id: int
          The workspace ID.

        :returns: :class:`WorkspaceNetworkOption`
        """

        headers = {
            "Accept": "application/json",
        }

        res = self._api.do(
            "GET", f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}/network", headers=headers
        )
        return WorkspaceNetworkOption.from_dict(res)

    def update_workspace_network_option_rpc(
        self, workspace_id: int, workspace_network_option: WorkspaceNetworkOption
    ) -> WorkspaceNetworkOption:
        """Updates the network option for a workspace. This operation associates the workspace with the specified
        network policy. To revert to the default policy, specify 'default-policy' as the network_policy_id.

        :param workspace_id: int
          The workspace ID.
        :param workspace_network_option: :class:`WorkspaceNetworkOption`
          The network option details for the workspace.

        :returns: :class:`WorkspaceNetworkOption`
        """

        body = workspace_network_option.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        res = self._api.do(
            "PUT",
            f"/api/2.0/accounts/{self._api.account_id}/workspaces/{workspace_id}/network",
            body=body,
            headers=headers,
        )
        return WorkspaceNetworkOption.from_dict(res)
