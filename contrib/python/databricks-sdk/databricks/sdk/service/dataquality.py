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


class AggregationGranularity(Enum):
    """The granularity for aggregating data into time windows based on their timestamp."""

    AGGREGATION_GRANULARITY_1_DAY = "AGGREGATION_GRANULARITY_1_DAY"
    AGGREGATION_GRANULARITY_1_HOUR = "AGGREGATION_GRANULARITY_1_HOUR"
    AGGREGATION_GRANULARITY_1_MONTH = "AGGREGATION_GRANULARITY_1_MONTH"
    AGGREGATION_GRANULARITY_1_WEEK = "AGGREGATION_GRANULARITY_1_WEEK"
    AGGREGATION_GRANULARITY_1_YEAR = "AGGREGATION_GRANULARITY_1_YEAR"
    AGGREGATION_GRANULARITY_2_WEEKS = "AGGREGATION_GRANULARITY_2_WEEKS"
    AGGREGATION_GRANULARITY_30_MINUTES = "AGGREGATION_GRANULARITY_30_MINUTES"
    AGGREGATION_GRANULARITY_3_WEEKS = "AGGREGATION_GRANULARITY_3_WEEKS"
    AGGREGATION_GRANULARITY_4_WEEKS = "AGGREGATION_GRANULARITY_4_WEEKS"
    AGGREGATION_GRANULARITY_5_MINUTES = "AGGREGATION_GRANULARITY_5_MINUTES"


@dataclass
class AnomalyDetectionConfig:
    """Anomaly Detection Configurations."""

    excluded_table_full_names: Optional[List[str]] = None
    """List of fully qualified table names to exclude from anomaly detection."""

    def as_dict(self) -> dict:
        """Serializes the AnomalyDetectionConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.excluded_table_full_names:
            body["excluded_table_full_names"] = [v for v in self.excluded_table_full_names]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AnomalyDetectionConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.excluded_table_full_names:
            body["excluded_table_full_names"] = self.excluded_table_full_names
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AnomalyDetectionConfig:
        """Deserializes the AnomalyDetectionConfig from a dictionary."""
        return cls(excluded_table_full_names=d.get("excluded_table_full_names", None))


@dataclass
class CancelRefreshResponse:
    """Response to cancelling a refresh."""

    refresh: Optional[Refresh] = None
    """The refresh to cancel."""

    def as_dict(self) -> dict:
        """Serializes the CancelRefreshResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.refresh:
            body["refresh"] = self.refresh.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CancelRefreshResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.refresh:
            body["refresh"] = self.refresh
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CancelRefreshResponse:
        """Deserializes the CancelRefreshResponse from a dictionary."""
        return cls(refresh=_from_dict(d, "refresh", Refresh))


@dataclass
class CronSchedule:
    """The data quality monitoring workflow cron schedule."""

    quartz_cron_expression: str
    """The expression that determines when to run the monitor. See [examples].
    
    [examples]: https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html"""

    timezone_id: str
    """A Java timezone id. The schedule for a job will be resolved with respect to this timezone. See
    `Java TimeZone <http://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html>`_ for details.
    The timezone id (e.g., ``America/Los_Angeles``) in which to evaluate the quartz expression."""

    pause_status: Optional[CronSchedulePauseStatus] = None
    """Read only field that indicates whether the schedule is paused or not."""

    def as_dict(self) -> dict:
        """Serializes the CronSchedule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status.value
        if self.quartz_cron_expression is not None:
            body["quartz_cron_expression"] = self.quartz_cron_expression
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CronSchedule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status
        if self.quartz_cron_expression is not None:
            body["quartz_cron_expression"] = self.quartz_cron_expression
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CronSchedule:
        """Deserializes the CronSchedule from a dictionary."""
        return cls(
            pause_status=_enum(d, "pause_status", CronSchedulePauseStatus),
            quartz_cron_expression=d.get("quartz_cron_expression", None),
            timezone_id=d.get("timezone_id", None),
        )


class CronSchedulePauseStatus(Enum):
    """The data quality monitoring workflow cron schedule pause status."""

    CRON_SCHEDULE_PAUSE_STATUS_PAUSED = "CRON_SCHEDULE_PAUSE_STATUS_PAUSED"
    CRON_SCHEDULE_PAUSE_STATUS_UNPAUSED = "CRON_SCHEDULE_PAUSE_STATUS_UNPAUSED"


@dataclass
class DataProfilingConfig:
    """Data Profiling Configurations."""

    output_schema_id: str
    """ID of the schema where output tables are created."""

    assets_dir: Optional[str] = None
    """Field for specifying the absolute path to a custom directory to store data-monitoring assets.
    Normally prepopulated to a default user location via UI and Python APIs."""

    baseline_table_name: Optional[str] = None
    """Baseline table name. Baseline data is used to compute drift from the data in the monitored
    `table_name`. The baseline table and the monitored table shall have the same schema."""

    custom_metrics: Optional[List[DataProfilingCustomMetric]] = None
    """Custom metrics."""

    dashboard_id: Optional[str] = None
    """Id of dashboard that visualizes the computed metrics. This can be empty if the monitor is in
    PENDING state."""

    drift_metrics_table_name: Optional[str] = None
    """Table that stores drift metrics data. Format: `catalog.schema.table_name`."""

    effective_warehouse_id: Optional[str] = None
    """The warehouse for dashboard creation"""

    inference_log: Optional[InferenceLogConfig] = None
    """`Analysis Configuration` for monitoring inference log tables."""

    latest_monitor_failure_message: Optional[str] = None
    """The latest error message for a monitor failure."""

    monitor_version: Optional[int] = None
    """Represents the current monitor configuration version in use. The version will be represented in
    a numeric fashion (1,2,3...). The field has flexibility to take on negative values, which can
    indicate corrupted monitor_version numbers."""

    monitored_table_name: Optional[str] = None
    """Unity Catalog table to monitor. Format: `catalog.schema.table_name`"""

    notification_settings: Optional[NotificationSettings] = None
    """Field for specifying notification settings."""

    profile_metrics_table_name: Optional[str] = None
    """Table that stores profile metrics data. Format: `catalog.schema.table_name`."""

    schedule: Optional[CronSchedule] = None
    """The cron schedule."""

    skip_builtin_dashboard: Optional[bool] = None
    """Whether to skip creating a default dashboard summarizing data quality metrics."""

    slicing_exprs: Optional[List[str]] = None
    """List of column expressions to slice data with for targeted analysis. The data is grouped by each
    expression independently, resulting in a separate slice for each predicate and its complements.
    For example `slicing_exprs=[“col_1”, “col_2 > 10”]` will generate the following slices:
    two slices for `col_2 > 10` (True and False), and one slice per unique value in `col1`. For
    high-cardinality columns, only the top 100 unique values by frequency will generate slices."""

    snapshot: Optional[SnapshotConfig] = None
    """`Analysis Configuration` for monitoring snapshot tables."""

    status: Optional[DataProfilingStatus] = None
    """The data profiling monitor status."""

    time_series: Optional[TimeSeriesConfig] = None
    """`Analysis Configuration` for monitoring time series tables."""

    warehouse_id: Optional[str] = None
    """Optional argument to specify the warehouse for dashboard creation. If not specified, the first
    running warehouse will be used."""

    def as_dict(self) -> dict:
        """Serializes the DataProfilingConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.assets_dir is not None:
            body["assets_dir"] = self.assets_dir
        if self.baseline_table_name is not None:
            body["baseline_table_name"] = self.baseline_table_name
        if self.custom_metrics:
            body["custom_metrics"] = [v.as_dict() for v in self.custom_metrics]
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.drift_metrics_table_name is not None:
            body["drift_metrics_table_name"] = self.drift_metrics_table_name
        if self.effective_warehouse_id is not None:
            body["effective_warehouse_id"] = self.effective_warehouse_id
        if self.inference_log:
            body["inference_log"] = self.inference_log.as_dict()
        if self.latest_monitor_failure_message is not None:
            body["latest_monitor_failure_message"] = self.latest_monitor_failure_message
        if self.monitor_version is not None:
            body["monitor_version"] = self.monitor_version
        if self.monitored_table_name is not None:
            body["monitored_table_name"] = self.monitored_table_name
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings.as_dict()
        if self.output_schema_id is not None:
            body["output_schema_id"] = self.output_schema_id
        if self.profile_metrics_table_name is not None:
            body["profile_metrics_table_name"] = self.profile_metrics_table_name
        if self.schedule:
            body["schedule"] = self.schedule.as_dict()
        if self.skip_builtin_dashboard is not None:
            body["skip_builtin_dashboard"] = self.skip_builtin_dashboard
        if self.slicing_exprs:
            body["slicing_exprs"] = [v for v in self.slicing_exprs]
        if self.snapshot:
            body["snapshot"] = self.snapshot.as_dict()
        if self.status is not None:
            body["status"] = self.status.value
        if self.time_series:
            body["time_series"] = self.time_series.as_dict()
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DataProfilingConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.assets_dir is not None:
            body["assets_dir"] = self.assets_dir
        if self.baseline_table_name is not None:
            body["baseline_table_name"] = self.baseline_table_name
        if self.custom_metrics:
            body["custom_metrics"] = self.custom_metrics
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.drift_metrics_table_name is not None:
            body["drift_metrics_table_name"] = self.drift_metrics_table_name
        if self.effective_warehouse_id is not None:
            body["effective_warehouse_id"] = self.effective_warehouse_id
        if self.inference_log:
            body["inference_log"] = self.inference_log
        if self.latest_monitor_failure_message is not None:
            body["latest_monitor_failure_message"] = self.latest_monitor_failure_message
        if self.monitor_version is not None:
            body["monitor_version"] = self.monitor_version
        if self.monitored_table_name is not None:
            body["monitored_table_name"] = self.monitored_table_name
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings
        if self.output_schema_id is not None:
            body["output_schema_id"] = self.output_schema_id
        if self.profile_metrics_table_name is not None:
            body["profile_metrics_table_name"] = self.profile_metrics_table_name
        if self.schedule:
            body["schedule"] = self.schedule
        if self.skip_builtin_dashboard is not None:
            body["skip_builtin_dashboard"] = self.skip_builtin_dashboard
        if self.slicing_exprs:
            body["slicing_exprs"] = self.slicing_exprs
        if self.snapshot:
            body["snapshot"] = self.snapshot
        if self.status is not None:
            body["status"] = self.status
        if self.time_series:
            body["time_series"] = self.time_series
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DataProfilingConfig:
        """Deserializes the DataProfilingConfig from a dictionary."""
        return cls(
            assets_dir=d.get("assets_dir", None),
            baseline_table_name=d.get("baseline_table_name", None),
            custom_metrics=_repeated_dict(d, "custom_metrics", DataProfilingCustomMetric),
            dashboard_id=d.get("dashboard_id", None),
            drift_metrics_table_name=d.get("drift_metrics_table_name", None),
            effective_warehouse_id=d.get("effective_warehouse_id", None),
            inference_log=_from_dict(d, "inference_log", InferenceLogConfig),
            latest_monitor_failure_message=d.get("latest_monitor_failure_message", None),
            monitor_version=d.get("monitor_version", None),
            monitored_table_name=d.get("monitored_table_name", None),
            notification_settings=_from_dict(d, "notification_settings", NotificationSettings),
            output_schema_id=d.get("output_schema_id", None),
            profile_metrics_table_name=d.get("profile_metrics_table_name", None),
            schedule=_from_dict(d, "schedule", CronSchedule),
            skip_builtin_dashboard=d.get("skip_builtin_dashboard", None),
            slicing_exprs=d.get("slicing_exprs", None),
            snapshot=_from_dict(d, "snapshot", SnapshotConfig),
            status=_enum(d, "status", DataProfilingStatus),
            time_series=_from_dict(d, "time_series", TimeSeriesConfig),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class DataProfilingCustomMetric:
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

    type: DataProfilingCustomMetricType
    """The type of the custom metric."""

    def as_dict(self) -> dict:
        """Serializes the DataProfilingCustomMetric into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the DataProfilingCustomMetric into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> DataProfilingCustomMetric:
        """Deserializes the DataProfilingCustomMetric from a dictionary."""
        return cls(
            definition=d.get("definition", None),
            input_columns=d.get("input_columns", None),
            name=d.get("name", None),
            output_data_type=d.get("output_data_type", None),
            type=_enum(d, "type", DataProfilingCustomMetricType),
        )


class DataProfilingCustomMetricType(Enum):
    """The custom metric type."""

    DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE = "DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE"
    DATA_PROFILING_CUSTOM_METRIC_TYPE_DERIVED = "DATA_PROFILING_CUSTOM_METRIC_TYPE_DERIVED"
    DATA_PROFILING_CUSTOM_METRIC_TYPE_DRIFT = "DATA_PROFILING_CUSTOM_METRIC_TYPE_DRIFT"


class DataProfilingStatus(Enum):
    """The status of the data profiling monitor."""

    DATA_PROFILING_STATUS_ACTIVE = "DATA_PROFILING_STATUS_ACTIVE"
    DATA_PROFILING_STATUS_DELETE_PENDING = "DATA_PROFILING_STATUS_DELETE_PENDING"
    DATA_PROFILING_STATUS_ERROR = "DATA_PROFILING_STATUS_ERROR"
    DATA_PROFILING_STATUS_FAILED = "DATA_PROFILING_STATUS_FAILED"
    DATA_PROFILING_STATUS_PENDING = "DATA_PROFILING_STATUS_PENDING"


@dataclass
class InferenceLogConfig:
    """Inference log configuration."""

    problem_type: InferenceProblemType
    """Problem type the model aims to solve."""

    timestamp_column: str
    """Column for the timestamp."""

    granularities: List[AggregationGranularity]
    """List of granularities to use when aggregating data into time windows based on their timestamp."""

    prediction_column: str
    """Column for the prediction."""

    model_id_column: str
    """Column for the model identifier."""

    label_column: Optional[str] = None
    """Column for the label."""

    def as_dict(self) -> dict:
        """Serializes the InferenceLogConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.granularities:
            body["granularities"] = [v.value for v in self.granularities]
        if self.label_column is not None:
            body["label_column"] = self.label_column
        if self.model_id_column is not None:
            body["model_id_column"] = self.model_id_column
        if self.prediction_column is not None:
            body["prediction_column"] = self.prediction_column
        if self.problem_type is not None:
            body["problem_type"] = self.problem_type.value
        if self.timestamp_column is not None:
            body["timestamp_column"] = self.timestamp_column
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the InferenceLogConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.granularities:
            body["granularities"] = self.granularities
        if self.label_column is not None:
            body["label_column"] = self.label_column
        if self.model_id_column is not None:
            body["model_id_column"] = self.model_id_column
        if self.prediction_column is not None:
            body["prediction_column"] = self.prediction_column
        if self.problem_type is not None:
            body["problem_type"] = self.problem_type
        if self.timestamp_column is not None:
            body["timestamp_column"] = self.timestamp_column
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> InferenceLogConfig:
        """Deserializes the InferenceLogConfig from a dictionary."""
        return cls(
            granularities=_repeated_enum(d, "granularities", AggregationGranularity),
            label_column=d.get("label_column", None),
            model_id_column=d.get("model_id_column", None),
            prediction_column=d.get("prediction_column", None),
            problem_type=_enum(d, "problem_type", InferenceProblemType),
            timestamp_column=d.get("timestamp_column", None),
        )


class InferenceProblemType(Enum):
    """Inference problem type the model aims to solve."""

    INFERENCE_PROBLEM_TYPE_CLASSIFICATION = "INFERENCE_PROBLEM_TYPE_CLASSIFICATION"
    INFERENCE_PROBLEM_TYPE_REGRESSION = "INFERENCE_PROBLEM_TYPE_REGRESSION"


@dataclass
class ListMonitorResponse:
    """Response for listing Monitors."""

    monitors: Optional[List[Monitor]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListMonitorResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.monitors:
            body["monitors"] = [v.as_dict() for v in self.monitors]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListMonitorResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.monitors:
            body["monitors"] = self.monitors
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListMonitorResponse:
        """Deserializes the ListMonitorResponse from a dictionary."""
        return cls(monitors=_repeated_dict(d, "monitors", Monitor), next_page_token=d.get("next_page_token", None))


@dataclass
class ListRefreshResponse:
    """Response for listing refreshes."""

    next_page_token: Optional[str] = None

    refreshes: Optional[List[Refresh]] = None

    def as_dict(self) -> dict:
        """Serializes the ListRefreshResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.refreshes:
            body["refreshes"] = [v.as_dict() for v in self.refreshes]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListRefreshResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.refreshes:
            body["refreshes"] = self.refreshes
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListRefreshResponse:
        """Deserializes the ListRefreshResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), refreshes=_repeated_dict(d, "refreshes", Refresh))


@dataclass
class Monitor:
    """Monitor for the data quality of unity catalog entities such as schema or table."""

    object_type: str
    """The type of the monitored object. Can be one of the following: `schema` or `table`."""

    object_id: str
    """The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.
    
    Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
    Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.
    
    Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
    Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.
    
    [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
    [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
    [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id"""

    anomaly_detection_config: Optional[AnomalyDetectionConfig] = None
    """Anomaly Detection Configuration, applicable to `schema` object types."""

    data_profiling_config: Optional[DataProfilingConfig] = None
    """Data Profiling Configuration, applicable to `table` object types. Exactly one `Analysis
    Configuration` must be present."""

    def as_dict(self) -> dict:
        """Serializes the Monitor into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.anomaly_detection_config:
            body["anomaly_detection_config"] = self.anomaly_detection_config.as_dict()
        if self.data_profiling_config:
            body["data_profiling_config"] = self.data_profiling_config.as_dict()
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Monitor into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.anomaly_detection_config:
            body["anomaly_detection_config"] = self.anomaly_detection_config
        if self.data_profiling_config:
            body["data_profiling_config"] = self.data_profiling_config
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Monitor:
        """Deserializes the Monitor from a dictionary."""
        return cls(
            anomaly_detection_config=_from_dict(d, "anomaly_detection_config", AnomalyDetectionConfig),
            data_profiling_config=_from_dict(d, "data_profiling_config", DataProfilingConfig),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class NotificationDestination:
    """Destination of the data quality monitoring notification."""

    email_addresses: Optional[List[str]] = None
    """The list of email addresses to send the notification to. A maximum of 5 email addresses is
    supported."""

    def as_dict(self) -> dict:
        """Serializes the NotificationDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.email_addresses:
            body["email_addresses"] = [v for v in self.email_addresses]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotificationDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.email_addresses:
            body["email_addresses"] = self.email_addresses
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotificationDestination:
        """Deserializes the NotificationDestination from a dictionary."""
        return cls(email_addresses=d.get("email_addresses", None))


@dataclass
class NotificationSettings:
    """Settings for sending notifications on the data quality monitoring."""

    on_failure: Optional[NotificationDestination] = None
    """Destinations to send notifications on failure/timeout."""

    def as_dict(self) -> dict:
        """Serializes the NotificationSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.on_failure:
            body["on_failure"] = self.on_failure.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotificationSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.on_failure:
            body["on_failure"] = self.on_failure
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotificationSettings:
        """Deserializes the NotificationSettings from a dictionary."""
        return cls(on_failure=_from_dict(d, "on_failure", NotificationDestination))


@dataclass
class Refresh:
    """The Refresh object gives information on a refresh of the data quality monitoring pipeline."""

    object_type: str
    """The type of the monitored object. Can be one of the following: `schema`or `table`."""

    object_id: str
    """The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.
    
    Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
    Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.
    
    Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
    Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.
    
    [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
    [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
    [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id"""

    end_time_ms: Optional[int] = None
    """Time when the refresh ended (milliseconds since 1/1/1970 UTC)."""

    message: Optional[str] = None
    """An optional message to give insight into the current state of the refresh (e.g. FAILURE
    messages)."""

    refresh_id: Optional[int] = None
    """Unique id of the refresh operation."""

    start_time_ms: Optional[int] = None
    """Time when the refresh started (milliseconds since 1/1/1970 UTC)."""

    state: Optional[RefreshState] = None
    """The current state of the refresh."""

    trigger: Optional[RefreshTrigger] = None
    """What triggered the refresh."""

    def as_dict(self) -> dict:
        """Serializes the Refresh into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.end_time_ms is not None:
            body["end_time_ms"] = self.end_time_ms
        if self.message is not None:
            body["message"] = self.message
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
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
        """Serializes the Refresh into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.end_time_ms is not None:
            body["end_time_ms"] = self.end_time_ms
        if self.message is not None:
            body["message"] = self.message
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
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
    def from_dict(cls, d: Dict[str, Any]) -> Refresh:
        """Deserializes the Refresh from a dictionary."""
        return cls(
            end_time_ms=d.get("end_time_ms", None),
            message=d.get("message", None),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
            refresh_id=d.get("refresh_id", None),
            start_time_ms=d.get("start_time_ms", None),
            state=_enum(d, "state", RefreshState),
            trigger=_enum(d, "trigger", RefreshTrigger),
        )


class RefreshState(Enum):
    """The state of the refresh."""

    MONITOR_REFRESH_STATE_CANCELED = "MONITOR_REFRESH_STATE_CANCELED"
    MONITOR_REFRESH_STATE_FAILED = "MONITOR_REFRESH_STATE_FAILED"
    MONITOR_REFRESH_STATE_PENDING = "MONITOR_REFRESH_STATE_PENDING"
    MONITOR_REFRESH_STATE_RUNNING = "MONITOR_REFRESH_STATE_RUNNING"
    MONITOR_REFRESH_STATE_SUCCESS = "MONITOR_REFRESH_STATE_SUCCESS"
    MONITOR_REFRESH_STATE_UNKNOWN = "MONITOR_REFRESH_STATE_UNKNOWN"


class RefreshTrigger(Enum):
    """The trigger of the refresh."""

    MONITOR_REFRESH_TRIGGER_DATA_CHANGE = "MONITOR_REFRESH_TRIGGER_DATA_CHANGE"
    MONITOR_REFRESH_TRIGGER_MANUAL = "MONITOR_REFRESH_TRIGGER_MANUAL"
    MONITOR_REFRESH_TRIGGER_SCHEDULE = "MONITOR_REFRESH_TRIGGER_SCHEDULE"
    MONITOR_REFRESH_TRIGGER_UNKNOWN = "MONITOR_REFRESH_TRIGGER_UNKNOWN"


@dataclass
class SnapshotConfig:
    """Snapshot analysis configuration."""

    def as_dict(self) -> dict:
        """Serializes the SnapshotConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SnapshotConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SnapshotConfig:
        """Deserializes the SnapshotConfig from a dictionary."""
        return cls()


@dataclass
class TimeSeriesConfig:
    """Time series analysis configuration."""

    timestamp_column: str
    """Column for the timestamp."""

    granularities: List[AggregationGranularity]
    """List of granularities to use when aggregating data into time windows based on their timestamp."""

    def as_dict(self) -> dict:
        """Serializes the TimeSeriesConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.granularities:
            body["granularities"] = [v.value for v in self.granularities]
        if self.timestamp_column is not None:
            body["timestamp_column"] = self.timestamp_column
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TimeSeriesConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.granularities:
            body["granularities"] = self.granularities
        if self.timestamp_column is not None:
            body["timestamp_column"] = self.timestamp_column
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TimeSeriesConfig:
        """Deserializes the TimeSeriesConfig from a dictionary."""
        return cls(
            granularities=_repeated_enum(d, "granularities", AggregationGranularity),
            timestamp_column=d.get("timestamp_column", None),
        )


class DataQualityAPI:
    """Manage the data quality of Unity Catalog objects (currently support `schema` and `table`)"""

    def __init__(self, api_client):
        self._api = api_client

    def cancel_refresh(self, object_type: str, object_id: str, refresh_id: int) -> CancelRefreshResponse:
        """Cancels a data quality monitor refresh. Currently only supported for the `table` `object_type`. The
        call must be made in the same workspace as where the monitor was created.

        The caller must have either of the following sets of permissions: 1. **MANAGE** and **USE_CATALOG** on
        the table's parent catalog. 2. **USE_CATALOG** on the table's parent catalog, and **MANAGE** and
        **USE_SCHEMA** on the table's parent schema. 3. **USE_CATALOG** on the table's parent catalog,
        **USE_SCHEMA** on the table's parent schema, and **MANAGE** on the table.

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id
        :param refresh_id: int
          Unique id of the refresh operation.

        :returns: :class:`CancelRefreshResponse`
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/data-quality/v1/monitors/{object_type}/{object_id}/refreshes/{refresh_id}/cancel",
            headers=headers,
        )
        return CancelRefreshResponse.from_dict(res)

    def create_monitor(self, monitor: Monitor) -> Monitor:
        """Create a data quality monitor on a Unity Catalog object. The caller must provide either
        `anomaly_detection_config` for a schema monitor or `data_profiling_config` for a table monitor.

        For the `table` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the table's parent catalog, **USE_SCHEMA** on the table's parent
        schema, and **SELECT** on the table 2. **USE_CATALOG** on the table's parent catalog, **MANAGE** and
        **USE_SCHEMA** on the table's parent schema, and **SELECT** on the table. 3. **USE_CATALOG** on the
        table's parent catalog, **USE_SCHEMA** on the table's parent schema, and **MANAGE** and **SELECT** on
        the table.

        Workspace assets, such as the dashboard, will be created in the workspace where this call was made.

        For the `schema` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the schema's parent catalog. 2. **USE_CATALOG** on the schema's
        parent catalog, and **MANAGE** and **USE_SCHEMA** on the schema.

        :param monitor: :class:`Monitor`
          The monitor to create.

        :returns: :class:`Monitor`
        """

        body = monitor.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/data-quality/v1/monitors", body=body, headers=headers)
        return Monitor.from_dict(res)

    def create_refresh(self, object_type: str, object_id: str, refresh: Refresh) -> Refresh:
        """Creates a refresh. Currently only supported for the `table` `object_type`. The call must be made in
        the same workspace as where the monitor was created.

        The caller must have either of the following sets of permissions: 1. **MANAGE** and **USE_CATALOG** on
        the table's parent catalog. 2. **USE_CATALOG** on the table's parent catalog, and **MANAGE** and
        **USE_SCHEMA** on the table's parent schema. 3. **USE_CATALOG** on the table's parent catalog,
        **USE_SCHEMA** on the table's parent schema, and **MANAGE** on the table.

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema`or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id
        :param refresh: :class:`Refresh`
          The refresh to create

        :returns: :class:`Refresh`
        """

        body = refresh.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", f"/api/data-quality/v1/monitors/{object_type}/{object_id}/refreshes", body=body, headers=headers
        )
        return Refresh.from_dict(res)

    def delete_monitor(self, object_type: str, object_id: str):
        """Delete a data quality monitor on Unity Catalog object.

        For the `table` `object_type`, the caller must have either of the following sets of permissions:
        **MANAGE** and **USE_CATALOG** on the table's parent catalog. **USE_CATALOG** on the table's parent
        catalog, and **MANAGE** and **USE_SCHEMA** on the table's parent schema. **USE_CATALOG** on the
        table's parent catalog, **USE_SCHEMA** on the table's parent schema, and **MANAGE** on the table.

        Note that the metric tables and dashboard will not be deleted as part of this call; those assets must
        be manually cleaned up (if desired).

        For the `schema` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the schema's parent catalog. 2. **USE_CATALOG** on the schema's
        parent catalog, and **MANAGE** and **USE_SCHEMA** on the schema.

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/data-quality/v1/monitors/{object_type}/{object_id}", headers=headers)

    def delete_refresh(self, object_type: str, object_id: str, refresh_id: int):
        """(Unimplemented) Delete a refresh

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id
        :param refresh_id: int
          Unique id of the refresh operation.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/data-quality/v1/monitors/{object_type}/{object_id}/refreshes/{refresh_id}", headers=headers
        )

    def get_monitor(self, object_type: str, object_id: str) -> Monitor:
        """Read a data quality monitor on a Unity Catalog object.

        For the `table` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the table's parent catalog. 2. **USE_CATALOG** on the table's parent
        catalog, and **MANAGE** and **USE_SCHEMA** on the table's parent schema. 3. **USE_CATALOG** on the
        table's parent catalog, **USE_SCHEMA** on the table's parent schema, and **SELECT** on the table.

        For the `schema` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the schema's parent catalog. 2. **USE_CATALOG** on the schema's
        parent catalog, and **USE_SCHEMA** on the schema.

        The returned information includes configuration values on the entity and parent entity as well as
        information on assets created by the monitor. Some information (e.g. dashboard) may be filtered out if
        the caller is in a different workspace than where the monitor was created.

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id

        :returns: :class:`Monitor`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/data-quality/v1/monitors/{object_type}/{object_id}", headers=headers)
        return Monitor.from_dict(res)

    def get_refresh(self, object_type: str, object_id: str, refresh_id: int) -> Refresh:
        """Get data quality monitor refresh. The call must be made in the same workspace as where the monitor was
        created.

        For the `table` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the table's parent catalog. 2. **USE_CATALOG** on the table's parent
        catalog, and **MANAGE** and **USE_SCHEMA** on the table's parent schema. 3. **USE_CATALOG** on the
        table's parent catalog, **USE_SCHEMA** on the table's parent schema, and **SELECT** on the table.

        For the `schema` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the schema's parent catalog. 2. **USE_CATALOG** on the schema's
        parent catalog, and **USE_SCHEMA** on the schema.

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id
        :param refresh_id: int
          Unique id of the refresh operation.

        :returns: :class:`Refresh`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/data-quality/v1/monitors/{object_type}/{object_id}/refreshes/{refresh_id}", headers=headers
        )
        return Refresh.from_dict(res)

    def list_monitor(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[Monitor]:
        """(Unimplemented) List data quality monitors.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`Monitor`
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
            json = self._api.do("GET", "/api/data-quality/v1/monitors", query=query, headers=headers)
            if "monitors" in json:
                for v in json["monitors"]:
                    yield Monitor.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_refresh(
        self, object_type: str, object_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[Refresh]:
        """List data quality monitor refreshes. The call must be made in the same workspace as where the monitor
        was created.

        For the `table` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the table's parent catalog. 2. **USE_CATALOG** on the table's parent
        catalog, and **MANAGE** and **USE_SCHEMA** on the table's parent schema. 3. **USE_CATALOG** on the
        table's parent catalog, **USE_SCHEMA** on the table's parent schema, and **SELECT** on the table.

        For the `schema` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the schema's parent catalog. 2. **USE_CATALOG** on the schema's
        parent catalog, and **USE_SCHEMA** on the schema.

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`Refresh`
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
                "GET",
                f"/api/data-quality/v1/monitors/{object_type}/{object_id}/refreshes",
                query=query,
                headers=headers,
            )
            if "refreshes" in json:
                for v in json["refreshes"]:
                    yield Refresh.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_monitor(self, object_type: str, object_id: str, monitor: Monitor, update_mask: str) -> Monitor:
        """Update a data quality monitor on Unity Catalog object.

        For the `table` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the table's parent catalog. 2. **USE_CATALOG** on the table's parent
        catalog, and **MANAGE** and **USE_SCHEMA** on the table's parent schema. 3. **USE_CATALOG** on the
        table's parent catalog, **USE_SCHEMA** on the table's parent schema, and **MANAGE** on the table.

        For the `schema` `object_type`, the caller must have either of the following sets of permissions: 1.
        **MANAGE** and **USE_CATALOG** on the schema's parent catalog. 2. **USE_CATALOG** on the schema's
        parent catalog, and **MANAGE** and **USE_SCHEMA** on the schema.

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id
        :param monitor: :class:`Monitor`
          The monitor to update.
        :param update_mask: str
          The field mask to specify which fields to update as a comma-separated list. Example value:
          `data_profiling_config.custom_metrics,data_profiling_config.schedule.quartz_cron_expression`

        :returns: :class:`Monitor`
        """

        body = monitor.as_dict()
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
            "PATCH", f"/api/data-quality/v1/monitors/{object_type}/{object_id}", query=query, body=body, headers=headers
        )
        return Monitor.from_dict(res)

    def update_refresh(
        self, object_type: str, object_id: str, refresh_id: int, refresh: Refresh, update_mask: str
    ) -> Refresh:
        """(Unimplemented) Update a refresh

        :param object_type: str
          The type of the monitored object. Can be one of the following: `schema` or `table`.
        :param object_id: str
          The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`.

          Find the `schema_id` from either: 1. The [schema_id] of the `Schemas` resource. 2. In [Catalog
          Explorer] > select the `schema` > go to the `Details` tab > the `Schema ID` field.

          Find the `table_id` from either: 1. The [table_id] of the `Tables` resource. 2. In [Catalog
          Explorer] > select the `table` > go to the `Details` tab > the `Table ID` field.

          [Catalog Explorer]: https://docs.databricks.com/aws/en/catalog-explorer/
          [schema_id]: https://docs.databricks.com/api/workspace/schemas/get#schema_id
          [table_id]: https://docs.databricks.com/api/workspace/tables/get#table_id
        :param refresh_id: int
          Unique id of the refresh operation.
        :param refresh: :class:`Refresh`
          The refresh to update.
        :param update_mask: str
          The field mask to specify which fields to update.

        :returns: :class:`Refresh`
        """

        body = refresh.as_dict()
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
            f"/api/data-quality/v1/monitors/{object_type}/{object_id}/refreshes/{refresh_id}",
            query=query,
            body=body,
            headers=headers,
        )
        return Refresh.from_dict(res)
