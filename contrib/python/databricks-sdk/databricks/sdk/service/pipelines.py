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
from databricks.sdk.service import compute
from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict, _repeated_enum)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AutoFullRefreshPolicy:
    """Policy for auto full refresh."""

    enabled: bool
    """(Required, Mutable) Whether to enable auto full refresh or not."""

    min_interval_hours: Optional[int] = None
    """(Optional, Mutable) Specify the minimum interval in hours between the timestamp at which a table
    was last full refreshed and the current timestamp for triggering auto full If unspecified and
    autoFullRefresh is enabled then by default min_interval_hours is 24 hours."""

    def as_dict(self) -> dict:
        """Serializes the AutoFullRefreshPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.min_interval_hours is not None:
            body["min_interval_hours"] = self.min_interval_hours
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AutoFullRefreshPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        if self.min_interval_hours is not None:
            body["min_interval_hours"] = self.min_interval_hours
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AutoFullRefreshPolicy:
        """Deserializes the AutoFullRefreshPolicy from a dictionary."""
        return cls(enabled=d.get("enabled", None), min_interval_hours=d.get("min_interval_hours", None))


class CloneMode(Enum):
    """Enum to specify which mode of clone to execute"""

    MIGRATE_TO_UC = "MIGRATE_TO_UC"


@dataclass
class ClonePipelineResponse:
    pipeline_id: Optional[str] = None
    """The pipeline id of the cloned pipeline"""

    def as_dict(self) -> dict:
        """Serializes the ClonePipelineResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClonePipelineResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClonePipelineResponse:
        """Deserializes the ClonePipelineResponse from a dictionary."""
        return cls(pipeline_id=d.get("pipeline_id", None))


@dataclass
class ConnectionParameters:
    source_catalog: Optional[str] = None
    """Source catalog for initial connection. This is necessary for schema exploration in some database
    systems like Oracle, and optional but nice-to-have in some other database systems like Postgres.
    For Oracle databases, this maps to a service name."""

    def as_dict(self) -> dict:
        """Serializes the ConnectionParameters into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ConnectionParameters into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ConnectionParameters:
        """Deserializes the ConnectionParameters from a dictionary."""
        return cls(source_catalog=d.get("source_catalog", None))


@dataclass
class CreatePipelineResponse:
    effective_settings: Optional[PipelineSpec] = None
    """Only returned when dry_run is true."""

    pipeline_id: Optional[str] = None
    """The unique identifier for the newly created pipeline. Only returned when dry_run is false."""

    def as_dict(self) -> dict:
        """Serializes the CreatePipelineResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.effective_settings:
            body["effective_settings"] = self.effective_settings.as_dict()
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreatePipelineResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.effective_settings:
            body["effective_settings"] = self.effective_settings
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreatePipelineResponse:
        """Deserializes the CreatePipelineResponse from a dictionary."""
        return cls(
            effective_settings=_from_dict(d, "effective_settings", PipelineSpec), pipeline_id=d.get("pipeline_id", None)
        )


@dataclass
class CronTrigger:
    quartz_cron_schedule: Optional[str] = None

    timezone_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the CronTrigger into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.quartz_cron_schedule is not None:
            body["quartz_cron_schedule"] = self.quartz_cron_schedule
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CronTrigger into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.quartz_cron_schedule is not None:
            body["quartz_cron_schedule"] = self.quartz_cron_schedule
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CronTrigger:
        """Deserializes the CronTrigger from a dictionary."""
        return cls(quartz_cron_schedule=d.get("quartz_cron_schedule", None), timezone_id=d.get("timezone_id", None))


@dataclass
class DataPlaneId:
    instance: Optional[str] = None
    """The instance name of the data plane emitting an event."""

    seq_no: Optional[int] = None
    """A sequence number, unique and increasing within the data plane instance."""

    def as_dict(self) -> dict:
        """Serializes the DataPlaneId into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.instance is not None:
            body["instance"] = self.instance
        if self.seq_no is not None:
            body["seq_no"] = self.seq_no
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DataPlaneId into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.instance is not None:
            body["instance"] = self.instance
        if self.seq_no is not None:
            body["seq_no"] = self.seq_no
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DataPlaneId:
        """Deserializes the DataPlaneId from a dictionary."""
        return cls(instance=d.get("instance", None), seq_no=d.get("seq_no", None))


class DayOfWeek(Enum):
    """Days of week in which the window is allowed to happen. If not specified all days of the week
    will be used."""

    FRIDAY = "FRIDAY"
    MONDAY = "MONDAY"
    SATURDAY = "SATURDAY"
    SUNDAY = "SUNDAY"
    THURSDAY = "THURSDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"


@dataclass
class DeletePipelineResponse:
    def as_dict(self) -> dict:
        """Serializes the DeletePipelineResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeletePipelineResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeletePipelineResponse:
        """Deserializes the DeletePipelineResponse from a dictionary."""
        return cls()


class DeploymentKind(Enum):
    """The deployment method that manages the pipeline: - BUNDLE: The pipeline is managed by a
    Databricks Asset Bundle."""

    BUNDLE = "BUNDLE"


@dataclass
class EditPipelineResponse:
    def as_dict(self) -> dict:
        """Serializes the EditPipelineResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EditPipelineResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EditPipelineResponse:
        """Deserializes the EditPipelineResponse from a dictionary."""
        return cls()


@dataclass
class ErrorDetail:
    exceptions: Optional[List[SerializedException]] = None
    """The exception thrown for this error, with its chain of cause."""

    fatal: Optional[bool] = None
    """Whether this error is considered fatal, that is, unrecoverable."""

    def as_dict(self) -> dict:
        """Serializes the ErrorDetail into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exceptions:
            body["exceptions"] = [v.as_dict() for v in self.exceptions]
        if self.fatal is not None:
            body["fatal"] = self.fatal
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ErrorDetail into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exceptions:
            body["exceptions"] = self.exceptions
        if self.fatal is not None:
            body["fatal"] = self.fatal
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ErrorDetail:
        """Deserializes the ErrorDetail from a dictionary."""
        return cls(exceptions=_repeated_dict(d, "exceptions", SerializedException), fatal=d.get("fatal", None))


class EventLevel(Enum):
    """The severity level of the event."""

    ERROR = "ERROR"
    INFO = "INFO"
    METRICS = "METRICS"
    WARN = "WARN"


@dataclass
class EventLogSpec:
    """Configurable event log parameters."""

    catalog: Optional[str] = None
    """The UC catalog the event log is published under."""

    name: Optional[str] = None
    """The name the event log is published to in UC."""

    schema: Optional[str] = None
    """The UC schema the event log is published under."""

    def as_dict(self) -> dict:
        """Serializes the EventLogSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EventLogSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EventLogSpec:
        """Deserializes the EventLogSpec from a dictionary."""
        return cls(catalog=d.get("catalog", None), name=d.get("name", None), schema=d.get("schema", None))


@dataclass
class FileLibrary:
    path: Optional[str] = None
    """The absolute path of the source code."""

    def as_dict(self) -> dict:
        """Serializes the FileLibrary into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.path is not None:
            body["path"] = self.path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileLibrary into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.path is not None:
            body["path"] = self.path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileLibrary:
        """Deserializes the FileLibrary from a dictionary."""
        return cls(path=d.get("path", None))


@dataclass
class Filters:
    exclude: Optional[List[str]] = None
    """Paths to exclude."""

    include: Optional[List[str]] = None
    """Paths to include."""

    def as_dict(self) -> dict:
        """Serializes the Filters into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exclude:
            body["exclude"] = [v for v in self.exclude]
        if self.include:
            body["include"] = [v for v in self.include]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Filters into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exclude:
            body["exclude"] = self.exclude
        if self.include:
            body["include"] = self.include
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Filters:
        """Deserializes the Filters from a dictionary."""
        return cls(exclude=d.get("exclude", None), include=d.get("include", None))


@dataclass
class GetPipelinePermissionLevelsResponse:
    permission_levels: Optional[List[PipelinePermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetPipelinePermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetPipelinePermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetPipelinePermissionLevelsResponse:
        """Deserializes the GetPipelinePermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", PipelinePermissionsDescription))


@dataclass
class GetPipelineResponse:
    cause: Optional[str] = None
    """An optional message detailing the cause of the pipeline state."""

    cluster_id: Optional[str] = None
    """The ID of the cluster that the pipeline is running on."""

    creator_user_name: Optional[str] = None
    """The username of the pipeline creator."""

    effective_budget_policy_id: Optional[str] = None
    """Serverless budget policy ID of this pipeline."""

    effective_publishing_mode: Optional[PublishingMode] = None
    """Publishing mode of the pipeline"""

    health: Optional[GetPipelineResponseHealth] = None
    """The health of a pipeline."""

    last_modified: Optional[int] = None
    """The last time the pipeline settings were modified or created."""

    latest_updates: Optional[List[UpdateStateInfo]] = None
    """Status of the latest updates for the pipeline. Ordered with the newest update first."""

    name: Optional[str] = None
    """A human friendly identifier for the pipeline, taken from the `spec`."""

    pipeline_id: Optional[str] = None
    """The ID of the pipeline."""

    run_as: Optional[RunAs] = None
    """The user or service principal that the pipeline runs as, if specified in the request. This field
    indicates the explicit configuration of `run_as` for the pipeline. To find the value in all
    cases, explicit or implicit, use `run_as_user_name`."""

    run_as_user_name: Optional[str] = None
    """Username of the user that the pipeline will run on behalf of."""

    spec: Optional[PipelineSpec] = None
    """The pipeline specification. This field is not returned when called by `ListPipelines`."""

    state: Optional[PipelineState] = None
    """The pipeline state."""

    def as_dict(self) -> dict:
        """Serializes the GetPipelineResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cause is not None:
            body["cause"] = self.cause
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_publishing_mode is not None:
            body["effective_publishing_mode"] = self.effective_publishing_mode.value
        if self.health is not None:
            body["health"] = self.health.value
        if self.last_modified is not None:
            body["last_modified"] = self.last_modified
        if self.latest_updates:
            body["latest_updates"] = [v.as_dict() for v in self.latest_updates]
        if self.name is not None:
            body["name"] = self.name
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.run_as:
            body["run_as"] = self.run_as.as_dict()
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetPipelineResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cause is not None:
            body["cause"] = self.cause
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_publishing_mode is not None:
            body["effective_publishing_mode"] = self.effective_publishing_mode
        if self.health is not None:
            body["health"] = self.health
        if self.last_modified is not None:
            body["last_modified"] = self.last_modified
        if self.latest_updates:
            body["latest_updates"] = self.latest_updates
        if self.name is not None:
            body["name"] = self.name
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.run_as:
            body["run_as"] = self.run_as
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.spec:
            body["spec"] = self.spec
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetPipelineResponse:
        """Deserializes the GetPipelineResponse from a dictionary."""
        return cls(
            cause=d.get("cause", None),
            cluster_id=d.get("cluster_id", None),
            creator_user_name=d.get("creator_user_name", None),
            effective_budget_policy_id=d.get("effective_budget_policy_id", None),
            effective_publishing_mode=_enum(d, "effective_publishing_mode", PublishingMode),
            health=_enum(d, "health", GetPipelineResponseHealth),
            last_modified=d.get("last_modified", None),
            latest_updates=_repeated_dict(d, "latest_updates", UpdateStateInfo),
            name=d.get("name", None),
            pipeline_id=d.get("pipeline_id", None),
            run_as=_from_dict(d, "run_as", RunAs),
            run_as_user_name=d.get("run_as_user_name", None),
            spec=_from_dict(d, "spec", PipelineSpec),
            state=_enum(d, "state", PipelineState),
        )


class GetPipelineResponseHealth(Enum):
    """The health of a pipeline."""

    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"


@dataclass
class GetUpdateResponse:
    update: Optional[UpdateInfo] = None
    """The current update info."""

    def as_dict(self) -> dict:
        """Serializes the GetUpdateResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.update:
            body["update"] = self.update.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetUpdateResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.update:
            body["update"] = self.update
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetUpdateResponse:
        """Deserializes the GetUpdateResponse from a dictionary."""
        return cls(update=_from_dict(d, "update", UpdateInfo))


@dataclass
class IngestionConfig:
    report: Optional[ReportSpec] = None
    """Select a specific source report."""

    schema: Optional[SchemaSpec] = None
    """Select all tables from a specific source schema."""

    table: Optional[TableSpec] = None
    """Select a specific source table."""

    def as_dict(self) -> dict:
        """Serializes the IngestionConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.report:
            body["report"] = self.report.as_dict()
        if self.schema:
            body["schema"] = self.schema.as_dict()
        if self.table:
            body["table"] = self.table.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IngestionConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.report:
            body["report"] = self.report
        if self.schema:
            body["schema"] = self.schema
        if self.table:
            body["table"] = self.table
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IngestionConfig:
        """Deserializes the IngestionConfig from a dictionary."""
        return cls(
            report=_from_dict(d, "report", ReportSpec),
            schema=_from_dict(d, "schema", SchemaSpec),
            table=_from_dict(d, "table", TableSpec),
        )


@dataclass
class IngestionGatewayPipelineDefinition:
    connection_name: str
    """Immutable. The Unity Catalog connection that this gateway pipeline uses to communicate with the
    source."""

    gateway_storage_catalog: str
    """Required, Immutable. The name of the catalog for the gateway pipeline's storage location."""

    gateway_storage_schema: str
    """Required, Immutable. The name of the schema for the gateway pipelines's storage location."""

    connection_id: Optional[str] = None
    """[Deprecated, use connection_name instead] Immutable. The Unity Catalog connection that this
    gateway pipeline uses to communicate with the source."""

    connection_parameters: Optional[ConnectionParameters] = None
    """Optional, Internal. Parameters required to establish an initial connection with the source."""

    gateway_storage_name: Optional[str] = None
    """Optional. The Unity Catalog-compatible name for the gateway storage location. This is the
    destination to use for the data that is extracted by the gateway. Spark Declarative Pipelines
    system will automatically create the storage location under the catalog and schema."""

    def as_dict(self) -> dict:
        """Serializes the IngestionGatewayPipelineDefinition into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection_id is not None:
            body["connection_id"] = self.connection_id
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        if self.connection_parameters:
            body["connection_parameters"] = self.connection_parameters.as_dict()
        if self.gateway_storage_catalog is not None:
            body["gateway_storage_catalog"] = self.gateway_storage_catalog
        if self.gateway_storage_name is not None:
            body["gateway_storage_name"] = self.gateway_storage_name
        if self.gateway_storage_schema is not None:
            body["gateway_storage_schema"] = self.gateway_storage_schema
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IngestionGatewayPipelineDefinition into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection_id is not None:
            body["connection_id"] = self.connection_id
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        if self.connection_parameters:
            body["connection_parameters"] = self.connection_parameters
        if self.gateway_storage_catalog is not None:
            body["gateway_storage_catalog"] = self.gateway_storage_catalog
        if self.gateway_storage_name is not None:
            body["gateway_storage_name"] = self.gateway_storage_name
        if self.gateway_storage_schema is not None:
            body["gateway_storage_schema"] = self.gateway_storage_schema
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IngestionGatewayPipelineDefinition:
        """Deserializes the IngestionGatewayPipelineDefinition from a dictionary."""
        return cls(
            connection_id=d.get("connection_id", None),
            connection_name=d.get("connection_name", None),
            connection_parameters=_from_dict(d, "connection_parameters", ConnectionParameters),
            gateway_storage_catalog=d.get("gateway_storage_catalog", None),
            gateway_storage_name=d.get("gateway_storage_name", None),
            gateway_storage_schema=d.get("gateway_storage_schema", None),
        )


@dataclass
class IngestionPipelineDefinition:
    connection_name: Optional[str] = None
    """The Unity Catalog connection that this ingestion pipeline uses to communicate with the source.
    This is used with both connectors for applications like Salesforce, Workday, and so on, and also
    database connectors like Oracle, (connector_type = QUERY_BASED OR connector_type = CDC). If
    connection name corresponds to database connectors like Oracle, and connector_type is not
    provided then connector_type defaults to QUERY_BASED. If connector_type is passed as CDC we use
    Combined Cdc Managed Ingestion pipeline. Under certain conditions, this can be replaced with
    ingestion_gateway_id to change the connector to Cdc Managed Ingestion Pipeline with Gateway
    pipeline."""

    full_refresh_window: Optional[OperationTimeWindow] = None
    """(Optional) A window that specifies a set of time ranges for snapshot queries in CDC."""

    ingest_from_uc_foreign_catalog: Optional[bool] = None
    """Immutable. If set to true, the pipeline will ingest tables from the UC foreign catalogs directly
    without the need to specify a UC connection or ingestion gateway. The `source_catalog` fields in
    objects of IngestionConfig are interpreted as the UC foreign catalogs to ingest from."""

    ingestion_gateway_id: Optional[str] = None
    """Identifier for the gateway that is used by this ingestion pipeline to communicate with the
    source database. This is used with CDC connectors to databases like SQL Server using a gateway
    pipeline (connector_type = CDC). Under certain conditions, this can be replaced with
    connection_name to change the connector to Combined Cdc Managed Ingestion Pipeline."""

    netsuite_jar_path: Optional[str] = None
    """Netsuite only configuration. When the field is set for a netsuite connector, the jar stored in
    the field will be validated and added to the classpath of pipeline's cluster."""

    objects: Optional[List[IngestionConfig]] = None
    """Required. Settings specifying tables to replicate and the destination for the replicated tables."""

    source_configurations: Optional[List[SourceConfig]] = None
    """Top-level source configurations"""

    source_type: Optional[IngestionSourceType] = None
    """The type of the foreign source. The source type will be inferred from the source connection or
    ingestion gateway. This field is output only and will be ignored if provided."""

    table_configuration: Optional[TableSpecificConfig] = None
    """Configuration settings to control the ingestion of tables. These settings are applied to all
    tables in the pipeline."""

    def as_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinition into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        if self.full_refresh_window:
            body["full_refresh_window"] = self.full_refresh_window.as_dict()
        if self.ingest_from_uc_foreign_catalog is not None:
            body["ingest_from_uc_foreign_catalog"] = self.ingest_from_uc_foreign_catalog
        if self.ingestion_gateway_id is not None:
            body["ingestion_gateway_id"] = self.ingestion_gateway_id
        if self.netsuite_jar_path is not None:
            body["netsuite_jar_path"] = self.netsuite_jar_path
        if self.objects:
            body["objects"] = [v.as_dict() for v in self.objects]
        if self.source_configurations:
            body["source_configurations"] = [v.as_dict() for v in self.source_configurations]
        if self.source_type is not None:
            body["source_type"] = self.source_type.value
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinition into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection_name is not None:
            body["connection_name"] = self.connection_name
        if self.full_refresh_window:
            body["full_refresh_window"] = self.full_refresh_window
        if self.ingest_from_uc_foreign_catalog is not None:
            body["ingest_from_uc_foreign_catalog"] = self.ingest_from_uc_foreign_catalog
        if self.ingestion_gateway_id is not None:
            body["ingestion_gateway_id"] = self.ingestion_gateway_id
        if self.netsuite_jar_path is not None:
            body["netsuite_jar_path"] = self.netsuite_jar_path
        if self.objects:
            body["objects"] = self.objects
        if self.source_configurations:
            body["source_configurations"] = self.source_configurations
        if self.source_type is not None:
            body["source_type"] = self.source_type
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IngestionPipelineDefinition:
        """Deserializes the IngestionPipelineDefinition from a dictionary."""
        return cls(
            connection_name=d.get("connection_name", None),
            full_refresh_window=_from_dict(d, "full_refresh_window", OperationTimeWindow),
            ingest_from_uc_foreign_catalog=d.get("ingest_from_uc_foreign_catalog", None),
            ingestion_gateway_id=d.get("ingestion_gateway_id", None),
            netsuite_jar_path=d.get("netsuite_jar_path", None),
            objects=_repeated_dict(d, "objects", IngestionConfig),
            source_configurations=_repeated_dict(d, "source_configurations", SourceConfig),
            source_type=_enum(d, "source_type", IngestionSourceType),
            table_configuration=_from_dict(d, "table_configuration", TableSpecificConfig),
        )


@dataclass
class IngestionPipelineDefinitionTableSpecificConfigQueryBasedConnectorConfig:
    """Configurations that are only applicable for query-based ingestion connectors."""

    cursor_columns: Optional[List[str]] = None
    """The names of the monotonically increasing columns in the source table that are used to enable
    the table to be read and ingested incrementally through structured streaming. The columns are
    allowed to have repeated values but have to be non-decreasing. If the source data is merged into
    the destination (e.g., using SCD Type 1 or Type 2), these columns will implicitly define the
    `sequence_by` behavior. You can still explicitly set `sequence_by` to override this default."""

    deletion_condition: Optional[str] = None
    """Specifies a SQL WHERE condition that specifies that the source row has been deleted. This is
    sometimes referred to as "soft-deletes". For example: "Operation = 'DELETE'" or "is_deleted =
    true". This field is orthogonal to `hard_deletion_sync_interval_in_seconds`, one for
    soft-deletes and the other for hard-deletes. See also the
    hard_deletion_sync_min_interval_in_seconds field for handling of "hard deletes" where the source
    rows are physically removed from the table."""

    hard_deletion_sync_min_interval_in_seconds: Optional[int] = None
    """Specifies the minimum interval (in seconds) between snapshots on primary keys for detecting and
    synchronizing hard deletions—i.e., rows that have been physically removed from the source
    table. This interval acts as a lower bound. If ingestion runs less frequently than this value,
    hard deletion synchronization will align with the actual ingestion frequency instead of
    happening more often. If not set, hard deletion synchronization via snapshots is disabled. This
    field is mutable and can be updated without triggering a full snapshot."""

    def as_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinitionTableSpecificConfigQueryBasedConnectorConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cursor_columns:
            body["cursor_columns"] = [v for v in self.cursor_columns]
        if self.deletion_condition is not None:
            body["deletion_condition"] = self.deletion_condition
        if self.hard_deletion_sync_min_interval_in_seconds is not None:
            body["hard_deletion_sync_min_interval_in_seconds"] = self.hard_deletion_sync_min_interval_in_seconds
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinitionTableSpecificConfigQueryBasedConnectorConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cursor_columns:
            body["cursor_columns"] = self.cursor_columns
        if self.deletion_condition is not None:
            body["deletion_condition"] = self.deletion_condition
        if self.hard_deletion_sync_min_interval_in_seconds is not None:
            body["hard_deletion_sync_min_interval_in_seconds"] = self.hard_deletion_sync_min_interval_in_seconds
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IngestionPipelineDefinitionTableSpecificConfigQueryBasedConnectorConfig:
        """Deserializes the IngestionPipelineDefinitionTableSpecificConfigQueryBasedConnectorConfig from a dictionary."""
        return cls(
            cursor_columns=d.get("cursor_columns", None),
            deletion_condition=d.get("deletion_condition", None),
            hard_deletion_sync_min_interval_in_seconds=d.get("hard_deletion_sync_min_interval_in_seconds", None),
        )


@dataclass
class IngestionPipelineDefinitionWorkdayReportParameters:
    incremental: Optional[bool] = None
    """(Optional) Marks the report as incremental. This field is deprecated and should not be used. Use
    `parameters` instead. The incremental behavior is now controlled by the `parameters` field."""

    parameters: Optional[Dict[str, str]] = None
    """Parameters for the Workday report. Each key represents the parameter name (e.g., "start_date",
    "end_date"), and the corresponding value is a SQL-like expression used to compute the parameter
    value at runtime. Example: { "start_date": "{ coalesce(current_offset(), date(\"2025-02-01\"))
    }", "end_date": "{ current_date() - INTERVAL 1 DAY }" }"""

    report_parameters: Optional[List[IngestionPipelineDefinitionWorkdayReportParametersQueryKeyValue]] = None
    """(Optional) Additional custom parameters for Workday Report This field is deprecated and should
    not be used. Use `parameters` instead."""

    def as_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinitionWorkdayReportParameters into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.incremental is not None:
            body["incremental"] = self.incremental
        if self.parameters:
            body["parameters"] = self.parameters
        if self.report_parameters:
            body["report_parameters"] = [v.as_dict() for v in self.report_parameters]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinitionWorkdayReportParameters into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.incremental is not None:
            body["incremental"] = self.incremental
        if self.parameters:
            body["parameters"] = self.parameters
        if self.report_parameters:
            body["report_parameters"] = self.report_parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IngestionPipelineDefinitionWorkdayReportParameters:
        """Deserializes the IngestionPipelineDefinitionWorkdayReportParameters from a dictionary."""
        return cls(
            incremental=d.get("incremental", None),
            parameters=d.get("parameters", None),
            report_parameters=_repeated_dict(
                d, "report_parameters", IngestionPipelineDefinitionWorkdayReportParametersQueryKeyValue
            ),
        )


@dataclass
class IngestionPipelineDefinitionWorkdayReportParametersQueryKeyValue:
    key: Optional[str] = None
    """Key for the report parameter, can be a column name or other metadata"""

    value: Optional[str] = None
    """Value for the report parameter. Possible values it can take are these sql functions: 1.
    coalesce(current_offset(), date("YYYY-MM-DD")) -> if current_offset() is null, then the passed
    date, else current_offset() 2. current_date() 3. date_sub(current_date(), x) -> subtract x (some
    non-negative integer) days from current date"""

    def as_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinitionWorkdayReportParametersQueryKeyValue into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the IngestionPipelineDefinitionWorkdayReportParametersQueryKeyValue into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> IngestionPipelineDefinitionWorkdayReportParametersQueryKeyValue:
        """Deserializes the IngestionPipelineDefinitionWorkdayReportParametersQueryKeyValue from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


class IngestionSourceType(Enum):

    BIGQUERY = "BIGQUERY"
    DYNAMICS365 = "DYNAMICS365"
    FOREIGN_CATALOG = "FOREIGN_CATALOG"
    GA4_RAW_DATA = "GA4_RAW_DATA"
    MANAGED_POSTGRESQL = "MANAGED_POSTGRESQL"
    MYSQL = "MYSQL"
    NETSUITE = "NETSUITE"
    ORACLE = "ORACLE"
    POSTGRESQL = "POSTGRESQL"
    SALESFORCE = "SALESFORCE"
    SERVICENOW = "SERVICENOW"
    SHAREPOINT = "SHAREPOINT"
    SQLSERVER = "SQLSERVER"
    TERADATA = "TERADATA"
    WORKDAY_RAAS = "WORKDAY_RAAS"


@dataclass
class ListPipelineEventsResponse:
    events: Optional[List[PipelineEvent]] = None
    """The list of events matching the request criteria."""

    next_page_token: Optional[str] = None
    """If present, a token to fetch the next page of events."""

    prev_page_token: Optional[str] = None
    """If present, a token to fetch the previous page of events."""

    def as_dict(self) -> dict:
        """Serializes the ListPipelineEventsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.events:
            body["events"] = [v.as_dict() for v in self.events]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListPipelineEventsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.events:
            body["events"] = self.events
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListPipelineEventsResponse:
        """Deserializes the ListPipelineEventsResponse from a dictionary."""
        return cls(
            events=_repeated_dict(d, "events", PipelineEvent),
            next_page_token=d.get("next_page_token", None),
            prev_page_token=d.get("prev_page_token", None),
        )


@dataclass
class ListPipelinesResponse:
    next_page_token: Optional[str] = None
    """If present, a token to fetch the next page of events."""

    statuses: Optional[List[PipelineStateInfo]] = None
    """The list of events matching the request criteria."""

    def as_dict(self) -> dict:
        """Serializes the ListPipelinesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.statuses:
            body["statuses"] = [v.as_dict() for v in self.statuses]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListPipelinesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.statuses:
            body["statuses"] = self.statuses
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListPipelinesResponse:
        """Deserializes the ListPipelinesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), statuses=_repeated_dict(d, "statuses", PipelineStateInfo)
        )


@dataclass
class ListUpdatesResponse:
    next_page_token: Optional[str] = None
    """If present, then there are more results, and this a token to be used in a subsequent request to
    fetch the next page."""

    prev_page_token: Optional[str] = None
    """If present, then this token can be used in a subsequent request to fetch the previous page."""

    updates: Optional[List[UpdateInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ListUpdatesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        if self.updates:
            body["updates"] = [v.as_dict() for v in self.updates]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListUpdatesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        if self.updates:
            body["updates"] = self.updates
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListUpdatesResponse:
        """Deserializes the ListUpdatesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            prev_page_token=d.get("prev_page_token", None),
            updates=_repeated_dict(d, "updates", UpdateInfo),
        )


@dataclass
class ManualTrigger:
    def as_dict(self) -> dict:
        """Serializes the ManualTrigger into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ManualTrigger into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ManualTrigger:
        """Deserializes the ManualTrigger from a dictionary."""
        return cls()


class MaturityLevel(Enum):
    """Maturity level for EventDetails."""

    DEPRECATED = "DEPRECATED"
    EVOLVING = "EVOLVING"
    STABLE = "STABLE"


@dataclass
class NotebookLibrary:
    path: Optional[str] = None
    """The absolute path of the source code."""

    def as_dict(self) -> dict:
        """Serializes the NotebookLibrary into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.path is not None:
            body["path"] = self.path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotebookLibrary into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.path is not None:
            body["path"] = self.path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotebookLibrary:
        """Deserializes the NotebookLibrary from a dictionary."""
        return cls(path=d.get("path", None))


@dataclass
class Notifications:
    alerts: Optional[List[str]] = None
    """A list of alerts that trigger the sending of notifications to the configured destinations. The
    supported alerts are:
    
    * `on-update-success`: A pipeline update completes successfully. * `on-update-failure`: Each
    time a pipeline update fails. * `on-update-fatal-failure`: A pipeline update fails with a
    non-retryable (fatal) error. * `on-flow-failure`: A single data flow fails."""

    email_recipients: Optional[List[str]] = None
    """A list of email addresses notified when a configured alert is triggered."""

    def as_dict(self) -> dict:
        """Serializes the Notifications into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alerts:
            body["alerts"] = [v for v in self.alerts]
        if self.email_recipients:
            body["email_recipients"] = [v for v in self.email_recipients]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Notifications into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alerts:
            body["alerts"] = self.alerts
        if self.email_recipients:
            body["email_recipients"] = self.email_recipients
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Notifications:
        """Deserializes the Notifications from a dictionary."""
        return cls(alerts=d.get("alerts", None), email_recipients=d.get("email_recipients", None))


@dataclass
class OperationTimeWindow:
    """Proto representing a window"""

    start_hour: int
    """An integer between 0 and 23 denoting the start hour for the window in the 24-hour day."""

    days_of_week: Optional[List[DayOfWeek]] = None
    """Days of week in which the window is allowed to happen If not specified all days of the week will
    be used."""

    time_zone_id: Optional[str] = None
    """Time zone id of window. See
    https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-conf-mgmt-set-timezone.html
    for details. If not specified, UTC will be used."""

    def as_dict(self) -> dict:
        """Serializes the OperationTimeWindow into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.days_of_week:
            body["days_of_week"] = [v.value for v in self.days_of_week]
        if self.start_hour is not None:
            body["start_hour"] = self.start_hour
        if self.time_zone_id is not None:
            body["time_zone_id"] = self.time_zone_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OperationTimeWindow into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.days_of_week:
            body["days_of_week"] = self.days_of_week
        if self.start_hour is not None:
            body["start_hour"] = self.start_hour
        if self.time_zone_id is not None:
            body["time_zone_id"] = self.time_zone_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OperationTimeWindow:
        """Deserializes the OperationTimeWindow from a dictionary."""
        return cls(
            days_of_week=_repeated_enum(d, "days_of_week", DayOfWeek),
            start_hour=d.get("start_hour", None),
            time_zone_id=d.get("time_zone_id", None),
        )


@dataclass
class Origin:
    batch_id: Optional[int] = None
    """The id of a batch. Unique within a flow."""

    cloud: Optional[str] = None
    """The cloud provider, e.g., AWS or Azure."""

    cluster_id: Optional[str] = None
    """The id of the cluster where an execution happens. Unique within a region."""

    dataset_name: Optional[str] = None
    """The name of a dataset. Unique within a pipeline."""

    flow_id: Optional[str] = None
    """The id of the flow. Globally unique. Incremental queries will generally reuse the same id while
    complete queries will have a new id per update."""

    flow_name: Optional[str] = None
    """The name of the flow. Not unique."""

    host: Optional[str] = None
    """The optional host name where the event was triggered"""

    maintenance_id: Optional[str] = None
    """The id of a maintenance run. Globally unique."""

    materialization_name: Optional[str] = None
    """Materialization name."""

    org_id: Optional[int] = None
    """The org id of the user. Unique within a cloud."""

    pipeline_id: Optional[str] = None
    """The id of the pipeline. Globally unique."""

    pipeline_name: Optional[str] = None
    """The name of the pipeline. Not unique."""

    region: Optional[str] = None
    """The cloud region."""

    request_id: Optional[str] = None
    """The id of the request that caused an update."""

    table_id: Optional[str] = None
    """The id of a (delta) table. Globally unique."""

    uc_resource_id: Optional[str] = None
    """The Unity Catalog id of the MV or ST being updated."""

    update_id: Optional[str] = None
    """The id of an execution. Globally unique."""

    def as_dict(self) -> dict:
        """Serializes the Origin into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.batch_id is not None:
            body["batch_id"] = self.batch_id
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        if self.flow_id is not None:
            body["flow_id"] = self.flow_id
        if self.flow_name is not None:
            body["flow_name"] = self.flow_name
        if self.host is not None:
            body["host"] = self.host
        if self.maintenance_id is not None:
            body["maintenance_id"] = self.maintenance_id
        if self.materialization_name is not None:
            body["materialization_name"] = self.materialization_name
        if self.org_id is not None:
            body["org_id"] = self.org_id
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.pipeline_name is not None:
            body["pipeline_name"] = self.pipeline_name
        if self.region is not None:
            body["region"] = self.region
        if self.request_id is not None:
            body["request_id"] = self.request_id
        if self.table_id is not None:
            body["table_id"] = self.table_id
        if self.uc_resource_id is not None:
            body["uc_resource_id"] = self.uc_resource_id
        if self.update_id is not None:
            body["update_id"] = self.update_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Origin into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.batch_id is not None:
            body["batch_id"] = self.batch_id
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        if self.flow_id is not None:
            body["flow_id"] = self.flow_id
        if self.flow_name is not None:
            body["flow_name"] = self.flow_name
        if self.host is not None:
            body["host"] = self.host
        if self.maintenance_id is not None:
            body["maintenance_id"] = self.maintenance_id
        if self.materialization_name is not None:
            body["materialization_name"] = self.materialization_name
        if self.org_id is not None:
            body["org_id"] = self.org_id
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.pipeline_name is not None:
            body["pipeline_name"] = self.pipeline_name
        if self.region is not None:
            body["region"] = self.region
        if self.request_id is not None:
            body["request_id"] = self.request_id
        if self.table_id is not None:
            body["table_id"] = self.table_id
        if self.uc_resource_id is not None:
            body["uc_resource_id"] = self.uc_resource_id
        if self.update_id is not None:
            body["update_id"] = self.update_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Origin:
        """Deserializes the Origin from a dictionary."""
        return cls(
            batch_id=d.get("batch_id", None),
            cloud=d.get("cloud", None),
            cluster_id=d.get("cluster_id", None),
            dataset_name=d.get("dataset_name", None),
            flow_id=d.get("flow_id", None),
            flow_name=d.get("flow_name", None),
            host=d.get("host", None),
            maintenance_id=d.get("maintenance_id", None),
            materialization_name=d.get("materialization_name", None),
            org_id=d.get("org_id", None),
            pipeline_id=d.get("pipeline_id", None),
            pipeline_name=d.get("pipeline_name", None),
            region=d.get("region", None),
            request_id=d.get("request_id", None),
            table_id=d.get("table_id", None),
            uc_resource_id=d.get("uc_resource_id", None),
            update_id=d.get("update_id", None),
        )


@dataclass
class PathPattern:
    include: Optional[str] = None
    """The source code to include for pipelines"""

    def as_dict(self) -> dict:
        """Serializes the PathPattern into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.include is not None:
            body["include"] = self.include
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PathPattern into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.include is not None:
            body["include"] = self.include
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PathPattern:
        """Deserializes the PathPattern from a dictionary."""
        return cls(include=d.get("include", None))


@dataclass
class PipelineAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[PipelinePermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the PipelineAccessControlRequest into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the PipelineAccessControlRequest into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> PipelineAccessControlRequest:
        """Deserializes the PipelineAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", PipelinePermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class PipelineAccessControlResponse:
    all_permissions: Optional[List[PipelinePermission]] = None
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
        """Serializes the PipelineAccessControlResponse into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the PipelineAccessControlResponse into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> PipelineAccessControlResponse:
        """Deserializes the PipelineAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", PipelinePermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class PipelineCluster:
    apply_policy_default_values: Optional[bool] = None
    """Note: This field won't be persisted. Only API users will check this field."""

    autoscale: Optional[PipelineClusterAutoscale] = None
    """Parameters needed in order to automatically scale clusters up and down based on load. Note:
    autoscaling works best with DB runtime versions 3.0 or later."""

    aws_attributes: Optional[compute.AwsAttributes] = None
    """Attributes related to clusters running on Amazon Web Services. If not specified at cluster
    creation, a set of default values will be used."""

    azure_attributes: Optional[compute.AzureAttributes] = None
    """Attributes related to clusters running on Microsoft Azure. If not specified at cluster creation,
    a set of default values will be used."""

    cluster_log_conf: Optional[compute.ClusterLogConf] = None
    """The configuration for delivering spark logs to a long-term storage destination. Only dbfs
    destinations are supported. Only one destination can be specified for one cluster. If the conf
    is given, the logs will be delivered to the destination every `5 mins`. The destination of
    driver logs is `$destination/$clusterId/driver`, while the destination of executor logs is
    `$destination/$clusterId/executor`."""

    custom_tags: Optional[Dict[str, str]] = None
    """Additional tags for cluster resources. Databricks will tag all cluster resources (e.g., AWS
    instances and EBS volumes) with these tags in addition to `default_tags`. Notes:
    
    - Currently, Databricks allows at most 45 custom tags
    
    - Clusters can only reuse cloud resources if the resources' tags are a subset of the cluster
    tags"""

    driver_instance_pool_id: Optional[str] = None
    """The optional ID of the instance pool for the driver of the cluster belongs. The pool cluster
    uses the instance pool with id (instance_pool_id) if the driver pool is not assigned."""

    driver_node_type_id: Optional[str] = None
    """The node type of the Spark driver. Note that this field is optional; if unset, the driver node
    type will be set as the same value as `node_type_id` defined above."""

    enable_local_disk_encryption: Optional[bool] = None
    """Whether to enable local disk encryption for the cluster."""

    gcp_attributes: Optional[compute.GcpAttributes] = None
    """Attributes related to clusters running on Google Cloud Platform. If not specified at cluster
    creation, a set of default values will be used."""

    init_scripts: Optional[List[compute.InitScriptInfo]] = None
    """The configuration for storing init scripts. Any number of destinations can be specified. The
    scripts are executed sequentially in the order provided. If `cluster_log_conf` is specified,
    init script logs are sent to `<destination>/<cluster-ID>/init_scripts`."""

    instance_pool_id: Optional[str] = None
    """The optional ID of the instance pool to which the cluster belongs."""

    label: Optional[str] = None
    """A label for the cluster specification, either `default` to configure the default cluster, or
    `maintenance` to configure the maintenance cluster. This field is optional. The default value is
    `default`."""

    node_type_id: Optional[str] = None
    """This field encodes, through a single value, the resources available to each of the Spark nodes
    in this cluster. For example, the Spark nodes can be provisioned and optimized for memory or
    compute intensive workloads. A list of available node types can be retrieved by using the
    :method:clusters/listNodeTypes API call."""

    num_workers: Optional[int] = None
    """Number of worker nodes that this cluster should have. A cluster has one Spark Driver and
    `num_workers` Executors for a total of `num_workers` + 1 Spark nodes.
    
    Note: When reading the properties of a cluster, this field reflects the desired number of
    workers rather than the actual current number of workers. For instance, if a cluster is resized
    from 5 to 10 workers, this field will immediately be updated to reflect the target size of 10
    workers, whereas the workers listed in `spark_info` will gradually increase from 5 to 10 as the
    new nodes are provisioned."""

    policy_id: Optional[str] = None
    """The ID of the cluster policy used to create the cluster if applicable."""

    spark_conf: Optional[Dict[str, str]] = None
    """An object containing a set of optional, user-specified Spark configuration key-value pairs. See
    :method:clusters/create for more details."""

    spark_env_vars: Optional[Dict[str, str]] = None
    """An object containing a set of optional, user-specified environment variable key-value pairs.
    Please note that key-value pair of the form (X,Y) will be exported as is (i.e., `export X='Y'`)
    while launching the driver and workers.
    
    In order to specify an additional set of `SPARK_DAEMON_JAVA_OPTS`, we recommend appending them
    to `$SPARK_DAEMON_JAVA_OPTS` as shown in the example below. This ensures that all default
    databricks managed environmental variables are included as well.
    
    Example Spark environment variables: `{"SPARK_WORKER_MEMORY": "28000m", "SPARK_LOCAL_DIRS":
    "/local_disk0"}` or `{"SPARK_DAEMON_JAVA_OPTS": "$SPARK_DAEMON_JAVA_OPTS
    -Dspark.shuffle.service.enabled=true"}`"""

    ssh_public_keys: Optional[List[str]] = None
    """SSH public key contents that will be added to each Spark node in this cluster. The corresponding
    private keys can be used to login with the user name `ubuntu` on port `2200`. Up to 10 keys can
    be specified."""

    def as_dict(self) -> dict:
        """Serializes the PipelineCluster into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.apply_policy_default_values is not None:
            body["apply_policy_default_values"] = self.apply_policy_default_values
        if self.autoscale:
            body["autoscale"] = self.autoscale.as_dict()
        if self.aws_attributes:
            body["aws_attributes"] = self.aws_attributes.as_dict()
        if self.azure_attributes:
            body["azure_attributes"] = self.azure_attributes.as_dict()
        if self.cluster_log_conf:
            body["cluster_log_conf"] = self.cluster_log_conf.as_dict()
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.driver_instance_pool_id is not None:
            body["driver_instance_pool_id"] = self.driver_instance_pool_id
        if self.driver_node_type_id is not None:
            body["driver_node_type_id"] = self.driver_node_type_id
        if self.enable_local_disk_encryption is not None:
            body["enable_local_disk_encryption"] = self.enable_local_disk_encryption
        if self.gcp_attributes:
            body["gcp_attributes"] = self.gcp_attributes.as_dict()
        if self.init_scripts:
            body["init_scripts"] = [v.as_dict() for v in self.init_scripts]
        if self.instance_pool_id is not None:
            body["instance_pool_id"] = self.instance_pool_id
        if self.label is not None:
            body["label"] = self.label
        if self.node_type_id is not None:
            body["node_type_id"] = self.node_type_id
        if self.num_workers is not None:
            body["num_workers"] = self.num_workers
        if self.policy_id is not None:
            body["policy_id"] = self.policy_id
        if self.spark_conf:
            body["spark_conf"] = self.spark_conf
        if self.spark_env_vars:
            body["spark_env_vars"] = self.spark_env_vars
        if self.ssh_public_keys:
            body["ssh_public_keys"] = [v for v in self.ssh_public_keys]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineCluster into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.apply_policy_default_values is not None:
            body["apply_policy_default_values"] = self.apply_policy_default_values
        if self.autoscale:
            body["autoscale"] = self.autoscale
        if self.aws_attributes:
            body["aws_attributes"] = self.aws_attributes
        if self.azure_attributes:
            body["azure_attributes"] = self.azure_attributes
        if self.cluster_log_conf:
            body["cluster_log_conf"] = self.cluster_log_conf
        if self.custom_tags:
            body["custom_tags"] = self.custom_tags
        if self.driver_instance_pool_id is not None:
            body["driver_instance_pool_id"] = self.driver_instance_pool_id
        if self.driver_node_type_id is not None:
            body["driver_node_type_id"] = self.driver_node_type_id
        if self.enable_local_disk_encryption is not None:
            body["enable_local_disk_encryption"] = self.enable_local_disk_encryption
        if self.gcp_attributes:
            body["gcp_attributes"] = self.gcp_attributes
        if self.init_scripts:
            body["init_scripts"] = self.init_scripts
        if self.instance_pool_id is not None:
            body["instance_pool_id"] = self.instance_pool_id
        if self.label is not None:
            body["label"] = self.label
        if self.node_type_id is not None:
            body["node_type_id"] = self.node_type_id
        if self.num_workers is not None:
            body["num_workers"] = self.num_workers
        if self.policy_id is not None:
            body["policy_id"] = self.policy_id
        if self.spark_conf:
            body["spark_conf"] = self.spark_conf
        if self.spark_env_vars:
            body["spark_env_vars"] = self.spark_env_vars
        if self.ssh_public_keys:
            body["ssh_public_keys"] = self.ssh_public_keys
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineCluster:
        """Deserializes the PipelineCluster from a dictionary."""
        return cls(
            apply_policy_default_values=d.get("apply_policy_default_values", None),
            autoscale=_from_dict(d, "autoscale", PipelineClusterAutoscale),
            aws_attributes=_from_dict(d, "aws_attributes", compute.AwsAttributes),
            azure_attributes=_from_dict(d, "azure_attributes", compute.AzureAttributes),
            cluster_log_conf=_from_dict(d, "cluster_log_conf", compute.ClusterLogConf),
            custom_tags=d.get("custom_tags", None),
            driver_instance_pool_id=d.get("driver_instance_pool_id", None),
            driver_node_type_id=d.get("driver_node_type_id", None),
            enable_local_disk_encryption=d.get("enable_local_disk_encryption", None),
            gcp_attributes=_from_dict(d, "gcp_attributes", compute.GcpAttributes),
            init_scripts=_repeated_dict(d, "init_scripts", compute.InitScriptInfo),
            instance_pool_id=d.get("instance_pool_id", None),
            label=d.get("label", None),
            node_type_id=d.get("node_type_id", None),
            num_workers=d.get("num_workers", None),
            policy_id=d.get("policy_id", None),
            spark_conf=d.get("spark_conf", None),
            spark_env_vars=d.get("spark_env_vars", None),
            ssh_public_keys=d.get("ssh_public_keys", None),
        )


@dataclass
class PipelineClusterAutoscale:
    min_workers: int
    """The minimum number of workers the cluster can scale down to when underutilized. It is also the
    initial number of workers the cluster will have after creation."""

    max_workers: int
    """The maximum number of workers to which the cluster can scale up when overloaded. `max_workers`
    must be strictly greater than `min_workers`."""

    mode: Optional[PipelineClusterAutoscaleMode] = None
    """Databricks Enhanced Autoscaling optimizes cluster utilization by automatically allocating
    cluster resources based on workload volume, with minimal impact to the data processing latency
    of your pipelines. Enhanced Autoscaling is available for `updates` clusters only. The legacy
    autoscaling feature is used for `maintenance` clusters."""

    def as_dict(self) -> dict:
        """Serializes the PipelineClusterAutoscale into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.max_workers is not None:
            body["max_workers"] = self.max_workers
        if self.min_workers is not None:
            body["min_workers"] = self.min_workers
        if self.mode is not None:
            body["mode"] = self.mode.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineClusterAutoscale into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.max_workers is not None:
            body["max_workers"] = self.max_workers
        if self.min_workers is not None:
            body["min_workers"] = self.min_workers
        if self.mode is not None:
            body["mode"] = self.mode
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineClusterAutoscale:
        """Deserializes the PipelineClusterAutoscale from a dictionary."""
        return cls(
            max_workers=d.get("max_workers", None),
            min_workers=d.get("min_workers", None),
            mode=_enum(d, "mode", PipelineClusterAutoscaleMode),
        )


class PipelineClusterAutoscaleMode(Enum):
    """Databricks Enhanced Autoscaling optimizes cluster utilization by automatically allocating
    cluster resources based on workload volume, with minimal impact to the data processing latency
    of your pipelines. Enhanced Autoscaling is available for `updates` clusters only. The legacy
    autoscaling feature is used for `maintenance` clusters."""

    ENHANCED = "ENHANCED"
    LEGACY = "LEGACY"


@dataclass
class PipelineDeployment:
    kind: DeploymentKind
    """The deployment method that manages the pipeline."""

    metadata_file_path: Optional[str] = None
    """The path to the file containing metadata about the deployment."""

    def as_dict(self) -> dict:
        """Serializes the PipelineDeployment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.kind is not None:
            body["kind"] = self.kind.value
        if self.metadata_file_path is not None:
            body["metadata_file_path"] = self.metadata_file_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineDeployment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.kind is not None:
            body["kind"] = self.kind
        if self.metadata_file_path is not None:
            body["metadata_file_path"] = self.metadata_file_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineDeployment:
        """Deserializes the PipelineDeployment from a dictionary."""
        return cls(kind=_enum(d, "kind", DeploymentKind), metadata_file_path=d.get("metadata_file_path", None))


@dataclass
class PipelineEvent:
    error: Optional[ErrorDetail] = None
    """Information about an error captured by the event."""

    event_type: Optional[str] = None
    """The event type. Should always correspond to the details"""

    id: Optional[str] = None
    """A time-based, globally unique id."""

    level: Optional[EventLevel] = None
    """The severity level of the event."""

    maturity_level: Optional[MaturityLevel] = None
    """Maturity level for event_type."""

    message: Optional[str] = None
    """The display message associated with the event."""

    origin: Optional[Origin] = None
    """Describes where the event originates from."""

    sequence: Optional[Sequencing] = None
    """A sequencing object to identify and order events."""

    timestamp: Optional[str] = None
    """The time of the event."""

    truncation: Optional[Truncation] = None
    """Information about which fields were truncated from this event due to size constraints. If empty
    or absent, no truncation occurred. See https://docs.databricks.com/en/ldp/monitor-event-logs for
    information on retrieving complete event data."""

    def as_dict(self) -> dict:
        """Serializes the PipelineEvent into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.error:
            body["error"] = self.error.as_dict()
        if self.event_type is not None:
            body["event_type"] = self.event_type
        if self.id is not None:
            body["id"] = self.id
        if self.level is not None:
            body["level"] = self.level.value
        if self.maturity_level is not None:
            body["maturity_level"] = self.maturity_level.value
        if self.message is not None:
            body["message"] = self.message
        if self.origin:
            body["origin"] = self.origin.as_dict()
        if self.sequence:
            body["sequence"] = self.sequence.as_dict()
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.truncation:
            body["truncation"] = self.truncation.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineEvent into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.error:
            body["error"] = self.error
        if self.event_type is not None:
            body["event_type"] = self.event_type
        if self.id is not None:
            body["id"] = self.id
        if self.level is not None:
            body["level"] = self.level
        if self.maturity_level is not None:
            body["maturity_level"] = self.maturity_level
        if self.message is not None:
            body["message"] = self.message
        if self.origin:
            body["origin"] = self.origin
        if self.sequence:
            body["sequence"] = self.sequence
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.truncation:
            body["truncation"] = self.truncation
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineEvent:
        """Deserializes the PipelineEvent from a dictionary."""
        return cls(
            error=_from_dict(d, "error", ErrorDetail),
            event_type=d.get("event_type", None),
            id=d.get("id", None),
            level=_enum(d, "level", EventLevel),
            maturity_level=_enum(d, "maturity_level", MaturityLevel),
            message=d.get("message", None),
            origin=_from_dict(d, "origin", Origin),
            sequence=_from_dict(d, "sequence", Sequencing),
            timestamp=d.get("timestamp", None),
            truncation=_from_dict(d, "truncation", Truncation),
        )


@dataclass
class PipelineLibrary:
    file: Optional[FileLibrary] = None
    """The path to a file that defines a pipeline and is stored in the Databricks Repos."""

    glob: Optional[PathPattern] = None
    """The unified field to include source codes. Each entry can be a notebook path, a file path, or a
    folder path that ends `/**`. This field cannot be used together with `notebook` or `file`."""

    jar: Optional[str] = None
    """URI of the jar to be installed. Currently only DBFS is supported."""

    maven: Optional[compute.MavenLibrary] = None
    """Specification of a maven library to be installed."""

    notebook: Optional[NotebookLibrary] = None
    """The path to a notebook that defines a pipeline and is stored in the Databricks workspace."""

    whl: Optional[str] = None
    """URI of the whl to be installed."""

    def as_dict(self) -> dict:
        """Serializes the PipelineLibrary into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file:
            body["file"] = self.file.as_dict()
        if self.glob:
            body["glob"] = self.glob.as_dict()
        if self.jar is not None:
            body["jar"] = self.jar
        if self.maven:
            body["maven"] = self.maven.as_dict()
        if self.notebook:
            body["notebook"] = self.notebook.as_dict()
        if self.whl is not None:
            body["whl"] = self.whl
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineLibrary into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file:
            body["file"] = self.file
        if self.glob:
            body["glob"] = self.glob
        if self.jar is not None:
            body["jar"] = self.jar
        if self.maven:
            body["maven"] = self.maven
        if self.notebook:
            body["notebook"] = self.notebook
        if self.whl is not None:
            body["whl"] = self.whl
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineLibrary:
        """Deserializes the PipelineLibrary from a dictionary."""
        return cls(
            file=_from_dict(d, "file", FileLibrary),
            glob=_from_dict(d, "glob", PathPattern),
            jar=d.get("jar", None),
            maven=_from_dict(d, "maven", compute.MavenLibrary),
            notebook=_from_dict(d, "notebook", NotebookLibrary),
            whl=d.get("whl", None),
        )


@dataclass
class PipelinePermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[PipelinePermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the PipelinePermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelinePermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelinePermission:
        """Deserializes the PipelinePermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", PipelinePermissionLevel),
        )


class PipelinePermissionLevel(Enum):
    """Permission level"""

    CAN_MANAGE = "CAN_MANAGE"
    CAN_RUN = "CAN_RUN"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"


@dataclass
class PipelinePermissions:
    access_control_list: Optional[List[PipelineAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the PipelinePermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelinePermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelinePermissions:
        """Deserializes the PipelinePermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", PipelineAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class PipelinePermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[PipelinePermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the PipelinePermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelinePermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelinePermissionsDescription:
        """Deserializes the PipelinePermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None),
            permission_level=_enum(d, "permission_level", PipelinePermissionLevel),
        )


@dataclass
class PipelineSpec:
    budget_policy_id: Optional[str] = None
    """Budget policy of this pipeline."""

    catalog: Optional[str] = None
    """A catalog in Unity Catalog to publish data from this pipeline to. If `target` is specified,
    tables in this pipeline are published to a `target` schema inside `catalog` (for example,
    `catalog`.`target`.`table`). If `target` is not specified, no data is published to Unity
    Catalog."""

    channel: Optional[str] = None
    """DLT Release Channel that specifies which version to use."""

    clusters: Optional[List[PipelineCluster]] = None
    """Cluster settings for this pipeline deployment."""

    configuration: Optional[Dict[str, str]] = None
    """String-String configuration for this pipeline execution."""

    continuous: Optional[bool] = None
    """Whether the pipeline is continuous or triggered. This replaces `trigger`."""

    deployment: Optional[PipelineDeployment] = None
    """Deployment type of this pipeline."""

    development: Optional[bool] = None
    """Whether the pipeline is in Development mode. Defaults to false."""

    edition: Optional[str] = None
    """Pipeline product edition."""

    environment: Optional[PipelinesEnvironment] = None
    """Environment specification for this pipeline used to install dependencies."""

    event_log: Optional[EventLogSpec] = None
    """Event log configuration for this pipeline"""

    filters: Optional[Filters] = None
    """Filters on which Pipeline packages to include in the deployed graph."""

    gateway_definition: Optional[IngestionGatewayPipelineDefinition] = None
    """The definition of a gateway pipeline to support change data capture."""

    id: Optional[str] = None
    """Unique identifier for this pipeline."""

    ingestion_definition: Optional[IngestionPipelineDefinition] = None
    """The configuration for a managed ingestion pipeline. These settings cannot be used with the
    'libraries', 'schema', 'target', or 'catalog' settings."""

    libraries: Optional[List[PipelineLibrary]] = None
    """Libraries or code needed by this deployment."""

    name: Optional[str] = None
    """Friendly identifier for this pipeline."""

    notifications: Optional[List[Notifications]] = None
    """List of notification settings for this pipeline."""

    photon: Optional[bool] = None
    """Whether Photon is enabled for this pipeline."""

    restart_window: Optional[RestartWindow] = None
    """Restart window of this pipeline."""

    root_path: Optional[str] = None
    """Root path for this pipeline. This is used as the root directory when editing the pipeline in the
    Databricks user interface and it is added to sys.path when executing Python sources during
    pipeline execution."""

    schema: Optional[str] = None
    """The default schema (database) where tables are read from or published to."""

    serverless: Optional[bool] = None
    """Whether serverless compute is enabled for this pipeline."""

    storage: Optional[str] = None
    """DBFS root directory for storing checkpoints and tables."""

    tags: Optional[Dict[str, str]] = None
    """A map of tags associated with the pipeline. These are forwarded to the cluster as cluster tags,
    and are therefore subject to the same limitations. A maximum of 25 tags can be added to the
    pipeline."""

    target: Optional[str] = None
    """Target schema (database) to add tables in this pipeline to. Exactly one of `schema` or `target`
    must be specified. To publish to Unity Catalog, also specify `catalog`. This legacy field is
    deprecated for pipeline creation in favor of the `schema` field."""

    trigger: Optional[PipelineTrigger] = None
    """Which pipeline trigger to use. Deprecated: Use `continuous` instead."""

    usage_policy_id: Optional[str] = None
    """Usage policy of this pipeline."""

    def as_dict(self) -> dict:
        """Serializes the PipelineSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.channel is not None:
            body["channel"] = self.channel
        if self.clusters:
            body["clusters"] = [v.as_dict() for v in self.clusters]
        if self.configuration:
            body["configuration"] = self.configuration
        if self.continuous is not None:
            body["continuous"] = self.continuous
        if self.deployment:
            body["deployment"] = self.deployment.as_dict()
        if self.development is not None:
            body["development"] = self.development
        if self.edition is not None:
            body["edition"] = self.edition
        if self.environment:
            body["environment"] = self.environment.as_dict()
        if self.event_log:
            body["event_log"] = self.event_log.as_dict()
        if self.filters:
            body["filters"] = self.filters.as_dict()
        if self.gateway_definition:
            body["gateway_definition"] = self.gateway_definition.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.ingestion_definition:
            body["ingestion_definition"] = self.ingestion_definition.as_dict()
        if self.libraries:
            body["libraries"] = [v.as_dict() for v in self.libraries]
        if self.name is not None:
            body["name"] = self.name
        if self.notifications:
            body["notifications"] = [v.as_dict() for v in self.notifications]
        if self.photon is not None:
            body["photon"] = self.photon
        if self.restart_window:
            body["restart_window"] = self.restart_window.as_dict()
        if self.root_path is not None:
            body["root_path"] = self.root_path
        if self.schema is not None:
            body["schema"] = self.schema
        if self.serverless is not None:
            body["serverless"] = self.serverless
        if self.storage is not None:
            body["storage"] = self.storage
        if self.tags:
            body["tags"] = self.tags
        if self.target is not None:
            body["target"] = self.target
        if self.trigger:
            body["trigger"] = self.trigger.as_dict()
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.channel is not None:
            body["channel"] = self.channel
        if self.clusters:
            body["clusters"] = self.clusters
        if self.configuration:
            body["configuration"] = self.configuration
        if self.continuous is not None:
            body["continuous"] = self.continuous
        if self.deployment:
            body["deployment"] = self.deployment
        if self.development is not None:
            body["development"] = self.development
        if self.edition is not None:
            body["edition"] = self.edition
        if self.environment:
            body["environment"] = self.environment
        if self.event_log:
            body["event_log"] = self.event_log
        if self.filters:
            body["filters"] = self.filters
        if self.gateway_definition:
            body["gateway_definition"] = self.gateway_definition
        if self.id is not None:
            body["id"] = self.id
        if self.ingestion_definition:
            body["ingestion_definition"] = self.ingestion_definition
        if self.libraries:
            body["libraries"] = self.libraries
        if self.name is not None:
            body["name"] = self.name
        if self.notifications:
            body["notifications"] = self.notifications
        if self.photon is not None:
            body["photon"] = self.photon
        if self.restart_window:
            body["restart_window"] = self.restart_window
        if self.root_path is not None:
            body["root_path"] = self.root_path
        if self.schema is not None:
            body["schema"] = self.schema
        if self.serverless is not None:
            body["serverless"] = self.serverless
        if self.storage is not None:
            body["storage"] = self.storage
        if self.tags:
            body["tags"] = self.tags
        if self.target is not None:
            body["target"] = self.target
        if self.trigger:
            body["trigger"] = self.trigger
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineSpec:
        """Deserializes the PipelineSpec from a dictionary."""
        return cls(
            budget_policy_id=d.get("budget_policy_id", None),
            catalog=d.get("catalog", None),
            channel=d.get("channel", None),
            clusters=_repeated_dict(d, "clusters", PipelineCluster),
            configuration=d.get("configuration", None),
            continuous=d.get("continuous", None),
            deployment=_from_dict(d, "deployment", PipelineDeployment),
            development=d.get("development", None),
            edition=d.get("edition", None),
            environment=_from_dict(d, "environment", PipelinesEnvironment),
            event_log=_from_dict(d, "event_log", EventLogSpec),
            filters=_from_dict(d, "filters", Filters),
            gateway_definition=_from_dict(d, "gateway_definition", IngestionGatewayPipelineDefinition),
            id=d.get("id", None),
            ingestion_definition=_from_dict(d, "ingestion_definition", IngestionPipelineDefinition),
            libraries=_repeated_dict(d, "libraries", PipelineLibrary),
            name=d.get("name", None),
            notifications=_repeated_dict(d, "notifications", Notifications),
            photon=d.get("photon", None),
            restart_window=_from_dict(d, "restart_window", RestartWindow),
            root_path=d.get("root_path", None),
            schema=d.get("schema", None),
            serverless=d.get("serverless", None),
            storage=d.get("storage", None),
            tags=d.get("tags", None),
            target=d.get("target", None),
            trigger=_from_dict(d, "trigger", PipelineTrigger),
            usage_policy_id=d.get("usage_policy_id", None),
        )


class PipelineState(Enum):
    """The pipeline state."""

    DELETED = "DELETED"
    DEPLOYING = "DEPLOYING"
    FAILED = "FAILED"
    IDLE = "IDLE"
    RECOVERING = "RECOVERING"
    RESETTING = "RESETTING"
    RUNNING = "RUNNING"
    STARTING = "STARTING"
    STOPPING = "STOPPING"


@dataclass
class PipelineStateInfo:
    cluster_id: Optional[str] = None
    """The unique identifier of the cluster running the pipeline."""

    creator_user_name: Optional[str] = None
    """The username of the pipeline creator."""

    health: Optional[PipelineStateInfoHealth] = None
    """The health of a pipeline."""

    latest_updates: Optional[List[UpdateStateInfo]] = None
    """Status of the latest updates for the pipeline. Ordered with the newest update first."""

    name: Optional[str] = None
    """The user-friendly name of the pipeline."""

    pipeline_id: Optional[str] = None
    """The unique identifier of the pipeline."""

    run_as_user_name: Optional[str] = None
    """The username that the pipeline runs as. This is a read only value derived from the pipeline
    owner."""

    state: Optional[PipelineState] = None

    def as_dict(self) -> dict:
        """Serializes the PipelineStateInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.health is not None:
            body["health"] = self.health.value
        if self.latest_updates:
            body["latest_updates"] = [v.as_dict() for v in self.latest_updates]
        if self.name is not None:
            body["name"] = self.name
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineStateInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.health is not None:
            body["health"] = self.health
        if self.latest_updates:
            body["latest_updates"] = self.latest_updates
        if self.name is not None:
            body["name"] = self.name
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineStateInfo:
        """Deserializes the PipelineStateInfo from a dictionary."""
        return cls(
            cluster_id=d.get("cluster_id", None),
            creator_user_name=d.get("creator_user_name", None),
            health=_enum(d, "health", PipelineStateInfoHealth),
            latest_updates=_repeated_dict(d, "latest_updates", UpdateStateInfo),
            name=d.get("name", None),
            pipeline_id=d.get("pipeline_id", None),
            run_as_user_name=d.get("run_as_user_name", None),
            state=_enum(d, "state", PipelineState),
        )


class PipelineStateInfoHealth(Enum):
    """The health of a pipeline."""

    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"


@dataclass
class PipelineTrigger:
    cron: Optional[CronTrigger] = None

    manual: Optional[ManualTrigger] = None

    def as_dict(self) -> dict:
        """Serializes the PipelineTrigger into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cron:
            body["cron"] = self.cron.as_dict()
        if self.manual:
            body["manual"] = self.manual.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineTrigger into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cron:
            body["cron"] = self.cron
        if self.manual:
            body["manual"] = self.manual
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineTrigger:
        """Deserializes the PipelineTrigger from a dictionary."""
        return cls(cron=_from_dict(d, "cron", CronTrigger), manual=_from_dict(d, "manual", ManualTrigger))


@dataclass
class PipelinesEnvironment:
    """The environment entity used to preserve serverless environment side panel, jobs' environment for
    non-notebook task, and DLT's environment for classic and serverless pipelines. In this minimal
    environment spec, only pip dependencies are supported."""

    dependencies: Optional[List[str]] = None
    """List of pip dependencies, as supported by the version of pip in this environment. Each
    dependency is a pip requirement file line
    https://pip.pypa.io/en/stable/reference/requirements-file-format/ Allowed dependency could be
    <requirement specifier>, <archive url/path>, <local project path>(WSFS or Volumes in
    Databricks), <vcs project url>"""

    def as_dict(self) -> dict:
        """Serializes the PipelinesEnvironment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dependencies:
            body["dependencies"] = [v for v in self.dependencies]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelinesEnvironment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dependencies:
            body["dependencies"] = self.dependencies
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelinesEnvironment:
        """Deserializes the PipelinesEnvironment from a dictionary."""
        return cls(dependencies=d.get("dependencies", None))


@dataclass
class PostgresCatalogConfig:
    """PG-specific catalog-level configuration parameters"""

    slot_config: Optional[PostgresSlotConfig] = None
    """Optional. The Postgres slot configuration to use for logical replication"""

    def as_dict(self) -> dict:
        """Serializes the PostgresCatalogConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.slot_config:
            body["slot_config"] = self.slot_config.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PostgresCatalogConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.slot_config:
            body["slot_config"] = self.slot_config
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PostgresCatalogConfig:
        """Deserializes the PostgresCatalogConfig from a dictionary."""
        return cls(slot_config=_from_dict(d, "slot_config", PostgresSlotConfig))


@dataclass
class PostgresSlotConfig:
    """PostgresSlotConfig contains the configuration for a Postgres logical replication slot"""

    publication_name: Optional[str] = None
    """The name of the publication to use for the Postgres source"""

    slot_name: Optional[str] = None
    """The name of the logical replication slot to use for the Postgres source"""

    def as_dict(self) -> dict:
        """Serializes the PostgresSlotConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.publication_name is not None:
            body["publication_name"] = self.publication_name
        if self.slot_name is not None:
            body["slot_name"] = self.slot_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PostgresSlotConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.publication_name is not None:
            body["publication_name"] = self.publication_name
        if self.slot_name is not None:
            body["slot_name"] = self.slot_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PostgresSlotConfig:
        """Deserializes the PostgresSlotConfig from a dictionary."""
        return cls(publication_name=d.get("publication_name", None), slot_name=d.get("slot_name", None))


class PublishingMode(Enum):
    """Enum representing the publishing mode of a pipeline."""

    DEFAULT_PUBLISHING_MODE = "DEFAULT_PUBLISHING_MODE"
    LEGACY_PUBLISHING_MODE = "LEGACY_PUBLISHING_MODE"


@dataclass
class ReplaceWhereOverride:
    """Specifies a replace_where predicate override for a replace where flow."""

    flow_name: Optional[str] = None
    """Name of the flow to apply this override to."""

    predicate_override: Optional[str] = None
    """SQL predicate string to use as replace_where condition. Example: `date = '2024-10-10' AND city =
    'xyz'`"""

    def as_dict(self) -> dict:
        """Serializes the ReplaceWhereOverride into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.flow_name is not None:
            body["flow_name"] = self.flow_name
        if self.predicate_override is not None:
            body["predicate_override"] = self.predicate_override
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ReplaceWhereOverride into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.flow_name is not None:
            body["flow_name"] = self.flow_name
        if self.predicate_override is not None:
            body["predicate_override"] = self.predicate_override
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ReplaceWhereOverride:
        """Deserializes the ReplaceWhereOverride from a dictionary."""
        return cls(flow_name=d.get("flow_name", None), predicate_override=d.get("predicate_override", None))


@dataclass
class ReportSpec:
    source_url: str
    """Required. Report URL in the source system."""

    destination_catalog: str
    """Required. Destination catalog to store table."""

    destination_schema: str
    """Required. Destination schema to store table."""

    destination_table: Optional[str] = None
    """Required. Destination table name. The pipeline fails if a table with that name already exists."""

    table_configuration: Optional[TableSpecificConfig] = None
    """Configuration settings to control the ingestion of tables. These settings override the
    table_configuration defined in the IngestionPipelineDefinition object."""

    def as_dict(self) -> dict:
        """Serializes the ReportSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_catalog is not None:
            body["destination_catalog"] = self.destination_catalog
        if self.destination_schema is not None:
            body["destination_schema"] = self.destination_schema
        if self.destination_table is not None:
            body["destination_table"] = self.destination_table
        if self.source_url is not None:
            body["source_url"] = self.source_url
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ReportSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_catalog is not None:
            body["destination_catalog"] = self.destination_catalog
        if self.destination_schema is not None:
            body["destination_schema"] = self.destination_schema
        if self.destination_table is not None:
            body["destination_table"] = self.destination_table
        if self.source_url is not None:
            body["source_url"] = self.source_url
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ReportSpec:
        """Deserializes the ReportSpec from a dictionary."""
        return cls(
            destination_catalog=d.get("destination_catalog", None),
            destination_schema=d.get("destination_schema", None),
            destination_table=d.get("destination_table", None),
            source_url=d.get("source_url", None),
            table_configuration=_from_dict(d, "table_configuration", TableSpecificConfig),
        )


@dataclass
class RestartWindow:
    start_hour: int
    """An integer between 0 and 23 denoting the start hour for the restart window in the 24-hour day.
    Continuous pipeline restart is triggered only within a five-hour window starting at this hour."""

    days_of_week: Optional[List[DayOfWeek]] = None
    """Days of week in which the restart is allowed to happen (within a five-hour window starting at
    start_hour). If not specified all days of the week will be used."""

    time_zone_id: Optional[str] = None
    """Time zone id of restart window. See
    https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-conf-mgmt-set-timezone.html
    for details. If not specified, UTC will be used."""

    def as_dict(self) -> dict:
        """Serializes the RestartWindow into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.days_of_week:
            body["days_of_week"] = [v.value for v in self.days_of_week]
        if self.start_hour is not None:
            body["start_hour"] = self.start_hour
        if self.time_zone_id is not None:
            body["time_zone_id"] = self.time_zone_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RestartWindow into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.days_of_week:
            body["days_of_week"] = self.days_of_week
        if self.start_hour is not None:
            body["start_hour"] = self.start_hour
        if self.time_zone_id is not None:
            body["time_zone_id"] = self.time_zone_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RestartWindow:
        """Deserializes the RestartWindow from a dictionary."""
        return cls(
            days_of_week=_repeated_enum(d, "days_of_week", DayOfWeek),
            start_hour=d.get("start_hour", None),
            time_zone_id=d.get("time_zone_id", None),
        )


@dataclass
class RewindDatasetSpec:
    """Configuration for rewinding a specific dataset."""

    cascade: Optional[bool] = None
    """Whether to cascade the rewind to dependent datasets. Must be specified."""

    identifier: Optional[str] = None
    """The identifier of the dataset (e.g., "main.foo.tbl1")."""

    reset_checkpoints: Optional[bool] = None
    """Whether to reset checkpoints for this dataset."""

    def as_dict(self) -> dict:
        """Serializes the RewindDatasetSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cascade is not None:
            body["cascade"] = self.cascade
        if self.identifier is not None:
            body["identifier"] = self.identifier
        if self.reset_checkpoints is not None:
            body["reset_checkpoints"] = self.reset_checkpoints
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RewindDatasetSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cascade is not None:
            body["cascade"] = self.cascade
        if self.identifier is not None:
            body["identifier"] = self.identifier
        if self.reset_checkpoints is not None:
            body["reset_checkpoints"] = self.reset_checkpoints
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RewindDatasetSpec:
        """Deserializes the RewindDatasetSpec from a dictionary."""
        return cls(
            cascade=d.get("cascade", None),
            identifier=d.get("identifier", None),
            reset_checkpoints=d.get("reset_checkpoints", None),
        )


@dataclass
class RewindSpec:
    """Information about a rewind being requested for this pipeline or some of the datasets in it."""

    datasets: Optional[List[RewindDatasetSpec]] = None
    """List of datasets to rewind with specific configuration for each. When not specified, all
    datasets will be rewound with cascade = true and reset_checkpoints = true."""

    dry_run: Optional[bool] = None
    """If true, this is a dry run and we should emit the RewindSummary but not perform the rewind."""

    rewind_timestamp: Optional[str] = None
    """The base timestamp to rewind to. Must be specified."""

    def as_dict(self) -> dict:
        """Serializes the RewindSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.datasets:
            body["datasets"] = [v.as_dict() for v in self.datasets]
        if self.dry_run is not None:
            body["dry_run"] = self.dry_run
        if self.rewind_timestamp is not None:
            body["rewind_timestamp"] = self.rewind_timestamp
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RewindSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.datasets:
            body["datasets"] = self.datasets
        if self.dry_run is not None:
            body["dry_run"] = self.dry_run
        if self.rewind_timestamp is not None:
            body["rewind_timestamp"] = self.rewind_timestamp
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RewindSpec:
        """Deserializes the RewindSpec from a dictionary."""
        return cls(
            datasets=_repeated_dict(d, "datasets", RewindDatasetSpec),
            dry_run=d.get("dry_run", None),
            rewind_timestamp=d.get("rewind_timestamp", None),
        )


@dataclass
class RunAs:
    """Write-only setting, available only in Create/Update calls. Specifies the user or service
    principal that the pipeline runs as. If not specified, the pipeline runs as the user who created
    the pipeline.

    Only `user_name` or `service_principal_name` can be specified. If both are specified, an error
    is thrown."""

    service_principal_name: Optional[str] = None
    """Application ID of an active service principal. Setting this field requires the
    `servicePrincipal/user` role."""

    user_name: Optional[str] = None
    """The email of an active workspace user. Users can only set this field to their own email."""

    def as_dict(self) -> dict:
        """Serializes the RunAs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunAs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunAs:
        """Deserializes the RunAs from a dictionary."""
        return cls(service_principal_name=d.get("service_principal_name", None), user_name=d.get("user_name", None))


@dataclass
class SchemaSpec:
    source_schema: str
    """Required. Schema name in the source database."""

    destination_catalog: str
    """Required. Destination catalog to store tables."""

    destination_schema: str
    """Required. Destination schema to store tables in. Tables with the same name as the source tables
    are created in this destination schema. The pipeline fails If a table with the same name already
    exists."""

    source_catalog: Optional[str] = None
    """The source catalog name. Might be optional depending on the type of source."""

    table_configuration: Optional[TableSpecificConfig] = None
    """Configuration settings to control the ingestion of tables. These settings are applied to all
    tables in this schema and override the table_configuration defined in the
    IngestionPipelineDefinition object."""

    def as_dict(self) -> dict:
        """Serializes the SchemaSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_catalog is not None:
            body["destination_catalog"] = self.destination_catalog
        if self.destination_schema is not None:
            body["destination_schema"] = self.destination_schema
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        if self.source_schema is not None:
            body["source_schema"] = self.source_schema
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SchemaSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_catalog is not None:
            body["destination_catalog"] = self.destination_catalog
        if self.destination_schema is not None:
            body["destination_schema"] = self.destination_schema
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        if self.source_schema is not None:
            body["source_schema"] = self.source_schema
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SchemaSpec:
        """Deserializes the SchemaSpec from a dictionary."""
        return cls(
            destination_catalog=d.get("destination_catalog", None),
            destination_schema=d.get("destination_schema", None),
            source_catalog=d.get("source_catalog", None),
            source_schema=d.get("source_schema", None),
            table_configuration=_from_dict(d, "table_configuration", TableSpecificConfig),
        )


@dataclass
class Sequencing:
    control_plane_seq_no: Optional[int] = None
    """A sequence number, unique and increasing per pipeline."""

    data_plane_id: Optional[DataPlaneId] = None
    """the ID assigned by the data plane."""

    def as_dict(self) -> dict:
        """Serializes the Sequencing into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.control_plane_seq_no is not None:
            body["control_plane_seq_no"] = self.control_plane_seq_no
        if self.data_plane_id:
            body["data_plane_id"] = self.data_plane_id.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Sequencing into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.control_plane_seq_no is not None:
            body["control_plane_seq_no"] = self.control_plane_seq_no
        if self.data_plane_id:
            body["data_plane_id"] = self.data_plane_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Sequencing:
        """Deserializes the Sequencing from a dictionary."""
        return cls(
            control_plane_seq_no=d.get("control_plane_seq_no", None),
            data_plane_id=_from_dict(d, "data_plane_id", DataPlaneId),
        )


@dataclass
class SerializedException:
    class_name: Optional[str] = None
    """Runtime class of the exception"""

    message: Optional[str] = None
    """Exception message"""

    stack: Optional[List[StackFrame]] = None
    """Stack trace consisting of a list of stack frames"""

    def as_dict(self) -> dict:
        """Serializes the SerializedException into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.class_name is not None:
            body["class_name"] = self.class_name
        if self.message is not None:
            body["message"] = self.message
        if self.stack:
            body["stack"] = [v.as_dict() for v in self.stack]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SerializedException into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.class_name is not None:
            body["class_name"] = self.class_name
        if self.message is not None:
            body["message"] = self.message
        if self.stack:
            body["stack"] = self.stack
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SerializedException:
        """Deserializes the SerializedException from a dictionary."""
        return cls(
            class_name=d.get("class_name", None),
            message=d.get("message", None),
            stack=_repeated_dict(d, "stack", StackFrame),
        )


@dataclass
class SourceCatalogConfig:
    """SourceCatalogConfig contains catalog-level custom configuration parameters for each source"""

    postgres: Optional[PostgresCatalogConfig] = None
    """Postgres-specific catalog-level configuration parameters"""

    source_catalog: Optional[str] = None
    """Source catalog name"""

    def as_dict(self) -> dict:
        """Serializes the SourceCatalogConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.postgres:
            body["postgres"] = self.postgres.as_dict()
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SourceCatalogConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.postgres:
            body["postgres"] = self.postgres
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SourceCatalogConfig:
        """Deserializes the SourceCatalogConfig from a dictionary."""
        return cls(
            postgres=_from_dict(d, "postgres", PostgresCatalogConfig), source_catalog=d.get("source_catalog", None)
        )


@dataclass
class SourceConfig:
    catalog: Optional[SourceCatalogConfig] = None
    """Catalog-level source configuration parameters"""

    def as_dict(self) -> dict:
        """Serializes the SourceConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog:
            body["catalog"] = self.catalog.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SourceConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog:
            body["catalog"] = self.catalog
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SourceConfig:
        """Deserializes the SourceConfig from a dictionary."""
        return cls(catalog=_from_dict(d, "catalog", SourceCatalogConfig))


@dataclass
class StackFrame:
    declaring_class: Optional[str] = None
    """Class from which the method call originated"""

    file_name: Optional[str] = None
    """File where the method is defined"""

    line_number: Optional[int] = None
    """Line from which the method was called"""

    method_name: Optional[str] = None
    """Name of the method which was called"""

    def as_dict(self) -> dict:
        """Serializes the StackFrame into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.declaring_class is not None:
            body["declaring_class"] = self.declaring_class
        if self.file_name is not None:
            body["file_name"] = self.file_name
        if self.line_number is not None:
            body["line_number"] = self.line_number
        if self.method_name is not None:
            body["method_name"] = self.method_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StackFrame into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.declaring_class is not None:
            body["declaring_class"] = self.declaring_class
        if self.file_name is not None:
            body["file_name"] = self.file_name
        if self.line_number is not None:
            body["line_number"] = self.line_number
        if self.method_name is not None:
            body["method_name"] = self.method_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StackFrame:
        """Deserializes the StackFrame from a dictionary."""
        return cls(
            declaring_class=d.get("declaring_class", None),
            file_name=d.get("file_name", None),
            line_number=d.get("line_number", None),
            method_name=d.get("method_name", None),
        )


class StartUpdateCause(Enum):
    """What triggered this update."""

    API_CALL = "API_CALL"
    INFRASTRUCTURE_MAINTENANCE = "INFRASTRUCTURE_MAINTENANCE"
    JOB_TASK = "JOB_TASK"
    RETRY_ON_FAILURE = "RETRY_ON_FAILURE"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"
    SERVICE_UPGRADE = "SERVICE_UPGRADE"
    USER_ACTION = "USER_ACTION"


@dataclass
class StartUpdateResponse:
    update_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the StartUpdateResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.update_id is not None:
            body["update_id"] = self.update_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StartUpdateResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.update_id is not None:
            body["update_id"] = self.update_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StartUpdateResponse:
        """Deserializes the StartUpdateResponse from a dictionary."""
        return cls(update_id=d.get("update_id", None))


@dataclass
class StopPipelineResponse:
    def as_dict(self) -> dict:
        """Serializes the StopPipelineResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the StopPipelineResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StopPipelineResponse:
        """Deserializes the StopPipelineResponse from a dictionary."""
        return cls()


@dataclass
class TableSpec:
    source_table: str
    """Required. Table name in the source database."""

    destination_catalog: str
    """Required. Destination catalog to store table."""

    destination_schema: str
    """Required. Destination schema to store table."""

    destination_table: Optional[str] = None
    """Optional. Destination table name. The pipeline fails if a table with that name already exists.
    If not set, the source table name is used."""

    source_catalog: Optional[str] = None
    """Source catalog name. Might be optional depending on the type of source."""

    source_schema: Optional[str] = None
    """Schema name in the source database. Might be optional depending on the type of source."""

    table_configuration: Optional[TableSpecificConfig] = None
    """Configuration settings to control the ingestion of tables. These settings override the
    table_configuration defined in the IngestionPipelineDefinition object and the SchemaSpec."""

    def as_dict(self) -> dict:
        """Serializes the TableSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_catalog is not None:
            body["destination_catalog"] = self.destination_catalog
        if self.destination_schema is not None:
            body["destination_schema"] = self.destination_schema
        if self.destination_table is not None:
            body["destination_table"] = self.destination_table
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        if self.source_schema is not None:
            body["source_schema"] = self.source_schema
        if self.source_table is not None:
            body["source_table"] = self.source_table
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_catalog is not None:
            body["destination_catalog"] = self.destination_catalog
        if self.destination_schema is not None:
            body["destination_schema"] = self.destination_schema
        if self.destination_table is not None:
            body["destination_table"] = self.destination_table
        if self.source_catalog is not None:
            body["source_catalog"] = self.source_catalog
        if self.source_schema is not None:
            body["source_schema"] = self.source_schema
        if self.source_table is not None:
            body["source_table"] = self.source_table
        if self.table_configuration:
            body["table_configuration"] = self.table_configuration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableSpec:
        """Deserializes the TableSpec from a dictionary."""
        return cls(
            destination_catalog=d.get("destination_catalog", None),
            destination_schema=d.get("destination_schema", None),
            destination_table=d.get("destination_table", None),
            source_catalog=d.get("source_catalog", None),
            source_schema=d.get("source_schema", None),
            source_table=d.get("source_table", None),
            table_configuration=_from_dict(d, "table_configuration", TableSpecificConfig),
        )


@dataclass
class TableSpecificConfig:
    auto_full_refresh_policy: Optional[AutoFullRefreshPolicy] = None
    """(Optional, Mutable) Policy for auto full refresh, if enabled pipeline will automatically try to
    fix issues by doing a full refresh on the table in the retry run. auto_full_refresh_policy in
    table configuration will override the above level auto_full_refresh_policy. For example, {
    "auto_full_refresh_policy": { "enabled": true, "min_interval_hours": 23, } } If unspecified,
    auto full refresh is disabled."""

    exclude_columns: Optional[List[str]] = None
    """A list of column names to be excluded for the ingestion. When not specified, include_columns
    fully controls what columns to be ingested. When specified, all other columns including future
    ones will be automatically included for ingestion. This field in mutually exclusive with
    `include_columns`."""

    include_columns: Optional[List[str]] = None
    """A list of column names to be included for the ingestion. When not specified, all columns except
    ones in exclude_columns will be included. Future columns will be automatically included. When
    specified, all other future columns will be automatically excluded from ingestion. This field in
    mutually exclusive with `exclude_columns`."""

    primary_keys: Optional[List[str]] = None
    """The primary key of the table used to apply changes."""

    query_based_connector_config: Optional[IngestionPipelineDefinitionTableSpecificConfigQueryBasedConnectorConfig] = (
        None
    )

    row_filter: Optional[str] = None
    """(Optional, Immutable) The row filter condition to be applied to the table. It must not contain
    the WHERE keyword, only the actual filter condition. It must be in DBSQL format."""

    salesforce_include_formula_fields: Optional[bool] = None
    """If true, formula fields defined in the table are included in the ingestion. This setting is only
    valid for the Salesforce connector"""

    scd_type: Optional[TableSpecificConfigScdType] = None
    """The SCD type to use to ingest the table."""

    sequence_by: Optional[List[str]] = None
    """The column names specifying the logical order of events in the source data. Spark Declarative
    Pipelines uses this sequencing to handle change events that arrive out of order."""

    workday_report_parameters: Optional[IngestionPipelineDefinitionWorkdayReportParameters] = None
    """(Optional) Additional custom parameters for Workday Report"""

    def as_dict(self) -> dict:
        """Serializes the TableSpecificConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.auto_full_refresh_policy:
            body["auto_full_refresh_policy"] = self.auto_full_refresh_policy.as_dict()
        if self.exclude_columns:
            body["exclude_columns"] = [v for v in self.exclude_columns]
        if self.include_columns:
            body["include_columns"] = [v for v in self.include_columns]
        if self.primary_keys:
            body["primary_keys"] = [v for v in self.primary_keys]
        if self.query_based_connector_config:
            body["query_based_connector_config"] = self.query_based_connector_config.as_dict()
        if self.row_filter is not None:
            body["row_filter"] = self.row_filter
        if self.salesforce_include_formula_fields is not None:
            body["salesforce_include_formula_fields"] = self.salesforce_include_formula_fields
        if self.scd_type is not None:
            body["scd_type"] = self.scd_type.value
        if self.sequence_by:
            body["sequence_by"] = [v for v in self.sequence_by]
        if self.workday_report_parameters:
            body["workday_report_parameters"] = self.workday_report_parameters.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableSpecificConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.auto_full_refresh_policy:
            body["auto_full_refresh_policy"] = self.auto_full_refresh_policy
        if self.exclude_columns:
            body["exclude_columns"] = self.exclude_columns
        if self.include_columns:
            body["include_columns"] = self.include_columns
        if self.primary_keys:
            body["primary_keys"] = self.primary_keys
        if self.query_based_connector_config:
            body["query_based_connector_config"] = self.query_based_connector_config
        if self.row_filter is not None:
            body["row_filter"] = self.row_filter
        if self.salesforce_include_formula_fields is not None:
            body["salesforce_include_formula_fields"] = self.salesforce_include_formula_fields
        if self.scd_type is not None:
            body["scd_type"] = self.scd_type
        if self.sequence_by:
            body["sequence_by"] = self.sequence_by
        if self.workday_report_parameters:
            body["workday_report_parameters"] = self.workday_report_parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableSpecificConfig:
        """Deserializes the TableSpecificConfig from a dictionary."""
        return cls(
            auto_full_refresh_policy=_from_dict(d, "auto_full_refresh_policy", AutoFullRefreshPolicy),
            exclude_columns=d.get("exclude_columns", None),
            include_columns=d.get("include_columns", None),
            primary_keys=d.get("primary_keys", None),
            query_based_connector_config=_from_dict(
                d,
                "query_based_connector_config",
                IngestionPipelineDefinitionTableSpecificConfigQueryBasedConnectorConfig,
            ),
            row_filter=d.get("row_filter", None),
            salesforce_include_formula_fields=d.get("salesforce_include_formula_fields", None),
            scd_type=_enum(d, "scd_type", TableSpecificConfigScdType),
            sequence_by=d.get("sequence_by", None),
            workday_report_parameters=_from_dict(
                d, "workday_report_parameters", IngestionPipelineDefinitionWorkdayReportParameters
            ),
        )


class TableSpecificConfigScdType(Enum):
    """The SCD type to use to ingest the table."""

    APPEND_ONLY = "APPEND_ONLY"
    SCD_TYPE_1 = "SCD_TYPE_1"
    SCD_TYPE_2 = "SCD_TYPE_2"


@dataclass
class Truncation:
    """Information about truncations applied to this event."""

    truncated_fields: Optional[List[TruncationTruncationDetail]] = None
    """List of fields that were truncated from this event. If empty or absent, no truncation occurred."""

    def as_dict(self) -> dict:
        """Serializes the Truncation into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.truncated_fields:
            body["truncated_fields"] = [v.as_dict() for v in self.truncated_fields]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Truncation into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.truncated_fields:
            body["truncated_fields"] = self.truncated_fields
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Truncation:
        """Deserializes the Truncation from a dictionary."""
        return cls(truncated_fields=_repeated_dict(d, "truncated_fields", TruncationTruncationDetail))


@dataclass
class TruncationTruncationDetail:
    """Details about a specific field that was truncated."""

    field_name: Optional[str] = None
    """The name of the truncated field (e.g., "error"). Corresponds to field names in PipelineEvent."""

    def as_dict(self) -> dict:
        """Serializes the TruncationTruncationDetail into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.field_name is not None:
            body["field_name"] = self.field_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TruncationTruncationDetail into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.field_name is not None:
            body["field_name"] = self.field_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TruncationTruncationDetail:
        """Deserializes the TruncationTruncationDetail from a dictionary."""
        return cls(field_name=d.get("field_name", None))


@dataclass
class UpdateInfo:
    cause: Optional[UpdateInfoCause] = None
    """What triggered this update."""

    cluster_id: Optional[str] = None
    """The ID of the cluster that the update is running on."""

    config: Optional[PipelineSpec] = None
    """The pipeline configuration with system defaults applied where unspecified by the user. Not
    returned by ListUpdates."""

    creation_time: Optional[int] = None
    """The time when this update was created."""

    full_refresh: Optional[bool] = None
    """If true, this update will reset all tables before running."""

    full_refresh_selection: Optional[List[str]] = None
    """A list of tables to update with fullRefresh. If both refresh_selection and
    full_refresh_selection are empty, this is a full graph update. Full Refresh on a table means
    that the states of the table will be reset before the refresh."""

    parameters: Optional[Dict[str, str]] = None
    """Key/value map of parameters used to initiate the update"""

    pipeline_id: Optional[str] = None
    """The ID of the pipeline."""

    refresh_selection: Optional[List[str]] = None
    """A list of tables to update without fullRefresh. If both refresh_selection and
    full_refresh_selection are empty, this is a full graph update. Full Refresh on a table means
    that the states of the table will be reset before the refresh."""

    state: Optional[UpdateInfoState] = None
    """The update state."""

    update_id: Optional[str] = None
    """The ID of this update."""

    validate_only: Optional[bool] = None
    """If true, this update only validates the correctness of pipeline source code but does not
    materialize or publish any datasets."""

    def as_dict(self) -> dict:
        """Serializes the UpdateInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cause is not None:
            body["cause"] = self.cause.value
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.config:
            body["config"] = self.config.as_dict()
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.full_refresh is not None:
            body["full_refresh"] = self.full_refresh
        if self.full_refresh_selection:
            body["full_refresh_selection"] = [v for v in self.full_refresh_selection]
        if self.parameters:
            body["parameters"] = self.parameters
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.refresh_selection:
            body["refresh_selection"] = [v for v in self.refresh_selection]
        if self.state is not None:
            body["state"] = self.state.value
        if self.update_id is not None:
            body["update_id"] = self.update_id
        if self.validate_only is not None:
            body["validate_only"] = self.validate_only
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cause is not None:
            body["cause"] = self.cause
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.config:
            body["config"] = self.config
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.full_refresh is not None:
            body["full_refresh"] = self.full_refresh
        if self.full_refresh_selection:
            body["full_refresh_selection"] = self.full_refresh_selection
        if self.parameters:
            body["parameters"] = self.parameters
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        if self.refresh_selection:
            body["refresh_selection"] = self.refresh_selection
        if self.state is not None:
            body["state"] = self.state
        if self.update_id is not None:
            body["update_id"] = self.update_id
        if self.validate_only is not None:
            body["validate_only"] = self.validate_only
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateInfo:
        """Deserializes the UpdateInfo from a dictionary."""
        return cls(
            cause=_enum(d, "cause", UpdateInfoCause),
            cluster_id=d.get("cluster_id", None),
            config=_from_dict(d, "config", PipelineSpec),
            creation_time=d.get("creation_time", None),
            full_refresh=d.get("full_refresh", None),
            full_refresh_selection=d.get("full_refresh_selection", None),
            parameters=d.get("parameters", None),
            pipeline_id=d.get("pipeline_id", None),
            refresh_selection=d.get("refresh_selection", None),
            state=_enum(d, "state", UpdateInfoState),
            update_id=d.get("update_id", None),
            validate_only=d.get("validate_only", None),
        )


class UpdateInfoCause(Enum):
    """What triggered this update."""

    API_CALL = "API_CALL"
    INFRASTRUCTURE_MAINTENANCE = "INFRASTRUCTURE_MAINTENANCE"
    JOB_TASK = "JOB_TASK"
    RETRY_ON_FAILURE = "RETRY_ON_FAILURE"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"
    SERVICE_UPGRADE = "SERVICE_UPGRADE"
    USER_ACTION = "USER_ACTION"


class UpdateInfoState(Enum):
    """The update state."""

    CANCELED = "CANCELED"
    COMPLETED = "COMPLETED"
    CREATED = "CREATED"
    FAILED = "FAILED"
    INITIALIZING = "INITIALIZING"
    QUEUED = "QUEUED"
    RESETTING = "RESETTING"
    RUNNING = "RUNNING"
    SETTING_UP_TABLES = "SETTING_UP_TABLES"
    STOPPING = "STOPPING"
    WAITING_FOR_RESOURCES = "WAITING_FOR_RESOURCES"


@dataclass
class UpdateStateInfo:
    creation_time: Optional[str] = None

    state: Optional[UpdateStateInfoState] = None

    update_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateStateInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.state is not None:
            body["state"] = self.state.value
        if self.update_id is not None:
            body["update_id"] = self.update_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateStateInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.state is not None:
            body["state"] = self.state
        if self.update_id is not None:
            body["update_id"] = self.update_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateStateInfo:
        """Deserializes the UpdateStateInfo from a dictionary."""
        return cls(
            creation_time=d.get("creation_time", None),
            state=_enum(d, "state", UpdateStateInfoState),
            update_id=d.get("update_id", None),
        )


class UpdateStateInfoState(Enum):
    """The update state."""

    CANCELED = "CANCELED"
    COMPLETED = "COMPLETED"
    CREATED = "CREATED"
    FAILED = "FAILED"
    INITIALIZING = "INITIALIZING"
    QUEUED = "QUEUED"
    RESETTING = "RESETTING"
    RUNNING = "RUNNING"
    SETTING_UP_TABLES = "SETTING_UP_TABLES"
    STOPPING = "STOPPING"
    WAITING_FOR_RESOURCES = "WAITING_FOR_RESOURCES"


class PipelinesAPI:
    """The Lakeflow Spark Declarative Pipelines API allows you to create, edit, delete, start, and view details
    about pipelines.

    Spark Declarative Pipelines is a framework for building reliable, maintainable, and testable data
    processing pipelines. You define the transformations to perform on your data, and Spark Declarative
    Pipelines manages task orchestration, cluster management, monitoring, data quality, and error handling.

    Instead of defining your data pipelines using a series of separate Apache Spark tasks, Spark Declarative
    Pipelines manages how your data is transformed based on a target schema you define for each processing
    step. You can also enforce data quality with Spark Declarative Pipelines expectations. Expectations allow
    you to define expected data quality and specify how to handle records that fail those expectations."""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_pipeline_idle(
        self,
        pipeline_id: str,
        timeout=timedelta(minutes=20),
        callback: Optional[Callable[[GetPipelineResponse], None]] = None,
    ) -> GetPipelineResponse:
        deadline = time.time() + timeout.total_seconds()
        target_states = (PipelineState.IDLE,)
        failure_states = (PipelineState.FAILED,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get(pipeline_id=pipeline_id)
            status = poll.state
            status_message = poll.cause
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach IDLE, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"pipeline_id={pipeline_id}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def clone(
        self,
        pipeline_id: str,
        *,
        allow_duplicate_names: Optional[bool] = None,
        budget_policy_id: Optional[str] = None,
        catalog: Optional[str] = None,
        channel: Optional[str] = None,
        clone_mode: Optional[CloneMode] = None,
        clusters: Optional[List[PipelineCluster]] = None,
        configuration: Optional[Dict[str, str]] = None,
        continuous: Optional[bool] = None,
        deployment: Optional[PipelineDeployment] = None,
        development: Optional[bool] = None,
        edition: Optional[str] = None,
        environment: Optional[PipelinesEnvironment] = None,
        event_log: Optional[EventLogSpec] = None,
        expected_last_modified: Optional[int] = None,
        filters: Optional[Filters] = None,
        gateway_definition: Optional[IngestionGatewayPipelineDefinition] = None,
        id: Optional[str] = None,
        ingestion_definition: Optional[IngestionPipelineDefinition] = None,
        libraries: Optional[List[PipelineLibrary]] = None,
        name: Optional[str] = None,
        notifications: Optional[List[Notifications]] = None,
        photon: Optional[bool] = None,
        restart_window: Optional[RestartWindow] = None,
        root_path: Optional[str] = None,
        schema: Optional[str] = None,
        serverless: Optional[bool] = None,
        storage: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        target: Optional[str] = None,
        trigger: Optional[PipelineTrigger] = None,
        usage_policy_id: Optional[str] = None,
    ) -> ClonePipelineResponse:
        """Creates a new pipeline using Unity Catalog from a pipeline using Hive Metastore. This method returns
        the ID of the newly created clone. Additionally, this method starts an update for the newly created
        pipeline.

        :param pipeline_id: str
          Source pipeline to clone from
        :param allow_duplicate_names: bool (optional)
          If false, deployment will fail if name conflicts with that of another pipeline.
        :param budget_policy_id: str (optional)
          Budget policy of this pipeline.
        :param catalog: str (optional)
          A catalog in Unity Catalog to publish data from this pipeline to. If `target` is specified, tables
          in this pipeline are published to a `target` schema inside `catalog` (for example,
          `catalog`.`target`.`table`). If `target` is not specified, no data is published to Unity Catalog.
        :param channel: str (optional)
          DLT Release Channel that specifies which version to use.
        :param clone_mode: :class:`CloneMode` (optional)
          The type of clone to perform. Currently, only deep copies are supported
        :param clusters: List[:class:`PipelineCluster`] (optional)
          Cluster settings for this pipeline deployment.
        :param configuration: Dict[str,str] (optional)
          String-String configuration for this pipeline execution.
        :param continuous: bool (optional)
          Whether the pipeline is continuous or triggered. This replaces `trigger`.
        :param deployment: :class:`PipelineDeployment` (optional)
          Deployment type of this pipeline.
        :param development: bool (optional)
          Whether the pipeline is in Development mode. Defaults to false.
        :param edition: str (optional)
          Pipeline product edition.
        :param environment: :class:`PipelinesEnvironment` (optional)
          Environment specification for this pipeline used to install dependencies.
        :param event_log: :class:`EventLogSpec` (optional)
          Event log configuration for this pipeline
        :param expected_last_modified: int (optional)
          If present, the last-modified time of the pipeline settings before the clone. If the settings were
          modified after that time, then the request will fail with a conflict.
        :param filters: :class:`Filters` (optional)
          Filters on which Pipeline packages to include in the deployed graph.
        :param gateway_definition: :class:`IngestionGatewayPipelineDefinition` (optional)
          The definition of a gateway pipeline to support change data capture.
        :param id: str (optional)
          Unique identifier for this pipeline.
        :param ingestion_definition: :class:`IngestionPipelineDefinition` (optional)
          The configuration for a managed ingestion pipeline. These settings cannot be used with the
          'libraries', 'schema', 'target', or 'catalog' settings.
        :param libraries: List[:class:`PipelineLibrary`] (optional)
          Libraries or code needed by this deployment.
        :param name: str (optional)
          Friendly identifier for this pipeline.
        :param notifications: List[:class:`Notifications`] (optional)
          List of notification settings for this pipeline.
        :param photon: bool (optional)
          Whether Photon is enabled for this pipeline.
        :param restart_window: :class:`RestartWindow` (optional)
          Restart window of this pipeline.
        :param root_path: str (optional)
          Root path for this pipeline. This is used as the root directory when editing the pipeline in the
          Databricks user interface and it is added to sys.path when executing Python sources during pipeline
          execution.
        :param schema: str (optional)
          The default schema (database) where tables are read from or published to.
        :param serverless: bool (optional)
          Whether serverless compute is enabled for this pipeline.
        :param storage: str (optional)
          DBFS root directory for storing checkpoints and tables.
        :param tags: Dict[str,str] (optional)
          A map of tags associated with the pipeline. These are forwarded to the cluster as cluster tags, and
          are therefore subject to the same limitations. A maximum of 25 tags can be added to the pipeline.
        :param target: str (optional)
          Target schema (database) to add tables in this pipeline to. Exactly one of `schema` or `target` must
          be specified. To publish to Unity Catalog, also specify `catalog`. This legacy field is deprecated
          for pipeline creation in favor of the `schema` field.
        :param trigger: :class:`PipelineTrigger` (optional)
          Which pipeline trigger to use. Deprecated: Use `continuous` instead.
        :param usage_policy_id: str (optional)
          Usage policy of this pipeline.

        :returns: :class:`ClonePipelineResponse`
        """

        body = {}
        if allow_duplicate_names is not None:
            body["allow_duplicate_names"] = allow_duplicate_names
        if budget_policy_id is not None:
            body["budget_policy_id"] = budget_policy_id
        if catalog is not None:
            body["catalog"] = catalog
        if channel is not None:
            body["channel"] = channel
        if clone_mode is not None:
            body["clone_mode"] = clone_mode.value
        if clusters is not None:
            body["clusters"] = [v.as_dict() for v in clusters]
        if configuration is not None:
            body["configuration"] = configuration
        if continuous is not None:
            body["continuous"] = continuous
        if deployment is not None:
            body["deployment"] = deployment.as_dict()
        if development is not None:
            body["development"] = development
        if edition is not None:
            body["edition"] = edition
        if environment is not None:
            body["environment"] = environment.as_dict()
        if event_log is not None:
            body["event_log"] = event_log.as_dict()
        if expected_last_modified is not None:
            body["expected_last_modified"] = expected_last_modified
        if filters is not None:
            body["filters"] = filters.as_dict()
        if gateway_definition is not None:
            body["gateway_definition"] = gateway_definition.as_dict()
        if id is not None:
            body["id"] = id
        if ingestion_definition is not None:
            body["ingestion_definition"] = ingestion_definition.as_dict()
        if libraries is not None:
            body["libraries"] = [v.as_dict() for v in libraries]
        if name is not None:
            body["name"] = name
        if notifications is not None:
            body["notifications"] = [v.as_dict() for v in notifications]
        if photon is not None:
            body["photon"] = photon
        if restart_window is not None:
            body["restart_window"] = restart_window.as_dict()
        if root_path is not None:
            body["root_path"] = root_path
        if schema is not None:
            body["schema"] = schema
        if serverless is not None:
            body["serverless"] = serverless
        if storage is not None:
            body["storage"] = storage
        if tags is not None:
            body["tags"] = tags
        if target is not None:
            body["target"] = target
        if trigger is not None:
            body["trigger"] = trigger.as_dict()
        if usage_policy_id is not None:
            body["usage_policy_id"] = usage_policy_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/pipelines/{pipeline_id}/clone", body=body, headers=headers)
        return ClonePipelineResponse.from_dict(res)

    def create(
        self,
        *,
        allow_duplicate_names: Optional[bool] = None,
        budget_policy_id: Optional[str] = None,
        catalog: Optional[str] = None,
        channel: Optional[str] = None,
        clusters: Optional[List[PipelineCluster]] = None,
        configuration: Optional[Dict[str, str]] = None,
        continuous: Optional[bool] = None,
        deployment: Optional[PipelineDeployment] = None,
        development: Optional[bool] = None,
        dry_run: Optional[bool] = None,
        edition: Optional[str] = None,
        environment: Optional[PipelinesEnvironment] = None,
        event_log: Optional[EventLogSpec] = None,
        filters: Optional[Filters] = None,
        gateway_definition: Optional[IngestionGatewayPipelineDefinition] = None,
        id: Optional[str] = None,
        ingestion_definition: Optional[IngestionPipelineDefinition] = None,
        libraries: Optional[List[PipelineLibrary]] = None,
        name: Optional[str] = None,
        notifications: Optional[List[Notifications]] = None,
        photon: Optional[bool] = None,
        restart_window: Optional[RestartWindow] = None,
        root_path: Optional[str] = None,
        run_as: Optional[RunAs] = None,
        schema: Optional[str] = None,
        serverless: Optional[bool] = None,
        storage: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        target: Optional[str] = None,
        trigger: Optional[PipelineTrigger] = None,
        usage_policy_id: Optional[str] = None,
    ) -> CreatePipelineResponse:
        """Creates a new data processing pipeline based on the requested configuration. If successful, this
        method returns the ID of the new pipeline.

        :param allow_duplicate_names: bool (optional)
          If false, deployment will fail if name conflicts with that of another pipeline.
        :param budget_policy_id: str (optional)
          Budget policy of this pipeline.
        :param catalog: str (optional)
          A catalog in Unity Catalog to publish data from this pipeline to. If `target` is specified, tables
          in this pipeline are published to a `target` schema inside `catalog` (for example,
          `catalog`.`target`.`table`). If `target` is not specified, no data is published to Unity Catalog.
        :param channel: str (optional)
          DLT Release Channel that specifies which version to use.
        :param clusters: List[:class:`PipelineCluster`] (optional)
          Cluster settings for this pipeline deployment.
        :param configuration: Dict[str,str] (optional)
          String-String configuration for this pipeline execution.
        :param continuous: bool (optional)
          Whether the pipeline is continuous or triggered. This replaces `trigger`.
        :param deployment: :class:`PipelineDeployment` (optional)
          Deployment type of this pipeline.
        :param development: bool (optional)
          Whether the pipeline is in Development mode. Defaults to false.
        :param dry_run: bool (optional)
        :param edition: str (optional)
          Pipeline product edition.
        :param environment: :class:`PipelinesEnvironment` (optional)
          Environment specification for this pipeline used to install dependencies.
        :param event_log: :class:`EventLogSpec` (optional)
          Event log configuration for this pipeline
        :param filters: :class:`Filters` (optional)
          Filters on which Pipeline packages to include in the deployed graph.
        :param gateway_definition: :class:`IngestionGatewayPipelineDefinition` (optional)
          The definition of a gateway pipeline to support change data capture.
        :param id: str (optional)
          Unique identifier for this pipeline.
        :param ingestion_definition: :class:`IngestionPipelineDefinition` (optional)
          The configuration for a managed ingestion pipeline. These settings cannot be used with the
          'libraries', 'schema', 'target', or 'catalog' settings.
        :param libraries: List[:class:`PipelineLibrary`] (optional)
          Libraries or code needed by this deployment.
        :param name: str (optional)
          Friendly identifier for this pipeline.
        :param notifications: List[:class:`Notifications`] (optional)
          List of notification settings for this pipeline.
        :param photon: bool (optional)
          Whether Photon is enabled for this pipeline.
        :param restart_window: :class:`RestartWindow` (optional)
          Restart window of this pipeline.
        :param root_path: str (optional)
          Root path for this pipeline. This is used as the root directory when editing the pipeline in the
          Databricks user interface and it is added to sys.path when executing Python sources during pipeline
          execution.
        :param run_as: :class:`RunAs` (optional)
        :param schema: str (optional)
          The default schema (database) where tables are read from or published to.
        :param serverless: bool (optional)
          Whether serverless compute is enabled for this pipeline.
        :param storage: str (optional)
          DBFS root directory for storing checkpoints and tables.
        :param tags: Dict[str,str] (optional)
          A map of tags associated with the pipeline. These are forwarded to the cluster as cluster tags, and
          are therefore subject to the same limitations. A maximum of 25 tags can be added to the pipeline.
        :param target: str (optional)
          Target schema (database) to add tables in this pipeline to. Exactly one of `schema` or `target` must
          be specified. To publish to Unity Catalog, also specify `catalog`. This legacy field is deprecated
          for pipeline creation in favor of the `schema` field.
        :param trigger: :class:`PipelineTrigger` (optional)
          Which pipeline trigger to use. Deprecated: Use `continuous` instead.
        :param usage_policy_id: str (optional)
          Usage policy of this pipeline.

        :returns: :class:`CreatePipelineResponse`
        """

        body = {}
        if allow_duplicate_names is not None:
            body["allow_duplicate_names"] = allow_duplicate_names
        if budget_policy_id is not None:
            body["budget_policy_id"] = budget_policy_id
        if catalog is not None:
            body["catalog"] = catalog
        if channel is not None:
            body["channel"] = channel
        if clusters is not None:
            body["clusters"] = [v.as_dict() for v in clusters]
        if configuration is not None:
            body["configuration"] = configuration
        if continuous is not None:
            body["continuous"] = continuous
        if deployment is not None:
            body["deployment"] = deployment.as_dict()
        if development is not None:
            body["development"] = development
        if dry_run is not None:
            body["dry_run"] = dry_run
        if edition is not None:
            body["edition"] = edition
        if environment is not None:
            body["environment"] = environment.as_dict()
        if event_log is not None:
            body["event_log"] = event_log.as_dict()
        if filters is not None:
            body["filters"] = filters.as_dict()
        if gateway_definition is not None:
            body["gateway_definition"] = gateway_definition.as_dict()
        if id is not None:
            body["id"] = id
        if ingestion_definition is not None:
            body["ingestion_definition"] = ingestion_definition.as_dict()
        if libraries is not None:
            body["libraries"] = [v.as_dict() for v in libraries]
        if name is not None:
            body["name"] = name
        if notifications is not None:
            body["notifications"] = [v.as_dict() for v in notifications]
        if photon is not None:
            body["photon"] = photon
        if restart_window is not None:
            body["restart_window"] = restart_window.as_dict()
        if root_path is not None:
            body["root_path"] = root_path
        if run_as is not None:
            body["run_as"] = run_as.as_dict()
        if schema is not None:
            body["schema"] = schema
        if serverless is not None:
            body["serverless"] = serverless
        if storage is not None:
            body["storage"] = storage
        if tags is not None:
            body["tags"] = tags
        if target is not None:
            body["target"] = target
        if trigger is not None:
            body["trigger"] = trigger.as_dict()
        if usage_policy_id is not None:
            body["usage_policy_id"] = usage_policy_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/pipelines", body=body, headers=headers)
        return CreatePipelineResponse.from_dict(res)

    def delete(self, pipeline_id: str, *, force: Optional[bool] = None):
        """Deletes a pipeline. If the pipeline publishes to Unity Catalog, pipeline deletion will cascade to all
        pipeline tables. Please reach out to Databricks support for assistance to undo this action.

        :param pipeline_id: str
        :param force: bool (optional)
          If true, deletion will proceed even if resource cleanup fails. By default, deletion will fail if
          resources cleanup is required but fails.


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

        self._api.do("DELETE", f"/api/2.0/pipelines/{pipeline_id}", query=query, headers=headers)

    def get(self, pipeline_id: str) -> GetPipelineResponse:
        """Get a pipeline.

        :param pipeline_id: str

        :returns: :class:`GetPipelineResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/pipelines/{pipeline_id}", headers=headers)
        return GetPipelineResponse.from_dict(res)

    def get_permission_levels(self, pipeline_id: str) -> GetPipelinePermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param pipeline_id: str
          The pipeline for which to get or manage permissions.

        :returns: :class:`GetPipelinePermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/pipelines/{pipeline_id}/permissionLevels", headers=headers)
        return GetPipelinePermissionLevelsResponse.from_dict(res)

    def get_permissions(self, pipeline_id: str) -> PipelinePermissions:
        """Gets the permissions of a pipeline. Pipelines can inherit permissions from their root object.

        :param pipeline_id: str
          The pipeline for which to get or manage permissions.

        :returns: :class:`PipelinePermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/pipelines/{pipeline_id}", headers=headers)
        return PipelinePermissions.from_dict(res)

    def get_update(self, pipeline_id: str, update_id: str) -> GetUpdateResponse:
        """Gets an update from an active pipeline.

        :param pipeline_id: str
          The ID of the pipeline.
        :param update_id: str
          The ID of the update.

        :returns: :class:`GetUpdateResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}", headers=headers)
        return GetUpdateResponse.from_dict(res)

    def list_pipeline_events(
        self,
        pipeline_id: str,
        *,
        filter: Optional[str] = None,
        max_results: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[PipelineEvent]:
        """Retrieves events for a pipeline.

        :param pipeline_id: str
          The pipeline to return events for.
        :param filter: str (optional)
          Criteria to select a subset of results, expressed using a SQL-like syntax. The supported filters
          are: 1. level='INFO' (or WARN or ERROR) 2. level in ('INFO', 'WARN') 3. id='[event-id]' 4. timestamp
          > 'TIMESTAMP' (or >=,<,<=,=)

          Composite expressions are supported, for example: level in ('ERROR', 'WARN') AND timestamp>
          '2021-07-22T06:37:33.083Z'
        :param max_results: int (optional)
          Max number of entries to return in a single page. The system may return fewer than max_results
          events in a response, even if there are more events available.
        :param order_by: List[str] (optional)
          A string indicating a sort order by timestamp for the results, for example, ["timestamp asc"]. The
          sort order can be ascending or descending. By default, events are returned in descending order by
          timestamp.
        :param page_token: str (optional)
          Page token returned by previous call. This field is mutually exclusive with all fields in this
          request except max_results. An error is returned if any fields other than max_results are set when
          this field is set.

        :returns: Iterator over :class:`PipelineEvent`
        """

        query = {}
        if filter is not None:
            query["filter"] = filter
        if max_results is not None:
            query["max_results"] = max_results
        if order_by is not None:
            query["order_by"] = [v for v in order_by]
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", f"/api/2.0/pipelines/{pipeline_id}/events", query=query, headers=headers)
            if "events" in json:
                for v in json["events"]:
                    yield PipelineEvent.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_pipelines(
        self,
        *,
        filter: Optional[str] = None,
        max_results: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[PipelineStateInfo]:
        """Lists pipelines defined in the Spark Declarative Pipelines system.

        :param filter: str (optional)
          Select a subset of results based on the specified criteria. The supported filters are:

          * `notebook='<path>'` to select pipelines that reference the provided notebook path. * `name LIKE
          '[pattern]'` to select pipelines with a name that matches pattern. Wildcards are supported, for
          example: `name LIKE '%shopping%'`

          Composite filters are not supported. This field is optional.
        :param max_results: int (optional)
          The maximum number of entries to return in a single page. The system may return fewer than
          max_results events in a response, even if there are more events available. This field is optional.
          The default value is 25. The maximum value is 100. An error is returned if the value of max_results
          is greater than 100.
        :param order_by: List[str] (optional)
          A list of strings specifying the order of results. Supported order_by fields are id and name. The
          default is id asc. This field is optional.
        :param page_token: str (optional)
          Page token returned by previous call

        :returns: Iterator over :class:`PipelineStateInfo`
        """

        query = {}
        if filter is not None:
            query["filter"] = filter
        if max_results is not None:
            query["max_results"] = max_results
        if order_by is not None:
            query["order_by"] = [v for v in order_by]
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/pipelines", query=query, headers=headers)
            if "statuses" in json:
                for v in json["statuses"]:
                    yield PipelineStateInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_updates(
        self,
        pipeline_id: str,
        *,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        until_update_id: Optional[str] = None,
    ) -> ListUpdatesResponse:
        """List updates for an active pipeline.

        :param pipeline_id: str
          The pipeline to return updates for.
        :param max_results: int (optional)
          Max number of entries to return in a single page.
        :param page_token: str (optional)
          Page token returned by previous call
        :param until_update_id: str (optional)
          If present, returns updates until and including this update_id.

        :returns: :class:`ListUpdatesResponse`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if until_update_id is not None:
            query["until_update_id"] = until_update_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/pipelines/{pipeline_id}/updates", query=query, headers=headers)
        return ListUpdatesResponse.from_dict(res)

    def set_permissions(
        self, pipeline_id: str, *, access_control_list: Optional[List[PipelineAccessControlRequest]] = None
    ) -> PipelinePermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param pipeline_id: str
          The pipeline for which to get or manage permissions.
        :param access_control_list: List[:class:`PipelineAccessControlRequest`] (optional)

        :returns: :class:`PipelinePermissions`
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

        res = self._api.do("PUT", f"/api/2.0/permissions/pipelines/{pipeline_id}", body=body, headers=headers)
        return PipelinePermissions.from_dict(res)

    def start_update(
        self,
        pipeline_id: str,
        *,
        cause: Optional[StartUpdateCause] = None,
        full_refresh: Optional[bool] = None,
        full_refresh_selection: Optional[List[str]] = None,
        parameters: Optional[Dict[str, str]] = None,
        refresh_selection: Optional[List[str]] = None,
        replace_where_overrides: Optional[List[ReplaceWhereOverride]] = None,
        rewind_spec: Optional[RewindSpec] = None,
        validate_only: Optional[bool] = None,
    ) -> StartUpdateResponse:
        """Starts a new update for the pipeline. If there is already an active update for the pipeline, the
        request will fail and the active update will remain running.

        :param pipeline_id: str
        :param cause: :class:`StartUpdateCause` (optional)
        :param full_refresh: bool (optional)
          If true, this update will reset all tables before running.
        :param full_refresh_selection: List[str] (optional)
          A list of tables to update with fullRefresh. If both refresh_selection and full_refresh_selection
          are empty, this is a full graph update. Full Refresh on a table means that the states of the table
          will be reset before the refresh.
        :param parameters: Dict[str,str] (optional)
          Key/value map of parameters to pass to the pipeline execution
        :param refresh_selection: List[str] (optional)
          A list of tables to update without fullRefresh. If both refresh_selection and full_refresh_selection
          are empty, this is a full graph update. Full Refresh on a table means that the states of the table
          will be reset before the refresh.
        :param replace_where_overrides: List[:class:`ReplaceWhereOverride`] (optional)
          A list of predicate overrides for replace_where flows in this update. Only replace_where flows may
          be specified. Flows not listed use their original predicate.
        :param rewind_spec: :class:`RewindSpec` (optional)
          The information about the requested rewind operation. If specified this is a rewind mode update.
        :param validate_only: bool (optional)
          If true, this update only validates the correctness of pipeline source code but does not materialize
          or publish any datasets.

        :returns: :class:`StartUpdateResponse`
        """

        body = {}
        if cause is not None:
            body["cause"] = cause.value
        if full_refresh is not None:
            body["full_refresh"] = full_refresh
        if full_refresh_selection is not None:
            body["full_refresh_selection"] = [v for v in full_refresh_selection]
        if parameters is not None:
            body["parameters"] = parameters
        if refresh_selection is not None:
            body["refresh_selection"] = [v for v in refresh_selection]
        if replace_where_overrides is not None:
            body["replace_where_overrides"] = [v.as_dict() for v in replace_where_overrides]
        if rewind_spec is not None:
            body["rewind_spec"] = rewind_spec.as_dict()
        if validate_only is not None:
            body["validate_only"] = validate_only
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", body=body, headers=headers)
        return StartUpdateResponse.from_dict(res)

    def stop(self, pipeline_id: str) -> Wait[GetPipelineResponse]:
        """Stops the pipeline by canceling the active update. If there is no active update for the pipeline, this
        request is a no-op.

        :param pipeline_id: str

        :returns:
          Long-running operation waiter for :class:`GetPipelineResponse`.
          See :method:wait_get_pipeline_idle for more details.
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", f"/api/2.0/pipelines/{pipeline_id}/stop", headers=headers)
        return Wait(self.wait_get_pipeline_idle, pipeline_id=pipeline_id)

    def stop_and_wait(self, pipeline_id: str, timeout=timedelta(minutes=20)) -> GetPipelineResponse:
        return self.stop(pipeline_id=pipeline_id).result(timeout=timeout)

    def update(
        self,
        pipeline_id: str,
        *,
        allow_duplicate_names: Optional[bool] = None,
        budget_policy_id: Optional[str] = None,
        catalog: Optional[str] = None,
        channel: Optional[str] = None,
        clusters: Optional[List[PipelineCluster]] = None,
        configuration: Optional[Dict[str, str]] = None,
        continuous: Optional[bool] = None,
        deployment: Optional[PipelineDeployment] = None,
        development: Optional[bool] = None,
        edition: Optional[str] = None,
        environment: Optional[PipelinesEnvironment] = None,
        event_log: Optional[EventLogSpec] = None,
        expected_last_modified: Optional[int] = None,
        filters: Optional[Filters] = None,
        gateway_definition: Optional[IngestionGatewayPipelineDefinition] = None,
        id: Optional[str] = None,
        ingestion_definition: Optional[IngestionPipelineDefinition] = None,
        libraries: Optional[List[PipelineLibrary]] = None,
        name: Optional[str] = None,
        notifications: Optional[List[Notifications]] = None,
        photon: Optional[bool] = None,
        restart_window: Optional[RestartWindow] = None,
        root_path: Optional[str] = None,
        run_as: Optional[RunAs] = None,
        schema: Optional[str] = None,
        serverless: Optional[bool] = None,
        storage: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        target: Optional[str] = None,
        trigger: Optional[PipelineTrigger] = None,
        usage_policy_id: Optional[str] = None,
    ):
        """Updates a pipeline with the supplied configuration.

        :param pipeline_id: str
          Unique identifier for this pipeline.
        :param allow_duplicate_names: bool (optional)
          If false, deployment will fail if name has changed and conflicts the name of another pipeline.
        :param budget_policy_id: str (optional)
          Budget policy of this pipeline.
        :param catalog: str (optional)
          A catalog in Unity Catalog to publish data from this pipeline to. If `target` is specified, tables
          in this pipeline are published to a `target` schema inside `catalog` (for example,
          `catalog`.`target`.`table`). If `target` is not specified, no data is published to Unity Catalog.
        :param channel: str (optional)
          DLT Release Channel that specifies which version to use.
        :param clusters: List[:class:`PipelineCluster`] (optional)
          Cluster settings for this pipeline deployment.
        :param configuration: Dict[str,str] (optional)
          String-String configuration for this pipeline execution.
        :param continuous: bool (optional)
          Whether the pipeline is continuous or triggered. This replaces `trigger`.
        :param deployment: :class:`PipelineDeployment` (optional)
          Deployment type of this pipeline.
        :param development: bool (optional)
          Whether the pipeline is in Development mode. Defaults to false.
        :param edition: str (optional)
          Pipeline product edition.
        :param environment: :class:`PipelinesEnvironment` (optional)
          Environment specification for this pipeline used to install dependencies.
        :param event_log: :class:`EventLogSpec` (optional)
          Event log configuration for this pipeline
        :param expected_last_modified: int (optional)
          If present, the last-modified time of the pipeline settings before the edit. If the settings were
          modified after that time, then the request will fail with a conflict.
        :param filters: :class:`Filters` (optional)
          Filters on which Pipeline packages to include in the deployed graph.
        :param gateway_definition: :class:`IngestionGatewayPipelineDefinition` (optional)
          The definition of a gateway pipeline to support change data capture.
        :param id: str (optional)
          Unique identifier for this pipeline.
        :param ingestion_definition: :class:`IngestionPipelineDefinition` (optional)
          The configuration for a managed ingestion pipeline. These settings cannot be used with the
          'libraries', 'schema', 'target', or 'catalog' settings.
        :param libraries: List[:class:`PipelineLibrary`] (optional)
          Libraries or code needed by this deployment.
        :param name: str (optional)
          Friendly identifier for this pipeline.
        :param notifications: List[:class:`Notifications`] (optional)
          List of notification settings for this pipeline.
        :param photon: bool (optional)
          Whether Photon is enabled for this pipeline.
        :param restart_window: :class:`RestartWindow` (optional)
          Restart window of this pipeline.
        :param root_path: str (optional)
          Root path for this pipeline. This is used as the root directory when editing the pipeline in the
          Databricks user interface and it is added to sys.path when executing Python sources during pipeline
          execution.
        :param run_as: :class:`RunAs` (optional)
        :param schema: str (optional)
          The default schema (database) where tables are read from or published to.
        :param serverless: bool (optional)
          Whether serverless compute is enabled for this pipeline.
        :param storage: str (optional)
          DBFS root directory for storing checkpoints and tables.
        :param tags: Dict[str,str] (optional)
          A map of tags associated with the pipeline. These are forwarded to the cluster as cluster tags, and
          are therefore subject to the same limitations. A maximum of 25 tags can be added to the pipeline.
        :param target: str (optional)
          Target schema (database) to add tables in this pipeline to. Exactly one of `schema` or `target` must
          be specified. To publish to Unity Catalog, also specify `catalog`. This legacy field is deprecated
          for pipeline creation in favor of the `schema` field.
        :param trigger: :class:`PipelineTrigger` (optional)
          Which pipeline trigger to use. Deprecated: Use `continuous` instead.
        :param usage_policy_id: str (optional)
          Usage policy of this pipeline.


        """

        body = {}
        if allow_duplicate_names is not None:
            body["allow_duplicate_names"] = allow_duplicate_names
        if budget_policy_id is not None:
            body["budget_policy_id"] = budget_policy_id
        if catalog is not None:
            body["catalog"] = catalog
        if channel is not None:
            body["channel"] = channel
        if clusters is not None:
            body["clusters"] = [v.as_dict() for v in clusters]
        if configuration is not None:
            body["configuration"] = configuration
        if continuous is not None:
            body["continuous"] = continuous
        if deployment is not None:
            body["deployment"] = deployment.as_dict()
        if development is not None:
            body["development"] = development
        if edition is not None:
            body["edition"] = edition
        if environment is not None:
            body["environment"] = environment.as_dict()
        if event_log is not None:
            body["event_log"] = event_log.as_dict()
        if expected_last_modified is not None:
            body["expected_last_modified"] = expected_last_modified
        if filters is not None:
            body["filters"] = filters.as_dict()
        if gateway_definition is not None:
            body["gateway_definition"] = gateway_definition.as_dict()
        if id is not None:
            body["id"] = id
        if ingestion_definition is not None:
            body["ingestion_definition"] = ingestion_definition.as_dict()
        if libraries is not None:
            body["libraries"] = [v.as_dict() for v in libraries]
        if name is not None:
            body["name"] = name
        if notifications is not None:
            body["notifications"] = [v.as_dict() for v in notifications]
        if photon is not None:
            body["photon"] = photon
        if restart_window is not None:
            body["restart_window"] = restart_window.as_dict()
        if root_path is not None:
            body["root_path"] = root_path
        if run_as is not None:
            body["run_as"] = run_as.as_dict()
        if schema is not None:
            body["schema"] = schema
        if serverless is not None:
            body["serverless"] = serverless
        if storage is not None:
            body["storage"] = storage
        if tags is not None:
            body["tags"] = tags
        if target is not None:
            body["target"] = target
        if trigger is not None:
            body["trigger"] = trigger.as_dict()
        if usage_policy_id is not None:
            body["usage_policy_id"] = usage_policy_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body=body, headers=headers)

    def update_permissions(
        self, pipeline_id: str, *, access_control_list: Optional[List[PipelineAccessControlRequest]] = None
    ) -> PipelinePermissions:
        """Updates the permissions on a pipeline. Pipelines can inherit permissions from their root object.

        :param pipeline_id: str
          The pipeline for which to get or manage permissions.
        :param access_control_list: List[:class:`PipelineAccessControlRequest`] (optional)

        :returns: :class:`PipelinePermissions`
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

        res = self._api.do("PATCH", f"/api/2.0/permissions/pipelines/{pipeline_id}", body=body, headers=headers)
        return PipelinePermissions.from_dict(res)
