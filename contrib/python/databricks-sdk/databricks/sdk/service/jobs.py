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
                                              _repeated_dict)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


class AuthenticationMethod(Enum):

    OAUTH = "OAUTH"
    PAT = "PAT"


@dataclass
class BaseJob:
    created_time: Optional[int] = None
    """The time at which this job was created in epoch milliseconds (milliseconds since 1/1/1970 UTC)."""

    creator_user_name: Optional[str] = None
    """The creator user name. This field won’t be included in the response if the user has already
    been deleted."""

    effective_budget_policy_id: Optional[str] = None
    """The id of the budget policy used by this job for cost attribution purposes. This may be set
    through (in order of precedence): 1. Budget admins through the account or workspace console 2.
    Jobs UI in the job details page and Jobs API using `budget_policy_id` 3. Inferred default based
    on accessible budget policies of the run_as identity on job creation or modification."""

    effective_usage_policy_id: Optional[str] = None
    """The id of the usage policy used by this job for cost attribution purposes."""

    has_more: Optional[bool] = None
    """Indicates if the job has more array properties (`tasks`, `job_clusters`) that are not shown.
    They can be accessed via :method:jobs/get endpoint. It is only relevant for API 2.2
    :method:jobs/list requests with `expand_tasks=true`."""

    job_id: Optional[int] = None
    """The canonical identifier for this job."""

    settings: Optional[JobSettings] = None
    """Settings for this job and all of its runs. These settings can be updated using the `resetJob`
    method."""

    trigger_state: Optional[TriggerStateProto] = None
    """State of the trigger associated with the job."""

    def as_dict(self) -> dict:
        """Serializes the BaseJob into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_time is not None:
            body["created_time"] = self.created_time
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.settings:
            body["settings"] = self.settings.as_dict()
        if self.trigger_state:
            body["trigger_state"] = self.trigger_state.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BaseJob into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_time is not None:
            body["created_time"] = self.created_time
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.settings:
            body["settings"] = self.settings
        if self.trigger_state:
            body["trigger_state"] = self.trigger_state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BaseJob:
        """Deserializes the BaseJob from a dictionary."""
        return cls(
            created_time=d.get("created_time", None),
            creator_user_name=d.get("creator_user_name", None),
            effective_budget_policy_id=d.get("effective_budget_policy_id", None),
            effective_usage_policy_id=d.get("effective_usage_policy_id", None),
            has_more=d.get("has_more", None),
            job_id=d.get("job_id", None),
            settings=_from_dict(d, "settings", JobSettings),
            trigger_state=_from_dict(d, "trigger_state", TriggerStateProto),
        )


@dataclass
class BaseRun:
    attempt_number: Optional[int] = None
    """The sequence number of this run attempt for a triggered job run. The initial attempt of a run
    has an attempt_number of 0. If the initial run attempt fails, and the job has a retry policy
    (`max_retries` > 0), subsequent runs are created with an `original_attempt_run_id` of the
    original attempt’s ID and an incrementing `attempt_number`. Runs are retried only until they
    succeed, and the maximum `attempt_number` is the same as the `max_retries` value for the job."""

    cleanup_duration: Optional[int] = None
    """The time in milliseconds it took to terminate the cluster and clean up any associated artifacts.
    The duration of a task run is the sum of the `setup_duration`, `execution_duration`, and the
    `cleanup_duration`. The `cleanup_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    cluster_instance: Optional[ClusterInstance] = None
    """The cluster used for this run. If the run is specified to use a new cluster, this field is set
    once the Jobs service has requested a cluster for the run."""

    cluster_spec: Optional[ClusterSpec] = None
    """A snapshot of the job’s cluster specification when this run was created."""

    creator_user_name: Optional[str] = None
    """The creator user name. This field won’t be included in the response if the user has already
    been deleted."""

    description: Optional[str] = None
    """Description of the run"""

    effective_performance_target: Optional[PerformanceTarget] = None
    """The actual performance target used by the serverless run during execution. This can differ from
    the client-set performance target on the request depending on whether the performance mode is
    supported by the job type.
    
    * `STANDARD`: Enables cost-efficient execution of serverless workloads. *
    `PERFORMANCE_OPTIMIZED`: Prioritizes fast startup and execution times through rapid scaling and
    optimized cluster performance."""

    effective_usage_policy_id: Optional[str] = None
    """The id of the usage policy used by this run for cost attribution purposes."""

    end_time: Optional[int] = None
    """The time at which this run ended in epoch milliseconds (milliseconds since 1/1/1970 UTC). This
    field is set to 0 if the job is still running."""

    execution_duration: Optional[int] = None
    """The time in milliseconds it took to execute the commands in the JAR or notebook until they
    completed, failed, timed out, were cancelled, or encountered an unexpected error. The duration
    of a task run is the sum of the `setup_duration`, `execution_duration`, and the
    `cleanup_duration`. The `execution_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    git_source: Optional[GitSource] = None
    """An optional specification for a remote Git repository containing the source code used by tasks.
    Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.
    
    If `git_source` is set, these tasks retrieve the file from the remote repository by default.
    However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task.
    
    Note: dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks
    are used, `git_source` must be defined on the job."""

    has_more: Optional[bool] = None
    """Indicates if the run has more array properties (`tasks`, `job_clusters`) that are not shown.
    They can be accessed via :method:jobs/getrun endpoint. It is only relevant for API 2.2
    :method:jobs/listruns requests with `expand_tasks=true`."""

    job_clusters: Optional[List[JobCluster]] = None
    """A list of job cluster specifications that can be shared and reused by tasks of this job.
    Libraries cannot be declared in a shared job cluster. You must declare dependent libraries in
    task settings. If more than 100 job clusters are available, you can paginate through them using
    :method:jobs/getrun."""

    job_id: Optional[int] = None
    """The canonical identifier of the job that contains this run."""

    job_parameters: Optional[List[JobParameter]] = None
    """Job-level parameters used in the run"""

    job_run_id: Optional[int] = None
    """ID of the job run that this run belongs to. For legacy and single-task job runs the field is
    populated with the job run ID. For task runs, the field is populated with the ID of the job run
    that the task run belongs to."""

    number_in_job: Optional[int] = None
    """A unique identifier for this job run. This is set to the same value as `run_id`."""

    original_attempt_run_id: Optional[int] = None
    """If this run is a retry of a prior run attempt, this field contains the run_id of the original
    attempt; otherwise, it is the same as the run_id."""

    overriding_parameters: Optional[RunParameters] = None
    """The parameters used for this run."""

    queue_duration: Optional[int] = None
    """The time in milliseconds that the run has spent in the queue."""

    repair_history: Optional[List[RepairHistoryItem]] = None
    """The repair history of the run."""

    run_duration: Optional[int] = None
    """The time in milliseconds it took the job run and all of its repairs to finish."""

    run_id: Optional[int] = None
    """The canonical identifier of the run. This ID is unique across all runs of all jobs."""

    run_name: Optional[str] = None
    """An optional name for the run. The maximum length is 4096 bytes in UTF-8 encoding."""

    run_page_url: Optional[str] = None
    """The URL to the detail page of the run."""

    run_type: Optional[RunType] = None

    schedule: Optional[CronSchedule] = None
    """The cron schedule that triggered this run if it was triggered by the periodic scheduler."""

    setup_duration: Optional[int] = None
    """The time in milliseconds it took to set up the cluster. For runs that run on new clusters this
    is the cluster creation time, for runs that run on existing clusters this time should be very
    short. The duration of a task run is the sum of the `setup_duration`, `execution_duration`, and
    the `cleanup_duration`. The `setup_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    start_time: Optional[int] = None
    """The time at which this run was started in epoch milliseconds (milliseconds since 1/1/1970 UTC).
    This may not be the time when the job task starts executing, for example, if the job is
    scheduled to run on a new cluster, this is the time the cluster creation call is issued."""

    state: Optional[RunState] = None
    """Deprecated. Please use the `status` field instead."""

    status: Optional[RunStatus] = None

    tasks: Optional[List[RunTask]] = None
    """The list of tasks performed by the run. Each task has its own `run_id` which you can use to call
    `JobsGetOutput` to retrieve the run resutls. If more than 100 tasks are available, you can
    paginate through them using :method:jobs/getrun. Use the `next_page_token` field at the object
    root to determine if more results are available."""

    trigger: Optional[TriggerType] = None

    trigger_info: Optional[TriggerInfo] = None

    def as_dict(self) -> dict:
        """Serializes the BaseRun into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.attempt_number is not None:
            body["attempt_number"] = self.attempt_number
        if self.cleanup_duration is not None:
            body["cleanup_duration"] = self.cleanup_duration
        if self.cluster_instance:
            body["cluster_instance"] = self.cluster_instance.as_dict()
        if self.cluster_spec:
            body["cluster_spec"] = self.cluster_spec.as_dict()
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.description is not None:
            body["description"] = self.description
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target.value
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.execution_duration is not None:
            body["execution_duration"] = self.execution_duration
        if self.git_source:
            body["git_source"] = self.git_source.as_dict()
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.job_clusters:
            body["job_clusters"] = [v.as_dict() for v in self.job_clusters]
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_parameters:
            body["job_parameters"] = [v.as_dict() for v in self.job_parameters]
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        if self.number_in_job is not None:
            body["number_in_job"] = self.number_in_job
        if self.original_attempt_run_id is not None:
            body["original_attempt_run_id"] = self.original_attempt_run_id
        if self.overriding_parameters:
            body["overriding_parameters"] = self.overriding_parameters.as_dict()
        if self.queue_duration is not None:
            body["queue_duration"] = self.queue_duration
        if self.repair_history:
            body["repair_history"] = [v.as_dict() for v in self.repair_history]
        if self.run_duration is not None:
            body["run_duration"] = self.run_duration
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_name is not None:
            body["run_name"] = self.run_name
        if self.run_page_url is not None:
            body["run_page_url"] = self.run_page_url
        if self.run_type is not None:
            body["run_type"] = self.run_type.value
        if self.schedule:
            body["schedule"] = self.schedule.as_dict()
        if self.setup_duration is not None:
            body["setup_duration"] = self.setup_duration
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.tasks:
            body["tasks"] = [v.as_dict() for v in self.tasks]
        if self.trigger is not None:
            body["trigger"] = self.trigger.value
        if self.trigger_info:
            body["trigger_info"] = self.trigger_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BaseRun into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.attempt_number is not None:
            body["attempt_number"] = self.attempt_number
        if self.cleanup_duration is not None:
            body["cleanup_duration"] = self.cleanup_duration
        if self.cluster_instance:
            body["cluster_instance"] = self.cluster_instance
        if self.cluster_spec:
            body["cluster_spec"] = self.cluster_spec
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.description is not None:
            body["description"] = self.description
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.execution_duration is not None:
            body["execution_duration"] = self.execution_duration
        if self.git_source:
            body["git_source"] = self.git_source
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.job_clusters:
            body["job_clusters"] = self.job_clusters
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_parameters:
            body["job_parameters"] = self.job_parameters
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        if self.number_in_job is not None:
            body["number_in_job"] = self.number_in_job
        if self.original_attempt_run_id is not None:
            body["original_attempt_run_id"] = self.original_attempt_run_id
        if self.overriding_parameters:
            body["overriding_parameters"] = self.overriding_parameters
        if self.queue_duration is not None:
            body["queue_duration"] = self.queue_duration
        if self.repair_history:
            body["repair_history"] = self.repair_history
        if self.run_duration is not None:
            body["run_duration"] = self.run_duration
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_name is not None:
            body["run_name"] = self.run_name
        if self.run_page_url is not None:
            body["run_page_url"] = self.run_page_url
        if self.run_type is not None:
            body["run_type"] = self.run_type
        if self.schedule:
            body["schedule"] = self.schedule
        if self.setup_duration is not None:
            body["setup_duration"] = self.setup_duration
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state
        if self.status:
            body["status"] = self.status
        if self.tasks:
            body["tasks"] = self.tasks
        if self.trigger is not None:
            body["trigger"] = self.trigger
        if self.trigger_info:
            body["trigger_info"] = self.trigger_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BaseRun:
        """Deserializes the BaseRun from a dictionary."""
        return cls(
            attempt_number=d.get("attempt_number", None),
            cleanup_duration=d.get("cleanup_duration", None),
            cluster_instance=_from_dict(d, "cluster_instance", ClusterInstance),
            cluster_spec=_from_dict(d, "cluster_spec", ClusterSpec),
            creator_user_name=d.get("creator_user_name", None),
            description=d.get("description", None),
            effective_performance_target=_enum(d, "effective_performance_target", PerformanceTarget),
            effective_usage_policy_id=d.get("effective_usage_policy_id", None),
            end_time=d.get("end_time", None),
            execution_duration=d.get("execution_duration", None),
            git_source=_from_dict(d, "git_source", GitSource),
            has_more=d.get("has_more", None),
            job_clusters=_repeated_dict(d, "job_clusters", JobCluster),
            job_id=d.get("job_id", None),
            job_parameters=_repeated_dict(d, "job_parameters", JobParameter),
            job_run_id=d.get("job_run_id", None),
            number_in_job=d.get("number_in_job", None),
            original_attempt_run_id=d.get("original_attempt_run_id", None),
            overriding_parameters=_from_dict(d, "overriding_parameters", RunParameters),
            queue_duration=d.get("queue_duration", None),
            repair_history=_repeated_dict(d, "repair_history", RepairHistoryItem),
            run_duration=d.get("run_duration", None),
            run_id=d.get("run_id", None),
            run_name=d.get("run_name", None),
            run_page_url=d.get("run_page_url", None),
            run_type=_enum(d, "run_type", RunType),
            schedule=_from_dict(d, "schedule", CronSchedule),
            setup_duration=d.get("setup_duration", None),
            start_time=d.get("start_time", None),
            state=_from_dict(d, "state", RunState),
            status=_from_dict(d, "status", RunStatus),
            tasks=_repeated_dict(d, "tasks", RunTask),
            trigger=_enum(d, "trigger", TriggerType),
            trigger_info=_from_dict(d, "trigger_info", TriggerInfo),
        )


class CleanRoomTaskRunLifeCycleState(Enum):
    """Copied from elastic-spark-common/api/messages/runs.proto. Using the original definition to
    remove coupling with jobs API definition"""

    BLOCKED = "BLOCKED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RUN_LIFE_CYCLE_STATE_UNSPECIFIED = "RUN_LIFE_CYCLE_STATE_UNSPECIFIED"
    SKIPPED = "SKIPPED"
    TERMINATED = "TERMINATED"
    TERMINATING = "TERMINATING"
    WAITING_FOR_RETRY = "WAITING_FOR_RETRY"


class CleanRoomTaskRunResultState(Enum):
    """Copied from elastic-spark-common/api/messages/runs.proto. Using the original definition to avoid
    cyclic dependency."""

    CANCELED = "CANCELED"
    DISABLED = "DISABLED"
    EVICTED = "EVICTED"
    EXCLUDED = "EXCLUDED"
    FAILED = "FAILED"
    MAXIMUM_CONCURRENT_RUNS_REACHED = "MAXIMUM_CONCURRENT_RUNS_REACHED"
    RUN_RESULT_STATE_UNSPECIFIED = "RUN_RESULT_STATE_UNSPECIFIED"
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_FAILURES = "SUCCESS_WITH_FAILURES"
    TIMEDOUT = "TIMEDOUT"
    UPSTREAM_CANCELED = "UPSTREAM_CANCELED"
    UPSTREAM_EVICTED = "UPSTREAM_EVICTED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"


@dataclass
class CleanRoomTaskRunState:
    """Stores the run state of the clean rooms notebook task."""

    life_cycle_state: Optional[CleanRoomTaskRunLifeCycleState] = None
    """A value indicating the run's current lifecycle state. This field is always available in the
    response. Note: Additional states might be introduced in future releases."""

    result_state: Optional[CleanRoomTaskRunResultState] = None
    """A value indicating the run's result. This field is only available for terminal lifecycle states.
    Note: Additional states might be introduced in future releases."""

    def as_dict(self) -> dict:
        """Serializes the CleanRoomTaskRunState into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.life_cycle_state is not None:
            body["life_cycle_state"] = self.life_cycle_state.value
        if self.result_state is not None:
            body["result_state"] = self.result_state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CleanRoomTaskRunState into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.life_cycle_state is not None:
            body["life_cycle_state"] = self.life_cycle_state
        if self.result_state is not None:
            body["result_state"] = self.result_state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CleanRoomTaskRunState:
        """Deserializes the CleanRoomTaskRunState from a dictionary."""
        return cls(
            life_cycle_state=_enum(d, "life_cycle_state", CleanRoomTaskRunLifeCycleState),
            result_state=_enum(d, "result_state", CleanRoomTaskRunResultState),
        )


@dataclass
class CleanRoomsNotebookTask:
    """Clean Rooms notebook task for V1 Clean Room service (GA). Replaces the deprecated
    CleanRoomNotebookTask (defined above) which was for V0 service."""

    clean_room_name: str
    """The clean room that the notebook belongs to."""

    notebook_name: str
    """Name of the notebook being run."""

    etag: Optional[str] = None
    """Checksum to validate the freshness of the notebook resource (i.e. the notebook being run is the
    latest version). It can be fetched by calling the :method:cleanroomassets/get API."""

    notebook_base_parameters: Optional[Dict[str, str]] = None
    """Base parameters to be used for the clean room notebook job."""

    def as_dict(self) -> dict:
        """Serializes the CleanRoomsNotebookTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.clean_room_name is not None:
            body["clean_room_name"] = self.clean_room_name
        if self.etag is not None:
            body["etag"] = self.etag
        if self.notebook_base_parameters:
            body["notebook_base_parameters"] = self.notebook_base_parameters
        if self.notebook_name is not None:
            body["notebook_name"] = self.notebook_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CleanRoomsNotebookTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.clean_room_name is not None:
            body["clean_room_name"] = self.clean_room_name
        if self.etag is not None:
            body["etag"] = self.etag
        if self.notebook_base_parameters:
            body["notebook_base_parameters"] = self.notebook_base_parameters
        if self.notebook_name is not None:
            body["notebook_name"] = self.notebook_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CleanRoomsNotebookTask:
        """Deserializes the CleanRoomsNotebookTask from a dictionary."""
        return cls(
            clean_room_name=d.get("clean_room_name", None),
            etag=d.get("etag", None),
            notebook_base_parameters=d.get("notebook_base_parameters", None),
            notebook_name=d.get("notebook_name", None),
        )


@dataclass
class CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput:
    clean_room_job_run_state: Optional[CleanRoomTaskRunState] = None
    """The run state of the clean rooms notebook task."""

    notebook_output: Optional[NotebookOutput] = None
    """The notebook output for the clean room run"""

    output_schema_info: Optional[OutputSchemaInfo] = None
    """Information on how to access the output schema for the clean room run"""

    def as_dict(self) -> dict:
        """Serializes the CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.clean_room_job_run_state:
            body["clean_room_job_run_state"] = self.clean_room_job_run_state.as_dict()
        if self.notebook_output:
            body["notebook_output"] = self.notebook_output.as_dict()
        if self.output_schema_info:
            body["output_schema_info"] = self.output_schema_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.clean_room_job_run_state:
            body["clean_room_job_run_state"] = self.clean_room_job_run_state
        if self.notebook_output:
            body["notebook_output"] = self.notebook_output
        if self.output_schema_info:
            body["output_schema_info"] = self.output_schema_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput:
        """Deserializes the CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput from a dictionary."""
        return cls(
            clean_room_job_run_state=_from_dict(d, "clean_room_job_run_state", CleanRoomTaskRunState),
            notebook_output=_from_dict(d, "notebook_output", NotebookOutput),
            output_schema_info=_from_dict(d, "output_schema_info", OutputSchemaInfo),
        )


@dataclass
class ClusterInstance:
    cluster_id: Optional[str] = None
    """The canonical identifier for the cluster used by a run. This field is always available for runs
    on existing clusters. For runs on new clusters, it becomes available once the cluster is
    created. This value can be used to view logs by browsing to
    `/#setting/sparkui/$cluster_id/driver-logs`. The logs continue to be available after the run
    completes.
    
    The response won’t include this field if the identifier is not available yet."""

    spark_context_id: Optional[str] = None
    """The canonical identifier for the Spark context used by a run. This field is filled in once the
    run begins execution. This value can be used to view the Spark UI by browsing to
    `/#setting/sparkui/$cluster_id/$spark_context_id`. The Spark UI continues to be available after
    the run has completed.
    
    The response won’t include this field if the identifier is not available yet."""

    def as_dict(self) -> dict:
        """Serializes the ClusterInstance into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.spark_context_id is not None:
            body["spark_context_id"] = self.spark_context_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClusterInstance into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cluster_id is not None:
            body["cluster_id"] = self.cluster_id
        if self.spark_context_id is not None:
            body["spark_context_id"] = self.spark_context_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClusterInstance:
        """Deserializes the ClusterInstance from a dictionary."""
        return cls(cluster_id=d.get("cluster_id", None), spark_context_id=d.get("spark_context_id", None))


@dataclass
class ClusterSpec:
    existing_cluster_id: Optional[str] = None
    """If existing_cluster_id, the ID of an existing cluster that is used for all runs. When running
    jobs or tasks on an existing cluster, you may need to manually restart the cluster if it stops
    responding. We suggest running jobs and tasks on new clusters for greater reliability"""

    job_cluster_key: Optional[str] = None
    """If job_cluster_key, this task is executed reusing the cluster specified in
    `job.settings.job_clusters`."""

    libraries: Optional[List[compute.Library]] = None
    """An optional list of libraries to be installed on the cluster. The default value is an empty
    list."""

    new_cluster: Optional[compute.ClusterSpec] = None
    """If new_cluster, a description of a new cluster that is created for each run."""

    def as_dict(self) -> dict:
        """Serializes the ClusterSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.libraries:
            body["libraries"] = [v.as_dict() for v in self.libraries]
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ClusterSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.libraries:
            body["libraries"] = self.libraries
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ClusterSpec:
        """Deserializes the ClusterSpec from a dictionary."""
        return cls(
            existing_cluster_id=d.get("existing_cluster_id", None),
            job_cluster_key=d.get("job_cluster_key", None),
            libraries=_repeated_dict(d, "libraries", compute.Library),
            new_cluster=_from_dict(d, "new_cluster", compute.ClusterSpec),
        )


@dataclass
class Compute:
    hardware_accelerator: Optional[compute.HardwareAcceleratorType] = None
    """Hardware accelerator configuration for Serverless GPU workloads."""

    def as_dict(self) -> dict:
        """Serializes the Compute into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.hardware_accelerator is not None:
            body["hardware_accelerator"] = self.hardware_accelerator.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Compute into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.hardware_accelerator is not None:
            body["hardware_accelerator"] = self.hardware_accelerator
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Compute:
        """Deserializes the Compute from a dictionary."""
        return cls(hardware_accelerator=_enum(d, "hardware_accelerator", compute.HardwareAcceleratorType))


@dataclass
class ComputeConfig:
    num_gpus: int
    """Number of GPUs."""

    gpu_node_pool_id: Optional[str] = None
    """IDof the GPU pool to use."""

    gpu_type: Optional[str] = None
    """GPU type."""

    def as_dict(self) -> dict:
        """Serializes the ComputeConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.gpu_node_pool_id is not None:
            body["gpu_node_pool_id"] = self.gpu_node_pool_id
        if self.gpu_type is not None:
            body["gpu_type"] = self.gpu_type
        if self.num_gpus is not None:
            body["num_gpus"] = self.num_gpus
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ComputeConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.gpu_node_pool_id is not None:
            body["gpu_node_pool_id"] = self.gpu_node_pool_id
        if self.gpu_type is not None:
            body["gpu_type"] = self.gpu_type
        if self.num_gpus is not None:
            body["num_gpus"] = self.num_gpus
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ComputeConfig:
        """Deserializes the ComputeConfig from a dictionary."""
        return cls(
            gpu_node_pool_id=d.get("gpu_node_pool_id", None),
            gpu_type=d.get("gpu_type", None),
            num_gpus=d.get("num_gpus", None),
        )


class Condition(Enum):

    ALL_UPDATED = "ALL_UPDATED"
    ANY_UPDATED = "ANY_UPDATED"


@dataclass
class ConditionTask:
    op: ConditionTaskOp
    """* `EQUAL_TO`, `NOT_EQUAL` operators perform string comparison of their operands. This means that
    `“12.0” == “12”` will evaluate to `false`. * `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`,
    `LESS_THAN`, `LESS_THAN_OR_EQUAL` operators perform numeric comparison of their operands.
    `“12.0” >= “12”` will evaluate to `true`, `“10.0” >= “12”` will evaluate to
    `false`.
    
    The boolean comparison to task values can be implemented with operators `EQUAL_TO`, `NOT_EQUAL`.
    If a task value was set to a boolean value, it will be serialized to `“true”` or
    `“false”` for the comparison."""

    left: str
    """The left operand of the condition task. Can be either a string value or a job state or parameter
    reference."""

    right: str
    """The right operand of the condition task. Can be either a string value or a job state or
    parameter reference."""

    def as_dict(self) -> dict:
        """Serializes the ConditionTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.left is not None:
            body["left"] = self.left
        if self.op is not None:
            body["op"] = self.op.value
        if self.right is not None:
            body["right"] = self.right
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ConditionTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.left is not None:
            body["left"] = self.left
        if self.op is not None:
            body["op"] = self.op
        if self.right is not None:
            body["right"] = self.right
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ConditionTask:
        """Deserializes the ConditionTask from a dictionary."""
        return cls(left=d.get("left", None), op=_enum(d, "op", ConditionTaskOp), right=d.get("right", None))


class ConditionTaskOp(Enum):
    """* `EQUAL_TO`, `NOT_EQUAL` operators perform string comparison of their operands. This means that
    `“12.0” == “12”` will evaluate to `false`. * `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`,
    `LESS_THAN`, `LESS_THAN_OR_EQUAL` operators perform numeric comparison of their operands.
    `“12.0” >= “12”` will evaluate to `true`, `“10.0” >= “12”` will evaluate to
    `false`.

    The boolean comparison to task values can be implemented with operators `EQUAL_TO`, `NOT_EQUAL`.
    If a task value was set to a boolean value, it will be serialized to `“true”` or
    `“false”` for the comparison."""

    EQUAL_TO = "EQUAL_TO"
    GREATER_THAN = "GREATER_THAN"
    GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL"
    LESS_THAN = "LESS_THAN"
    LESS_THAN_OR_EQUAL = "LESS_THAN_OR_EQUAL"
    NOT_EQUAL = "NOT_EQUAL"


@dataclass
class Continuous:
    pause_status: Optional[PauseStatus] = None
    """Indicate whether the continuous execution of the job is paused or not. Defaults to UNPAUSED."""

    task_retry_mode: Optional[TaskRetryMode] = None
    """Indicate whether the continuous job is applying task level retries or not. Defaults to NEVER."""

    def as_dict(self) -> dict:
        """Serializes the Continuous into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status.value
        if self.task_retry_mode is not None:
            body["task_retry_mode"] = self.task_retry_mode.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Continuous into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status
        if self.task_retry_mode is not None:
            body["task_retry_mode"] = self.task_retry_mode
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Continuous:
        """Deserializes the Continuous from a dictionary."""
        return cls(
            pause_status=_enum(d, "pause_status", PauseStatus),
            task_retry_mode=_enum(d, "task_retry_mode", TaskRetryMode),
        )


@dataclass
class CreateResponse:
    """Job was created successfully"""

    job_id: Optional[int] = None
    """The canonical identifier for the newly created job."""

    def as_dict(self) -> dict:
        """Serializes the CreateResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateResponse:
        """Deserializes the CreateResponse from a dictionary."""
        return cls(job_id=d.get("job_id", None))


@dataclass
class CronSchedule:
    quartz_cron_expression: str
    """A Cron expression using Quartz syntax that describes the schedule for a job. See [Cron Trigger]
    for details. This field is required.
    
    [Cron Trigger]: http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html"""

    timezone_id: str
    """A Java timezone ID. The schedule for a job is resolved with respect to this timezone. See [Java
    TimeZone] for details. This field is required.
    
    [Java TimeZone]: https://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html"""

    pause_status: Optional[PauseStatus] = None
    """Indicate whether this schedule is paused or not."""

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
            pause_status=_enum(d, "pause_status", PauseStatus),
            quartz_cron_expression=d.get("quartz_cron_expression", None),
            timezone_id=d.get("timezone_id", None),
        )


@dataclass
class DashboardPageSnapshot:
    page_display_name: Optional[str] = None

    widget_error_details: Optional[List[WidgetErrorDetail]] = None

    def as_dict(self) -> dict:
        """Serializes the DashboardPageSnapshot into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.page_display_name is not None:
            body["page_display_name"] = self.page_display_name
        if self.widget_error_details:
            body["widget_error_details"] = [v.as_dict() for v in self.widget_error_details]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DashboardPageSnapshot into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.page_display_name is not None:
            body["page_display_name"] = self.page_display_name
        if self.widget_error_details:
            body["widget_error_details"] = self.widget_error_details
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DashboardPageSnapshot:
        """Deserializes the DashboardPageSnapshot from a dictionary."""
        return cls(
            page_display_name=d.get("page_display_name", None),
            widget_error_details=_repeated_dict(d, "widget_error_details", WidgetErrorDetail),
        )


@dataclass
class DashboardTask:
    """Configures the Lakeview Dashboard job task type."""

    dashboard_id: Optional[str] = None
    """The identifier of the dashboard to refresh."""

    filters: Optional[Dict[str, str]] = None
    """Dashboard task parameters. Used to apply dashboard filter values during dashboard task
    execution. Parameter values get applied to any dashboard filters that have a matching URL
    identifier as the parameter key. The parameter value format is dependent on the filter type: -
    For text and single-select filters, provide a single value (e.g. `"value"`) - For date and
    datetime filters, provide the value in ISO 8601 format (e.g. `"2000-01-01T00:00:00"`) - For
    multi-select filters, provide a JSON array of values (e.g. `"[\"value1\",\"value2\"]"`) - For
    range and date range filters, provide a JSON object with `start` and `end` (e.g.
    `"{\"start\":\"1\",\"end\":\"10\"}"`)"""

    subscription: Optional[Subscription] = None
    """Optional: subscription configuration for sending the dashboard snapshot."""

    warehouse_id: Optional[str] = None
    """Optional: The warehouse id to execute the dashboard with for the schedule. If not specified, the
    default warehouse of the dashboard will be used."""

    def as_dict(self) -> dict:
        """Serializes the DashboardTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.filters:
            body["filters"] = self.filters
        if self.subscription:
            body["subscription"] = self.subscription.as_dict()
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DashboardTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.filters:
            body["filters"] = self.filters
        if self.subscription:
            body["subscription"] = self.subscription
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DashboardTask:
        """Deserializes the DashboardTask from a dictionary."""
        return cls(
            dashboard_id=d.get("dashboard_id", None),
            filters=d.get("filters", None),
            subscription=_from_dict(d, "subscription", Subscription),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class DashboardTaskOutput:
    page_snapshots: Optional[List[DashboardPageSnapshot]] = None
    """Should only be populated for manual PDF download jobs."""

    def as_dict(self) -> dict:
        """Serializes the DashboardTaskOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.page_snapshots:
            body["page_snapshots"] = [v.as_dict() for v in self.page_snapshots]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DashboardTaskOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.page_snapshots:
            body["page_snapshots"] = self.page_snapshots
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DashboardTaskOutput:
        """Deserializes the DashboardTaskOutput from a dictionary."""
        return cls(page_snapshots=_repeated_dict(d, "page_snapshots", DashboardPageSnapshot))


@dataclass
class DbtCloudJobRunStep:
    """Format of response retrieved from dbt Cloud, for inclusion in output Deprecated in favor of
    DbtPlatformJobRunStep"""

    index: Optional[int] = None
    """Orders the steps in the job"""

    logs: Optional[str] = None
    """Output of the step"""

    name: Optional[str] = None
    """Name of the step in the job"""

    status: Optional[DbtPlatformRunStatus] = None
    """State of the step"""

    def as_dict(self) -> dict:
        """Serializes the DbtCloudJobRunStep into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.index is not None:
            body["index"] = self.index
        if self.logs is not None:
            body["logs"] = self.logs
        if self.name is not None:
            body["name"] = self.name
        if self.status is not None:
            body["status"] = self.status.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtCloudJobRunStep into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.index is not None:
            body["index"] = self.index
        if self.logs is not None:
            body["logs"] = self.logs
        if self.name is not None:
            body["name"] = self.name
        if self.status is not None:
            body["status"] = self.status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtCloudJobRunStep:
        """Deserializes the DbtCloudJobRunStep from a dictionary."""
        return cls(
            index=d.get("index", None),
            logs=d.get("logs", None),
            name=d.get("name", None),
            status=_enum(d, "status", DbtPlatformRunStatus),
        )


@dataclass
class DbtCloudTask:
    """Deprecated in favor of DbtPlatformTask"""

    connection_resource_name: Optional[str] = None
    """The resource name of the UC connection that authenticates the dbt Cloud for this task"""

    dbt_cloud_job_id: Optional[int] = None
    """Id of the dbt Cloud job to be triggered"""

    def as_dict(self) -> dict:
        """Serializes the DbtCloudTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection_resource_name is not None:
            body["connection_resource_name"] = self.connection_resource_name
        if self.dbt_cloud_job_id is not None:
            body["dbt_cloud_job_id"] = self.dbt_cloud_job_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtCloudTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection_resource_name is not None:
            body["connection_resource_name"] = self.connection_resource_name
        if self.dbt_cloud_job_id is not None:
            body["dbt_cloud_job_id"] = self.dbt_cloud_job_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtCloudTask:
        """Deserializes the DbtCloudTask from a dictionary."""
        return cls(
            connection_resource_name=d.get("connection_resource_name", None),
            dbt_cloud_job_id=d.get("dbt_cloud_job_id", None),
        )


@dataclass
class DbtCloudTaskOutput:
    """Deprecated in favor of DbtPlatformTaskOutput"""

    dbt_cloud_job_run_id: Optional[int] = None
    """Id of the job run in dbt Cloud"""

    dbt_cloud_job_run_output: Optional[List[DbtCloudJobRunStep]] = None
    """Steps of the job run as received from dbt Cloud"""

    dbt_cloud_job_run_url: Optional[str] = None
    """Url where full run details can be viewed"""

    def as_dict(self) -> dict:
        """Serializes the DbtCloudTaskOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dbt_cloud_job_run_id is not None:
            body["dbt_cloud_job_run_id"] = self.dbt_cloud_job_run_id
        if self.dbt_cloud_job_run_output:
            body["dbt_cloud_job_run_output"] = [v.as_dict() for v in self.dbt_cloud_job_run_output]
        if self.dbt_cloud_job_run_url is not None:
            body["dbt_cloud_job_run_url"] = self.dbt_cloud_job_run_url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtCloudTaskOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dbt_cloud_job_run_id is not None:
            body["dbt_cloud_job_run_id"] = self.dbt_cloud_job_run_id
        if self.dbt_cloud_job_run_output:
            body["dbt_cloud_job_run_output"] = self.dbt_cloud_job_run_output
        if self.dbt_cloud_job_run_url is not None:
            body["dbt_cloud_job_run_url"] = self.dbt_cloud_job_run_url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtCloudTaskOutput:
        """Deserializes the DbtCloudTaskOutput from a dictionary."""
        return cls(
            dbt_cloud_job_run_id=d.get("dbt_cloud_job_run_id", None),
            dbt_cloud_job_run_output=_repeated_dict(d, "dbt_cloud_job_run_output", DbtCloudJobRunStep),
            dbt_cloud_job_run_url=d.get("dbt_cloud_job_run_url", None),
        )


@dataclass
class DbtOutput:
    artifacts_headers: Optional[Dict[str, str]] = None
    """An optional map of headers to send when retrieving the artifact from the `artifacts_link`."""

    artifacts_link: Optional[str] = None
    """A pre-signed URL to download the (compressed) dbt artifacts. This link is valid for a limited
    time (30 minutes). This information is only available after the run has finished."""

    def as_dict(self) -> dict:
        """Serializes the DbtOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.artifacts_headers:
            body["artifacts_headers"] = self.artifacts_headers
        if self.artifacts_link is not None:
            body["artifacts_link"] = self.artifacts_link
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.artifacts_headers:
            body["artifacts_headers"] = self.artifacts_headers
        if self.artifacts_link is not None:
            body["artifacts_link"] = self.artifacts_link
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtOutput:
        """Deserializes the DbtOutput from a dictionary."""
        return cls(artifacts_headers=d.get("artifacts_headers", None), artifacts_link=d.get("artifacts_link", None))


@dataclass
class DbtPlatformJobRunStep:
    """Format of response retrieved from dbt platform, for inclusion in output"""

    index: Optional[int] = None
    """Orders the steps in the job"""

    logs: Optional[str] = None
    """Output of the step"""

    logs_truncated: Optional[bool] = None
    """Whether the logs of this step have been truncated. If true, the logs has been truncated to 10000
    characters."""

    name: Optional[str] = None
    """Name of the step in the job"""

    name_truncated: Optional[bool] = None
    """Whether the name of the job has been truncated. If true, the name has been truncated to 100
    characters."""

    status: Optional[DbtPlatformRunStatus] = None
    """State of the step"""

    def as_dict(self) -> dict:
        """Serializes the DbtPlatformJobRunStep into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.index is not None:
            body["index"] = self.index
        if self.logs is not None:
            body["logs"] = self.logs
        if self.logs_truncated is not None:
            body["logs_truncated"] = self.logs_truncated
        if self.name is not None:
            body["name"] = self.name
        if self.name_truncated is not None:
            body["name_truncated"] = self.name_truncated
        if self.status is not None:
            body["status"] = self.status.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtPlatformJobRunStep into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.index is not None:
            body["index"] = self.index
        if self.logs is not None:
            body["logs"] = self.logs
        if self.logs_truncated is not None:
            body["logs_truncated"] = self.logs_truncated
        if self.name is not None:
            body["name"] = self.name
        if self.name_truncated is not None:
            body["name_truncated"] = self.name_truncated
        if self.status is not None:
            body["status"] = self.status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtPlatformJobRunStep:
        """Deserializes the DbtPlatformJobRunStep from a dictionary."""
        return cls(
            index=d.get("index", None),
            logs=d.get("logs", None),
            logs_truncated=d.get("logs_truncated", None),
            name=d.get("name", None),
            name_truncated=d.get("name_truncated", None),
            status=_enum(d, "status", DbtPlatformRunStatus),
        )


class DbtPlatformRunStatus(Enum):
    """Response enumeration from calling the dbt platform API, for inclusion in output"""

    CANCELLED = "CANCELLED"
    ERROR = "ERROR"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    STARTING = "STARTING"
    SUCCESS = "SUCCESS"


@dataclass
class DbtPlatformTask:
    connection_resource_name: Optional[str] = None
    """The resource name of the UC connection that authenticates the dbt platform for this task"""

    dbt_platform_job_id: Optional[str] = None
    """Id of the dbt platform job to be triggered. Specified as a string for maximum compatibility with
    clients."""

    def as_dict(self) -> dict:
        """Serializes the DbtPlatformTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection_resource_name is not None:
            body["connection_resource_name"] = self.connection_resource_name
        if self.dbt_platform_job_id is not None:
            body["dbt_platform_job_id"] = self.dbt_platform_job_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtPlatformTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection_resource_name is not None:
            body["connection_resource_name"] = self.connection_resource_name
        if self.dbt_platform_job_id is not None:
            body["dbt_platform_job_id"] = self.dbt_platform_job_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtPlatformTask:
        """Deserializes the DbtPlatformTask from a dictionary."""
        return cls(
            connection_resource_name=d.get("connection_resource_name", None),
            dbt_platform_job_id=d.get("dbt_platform_job_id", None),
        )


@dataclass
class DbtPlatformTaskOutput:
    dbt_platform_job_run_id: Optional[str] = None
    """Id of the job run in dbt platform. Specified as a string for maximum compatibility with clients."""

    dbt_platform_job_run_output: Optional[List[DbtPlatformJobRunStep]] = None
    """Steps of the job run as received from dbt platform"""

    dbt_platform_job_run_url: Optional[str] = None
    """Url where full run details can be viewed"""

    steps_truncated: Optional[bool] = None
    """Whether the number of steps in the output has been truncated. If true, the output will contain
    the first 20 steps of the output."""

    def as_dict(self) -> dict:
        """Serializes the DbtPlatformTaskOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dbt_platform_job_run_id is not None:
            body["dbt_platform_job_run_id"] = self.dbt_platform_job_run_id
        if self.dbt_platform_job_run_output:
            body["dbt_platform_job_run_output"] = [v.as_dict() for v in self.dbt_platform_job_run_output]
        if self.dbt_platform_job_run_url is not None:
            body["dbt_platform_job_run_url"] = self.dbt_platform_job_run_url
        if self.steps_truncated is not None:
            body["steps_truncated"] = self.steps_truncated
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtPlatformTaskOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dbt_platform_job_run_id is not None:
            body["dbt_platform_job_run_id"] = self.dbt_platform_job_run_id
        if self.dbt_platform_job_run_output:
            body["dbt_platform_job_run_output"] = self.dbt_platform_job_run_output
        if self.dbt_platform_job_run_url is not None:
            body["dbt_platform_job_run_url"] = self.dbt_platform_job_run_url
        if self.steps_truncated is not None:
            body["steps_truncated"] = self.steps_truncated
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtPlatformTaskOutput:
        """Deserializes the DbtPlatformTaskOutput from a dictionary."""
        return cls(
            dbt_platform_job_run_id=d.get("dbt_platform_job_run_id", None),
            dbt_platform_job_run_output=_repeated_dict(d, "dbt_platform_job_run_output", DbtPlatformJobRunStep),
            dbt_platform_job_run_url=d.get("dbt_platform_job_run_url", None),
            steps_truncated=d.get("steps_truncated", None),
        )


@dataclass
class DbtTask:
    commands: List[str]
    """A list of dbt commands to execute. All commands must start with `dbt`. This parameter must not
    be empty. A maximum of up to 10 commands can be provided."""

    catalog: Optional[str] = None
    """Optional name of the catalog to use. The value is the top level in the 3-level namespace of
    Unity Catalog (catalog / schema / relation). The catalog value can only be specified if a
    warehouse_id is specified. Requires dbt-databricks >= 1.1.1."""

    profiles_directory: Optional[str] = None
    """Optional (relative) path to the profiles directory. Can only be specified if no warehouse_id is
    specified. If no warehouse_id is specified and this folder is unset, the root directory is used."""

    project_directory: Optional[str] = None
    """Path to the project directory. Optional for Git sourced tasks, in which case if no value is
    provided, the root of the Git repository is used."""

    schema: Optional[str] = None
    """Optional schema to write to. This parameter is only used when a warehouse_id is also provided.
    If not provided, the `default` schema is used."""

    source: Optional[Source] = None
    """Optional location type of the project directory. When set to `WORKSPACE`, the project will be
    retrieved from the local Databricks workspace. When set to `GIT`, the project will be retrieved
    from a Git repository defined in `git_source`. If the value is empty, the task will use `GIT` if
    `git_source` is defined and `WORKSPACE` otherwise.
    
    * `WORKSPACE`: Project is located in Databricks workspace. * `GIT`: Project is located in cloud
    Git provider."""

    warehouse_id: Optional[str] = None
    """ID of the SQL warehouse to connect to. If provided, we automatically generate and provide the
    profile and connection details to dbt. It can be overridden on a per-command basis by using the
    `--profiles-dir` command line argument."""

    def as_dict(self) -> dict:
        """Serializes the DbtTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.commands:
            body["commands"] = [v for v in self.commands]
        if self.profiles_directory is not None:
            body["profiles_directory"] = self.profiles_directory
        if self.project_directory is not None:
            body["project_directory"] = self.project_directory
        if self.schema is not None:
            body["schema"] = self.schema
        if self.source is not None:
            body["source"] = self.source.value
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DbtTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.commands:
            body["commands"] = self.commands
        if self.profiles_directory is not None:
            body["profiles_directory"] = self.profiles_directory
        if self.project_directory is not None:
            body["project_directory"] = self.project_directory
        if self.schema is not None:
            body["schema"] = self.schema
        if self.source is not None:
            body["source"] = self.source
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DbtTask:
        """Deserializes the DbtTask from a dictionary."""
        return cls(
            catalog=d.get("catalog", None),
            commands=d.get("commands", None),
            profiles_directory=d.get("profiles_directory", None),
            project_directory=d.get("project_directory", None),
            schema=d.get("schema", None),
            source=_enum(d, "source", Source),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class EnforcePolicyComplianceForJobResponseJobClusterSettingsChange:
    """Represents a change to the job cluster's settings that would be required for the job clusters to
    become compliant with their policies."""

    field: Optional[str] = None
    """The field where this change would be made, prepended with the job cluster key."""

    new_value: Optional[str] = None
    """The new value of this field after enforcing policy compliance (either a number, a boolean, or a
    string) converted to a string. This is intended to be read by a human. The typed new value of
    this field can be retrieved by reading the settings field in the API response."""

    previous_value: Optional[str] = None
    """The previous value of this field before enforcing policy compliance (either a number, a boolean,
    or a string) converted to a string. This is intended to be read by a human. The type of the
    field can be retrieved by reading the settings field in the API response."""

    def as_dict(self) -> dict:
        """Serializes the EnforcePolicyComplianceForJobResponseJobClusterSettingsChange into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.field is not None:
            body["field"] = self.field
        if self.new_value is not None:
            body["new_value"] = self.new_value
        if self.previous_value is not None:
            body["previous_value"] = self.previous_value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnforcePolicyComplianceForJobResponseJobClusterSettingsChange into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.field is not None:
            body["field"] = self.field
        if self.new_value is not None:
            body["new_value"] = self.new_value
        if self.previous_value is not None:
            body["previous_value"] = self.previous_value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnforcePolicyComplianceForJobResponseJobClusterSettingsChange:
        """Deserializes the EnforcePolicyComplianceForJobResponseJobClusterSettingsChange from a dictionary."""
        return cls(
            field=d.get("field", None), new_value=d.get("new_value", None), previous_value=d.get("previous_value", None)
        )


@dataclass
class EnforcePolicyComplianceResponse:
    has_changes: Optional[bool] = None
    """Whether any changes have been made to the job cluster settings for the job to become compliant
    with its policies."""

    job_cluster_changes: Optional[List[EnforcePolicyComplianceForJobResponseJobClusterSettingsChange]] = None
    """A list of job cluster changes that have been made to the job’s cluster settings in order for
    all job clusters to become compliant with their policies."""

    settings: Optional[JobSettings] = None
    """Updated job settings after policy enforcement. Policy enforcement only applies to job clusters
    that are created when running the job (which are specified in new_cluster) and does not apply to
    existing all-purpose clusters. Updated job settings are derived by applying policy default
    values to the existing job clusters in order to satisfy policy requirements."""

    def as_dict(self) -> dict:
        """Serializes the EnforcePolicyComplianceResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.has_changes is not None:
            body["has_changes"] = self.has_changes
        if self.job_cluster_changes:
            body["job_cluster_changes"] = [v.as_dict() for v in self.job_cluster_changes]
        if self.settings:
            body["settings"] = self.settings.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the EnforcePolicyComplianceResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.has_changes is not None:
            body["has_changes"] = self.has_changes
        if self.job_cluster_changes:
            body["job_cluster_changes"] = self.job_cluster_changes
        if self.settings:
            body["settings"] = self.settings
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> EnforcePolicyComplianceResponse:
        """Deserializes the EnforcePolicyComplianceResponse from a dictionary."""
        return cls(
            has_changes=d.get("has_changes", None),
            job_cluster_changes=_repeated_dict(
                d, "job_cluster_changes", EnforcePolicyComplianceForJobResponseJobClusterSettingsChange
            ),
            settings=_from_dict(d, "settings", JobSettings),
        )


@dataclass
class ExportRunOutput:
    """Run was exported successfully."""

    views: Optional[List[ViewItem]] = None
    """The exported content in HTML format (one for every view item). To extract the HTML notebook from
    the JSON response, download and run this [Python script](/_static/examples/extract.py)."""

    def as_dict(self) -> dict:
        """Serializes the ExportRunOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.views:
            body["views"] = [v.as_dict() for v in self.views]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExportRunOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.views:
            body["views"] = self.views
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExportRunOutput:
        """Deserializes the ExportRunOutput from a dictionary."""
        return cls(views=_repeated_dict(d, "views", ViewItem))


@dataclass
class FileArrivalTriggerConfiguration:
    url: str
    """URL to be monitored for file arrivals. The path must point to the root or a subpath of the
    external location."""

    min_time_between_triggers_seconds: Optional[int] = None
    """If set, the trigger starts a run only after the specified amount of time passed since the last
    time the trigger fired. The minimum allowed value is 60 seconds"""

    wait_after_last_change_seconds: Optional[int] = None
    """If set, the trigger starts a run only after no file activity has occurred for the specified
    amount of time. This makes it possible to wait for a batch of incoming files to arrive before
    triggering a run. The minimum allowed value is 60 seconds."""

    def as_dict(self) -> dict:
        """Serializes the FileArrivalTriggerConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.min_time_between_triggers_seconds is not None:
            body["min_time_between_triggers_seconds"] = self.min_time_between_triggers_seconds
        if self.url is not None:
            body["url"] = self.url
        if self.wait_after_last_change_seconds is not None:
            body["wait_after_last_change_seconds"] = self.wait_after_last_change_seconds
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileArrivalTriggerConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.min_time_between_triggers_seconds is not None:
            body["min_time_between_triggers_seconds"] = self.min_time_between_triggers_seconds
        if self.url is not None:
            body["url"] = self.url
        if self.wait_after_last_change_seconds is not None:
            body["wait_after_last_change_seconds"] = self.wait_after_last_change_seconds
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileArrivalTriggerConfiguration:
        """Deserializes the FileArrivalTriggerConfiguration from a dictionary."""
        return cls(
            min_time_between_triggers_seconds=d.get("min_time_between_triggers_seconds", None),
            url=d.get("url", None),
            wait_after_last_change_seconds=d.get("wait_after_last_change_seconds", None),
        )


@dataclass
class FileArrivalTriggerState:
    using_file_events: Optional[bool] = None
    """Indicates whether the trigger leverages file events to detect file arrivals."""

    def as_dict(self) -> dict:
        """Serializes the FileArrivalTriggerState into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.using_file_events is not None:
            body["using_file_events"] = self.using_file_events
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileArrivalTriggerState into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.using_file_events is not None:
            body["using_file_events"] = self.using_file_events
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileArrivalTriggerState:
        """Deserializes the FileArrivalTriggerState from a dictionary."""
        return cls(using_file_events=d.get("using_file_events", None))


@dataclass
class ForEachStats:
    error_message_stats: Optional[List[ForEachTaskErrorMessageStats]] = None
    """Sample of 3 most common error messages occurred during the iteration."""

    task_run_stats: Optional[ForEachTaskTaskRunStats] = None
    """Describes stats of the iteration. Only latest retries are considered."""

    def as_dict(self) -> dict:
        """Serializes the ForEachStats into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.error_message_stats:
            body["error_message_stats"] = [v.as_dict() for v in self.error_message_stats]
        if self.task_run_stats:
            body["task_run_stats"] = self.task_run_stats.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ForEachStats into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.error_message_stats:
            body["error_message_stats"] = self.error_message_stats
        if self.task_run_stats:
            body["task_run_stats"] = self.task_run_stats
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ForEachStats:
        """Deserializes the ForEachStats from a dictionary."""
        return cls(
            error_message_stats=_repeated_dict(d, "error_message_stats", ForEachTaskErrorMessageStats),
            task_run_stats=_from_dict(d, "task_run_stats", ForEachTaskTaskRunStats),
        )


@dataclass
class ForEachTask:
    inputs: str
    """Array for task to iterate on. This can be a JSON string or a reference to an array parameter."""

    task: Task
    """Configuration for the task that will be run for each element in the array"""

    concurrency: Optional[int] = None
    """An optional maximum allowed number of concurrent runs of the task. Set this value if you want to
    be able to execute multiple runs of the task concurrently."""

    def as_dict(self) -> dict:
        """Serializes the ForEachTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.concurrency is not None:
            body["concurrency"] = self.concurrency
        if self.inputs is not None:
            body["inputs"] = self.inputs
        if self.task:
            body["task"] = self.task.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ForEachTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.concurrency is not None:
            body["concurrency"] = self.concurrency
        if self.inputs is not None:
            body["inputs"] = self.inputs
        if self.task:
            body["task"] = self.task
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ForEachTask:
        """Deserializes the ForEachTask from a dictionary."""
        return cls(
            concurrency=d.get("concurrency", None), inputs=d.get("inputs", None), task=_from_dict(d, "task", Task)
        )


@dataclass
class ForEachTaskErrorMessageStats:
    count: Optional[int] = None
    """Describes the count of such error message encountered during the iterations."""

    error_message: Optional[str] = None
    """Describes the error message occured during the iterations."""

    termination_category: Optional[str] = None
    """Describes the termination reason for the error message."""

    def as_dict(self) -> dict:
        """Serializes the ForEachTaskErrorMessageStats into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.count is not None:
            body["count"] = self.count
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.termination_category is not None:
            body["termination_category"] = self.termination_category
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ForEachTaskErrorMessageStats into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.count is not None:
            body["count"] = self.count
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.termination_category is not None:
            body["termination_category"] = self.termination_category
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ForEachTaskErrorMessageStats:
        """Deserializes the ForEachTaskErrorMessageStats from a dictionary."""
        return cls(
            count=d.get("count", None),
            error_message=d.get("error_message", None),
            termination_category=d.get("termination_category", None),
        )


@dataclass
class ForEachTaskTaskRunStats:
    active_iterations: Optional[int] = None
    """Describes the iteration runs having an active lifecycle state or an active run sub state."""

    completed_iterations: Optional[int] = None
    """Describes the number of failed and succeeded iteration runs."""

    failed_iterations: Optional[int] = None
    """Describes the number of failed iteration runs."""

    scheduled_iterations: Optional[int] = None
    """Describes the number of iteration runs that have been scheduled."""

    succeeded_iterations: Optional[int] = None
    """Describes the number of succeeded iteration runs."""

    total_iterations: Optional[int] = None
    """Describes the length of the list of items to iterate over."""

    def as_dict(self) -> dict:
        """Serializes the ForEachTaskTaskRunStats into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.active_iterations is not None:
            body["active_iterations"] = self.active_iterations
        if self.completed_iterations is not None:
            body["completed_iterations"] = self.completed_iterations
        if self.failed_iterations is not None:
            body["failed_iterations"] = self.failed_iterations
        if self.scheduled_iterations is not None:
            body["scheduled_iterations"] = self.scheduled_iterations
        if self.succeeded_iterations is not None:
            body["succeeded_iterations"] = self.succeeded_iterations
        if self.total_iterations is not None:
            body["total_iterations"] = self.total_iterations
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ForEachTaskTaskRunStats into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.active_iterations is not None:
            body["active_iterations"] = self.active_iterations
        if self.completed_iterations is not None:
            body["completed_iterations"] = self.completed_iterations
        if self.failed_iterations is not None:
            body["failed_iterations"] = self.failed_iterations
        if self.scheduled_iterations is not None:
            body["scheduled_iterations"] = self.scheduled_iterations
        if self.succeeded_iterations is not None:
            body["succeeded_iterations"] = self.succeeded_iterations
        if self.total_iterations is not None:
            body["total_iterations"] = self.total_iterations
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ForEachTaskTaskRunStats:
        """Deserializes the ForEachTaskTaskRunStats from a dictionary."""
        return cls(
            active_iterations=d.get("active_iterations", None),
            completed_iterations=d.get("completed_iterations", None),
            failed_iterations=d.get("failed_iterations", None),
            scheduled_iterations=d.get("scheduled_iterations", None),
            succeeded_iterations=d.get("succeeded_iterations", None),
            total_iterations=d.get("total_iterations", None),
        )


class Format(Enum):

    MULTI_TASK = "MULTI_TASK"
    SINGLE_TASK = "SINGLE_TASK"


@dataclass
class GenAiComputeTask:
    dl_runtime_image: str
    """Runtime image"""

    command: Optional[str] = None
    """Command launcher to run the actual script, e.g. bash, python etc."""

    compute: Optional[ComputeConfig] = None

    mlflow_experiment_name: Optional[str] = None
    """Optional string containing the name of the MLflow experiment to log the run to. If name is not
    found, backend will create the mlflow experiment using the name."""

    source: Optional[Source] = None
    """Optional location type of the training script. When set to `WORKSPACE`, the script will be
    retrieved from the local Databricks workspace. When set to `GIT`, the script will be retrieved
    from a Git repository defined in `git_source`. If the value is empty, the task will use `GIT` if
    `git_source` is defined and `WORKSPACE` otherwise. * `WORKSPACE`: Script is located in
    Databricks workspace. * `GIT`: Script is located in cloud Git provider."""

    training_script_path: Optional[str] = None
    """The training script file path to be executed. Cloud file URIs (such as dbfs:/, s3:/, adls:/,
    gcs:/) and workspace paths are supported. For python files stored in the Databricks workspace,
    the path must be absolute and begin with `/`. For files stored in a remote repository, the path
    must be relative. This field is required."""

    yaml_parameters: Optional[str] = None
    """Optional string containing model parameters passed to the training script in yaml format. If
    present, then the content in yaml_parameters_file_path will be ignored."""

    yaml_parameters_file_path: Optional[str] = None
    """Optional path to a YAML file containing model parameters passed to the training script."""

    def as_dict(self) -> dict:
        """Serializes the GenAiComputeTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.command is not None:
            body["command"] = self.command
        if self.compute:
            body["compute"] = self.compute.as_dict()
        if self.dl_runtime_image is not None:
            body["dl_runtime_image"] = self.dl_runtime_image
        if self.mlflow_experiment_name is not None:
            body["mlflow_experiment_name"] = self.mlflow_experiment_name
        if self.source is not None:
            body["source"] = self.source.value
        if self.training_script_path is not None:
            body["training_script_path"] = self.training_script_path
        if self.yaml_parameters is not None:
            body["yaml_parameters"] = self.yaml_parameters
        if self.yaml_parameters_file_path is not None:
            body["yaml_parameters_file_path"] = self.yaml_parameters_file_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenAiComputeTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.command is not None:
            body["command"] = self.command
        if self.compute:
            body["compute"] = self.compute
        if self.dl_runtime_image is not None:
            body["dl_runtime_image"] = self.dl_runtime_image
        if self.mlflow_experiment_name is not None:
            body["mlflow_experiment_name"] = self.mlflow_experiment_name
        if self.source is not None:
            body["source"] = self.source
        if self.training_script_path is not None:
            body["training_script_path"] = self.training_script_path
        if self.yaml_parameters is not None:
            body["yaml_parameters"] = self.yaml_parameters
        if self.yaml_parameters_file_path is not None:
            body["yaml_parameters_file_path"] = self.yaml_parameters_file_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenAiComputeTask:
        """Deserializes the GenAiComputeTask from a dictionary."""
        return cls(
            command=d.get("command", None),
            compute=_from_dict(d, "compute", ComputeConfig),
            dl_runtime_image=d.get("dl_runtime_image", None),
            mlflow_experiment_name=d.get("mlflow_experiment_name", None),
            source=_enum(d, "source", Source),
            training_script_path=d.get("training_script_path", None),
            yaml_parameters=d.get("yaml_parameters", None),
            yaml_parameters_file_path=d.get("yaml_parameters_file_path", None),
        )


@dataclass
class GetJobPermissionLevelsResponse:
    permission_levels: Optional[List[JobPermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetJobPermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetJobPermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetJobPermissionLevelsResponse:
        """Deserializes the GetJobPermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", JobPermissionsDescription))


@dataclass
class GetPolicyComplianceResponse:
    is_compliant: Optional[bool] = None
    """Whether the job is compliant with its policies or not. Jobs could be out of compliance if a
    policy they are using was updated after the job was last edited and some of its job clusters no
    longer comply with their updated policies."""

    violations: Optional[Dict[str, str]] = None
    """An object containing key-value mappings representing the first 200 policy validation errors. The
    keys indicate the path where the policy validation error is occurring. An identifier for the job
    cluster is prepended to the path. The values indicate an error message describing the policy
    validation error."""

    def as_dict(self) -> dict:
        """Serializes the GetPolicyComplianceResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_compliant is not None:
            body["is_compliant"] = self.is_compliant
        if self.violations:
            body["violations"] = self.violations
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetPolicyComplianceResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_compliant is not None:
            body["is_compliant"] = self.is_compliant
        if self.violations:
            body["violations"] = self.violations
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetPolicyComplianceResponse:
        """Deserializes the GetPolicyComplianceResponse from a dictionary."""
        return cls(is_compliant=d.get("is_compliant", None), violations=d.get("violations", None))


class GitProvider(Enum):

    AWS_CODE_COMMIT = "awsCodeCommit"
    AZURE_DEV_OPS_SERVICES = "azureDevOpsServices"
    BITBUCKET_CLOUD = "bitbucketCloud"
    BITBUCKET_SERVER = "bitbucketServer"
    GIT_HUB = "gitHub"
    GIT_HUB_ENTERPRISE = "gitHubEnterprise"
    GIT_LAB = "gitLab"
    GIT_LAB_ENTERPRISE_EDITION = "gitLabEnterpriseEdition"


@dataclass
class GitSnapshot:
    """Read-only state of the remote repository at the time the job was run. This field is only
    included on job runs."""

    used_commit: Optional[str] = None
    """Commit that was used to execute the run. If git_branch was specified, this points to the HEAD of
    the branch at the time of the run; if git_tag was specified, this points to the commit the tag
    points to."""

    def as_dict(self) -> dict:
        """Serializes the GitSnapshot into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.used_commit is not None:
            body["used_commit"] = self.used_commit
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GitSnapshot into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.used_commit is not None:
            body["used_commit"] = self.used_commit
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GitSnapshot:
        """Deserializes the GitSnapshot from a dictionary."""
        return cls(used_commit=d.get("used_commit", None))


@dataclass
class GitSource:
    """An optional specification for a remote Git repository containing the source code used by tasks.
    Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.

    If `git_source` is set, these tasks retrieve the file from the remote repository by default.
    However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task.

    Note: dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks
    are used, `git_source` must be defined on the job."""

    git_url: str
    """URL of the repository to be cloned by this job."""

    git_provider: GitProvider
    """Unique identifier of the service used to host the Git repository. The value is case insensitive."""

    git_branch: Optional[str] = None
    """Name of the branch to be checked out and used by this job. This field cannot be specified in
    conjunction with git_tag or git_commit."""

    git_commit: Optional[str] = None
    """Commit to be checked out and used by this job. This field cannot be specified in conjunction
    with git_branch or git_tag."""

    git_snapshot: Optional[GitSnapshot] = None

    git_tag: Optional[str] = None
    """Name of the tag to be checked out and used by this job. This field cannot be specified in
    conjunction with git_branch or git_commit."""

    job_source: Optional[JobSource] = None
    """The source of the job specification in the remote repository when the job is source controlled."""

    sparse_checkout: Optional[SparseCheckout] = None

    def as_dict(self) -> dict:
        """Serializes the GitSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.git_branch is not None:
            body["git_branch"] = self.git_branch
        if self.git_commit is not None:
            body["git_commit"] = self.git_commit
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider.value
        if self.git_snapshot:
            body["git_snapshot"] = self.git_snapshot.as_dict()
        if self.git_tag is not None:
            body["git_tag"] = self.git_tag
        if self.git_url is not None:
            body["git_url"] = self.git_url
        if self.job_source:
            body["job_source"] = self.job_source.as_dict()
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GitSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.git_branch is not None:
            body["git_branch"] = self.git_branch
        if self.git_commit is not None:
            body["git_commit"] = self.git_commit
        if self.git_provider is not None:
            body["git_provider"] = self.git_provider
        if self.git_snapshot:
            body["git_snapshot"] = self.git_snapshot
        if self.git_tag is not None:
            body["git_tag"] = self.git_tag
        if self.git_url is not None:
            body["git_url"] = self.git_url
        if self.job_source:
            body["job_source"] = self.job_source
        if self.sparse_checkout:
            body["sparse_checkout"] = self.sparse_checkout
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GitSource:
        """Deserializes the GitSource from a dictionary."""
        return cls(
            git_branch=d.get("git_branch", None),
            git_commit=d.get("git_commit", None),
            git_provider=_enum(d, "git_provider", GitProvider),
            git_snapshot=_from_dict(d, "git_snapshot", GitSnapshot),
            git_tag=d.get("git_tag", None),
            git_url=d.get("git_url", None),
            job_source=_from_dict(d, "job_source", JobSource),
            sparse_checkout=_from_dict(d, "sparse_checkout", SparseCheckout),
        )


@dataclass
class Job:
    """Job was retrieved successfully."""

    created_time: Optional[int] = None
    """The time at which this job was created in epoch milliseconds (milliseconds since 1/1/1970 UTC)."""

    creator_user_name: Optional[str] = None
    """The creator user name. This field won’t be included in the response if the user has already
    been deleted."""

    effective_budget_policy_id: Optional[str] = None
    """The id of the budget policy used by this job for cost attribution purposes. This may be set
    through (in order of precedence): 1. Budget admins through the account or workspace console 2.
    Jobs UI in the job details page and Jobs API using `budget_policy_id` 3. Inferred default based
    on accessible budget policies of the run_as identity on job creation or modification."""

    effective_usage_policy_id: Optional[str] = None
    """The id of the usage policy used by this job for cost attribution purposes."""

    has_more: Optional[bool] = None
    """Indicates if the job has more array properties (`tasks`, `job_clusters`) that are not shown.
    They can be accessed via :method:jobs/get endpoint. It is only relevant for API 2.2
    :method:jobs/list requests with `expand_tasks=true`."""

    job_id: Optional[int] = None
    """The canonical identifier for this job."""

    next_page_token: Optional[str] = None
    """A token that can be used to list the next page of array properties."""

    run_as_user_name: Optional[str] = None
    """The email of an active workspace user or the application ID of a service principal that the job
    runs as. This value can be changed by setting the `run_as` field when creating or updating a
    job.
    
    By default, `run_as_user_name` is based on the current job settings and is set to the creator of
    the job if job access control is disabled or to the user with the `is_owner` permission if job
    access control is enabled."""

    settings: Optional[JobSettings] = None
    """Settings for this job and all of its runs. These settings can be updated using the `resetJob`
    method."""

    trigger_state: Optional[TriggerStateProto] = None
    """State of the trigger associated with the job."""

    def as_dict(self) -> dict:
        """Serializes the Job into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_time is not None:
            body["created_time"] = self.created_time
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.settings:
            body["settings"] = self.settings.as_dict()
        if self.trigger_state:
            body["trigger_state"] = self.trigger_state.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Job into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_time is not None:
            body["created_time"] = self.created_time
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.effective_budget_policy_id is not None:
            body["effective_budget_policy_id"] = self.effective_budget_policy_id
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.run_as_user_name is not None:
            body["run_as_user_name"] = self.run_as_user_name
        if self.settings:
            body["settings"] = self.settings
        if self.trigger_state:
            body["trigger_state"] = self.trigger_state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Job:
        """Deserializes the Job from a dictionary."""
        return cls(
            created_time=d.get("created_time", None),
            creator_user_name=d.get("creator_user_name", None),
            effective_budget_policy_id=d.get("effective_budget_policy_id", None),
            effective_usage_policy_id=d.get("effective_usage_policy_id", None),
            has_more=d.get("has_more", None),
            job_id=d.get("job_id", None),
            next_page_token=d.get("next_page_token", None),
            run_as_user_name=d.get("run_as_user_name", None),
            settings=_from_dict(d, "settings", JobSettings),
            trigger_state=_from_dict(d, "trigger_state", TriggerStateProto),
        )


@dataclass
class JobAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[JobPermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the JobAccessControlRequest into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the JobAccessControlRequest into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> JobAccessControlRequest:
        """Deserializes the JobAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", JobPermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class JobAccessControlResponse:
    all_permissions: Optional[List[JobPermission]] = None
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
        """Serializes the JobAccessControlResponse into a dictionary suitable for use as a JSON request body."""
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
        """Serializes the JobAccessControlResponse into a shallow dictionary of its immediate attributes."""
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
    def from_dict(cls, d: Dict[str, Any]) -> JobAccessControlResponse:
        """Deserializes the JobAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", JobPermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class JobCluster:
    job_cluster_key: str
    """A unique name for the job cluster. This field is required and must be unique within the job.
    `JobTaskSettings` may refer to this field to determine which cluster to launch for the task
    execution."""

    new_cluster: compute.ClusterSpec
    """If new_cluster, a description of a cluster that is created for each task."""

    def as_dict(self) -> dict:
        """Serializes the JobCluster into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobCluster into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobCluster:
        """Deserializes the JobCluster from a dictionary."""
        return cls(
            job_cluster_key=d.get("job_cluster_key", None),
            new_cluster=_from_dict(d, "new_cluster", compute.ClusterSpec),
        )


@dataclass
class JobCompliance:
    job_id: int
    """Canonical unique identifier for a job."""

    is_compliant: Optional[bool] = None
    """Whether this job is in compliance with the latest version of its policy."""

    violations: Optional[Dict[str, str]] = None
    """An object containing key-value mappings representing the first 200 policy validation errors. The
    keys indicate the path where the policy validation error is occurring. An identifier for the job
    cluster is prepended to the path. The values indicate an error message describing the policy
    validation error."""

    def as_dict(self) -> dict:
        """Serializes the JobCompliance into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_compliant is not None:
            body["is_compliant"] = self.is_compliant
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.violations:
            body["violations"] = self.violations
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobCompliance into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_compliant is not None:
            body["is_compliant"] = self.is_compliant
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.violations:
            body["violations"] = self.violations
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobCompliance:
        """Deserializes the JobCompliance from a dictionary."""
        return cls(
            is_compliant=d.get("is_compliant", None), job_id=d.get("job_id", None), violations=d.get("violations", None)
        )


@dataclass
class JobDeployment:
    kind: JobDeploymentKind
    """The kind of deployment that manages the job.
    
    * `BUNDLE`: The job is managed by Databricks Asset Bundle. * `SYSTEM_MANAGED`: The job is
    managed by Databricks and is read-only."""

    metadata_file_path: Optional[str] = None
    """Path of the file that contains deployment metadata."""

    def as_dict(self) -> dict:
        """Serializes the JobDeployment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.kind is not None:
            body["kind"] = self.kind.value
        if self.metadata_file_path is not None:
            body["metadata_file_path"] = self.metadata_file_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobDeployment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.kind is not None:
            body["kind"] = self.kind
        if self.metadata_file_path is not None:
            body["metadata_file_path"] = self.metadata_file_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobDeployment:
        """Deserializes the JobDeployment from a dictionary."""
        return cls(kind=_enum(d, "kind", JobDeploymentKind), metadata_file_path=d.get("metadata_file_path", None))


class JobDeploymentKind(Enum):
    """* `BUNDLE`: The job is managed by Databricks Asset Bundle. * `SYSTEM_MANAGED`: The job is
    managed by Databricks and is read-only."""

    BUNDLE = "BUNDLE"
    SYSTEM_MANAGED = "SYSTEM_MANAGED"


class JobEditMode(Enum):
    """Edit mode of the job.

    * `UI_LOCKED`: The job is in a locked UI state and cannot be modified. * `EDITABLE`: The job is
    in an editable state and can be modified."""

    EDITABLE = "EDITABLE"
    UI_LOCKED = "UI_LOCKED"


@dataclass
class JobEmailNotifications:
    no_alert_for_skipped_runs: Optional[bool] = None
    """If true, do not send email to recipients specified in `on_failure` if the run is skipped. This
    field is `deprecated`. Please use the `notification_settings.no_alert_for_skipped_runs` field."""

    on_duration_warning_threshold_exceeded: Optional[List[str]] = None
    """A list of email addresses to be notified when the duration of a run exceeds the threshold
    specified for the `RUN_DURATION_SECONDS` metric in the `health` field. If no rule for the
    `RUN_DURATION_SECONDS` metric is specified in the `health` field for the job, notifications are
    not sent."""

    on_failure: Optional[List[str]] = None
    """A list of email addresses to be notified when a run unsuccessfully completes. A run is
    considered to have completed unsuccessfully if it ends with an `INTERNAL_ERROR`
    `life_cycle_state` or a `FAILED`, or `TIMED_OUT` result_state. If this is not specified on job
    creation, reset, or update the list is empty, and notifications are not sent."""

    on_start: Optional[List[str]] = None
    """A list of email addresses to be notified when a run begins. If not specified on job creation,
    reset, or update, the list is empty, and notifications are not sent."""

    on_streaming_backlog_exceeded: Optional[List[str]] = None
    """A list of email addresses to notify when any streaming backlog thresholds are exceeded for any
    stream. Streaming backlog thresholds can be set in the `health` field using the following
    metrics: `STREAMING_BACKLOG_BYTES`, `STREAMING_BACKLOG_RECORDS`, `STREAMING_BACKLOG_SECONDS`, or
    `STREAMING_BACKLOG_FILES`. Alerting is based on the 10-minute average of these metrics. If the
    issue persists, notifications are resent every 30 minutes."""

    on_success: Optional[List[str]] = None
    """A list of email addresses to be notified when a run successfully completes. A run is considered
    to have completed successfully if it ends with a `TERMINATED` `life_cycle_state` and a `SUCCESS`
    result_state. If not specified on job creation, reset, or update, the list is empty, and
    notifications are not sent."""

    def as_dict(self) -> dict:
        """Serializes the JobEmailNotifications into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        if self.on_duration_warning_threshold_exceeded:
            body["on_duration_warning_threshold_exceeded"] = [v for v in self.on_duration_warning_threshold_exceeded]
        if self.on_failure:
            body["on_failure"] = [v for v in self.on_failure]
        if self.on_start:
            body["on_start"] = [v for v in self.on_start]
        if self.on_streaming_backlog_exceeded:
            body["on_streaming_backlog_exceeded"] = [v for v in self.on_streaming_backlog_exceeded]
        if self.on_success:
            body["on_success"] = [v for v in self.on_success]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobEmailNotifications into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        if self.on_duration_warning_threshold_exceeded:
            body["on_duration_warning_threshold_exceeded"] = self.on_duration_warning_threshold_exceeded
        if self.on_failure:
            body["on_failure"] = self.on_failure
        if self.on_start:
            body["on_start"] = self.on_start
        if self.on_streaming_backlog_exceeded:
            body["on_streaming_backlog_exceeded"] = self.on_streaming_backlog_exceeded
        if self.on_success:
            body["on_success"] = self.on_success
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobEmailNotifications:
        """Deserializes the JobEmailNotifications from a dictionary."""
        return cls(
            no_alert_for_skipped_runs=d.get("no_alert_for_skipped_runs", None),
            on_duration_warning_threshold_exceeded=d.get("on_duration_warning_threshold_exceeded", None),
            on_failure=d.get("on_failure", None),
            on_start=d.get("on_start", None),
            on_streaming_backlog_exceeded=d.get("on_streaming_backlog_exceeded", None),
            on_success=d.get("on_success", None),
        )


@dataclass
class JobEnvironment:
    environment_key: str
    """The key of an environment. It has to be unique within a job."""

    spec: Optional[compute.Environment] = None

    def as_dict(self) -> dict:
        """Serializes the JobEnvironment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.spec:
            body["spec"] = self.spec.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobEnvironment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.spec:
            body["spec"] = self.spec
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobEnvironment:
        """Deserializes the JobEnvironment from a dictionary."""
        return cls(environment_key=d.get("environment_key", None), spec=_from_dict(d, "spec", compute.Environment))


@dataclass
class JobNotificationSettings:
    no_alert_for_canceled_runs: Optional[bool] = None
    """If true, do not send notifications to recipients specified in `on_failure` if the run is
    canceled."""

    no_alert_for_skipped_runs: Optional[bool] = None
    """If true, do not send notifications to recipients specified in `on_failure` if the run is
    skipped."""

    def as_dict(self) -> dict:
        """Serializes the JobNotificationSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.no_alert_for_canceled_runs is not None:
            body["no_alert_for_canceled_runs"] = self.no_alert_for_canceled_runs
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobNotificationSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.no_alert_for_canceled_runs is not None:
            body["no_alert_for_canceled_runs"] = self.no_alert_for_canceled_runs
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobNotificationSettings:
        """Deserializes the JobNotificationSettings from a dictionary."""
        return cls(
            no_alert_for_canceled_runs=d.get("no_alert_for_canceled_runs", None),
            no_alert_for_skipped_runs=d.get("no_alert_for_skipped_runs", None),
        )


@dataclass
class JobParameter:
    default: Optional[str] = None
    """The optional default value of the parameter"""

    name: Optional[str] = None
    """The name of the parameter"""

    value: Optional[str] = None
    """The value used in the run"""

    def as_dict(self) -> dict:
        """Serializes the JobParameter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default is not None:
            body["default"] = self.default
        if self.name is not None:
            body["name"] = self.name
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobParameter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default is not None:
            body["default"] = self.default
        if self.name is not None:
            body["name"] = self.name
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobParameter:
        """Deserializes the JobParameter from a dictionary."""
        return cls(default=d.get("default", None), name=d.get("name", None), value=d.get("value", None))


@dataclass
class JobParameterDefinition:
    name: str
    """The name of the defined parameter. May only contain alphanumeric characters, `_`, `-`, and `.`"""

    default: str
    """Default value of the parameter."""

    def as_dict(self) -> dict:
        """Serializes the JobParameterDefinition into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.default is not None:
            body["default"] = self.default
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobParameterDefinition into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.default is not None:
            body["default"] = self.default
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobParameterDefinition:
        """Deserializes the JobParameterDefinition from a dictionary."""
        return cls(default=d.get("default", None), name=d.get("name", None))


@dataclass
class JobPermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[JobPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the JobPermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobPermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobPermission:
        """Deserializes the JobPermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", JobPermissionLevel),
        )


class JobPermissionLevel(Enum):
    """Permission level"""

    CAN_MANAGE = "CAN_MANAGE"
    CAN_MANAGE_RUN = "CAN_MANAGE_RUN"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"


@dataclass
class JobPermissions:
    access_control_list: Optional[List[JobAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the JobPermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobPermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobPermissions:
        """Deserializes the JobPermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", JobAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class JobPermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[JobPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the JobPermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobPermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobPermissionsDescription:
        """Deserializes the JobPermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None), permission_level=_enum(d, "permission_level", JobPermissionLevel)
        )


@dataclass
class JobRunAs:
    """Write-only setting. Specifies the user or service principal that the job runs as. If not
    specified, the job runs as the user who created the job.

    Either `user_name` or `service_principal_name` should be specified. If not, an error is thrown."""

    group_name: Optional[str] = None
    """Group name of an account group assigned to the workspace. Setting this field requires being a
    member of the group."""

    service_principal_name: Optional[str] = None
    """Application ID of an active service principal. Setting this field requires the
    `servicePrincipal/user` role."""

    user_name: Optional[str] = None
    """The email of an active workspace user. Non-admin users can only set this field to their own
    email."""

    def as_dict(self) -> dict:
        """Serializes the JobRunAs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobRunAs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobRunAs:
        """Deserializes the JobRunAs from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class JobSettings:
    budget_policy_id: Optional[str] = None
    """The id of the user specified budget policy to use for this job. If not specified, a default
    budget policy may be applied when creating or modifying the job. See
    `effective_budget_policy_id` for the budget policy used by this workload."""

    continuous: Optional[Continuous] = None
    """An optional continuous property for this job. The continuous property will ensure that there is
    always one run executing. Only one of `schedule` and `continuous` can be used."""

    deployment: Optional[JobDeployment] = None
    """Deployment information for jobs managed by external sources."""

    description: Optional[str] = None
    """An optional description for the job. The maximum length is 27700 characters in UTF-8 encoding."""

    edit_mode: Optional[JobEditMode] = None
    """Edit mode of the job.
    
    * `UI_LOCKED`: The job is in a locked UI state and cannot be modified. * `EDITABLE`: The job is
    in an editable state and can be modified."""

    email_notifications: Optional[JobEmailNotifications] = None
    """An optional set of email addresses that is notified when runs of this job begin or complete as
    well as when this job is deleted."""

    environments: Optional[List[JobEnvironment]] = None
    """A list of task execution environment specifications that can be referenced by serverless tasks
    of this job. For serverless notebook tasks, if the environment_key is not specified, the
    notebook environment will be used if present. If a jobs environment is specified, it will
    override the notebook environment. For other serverless tasks, the task environment is required
    to be specified using environment_key in the task settings."""

    format: Optional[Format] = None
    """Used to tell what is the format of the job. This field is ignored in Create/Update/Reset calls.
    When using the Jobs API 2.1 this value is always set to `"MULTI_TASK"`."""

    git_source: Optional[GitSource] = None
    """An optional specification for a remote Git repository containing the source code used by tasks.
    Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.
    
    If `git_source` is set, these tasks retrieve the file from the remote repository by default.
    However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task.
    
    Note: dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks
    are used, `git_source` must be defined on the job."""

    health: Optional[JobsHealthRules] = None

    job_clusters: Optional[List[JobCluster]] = None
    """A list of job cluster specifications that can be shared and reused by tasks of this job.
    Libraries cannot be declared in a shared job cluster. You must declare dependent libraries in
    task settings."""

    max_concurrent_runs: Optional[int] = None
    """An optional maximum allowed number of concurrent runs of the job. Set this value if you want to
    be able to execute multiple runs of the same job concurrently. This is useful for example if you
    trigger your job on a frequent schedule and want to allow consecutive runs to overlap with each
    other, or if you want to trigger multiple runs which differ by their input parameters. This
    setting affects only new runs. For example, suppose the job’s concurrency is 4 and there are 4
    concurrent active runs. Then setting the concurrency to 3 won’t kill any of the active runs.
    However, from then on, new runs are skipped unless there are fewer than 3 active runs. This
    value cannot exceed 1000. Setting this value to `0` causes all new runs to be skipped."""

    name: Optional[str] = None
    """An optional name for the job. The maximum length is 4096 bytes in UTF-8 encoding."""

    notification_settings: Optional[JobNotificationSettings] = None
    """Optional notification settings that are used when sending notifications to each of the
    `email_notifications` and `webhook_notifications` for this job."""

    parameters: Optional[List[JobParameterDefinition]] = None
    """Job-level parameter definitions"""

    performance_target: Optional[PerformanceTarget] = None
    """The performance mode on a serverless job. This field determines the level of compute performance
    or cost-efficiency for the run. The performance target does not apply to tasks that run on
    Serverless GPU compute.
    
    * `STANDARD`: Enables cost-efficient execution of serverless workloads. *
    `PERFORMANCE_OPTIMIZED`: Prioritizes fast startup and execution times through rapid scaling and
    optimized cluster performance."""

    queue: Optional[QueueSettings] = None
    """The queue settings of the job."""

    run_as: Optional[JobRunAs] = None
    """The user or service principal that the job runs as, if specified in the request. This field
    indicates the explicit configuration of `run_as` for the job. To find the value in all cases,
    explicit or implicit, use `run_as_user_name`."""

    schedule: Optional[CronSchedule] = None
    """An optional periodic schedule for this job. The default behavior is that the job only runs when
    triggered by clicking “Run Now” in the Jobs UI or sending an API request to `runNow`."""

    tags: Optional[Dict[str, str]] = None
    """A map of tags associated with the job. These are forwarded to the cluster as cluster tags for
    jobs clusters, and are subject to the same limitations as cluster tags. A maximum of 25 tags can
    be added to the job."""

    tasks: Optional[List[Task]] = None
    """A list of task specifications to be executed by this job. It supports up to 1000 elements in
    write endpoints (:method:jobs/create, :method:jobs/reset, :method:jobs/update,
    :method:jobs/submit). Read endpoints return only 100 tasks. If more than 100 tasks are
    available, you can paginate through them using :method:jobs/get. Use the `next_page_token` field
    at the object root to determine if more results are available."""

    timeout_seconds: Optional[int] = None
    """An optional timeout applied to each run of this job. A value of `0` means no timeout."""

    trigger: Optional[TriggerSettings] = None
    """A configuration to trigger a run when certain conditions are met. The default behavior is that
    the job runs only when triggered by clicking “Run Now” in the Jobs UI or sending an API
    request to `runNow`."""

    usage_policy_id: Optional[str] = None
    """The id of the user specified usage policy to use for this job. If not specified, a default usage
    policy may be applied when creating or modifying the job. See `effective_usage_policy_id` for
    the usage policy used by this workload."""

    webhook_notifications: Optional[WebhookNotifications] = None
    """A collection of system notification IDs to notify when runs of this job begin or complete."""

    def as_dict(self) -> dict:
        """Serializes the JobSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.continuous:
            body["continuous"] = self.continuous.as_dict()
        if self.deployment:
            body["deployment"] = self.deployment.as_dict()
        if self.description is not None:
            body["description"] = self.description
        if self.edit_mode is not None:
            body["edit_mode"] = self.edit_mode.value
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications.as_dict()
        if self.environments:
            body["environments"] = [v.as_dict() for v in self.environments]
        if self.format is not None:
            body["format"] = self.format.value
        if self.git_source:
            body["git_source"] = self.git_source.as_dict()
        if self.health:
            body["health"] = self.health.as_dict()
        if self.job_clusters:
            body["job_clusters"] = [v.as_dict() for v in self.job_clusters]
        if self.max_concurrent_runs is not None:
            body["max_concurrent_runs"] = self.max_concurrent_runs
        if self.name is not None:
            body["name"] = self.name
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings.as_dict()
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.performance_target is not None:
            body["performance_target"] = self.performance_target.value
        if self.queue:
            body["queue"] = self.queue.as_dict()
        if self.run_as:
            body["run_as"] = self.run_as.as_dict()
        if self.schedule:
            body["schedule"] = self.schedule.as_dict()
        if self.tags:
            body["tags"] = self.tags
        if self.tasks:
            body["tasks"] = [v.as_dict() for v in self.tasks]
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.trigger:
            body["trigger"] = self.trigger.as_dict()
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.continuous:
            body["continuous"] = self.continuous
        if self.deployment:
            body["deployment"] = self.deployment
        if self.description is not None:
            body["description"] = self.description
        if self.edit_mode is not None:
            body["edit_mode"] = self.edit_mode
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications
        if self.environments:
            body["environments"] = self.environments
        if self.format is not None:
            body["format"] = self.format
        if self.git_source:
            body["git_source"] = self.git_source
        if self.health:
            body["health"] = self.health
        if self.job_clusters:
            body["job_clusters"] = self.job_clusters
        if self.max_concurrent_runs is not None:
            body["max_concurrent_runs"] = self.max_concurrent_runs
        if self.name is not None:
            body["name"] = self.name
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings
        if self.parameters:
            body["parameters"] = self.parameters
        if self.performance_target is not None:
            body["performance_target"] = self.performance_target
        if self.queue:
            body["queue"] = self.queue
        if self.run_as:
            body["run_as"] = self.run_as
        if self.schedule:
            body["schedule"] = self.schedule
        if self.tags:
            body["tags"] = self.tags
        if self.tasks:
            body["tasks"] = self.tasks
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.trigger:
            body["trigger"] = self.trigger
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobSettings:
        """Deserializes the JobSettings from a dictionary."""
        return cls(
            budget_policy_id=d.get("budget_policy_id", None),
            continuous=_from_dict(d, "continuous", Continuous),
            deployment=_from_dict(d, "deployment", JobDeployment),
            description=d.get("description", None),
            edit_mode=_enum(d, "edit_mode", JobEditMode),
            email_notifications=_from_dict(d, "email_notifications", JobEmailNotifications),
            environments=_repeated_dict(d, "environments", JobEnvironment),
            format=_enum(d, "format", Format),
            git_source=_from_dict(d, "git_source", GitSource),
            health=_from_dict(d, "health", JobsHealthRules),
            job_clusters=_repeated_dict(d, "job_clusters", JobCluster),
            max_concurrent_runs=d.get("max_concurrent_runs", None),
            name=d.get("name", None),
            notification_settings=_from_dict(d, "notification_settings", JobNotificationSettings),
            parameters=_repeated_dict(d, "parameters", JobParameterDefinition),
            performance_target=_enum(d, "performance_target", PerformanceTarget),
            queue=_from_dict(d, "queue", QueueSettings),
            run_as=_from_dict(d, "run_as", JobRunAs),
            schedule=_from_dict(d, "schedule", CronSchedule),
            tags=d.get("tags", None),
            tasks=_repeated_dict(d, "tasks", Task),
            timeout_seconds=d.get("timeout_seconds", None),
            trigger=_from_dict(d, "trigger", TriggerSettings),
            usage_policy_id=d.get("usage_policy_id", None),
            webhook_notifications=_from_dict(d, "webhook_notifications", WebhookNotifications),
        )


@dataclass
class JobSource:
    """The source of the job specification in the remote repository when the job is source controlled."""

    job_config_path: str
    """Path of the job YAML file that contains the job specification."""

    import_from_git_branch: str
    """Name of the branch which the job is imported from."""

    dirty_state: Optional[JobSourceDirtyState] = None
    """Dirty state indicates the job is not fully synced with the job specification in the remote
    repository.
    
    Possible values are: * `NOT_SYNCED`: The job is not yet synced with the remote job
    specification. Import the remote job specification from UI to make the job fully synced. *
    `DISCONNECTED`: The job is temporary disconnected from the remote job specification and is
    allowed for live edit. Import the remote job specification again from UI to make the job fully
    synced."""

    def as_dict(self) -> dict:
        """Serializes the JobSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dirty_state is not None:
            body["dirty_state"] = self.dirty_state.value
        if self.import_from_git_branch is not None:
            body["import_from_git_branch"] = self.import_from_git_branch
        if self.job_config_path is not None:
            body["job_config_path"] = self.job_config_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dirty_state is not None:
            body["dirty_state"] = self.dirty_state
        if self.import_from_git_branch is not None:
            body["import_from_git_branch"] = self.import_from_git_branch
        if self.job_config_path is not None:
            body["job_config_path"] = self.job_config_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobSource:
        """Deserializes the JobSource from a dictionary."""
        return cls(
            dirty_state=_enum(d, "dirty_state", JobSourceDirtyState),
            import_from_git_branch=d.get("import_from_git_branch", None),
            job_config_path=d.get("job_config_path", None),
        )


class JobSourceDirtyState(Enum):
    """Dirty state indicates the job is not fully synced with the job specification in the remote
    repository.

    Possible values are: * `NOT_SYNCED`: The job is not yet synced with the remote job
    specification. Import the remote job specification from UI to make the job fully synced. *
    `DISCONNECTED`: The job is temporary disconnected from the remote job specification and is
    allowed for live edit. Import the remote job specification again from UI to make the job fully
    synced."""

    DISCONNECTED = "DISCONNECTED"
    NOT_SYNCED = "NOT_SYNCED"


class JobsHealthMetric(Enum):
    """Specifies the health metric that is being evaluated for a particular health rule.

    * `RUN_DURATION_SECONDS`: Expected total time for a run in seconds. * `STREAMING_BACKLOG_BYTES`:
    An estimate of the maximum bytes of data waiting to be consumed across all streams. This metric
    is in Public Preview. * `STREAMING_BACKLOG_RECORDS`: An estimate of the maximum offset lag
    across all streams. This metric is in Public Preview. * `STREAMING_BACKLOG_SECONDS`: An estimate
    of the maximum consumer delay across all streams. This metric is in Public Preview. *
    `STREAMING_BACKLOG_FILES`: An estimate of the maximum number of outstanding files across all
    streams. This metric is in Public Preview."""

    RUN_DURATION_SECONDS = "RUN_DURATION_SECONDS"
    STREAMING_BACKLOG_BYTES = "STREAMING_BACKLOG_BYTES"
    STREAMING_BACKLOG_FILES = "STREAMING_BACKLOG_FILES"
    STREAMING_BACKLOG_RECORDS = "STREAMING_BACKLOG_RECORDS"
    STREAMING_BACKLOG_SECONDS = "STREAMING_BACKLOG_SECONDS"


class JobsHealthOperator(Enum):
    """Specifies the operator used to compare the health metric value with the specified threshold."""

    GREATER_THAN = "GREATER_THAN"


@dataclass
class JobsHealthRule:
    metric: JobsHealthMetric

    op: JobsHealthOperator

    value: int
    """Specifies the threshold value that the health metric should obey to satisfy the health rule."""

    def as_dict(self) -> dict:
        """Serializes the JobsHealthRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metric is not None:
            body["metric"] = self.metric.value
        if self.op is not None:
            body["op"] = self.op.value
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobsHealthRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metric is not None:
            body["metric"] = self.metric
        if self.op is not None:
            body["op"] = self.op
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobsHealthRule:
        """Deserializes the JobsHealthRule from a dictionary."""
        return cls(
            metric=_enum(d, "metric", JobsHealthMetric),
            op=_enum(d, "op", JobsHealthOperator),
            value=d.get("value", None),
        )


@dataclass
class JobsHealthRules:
    """An optional set of health rules that can be defined for this job."""

    rules: Optional[List[JobsHealthRule]] = None

    def as_dict(self) -> dict:
        """Serializes the JobsHealthRules into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.rules:
            body["rules"] = [v.as_dict() for v in self.rules]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobsHealthRules into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.rules:
            body["rules"] = self.rules
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobsHealthRules:
        """Deserializes the JobsHealthRules from a dictionary."""
        return cls(rules=_repeated_dict(d, "rules", JobsHealthRule))


@dataclass
class ListJobComplianceForPolicyResponse:
    jobs: Optional[List[JobCompliance]] = None
    """A list of jobs and their policy compliance statuses."""

    next_page_token: Optional[str] = None
    """This field represents the pagination token to retrieve the next page of results. If this field
    is not in the response, it means no further results for the request."""

    prev_page_token: Optional[str] = None
    """This field represents the pagination token to retrieve the previous page of results. If this
    field is not in the response, it means no further results for the request."""

    def as_dict(self) -> dict:
        """Serializes the ListJobComplianceForPolicyResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.jobs:
            body["jobs"] = [v.as_dict() for v in self.jobs]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListJobComplianceForPolicyResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.jobs:
            body["jobs"] = self.jobs
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListJobComplianceForPolicyResponse:
        """Deserializes the ListJobComplianceForPolicyResponse from a dictionary."""
        return cls(
            jobs=_repeated_dict(d, "jobs", JobCompliance),
            next_page_token=d.get("next_page_token", None),
            prev_page_token=d.get("prev_page_token", None),
        )


@dataclass
class ListJobsResponse:
    """List of jobs was retrieved successfully."""

    has_more: Optional[bool] = None
    """If true, additional jobs matching the provided filter are available for listing."""

    jobs: Optional[List[BaseJob]] = None
    """The list of jobs. Only included in the response if there are jobs to list."""

    next_page_token: Optional[str] = None
    """A token that can be used to list the next page of jobs (if applicable)."""

    prev_page_token: Optional[str] = None
    """A token that can be used to list the previous page of jobs (if applicable)."""

    def as_dict(self) -> dict:
        """Serializes the ListJobsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.jobs:
            body["jobs"] = [v.as_dict() for v in self.jobs]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListJobsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.jobs:
            body["jobs"] = self.jobs
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListJobsResponse:
        """Deserializes the ListJobsResponse from a dictionary."""
        return cls(
            has_more=d.get("has_more", None),
            jobs=_repeated_dict(d, "jobs", BaseJob),
            next_page_token=d.get("next_page_token", None),
            prev_page_token=d.get("prev_page_token", None),
        )


@dataclass
class ListRunsResponse:
    """List of runs was retrieved successfully."""

    has_more: Optional[bool] = None
    """If true, additional runs matching the provided filter are available for listing."""

    next_page_token: Optional[str] = None
    """A token that can be used to list the next page of runs (if applicable)."""

    prev_page_token: Optional[str] = None
    """A token that can be used to list the previous page of runs (if applicable)."""

    runs: Optional[List[BaseRun]] = None
    """A list of runs, from most recently started to least. Only included in the response if there are
    runs to list."""

    def as_dict(self) -> dict:
        """Serializes the ListRunsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        if self.runs:
            body["runs"] = [v.as_dict() for v in self.runs]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListRunsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.prev_page_token is not None:
            body["prev_page_token"] = self.prev_page_token
        if self.runs:
            body["runs"] = self.runs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListRunsResponse:
        """Deserializes the ListRunsResponse from a dictionary."""
        return cls(
            has_more=d.get("has_more", None),
            next_page_token=d.get("next_page_token", None),
            prev_page_token=d.get("prev_page_token", None),
            runs=_repeated_dict(d, "runs", BaseRun),
        )


@dataclass
class ModelTriggerConfiguration:
    condition: ModelTriggerConfigurationCondition
    """The condition based on which to trigger a job run."""

    aliases: Optional[List[str]] = None
    """Aliases of the model versions to monitor. Can only be used in conjunction with condition
    MODEL_ALIAS_SET."""

    min_time_between_triggers_seconds: Optional[int] = None
    """If set, the trigger starts a run only after the specified amount of time has passed since the
    last time the trigger fired. The minimum allowed value is 60 seconds."""

    securable_name: Optional[str] = None
    """Name of the securable to monitor ("mycatalog.myschema.mymodel" in the case of model-level
    triggers, "mycatalog.myschema" in the case of schema-level triggers) or empty in the case of
    metastore-level triggers."""

    wait_after_last_change_seconds: Optional[int] = None
    """If set, the trigger starts a run only after no model updates have occurred for the specified
    time and can be used to wait for a series of model updates before triggering a run. The minimum
    allowed value is 60 seconds."""

    def as_dict(self) -> dict:
        """Serializes the ModelTriggerConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.aliases:
            body["aliases"] = [v for v in self.aliases]
        if self.condition is not None:
            body["condition"] = self.condition.value
        if self.min_time_between_triggers_seconds is not None:
            body["min_time_between_triggers_seconds"] = self.min_time_between_triggers_seconds
        if self.securable_name is not None:
            body["securable_name"] = self.securable_name
        if self.wait_after_last_change_seconds is not None:
            body["wait_after_last_change_seconds"] = self.wait_after_last_change_seconds
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelTriggerConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.aliases:
            body["aliases"] = self.aliases
        if self.condition is not None:
            body["condition"] = self.condition
        if self.min_time_between_triggers_seconds is not None:
            body["min_time_between_triggers_seconds"] = self.min_time_between_triggers_seconds
        if self.securable_name is not None:
            body["securable_name"] = self.securable_name
        if self.wait_after_last_change_seconds is not None:
            body["wait_after_last_change_seconds"] = self.wait_after_last_change_seconds
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelTriggerConfiguration:
        """Deserializes the ModelTriggerConfiguration from a dictionary."""
        return cls(
            aliases=d.get("aliases", None),
            condition=_enum(d, "condition", ModelTriggerConfigurationCondition),
            min_time_between_triggers_seconds=d.get("min_time_between_triggers_seconds", None),
            securable_name=d.get("securable_name", None),
            wait_after_last_change_seconds=d.get("wait_after_last_change_seconds", None),
        )


class ModelTriggerConfigurationCondition(Enum):

    MODEL_ALIAS_SET = "MODEL_ALIAS_SET"
    MODEL_CREATED = "MODEL_CREATED"
    MODEL_VERSION_READY = "MODEL_VERSION_READY"


@dataclass
class NotebookOutput:
    result: Optional[str] = None
    """The value passed to
    [dbutils.notebook.exit()](/notebooks/notebook-workflows.html#notebook-workflows-exit).
    Databricks restricts this API to return the first 5 MB of the value. For a larger result, your
    job can store the results in a cloud storage service. This field is absent if
    `dbutils.notebook.exit()` was never called."""

    truncated: Optional[bool] = None
    """Whether or not the result was truncated."""

    def as_dict(self) -> dict:
        """Serializes the NotebookOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.result is not None:
            body["result"] = self.result
        if self.truncated is not None:
            body["truncated"] = self.truncated
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotebookOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.result is not None:
            body["result"] = self.result
        if self.truncated is not None:
            body["truncated"] = self.truncated
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotebookOutput:
        """Deserializes the NotebookOutput from a dictionary."""
        return cls(result=d.get("result", None), truncated=d.get("truncated", None))


@dataclass
class NotebookTask:
    notebook_path: str
    """The path of the notebook to be run in the Databricks workspace or remote repository. For
    notebooks stored in the Databricks workspace, the path must be absolute and begin with a slash.
    For notebooks stored in a remote repository, the path must be relative. This field is required."""

    base_parameters: Optional[Dict[str, str]] = None
    """Base parameters to be used for each run of this job. If the run is initiated by a call to
    :method:jobs/run Now with parameters specified, the two parameters maps are merged. If the same
    key is specified in `base_parameters` and in `run-now`, the value from `run-now` is used. Use
    [Task parameter variables] to set parameters containing information about job runs.
    
    If the notebook takes a parameter that is not specified in the job’s `base_parameters` or the
    `run-now` override parameters, the default value from the notebook is used.
    
    Retrieve these parameters in a notebook using [dbutils.widgets.get].
    
    The JSON representation of this field cannot exceed 1MB.
    
    [Task parameter variables]: https://docs.databricks.com/jobs.html#parameter-variables
    [dbutils.widgets.get]: https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-widgets"""

    source: Optional[Source] = None
    """Optional location type of the notebook. When set to `WORKSPACE`, the notebook will be retrieved
    from the local Databricks workspace. When set to `GIT`, the notebook will be retrieved from a
    Git repository defined in `git_source`. If the value is empty, the task will use `GIT` if
    `git_source` is defined and `WORKSPACE` otherwise. * `WORKSPACE`: Notebook is located in
    Databricks workspace. * `GIT`: Notebook is located in cloud Git provider."""

    warehouse_id: Optional[str] = None
    """Optional `warehouse_id` to run the notebook on a SQL warehouse. Classic SQL warehouses are NOT
    supported, please use serverless or pro SQL warehouses.
    
    Note that SQL warehouses only support SQL cells; if the notebook contains non-SQL cells, the run
    will fail."""

    def as_dict(self) -> dict:
        """Serializes the NotebookTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.base_parameters:
            body["base_parameters"] = self.base_parameters
        if self.notebook_path is not None:
            body["notebook_path"] = self.notebook_path
        if self.source is not None:
            body["source"] = self.source.value
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the NotebookTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.base_parameters:
            body["base_parameters"] = self.base_parameters
        if self.notebook_path is not None:
            body["notebook_path"] = self.notebook_path
        if self.source is not None:
            body["source"] = self.source
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> NotebookTask:
        """Deserializes the NotebookTask from a dictionary."""
        return cls(
            base_parameters=d.get("base_parameters", None),
            notebook_path=d.get("notebook_path", None),
            source=_enum(d, "source", Source),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class OutputSchemaInfo:
    """Stores the catalog name, schema name, and the output schema expiration time for the clean room
    run."""

    catalog_name: Optional[str] = None

    expiration_time: Optional[int] = None
    """The expiration time for the output schema as a Unix timestamp in milliseconds."""

    schema_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the OutputSchemaInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OutputSchemaInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OutputSchemaInfo:
        """Deserializes the OutputSchemaInfo from a dictionary."""
        return cls(
            catalog_name=d.get("catalog_name", None),
            expiration_time=d.get("expiration_time", None),
            schema_name=d.get("schema_name", None),
        )


class PauseStatus(Enum):

    PAUSED = "PAUSED"
    UNPAUSED = "UNPAUSED"


class PerformanceTarget(Enum):
    """PerformanceTarget defines how performant (lower latency) or cost efficient the execution of run
    on serverless compute should be. The performance mode on the job or pipeline should map to a
    performance setting that is passed to Cluster Manager (see cluster-common PerformanceTarget)."""

    PERFORMANCE_OPTIMIZED = "PERFORMANCE_OPTIMIZED"
    STANDARD = "STANDARD"


@dataclass
class PeriodicTriggerConfiguration:
    interval: int
    """The interval at which the trigger should run."""

    unit: PeriodicTriggerConfigurationTimeUnit
    """The unit of time for the interval."""

    def as_dict(self) -> dict:
        """Serializes the PeriodicTriggerConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.interval is not None:
            body["interval"] = self.interval
        if self.unit is not None:
            body["unit"] = self.unit.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PeriodicTriggerConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.interval is not None:
            body["interval"] = self.interval
        if self.unit is not None:
            body["unit"] = self.unit
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PeriodicTriggerConfiguration:
        """Deserializes the PeriodicTriggerConfiguration from a dictionary."""
        return cls(interval=d.get("interval", None), unit=_enum(d, "unit", PeriodicTriggerConfigurationTimeUnit))


class PeriodicTriggerConfigurationTimeUnit(Enum):

    DAYS = "DAYS"
    HOURS = "HOURS"
    WEEKS = "WEEKS"


@dataclass
class PipelineParams:
    full_refresh: Optional[bool] = None
    """If true, triggers a full refresh on the delta live table."""

    def as_dict(self) -> dict:
        """Serializes the PipelineParams into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.full_refresh is not None:
            body["full_refresh"] = self.full_refresh
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineParams into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.full_refresh is not None:
            body["full_refresh"] = self.full_refresh
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineParams:
        """Deserializes the PipelineParams from a dictionary."""
        return cls(full_refresh=d.get("full_refresh", None))


@dataclass
class PipelineTask:
    pipeline_id: str
    """The full name of the pipeline task to execute."""

    full_refresh: Optional[bool] = None
    """If true, triggers a full refresh on the delta live table."""

    def as_dict(self) -> dict:
        """Serializes the PipelineTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.full_refresh is not None:
            body["full_refresh"] = self.full_refresh
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PipelineTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.full_refresh is not None:
            body["full_refresh"] = self.full_refresh
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PipelineTask:
        """Deserializes the PipelineTask from a dictionary."""
        return cls(full_refresh=d.get("full_refresh", None), pipeline_id=d.get("pipeline_id", None))


@dataclass
class PowerBiModel:
    authentication_method: Optional[AuthenticationMethod] = None
    """How the published Power BI model authenticates to Databricks"""

    model_name: Optional[str] = None
    """The name of the Power BI model"""

    overwrite_existing: Optional[bool] = None
    """Whether to overwrite existing Power BI models"""

    storage_mode: Optional[StorageMode] = None
    """The default storage mode of the Power BI model"""

    workspace_name: Optional[str] = None
    """The name of the Power BI workspace of the model"""

    def as_dict(self) -> dict:
        """Serializes the PowerBiModel into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.authentication_method is not None:
            body["authentication_method"] = self.authentication_method.value
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.overwrite_existing is not None:
            body["overwrite_existing"] = self.overwrite_existing
        if self.storage_mode is not None:
            body["storage_mode"] = self.storage_mode.value
        if self.workspace_name is not None:
            body["workspace_name"] = self.workspace_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PowerBiModel into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.authentication_method is not None:
            body["authentication_method"] = self.authentication_method
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.overwrite_existing is not None:
            body["overwrite_existing"] = self.overwrite_existing
        if self.storage_mode is not None:
            body["storage_mode"] = self.storage_mode
        if self.workspace_name is not None:
            body["workspace_name"] = self.workspace_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PowerBiModel:
        """Deserializes the PowerBiModel from a dictionary."""
        return cls(
            authentication_method=_enum(d, "authentication_method", AuthenticationMethod),
            model_name=d.get("model_name", None),
            overwrite_existing=d.get("overwrite_existing", None),
            storage_mode=_enum(d, "storage_mode", StorageMode),
            workspace_name=d.get("workspace_name", None),
        )


@dataclass
class PowerBiTable:
    catalog: Optional[str] = None
    """The catalog name in Databricks"""

    name: Optional[str] = None
    """The table name in Databricks"""

    schema: Optional[str] = None
    """The schema name in Databricks"""

    storage_mode: Optional[StorageMode] = None
    """The Power BI storage mode of the table"""

    def as_dict(self) -> dict:
        """Serializes the PowerBiTable into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        if self.storage_mode is not None:
            body["storage_mode"] = self.storage_mode.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PowerBiTable into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.name is not None:
            body["name"] = self.name
        if self.schema is not None:
            body["schema"] = self.schema
        if self.storage_mode is not None:
            body["storage_mode"] = self.storage_mode
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PowerBiTable:
        """Deserializes the PowerBiTable from a dictionary."""
        return cls(
            catalog=d.get("catalog", None),
            name=d.get("name", None),
            schema=d.get("schema", None),
            storage_mode=_enum(d, "storage_mode", StorageMode),
        )


@dataclass
class PowerBiTask:
    connection_resource_name: Optional[str] = None
    """The resource name of the UC connection to authenticate from Databricks to Power BI"""

    power_bi_model: Optional[PowerBiModel] = None
    """The semantic model to update"""

    refresh_after_update: Optional[bool] = None
    """Whether the model should be refreshed after the update"""

    tables: Optional[List[PowerBiTable]] = None
    """The tables to be exported to Power BI"""

    warehouse_id: Optional[str] = None
    """The SQL warehouse ID to use as the Power BI data source"""

    def as_dict(self) -> dict:
        """Serializes the PowerBiTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.connection_resource_name is not None:
            body["connection_resource_name"] = self.connection_resource_name
        if self.power_bi_model:
            body["power_bi_model"] = self.power_bi_model.as_dict()
        if self.refresh_after_update is not None:
            body["refresh_after_update"] = self.refresh_after_update
        if self.tables:
            body["tables"] = [v.as_dict() for v in self.tables]
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PowerBiTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.connection_resource_name is not None:
            body["connection_resource_name"] = self.connection_resource_name
        if self.power_bi_model:
            body["power_bi_model"] = self.power_bi_model
        if self.refresh_after_update is not None:
            body["refresh_after_update"] = self.refresh_after_update
        if self.tables:
            body["tables"] = self.tables
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PowerBiTask:
        """Deserializes the PowerBiTask from a dictionary."""
        return cls(
            connection_resource_name=d.get("connection_resource_name", None),
            power_bi_model=_from_dict(d, "power_bi_model", PowerBiModel),
            refresh_after_update=d.get("refresh_after_update", None),
            tables=_repeated_dict(d, "tables", PowerBiTable),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class PythonWheelTask:
    package_name: str
    """Name of the package to execute"""

    entry_point: str
    """Named entry point to use, if it does not exist in the metadata of the package it executes the
    function from the package directly using `$packageName.$entryPoint()`"""

    named_parameters: Optional[Dict[str, str]] = None
    """Command-line parameters passed to Python wheel task in the form of `["--name=task",
    "--data=dbfs:/path/to/data.json"]`. Leave it empty if `parameters` is not null."""

    parameters: Optional[List[str]] = None
    """Command-line parameters passed to Python wheel task. Leave it empty if `named_parameters` is not
    null."""

    def as_dict(self) -> dict:
        """Serializes the PythonWheelTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.entry_point is not None:
            body["entry_point"] = self.entry_point
        if self.named_parameters:
            body["named_parameters"] = self.named_parameters
        if self.package_name is not None:
            body["package_name"] = self.package_name
        if self.parameters:
            body["parameters"] = [v for v in self.parameters]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PythonWheelTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.entry_point is not None:
            body["entry_point"] = self.entry_point
        if self.named_parameters:
            body["named_parameters"] = self.named_parameters
        if self.package_name is not None:
            body["package_name"] = self.package_name
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PythonWheelTask:
        """Deserializes the PythonWheelTask from a dictionary."""
        return cls(
            entry_point=d.get("entry_point", None),
            named_parameters=d.get("named_parameters", None),
            package_name=d.get("package_name", None),
            parameters=d.get("parameters", None),
        )


@dataclass
class QueueDetails:
    code: Optional[QueueDetailsCodeCode] = None

    message: Optional[str] = None
    """A descriptive message with the queuing details. This field is unstructured, and its exact format
    is subject to change."""

    def as_dict(self) -> dict:
        """Serializes the QueueDetails into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.code is not None:
            body["code"] = self.code.value
        if self.message is not None:
            body["message"] = self.message
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueueDetails into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.code is not None:
            body["code"] = self.code
        if self.message is not None:
            body["message"] = self.message
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueueDetails:
        """Deserializes the QueueDetails from a dictionary."""
        return cls(code=_enum(d, "code", QueueDetailsCodeCode), message=d.get("message", None))


class QueueDetailsCodeCode(Enum):
    """The reason for queuing the run. * `ACTIVE_RUNS_LIMIT_REACHED`: The run was queued due to
    reaching the workspace limit of active task runs. * `MAX_CONCURRENT_RUNS_REACHED`: The run was
    queued due to reaching the per-job limit of concurrent job runs. *
    `ACTIVE_RUN_JOB_TASKS_LIMIT_REACHED`: The run was queued due to reaching the workspace limit of
    active run job tasks."""

    ACTIVE_RUNS_LIMIT_REACHED = "ACTIVE_RUNS_LIMIT_REACHED"
    ACTIVE_RUN_JOB_TASKS_LIMIT_REACHED = "ACTIVE_RUN_JOB_TASKS_LIMIT_REACHED"
    MAX_CONCURRENT_RUNS_REACHED = "MAX_CONCURRENT_RUNS_REACHED"


@dataclass
class QueueSettings:
    enabled: bool
    """If true, enable queueing for the job. This is a required field."""

    def as_dict(self) -> dict:
        """Serializes the QueueSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueueSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enabled is not None:
            body["enabled"] = self.enabled
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueueSettings:
        """Deserializes the QueueSettings from a dictionary."""
        return cls(enabled=d.get("enabled", None))


@dataclass
class RepairHistoryItem:
    effective_performance_target: Optional[PerformanceTarget] = None
    """The actual performance target used by the serverless run during execution. This can differ from
    the client-set performance target on the request depending on whether the performance mode is
    supported by the job type.
    
    * `STANDARD`: Enables cost-efficient execution of serverless workloads. *
    `PERFORMANCE_OPTIMIZED`: Prioritizes fast startup and execution times through rapid scaling and
    optimized cluster performance."""

    end_time: Optional[int] = None
    """The end time of the (repaired) run."""

    id: Optional[int] = None
    """The ID of the repair. Only returned for the items that represent a repair in `repair_history`."""

    start_time: Optional[int] = None
    """The start time of the (repaired) run."""

    state: Optional[RunState] = None
    """Deprecated. Please use the `status` field instead."""

    status: Optional[RunStatus] = None

    task_run_ids: Optional[List[int]] = None
    """The run IDs of the task runs that ran as part of this repair history item."""

    type: Optional[RepairHistoryItemType] = None
    """The repair history item type. Indicates whether a run is the original run or a repair run."""

    def as_dict(self) -> dict:
        """Serializes the RepairHistoryItem into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target.value
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.id is not None:
            body["id"] = self.id
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.task_run_ids:
            body["task_run_ids"] = [v for v in self.task_run_ids]
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepairHistoryItem into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.id is not None:
            body["id"] = self.id
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state
        if self.status:
            body["status"] = self.status
        if self.task_run_ids:
            body["task_run_ids"] = self.task_run_ids
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepairHistoryItem:
        """Deserializes the RepairHistoryItem from a dictionary."""
        return cls(
            effective_performance_target=_enum(d, "effective_performance_target", PerformanceTarget),
            end_time=d.get("end_time", None),
            id=d.get("id", None),
            start_time=d.get("start_time", None),
            state=_from_dict(d, "state", RunState),
            status=_from_dict(d, "status", RunStatus),
            task_run_ids=d.get("task_run_ids", None),
            type=_enum(d, "type", RepairHistoryItemType),
        )


class RepairHistoryItemType(Enum):
    """The repair history item type. Indicates whether a run is the original run or a repair run."""

    ORIGINAL = "ORIGINAL"
    REPAIR = "REPAIR"


@dataclass
class RepairRunResponse:
    """Run repair was initiated."""

    repair_id: Optional[int] = None
    """The ID of the repair. Must be provided in subsequent repairs using the `latest_repair_id` field
    to ensure sequential repairs."""

    def as_dict(self) -> dict:
        """Serializes the RepairRunResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.repair_id is not None:
            body["repair_id"] = self.repair_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepairRunResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.repair_id is not None:
            body["repair_id"] = self.repair_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepairRunResponse:
        """Deserializes the RepairRunResponse from a dictionary."""
        return cls(repair_id=d.get("repair_id", None))


@dataclass
class ResolvedConditionTaskValues:
    left: Optional[str] = None

    right: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedConditionTaskValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.left is not None:
            body["left"] = self.left
        if self.right is not None:
            body["right"] = self.right
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedConditionTaskValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.left is not None:
            body["left"] = self.left
        if self.right is not None:
            body["right"] = self.right
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedConditionTaskValues:
        """Deserializes the ResolvedConditionTaskValues from a dictionary."""
        return cls(left=d.get("left", None), right=d.get("right", None))


@dataclass
class ResolvedDbtTaskValues:
    commands: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedDbtTaskValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.commands:
            body["commands"] = [v for v in self.commands]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedDbtTaskValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.commands:
            body["commands"] = self.commands
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedDbtTaskValues:
        """Deserializes the ResolvedDbtTaskValues from a dictionary."""
        return cls(commands=d.get("commands", None))


@dataclass
class ResolvedNotebookTaskValues:
    base_parameters: Optional[Dict[str, str]] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedNotebookTaskValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.base_parameters:
            body["base_parameters"] = self.base_parameters
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedNotebookTaskValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.base_parameters:
            body["base_parameters"] = self.base_parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedNotebookTaskValues:
        """Deserializes the ResolvedNotebookTaskValues from a dictionary."""
        return cls(base_parameters=d.get("base_parameters", None))


@dataclass
class ResolvedParamPairValues:
    parameters: Optional[Dict[str, str]] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedParamPairValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedParamPairValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedParamPairValues:
        """Deserializes the ResolvedParamPairValues from a dictionary."""
        return cls(parameters=d.get("parameters", None))


@dataclass
class ResolvedPythonWheelTaskValues:
    named_parameters: Optional[Dict[str, str]] = None

    parameters: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedPythonWheelTaskValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.named_parameters:
            body["named_parameters"] = self.named_parameters
        if self.parameters:
            body["parameters"] = [v for v in self.parameters]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedPythonWheelTaskValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.named_parameters:
            body["named_parameters"] = self.named_parameters
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedPythonWheelTaskValues:
        """Deserializes the ResolvedPythonWheelTaskValues from a dictionary."""
        return cls(named_parameters=d.get("named_parameters", None), parameters=d.get("parameters", None))


@dataclass
class ResolvedRunJobTaskValues:
    job_parameters: Optional[Dict[str, str]] = None

    parameters: Optional[Dict[str, str]] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedRunJobTaskValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.job_parameters:
            body["job_parameters"] = self.job_parameters
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedRunJobTaskValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.job_parameters:
            body["job_parameters"] = self.job_parameters
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedRunJobTaskValues:
        """Deserializes the ResolvedRunJobTaskValues from a dictionary."""
        return cls(job_parameters=d.get("job_parameters", None), parameters=d.get("parameters", None))


@dataclass
class ResolvedStringParamsValues:
    parameters: Optional[List[str]] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedStringParamsValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.parameters:
            body["parameters"] = [v for v in self.parameters]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedStringParamsValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedStringParamsValues:
        """Deserializes the ResolvedStringParamsValues from a dictionary."""
        return cls(parameters=d.get("parameters", None))


@dataclass
class ResolvedValues:
    condition_task: Optional[ResolvedConditionTaskValues] = None

    dbt_task: Optional[ResolvedDbtTaskValues] = None

    notebook_task: Optional[ResolvedNotebookTaskValues] = None

    python_wheel_task: Optional[ResolvedPythonWheelTaskValues] = None

    run_job_task: Optional[ResolvedRunJobTaskValues] = None

    simulation_task: Optional[ResolvedParamPairValues] = None

    spark_jar_task: Optional[ResolvedStringParamsValues] = None

    spark_python_task: Optional[ResolvedStringParamsValues] = None

    spark_submit_task: Optional[ResolvedStringParamsValues] = None

    sql_task: Optional[ResolvedParamPairValues] = None

    def as_dict(self) -> dict:
        """Serializes the ResolvedValues into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.condition_task:
            body["condition_task"] = self.condition_task.as_dict()
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task.as_dict()
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task.as_dict()
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task.as_dict()
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task.as_dict()
        if self.simulation_task:
            body["simulation_task"] = self.simulation_task.as_dict()
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task.as_dict()
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task.as_dict()
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task.as_dict()
        if self.sql_task:
            body["sql_task"] = self.sql_task.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ResolvedValues into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.condition_task:
            body["condition_task"] = self.condition_task
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task
        if self.simulation_task:
            body["simulation_task"] = self.simulation_task
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task
        if self.sql_task:
            body["sql_task"] = self.sql_task
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ResolvedValues:
        """Deserializes the ResolvedValues from a dictionary."""
        return cls(
            condition_task=_from_dict(d, "condition_task", ResolvedConditionTaskValues),
            dbt_task=_from_dict(d, "dbt_task", ResolvedDbtTaskValues),
            notebook_task=_from_dict(d, "notebook_task", ResolvedNotebookTaskValues),
            python_wheel_task=_from_dict(d, "python_wheel_task", ResolvedPythonWheelTaskValues),
            run_job_task=_from_dict(d, "run_job_task", ResolvedRunJobTaskValues),
            simulation_task=_from_dict(d, "simulation_task", ResolvedParamPairValues),
            spark_jar_task=_from_dict(d, "spark_jar_task", ResolvedStringParamsValues),
            spark_python_task=_from_dict(d, "spark_python_task", ResolvedStringParamsValues),
            spark_submit_task=_from_dict(d, "spark_submit_task", ResolvedStringParamsValues),
            sql_task=_from_dict(d, "sql_task", ResolvedParamPairValues),
        )


@dataclass
class Run:
    """Run was retrieved successfully"""

    attempt_number: Optional[int] = None
    """The sequence number of this run attempt for a triggered job run. The initial attempt of a run
    has an attempt_number of 0. If the initial run attempt fails, and the job has a retry policy
    (`max_retries` > 0), subsequent runs are created with an `original_attempt_run_id` of the
    original attempt’s ID and an incrementing `attempt_number`. Runs are retried only until they
    succeed, and the maximum `attempt_number` is the same as the `max_retries` value for the job."""

    cleanup_duration: Optional[int] = None
    """The time in milliseconds it took to terminate the cluster and clean up any associated artifacts.
    The duration of a task run is the sum of the `setup_duration`, `execution_duration`, and the
    `cleanup_duration`. The `cleanup_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    cluster_instance: Optional[ClusterInstance] = None
    """The cluster used for this run. If the run is specified to use a new cluster, this field is set
    once the Jobs service has requested a cluster for the run."""

    cluster_spec: Optional[ClusterSpec] = None
    """A snapshot of the job’s cluster specification when this run was created."""

    creator_user_name: Optional[str] = None
    """The creator user name. This field won’t be included in the response if the user has already
    been deleted."""

    description: Optional[str] = None
    """Description of the run"""

    effective_performance_target: Optional[PerformanceTarget] = None
    """The actual performance target used by the serverless run during execution. This can differ from
    the client-set performance target on the request depending on whether the performance mode is
    supported by the job type.
    
    * `STANDARD`: Enables cost-efficient execution of serverless workloads. *
    `PERFORMANCE_OPTIMIZED`: Prioritizes fast startup and execution times through rapid scaling and
    optimized cluster performance."""

    effective_usage_policy_id: Optional[str] = None
    """The id of the usage policy used by this run for cost attribution purposes."""

    end_time: Optional[int] = None
    """The time at which this run ended in epoch milliseconds (milliseconds since 1/1/1970 UTC). This
    field is set to 0 if the job is still running."""

    execution_duration: Optional[int] = None
    """The time in milliseconds it took to execute the commands in the JAR or notebook until they
    completed, failed, timed out, were cancelled, or encountered an unexpected error. The duration
    of a task run is the sum of the `setup_duration`, `execution_duration`, and the
    `cleanup_duration`. The `execution_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    git_source: Optional[GitSource] = None
    """An optional specification for a remote Git repository containing the source code used by tasks.
    Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.
    
    If `git_source` is set, these tasks retrieve the file from the remote repository by default.
    However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task.
    
    Note: dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks
    are used, `git_source` must be defined on the job."""

    has_more: Optional[bool] = None
    """Indicates if the run has more array properties (`tasks`, `job_clusters`) that are not shown.
    They can be accessed via :method:jobs/getrun endpoint. It is only relevant for API 2.2
    :method:jobs/listruns requests with `expand_tasks=true`."""

    iterations: Optional[List[RunTask]] = None
    """Only populated by for-each iterations. The parent for-each task is located in tasks array."""

    job_clusters: Optional[List[JobCluster]] = None
    """A list of job cluster specifications that can be shared and reused by tasks of this job.
    Libraries cannot be declared in a shared job cluster. You must declare dependent libraries in
    task settings. If more than 100 job clusters are available, you can paginate through them using
    :method:jobs/getrun."""

    job_id: Optional[int] = None
    """The canonical identifier of the job that contains this run."""

    job_parameters: Optional[List[JobParameter]] = None
    """Job-level parameters used in the run"""

    job_run_id: Optional[int] = None
    """ID of the job run that this run belongs to. For legacy and single-task job runs the field is
    populated with the job run ID. For task runs, the field is populated with the ID of the job run
    that the task run belongs to."""

    next_page_token: Optional[str] = None
    """A token that can be used to list the next page of array properties."""

    number_in_job: Optional[int] = None
    """A unique identifier for this job run. This is set to the same value as `run_id`."""

    original_attempt_run_id: Optional[int] = None
    """If this run is a retry of a prior run attempt, this field contains the run_id of the original
    attempt; otherwise, it is the same as the run_id."""

    overriding_parameters: Optional[RunParameters] = None
    """The parameters used for this run."""

    queue_duration: Optional[int] = None
    """The time in milliseconds that the run has spent in the queue."""

    repair_history: Optional[List[RepairHistoryItem]] = None
    """The repair history of the run."""

    run_duration: Optional[int] = None
    """The time in milliseconds it took the job run and all of its repairs to finish."""

    run_id: Optional[int] = None
    """The canonical identifier of the run. This ID is unique across all runs of all jobs."""

    run_name: Optional[str] = None
    """An optional name for the run. The maximum length is 4096 bytes in UTF-8 encoding."""

    run_page_url: Optional[str] = None
    """The URL to the detail page of the run."""

    run_type: Optional[RunType] = None

    schedule: Optional[CronSchedule] = None
    """The cron schedule that triggered this run if it was triggered by the periodic scheduler."""

    setup_duration: Optional[int] = None
    """The time in milliseconds it took to set up the cluster. For runs that run on new clusters this
    is the cluster creation time, for runs that run on existing clusters this time should be very
    short. The duration of a task run is the sum of the `setup_duration`, `execution_duration`, and
    the `cleanup_duration`. The `setup_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    start_time: Optional[int] = None
    """The time at which this run was started in epoch milliseconds (milliseconds since 1/1/1970 UTC).
    This may not be the time when the job task starts executing, for example, if the job is
    scheduled to run on a new cluster, this is the time the cluster creation call is issued."""

    state: Optional[RunState] = None
    """Deprecated. Please use the `status` field instead."""

    status: Optional[RunStatus] = None

    tasks: Optional[List[RunTask]] = None
    """The list of tasks performed by the run. Each task has its own `run_id` which you can use to call
    `JobsGetOutput` to retrieve the run resutls. If more than 100 tasks are available, you can
    paginate through them using :method:jobs/getrun. Use the `next_page_token` field at the object
    root to determine if more results are available."""

    trigger: Optional[TriggerType] = None

    trigger_info: Optional[TriggerInfo] = None

    def as_dict(self) -> dict:
        """Serializes the Run into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.attempt_number is not None:
            body["attempt_number"] = self.attempt_number
        if self.cleanup_duration is not None:
            body["cleanup_duration"] = self.cleanup_duration
        if self.cluster_instance:
            body["cluster_instance"] = self.cluster_instance.as_dict()
        if self.cluster_spec:
            body["cluster_spec"] = self.cluster_spec.as_dict()
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.description is not None:
            body["description"] = self.description
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target.value
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.execution_duration is not None:
            body["execution_duration"] = self.execution_duration
        if self.git_source:
            body["git_source"] = self.git_source.as_dict()
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.iterations:
            body["iterations"] = [v.as_dict() for v in self.iterations]
        if self.job_clusters:
            body["job_clusters"] = [v.as_dict() for v in self.job_clusters]
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_parameters:
            body["job_parameters"] = [v.as_dict() for v in self.job_parameters]
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.number_in_job is not None:
            body["number_in_job"] = self.number_in_job
        if self.original_attempt_run_id is not None:
            body["original_attempt_run_id"] = self.original_attempt_run_id
        if self.overriding_parameters:
            body["overriding_parameters"] = self.overriding_parameters.as_dict()
        if self.queue_duration is not None:
            body["queue_duration"] = self.queue_duration
        if self.repair_history:
            body["repair_history"] = [v.as_dict() for v in self.repair_history]
        if self.run_duration is not None:
            body["run_duration"] = self.run_duration
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_name is not None:
            body["run_name"] = self.run_name
        if self.run_page_url is not None:
            body["run_page_url"] = self.run_page_url
        if self.run_type is not None:
            body["run_type"] = self.run_type.value
        if self.schedule:
            body["schedule"] = self.schedule.as_dict()
        if self.setup_duration is not None:
            body["setup_duration"] = self.setup_duration
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.tasks:
            body["tasks"] = [v.as_dict() for v in self.tasks]
        if self.trigger is not None:
            body["trigger"] = self.trigger.value
        if self.trigger_info:
            body["trigger_info"] = self.trigger_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Run into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.attempt_number is not None:
            body["attempt_number"] = self.attempt_number
        if self.cleanup_duration is not None:
            body["cleanup_duration"] = self.cleanup_duration
        if self.cluster_instance:
            body["cluster_instance"] = self.cluster_instance
        if self.cluster_spec:
            body["cluster_spec"] = self.cluster_spec
        if self.creator_user_name is not None:
            body["creator_user_name"] = self.creator_user_name
        if self.description is not None:
            body["description"] = self.description
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target
        if self.effective_usage_policy_id is not None:
            body["effective_usage_policy_id"] = self.effective_usage_policy_id
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.execution_duration is not None:
            body["execution_duration"] = self.execution_duration
        if self.git_source:
            body["git_source"] = self.git_source
        if self.has_more is not None:
            body["has_more"] = self.has_more
        if self.iterations:
            body["iterations"] = self.iterations
        if self.job_clusters:
            body["job_clusters"] = self.job_clusters
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_parameters:
            body["job_parameters"] = self.job_parameters
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.number_in_job is not None:
            body["number_in_job"] = self.number_in_job
        if self.original_attempt_run_id is not None:
            body["original_attempt_run_id"] = self.original_attempt_run_id
        if self.overriding_parameters:
            body["overriding_parameters"] = self.overriding_parameters
        if self.queue_duration is not None:
            body["queue_duration"] = self.queue_duration
        if self.repair_history:
            body["repair_history"] = self.repair_history
        if self.run_duration is not None:
            body["run_duration"] = self.run_duration
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_name is not None:
            body["run_name"] = self.run_name
        if self.run_page_url is not None:
            body["run_page_url"] = self.run_page_url
        if self.run_type is not None:
            body["run_type"] = self.run_type
        if self.schedule:
            body["schedule"] = self.schedule
        if self.setup_duration is not None:
            body["setup_duration"] = self.setup_duration
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state
        if self.status:
            body["status"] = self.status
        if self.tasks:
            body["tasks"] = self.tasks
        if self.trigger is not None:
            body["trigger"] = self.trigger
        if self.trigger_info:
            body["trigger_info"] = self.trigger_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Run:
        """Deserializes the Run from a dictionary."""
        return cls(
            attempt_number=d.get("attempt_number", None),
            cleanup_duration=d.get("cleanup_duration", None),
            cluster_instance=_from_dict(d, "cluster_instance", ClusterInstance),
            cluster_spec=_from_dict(d, "cluster_spec", ClusterSpec),
            creator_user_name=d.get("creator_user_name", None),
            description=d.get("description", None),
            effective_performance_target=_enum(d, "effective_performance_target", PerformanceTarget),
            effective_usage_policy_id=d.get("effective_usage_policy_id", None),
            end_time=d.get("end_time", None),
            execution_duration=d.get("execution_duration", None),
            git_source=_from_dict(d, "git_source", GitSource),
            has_more=d.get("has_more", None),
            iterations=_repeated_dict(d, "iterations", RunTask),
            job_clusters=_repeated_dict(d, "job_clusters", JobCluster),
            job_id=d.get("job_id", None),
            job_parameters=_repeated_dict(d, "job_parameters", JobParameter),
            job_run_id=d.get("job_run_id", None),
            next_page_token=d.get("next_page_token", None),
            number_in_job=d.get("number_in_job", None),
            original_attempt_run_id=d.get("original_attempt_run_id", None),
            overriding_parameters=_from_dict(d, "overriding_parameters", RunParameters),
            queue_duration=d.get("queue_duration", None),
            repair_history=_repeated_dict(d, "repair_history", RepairHistoryItem),
            run_duration=d.get("run_duration", None),
            run_id=d.get("run_id", None),
            run_name=d.get("run_name", None),
            run_page_url=d.get("run_page_url", None),
            run_type=_enum(d, "run_type", RunType),
            schedule=_from_dict(d, "schedule", CronSchedule),
            setup_duration=d.get("setup_duration", None),
            start_time=d.get("start_time", None),
            state=_from_dict(d, "state", RunState),
            status=_from_dict(d, "status", RunStatus),
            tasks=_repeated_dict(d, "tasks", RunTask),
            trigger=_enum(d, "trigger", TriggerType),
            trigger_info=_from_dict(d, "trigger_info", TriggerInfo),
        )


@dataclass
class RunConditionTask:
    op: ConditionTaskOp
    """* `EQUAL_TO`, `NOT_EQUAL` operators perform string comparison of their operands. This means that
    `“12.0” == “12”` will evaluate to `false`. * `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`,
    `LESS_THAN`, `LESS_THAN_OR_EQUAL` operators perform numeric comparison of their operands.
    `“12.0” >= “12”` will evaluate to `true`, `“10.0” >= “12”` will evaluate to
    `false`.
    
    The boolean comparison to task values can be implemented with operators `EQUAL_TO`, `NOT_EQUAL`.
    If a task value was set to a boolean value, it will be serialized to `“true”` or
    `“false”` for the comparison."""

    left: str
    """The left operand of the condition task. Can be either a string value or a job state or parameter
    reference."""

    right: str
    """The right operand of the condition task. Can be either a string value or a job state or
    parameter reference."""

    outcome: Optional[str] = None
    """The condition expression evaluation result. Filled in if the task was successfully completed.
    Can be `"true"` or `"false"`"""

    def as_dict(self) -> dict:
        """Serializes the RunConditionTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.left is not None:
            body["left"] = self.left
        if self.op is not None:
            body["op"] = self.op.value
        if self.outcome is not None:
            body["outcome"] = self.outcome
        if self.right is not None:
            body["right"] = self.right
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunConditionTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.left is not None:
            body["left"] = self.left
        if self.op is not None:
            body["op"] = self.op
        if self.outcome is not None:
            body["outcome"] = self.outcome
        if self.right is not None:
            body["right"] = self.right
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunConditionTask:
        """Deserializes the RunConditionTask from a dictionary."""
        return cls(
            left=d.get("left", None),
            op=_enum(d, "op", ConditionTaskOp),
            outcome=d.get("outcome", None),
            right=d.get("right", None),
        )


@dataclass
class RunForEachTask:
    inputs: str
    """Array for task to iterate on. This can be a JSON string or a reference to an array parameter."""

    task: Task
    """Configuration for the task that will be run for each element in the array"""

    concurrency: Optional[int] = None
    """An optional maximum allowed number of concurrent runs of the task. Set this value if you want to
    be able to execute multiple runs of the task concurrently."""

    stats: Optional[ForEachStats] = None
    """Read only field. Populated for GetRun and ListRuns RPC calls and stores the execution stats of
    an For each task"""

    def as_dict(self) -> dict:
        """Serializes the RunForEachTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.concurrency is not None:
            body["concurrency"] = self.concurrency
        if self.inputs is not None:
            body["inputs"] = self.inputs
        if self.stats:
            body["stats"] = self.stats.as_dict()
        if self.task:
            body["task"] = self.task.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunForEachTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.concurrency is not None:
            body["concurrency"] = self.concurrency
        if self.inputs is not None:
            body["inputs"] = self.inputs
        if self.stats:
            body["stats"] = self.stats
        if self.task:
            body["task"] = self.task
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunForEachTask:
        """Deserializes the RunForEachTask from a dictionary."""
        return cls(
            concurrency=d.get("concurrency", None),
            inputs=d.get("inputs", None),
            stats=_from_dict(d, "stats", ForEachStats),
            task=_from_dict(d, "task", Task),
        )


class RunIf(Enum):
    """An optional value indicating the condition that determines whether the task should be run once
    its dependencies have been completed. When omitted, defaults to `ALL_SUCCESS`.

    Possible values are: * `ALL_SUCCESS`: All dependencies have executed and succeeded *
    `AT_LEAST_ONE_SUCCESS`: At least one dependency has succeeded * `NONE_FAILED`: None of the
    dependencies have failed and at least one was executed * `ALL_DONE`: All dependencies have been
    completed * `AT_LEAST_ONE_FAILED`: At least one dependency failed * `ALL_FAILED`: ALl
    dependencies have failed"""

    ALL_DONE = "ALL_DONE"
    ALL_FAILED = "ALL_FAILED"
    ALL_SUCCESS = "ALL_SUCCESS"
    AT_LEAST_ONE_FAILED = "AT_LEAST_ONE_FAILED"
    AT_LEAST_ONE_SUCCESS = "AT_LEAST_ONE_SUCCESS"
    NONE_FAILED = "NONE_FAILED"


@dataclass
class RunJobOutput:
    run_id: Optional[int] = None
    """The run id of the triggered job run"""

    def as_dict(self) -> dict:
        """Serializes the RunJobOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunJobOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunJobOutput:
        """Deserializes the RunJobOutput from a dictionary."""
        return cls(run_id=d.get("run_id", None))


@dataclass
class RunJobTask:
    job_id: int
    """ID of the job to trigger."""

    dbt_commands: Optional[List[str]] = None
    """An array of commands to execute for jobs with the dbt task, for example `"dbt_commands": ["dbt
    deps", "dbt seed", "dbt deps", "dbt seed", "dbt run"]`
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    jar_params: Optional[List[str]] = None
    """A list of parameters for jobs with Spark JAR tasks, for example `"jar_params": ["john doe",
    "35"]`. The parameters are used to invoke the main function of the main class specified in the
    Spark JAR task. If not specified upon `run-now`, it defaults to an empty list. jar_params cannot
    be specified in conjunction with notebook_params. The JSON representation of this field (for
    example `{"jar_params":["john doe","35"]}`) cannot exceed 10,000 bytes.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    job_parameters: Optional[Dict[str, str]] = None
    """Job-level parameters used to trigger the job."""

    notebook_params: Optional[Dict[str, str]] = None
    """A map from keys to values for jobs with notebook task, for example `"notebook_params": {"name":
    "john doe", "age": "35"}`. The map is passed to the notebook and is accessible through the
    [dbutils.widgets.get] function.
    
    If not specified upon `run-now`, the triggered run uses the job’s base parameters.
    
    notebook_params cannot be specified in conjunction with jar_params.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    The JSON representation of this field (for example `{"notebook_params":{"name":"john
    doe","age":"35"}}`) cannot exceed 10,000 bytes.
    
    [dbutils.widgets.get]: https://docs.databricks.com/dev-tools/databricks-utils.html
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    pipeline_params: Optional[PipelineParams] = None
    """Controls whether the pipeline should perform a full refresh"""

    python_named_params: Optional[Dict[str, str]] = None

    python_params: Optional[List[str]] = None
    """A list of parameters for jobs with Python tasks, for example `"python_params": ["john doe",
    "35"]`. The parameters are passed to Python file as command-line parameters. If specified upon
    `run-now`, it would overwrite the parameters specified in job setting. The JSON representation
    of this field (for example `{"python_params":["john doe","35"]}`) cannot exceed 10,000 bytes.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    Important
    
    These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
    returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
    emojis.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    spark_submit_params: Optional[List[str]] = None
    """A list of parameters for jobs with spark submit task, for example `"spark_submit_params":
    ["--class", "org.apache.spark.examples.SparkPi"]`. The parameters are passed to spark-submit
    script as command-line parameters. If specified upon `run-now`, it would overwrite the
    parameters specified in job setting. The JSON representation of this field (for example
    `{"python_params":["john doe","35"]}`) cannot exceed 10,000 bytes.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    Important
    
    These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
    returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
    emojis.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    sql_params: Optional[Dict[str, str]] = None
    """A map from keys to values for jobs with SQL task, for example `"sql_params": {"name": "john
    doe", "age": "35"}`. The SQL alert task does not support custom parameters.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    def as_dict(self) -> dict:
        """Serializes the RunJobTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dbt_commands:
            body["dbt_commands"] = [v for v in self.dbt_commands]
        if self.jar_params:
            body["jar_params"] = [v for v in self.jar_params]
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_parameters:
            body["job_parameters"] = self.job_parameters
        if self.notebook_params:
            body["notebook_params"] = self.notebook_params
        if self.pipeline_params:
            body["pipeline_params"] = self.pipeline_params.as_dict()
        if self.python_named_params:
            body["python_named_params"] = self.python_named_params
        if self.python_params:
            body["python_params"] = [v for v in self.python_params]
        if self.spark_submit_params:
            body["spark_submit_params"] = [v for v in self.spark_submit_params]
        if self.sql_params:
            body["sql_params"] = self.sql_params
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunJobTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dbt_commands:
            body["dbt_commands"] = self.dbt_commands
        if self.jar_params:
            body["jar_params"] = self.jar_params
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_parameters:
            body["job_parameters"] = self.job_parameters
        if self.notebook_params:
            body["notebook_params"] = self.notebook_params
        if self.pipeline_params:
            body["pipeline_params"] = self.pipeline_params
        if self.python_named_params:
            body["python_named_params"] = self.python_named_params
        if self.python_params:
            body["python_params"] = self.python_params
        if self.spark_submit_params:
            body["spark_submit_params"] = self.spark_submit_params
        if self.sql_params:
            body["sql_params"] = self.sql_params
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunJobTask:
        """Deserializes the RunJobTask from a dictionary."""
        return cls(
            dbt_commands=d.get("dbt_commands", None),
            jar_params=d.get("jar_params", None),
            job_id=d.get("job_id", None),
            job_parameters=d.get("job_parameters", None),
            notebook_params=d.get("notebook_params", None),
            pipeline_params=_from_dict(d, "pipeline_params", PipelineParams),
            python_named_params=d.get("python_named_params", None),
            python_params=d.get("python_params", None),
            spark_submit_params=d.get("spark_submit_params", None),
            sql_params=d.get("sql_params", None),
        )


class RunLifeCycleState(Enum):
    """A value indicating the run's lifecycle state. The possible values are: * `QUEUED`: The run is
    queued. * `PENDING`: The run is waiting to be executed while the cluster and execution context
    are being prepared. * `RUNNING`: The task of this run is being executed. * `TERMINATING`: The
    task of this run has completed, and the cluster and execution context are being cleaned up. *
    `TERMINATED`: The task of this run has completed, and the cluster and execution context have
    been cleaned up. This state is terminal. * `SKIPPED`: This run was aborted because a previous
    run of the same job was already active. This state is terminal. * `INTERNAL_ERROR`: An
    exceptional state that indicates a failure in the Jobs service, such as network failure over a
    long period. If a run on a new cluster ends in the `INTERNAL_ERROR` state, the Jobs service
    terminates the cluster as soon as possible. This state is terminal. * `BLOCKED`: The run is
    blocked on an upstream dependency. * `WAITING_FOR_RETRY`: The run is waiting for a retry."""

    BLOCKED = "BLOCKED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SKIPPED = "SKIPPED"
    TERMINATED = "TERMINATED"
    TERMINATING = "TERMINATING"
    WAITING_FOR_RETRY = "WAITING_FOR_RETRY"


class RunLifecycleStateV2State(Enum):
    """The current state of the run."""

    BLOCKED = "BLOCKED"
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    TERMINATING = "TERMINATING"
    WAITING = "WAITING"


@dataclass
class RunNowResponse:
    """Run was started successfully."""

    number_in_job: Optional[int] = None
    """A unique identifier for this job run. This is set to the same value as `run_id`."""

    run_id: Optional[int] = None
    """The globally unique ID of the newly triggered run."""

    def as_dict(self) -> dict:
        """Serializes the RunNowResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.number_in_job is not None:
            body["number_in_job"] = self.number_in_job
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunNowResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.number_in_job is not None:
            body["number_in_job"] = self.number_in_job
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunNowResponse:
        """Deserializes the RunNowResponse from a dictionary."""
        return cls(number_in_job=d.get("number_in_job", None), run_id=d.get("run_id", None))


@dataclass
class RunOutput:
    """Run output was retrieved successfully."""

    clean_rooms_notebook_output: Optional[CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput] = None
    """The output of a clean rooms notebook task, if available"""

    dashboard_output: Optional[DashboardTaskOutput] = None
    """The output of a dashboard task, if available"""

    dbt_cloud_output: Optional[DbtCloudTaskOutput] = None
    """Deprecated in favor of the new dbt_platform_output"""

    dbt_output: Optional[DbtOutput] = None
    """The output of a dbt task, if available."""

    dbt_platform_output: Optional[DbtPlatformTaskOutput] = None

    error: Optional[str] = None
    """An error message indicating why a task failed or why output is not available. The message is
    unstructured, and its exact format is subject to change."""

    error_trace: Optional[str] = None
    """If there was an error executing the run, this field contains any available stack traces."""

    info: Optional[str] = None

    logs: Optional[str] = None
    """The output from tasks that write to standard streams (stdout/stderr) such as spark_jar_task,
    spark_python_task, python_wheel_task.
    
    It's not supported for the notebook_task, pipeline_task or spark_submit_task.
    
    Databricks restricts this API to return the last 5 MB of these logs."""

    logs_truncated: Optional[bool] = None
    """Whether the logs are truncated."""

    metadata: Optional[Run] = None
    """All details of the run except for its output."""

    notebook_output: Optional[NotebookOutput] = None
    """The output of a notebook task, if available. A notebook task that terminates (either
    successfully or with a failure) without calling `dbutils.notebook.exit()` is considered to have
    an empty output. This field is set but its result value is empty. Databricks restricts this API
    to return the first 5 MB of the output. To return a larger result, use the [ClusterLogConf]
    field to configure log storage for the job cluster.
    
    [ClusterLogConf]: https://docs.databricks.com/dev-tools/api/latest/clusters.html#clusterlogconf"""

    run_job_output: Optional[RunJobOutput] = None
    """The output of a run job task, if available"""

    sql_output: Optional[SqlOutput] = None
    """The output of a SQL task, if available."""

    def as_dict(self) -> dict:
        """Serializes the RunOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.clean_rooms_notebook_output:
            body["clean_rooms_notebook_output"] = self.clean_rooms_notebook_output.as_dict()
        if self.dashboard_output:
            body["dashboard_output"] = self.dashboard_output.as_dict()
        if self.dbt_cloud_output:
            body["dbt_cloud_output"] = self.dbt_cloud_output.as_dict()
        if self.dbt_output:
            body["dbt_output"] = self.dbt_output.as_dict()
        if self.dbt_platform_output:
            body["dbt_platform_output"] = self.dbt_platform_output.as_dict()
        if self.error is not None:
            body["error"] = self.error
        if self.error_trace is not None:
            body["error_trace"] = self.error_trace
        if self.info is not None:
            body["info"] = self.info
        if self.logs is not None:
            body["logs"] = self.logs
        if self.logs_truncated is not None:
            body["logs_truncated"] = self.logs_truncated
        if self.metadata:
            body["metadata"] = self.metadata.as_dict()
        if self.notebook_output:
            body["notebook_output"] = self.notebook_output.as_dict()
        if self.run_job_output:
            body["run_job_output"] = self.run_job_output.as_dict()
        if self.sql_output:
            body["sql_output"] = self.sql_output.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.clean_rooms_notebook_output:
            body["clean_rooms_notebook_output"] = self.clean_rooms_notebook_output
        if self.dashboard_output:
            body["dashboard_output"] = self.dashboard_output
        if self.dbt_cloud_output:
            body["dbt_cloud_output"] = self.dbt_cloud_output
        if self.dbt_output:
            body["dbt_output"] = self.dbt_output
        if self.dbt_platform_output:
            body["dbt_platform_output"] = self.dbt_platform_output
        if self.error is not None:
            body["error"] = self.error
        if self.error_trace is not None:
            body["error_trace"] = self.error_trace
        if self.info is not None:
            body["info"] = self.info
        if self.logs is not None:
            body["logs"] = self.logs
        if self.logs_truncated is not None:
            body["logs_truncated"] = self.logs_truncated
        if self.metadata:
            body["metadata"] = self.metadata
        if self.notebook_output:
            body["notebook_output"] = self.notebook_output
        if self.run_job_output:
            body["run_job_output"] = self.run_job_output
        if self.sql_output:
            body["sql_output"] = self.sql_output
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunOutput:
        """Deserializes the RunOutput from a dictionary."""
        return cls(
            clean_rooms_notebook_output=_from_dict(
                d, "clean_rooms_notebook_output", CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput
            ),
            dashboard_output=_from_dict(d, "dashboard_output", DashboardTaskOutput),
            dbt_cloud_output=_from_dict(d, "dbt_cloud_output", DbtCloudTaskOutput),
            dbt_output=_from_dict(d, "dbt_output", DbtOutput),
            dbt_platform_output=_from_dict(d, "dbt_platform_output", DbtPlatformTaskOutput),
            error=d.get("error", None),
            error_trace=d.get("error_trace", None),
            info=d.get("info", None),
            logs=d.get("logs", None),
            logs_truncated=d.get("logs_truncated", None),
            metadata=_from_dict(d, "metadata", Run),
            notebook_output=_from_dict(d, "notebook_output", NotebookOutput),
            run_job_output=_from_dict(d, "run_job_output", RunJobOutput),
            sql_output=_from_dict(d, "sql_output", SqlOutput),
        )


@dataclass
class RunParameters:
    dbt_commands: Optional[List[str]] = None
    """An array of commands to execute for jobs with the dbt task, for example `"dbt_commands": ["dbt
    deps", "dbt seed", "dbt deps", "dbt seed", "dbt run"]`
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    jar_params: Optional[List[str]] = None
    """A list of parameters for jobs with Spark JAR tasks, for example `"jar_params": ["john doe",
    "35"]`. The parameters are used to invoke the main function of the main class specified in the
    Spark JAR task. If not specified upon `run-now`, it defaults to an empty list. jar_params cannot
    be specified in conjunction with notebook_params. The JSON representation of this field (for
    example `{"jar_params":["john doe","35"]}`) cannot exceed 10,000 bytes.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    notebook_params: Optional[Dict[str, str]] = None
    """A map from keys to values for jobs with notebook task, for example `"notebook_params": {"name":
    "john doe", "age": "35"}`. The map is passed to the notebook and is accessible through the
    [dbutils.widgets.get] function.
    
    If not specified upon `run-now`, the triggered run uses the job’s base parameters.
    
    notebook_params cannot be specified in conjunction with jar_params.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    The JSON representation of this field (for example `{"notebook_params":{"name":"john
    doe","age":"35"}}`) cannot exceed 10,000 bytes.
    
    [dbutils.widgets.get]: https://docs.databricks.com/dev-tools/databricks-utils.html
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    pipeline_params: Optional[PipelineParams] = None
    """Controls whether the pipeline should perform a full refresh"""

    python_named_params: Optional[Dict[str, str]] = None

    python_params: Optional[List[str]] = None
    """A list of parameters for jobs with Python tasks, for example `"python_params": ["john doe",
    "35"]`. The parameters are passed to Python file as command-line parameters. If specified upon
    `run-now`, it would overwrite the parameters specified in job setting. The JSON representation
    of this field (for example `{"python_params":["john doe","35"]}`) cannot exceed 10,000 bytes.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    Important
    
    These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
    returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
    emojis.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    spark_submit_params: Optional[List[str]] = None
    """A list of parameters for jobs with spark submit task, for example `"spark_submit_params":
    ["--class", "org.apache.spark.examples.SparkPi"]`. The parameters are passed to spark-submit
    script as command-line parameters. If specified upon `run-now`, it would overwrite the
    parameters specified in job setting. The JSON representation of this field (for example
    `{"python_params":["john doe","35"]}`) cannot exceed 10,000 bytes.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    Important
    
    These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
    returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
    emojis.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    sql_params: Optional[Dict[str, str]] = None
    """A map from keys to values for jobs with SQL task, for example `"sql_params": {"name": "john
    doe", "age": "35"}`. The SQL alert task does not support custom parameters.
    
    ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.
    
    [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown"""

    def as_dict(self) -> dict:
        """Serializes the RunParameters into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dbt_commands:
            body["dbt_commands"] = [v for v in self.dbt_commands]
        if self.jar_params:
            body["jar_params"] = [v for v in self.jar_params]
        if self.notebook_params:
            body["notebook_params"] = self.notebook_params
        if self.pipeline_params:
            body["pipeline_params"] = self.pipeline_params.as_dict()
        if self.python_named_params:
            body["python_named_params"] = self.python_named_params
        if self.python_params:
            body["python_params"] = [v for v in self.python_params]
        if self.spark_submit_params:
            body["spark_submit_params"] = [v for v in self.spark_submit_params]
        if self.sql_params:
            body["sql_params"] = self.sql_params
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunParameters into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dbt_commands:
            body["dbt_commands"] = self.dbt_commands
        if self.jar_params:
            body["jar_params"] = self.jar_params
        if self.notebook_params:
            body["notebook_params"] = self.notebook_params
        if self.pipeline_params:
            body["pipeline_params"] = self.pipeline_params
        if self.python_named_params:
            body["python_named_params"] = self.python_named_params
        if self.python_params:
            body["python_params"] = self.python_params
        if self.spark_submit_params:
            body["spark_submit_params"] = self.spark_submit_params
        if self.sql_params:
            body["sql_params"] = self.sql_params
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunParameters:
        """Deserializes the RunParameters from a dictionary."""
        return cls(
            dbt_commands=d.get("dbt_commands", None),
            jar_params=d.get("jar_params", None),
            notebook_params=d.get("notebook_params", None),
            pipeline_params=_from_dict(d, "pipeline_params", PipelineParams),
            python_named_params=d.get("python_named_params", None),
            python_params=d.get("python_params", None),
            spark_submit_params=d.get("spark_submit_params", None),
            sql_params=d.get("sql_params", None),
        )


class RunResultState(Enum):
    """A value indicating the run's result. The possible values are: * `SUCCESS`: The task completed
    successfully. * `FAILED`: The task completed with an error. * `TIMEDOUT`: The run was stopped
    after reaching the timeout. * `CANCELED`: The run was canceled at user request. *
    `MAXIMUM_CONCURRENT_RUNS_REACHED`: The run was skipped because the maximum concurrent runs were
    reached. * `EXCLUDED`: The run was skipped because the necessary conditions were not met. *
    `SUCCESS_WITH_FAILURES`: The job run completed successfully with some failures; leaf tasks were
    successful. * `UPSTREAM_FAILED`: The run was skipped because of an upstream failure. *
    `UPSTREAM_CANCELED`: The run was skipped because an upstream task was canceled. * `DISABLED`:
    The run was skipped because it was disabled explicitly by the user."""

    CANCELED = "CANCELED"
    DISABLED = "DISABLED"
    EXCLUDED = "EXCLUDED"
    FAILED = "FAILED"
    MAXIMUM_CONCURRENT_RUNS_REACHED = "MAXIMUM_CONCURRENT_RUNS_REACHED"
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_FAILURES = "SUCCESS_WITH_FAILURES"
    TIMEDOUT = "TIMEDOUT"
    UPSTREAM_CANCELED = "UPSTREAM_CANCELED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"


@dataclass
class RunState:
    """The current state of the run."""

    life_cycle_state: Optional[RunLifeCycleState] = None
    """A value indicating the run's current lifecycle state. This field is always available in the
    response. Note: Additional states might be introduced in future releases."""

    queue_reason: Optional[str] = None
    """The reason indicating why the run was queued."""

    result_state: Optional[RunResultState] = None
    """A value indicating the run's result. This field is only available for terminal lifecycle states.
    Note: Additional states might be introduced in future releases."""

    state_message: Optional[str] = None
    """A descriptive message for the current state. This field is unstructured, and its exact format is
    subject to change."""

    user_cancelled_or_timedout: Optional[bool] = None
    """A value indicating whether a run was canceled manually by a user or by the scheduler because the
    run timed out."""

    def as_dict(self) -> dict:
        """Serializes the RunState into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.life_cycle_state is not None:
            body["life_cycle_state"] = self.life_cycle_state.value
        if self.queue_reason is not None:
            body["queue_reason"] = self.queue_reason
        if self.result_state is not None:
            body["result_state"] = self.result_state.value
        if self.state_message is not None:
            body["state_message"] = self.state_message
        if self.user_cancelled_or_timedout is not None:
            body["user_cancelled_or_timedout"] = self.user_cancelled_or_timedout
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunState into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.life_cycle_state is not None:
            body["life_cycle_state"] = self.life_cycle_state
        if self.queue_reason is not None:
            body["queue_reason"] = self.queue_reason
        if self.result_state is not None:
            body["result_state"] = self.result_state
        if self.state_message is not None:
            body["state_message"] = self.state_message
        if self.user_cancelled_or_timedout is not None:
            body["user_cancelled_or_timedout"] = self.user_cancelled_or_timedout
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunState:
        """Deserializes the RunState from a dictionary."""
        return cls(
            life_cycle_state=_enum(d, "life_cycle_state", RunLifeCycleState),
            queue_reason=d.get("queue_reason", None),
            result_state=_enum(d, "result_state", RunResultState),
            state_message=d.get("state_message", None),
            user_cancelled_or_timedout=d.get("user_cancelled_or_timedout", None),
        )


@dataclass
class RunStatus:
    """The current status of the run"""

    queue_details: Optional[QueueDetails] = None
    """If the run was queued, details about the reason for queuing the run."""

    state: Optional[RunLifecycleStateV2State] = None

    termination_details: Optional[TerminationDetails] = None
    """If the run is in a TERMINATING or TERMINATED state, details about the reason for terminating the
    run."""

    def as_dict(self) -> dict:
        """Serializes the RunStatus into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.queue_details:
            body["queue_details"] = self.queue_details.as_dict()
        if self.state is not None:
            body["state"] = self.state.value
        if self.termination_details:
            body["termination_details"] = self.termination_details.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunStatus into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.queue_details:
            body["queue_details"] = self.queue_details
        if self.state is not None:
            body["state"] = self.state
        if self.termination_details:
            body["termination_details"] = self.termination_details
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunStatus:
        """Deserializes the RunStatus from a dictionary."""
        return cls(
            queue_details=_from_dict(d, "queue_details", QueueDetails),
            state=_enum(d, "state", RunLifecycleStateV2State),
            termination_details=_from_dict(d, "termination_details", TerminationDetails),
        )


@dataclass
class RunTask:
    """Used when outputting a child run, in GetRun or ListRuns."""

    task_key: str
    """A unique name for the task. This field is used to refer to this task from other tasks. This
    field is required and must be unique within its parent job. On Update or Reset, this field is
    used to reference the tasks to be updated or reset."""

    attempt_number: Optional[int] = None
    """The sequence number of this run attempt for a triggered job run. The initial attempt of a run
    has an attempt_number of 0. If the initial run attempt fails, and the job has a retry policy
    (`max_retries` > 0), subsequent runs are created with an `original_attempt_run_id` of the
    original attempt’s ID and an incrementing `attempt_number`. Runs are retried only until they
    succeed, and the maximum `attempt_number` is the same as the `max_retries` value for the job."""

    clean_rooms_notebook_task: Optional[CleanRoomsNotebookTask] = None
    """The task runs a [clean rooms] notebook when the `clean_rooms_notebook_task` field is present.
    
    [clean rooms]: https://docs.databricks.com/clean-rooms/index.html"""

    cleanup_duration: Optional[int] = None
    """The time in milliseconds it took to terminate the cluster and clean up any associated artifacts.
    The duration of a task run is the sum of the `setup_duration`, `execution_duration`, and the
    `cleanup_duration`. The `cleanup_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    cluster_instance: Optional[ClusterInstance] = None
    """The cluster used for this run. If the run is specified to use a new cluster, this field is set
    once the Jobs service has requested a cluster for the run."""

    compute: Optional[Compute] = None
    """Task level compute configuration."""

    condition_task: Optional[RunConditionTask] = None
    """The task evaluates a condition that can be used to control the execution of other tasks when the
    `condition_task` field is present. The condition task does not require a cluster to execute and
    does not support retries or notifications."""

    dashboard_task: Optional[DashboardTask] = None
    """The task refreshes a dashboard and sends a snapshot to subscribers."""

    dbt_cloud_task: Optional[DbtCloudTask] = None
    """Task type for dbt cloud, deprecated in favor of the new name dbt_platform_task"""

    dbt_platform_task: Optional[DbtPlatformTask] = None

    dbt_task: Optional[DbtTask] = None
    """The task runs one or more dbt commands when the `dbt_task` field is present. The dbt task
    requires both Databricks SQL and the ability to use a serverless or a pro SQL warehouse."""

    depends_on: Optional[List[TaskDependency]] = None
    """An optional array of objects specifying the dependency graph of the task. All tasks specified in
    this field must complete successfully before executing this task. The key is `task_key`, and the
    value is the name assigned to the dependent task."""

    description: Optional[str] = None
    """An optional description for this task."""

    disable_auto_optimization: Optional[bool] = None
    """An option to disable auto optimization in serverless"""

    effective_performance_target: Optional[PerformanceTarget] = None
    """The actual performance target used by the serverless run during execution. This can differ from
    the client-set performance target on the request depending on whether the performance mode is
    supported by the job type.
    
    * `STANDARD`: Enables cost-efficient execution of serverless workloads. *
    `PERFORMANCE_OPTIMIZED`: Prioritizes fast startup and execution times through rapid scaling and
    optimized cluster performance."""

    email_notifications: Optional[JobEmailNotifications] = None
    """An optional set of email addresses notified when the task run begins or completes. The default
    behavior is to not send any emails."""

    end_time: Optional[int] = None
    """The time at which this run ended in epoch milliseconds (milliseconds since 1/1/1970 UTC). This
    field is set to 0 if the job is still running."""

    environment_key: Optional[str] = None
    """The key that references an environment spec in a job. This field is required for Python script,
    Python wheel and dbt tasks when using serverless compute."""

    execution_duration: Optional[int] = None
    """The time in milliseconds it took to execute the commands in the JAR or notebook until they
    completed, failed, timed out, were cancelled, or encountered an unexpected error. The duration
    of a task run is the sum of the `setup_duration`, `execution_duration`, and the
    `cleanup_duration`. The `execution_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    existing_cluster_id: Optional[str] = None
    """If existing_cluster_id, the ID of an existing cluster that is used for all runs. When running
    jobs or tasks on an existing cluster, you may need to manually restart the cluster if it stops
    responding. We suggest running jobs and tasks on new clusters for greater reliability"""

    for_each_task: Optional[RunForEachTask] = None
    """The task executes a nested task for every input provided when the `for_each_task` field is
    present."""

    gen_ai_compute_task: Optional[GenAiComputeTask] = None

    git_source: Optional[GitSource] = None
    """An optional specification for a remote Git repository containing the source code used by tasks.
    Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.
    If `git_source` is set, these tasks retrieve the file from the remote repository by default.
    However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task. Note:
    dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks are
    used, `git_source` must be defined on the job."""

    job_cluster_key: Optional[str] = None
    """If job_cluster_key, this task is executed reusing the cluster specified in
    `job.settings.job_clusters`."""

    libraries: Optional[List[compute.Library]] = None
    """An optional list of libraries to be installed on the cluster. The default value is an empty
    list."""

    max_retries: Optional[int] = None
    """An optional maximum number of times to retry an unsuccessful run. A run is considered to be
    unsuccessful if it completes with the `FAILED` result_state or `INTERNAL_ERROR`
    `life_cycle_state`. The value `-1` means to retry indefinitely and the value `0` means to never
    retry."""

    min_retry_interval_millis: Optional[int] = None
    """An optional minimal interval in milliseconds between the start of the failed run and the
    subsequent retry run. The default behavior is that unsuccessful runs are immediately retried."""

    new_cluster: Optional[compute.ClusterSpec] = None
    """If new_cluster, a description of a new cluster that is created for each run."""

    notebook_task: Optional[NotebookTask] = None
    """The task runs a notebook when the `notebook_task` field is present."""

    notification_settings: Optional[TaskNotificationSettings] = None
    """Optional notification settings that are used when sending notifications to each of the
    `email_notifications` and `webhook_notifications` for this task run."""

    pipeline_task: Optional[PipelineTask] = None
    """The task triggers a pipeline update when the `pipeline_task` field is present. Only pipelines
    configured to use triggered more are supported."""

    power_bi_task: Optional[PowerBiTask] = None
    """The task triggers a Power BI semantic model update when the `power_bi_task` field is present."""

    python_wheel_task: Optional[PythonWheelTask] = None
    """The task runs a Python wheel when the `python_wheel_task` field is present."""

    queue_duration: Optional[int] = None
    """The time in milliseconds that the run has spent in the queue."""

    resolved_values: Optional[ResolvedValues] = None
    """Parameter values including resolved references"""

    retry_on_timeout: Optional[bool] = None
    """An optional policy to specify whether to retry a job when it times out. The default behavior is
    to not retry on timeout."""

    run_duration: Optional[int] = None
    """The time in milliseconds it took the job run and all of its repairs to finish."""

    run_id: Optional[int] = None
    """The ID of the task run."""

    run_if: Optional[RunIf] = None
    """An optional value indicating the condition that determines whether the task should be run once
    its dependencies have been completed. When omitted, defaults to `ALL_SUCCESS`. See
    :method:jobs/create for a list of possible values."""

    run_job_task: Optional[RunJobTask] = None
    """The task triggers another job when the `run_job_task` field is present."""

    run_page_url: Optional[str] = None

    setup_duration: Optional[int] = None
    """The time in milliseconds it took to set up the cluster. For runs that run on new clusters this
    is the cluster creation time, for runs that run on existing clusters this time should be very
    short. The duration of a task run is the sum of the `setup_duration`, `execution_duration`, and
    the `cleanup_duration`. The `setup_duration` field is set to 0 for multitask job runs. The total
    duration of a multitask job run is the value of the `run_duration` field."""

    spark_jar_task: Optional[SparkJarTask] = None
    """The task runs a JAR when the `spark_jar_task` field is present."""

    spark_python_task: Optional[SparkPythonTask] = None
    """The task runs a Python file when the `spark_python_task` field is present."""

    spark_submit_task: Optional[SparkSubmitTask] = None
    """(Legacy) The task runs the spark-submit script when the spark_submit_task field is present.
    Databricks recommends using the spark_jar_task instead; see [Spark Submit task for
    jobs](/jobs/spark-submit)."""

    sql_task: Optional[SqlTask] = None
    """The task runs a SQL query or file, or it refreshes a SQL alert or a legacy SQL dashboard when
    the `sql_task` field is present."""

    start_time: Optional[int] = None
    """The time at which this run was started in epoch milliseconds (milliseconds since 1/1/1970 UTC).
    This may not be the time when the job task starts executing, for example, if the job is
    scheduled to run on a new cluster, this is the time the cluster creation call is issued."""

    state: Optional[RunState] = None
    """Deprecated. Please use the `status` field instead."""

    status: Optional[RunStatus] = None

    timeout_seconds: Optional[int] = None
    """An optional timeout applied to each run of this job task. A value of `0` means no timeout."""

    webhook_notifications: Optional[WebhookNotifications] = None
    """A collection of system notification IDs to notify when the run begins or completes. The default
    behavior is to not send any system notifications. Task webhooks respect the task notification
    settings."""

    def as_dict(self) -> dict:
        """Serializes the RunTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.attempt_number is not None:
            body["attempt_number"] = self.attempt_number
        if self.clean_rooms_notebook_task:
            body["clean_rooms_notebook_task"] = self.clean_rooms_notebook_task.as_dict()
        if self.cleanup_duration is not None:
            body["cleanup_duration"] = self.cleanup_duration
        if self.cluster_instance:
            body["cluster_instance"] = self.cluster_instance.as_dict()
        if self.compute:
            body["compute"] = self.compute.as_dict()
        if self.condition_task:
            body["condition_task"] = self.condition_task.as_dict()
        if self.dashboard_task:
            body["dashboard_task"] = self.dashboard_task.as_dict()
        if self.dbt_cloud_task:
            body["dbt_cloud_task"] = self.dbt_cloud_task.as_dict()
        if self.dbt_platform_task:
            body["dbt_platform_task"] = self.dbt_platform_task.as_dict()
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task.as_dict()
        if self.depends_on:
            body["depends_on"] = [v.as_dict() for v in self.depends_on]
        if self.description is not None:
            body["description"] = self.description
        if self.disable_auto_optimization is not None:
            body["disable_auto_optimization"] = self.disable_auto_optimization
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target.value
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications.as_dict()
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.execution_duration is not None:
            body["execution_duration"] = self.execution_duration
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.for_each_task:
            body["for_each_task"] = self.for_each_task.as_dict()
        if self.gen_ai_compute_task:
            body["gen_ai_compute_task"] = self.gen_ai_compute_task.as_dict()
        if self.git_source:
            body["git_source"] = self.git_source.as_dict()
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.libraries:
            body["libraries"] = [v.as_dict() for v in self.libraries]
        if self.max_retries is not None:
            body["max_retries"] = self.max_retries
        if self.min_retry_interval_millis is not None:
            body["min_retry_interval_millis"] = self.min_retry_interval_millis
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster.as_dict()
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task.as_dict()
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings.as_dict()
        if self.pipeline_task:
            body["pipeline_task"] = self.pipeline_task.as_dict()
        if self.power_bi_task:
            body["power_bi_task"] = self.power_bi_task.as_dict()
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task.as_dict()
        if self.queue_duration is not None:
            body["queue_duration"] = self.queue_duration
        if self.resolved_values:
            body["resolved_values"] = self.resolved_values.as_dict()
        if self.retry_on_timeout is not None:
            body["retry_on_timeout"] = self.retry_on_timeout
        if self.run_duration is not None:
            body["run_duration"] = self.run_duration
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_if is not None:
            body["run_if"] = self.run_if.value
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task.as_dict()
        if self.run_page_url is not None:
            body["run_page_url"] = self.run_page_url
        if self.setup_duration is not None:
            body["setup_duration"] = self.setup_duration
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task.as_dict()
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task.as_dict()
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task.as_dict()
        if self.sql_task:
            body["sql_task"] = self.sql_task.as_dict()
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state.as_dict()
        if self.status:
            body["status"] = self.status.as_dict()
        if self.task_key is not None:
            body["task_key"] = self.task_key
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.attempt_number is not None:
            body["attempt_number"] = self.attempt_number
        if self.clean_rooms_notebook_task:
            body["clean_rooms_notebook_task"] = self.clean_rooms_notebook_task
        if self.cleanup_duration is not None:
            body["cleanup_duration"] = self.cleanup_duration
        if self.cluster_instance:
            body["cluster_instance"] = self.cluster_instance
        if self.compute:
            body["compute"] = self.compute
        if self.condition_task:
            body["condition_task"] = self.condition_task
        if self.dashboard_task:
            body["dashboard_task"] = self.dashboard_task
        if self.dbt_cloud_task:
            body["dbt_cloud_task"] = self.dbt_cloud_task
        if self.dbt_platform_task:
            body["dbt_platform_task"] = self.dbt_platform_task
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task
        if self.depends_on:
            body["depends_on"] = self.depends_on
        if self.description is not None:
            body["description"] = self.description
        if self.disable_auto_optimization is not None:
            body["disable_auto_optimization"] = self.disable_auto_optimization
        if self.effective_performance_target is not None:
            body["effective_performance_target"] = self.effective_performance_target
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.execution_duration is not None:
            body["execution_duration"] = self.execution_duration
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.for_each_task:
            body["for_each_task"] = self.for_each_task
        if self.gen_ai_compute_task:
            body["gen_ai_compute_task"] = self.gen_ai_compute_task
        if self.git_source:
            body["git_source"] = self.git_source
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.libraries:
            body["libraries"] = self.libraries
        if self.max_retries is not None:
            body["max_retries"] = self.max_retries
        if self.min_retry_interval_millis is not None:
            body["min_retry_interval_millis"] = self.min_retry_interval_millis
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings
        if self.pipeline_task:
            body["pipeline_task"] = self.pipeline_task
        if self.power_bi_task:
            body["power_bi_task"] = self.power_bi_task
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task
        if self.queue_duration is not None:
            body["queue_duration"] = self.queue_duration
        if self.resolved_values:
            body["resolved_values"] = self.resolved_values
        if self.retry_on_timeout is not None:
            body["retry_on_timeout"] = self.retry_on_timeout
        if self.run_duration is not None:
            body["run_duration"] = self.run_duration
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_if is not None:
            body["run_if"] = self.run_if
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task
        if self.run_page_url is not None:
            body["run_page_url"] = self.run_page_url
        if self.setup_duration is not None:
            body["setup_duration"] = self.setup_duration
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task
        if self.sql_task:
            body["sql_task"] = self.sql_task
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.state:
            body["state"] = self.state
        if self.status:
            body["status"] = self.status
        if self.task_key is not None:
            body["task_key"] = self.task_key
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunTask:
        """Deserializes the RunTask from a dictionary."""
        return cls(
            attempt_number=d.get("attempt_number", None),
            clean_rooms_notebook_task=_from_dict(d, "clean_rooms_notebook_task", CleanRoomsNotebookTask),
            cleanup_duration=d.get("cleanup_duration", None),
            cluster_instance=_from_dict(d, "cluster_instance", ClusterInstance),
            compute=_from_dict(d, "compute", Compute),
            condition_task=_from_dict(d, "condition_task", RunConditionTask),
            dashboard_task=_from_dict(d, "dashboard_task", DashboardTask),
            dbt_cloud_task=_from_dict(d, "dbt_cloud_task", DbtCloudTask),
            dbt_platform_task=_from_dict(d, "dbt_platform_task", DbtPlatformTask),
            dbt_task=_from_dict(d, "dbt_task", DbtTask),
            depends_on=_repeated_dict(d, "depends_on", TaskDependency),
            description=d.get("description", None),
            disable_auto_optimization=d.get("disable_auto_optimization", None),
            effective_performance_target=_enum(d, "effective_performance_target", PerformanceTarget),
            email_notifications=_from_dict(d, "email_notifications", JobEmailNotifications),
            end_time=d.get("end_time", None),
            environment_key=d.get("environment_key", None),
            execution_duration=d.get("execution_duration", None),
            existing_cluster_id=d.get("existing_cluster_id", None),
            for_each_task=_from_dict(d, "for_each_task", RunForEachTask),
            gen_ai_compute_task=_from_dict(d, "gen_ai_compute_task", GenAiComputeTask),
            git_source=_from_dict(d, "git_source", GitSource),
            job_cluster_key=d.get("job_cluster_key", None),
            libraries=_repeated_dict(d, "libraries", compute.Library),
            max_retries=d.get("max_retries", None),
            min_retry_interval_millis=d.get("min_retry_interval_millis", None),
            new_cluster=_from_dict(d, "new_cluster", compute.ClusterSpec),
            notebook_task=_from_dict(d, "notebook_task", NotebookTask),
            notification_settings=_from_dict(d, "notification_settings", TaskNotificationSettings),
            pipeline_task=_from_dict(d, "pipeline_task", PipelineTask),
            power_bi_task=_from_dict(d, "power_bi_task", PowerBiTask),
            python_wheel_task=_from_dict(d, "python_wheel_task", PythonWheelTask),
            queue_duration=d.get("queue_duration", None),
            resolved_values=_from_dict(d, "resolved_values", ResolvedValues),
            retry_on_timeout=d.get("retry_on_timeout", None),
            run_duration=d.get("run_duration", None),
            run_id=d.get("run_id", None),
            run_if=_enum(d, "run_if", RunIf),
            run_job_task=_from_dict(d, "run_job_task", RunJobTask),
            run_page_url=d.get("run_page_url", None),
            setup_duration=d.get("setup_duration", None),
            spark_jar_task=_from_dict(d, "spark_jar_task", SparkJarTask),
            spark_python_task=_from_dict(d, "spark_python_task", SparkPythonTask),
            spark_submit_task=_from_dict(d, "spark_submit_task", SparkSubmitTask),
            sql_task=_from_dict(d, "sql_task", SqlTask),
            start_time=d.get("start_time", None),
            state=_from_dict(d, "state", RunState),
            status=_from_dict(d, "status", RunStatus),
            task_key=d.get("task_key", None),
            timeout_seconds=d.get("timeout_seconds", None),
            webhook_notifications=_from_dict(d, "webhook_notifications", WebhookNotifications),
        )


class RunType(Enum):
    """The type of a run. * `JOB_RUN`: Normal job run. A run created with :method:jobs/runNow. *
    `WORKFLOW_RUN`: Workflow run. A run created with [dbutils.notebook.run]. * `SUBMIT_RUN`: Submit
    run. A run created with :method:jobs/submit.

    [dbutils.notebook.run]: https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-workflow"""

    JOB_RUN = "JOB_RUN"
    SUBMIT_RUN = "SUBMIT_RUN"
    WORKFLOW_RUN = "WORKFLOW_RUN"


class Source(Enum):
    """Optional location type of the SQL file. When set to `WORKSPACE`, the SQL file will be retrieved\
    from the local Databricks workspace. When set to `GIT`, the SQL file will be retrieved from a
    Git repository defined in `git_source`. If the value is empty, the task will use `GIT` if
    `git_source` is defined and `WORKSPACE` otherwise.
    
    * `WORKSPACE`: SQL file is located in Databricks workspace. * `GIT`: SQL file is located in
    cloud Git provider."""

    GIT = "GIT"
    WORKSPACE = "WORKSPACE"


@dataclass
class SparkJarTask:
    jar_uri: Optional[str] = None
    """Deprecated since 04/2016. For classic compute, provide a `jar` through the `libraries` field
    instead. For serverless compute, provide a `jar` though the `java_dependencies` field inside the
    `environments` list.
    
    See the examples of classic and serverless compute usage at the top of the page."""

    main_class_name: Optional[str] = None
    """The full name of the class containing the main method to be executed. This class must be
    contained in a JAR provided as a library.
    
    The code must use `SparkContext.getOrCreate` to obtain a Spark context; otherwise, runs of the
    job fail."""

    parameters: Optional[List[str]] = None
    """Parameters passed to the main method.
    
    Use [Task parameter variables] to set parameters containing information about job runs.
    
    [Task parameter variables]: https://docs.databricks.com/jobs.html#parameter-variables"""

    run_as_repl: Optional[bool] = None
    """Deprecated. A value of `false` is no longer supported."""

    def as_dict(self) -> dict:
        """Serializes the SparkJarTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.jar_uri is not None:
            body["jar_uri"] = self.jar_uri
        if self.main_class_name is not None:
            body["main_class_name"] = self.main_class_name
        if self.parameters:
            body["parameters"] = [v for v in self.parameters]
        if self.run_as_repl is not None:
            body["run_as_repl"] = self.run_as_repl
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SparkJarTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.jar_uri is not None:
            body["jar_uri"] = self.jar_uri
        if self.main_class_name is not None:
            body["main_class_name"] = self.main_class_name
        if self.parameters:
            body["parameters"] = self.parameters
        if self.run_as_repl is not None:
            body["run_as_repl"] = self.run_as_repl
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SparkJarTask:
        """Deserializes the SparkJarTask from a dictionary."""
        return cls(
            jar_uri=d.get("jar_uri", None),
            main_class_name=d.get("main_class_name", None),
            parameters=d.get("parameters", None),
            run_as_repl=d.get("run_as_repl", None),
        )


@dataclass
class SparkPythonTask:
    python_file: str
    """The Python file to be executed. Cloud file URIs (such as dbfs:/, s3:/, adls:/, gcs:/) and
    workspace paths are supported. For python files stored in the Databricks workspace, the path
    must be absolute and begin with `/`. For files stored in a remote repository, the path must be
    relative. This field is required."""

    parameters: Optional[List[str]] = None
    """Command line parameters passed to the Python file.
    
    Use [Task parameter variables] to set parameters containing information about job runs.
    
    [Task parameter variables]: https://docs.databricks.com/jobs.html#parameter-variables"""

    source: Optional[Source] = None
    """Optional location type of the Python file. When set to `WORKSPACE` or not specified, the file
    will be retrieved from the local Databricks workspace or cloud location (if the `python_file`
    has a URI format). When set to `GIT`, the Python file will be retrieved from a Git repository
    defined in `git_source`.
    
    * `WORKSPACE`: The Python file is located in a Databricks workspace or at a cloud filesystem
    URI. * `GIT`: The Python file is located in a remote Git repository."""

    def as_dict(self) -> dict:
        """Serializes the SparkPythonTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.parameters:
            body["parameters"] = [v for v in self.parameters]
        if self.python_file is not None:
            body["python_file"] = self.python_file
        if self.source is not None:
            body["source"] = self.source.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SparkPythonTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.parameters:
            body["parameters"] = self.parameters
        if self.python_file is not None:
            body["python_file"] = self.python_file
        if self.source is not None:
            body["source"] = self.source
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SparkPythonTask:
        """Deserializes the SparkPythonTask from a dictionary."""
        return cls(
            parameters=d.get("parameters", None),
            python_file=d.get("python_file", None),
            source=_enum(d, "source", Source),
        )


@dataclass
class SparkSubmitTask:
    parameters: Optional[List[str]] = None
    """Command-line parameters passed to spark submit.
    
    Use [Task parameter variables] to set parameters containing information about job runs.
    
    [Task parameter variables]: https://docs.databricks.com/jobs.html#parameter-variables"""

    def as_dict(self) -> dict:
        """Serializes the SparkSubmitTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.parameters:
            body["parameters"] = [v for v in self.parameters]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SparkSubmitTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.parameters:
            body["parameters"] = self.parameters
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SparkSubmitTask:
        """Deserializes the SparkSubmitTask from a dictionary."""
        return cls(parameters=d.get("parameters", None))


@dataclass
class SparseCheckout:
    patterns: Optional[List[str]] = None
    """List of patterns to include for sparse checkout."""

    def as_dict(self) -> dict:
        """Serializes the SparseCheckout into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.patterns:
            body["patterns"] = [v for v in self.patterns]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SparseCheckout into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.patterns:
            body["patterns"] = self.patterns
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SparseCheckout:
        """Deserializes the SparseCheckout from a dictionary."""
        return cls(patterns=d.get("patterns", None))


@dataclass
class SqlAlertOutput:
    alert_state: Optional[SqlAlertState] = None

    output_link: Optional[str] = None
    """The link to find the output results."""

    query_text: Optional[str] = None
    """The text of the SQL query. Can Run permission of the SQL query associated with the SQL alert is
    required to view this field."""

    sql_statements: Optional[List[SqlStatementOutput]] = None
    """Information about SQL statements executed in the run."""

    warehouse_id: Optional[str] = None
    """The canonical identifier of the SQL warehouse."""

    def as_dict(self) -> dict:
        """Serializes the SqlAlertOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alert_state is not None:
            body["alert_state"] = self.alert_state.value
        if self.output_link is not None:
            body["output_link"] = self.output_link
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.sql_statements:
            body["sql_statements"] = [v.as_dict() for v in self.sql_statements]
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlAlertOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alert_state is not None:
            body["alert_state"] = self.alert_state
        if self.output_link is not None:
            body["output_link"] = self.output_link
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.sql_statements:
            body["sql_statements"] = self.sql_statements
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlAlertOutput:
        """Deserializes the SqlAlertOutput from a dictionary."""
        return cls(
            alert_state=_enum(d, "alert_state", SqlAlertState),
            output_link=d.get("output_link", None),
            query_text=d.get("query_text", None),
            sql_statements=_repeated_dict(d, "sql_statements", SqlStatementOutput),
            warehouse_id=d.get("warehouse_id", None),
        )


class SqlAlertState(Enum):
    """The state of the SQL alert.

    * UNKNOWN: alert yet to be evaluated * OK: alert evaluated and did not fulfill trigger
    conditions * TRIGGERED: alert evaluated and fulfilled trigger conditions"""

    OK = "OK"
    TRIGGERED = "TRIGGERED"
    UNKNOWN = "UNKNOWN"


@dataclass
class SqlDashboardOutput:
    warehouse_id: Optional[str] = None
    """The canonical identifier of the SQL warehouse."""

    widgets: Optional[List[SqlDashboardWidgetOutput]] = None
    """Widgets executed in the run. Only SQL query based widgets are listed."""

    def as_dict(self) -> dict:
        """Serializes the SqlDashboardOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        if self.widgets:
            body["widgets"] = [v.as_dict() for v in self.widgets]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlDashboardOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        if self.widgets:
            body["widgets"] = self.widgets
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlDashboardOutput:
        """Deserializes the SqlDashboardOutput from a dictionary."""
        return cls(
            warehouse_id=d.get("warehouse_id", None), widgets=_repeated_dict(d, "widgets", SqlDashboardWidgetOutput)
        )


@dataclass
class SqlDashboardWidgetOutput:
    end_time: Optional[int] = None
    """Time (in epoch milliseconds) when execution of the SQL widget ends."""

    error: Optional[SqlOutputError] = None
    """The information about the error when execution fails."""

    output_link: Optional[str] = None
    """The link to find the output results."""

    start_time: Optional[int] = None
    """Time (in epoch milliseconds) when execution of the SQL widget starts."""

    status: Optional[SqlDashboardWidgetOutputStatus] = None
    """The execution status of the SQL widget."""

    widget_id: Optional[str] = None
    """The canonical identifier of the SQL widget."""

    widget_title: Optional[str] = None
    """The title of the SQL widget."""

    def as_dict(self) -> dict:
        """Serializes the SqlDashboardWidgetOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.error:
            body["error"] = self.error.as_dict()
        if self.output_link is not None:
            body["output_link"] = self.output_link
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.status is not None:
            body["status"] = self.status.value
        if self.widget_id is not None:
            body["widget_id"] = self.widget_id
        if self.widget_title is not None:
            body["widget_title"] = self.widget_title
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlDashboardWidgetOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.error:
            body["error"] = self.error
        if self.output_link is not None:
            body["output_link"] = self.output_link
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.status is not None:
            body["status"] = self.status
        if self.widget_id is not None:
            body["widget_id"] = self.widget_id
        if self.widget_title is not None:
            body["widget_title"] = self.widget_title
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlDashboardWidgetOutput:
        """Deserializes the SqlDashboardWidgetOutput from a dictionary."""
        return cls(
            end_time=d.get("end_time", None),
            error=_from_dict(d, "error", SqlOutputError),
            output_link=d.get("output_link", None),
            start_time=d.get("start_time", None),
            status=_enum(d, "status", SqlDashboardWidgetOutputStatus),
            widget_id=d.get("widget_id", None),
            widget_title=d.get("widget_title", None),
        )


class SqlDashboardWidgetOutputStatus(Enum):

    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"


@dataclass
class SqlOutput:
    alert_output: Optional[SqlAlertOutput] = None
    """The output of a SQL alert task, if available."""

    dashboard_output: Optional[SqlDashboardOutput] = None
    """The output of a SQL dashboard task, if available."""

    query_output: Optional[SqlQueryOutput] = None
    """The output of a SQL query task, if available."""

    def as_dict(self) -> dict:
        """Serializes the SqlOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alert_output:
            body["alert_output"] = self.alert_output.as_dict()
        if self.dashboard_output:
            body["dashboard_output"] = self.dashboard_output.as_dict()
        if self.query_output:
            body["query_output"] = self.query_output.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alert_output:
            body["alert_output"] = self.alert_output
        if self.dashboard_output:
            body["dashboard_output"] = self.dashboard_output
        if self.query_output:
            body["query_output"] = self.query_output
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlOutput:
        """Deserializes the SqlOutput from a dictionary."""
        return cls(
            alert_output=_from_dict(d, "alert_output", SqlAlertOutput),
            dashboard_output=_from_dict(d, "dashboard_output", SqlDashboardOutput),
            query_output=_from_dict(d, "query_output", SqlQueryOutput),
        )


@dataclass
class SqlOutputError:
    message: Optional[str] = None
    """The error message when execution fails."""

    def as_dict(self) -> dict:
        """Serializes the SqlOutputError into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlOutputError into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlOutputError:
        """Deserializes the SqlOutputError from a dictionary."""
        return cls(message=d.get("message", None))


@dataclass
class SqlQueryOutput:
    endpoint_id: Optional[str] = None

    output_link: Optional[str] = None
    """The link to find the output results."""

    query_text: Optional[str] = None
    """The text of the SQL query. Can Run permission of the SQL query is required to view this field."""

    sql_statements: Optional[List[SqlStatementOutput]] = None
    """Information about SQL statements executed in the run."""

    warehouse_id: Optional[str] = None
    """The canonical identifier of the SQL warehouse."""

    def as_dict(self) -> dict:
        """Serializes the SqlQueryOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.endpoint_id is not None:
            body["endpoint_id"] = self.endpoint_id
        if self.output_link is not None:
            body["output_link"] = self.output_link
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.sql_statements:
            body["sql_statements"] = [v.as_dict() for v in self.sql_statements]
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlQueryOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.endpoint_id is not None:
            body["endpoint_id"] = self.endpoint_id
        if self.output_link is not None:
            body["output_link"] = self.output_link
        if self.query_text is not None:
            body["query_text"] = self.query_text
        if self.sql_statements:
            body["sql_statements"] = self.sql_statements
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlQueryOutput:
        """Deserializes the SqlQueryOutput from a dictionary."""
        return cls(
            endpoint_id=d.get("endpoint_id", None),
            output_link=d.get("output_link", None),
            query_text=d.get("query_text", None),
            sql_statements=_repeated_dict(d, "sql_statements", SqlStatementOutput),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class SqlStatementOutput:
    lookup_key: Optional[str] = None
    """A key that can be used to look up query details."""

    def as_dict(self) -> dict:
        """Serializes the SqlStatementOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.lookup_key is not None:
            body["lookup_key"] = self.lookup_key
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlStatementOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.lookup_key is not None:
            body["lookup_key"] = self.lookup_key
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlStatementOutput:
        """Deserializes the SqlStatementOutput from a dictionary."""
        return cls(lookup_key=d.get("lookup_key", None))


@dataclass
class SqlTask:
    warehouse_id: str
    """The canonical identifier of the SQL warehouse. Recommended to use with serverless or pro SQL
    warehouses. Classic SQL warehouses are only supported for SQL alert, dashboard and query tasks
    and are limited to scheduled single-task jobs."""

    alert: Optional[SqlTaskAlert] = None
    """If alert, indicates that this job must refresh a SQL alert."""

    dashboard: Optional[SqlTaskDashboard] = None
    """If dashboard, indicates that this job must refresh a SQL dashboard."""

    file: Optional[SqlTaskFile] = None
    """If file, indicates that this job runs a SQL file in a remote Git repository."""

    parameters: Optional[Dict[str, str]] = None
    """Parameters to be used for each run of this job. The SQL alert task does not support custom
    parameters."""

    query: Optional[SqlTaskQuery] = None
    """If query, indicates that this job must execute a SQL query."""

    def as_dict(self) -> dict:
        """Serializes the SqlTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alert:
            body["alert"] = self.alert.as_dict()
        if self.dashboard:
            body["dashboard"] = self.dashboard.as_dict()
        if self.file:
            body["file"] = self.file.as_dict()
        if self.parameters:
            body["parameters"] = self.parameters
        if self.query:
            body["query"] = self.query.as_dict()
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alert:
            body["alert"] = self.alert
        if self.dashboard:
            body["dashboard"] = self.dashboard
        if self.file:
            body["file"] = self.file
        if self.parameters:
            body["parameters"] = self.parameters
        if self.query:
            body["query"] = self.query
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlTask:
        """Deserializes the SqlTask from a dictionary."""
        return cls(
            alert=_from_dict(d, "alert", SqlTaskAlert),
            dashboard=_from_dict(d, "dashboard", SqlTaskDashboard),
            file=_from_dict(d, "file", SqlTaskFile),
            parameters=d.get("parameters", None),
            query=_from_dict(d, "query", SqlTaskQuery),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class SqlTaskAlert:
    alert_id: str
    """The canonical identifier of the SQL alert."""

    pause_subscriptions: Optional[bool] = None
    """If true, the alert notifications are not sent to subscribers."""

    subscriptions: Optional[List[SqlTaskSubscription]] = None
    """If specified, alert notifications are sent to subscribers."""

    def as_dict(self) -> dict:
        """Serializes the SqlTaskAlert into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alert_id is not None:
            body["alert_id"] = self.alert_id
        if self.pause_subscriptions is not None:
            body["pause_subscriptions"] = self.pause_subscriptions
        if self.subscriptions:
            body["subscriptions"] = [v.as_dict() for v in self.subscriptions]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlTaskAlert into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alert_id is not None:
            body["alert_id"] = self.alert_id
        if self.pause_subscriptions is not None:
            body["pause_subscriptions"] = self.pause_subscriptions
        if self.subscriptions:
            body["subscriptions"] = self.subscriptions
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlTaskAlert:
        """Deserializes the SqlTaskAlert from a dictionary."""
        return cls(
            alert_id=d.get("alert_id", None),
            pause_subscriptions=d.get("pause_subscriptions", None),
            subscriptions=_repeated_dict(d, "subscriptions", SqlTaskSubscription),
        )


@dataclass
class SqlTaskDashboard:
    dashboard_id: str
    """The canonical identifier of the SQL dashboard."""

    custom_subject: Optional[str] = None
    """Subject of the email sent to subscribers of this task."""

    pause_subscriptions: Optional[bool] = None
    """If true, the dashboard snapshot is not taken, and emails are not sent to subscribers."""

    subscriptions: Optional[List[SqlTaskSubscription]] = None
    """If specified, dashboard snapshots are sent to subscriptions."""

    def as_dict(self) -> dict:
        """Serializes the SqlTaskDashboard into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.pause_subscriptions is not None:
            body["pause_subscriptions"] = self.pause_subscriptions
        if self.subscriptions:
            body["subscriptions"] = [v.as_dict() for v in self.subscriptions]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlTaskDashboard into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.pause_subscriptions is not None:
            body["pause_subscriptions"] = self.pause_subscriptions
        if self.subscriptions:
            body["subscriptions"] = self.subscriptions
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlTaskDashboard:
        """Deserializes the SqlTaskDashboard from a dictionary."""
        return cls(
            custom_subject=d.get("custom_subject", None),
            dashboard_id=d.get("dashboard_id", None),
            pause_subscriptions=d.get("pause_subscriptions", None),
            subscriptions=_repeated_dict(d, "subscriptions", SqlTaskSubscription),
        )


@dataclass
class SqlTaskFile:
    path: str
    """Path of the SQL file. Must be relative if the source is a remote Git repository and absolute for
    workspace paths."""

    source: Optional[Source] = None
    """Optional location type of the SQL file. When set to `WORKSPACE`, the SQL file will be retrieved
    from the local Databricks workspace. When set to `GIT`, the SQL file will be retrieved from a
    Git repository defined in `git_source`. If the value is empty, the task will use `GIT` if
    `git_source` is defined and `WORKSPACE` otherwise.
    
    * `WORKSPACE`: SQL file is located in Databricks workspace. * `GIT`: SQL file is located in
    cloud Git provider."""

    def as_dict(self) -> dict:
        """Serializes the SqlTaskFile into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.path is not None:
            body["path"] = self.path
        if self.source is not None:
            body["source"] = self.source.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlTaskFile into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.path is not None:
            body["path"] = self.path
        if self.source is not None:
            body["source"] = self.source
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlTaskFile:
        """Deserializes the SqlTaskFile from a dictionary."""
        return cls(path=d.get("path", None), source=_enum(d, "source", Source))


@dataclass
class SqlTaskQuery:
    query_id: str
    """The canonical identifier of the SQL query."""

    def as_dict(self) -> dict:
        """Serializes the SqlTaskQuery into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.query_id is not None:
            body["query_id"] = self.query_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlTaskQuery into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.query_id is not None:
            body["query_id"] = self.query_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlTaskQuery:
        """Deserializes the SqlTaskQuery from a dictionary."""
        return cls(query_id=d.get("query_id", None))


@dataclass
class SqlTaskSubscription:
    destination_id: Optional[str] = None
    """The canonical identifier of the destination to receive email notification. This parameter is
    mutually exclusive with user_name. You cannot set both destination_id and user_name for
    subscription notifications."""

    user_name: Optional[str] = None
    """The user name to receive the subscription email. This parameter is mutually exclusive with
    destination_id. You cannot set both destination_id and user_name for subscription notifications."""

    def as_dict(self) -> dict:
        """Serializes the SqlTaskSubscription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SqlTaskSubscription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SqlTaskSubscription:
        """Deserializes the SqlTaskSubscription from a dictionary."""
        return cls(destination_id=d.get("destination_id", None), user_name=d.get("user_name", None))


class StorageMode(Enum):

    DIRECT_QUERY = "DIRECT_QUERY"
    DUAL = "DUAL"
    IMPORT = "IMPORT"


@dataclass
class SubmitRunResponse:
    """Run was created and started successfully."""

    run_id: Optional[int] = None
    """The canonical identifier for the newly submitted run."""

    def as_dict(self) -> dict:
        """Serializes the SubmitRunResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SubmitRunResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SubmitRunResponse:
        """Deserializes the SubmitRunResponse from a dictionary."""
        return cls(run_id=d.get("run_id", None))


@dataclass
class SubmitTask:
    task_key: str
    """A unique name for the task. This field is used to refer to this task from other tasks. This
    field is required and must be unique within its parent job. On Update or Reset, this field is
    used to reference the tasks to be updated or reset."""

    clean_rooms_notebook_task: Optional[CleanRoomsNotebookTask] = None
    """The task runs a [clean rooms] notebook when the `clean_rooms_notebook_task` field is present.
    
    [clean rooms]: https://docs.databricks.com/clean-rooms/index.html"""

    compute: Optional[Compute] = None
    """Task level compute configuration."""

    condition_task: Optional[ConditionTask] = None
    """The task evaluates a condition that can be used to control the execution of other tasks when the
    `condition_task` field is present. The condition task does not require a cluster to execute and
    does not support retries or notifications."""

    dashboard_task: Optional[DashboardTask] = None
    """The task refreshes a dashboard and sends a snapshot to subscribers."""

    dbt_cloud_task: Optional[DbtCloudTask] = None
    """Task type for dbt cloud, deprecated in favor of the new name dbt_platform_task"""

    dbt_platform_task: Optional[DbtPlatformTask] = None

    dbt_task: Optional[DbtTask] = None
    """The task runs one or more dbt commands when the `dbt_task` field is present. The dbt task
    requires both Databricks SQL and the ability to use a serverless or a pro SQL warehouse."""

    depends_on: Optional[List[TaskDependency]] = None
    """An optional array of objects specifying the dependency graph of the task. All tasks specified in
    this field must complete successfully before executing this task. The key is `task_key`, and the
    value is the name assigned to the dependent task."""

    description: Optional[str] = None
    """An optional description for this task."""

    disable_auto_optimization: Optional[bool] = None
    """An option to disable auto optimization in serverless"""

    email_notifications: Optional[JobEmailNotifications] = None
    """An optional set of email addresses notified when the task run begins or completes. The default
    behavior is to not send any emails."""

    environment_key: Optional[str] = None
    """The key that references an environment spec in a job. This field is required for Python script,
    Python wheel and dbt tasks when using serverless compute."""

    existing_cluster_id: Optional[str] = None
    """If existing_cluster_id, the ID of an existing cluster that is used for all runs. When running
    jobs or tasks on an existing cluster, you may need to manually restart the cluster if it stops
    responding. We suggest running jobs and tasks on new clusters for greater reliability"""

    for_each_task: Optional[ForEachTask] = None
    """The task executes a nested task for every input provided when the `for_each_task` field is
    present."""

    gen_ai_compute_task: Optional[GenAiComputeTask] = None

    health: Optional[JobsHealthRules] = None

    libraries: Optional[List[compute.Library]] = None
    """An optional list of libraries to be installed on the cluster. The default value is an empty
    list."""

    max_retries: Optional[int] = None
    """An optional maximum number of times to retry an unsuccessful run. A run is considered to be
    unsuccessful if it completes with the `FAILED` result_state or `INTERNAL_ERROR`
    `life_cycle_state`. The value `-1` means to retry indefinitely and the value `0` means to never
    retry."""

    min_retry_interval_millis: Optional[int] = None
    """An optional minimal interval in milliseconds between the start of the failed run and the
    subsequent retry run. The default behavior is that unsuccessful runs are immediately retried."""

    new_cluster: Optional[compute.ClusterSpec] = None
    """If new_cluster, a description of a new cluster that is created for each run."""

    notebook_task: Optional[NotebookTask] = None
    """The task runs a notebook when the `notebook_task` field is present."""

    notification_settings: Optional[TaskNotificationSettings] = None
    """Optional notification settings that are used when sending notifications to each of the
    `email_notifications` and `webhook_notifications` for this task run."""

    pipeline_task: Optional[PipelineTask] = None
    """The task triggers a pipeline update when the `pipeline_task` field is present. Only pipelines
    configured to use triggered more are supported."""

    power_bi_task: Optional[PowerBiTask] = None
    """The task triggers a Power BI semantic model update when the `power_bi_task` field is present."""

    python_wheel_task: Optional[PythonWheelTask] = None
    """The task runs a Python wheel when the `python_wheel_task` field is present."""

    retry_on_timeout: Optional[bool] = None
    """An optional policy to specify whether to retry a job when it times out. The default behavior is
    to not retry on timeout."""

    run_if: Optional[RunIf] = None
    """An optional value indicating the condition that determines whether the task should be run once
    its dependencies have been completed. When omitted, defaults to `ALL_SUCCESS`. See
    :method:jobs/create for a list of possible values."""

    run_job_task: Optional[RunJobTask] = None
    """The task triggers another job when the `run_job_task` field is present."""

    spark_jar_task: Optional[SparkJarTask] = None
    """The task runs a JAR when the `spark_jar_task` field is present."""

    spark_python_task: Optional[SparkPythonTask] = None
    """The task runs a Python file when the `spark_python_task` field is present."""

    spark_submit_task: Optional[SparkSubmitTask] = None
    """(Legacy) The task runs the spark-submit script when the spark_submit_task field is present.
    Databricks recommends using the spark_jar_task instead; see [Spark Submit task for
    jobs](/jobs/spark-submit)."""

    sql_task: Optional[SqlTask] = None
    """The task runs a SQL query or file, or it refreshes a SQL alert or a legacy SQL dashboard when
    the `sql_task` field is present."""

    timeout_seconds: Optional[int] = None
    """An optional timeout applied to each run of this job task. A value of `0` means no timeout."""

    webhook_notifications: Optional[WebhookNotifications] = None
    """A collection of system notification IDs to notify when the run begins or completes. The default
    behavior is to not send any system notifications. Task webhooks respect the task notification
    settings."""

    def as_dict(self) -> dict:
        """Serializes the SubmitTask into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.clean_rooms_notebook_task:
            body["clean_rooms_notebook_task"] = self.clean_rooms_notebook_task.as_dict()
        if self.compute:
            body["compute"] = self.compute.as_dict()
        if self.condition_task:
            body["condition_task"] = self.condition_task.as_dict()
        if self.dashboard_task:
            body["dashboard_task"] = self.dashboard_task.as_dict()
        if self.dbt_cloud_task:
            body["dbt_cloud_task"] = self.dbt_cloud_task.as_dict()
        if self.dbt_platform_task:
            body["dbt_platform_task"] = self.dbt_platform_task.as_dict()
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task.as_dict()
        if self.depends_on:
            body["depends_on"] = [v.as_dict() for v in self.depends_on]
        if self.description is not None:
            body["description"] = self.description
        if self.disable_auto_optimization is not None:
            body["disable_auto_optimization"] = self.disable_auto_optimization
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications.as_dict()
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.for_each_task:
            body["for_each_task"] = self.for_each_task.as_dict()
        if self.gen_ai_compute_task:
            body["gen_ai_compute_task"] = self.gen_ai_compute_task.as_dict()
        if self.health:
            body["health"] = self.health.as_dict()
        if self.libraries:
            body["libraries"] = [v.as_dict() for v in self.libraries]
        if self.max_retries is not None:
            body["max_retries"] = self.max_retries
        if self.min_retry_interval_millis is not None:
            body["min_retry_interval_millis"] = self.min_retry_interval_millis
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster.as_dict()
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task.as_dict()
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings.as_dict()
        if self.pipeline_task:
            body["pipeline_task"] = self.pipeline_task.as_dict()
        if self.power_bi_task:
            body["power_bi_task"] = self.power_bi_task.as_dict()
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task.as_dict()
        if self.retry_on_timeout is not None:
            body["retry_on_timeout"] = self.retry_on_timeout
        if self.run_if is not None:
            body["run_if"] = self.run_if.value
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task.as_dict()
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task.as_dict()
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task.as_dict()
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task.as_dict()
        if self.sql_task:
            body["sql_task"] = self.sql_task.as_dict()
        if self.task_key is not None:
            body["task_key"] = self.task_key
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SubmitTask into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.clean_rooms_notebook_task:
            body["clean_rooms_notebook_task"] = self.clean_rooms_notebook_task
        if self.compute:
            body["compute"] = self.compute
        if self.condition_task:
            body["condition_task"] = self.condition_task
        if self.dashboard_task:
            body["dashboard_task"] = self.dashboard_task
        if self.dbt_cloud_task:
            body["dbt_cloud_task"] = self.dbt_cloud_task
        if self.dbt_platform_task:
            body["dbt_platform_task"] = self.dbt_platform_task
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task
        if self.depends_on:
            body["depends_on"] = self.depends_on
        if self.description is not None:
            body["description"] = self.description
        if self.disable_auto_optimization is not None:
            body["disable_auto_optimization"] = self.disable_auto_optimization
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.for_each_task:
            body["for_each_task"] = self.for_each_task
        if self.gen_ai_compute_task:
            body["gen_ai_compute_task"] = self.gen_ai_compute_task
        if self.health:
            body["health"] = self.health
        if self.libraries:
            body["libraries"] = self.libraries
        if self.max_retries is not None:
            body["max_retries"] = self.max_retries
        if self.min_retry_interval_millis is not None:
            body["min_retry_interval_millis"] = self.min_retry_interval_millis
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings
        if self.pipeline_task:
            body["pipeline_task"] = self.pipeline_task
        if self.power_bi_task:
            body["power_bi_task"] = self.power_bi_task
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task
        if self.retry_on_timeout is not None:
            body["retry_on_timeout"] = self.retry_on_timeout
        if self.run_if is not None:
            body["run_if"] = self.run_if
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task
        if self.sql_task:
            body["sql_task"] = self.sql_task
        if self.task_key is not None:
            body["task_key"] = self.task_key
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SubmitTask:
        """Deserializes the SubmitTask from a dictionary."""
        return cls(
            clean_rooms_notebook_task=_from_dict(d, "clean_rooms_notebook_task", CleanRoomsNotebookTask),
            compute=_from_dict(d, "compute", Compute),
            condition_task=_from_dict(d, "condition_task", ConditionTask),
            dashboard_task=_from_dict(d, "dashboard_task", DashboardTask),
            dbt_cloud_task=_from_dict(d, "dbt_cloud_task", DbtCloudTask),
            dbt_platform_task=_from_dict(d, "dbt_platform_task", DbtPlatformTask),
            dbt_task=_from_dict(d, "dbt_task", DbtTask),
            depends_on=_repeated_dict(d, "depends_on", TaskDependency),
            description=d.get("description", None),
            disable_auto_optimization=d.get("disable_auto_optimization", None),
            email_notifications=_from_dict(d, "email_notifications", JobEmailNotifications),
            environment_key=d.get("environment_key", None),
            existing_cluster_id=d.get("existing_cluster_id", None),
            for_each_task=_from_dict(d, "for_each_task", ForEachTask),
            gen_ai_compute_task=_from_dict(d, "gen_ai_compute_task", GenAiComputeTask),
            health=_from_dict(d, "health", JobsHealthRules),
            libraries=_repeated_dict(d, "libraries", compute.Library),
            max_retries=d.get("max_retries", None),
            min_retry_interval_millis=d.get("min_retry_interval_millis", None),
            new_cluster=_from_dict(d, "new_cluster", compute.ClusterSpec),
            notebook_task=_from_dict(d, "notebook_task", NotebookTask),
            notification_settings=_from_dict(d, "notification_settings", TaskNotificationSettings),
            pipeline_task=_from_dict(d, "pipeline_task", PipelineTask),
            power_bi_task=_from_dict(d, "power_bi_task", PowerBiTask),
            python_wheel_task=_from_dict(d, "python_wheel_task", PythonWheelTask),
            retry_on_timeout=d.get("retry_on_timeout", None),
            run_if=_enum(d, "run_if", RunIf),
            run_job_task=_from_dict(d, "run_job_task", RunJobTask),
            spark_jar_task=_from_dict(d, "spark_jar_task", SparkJarTask),
            spark_python_task=_from_dict(d, "spark_python_task", SparkPythonTask),
            spark_submit_task=_from_dict(d, "spark_submit_task", SparkSubmitTask),
            sql_task=_from_dict(d, "sql_task", SqlTask),
            task_key=d.get("task_key", None),
            timeout_seconds=d.get("timeout_seconds", None),
            webhook_notifications=_from_dict(d, "webhook_notifications", WebhookNotifications),
        )


@dataclass
class Subscription:
    custom_subject: Optional[str] = None
    """Optional: Allows users to specify a custom subject line on the email sent to subscribers."""

    paused: Optional[bool] = None
    """When true, the subscription will not send emails."""

    subscribers: Optional[List[SubscriptionSubscriber]] = None
    """The list of subscribers to send the snapshot of the dashboard to."""

    def as_dict(self) -> dict:
        """Serializes the Subscription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.paused is not None:
            body["paused"] = self.paused
        if self.subscribers:
            body["subscribers"] = [v.as_dict() for v in self.subscribers]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Subscription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.custom_subject is not None:
            body["custom_subject"] = self.custom_subject
        if self.paused is not None:
            body["paused"] = self.paused
        if self.subscribers:
            body["subscribers"] = self.subscribers
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Subscription:
        """Deserializes the Subscription from a dictionary."""
        return cls(
            custom_subject=d.get("custom_subject", None),
            paused=d.get("paused", None),
            subscribers=_repeated_dict(d, "subscribers", SubscriptionSubscriber),
        )


@dataclass
class SubscriptionSubscriber:
    destination_id: Optional[str] = None
    """A snapshot of the dashboard will be sent to the destination when the `destination_id` field is
    present."""

    user_name: Optional[str] = None
    """A snapshot of the dashboard will be sent to the user's email when the `user_name` field is
    present."""

    def as_dict(self) -> dict:
        """Serializes the SubscriptionSubscriber into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SubscriptionSubscriber into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SubscriptionSubscriber:
        """Deserializes the SubscriptionSubscriber from a dictionary."""
        return cls(destination_id=d.get("destination_id", None), user_name=d.get("user_name", None))


@dataclass
class TableState:
    has_seen_updates: Optional[bool] = None
    """Whether or not the table has seen updates since either the creation of the trigger or the last
    successful evaluation of the trigger"""

    table_name: Optional[str] = None
    """Full table name of the table to monitor, e.g. `mycatalog.myschema.mytable`"""

    def as_dict(self) -> dict:
        """Serializes the TableState into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.has_seen_updates is not None:
            body["has_seen_updates"] = self.has_seen_updates
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableState into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.has_seen_updates is not None:
            body["has_seen_updates"] = self.has_seen_updates
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableState:
        """Deserializes the TableState from a dictionary."""
        return cls(has_seen_updates=d.get("has_seen_updates", None), table_name=d.get("table_name", None))


@dataclass
class TableTriggerState:
    last_seen_table_states: Optional[List[TableState]] = None

    using_scalable_monitoring: Optional[bool] = None
    """Indicates whether the trigger is using scalable monitoring."""

    def as_dict(self) -> dict:
        """Serializes the TableTriggerState into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.last_seen_table_states:
            body["last_seen_table_states"] = [v.as_dict() for v in self.last_seen_table_states]
        if self.using_scalable_monitoring is not None:
            body["using_scalable_monitoring"] = self.using_scalable_monitoring
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableTriggerState into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.last_seen_table_states:
            body["last_seen_table_states"] = self.last_seen_table_states
        if self.using_scalable_monitoring is not None:
            body["using_scalable_monitoring"] = self.using_scalable_monitoring
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableTriggerState:
        """Deserializes the TableTriggerState from a dictionary."""
        return cls(
            last_seen_table_states=_repeated_dict(d, "last_seen_table_states", TableState),
            using_scalable_monitoring=d.get("using_scalable_monitoring", None),
        )


@dataclass
class TableUpdateTriggerConfiguration:
    table_names: List[str]
    """A list of tables to monitor for changes. The table name must be in the format
    `catalog_name.schema_name.table_name`."""

    condition: Optional[Condition] = None
    """The table(s) condition based on which to trigger a job run."""

    min_time_between_triggers_seconds: Optional[int] = None
    """If set, the trigger starts a run only after the specified amount of time has passed since the
    last time the trigger fired. The minimum allowed value is 60 seconds."""

    wait_after_last_change_seconds: Optional[int] = None
    """If set, the trigger starts a run only after no table updates have occurred for the specified
    time and can be used to wait for a series of table updates before triggering a run. The minimum
    allowed value is 60 seconds."""

    def as_dict(self) -> dict:
        """Serializes the TableUpdateTriggerConfiguration into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.condition is not None:
            body["condition"] = self.condition.value
        if self.min_time_between_triggers_seconds is not None:
            body["min_time_between_triggers_seconds"] = self.min_time_between_triggers_seconds
        if self.table_names:
            body["table_names"] = [v for v in self.table_names]
        if self.wait_after_last_change_seconds is not None:
            body["wait_after_last_change_seconds"] = self.wait_after_last_change_seconds
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TableUpdateTriggerConfiguration into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.condition is not None:
            body["condition"] = self.condition
        if self.min_time_between_triggers_seconds is not None:
            body["min_time_between_triggers_seconds"] = self.min_time_between_triggers_seconds
        if self.table_names:
            body["table_names"] = self.table_names
        if self.wait_after_last_change_seconds is not None:
            body["wait_after_last_change_seconds"] = self.wait_after_last_change_seconds
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TableUpdateTriggerConfiguration:
        """Deserializes the TableUpdateTriggerConfiguration from a dictionary."""
        return cls(
            condition=_enum(d, "condition", Condition),
            min_time_between_triggers_seconds=d.get("min_time_between_triggers_seconds", None),
            table_names=d.get("table_names", None),
            wait_after_last_change_seconds=d.get("wait_after_last_change_seconds", None),
        )


@dataclass
class Task:
    task_key: str
    """A unique name for the task. This field is used to refer to this task from other tasks. This
    field is required and must be unique within its parent job. On Update or Reset, this field is
    used to reference the tasks to be updated or reset."""

    clean_rooms_notebook_task: Optional[CleanRoomsNotebookTask] = None
    """The task runs a [clean rooms] notebook when the `clean_rooms_notebook_task` field is present.
    
    [clean rooms]: https://docs.databricks.com/clean-rooms/index.html"""

    compute: Optional[Compute] = None
    """Task level compute configuration."""

    condition_task: Optional[ConditionTask] = None
    """The task evaluates a condition that can be used to control the execution of other tasks when the
    `condition_task` field is present. The condition task does not require a cluster to execute and
    does not support retries or notifications."""

    dashboard_task: Optional[DashboardTask] = None
    """The task refreshes a dashboard and sends a snapshot to subscribers."""

    dbt_cloud_task: Optional[DbtCloudTask] = None
    """Task type for dbt cloud, deprecated in favor of the new name dbt_platform_task"""

    dbt_platform_task: Optional[DbtPlatformTask] = None

    dbt_task: Optional[DbtTask] = None
    """The task runs one or more dbt commands when the `dbt_task` field is present. The dbt task
    requires both Databricks SQL and the ability to use a serverless or a pro SQL warehouse."""

    depends_on: Optional[List[TaskDependency]] = None
    """An optional array of objects specifying the dependency graph of the task. All tasks specified in
    this field must complete before executing this task. The task will run only if the `run_if`
    condition is true. The key is `task_key`, and the value is the name assigned to the dependent
    task."""

    description: Optional[str] = None
    """An optional description for this task."""

    disable_auto_optimization: Optional[bool] = None
    """An option to disable auto optimization in serverless"""

    disabled: Optional[bool] = None
    """An optional flag to disable the task. If set to true, the task will not run even if it is part
    of a job."""

    email_notifications: Optional[TaskEmailNotifications] = None
    """An optional set of email addresses that is notified when runs of this task begin or complete as
    well as when this task is deleted. The default behavior is to not send any emails."""

    environment_key: Optional[str] = None
    """The key that references an environment spec in a job. This field is required for Python script,
    Python wheel and dbt tasks when using serverless compute."""

    existing_cluster_id: Optional[str] = None
    """If existing_cluster_id, the ID of an existing cluster that is used for all runs. When running
    jobs or tasks on an existing cluster, you may need to manually restart the cluster if it stops
    responding. We suggest running jobs and tasks on new clusters for greater reliability"""

    for_each_task: Optional[ForEachTask] = None
    """The task executes a nested task for every input provided when the `for_each_task` field is
    present."""

    gen_ai_compute_task: Optional[GenAiComputeTask] = None

    health: Optional[JobsHealthRules] = None

    job_cluster_key: Optional[str] = None
    """If job_cluster_key, this task is executed reusing the cluster specified in
    `job.settings.job_clusters`."""

    libraries: Optional[List[compute.Library]] = None
    """An optional list of libraries to be installed on the cluster. The default value is an empty
    list."""

    max_retries: Optional[int] = None
    """An optional maximum number of times to retry an unsuccessful run. A run is considered to be
    unsuccessful if it completes with the `FAILED` result_state or `INTERNAL_ERROR`
    `life_cycle_state`. The value `-1` means to retry indefinitely and the value `0` means to never
    retry."""

    min_retry_interval_millis: Optional[int] = None
    """An optional minimal interval in milliseconds between the start of the failed run and the
    subsequent retry run. The default behavior is that unsuccessful runs are immediately retried."""

    new_cluster: Optional[compute.ClusterSpec] = None
    """If new_cluster, a description of a new cluster that is created for each run."""

    notebook_task: Optional[NotebookTask] = None
    """The task runs a notebook when the `notebook_task` field is present."""

    notification_settings: Optional[TaskNotificationSettings] = None
    """Optional notification settings that are used when sending notifications to each of the
    `email_notifications` and `webhook_notifications` for this task."""

    pipeline_task: Optional[PipelineTask] = None
    """The task triggers a pipeline update when the `pipeline_task` field is present. Only pipelines
    configured to use triggered more are supported."""

    power_bi_task: Optional[PowerBiTask] = None
    """The task triggers a Power BI semantic model update when the `power_bi_task` field is present."""

    python_wheel_task: Optional[PythonWheelTask] = None
    """The task runs a Python wheel when the `python_wheel_task` field is present."""

    retry_on_timeout: Optional[bool] = None
    """An optional policy to specify whether to retry a job when it times out. The default behavior is
    to not retry on timeout."""

    run_if: Optional[RunIf] = None
    """An optional value specifying the condition determining whether the task is run once its
    dependencies have been completed.
    
    * `ALL_SUCCESS`: All dependencies have executed and succeeded * `AT_LEAST_ONE_SUCCESS`: At least
    one dependency has succeeded * `NONE_FAILED`: None of the dependencies have failed and at least
    one was executed * `ALL_DONE`: All dependencies have been completed * `AT_LEAST_ONE_FAILED`: At
    least one dependency failed * `ALL_FAILED`: ALl dependencies have failed"""

    run_job_task: Optional[RunJobTask] = None
    """The task triggers another job when the `run_job_task` field is present."""

    spark_jar_task: Optional[SparkJarTask] = None
    """The task runs a JAR when the `spark_jar_task` field is present."""

    spark_python_task: Optional[SparkPythonTask] = None
    """The task runs a Python file when the `spark_python_task` field is present."""

    spark_submit_task: Optional[SparkSubmitTask] = None
    """(Legacy) The task runs the spark-submit script when the spark_submit_task field is present.
    Databricks recommends using the spark_jar_task instead; see [Spark Submit task for
    jobs](/jobs/spark-submit)."""

    sql_task: Optional[SqlTask] = None
    """The task runs a SQL query or file, or it refreshes a SQL alert or a legacy SQL dashboard when
    the `sql_task` field is present."""

    timeout_seconds: Optional[int] = None
    """An optional timeout applied to each run of this job task. A value of `0` means no timeout."""

    webhook_notifications: Optional[WebhookNotifications] = None
    """A collection of system notification IDs to notify when runs of this task begin or complete. The
    default behavior is to not send any system notifications."""

    def as_dict(self) -> dict:
        """Serializes the Task into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.clean_rooms_notebook_task:
            body["clean_rooms_notebook_task"] = self.clean_rooms_notebook_task.as_dict()
        if self.compute:
            body["compute"] = self.compute.as_dict()
        if self.condition_task:
            body["condition_task"] = self.condition_task.as_dict()
        if self.dashboard_task:
            body["dashboard_task"] = self.dashboard_task.as_dict()
        if self.dbt_cloud_task:
            body["dbt_cloud_task"] = self.dbt_cloud_task.as_dict()
        if self.dbt_platform_task:
            body["dbt_platform_task"] = self.dbt_platform_task.as_dict()
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task.as_dict()
        if self.depends_on:
            body["depends_on"] = [v.as_dict() for v in self.depends_on]
        if self.description is not None:
            body["description"] = self.description
        if self.disable_auto_optimization is not None:
            body["disable_auto_optimization"] = self.disable_auto_optimization
        if self.disabled is not None:
            body["disabled"] = self.disabled
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications.as_dict()
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.for_each_task:
            body["for_each_task"] = self.for_each_task.as_dict()
        if self.gen_ai_compute_task:
            body["gen_ai_compute_task"] = self.gen_ai_compute_task.as_dict()
        if self.health:
            body["health"] = self.health.as_dict()
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.libraries:
            body["libraries"] = [v.as_dict() for v in self.libraries]
        if self.max_retries is not None:
            body["max_retries"] = self.max_retries
        if self.min_retry_interval_millis is not None:
            body["min_retry_interval_millis"] = self.min_retry_interval_millis
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster.as_dict()
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task.as_dict()
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings.as_dict()
        if self.pipeline_task:
            body["pipeline_task"] = self.pipeline_task.as_dict()
        if self.power_bi_task:
            body["power_bi_task"] = self.power_bi_task.as_dict()
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task.as_dict()
        if self.retry_on_timeout is not None:
            body["retry_on_timeout"] = self.retry_on_timeout
        if self.run_if is not None:
            body["run_if"] = self.run_if.value
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task.as_dict()
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task.as_dict()
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task.as_dict()
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task.as_dict()
        if self.sql_task:
            body["sql_task"] = self.sql_task.as_dict()
        if self.task_key is not None:
            body["task_key"] = self.task_key
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Task into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.clean_rooms_notebook_task:
            body["clean_rooms_notebook_task"] = self.clean_rooms_notebook_task
        if self.compute:
            body["compute"] = self.compute
        if self.condition_task:
            body["condition_task"] = self.condition_task
        if self.dashboard_task:
            body["dashboard_task"] = self.dashboard_task
        if self.dbt_cloud_task:
            body["dbt_cloud_task"] = self.dbt_cloud_task
        if self.dbt_platform_task:
            body["dbt_platform_task"] = self.dbt_platform_task
        if self.dbt_task:
            body["dbt_task"] = self.dbt_task
        if self.depends_on:
            body["depends_on"] = self.depends_on
        if self.description is not None:
            body["description"] = self.description
        if self.disable_auto_optimization is not None:
            body["disable_auto_optimization"] = self.disable_auto_optimization
        if self.disabled is not None:
            body["disabled"] = self.disabled
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications
        if self.environment_key is not None:
            body["environment_key"] = self.environment_key
        if self.existing_cluster_id is not None:
            body["existing_cluster_id"] = self.existing_cluster_id
        if self.for_each_task:
            body["for_each_task"] = self.for_each_task
        if self.gen_ai_compute_task:
            body["gen_ai_compute_task"] = self.gen_ai_compute_task
        if self.health:
            body["health"] = self.health
        if self.job_cluster_key is not None:
            body["job_cluster_key"] = self.job_cluster_key
        if self.libraries:
            body["libraries"] = self.libraries
        if self.max_retries is not None:
            body["max_retries"] = self.max_retries
        if self.min_retry_interval_millis is not None:
            body["min_retry_interval_millis"] = self.min_retry_interval_millis
        if self.new_cluster:
            body["new_cluster"] = self.new_cluster
        if self.notebook_task:
            body["notebook_task"] = self.notebook_task
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings
        if self.pipeline_task:
            body["pipeline_task"] = self.pipeline_task
        if self.power_bi_task:
            body["power_bi_task"] = self.power_bi_task
        if self.python_wheel_task:
            body["python_wheel_task"] = self.python_wheel_task
        if self.retry_on_timeout is not None:
            body["retry_on_timeout"] = self.retry_on_timeout
        if self.run_if is not None:
            body["run_if"] = self.run_if
        if self.run_job_task:
            body["run_job_task"] = self.run_job_task
        if self.spark_jar_task:
            body["spark_jar_task"] = self.spark_jar_task
        if self.spark_python_task:
            body["spark_python_task"] = self.spark_python_task
        if self.spark_submit_task:
            body["spark_submit_task"] = self.spark_submit_task
        if self.sql_task:
            body["sql_task"] = self.sql_task
        if self.task_key is not None:
            body["task_key"] = self.task_key
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Task:
        """Deserializes the Task from a dictionary."""
        return cls(
            clean_rooms_notebook_task=_from_dict(d, "clean_rooms_notebook_task", CleanRoomsNotebookTask),
            compute=_from_dict(d, "compute", Compute),
            condition_task=_from_dict(d, "condition_task", ConditionTask),
            dashboard_task=_from_dict(d, "dashboard_task", DashboardTask),
            dbt_cloud_task=_from_dict(d, "dbt_cloud_task", DbtCloudTask),
            dbt_platform_task=_from_dict(d, "dbt_platform_task", DbtPlatformTask),
            dbt_task=_from_dict(d, "dbt_task", DbtTask),
            depends_on=_repeated_dict(d, "depends_on", TaskDependency),
            description=d.get("description", None),
            disable_auto_optimization=d.get("disable_auto_optimization", None),
            disabled=d.get("disabled", None),
            email_notifications=_from_dict(d, "email_notifications", TaskEmailNotifications),
            environment_key=d.get("environment_key", None),
            existing_cluster_id=d.get("existing_cluster_id", None),
            for_each_task=_from_dict(d, "for_each_task", ForEachTask),
            gen_ai_compute_task=_from_dict(d, "gen_ai_compute_task", GenAiComputeTask),
            health=_from_dict(d, "health", JobsHealthRules),
            job_cluster_key=d.get("job_cluster_key", None),
            libraries=_repeated_dict(d, "libraries", compute.Library),
            max_retries=d.get("max_retries", None),
            min_retry_interval_millis=d.get("min_retry_interval_millis", None),
            new_cluster=_from_dict(d, "new_cluster", compute.ClusterSpec),
            notebook_task=_from_dict(d, "notebook_task", NotebookTask),
            notification_settings=_from_dict(d, "notification_settings", TaskNotificationSettings),
            pipeline_task=_from_dict(d, "pipeline_task", PipelineTask),
            power_bi_task=_from_dict(d, "power_bi_task", PowerBiTask),
            python_wheel_task=_from_dict(d, "python_wheel_task", PythonWheelTask),
            retry_on_timeout=d.get("retry_on_timeout", None),
            run_if=_enum(d, "run_if", RunIf),
            run_job_task=_from_dict(d, "run_job_task", RunJobTask),
            spark_jar_task=_from_dict(d, "spark_jar_task", SparkJarTask),
            spark_python_task=_from_dict(d, "spark_python_task", SparkPythonTask),
            spark_submit_task=_from_dict(d, "spark_submit_task", SparkSubmitTask),
            sql_task=_from_dict(d, "sql_task", SqlTask),
            task_key=d.get("task_key", None),
            timeout_seconds=d.get("timeout_seconds", None),
            webhook_notifications=_from_dict(d, "webhook_notifications", WebhookNotifications),
        )


@dataclass
class TaskDependency:
    task_key: str
    """The name of the task this task depends on."""

    outcome: Optional[str] = None
    """Can only be specified on condition task dependencies. The outcome of the dependent task that
    must be met for this task to run."""

    def as_dict(self) -> dict:
        """Serializes the TaskDependency into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.outcome is not None:
            body["outcome"] = self.outcome
        if self.task_key is not None:
            body["task_key"] = self.task_key
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TaskDependency into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.outcome is not None:
            body["outcome"] = self.outcome
        if self.task_key is not None:
            body["task_key"] = self.task_key
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TaskDependency:
        """Deserializes the TaskDependency from a dictionary."""
        return cls(outcome=d.get("outcome", None), task_key=d.get("task_key", None))


@dataclass
class TaskEmailNotifications:
    no_alert_for_skipped_runs: Optional[bool] = None
    """If true, do not send email to recipients specified in `on_failure` if the run is skipped. This
    field is `deprecated`. Please use the `notification_settings.no_alert_for_skipped_runs` field."""

    on_duration_warning_threshold_exceeded: Optional[List[str]] = None
    """A list of email addresses to be notified when the duration of a run exceeds the threshold
    specified for the `RUN_DURATION_SECONDS` metric in the `health` field. If no rule for the
    `RUN_DURATION_SECONDS` metric is specified in the `health` field for the job, notifications are
    not sent."""

    on_failure: Optional[List[str]] = None
    """A list of email addresses to be notified when a run unsuccessfully completes. A run is
    considered to have completed unsuccessfully if it ends with an `INTERNAL_ERROR`
    `life_cycle_state` or a `FAILED`, or `TIMED_OUT` result_state. If this is not specified on job
    creation, reset, or update the list is empty, and notifications are not sent."""

    on_start: Optional[List[str]] = None
    """A list of email addresses to be notified when a run begins. If not specified on job creation,
    reset, or update, the list is empty, and notifications are not sent."""

    on_streaming_backlog_exceeded: Optional[List[str]] = None
    """A list of email addresses to notify when any streaming backlog thresholds are exceeded for any
    stream. Streaming backlog thresholds can be set in the `health` field using the following
    metrics: `STREAMING_BACKLOG_BYTES`, `STREAMING_BACKLOG_RECORDS`, `STREAMING_BACKLOG_SECONDS`, or
    `STREAMING_BACKLOG_FILES`. Alerting is based on the 10-minute average of these metrics. If the
    issue persists, notifications are resent every 30 minutes."""

    on_success: Optional[List[str]] = None
    """A list of email addresses to be notified when a run successfully completes. A run is considered
    to have completed successfully if it ends with a `TERMINATED` `life_cycle_state` and a `SUCCESS`
    result_state. If not specified on job creation, reset, or update, the list is empty, and
    notifications are not sent."""

    def as_dict(self) -> dict:
        """Serializes the TaskEmailNotifications into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        if self.on_duration_warning_threshold_exceeded:
            body["on_duration_warning_threshold_exceeded"] = [v for v in self.on_duration_warning_threshold_exceeded]
        if self.on_failure:
            body["on_failure"] = [v for v in self.on_failure]
        if self.on_start:
            body["on_start"] = [v for v in self.on_start]
        if self.on_streaming_backlog_exceeded:
            body["on_streaming_backlog_exceeded"] = [v for v in self.on_streaming_backlog_exceeded]
        if self.on_success:
            body["on_success"] = [v for v in self.on_success]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TaskEmailNotifications into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        if self.on_duration_warning_threshold_exceeded:
            body["on_duration_warning_threshold_exceeded"] = self.on_duration_warning_threshold_exceeded
        if self.on_failure:
            body["on_failure"] = self.on_failure
        if self.on_start:
            body["on_start"] = self.on_start
        if self.on_streaming_backlog_exceeded:
            body["on_streaming_backlog_exceeded"] = self.on_streaming_backlog_exceeded
        if self.on_success:
            body["on_success"] = self.on_success
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TaskEmailNotifications:
        """Deserializes the TaskEmailNotifications from a dictionary."""
        return cls(
            no_alert_for_skipped_runs=d.get("no_alert_for_skipped_runs", None),
            on_duration_warning_threshold_exceeded=d.get("on_duration_warning_threshold_exceeded", None),
            on_failure=d.get("on_failure", None),
            on_start=d.get("on_start", None),
            on_streaming_backlog_exceeded=d.get("on_streaming_backlog_exceeded", None),
            on_success=d.get("on_success", None),
        )


@dataclass
class TaskNotificationSettings:
    alert_on_last_attempt: Optional[bool] = None
    """If true, do not send notifications to recipients specified in `on_start` for the retried runs
    and do not send notifications to recipients specified in `on_failure` until the last retry of
    the run."""

    no_alert_for_canceled_runs: Optional[bool] = None
    """If true, do not send notifications to recipients specified in `on_failure` if the run is
    canceled."""

    no_alert_for_skipped_runs: Optional[bool] = None
    """If true, do not send notifications to recipients specified in `on_failure` if the run is
    skipped."""

    def as_dict(self) -> dict:
        """Serializes the TaskNotificationSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.alert_on_last_attempt is not None:
            body["alert_on_last_attempt"] = self.alert_on_last_attempt
        if self.no_alert_for_canceled_runs is not None:
            body["no_alert_for_canceled_runs"] = self.no_alert_for_canceled_runs
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TaskNotificationSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.alert_on_last_attempt is not None:
            body["alert_on_last_attempt"] = self.alert_on_last_attempt
        if self.no_alert_for_canceled_runs is not None:
            body["no_alert_for_canceled_runs"] = self.no_alert_for_canceled_runs
        if self.no_alert_for_skipped_runs is not None:
            body["no_alert_for_skipped_runs"] = self.no_alert_for_skipped_runs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TaskNotificationSettings:
        """Deserializes the TaskNotificationSettings from a dictionary."""
        return cls(
            alert_on_last_attempt=d.get("alert_on_last_attempt", None),
            no_alert_for_canceled_runs=d.get("no_alert_for_canceled_runs", None),
            no_alert_for_skipped_runs=d.get("no_alert_for_skipped_runs", None),
        )


class TaskRetryMode(Enum):
    """task retry mode of the continuous job * NEVER: The failed task will not be retried. *
    ON_FAILURE: Retry a failed task if at least one other task in the job is still running its first
    attempt. When this condition is no longer met or the retry limit is reached, the job run is
    cancelled and a new run is started."""

    NEVER = "NEVER"
    ON_FAILURE = "ON_FAILURE"


class TerminationCodeCode(Enum):
    """The code indicates why the run was terminated. Additional codes might be introduced in future
    releases. * `SUCCESS`: The run was completed successfully. * `SUCCESS_WITH_FAILURES`: The run
    was completed successfully but some child runs failed. * `USER_CANCELED`: The run was
    successfully canceled during execution by a user. * `CANCELED`: The run was canceled during
    execution by the Databricks platform; for example, if the maximum run duration was exceeded. *
    `SKIPPED`: Run was never executed, for example, if the upstream task run failed, the dependency
    type condition was not met, or there were no material tasks to execute. * `INTERNAL_ERROR`: The
    run encountered an unexpected error. Refer to the state message for further details. *
    `DRIVER_ERROR`: The run encountered an error while communicating with the Spark Driver. *
    `CLUSTER_ERROR`: The run failed due to a cluster error. Refer to the state message for further
    details. * `REPOSITORY_CHECKOUT_FAILED`: Failed to complete the checkout due to an error when
    communicating with the third party service. * `INVALID_CLUSTER_REQUEST`: The run failed because
    it issued an invalid request to start the cluster. * `WORKSPACE_RUN_LIMIT_EXCEEDED`: The
    workspace has reached the quota for the maximum number of concurrent active runs. Consider
    scheduling the runs over a larger time frame. * `FEATURE_DISABLED`: The run failed because it
    tried to access a feature unavailable for the workspace. * `CLUSTER_REQUEST_LIMIT_EXCEEDED`: The
    number of cluster creation, start, and upsize requests have exceeded the allotted rate limit.
    Consider spreading the run execution over a larger time frame. * `STORAGE_ACCESS_ERROR`: The run
    failed due to an error when accessing the customer blob storage. Refer to the state message for
    further details. * `RUN_EXECUTION_ERROR`: The run was completed with task failures. For more
    details, refer to the state message or run output. * `UNAUTHORIZED_ERROR`: The run failed due to
    a permission issue while accessing a resource. Refer to the state message for further details. *
    `LIBRARY_INSTALLATION_ERROR`: The run failed while installing the user-requested library. Refer
    to the state message for further details. The causes might include, but are not limited to: The
    provided library is invalid, there are insufficient permissions to install the library, and so
    forth. * `MAX_CONCURRENT_RUNS_EXCEEDED`: The scheduled run exceeds the limit of maximum
    concurrent runs set for the job. * `MAX_SPARK_CONTEXTS_EXCEEDED`: The run is scheduled on a
    cluster that has already reached the maximum number of contexts it is configured to create. See:
    [Link]. * `RESOURCE_NOT_FOUND`: A resource necessary for run execution does not exist. Refer to
    the state message for further details. * `INVALID_RUN_CONFIGURATION`: The run failed due to an
    invalid configuration. Refer to the state message for further details. * `CLOUD_FAILURE`: The
    run failed due to a cloud provider issue. Refer to the state message for further details. *
    `MAX_JOB_QUEUE_SIZE_EXCEEDED`: The run was skipped due to reaching the job level queue size
    limit. * `DISABLED`: The run was never executed because it was disabled explicitly by the user.
    * `BREAKING_CHANGE`: Run failed because of an intentional breaking change in Spark, but it will
    be retried with a mitigation config.

    [Link]: https://kb.databricks.com/en_US/notebooks/too-many-execution-contexts-are-open-right-now"""

    BUDGET_POLICY_LIMIT_EXCEEDED = "BUDGET_POLICY_LIMIT_EXCEEDED"
    CANCELED = "CANCELED"
    CLOUD_FAILURE = "CLOUD_FAILURE"
    CLUSTER_ERROR = "CLUSTER_ERROR"
    CLUSTER_REQUEST_LIMIT_EXCEEDED = "CLUSTER_REQUEST_LIMIT_EXCEEDED"
    DISABLED = "DISABLED"
    DRIVER_ERROR = "DRIVER_ERROR"
    FEATURE_DISABLED = "FEATURE_DISABLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    INVALID_CLUSTER_REQUEST = "INVALID_CLUSTER_REQUEST"
    INVALID_RUN_CONFIGURATION = "INVALID_RUN_CONFIGURATION"
    LIBRARY_INSTALLATION_ERROR = "LIBRARY_INSTALLATION_ERROR"
    MAX_CONCURRENT_RUNS_EXCEEDED = "MAX_CONCURRENT_RUNS_EXCEEDED"
    MAX_JOB_QUEUE_SIZE_EXCEEDED = "MAX_JOB_QUEUE_SIZE_EXCEEDED"
    MAX_SPARK_CONTEXTS_EXCEEDED = "MAX_SPARK_CONTEXTS_EXCEEDED"
    REPOSITORY_CHECKOUT_FAILED = "REPOSITORY_CHECKOUT_FAILED"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    RUN_EXECUTION_ERROR = "RUN_EXECUTION_ERROR"
    SKIPPED = "SKIPPED"
    STORAGE_ACCESS_ERROR = "STORAGE_ACCESS_ERROR"
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_FAILURES = "SUCCESS_WITH_FAILURES"
    UNAUTHORIZED_ERROR = "UNAUTHORIZED_ERROR"
    USER_CANCELED = "USER_CANCELED"
    WORKSPACE_RUN_LIMIT_EXCEEDED = "WORKSPACE_RUN_LIMIT_EXCEEDED"


@dataclass
class TerminationDetails:
    code: Optional[TerminationCodeCode] = None

    message: Optional[str] = None
    """A descriptive message with the termination details. This field is unstructured and the format
    might change."""

    type: Optional[TerminationTypeType] = None

    def as_dict(self) -> dict:
        """Serializes the TerminationDetails into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.code is not None:
            body["code"] = self.code.value
        if self.message is not None:
            body["message"] = self.message
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TerminationDetails into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.code is not None:
            body["code"] = self.code
        if self.message is not None:
            body["message"] = self.message
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TerminationDetails:
        """Deserializes the TerminationDetails from a dictionary."""
        return cls(
            code=_enum(d, "code", TerminationCodeCode),
            message=d.get("message", None),
            type=_enum(d, "type", TerminationTypeType),
        )


class TerminationTypeType(Enum):
    """* `SUCCESS`: The run terminated without any issues * `INTERNAL_ERROR`: An error occurred in the
    Databricks platform. Please look at the [status page] or contact support if the issue persists.
    * `CLIENT_ERROR`: The run was terminated because of an error caused by user input or the job
    configuration. * `CLOUD_FAILURE`: The run was terminated because of an issue with your cloud
    provider.

    [status page]: https://status.databricks.com/"""

    CLIENT_ERROR = "CLIENT_ERROR"
    CLOUD_FAILURE = "CLOUD_FAILURE"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SUCCESS = "SUCCESS"


@dataclass
class TriggerInfo:
    """Additional details about what triggered the run"""

    run_id: Optional[int] = None
    """The run id of the Run Job task run"""

    def as_dict(self) -> dict:
        """Serializes the TriggerInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TriggerInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.run_id is not None:
            body["run_id"] = self.run_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TriggerInfo:
        """Deserializes the TriggerInfo from a dictionary."""
        return cls(run_id=d.get("run_id", None))


@dataclass
class TriggerSettings:
    file_arrival: Optional[FileArrivalTriggerConfiguration] = None
    """File arrival trigger settings."""

    model: Optional[ModelTriggerConfiguration] = None

    pause_status: Optional[PauseStatus] = None
    """Whether this trigger is paused or not."""

    periodic: Optional[PeriodicTriggerConfiguration] = None
    """Periodic trigger settings."""

    table_update: Optional[TableUpdateTriggerConfiguration] = None

    def as_dict(self) -> dict:
        """Serializes the TriggerSettings into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_arrival:
            body["file_arrival"] = self.file_arrival.as_dict()
        if self.model:
            body["model"] = self.model.as_dict()
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status.value
        if self.periodic:
            body["periodic"] = self.periodic.as_dict()
        if self.table_update:
            body["table_update"] = self.table_update.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TriggerSettings into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_arrival:
            body["file_arrival"] = self.file_arrival
        if self.model:
            body["model"] = self.model
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status
        if self.periodic:
            body["periodic"] = self.periodic
        if self.table_update:
            body["table_update"] = self.table_update
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TriggerSettings:
        """Deserializes the TriggerSettings from a dictionary."""
        return cls(
            file_arrival=_from_dict(d, "file_arrival", FileArrivalTriggerConfiguration),
            model=_from_dict(d, "model", ModelTriggerConfiguration),
            pause_status=_enum(d, "pause_status", PauseStatus),
            periodic=_from_dict(d, "periodic", PeriodicTriggerConfiguration),
            table_update=_from_dict(d, "table_update", TableUpdateTriggerConfiguration),
        )


@dataclass
class TriggerStateProto:
    file_arrival: Optional[FileArrivalTriggerState] = None

    table: Optional[TableTriggerState] = None

    def as_dict(self) -> dict:
        """Serializes the TriggerStateProto into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_arrival:
            body["file_arrival"] = self.file_arrival.as_dict()
        if self.table:
            body["table"] = self.table.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TriggerStateProto into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_arrival:
            body["file_arrival"] = self.file_arrival
        if self.table:
            body["table"] = self.table
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TriggerStateProto:
        """Deserializes the TriggerStateProto from a dictionary."""
        return cls(
            file_arrival=_from_dict(d, "file_arrival", FileArrivalTriggerState),
            table=_from_dict(d, "table", TableTriggerState),
        )


class TriggerType(Enum):
    """The type of trigger that fired this run.

    * `PERIODIC`: Schedules that periodically trigger runs, such as a cron scheduler. * `ONE_TIME`:
    One time triggers that fire a single run. This occurs you triggered a single run on demand
    through the UI or the API. * `RETRY`: Indicates a run that is triggered as a retry of a
    previously failed run. This occurs when you request to re-run the job in case of failures. *
    `RUN_JOB_TASK`: Indicates a run that is triggered using a Run Job task. * `FILE_ARRIVAL`:
    Indicates a run that is triggered by a file arrival. * `CONTINUOUS`: Indicates a run that is
    triggered by a continuous job. * `TABLE`: Indicates a run that is triggered by a table update. *
    `CONTINUOUS_RESTART`: Indicates a run created by user to manually restart a continuous job run.
    * `MODEL`: Indicates a run that is triggered by a model update."""

    CONTINUOUS = "CONTINUOUS"
    CONTINUOUS_RESTART = "CONTINUOUS_RESTART"
    FILE_ARRIVAL = "FILE_ARRIVAL"
    ONE_TIME = "ONE_TIME"
    PERIODIC = "PERIODIC"
    RETRY = "RETRY"
    RUN_JOB_TASK = "RUN_JOB_TASK"
    TABLE = "TABLE"


@dataclass
class ViewItem:
    content: Optional[str] = None
    """Content of the view."""

    name: Optional[str] = None
    """Name of the view item. In the case of code view, it would be the notebook’s name. In the case
    of dashboard view, it would be the dashboard’s name."""

    type: Optional[ViewType] = None
    """Type of the view item."""

    def as_dict(self) -> dict:
        """Serializes the ViewItem into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.content is not None:
            body["content"] = self.content
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ViewItem into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.content is not None:
            body["content"] = self.content
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ViewItem:
        """Deserializes the ViewItem from a dictionary."""
        return cls(content=d.get("content", None), name=d.get("name", None), type=_enum(d, "type", ViewType))


class ViewType(Enum):
    """* `NOTEBOOK`: Notebook view item. * `DASHBOARD`: Dashboard view item."""

    DASHBOARD = "DASHBOARD"
    NOTEBOOK = "NOTEBOOK"


class ViewsToExport(Enum):
    """* `CODE`: Code view of the notebook. * `DASHBOARDS`: All dashboard views of the notebook. *
    `ALL`: All views of the notebook."""

    ALL = "ALL"
    CODE = "CODE"
    DASHBOARDS = "DASHBOARDS"


@dataclass
class Webhook:
    id: str

    def as_dict(self) -> dict:
        """Serializes the Webhook into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Webhook into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Webhook:
        """Deserializes the Webhook from a dictionary."""
        return cls(id=d.get("id", None))


@dataclass
class WebhookNotifications:
    on_duration_warning_threshold_exceeded: Optional[List[Webhook]] = None
    """An optional list of system notification IDs to call when the duration of a run exceeds the
    threshold specified for the `RUN_DURATION_SECONDS` metric in the `health` field. A maximum of 3
    destinations can be specified for the `on_duration_warning_threshold_exceeded` property."""

    on_failure: Optional[List[Webhook]] = None
    """An optional list of system notification IDs to call when the run fails. A maximum of 3
    destinations can be specified for the `on_failure` property."""

    on_start: Optional[List[Webhook]] = None
    """An optional list of system notification IDs to call when the run starts. A maximum of 3
    destinations can be specified for the `on_start` property."""

    on_streaming_backlog_exceeded: Optional[List[Webhook]] = None
    """An optional list of system notification IDs to call when any streaming backlog thresholds are
    exceeded for any stream. Streaming backlog thresholds can be set in the `health` field using the
    following metrics: `STREAMING_BACKLOG_BYTES`, `STREAMING_BACKLOG_RECORDS`,
    `STREAMING_BACKLOG_SECONDS`, or `STREAMING_BACKLOG_FILES`. Alerting is based on the 10-minute
    average of these metrics. If the issue persists, notifications are resent every 30 minutes. A
    maximum of 3 destinations can be specified for the `on_streaming_backlog_exceeded` property."""

    on_success: Optional[List[Webhook]] = None
    """An optional list of system notification IDs to call when the run completes successfully. A
    maximum of 3 destinations can be specified for the `on_success` property."""

    def as_dict(self) -> dict:
        """Serializes the WebhookNotifications into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.on_duration_warning_threshold_exceeded:
            body["on_duration_warning_threshold_exceeded"] = [
                v.as_dict() for v in self.on_duration_warning_threshold_exceeded
            ]
        if self.on_failure:
            body["on_failure"] = [v.as_dict() for v in self.on_failure]
        if self.on_start:
            body["on_start"] = [v.as_dict() for v in self.on_start]
        if self.on_streaming_backlog_exceeded:
            body["on_streaming_backlog_exceeded"] = [v.as_dict() for v in self.on_streaming_backlog_exceeded]
        if self.on_success:
            body["on_success"] = [v.as_dict() for v in self.on_success]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WebhookNotifications into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.on_duration_warning_threshold_exceeded:
            body["on_duration_warning_threshold_exceeded"] = self.on_duration_warning_threshold_exceeded
        if self.on_failure:
            body["on_failure"] = self.on_failure
        if self.on_start:
            body["on_start"] = self.on_start
        if self.on_streaming_backlog_exceeded:
            body["on_streaming_backlog_exceeded"] = self.on_streaming_backlog_exceeded
        if self.on_success:
            body["on_success"] = self.on_success
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WebhookNotifications:
        """Deserializes the WebhookNotifications from a dictionary."""
        return cls(
            on_duration_warning_threshold_exceeded=_repeated_dict(d, "on_duration_warning_threshold_exceeded", Webhook),
            on_failure=_repeated_dict(d, "on_failure", Webhook),
            on_start=_repeated_dict(d, "on_start", Webhook),
            on_streaming_backlog_exceeded=_repeated_dict(d, "on_streaming_backlog_exceeded", Webhook),
            on_success=_repeated_dict(d, "on_success", Webhook),
        )


@dataclass
class WidgetErrorDetail:
    message: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the WidgetErrorDetail into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the WidgetErrorDetail into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.message is not None:
            body["message"] = self.message
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> WidgetErrorDetail:
        """Deserializes the WidgetErrorDetail from a dictionary."""
        return cls(message=d.get("message", None))


class JobsAPI:
    """The Jobs API allows you to create, edit, and delete jobs.

    You can use a Databricks job to run a data processing or data analysis task in a Databricks cluster with
    scalable resources. Your job can consist of a single task or can be a large, multi-task workflow with
    complex dependencies. Databricks manages the task orchestration, cluster management, monitoring, and error
    reporting for all of your jobs. You can run your jobs immediately or periodically through an easy-to-use
    scheduling system. You can implement job tasks using notebooks, JARS, Spark Declarative Pipelines, or
    Python, Scala, Spark submit, and Java applications.

    You should never hard code secrets or store them in plain text. Use the [Secrets CLI] to manage secrets in
    the [Databricks CLI]. Use the [Secrets utility] to reference secrets in notebooks and jobs.

    [Databricks CLI]: https://docs.databricks.com/dev-tools/cli/index.html
    [Secrets CLI]: https://docs.databricks.com/dev-tools/cli/secrets-cli.html
    [Secrets utility]: https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-secrets"""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_run_job_terminated_or_skipped(
        self, run_id: int, timeout=timedelta(minutes=20), callback: Optional[Callable[[Run], None]] = None
    ) -> Run:
        deadline = time.time() + timeout.total_seconds()
        target_states = (
            RunLifeCycleState.TERMINATED,
            RunLifeCycleState.SKIPPED,
        )
        failure_states = (RunLifeCycleState.INTERNAL_ERROR,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get_run(run_id=run_id)
            status = poll.state.life_cycle_state
            status_message = f"current status: {status}"
            if poll.state:
                status_message = poll.state.state_message
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach TERMINATED or SKIPPED, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"run_id={run_id}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def cancel_all_runs(self, *, all_queued_runs: Optional[bool] = None, job_id: Optional[int] = None):
        """Cancels all active runs of a job. The runs are canceled asynchronously, so it doesn't prevent new runs
        from being started.

        :param all_queued_runs: bool (optional)
          Optional boolean parameter to cancel all queued runs. If no job_id is provided, all queued runs in
          the workspace are canceled.
        :param job_id: int (optional)
          The canonical identifier of the job to cancel all runs of.


        """

        body = {}
        if all_queued_runs is not None:
            body["all_queued_runs"] = all_queued_runs
        if job_id is not None:
            body["job_id"] = job_id
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.2/jobs/runs/cancel-all", body=body, headers=headers)

    def cancel_run(self, run_id: int) -> Wait[Run]:
        """Cancels a job run or a task run. The run is canceled asynchronously, so it may still be running when
        this request completes.

        :param run_id: int
          This field is required.

        :returns:
          Long-running operation waiter for :class:`Run`.
          See :method:wait_get_run_job_terminated_or_skipped for more details.
        """

        body = {}
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.2/jobs/runs/cancel", body=body, headers=headers)
        return Wait(self.wait_get_run_job_terminated_or_skipped, run_id=run_id)

    def cancel_run_and_wait(self, run_id: int, timeout=timedelta(minutes=20)) -> Run:
        return self.cancel_run(run_id=run_id).result(timeout=timeout)

    def create(
        self,
        *,
        access_control_list: Optional[List[JobAccessControlRequest]] = None,
        budget_policy_id: Optional[str] = None,
        continuous: Optional[Continuous] = None,
        deployment: Optional[JobDeployment] = None,
        description: Optional[str] = None,
        edit_mode: Optional[JobEditMode] = None,
        email_notifications: Optional[JobEmailNotifications] = None,
        environments: Optional[List[JobEnvironment]] = None,
        format: Optional[Format] = None,
        git_source: Optional[GitSource] = None,
        health: Optional[JobsHealthRules] = None,
        job_clusters: Optional[List[JobCluster]] = None,
        max_concurrent_runs: Optional[int] = None,
        name: Optional[str] = None,
        notification_settings: Optional[JobNotificationSettings] = None,
        parameters: Optional[List[JobParameterDefinition]] = None,
        performance_target: Optional[PerformanceTarget] = None,
        queue: Optional[QueueSettings] = None,
        run_as: Optional[JobRunAs] = None,
        schedule: Optional[CronSchedule] = None,
        tags: Optional[Dict[str, str]] = None,
        tasks: Optional[List[Task]] = None,
        timeout_seconds: Optional[int] = None,
        trigger: Optional[TriggerSettings] = None,
        usage_policy_id: Optional[str] = None,
        webhook_notifications: Optional[WebhookNotifications] = None,
    ) -> CreateResponse:
        """Create a new job.

        :param access_control_list: List[:class:`JobAccessControlRequest`] (optional)
          List of permissions to set on the job.
        :param budget_policy_id: str (optional)
          The id of the user specified budget policy to use for this job. If not specified, a default budget
          policy may be applied when creating or modifying the job. See `effective_budget_policy_id` for the
          budget policy used by this workload.
        :param continuous: :class:`Continuous` (optional)
          An optional continuous property for this job. The continuous property will ensure that there is
          always one run executing. Only one of `schedule` and `continuous` can be used.
        :param deployment: :class:`JobDeployment` (optional)
          Deployment information for jobs managed by external sources.
        :param description: str (optional)
          An optional description for the job. The maximum length is 27700 characters in UTF-8 encoding.
        :param edit_mode: :class:`JobEditMode` (optional)
          Edit mode of the job.

          * `UI_LOCKED`: The job is in a locked UI state and cannot be modified. * `EDITABLE`: The job is in
          an editable state and can be modified.
        :param email_notifications: :class:`JobEmailNotifications` (optional)
          An optional set of email addresses that is notified when runs of this job begin or complete as well
          as when this job is deleted.
        :param environments: List[:class:`JobEnvironment`] (optional)
          A list of task execution environment specifications that can be referenced by serverless tasks of
          this job. For serverless notebook tasks, if the environment_key is not specified, the notebook
          environment will be used if present. If a jobs environment is specified, it will override the
          notebook environment. For other serverless tasks, the task environment is required to be specified
          using environment_key in the task settings.
        :param format: :class:`Format` (optional)
          Used to tell what is the format of the job. This field is ignored in Create/Update/Reset calls. When
          using the Jobs API 2.1 this value is always set to `"MULTI_TASK"`.
        :param git_source: :class:`GitSource` (optional)
          An optional specification for a remote Git repository containing the source code used by tasks.
          Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.

          If `git_source` is set, these tasks retrieve the file from the remote repository by default.
          However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task.

          Note: dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks are
          used, `git_source` must be defined on the job.
        :param health: :class:`JobsHealthRules` (optional)
        :param job_clusters: List[:class:`JobCluster`] (optional)
          A list of job cluster specifications that can be shared and reused by tasks of this job. Libraries
          cannot be declared in a shared job cluster. You must declare dependent libraries in task settings.
        :param max_concurrent_runs: int (optional)
          An optional maximum allowed number of concurrent runs of the job. Set this value if you want to be
          able to execute multiple runs of the same job concurrently. This is useful for example if you
          trigger your job on a frequent schedule and want to allow consecutive runs to overlap with each
          other, or if you want to trigger multiple runs which differ by their input parameters. This setting
          affects only new runs. For example, suppose the job’s concurrency is 4 and there are 4 concurrent
          active runs. Then setting the concurrency to 3 won’t kill any of the active runs. However, from
          then on, new runs are skipped unless there are fewer than 3 active runs. This value cannot exceed
          1000. Setting this value to `0` causes all new runs to be skipped.
        :param name: str (optional)
          An optional name for the job. The maximum length is 4096 bytes in UTF-8 encoding.
        :param notification_settings: :class:`JobNotificationSettings` (optional)
          Optional notification settings that are used when sending notifications to each of the
          `email_notifications` and `webhook_notifications` for this job.
        :param parameters: List[:class:`JobParameterDefinition`] (optional)
          Job-level parameter definitions
        :param performance_target: :class:`PerformanceTarget` (optional)
          The performance mode on a serverless job. This field determines the level of compute performance or
          cost-efficiency for the run. The performance target does not apply to tasks that run on Serverless
          GPU compute.

          * `STANDARD`: Enables cost-efficient execution of serverless workloads. * `PERFORMANCE_OPTIMIZED`:
          Prioritizes fast startup and execution times through rapid scaling and optimized cluster
          performance.
        :param queue: :class:`QueueSettings` (optional)
          The queue settings of the job.
        :param run_as: :class:`JobRunAs` (optional)
          The user or service principal that the job runs as, if specified in the request. This field
          indicates the explicit configuration of `run_as` for the job. To find the value in all cases,
          explicit or implicit, use `run_as_user_name`.
        :param schedule: :class:`CronSchedule` (optional)
          An optional periodic schedule for this job. The default behavior is that the job only runs when
          triggered by clicking “Run Now” in the Jobs UI or sending an API request to `runNow`.
        :param tags: Dict[str,str] (optional)
          A map of tags associated with the job. These are forwarded to the cluster as cluster tags for jobs
          clusters, and are subject to the same limitations as cluster tags. A maximum of 25 tags can be added
          to the job.
        :param tasks: List[:class:`Task`] (optional)
          A list of task specifications to be executed by this job. It supports up to 1000 elements in write
          endpoints (:method:jobs/create, :method:jobs/reset, :method:jobs/update, :method:jobs/submit). Read
          endpoints return only 100 tasks. If more than 100 tasks are available, you can paginate through them
          using :method:jobs/get. Use the `next_page_token` field at the object root to determine if more
          results are available.
        :param timeout_seconds: int (optional)
          An optional timeout applied to each run of this job. A value of `0` means no timeout.
        :param trigger: :class:`TriggerSettings` (optional)
          A configuration to trigger a run when certain conditions are met. The default behavior is that the
          job runs only when triggered by clicking “Run Now” in the Jobs UI or sending an API request to
          `runNow`.
        :param usage_policy_id: str (optional)
          The id of the user specified usage policy to use for this job. If not specified, a default usage
          policy may be applied when creating or modifying the job. See `effective_usage_policy_id` for the
          usage policy used by this workload.
        :param webhook_notifications: :class:`WebhookNotifications` (optional)
          A collection of system notification IDs to notify when runs of this job begin or complete.

        :returns: :class:`CreateResponse`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        if budget_policy_id is not None:
            body["budget_policy_id"] = budget_policy_id
        if continuous is not None:
            body["continuous"] = continuous.as_dict()
        if deployment is not None:
            body["deployment"] = deployment.as_dict()
        if description is not None:
            body["description"] = description
        if edit_mode is not None:
            body["edit_mode"] = edit_mode.value
        if email_notifications is not None:
            body["email_notifications"] = email_notifications.as_dict()
        if environments is not None:
            body["environments"] = [v.as_dict() for v in environments]
        if format is not None:
            body["format"] = format.value
        if git_source is not None:
            body["git_source"] = git_source.as_dict()
        if health is not None:
            body["health"] = health.as_dict()
        if job_clusters is not None:
            body["job_clusters"] = [v.as_dict() for v in job_clusters]
        if max_concurrent_runs is not None:
            body["max_concurrent_runs"] = max_concurrent_runs
        if name is not None:
            body["name"] = name
        if notification_settings is not None:
            body["notification_settings"] = notification_settings.as_dict()
        if parameters is not None:
            body["parameters"] = [v.as_dict() for v in parameters]
        if performance_target is not None:
            body["performance_target"] = performance_target.value
        if queue is not None:
            body["queue"] = queue.as_dict()
        if run_as is not None:
            body["run_as"] = run_as.as_dict()
        if schedule is not None:
            body["schedule"] = schedule.as_dict()
        if tags is not None:
            body["tags"] = tags
        if tasks is not None:
            body["tasks"] = [v.as_dict() for v in tasks]
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if trigger is not None:
            body["trigger"] = trigger.as_dict()
        if usage_policy_id is not None:
            body["usage_policy_id"] = usage_policy_id
        if webhook_notifications is not None:
            body["webhook_notifications"] = webhook_notifications.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.2/jobs/create", body=body, headers=headers)
        return CreateResponse.from_dict(res)

    def delete(self, job_id: int):
        """Deletes a job.

        :param job_id: int
          The canonical identifier of the job to delete. This field is required.


        """

        body = {}
        if job_id is not None:
            body["job_id"] = job_id
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.2/jobs/delete", body=body, headers=headers)

    def delete_run(self, run_id: int):
        """Deletes a non-active run. Returns an error if the run is active.

        :param run_id: int
          ID of the run to delete.


        """

        body = {}
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.2/jobs/runs/delete", body=body, headers=headers)

    def export_run(self, run_id: int, *, views_to_export: Optional[ViewsToExport] = None) -> ExportRunOutput:
        """Export and retrieve the job run task.

        :param run_id: int
          The canonical identifier for the run. This field is required.
        :param views_to_export: :class:`ViewsToExport` (optional)
          Which views to export (CODE, DASHBOARDS, or ALL). Defaults to CODE.

        :returns: :class:`ExportRunOutput`
        """

        query = {}
        if run_id is not None:
            query["run_id"] = run_id
        if views_to_export is not None:
            query["views_to_export"] = views_to_export.value
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.2/jobs/runs/export", query=query, headers=headers)
        return ExportRunOutput.from_dict(res)

    def get(self, job_id: int, *, page_token: Optional[str] = None) -> Job:
        """Retrieves the details for a single job.

        Large arrays in the results will be paginated when they exceed 100 elements. A request for a single
        job will return all properties for that job, and the first 100 elements of array properties (`tasks`,
        `job_clusters`, `environments` and `parameters`). Use the `next_page_token` field to check for more
        results and pass its value as the `page_token` in subsequent requests. If any array properties have
        more than 100 elements, additional results will be returned on subsequent requests. Arrays without
        additional results will be empty on later pages.

        :param job_id: int
          The canonical identifier of the job to retrieve information about. This field is required.
        :param page_token: str (optional)
          Use `next_page_token` returned from the previous GetJob response to request the next page of the
          job's array properties.

        :returns: :class:`Job`
        """

        query = {}
        if job_id is not None:
            query["job_id"] = job_id
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.2/jobs/get", query=query, headers=headers)
        return Job.from_dict(res)

    def get_permission_levels(self, job_id: str) -> GetJobPermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param job_id: str
          The job for which to get or manage permissions.

        :returns: :class:`GetJobPermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/jobs/{job_id}/permissionLevels", headers=headers)
        return GetJobPermissionLevelsResponse.from_dict(res)

    def get_permissions(self, job_id: str) -> JobPermissions:
        """Gets the permissions of a job. Jobs can inherit permissions from their root object.

        :param job_id: str
          The job for which to get or manage permissions.

        :returns: :class:`JobPermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/jobs/{job_id}", headers=headers)
        return JobPermissions.from_dict(res)

    def get_run(
        self,
        run_id: int,
        *,
        include_history: Optional[bool] = None,
        include_resolved_values: Optional[bool] = None,
        page_token: Optional[str] = None,
    ) -> Run:
        """Retrieves the metadata of a run.

        Large arrays in the results will be paginated when they exceed 100 elements. A request for a single
        run will return all properties for that run, and the first 100 elements of array properties (`tasks`,
        `job_clusters`, `job_parameters` and `repair_history`). Use the next_page_token field to check for
        more results and pass its value as the page_token in subsequent requests. If any array properties have
        more than 100 elements, additional results will be returned on subsequent requests. Arrays without
        additional results will be empty on later pages.

        :param run_id: int
          The canonical identifier of the run for which to retrieve the metadata. This field is required.
        :param include_history: bool (optional)
          Whether to include the repair history in the response.
        :param include_resolved_values: bool (optional)
          Whether to include resolved parameter values in the response.
        :param page_token: str (optional)
          Use `next_page_token` returned from the previous GetRun response to request the next page of the
          run's array properties.

        :returns: :class:`Run`
        """

        query = {}
        if include_history is not None:
            query["include_history"] = include_history
        if include_resolved_values is not None:
            query["include_resolved_values"] = include_resolved_values
        if page_token is not None:
            query["page_token"] = page_token
        if run_id is not None:
            query["run_id"] = run_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.2/jobs/runs/get", query=query, headers=headers)
        return Run.from_dict(res)

    def get_run_output(self, run_id: int) -> RunOutput:
        """Retrieve the output and metadata of a single task run. When a notebook task returns a value through
        the `dbutils.notebook.exit()` call, you can use this endpoint to retrieve that value. Databricks
        restricts this API to returning the first 5 MB of the output. To return a larger result, you can store
        job results in a cloud storage service.

        This endpoint validates that the __run_id__ parameter is valid and returns an HTTP status code 400 if
        the __run_id__ parameter is invalid. Runs are automatically removed after 60 days. If you to want to
        reference them beyond 60 days, you must save old run results before they expire.

        :param run_id: int
          The canonical identifier for the run.

        :returns: :class:`RunOutput`
        """

        query = {}
        if run_id is not None:
            query["run_id"] = run_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.2/jobs/runs/get-output", query=query, headers=headers)
        return RunOutput.from_dict(res)

    def list(
        self,
        *,
        expand_tasks: Optional[bool] = None,
        limit: Optional[int] = None,
        name: Optional[str] = None,
        offset: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[BaseJob]:
        """Retrieves a list of jobs.

        :param expand_tasks: bool (optional)
          Whether to include task and cluster details in the response. Note that only the first 100 elements
          will be shown. Use :method:jobs/get to paginate through all tasks and clusters.
        :param limit: int (optional)
          The number of jobs to return. This value must be greater than 0 and less or equal to 100. The
          default value is 20.
        :param name: str (optional)
          A filter on the list based on the exact (case insensitive) job name.
        :param offset: int (optional)
          The offset of the first job to return, relative to the most recently created job. Deprecated since
          June 2023. Use `page_token` to iterate through the pages instead.
        :param page_token: str (optional)
          Use `next_page_token` or `prev_page_token` returned from the previous request to list the next or
          previous page of jobs respectively.

        :returns: Iterator over :class:`BaseJob`
        """

        query = {}
        if expand_tasks is not None:
            query["expand_tasks"] = expand_tasks
        if limit is not None:
            query["limit"] = limit
        if name is not None:
            query["name"] = name
        if offset is not None:
            query["offset"] = offset
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.2/jobs/list", query=query, headers=headers)
            if "jobs" in json:
                for v in json["jobs"]:
                    yield BaseJob.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_runs(
        self,
        *,
        active_only: Optional[bool] = None,
        completed_only: Optional[bool] = None,
        expand_tasks: Optional[bool] = None,
        job_id: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page_token: Optional[str] = None,
        run_type: Optional[RunType] = None,
        start_time_from: Optional[int] = None,
        start_time_to: Optional[int] = None,
    ) -> Iterator[BaseRun]:
        """List runs in descending order by start time.

        :param active_only: bool (optional)
          If active_only is `true`, only active runs are included in the results; otherwise, lists both active
          and completed runs. An active run is a run in the `QUEUED`, `PENDING`, `RUNNING`, or `TERMINATING`.
          This field cannot be `true` when completed_only is `true`.
        :param completed_only: bool (optional)
          If completed_only is `true`, only completed runs are included in the results; otherwise, lists both
          active and completed runs. This field cannot be `true` when active_only is `true`.
        :param expand_tasks: bool (optional)
          Whether to include task and cluster details in the response. Note that only the first 100 elements
          will be shown. Use :method:jobs/getrun to paginate through all tasks and clusters.
        :param job_id: int (optional)
          The job for which to list runs. If omitted, the Jobs service lists runs from all jobs.
        :param limit: int (optional)
          The number of runs to return. This value must be greater than 0 and less than 25. The default value
          is 20. If a request specifies a limit of 0, the service instead uses the maximum limit.
        :param offset: int (optional)
          The offset of the first run to return, relative to the most recent run. Deprecated since June 2023.
          Use `page_token` to iterate through the pages instead.
        :param page_token: str (optional)
          Use `next_page_token` or `prev_page_token` returned from the previous request to list the next or
          previous page of runs respectively.
        :param run_type: :class:`RunType` (optional)
          The type of runs to return. For a description of run types, see :method:jobs/getRun.
        :param start_time_from: int (optional)
          Show runs that started _at or after_ this value. The value must be a UTC timestamp in milliseconds.
          Can be combined with _start_time_to_ to filter by a time range.
        :param start_time_to: int (optional)
          Show runs that started _at or before_ this value. The value must be a UTC timestamp in milliseconds.
          Can be combined with _start_time_from_ to filter by a time range.

        :returns: Iterator over :class:`BaseRun`
        """

        query = {}
        if active_only is not None:
            query["active_only"] = active_only
        if completed_only is not None:
            query["completed_only"] = completed_only
        if expand_tasks is not None:
            query["expand_tasks"] = expand_tasks
        if job_id is not None:
            query["job_id"] = job_id
        if limit is not None:
            query["limit"] = limit
        if offset is not None:
            query["offset"] = offset
        if page_token is not None:
            query["page_token"] = page_token
        if run_type is not None:
            query["run_type"] = run_type.value
        if start_time_from is not None:
            query["start_time_from"] = start_time_from
        if start_time_to is not None:
            query["start_time_to"] = start_time_to
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.2/jobs/runs/list", query=query, headers=headers)
            if "runs" in json:
                for v in json["runs"]:
                    yield BaseRun.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def repair_run(
        self,
        run_id: int,
        *,
        dbt_commands: Optional[List[str]] = None,
        jar_params: Optional[List[str]] = None,
        job_parameters: Optional[Dict[str, str]] = None,
        latest_repair_id: Optional[int] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        performance_target: Optional[PerformanceTarget] = None,
        pipeline_params: Optional[PipelineParams] = None,
        python_named_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        rerun_all_failed_tasks: Optional[bool] = None,
        rerun_dependent_tasks: Optional[bool] = None,
        rerun_tasks: Optional[List[str]] = None,
        spark_submit_params: Optional[List[str]] = None,
        sql_params: Optional[Dict[str, str]] = None,
    ) -> Wait[Run]:
        """Re-run one or more tasks. Tasks are re-run as part of the original job run. They use the current job
        and task settings, and can be viewed in the history for the original job run.

        :param run_id: int
          The job run ID of the run to repair. The run must not be in progress.
        :param dbt_commands: List[str] (optional)
          An array of commands to execute for jobs with the dbt task, for example `"dbt_commands": ["dbt
          deps", "dbt seed", "dbt deps", "dbt seed", "dbt run"]`

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param jar_params: List[str] (optional)
          A list of parameters for jobs with Spark JAR tasks, for example `"jar_params": ["john doe", "35"]`.
          The parameters are used to invoke the main function of the main class specified in the Spark JAR
          task. If not specified upon `run-now`, it defaults to an empty list. jar_params cannot be specified
          in conjunction with notebook_params. The JSON representation of this field (for example
          `{"jar_params":["john doe","35"]}`) cannot exceed 10,000 bytes.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param job_parameters: Dict[str,str] (optional)
          Job-level parameters used in the run. for example `"param": "overriding_val"`
        :param latest_repair_id: int (optional)
          The ID of the latest repair. This parameter is not required when repairing a run for the first time,
          but must be provided on subsequent requests to repair the same run.
        :param notebook_params: Dict[str,str] (optional)
          A map from keys to values for jobs with notebook task, for example `"notebook_params": {"name":
          "john doe", "age": "35"}`. The map is passed to the notebook and is accessible through the
          [dbutils.widgets.get] function.

          If not specified upon `run-now`, the triggered run uses the job’s base parameters.

          notebook_params cannot be specified in conjunction with jar_params.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          The JSON representation of this field (for example `{"notebook_params":{"name":"john
          doe","age":"35"}}`) cannot exceed 10,000 bytes.

          [dbutils.widgets.get]: https://docs.databricks.com/dev-tools/databricks-utils.html
          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param performance_target: :class:`PerformanceTarget` (optional)
          The performance mode on a serverless job. The performance target determines the level of compute
          performance or cost-efficiency for the run. This field overrides the performance target defined on
          the job level.

          * `STANDARD`: Enables cost-efficient execution of serverless workloads. * `PERFORMANCE_OPTIMIZED`:
          Prioritizes fast startup and execution times through rapid scaling and optimized cluster
          performance.
        :param pipeline_params: :class:`PipelineParams` (optional)
          Controls whether the pipeline should perform a full refresh
        :param python_named_params: Dict[str,str] (optional)
        :param python_params: List[str] (optional)
          A list of parameters for jobs with Python tasks, for example `"python_params": ["john doe", "35"]`.
          The parameters are passed to Python file as command-line parameters. If specified upon `run-now`, it
          would overwrite the parameters specified in job setting. The JSON representation of this field (for
          example `{"python_params":["john doe","35"]}`) cannot exceed 10,000 bytes.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          Important

          These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
          returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
          emojis.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param rerun_all_failed_tasks: bool (optional)
          If true, repair all failed tasks. Only one of `rerun_tasks` or `rerun_all_failed_tasks` can be used.
        :param rerun_dependent_tasks: bool (optional)
          If true, repair all tasks that depend on the tasks in `rerun_tasks`, even if they were previously
          successful. Can be also used in combination with `rerun_all_failed_tasks`.
        :param rerun_tasks: List[str] (optional)
          The task keys of the task runs to repair.
        :param spark_submit_params: List[str] (optional)
          A list of parameters for jobs with spark submit task, for example `"spark_submit_params":
          ["--class", "org.apache.spark.examples.SparkPi"]`. The parameters are passed to spark-submit script
          as command-line parameters. If specified upon `run-now`, it would overwrite the parameters specified
          in job setting. The JSON representation of this field (for example `{"python_params":["john
          doe","35"]}`) cannot exceed 10,000 bytes.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          Important

          These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
          returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
          emojis.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param sql_params: Dict[str,str] (optional)
          A map from keys to values for jobs with SQL task, for example `"sql_params": {"name": "john doe",
          "age": "35"}`. The SQL alert task does not support custom parameters.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown

        :returns:
          Long-running operation waiter for :class:`Run`.
          See :method:wait_get_run_job_terminated_or_skipped for more details.
        """

        body = {}
        if dbt_commands is not None:
            body["dbt_commands"] = [v for v in dbt_commands]
        if jar_params is not None:
            body["jar_params"] = [v for v in jar_params]
        if job_parameters is not None:
            body["job_parameters"] = job_parameters
        if latest_repair_id is not None:
            body["latest_repair_id"] = latest_repair_id
        if notebook_params is not None:
            body["notebook_params"] = notebook_params
        if performance_target is not None:
            body["performance_target"] = performance_target.value
        if pipeline_params is not None:
            body["pipeline_params"] = pipeline_params.as_dict()
        if python_named_params is not None:
            body["python_named_params"] = python_named_params
        if python_params is not None:
            body["python_params"] = [v for v in python_params]
        if rerun_all_failed_tasks is not None:
            body["rerun_all_failed_tasks"] = rerun_all_failed_tasks
        if rerun_dependent_tasks is not None:
            body["rerun_dependent_tasks"] = rerun_dependent_tasks
        if rerun_tasks is not None:
            body["rerun_tasks"] = [v for v in rerun_tasks]
        if run_id is not None:
            body["run_id"] = run_id
        if spark_submit_params is not None:
            body["spark_submit_params"] = [v for v in spark_submit_params]
        if sql_params is not None:
            body["sql_params"] = sql_params
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.2/jobs/runs/repair", body=body, headers=headers)
        return Wait(
            self.wait_get_run_job_terminated_or_skipped,
            response=RepairRunResponse.from_dict(op_response),
            run_id=run_id,
        )

    def repair_run_and_wait(
        self,
        run_id: int,
        *,
        dbt_commands: Optional[List[str]] = None,
        jar_params: Optional[List[str]] = None,
        job_parameters: Optional[Dict[str, str]] = None,
        latest_repair_id: Optional[int] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        performance_target: Optional[PerformanceTarget] = None,
        pipeline_params: Optional[PipelineParams] = None,
        python_named_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        rerun_all_failed_tasks: Optional[bool] = None,
        rerun_dependent_tasks: Optional[bool] = None,
        rerun_tasks: Optional[List[str]] = None,
        spark_submit_params: Optional[List[str]] = None,
        sql_params: Optional[Dict[str, str]] = None,
        timeout=timedelta(minutes=20),
    ) -> Run:
        return self.repair_run(
            dbt_commands=dbt_commands,
            jar_params=jar_params,
            job_parameters=job_parameters,
            latest_repair_id=latest_repair_id,
            notebook_params=notebook_params,
            performance_target=performance_target,
            pipeline_params=pipeline_params,
            python_named_params=python_named_params,
            python_params=python_params,
            rerun_all_failed_tasks=rerun_all_failed_tasks,
            rerun_dependent_tasks=rerun_dependent_tasks,
            rerun_tasks=rerun_tasks,
            run_id=run_id,
            spark_submit_params=spark_submit_params,
            sql_params=sql_params,
        ).result(timeout=timeout)

    def reset(self, job_id: int, new_settings: JobSettings):
        """Overwrite all settings for the given job. Use the [_Update_ endpoint](:method:jobs/update) to update
        job settings partially.

        :param job_id: int
          The canonical identifier of the job to reset. This field is required.
        :param new_settings: :class:`JobSettings`
          The new settings of the job. These settings completely replace the old settings.

          Changes to the field `JobBaseSettings.timeout_seconds` are applied to active runs. Changes to other
          fields are applied to future runs only.


        """

        body = {}
        if job_id is not None:
            body["job_id"] = job_id
        if new_settings is not None:
            body["new_settings"] = new_settings.as_dict()
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.2/jobs/reset", body=body, headers=headers)

    def run_now(
        self,
        job_id: int,
        *,
        dbt_commands: Optional[List[str]] = None,
        idempotency_token: Optional[str] = None,
        jar_params: Optional[List[str]] = None,
        job_parameters: Optional[Dict[str, str]] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        only: Optional[List[str]] = None,
        performance_target: Optional[PerformanceTarget] = None,
        pipeline_params: Optional[PipelineParams] = None,
        python_named_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        queue: Optional[QueueSettings] = None,
        spark_submit_params: Optional[List[str]] = None,
        sql_params: Optional[Dict[str, str]] = None,
    ) -> Wait[Run]:
        """Run a job and return the `run_id` of the triggered run.

        :param job_id: int
          The ID of the job to be executed
        :param dbt_commands: List[str] (optional)
          An array of commands to execute for jobs with the dbt task, for example `"dbt_commands": ["dbt
          deps", "dbt seed", "dbt deps", "dbt seed", "dbt run"]`

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param idempotency_token: str (optional)
          An optional token to guarantee the idempotency of job run requests. If a run with the provided token
          already exists, the request does not create a new run but returns the ID of the existing run
          instead. If a run with the provided token is deleted, an error is returned.

          If you specify the idempotency token, upon failure you can retry until the request succeeds.
          Databricks guarantees that exactly one run is launched with that idempotency token.

          This token must have at most 64 characters.

          For more information, see [How to ensure idempotency for jobs].

          [How to ensure idempotency for jobs]: https://kb.databricks.com/jobs/jobs-idempotency.html
        :param jar_params: List[str] (optional)
          A list of parameters for jobs with Spark JAR tasks, for example `"jar_params": ["john doe", "35"]`.
          The parameters are used to invoke the main function of the main class specified in the Spark JAR
          task. If not specified upon `run-now`, it defaults to an empty list. jar_params cannot be specified
          in conjunction with notebook_params. The JSON representation of this field (for example
          `{"jar_params":["john doe","35"]}`) cannot exceed 10,000 bytes.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param job_parameters: Dict[str,str] (optional)
          Job-level parameters used in the run. for example `"param": "overriding_val"`
        :param notebook_params: Dict[str,str] (optional)
          A map from keys to values for jobs with notebook task, for example `"notebook_params": {"name":
          "john doe", "age": "35"}`. The map is passed to the notebook and is accessible through the
          [dbutils.widgets.get] function.

          If not specified upon `run-now`, the triggered run uses the job’s base parameters.

          notebook_params cannot be specified in conjunction with jar_params.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          The JSON representation of this field (for example `{"notebook_params":{"name":"john
          doe","age":"35"}}`) cannot exceed 10,000 bytes.

          [dbutils.widgets.get]: https://docs.databricks.com/dev-tools/databricks-utils.html
          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param only: List[str] (optional)
          A list of task keys to run inside of the job. If this field is not provided, all tasks in the job
          will be run.
        :param performance_target: :class:`PerformanceTarget` (optional)
          The performance mode on a serverless job. The performance target determines the level of compute
          performance or cost-efficiency for the run. This field overrides the performance target defined on
          the job level.

          * `STANDARD`: Enables cost-efficient execution of serverless workloads. * `PERFORMANCE_OPTIMIZED`:
          Prioritizes fast startup and execution times through rapid scaling and optimized cluster
          performance.
        :param pipeline_params: :class:`PipelineParams` (optional)
          Controls whether the pipeline should perform a full refresh
        :param python_named_params: Dict[str,str] (optional)
        :param python_params: List[str] (optional)
          A list of parameters for jobs with Python tasks, for example `"python_params": ["john doe", "35"]`.
          The parameters are passed to Python file as command-line parameters. If specified upon `run-now`, it
          would overwrite the parameters specified in job setting. The JSON representation of this field (for
          example `{"python_params":["john doe","35"]}`) cannot exceed 10,000 bytes.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          Important

          These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
          returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
          emojis.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param queue: :class:`QueueSettings` (optional)
          The queue settings of the run.
        :param spark_submit_params: List[str] (optional)
          A list of parameters for jobs with spark submit task, for example `"spark_submit_params":
          ["--class", "org.apache.spark.examples.SparkPi"]`. The parameters are passed to spark-submit script
          as command-line parameters. If specified upon `run-now`, it would overwrite the parameters specified
          in job setting. The JSON representation of this field (for example `{"python_params":["john
          doe","35"]}`) cannot exceed 10,000 bytes.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          Important

          These parameters accept only Latin characters (ASCII character set). Using non-ASCII characters
          returns an error. Examples of invalid, non-ASCII characters are Chinese, Japanese kanjis, and
          emojis.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown
        :param sql_params: Dict[str,str] (optional)
          A map from keys to values for jobs with SQL task, for example `"sql_params": {"name": "john doe",
          "age": "35"}`. The SQL alert task does not support custom parameters.

          ⚠ **Deprecation note** Use [job parameters] to pass information down to tasks.

          [job parameters]: https://docs.databricks.com/jobs/job-parameters.html#job-parameter-pushdown

        :returns:
          Long-running operation waiter for :class:`Run`.
          See :method:wait_get_run_job_terminated_or_skipped for more details.
        """

        body = {}
        if dbt_commands is not None:
            body["dbt_commands"] = [v for v in dbt_commands]
        if idempotency_token is not None:
            body["idempotency_token"] = idempotency_token
        if jar_params is not None:
            body["jar_params"] = [v for v in jar_params]
        if job_id is not None:
            body["job_id"] = job_id
        if job_parameters is not None:
            body["job_parameters"] = job_parameters
        if notebook_params is not None:
            body["notebook_params"] = notebook_params
        if only is not None:
            body["only"] = [v for v in only]
        if performance_target is not None:
            body["performance_target"] = performance_target.value
        if pipeline_params is not None:
            body["pipeline_params"] = pipeline_params.as_dict()
        if python_named_params is not None:
            body["python_named_params"] = python_named_params
        if python_params is not None:
            body["python_params"] = [v for v in python_params]
        if queue is not None:
            body["queue"] = queue.as_dict()
        if spark_submit_params is not None:
            body["spark_submit_params"] = [v for v in spark_submit_params]
        if sql_params is not None:
            body["sql_params"] = sql_params
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.2/jobs/run-now", body=body, headers=headers)
        return Wait(
            self.wait_get_run_job_terminated_or_skipped,
            response=RunNowResponse.from_dict(op_response),
            run_id=op_response["run_id"],
        )

    def run_now_and_wait(
        self,
        job_id: int,
        *,
        dbt_commands: Optional[List[str]] = None,
        idempotency_token: Optional[str] = None,
        jar_params: Optional[List[str]] = None,
        job_parameters: Optional[Dict[str, str]] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        only: Optional[List[str]] = None,
        performance_target: Optional[PerformanceTarget] = None,
        pipeline_params: Optional[PipelineParams] = None,
        python_named_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        queue: Optional[QueueSettings] = None,
        spark_submit_params: Optional[List[str]] = None,
        sql_params: Optional[Dict[str, str]] = None,
        timeout=timedelta(minutes=20),
    ) -> Run:
        return self.run_now(
            dbt_commands=dbt_commands,
            idempotency_token=idempotency_token,
            jar_params=jar_params,
            job_id=job_id,
            job_parameters=job_parameters,
            notebook_params=notebook_params,
            only=only,
            performance_target=performance_target,
            pipeline_params=pipeline_params,
            python_named_params=python_named_params,
            python_params=python_params,
            queue=queue,
            spark_submit_params=spark_submit_params,
            sql_params=sql_params,
        ).result(timeout=timeout)

    def set_permissions(
        self, job_id: str, *, access_control_list: Optional[List[JobAccessControlRequest]] = None
    ) -> JobPermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param job_id: str
          The job for which to get or manage permissions.
        :param access_control_list: List[:class:`JobAccessControlRequest`] (optional)

        :returns: :class:`JobPermissions`
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

        res = self._api.do("PUT", f"/api/2.0/permissions/jobs/{job_id}", body=body, headers=headers)
        return JobPermissions.from_dict(res)

    def submit(
        self,
        *,
        access_control_list: Optional[List[JobAccessControlRequest]] = None,
        budget_policy_id: Optional[str] = None,
        email_notifications: Optional[JobEmailNotifications] = None,
        environments: Optional[List[JobEnvironment]] = None,
        git_source: Optional[GitSource] = None,
        health: Optional[JobsHealthRules] = None,
        idempotency_token: Optional[str] = None,
        notification_settings: Optional[JobNotificationSettings] = None,
        queue: Optional[QueueSettings] = None,
        run_as: Optional[JobRunAs] = None,
        run_name: Optional[str] = None,
        tasks: Optional[List[SubmitTask]] = None,
        timeout_seconds: Optional[int] = None,
        usage_policy_id: Optional[str] = None,
        webhook_notifications: Optional[WebhookNotifications] = None,
    ) -> Wait[Run]:
        """Submit a one-time run. This endpoint allows you to submit a workload directly without creating a job.
        Runs submitted using this endpoint don’t display in the UI. Use the `jobs/runs/get` API to check the
        run state after the job is submitted.

        **Important:** Jobs submitted using this endpoint are not saved as a job. They do not show up in the
        Jobs UI, and do not retry when they fail. Because they are not saved, Databricks cannot auto-optimize
        serverless compute in case of failure. If your job fails, you may want to use classic compute to
        specify the compute needs for the job. Alternatively, use the `POST /jobs/create` and `POST
        /jobs/run-now` endpoints to create and run a saved job.

        :param access_control_list: List[:class:`JobAccessControlRequest`] (optional)
          List of permissions to set on the job.
        :param budget_policy_id: str (optional)
          The user specified id of the budget policy to use for this one-time run. If not specified, the run
          will be not be attributed to any budget policy.
        :param email_notifications: :class:`JobEmailNotifications` (optional)
          An optional set of email addresses notified when the run begins or completes.
        :param environments: List[:class:`JobEnvironment`] (optional)
          A list of task execution environment specifications that can be referenced by tasks of this run.
        :param git_source: :class:`GitSource` (optional)
          An optional specification for a remote Git repository containing the source code used by tasks.
          Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.

          If `git_source` is set, these tasks retrieve the file from the remote repository by default.
          However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task.

          Note: dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks are
          used, `git_source` must be defined on the job.
        :param health: :class:`JobsHealthRules` (optional)
        :param idempotency_token: str (optional)
          An optional token that can be used to guarantee the idempotency of job run requests. If a run with
          the provided token already exists, the request does not create a new run but returns the ID of the
          existing run instead. If a run with the provided token is deleted, an error is returned.

          If you specify the idempotency token, upon failure you can retry until the request succeeds.
          Databricks guarantees that exactly one run is launched with that idempotency token.

          This token must have at most 64 characters.

          For more information, see [How to ensure idempotency for jobs].

          [How to ensure idempotency for jobs]: https://kb.databricks.com/jobs/jobs-idempotency.html
        :param notification_settings: :class:`JobNotificationSettings` (optional)
          Optional notification settings that are used when sending notifications to each of the
          `email_notifications` and `webhook_notifications` for this run.
        :param queue: :class:`QueueSettings` (optional)
          The queue settings of the one-time run.
        :param run_as: :class:`JobRunAs` (optional)
          Specifies the user or service principal that the job runs as. If not specified, the job runs as the
          user who submits the request.
        :param run_name: str (optional)
          An optional name for the run. The default value is `Untitled`.
        :param tasks: List[:class:`SubmitTask`] (optional)
        :param timeout_seconds: int (optional)
          An optional timeout applied to each run of this job. A value of `0` means no timeout.
        :param usage_policy_id: str (optional)
          The user specified id of the usage policy to use for this one-time run. If not specified, a default
          usage policy may be applied when creating or modifying the job.
        :param webhook_notifications: :class:`WebhookNotifications` (optional)
          A collection of system notification IDs to notify when the run begins or completes.

        :returns:
          Long-running operation waiter for :class:`Run`.
          See :method:wait_get_run_job_terminated_or_skipped for more details.
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        if budget_policy_id is not None:
            body["budget_policy_id"] = budget_policy_id
        if email_notifications is not None:
            body["email_notifications"] = email_notifications.as_dict()
        if environments is not None:
            body["environments"] = [v.as_dict() for v in environments]
        if git_source is not None:
            body["git_source"] = git_source.as_dict()
        if health is not None:
            body["health"] = health.as_dict()
        if idempotency_token is not None:
            body["idempotency_token"] = idempotency_token
        if notification_settings is not None:
            body["notification_settings"] = notification_settings.as_dict()
        if queue is not None:
            body["queue"] = queue.as_dict()
        if run_as is not None:
            body["run_as"] = run_as.as_dict()
        if run_name is not None:
            body["run_name"] = run_name
        if tasks is not None:
            body["tasks"] = [v.as_dict() for v in tasks]
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if usage_policy_id is not None:
            body["usage_policy_id"] = usage_policy_id
        if webhook_notifications is not None:
            body["webhook_notifications"] = webhook_notifications.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.2/jobs/runs/submit", body=body, headers=headers)
        return Wait(
            self.wait_get_run_job_terminated_or_skipped,
            response=SubmitRunResponse.from_dict(op_response),
            run_id=op_response["run_id"],
        )

    def submit_and_wait(
        self,
        *,
        access_control_list: Optional[List[JobAccessControlRequest]] = None,
        budget_policy_id: Optional[str] = None,
        email_notifications: Optional[JobEmailNotifications] = None,
        environments: Optional[List[JobEnvironment]] = None,
        git_source: Optional[GitSource] = None,
        health: Optional[JobsHealthRules] = None,
        idempotency_token: Optional[str] = None,
        notification_settings: Optional[JobNotificationSettings] = None,
        queue: Optional[QueueSettings] = None,
        run_as: Optional[JobRunAs] = None,
        run_name: Optional[str] = None,
        tasks: Optional[List[SubmitTask]] = None,
        timeout_seconds: Optional[int] = None,
        usage_policy_id: Optional[str] = None,
        webhook_notifications: Optional[WebhookNotifications] = None,
        timeout=timedelta(minutes=20),
    ) -> Run:
        return self.submit(
            access_control_list=access_control_list,
            budget_policy_id=budget_policy_id,
            email_notifications=email_notifications,
            environments=environments,
            git_source=git_source,
            health=health,
            idempotency_token=idempotency_token,
            notification_settings=notification_settings,
            queue=queue,
            run_as=run_as,
            run_name=run_name,
            tasks=tasks,
            timeout_seconds=timeout_seconds,
            usage_policy_id=usage_policy_id,
            webhook_notifications=webhook_notifications,
        ).result(timeout=timeout)

    def update(
        self, job_id: int, *, fields_to_remove: Optional[List[str]] = None, new_settings: Optional[JobSettings] = None
    ):
        """Add, update, or remove specific settings of an existing job. Use the [_Reset_
        endpoint](:method:jobs/reset) to overwrite all job settings.

        :param job_id: int
          The canonical identifier of the job to update. This field is required.
        :param fields_to_remove: List[str] (optional)
          Remove top-level fields in the job settings. Removing nested fields is not supported, except for
          tasks and job clusters (`tasks/task_1`). This field is optional.
        :param new_settings: :class:`JobSettings` (optional)
          The new settings for the job.

          Top-level fields specified in `new_settings` are completely replaced, except for arrays which are
          merged. That is, new and existing entries are completely replaced based on the respective key
          fields, i.e. `task_key` or `job_cluster_key`, while previous entries are kept.

          Partially updating nested fields is not supported.

          Changes to the field `JobSettings.timeout_seconds` are applied to active runs. Changes to other
          fields are applied to future runs only.


        """

        body = {}
        if fields_to_remove is not None:
            body["fields_to_remove"] = [v for v in fields_to_remove]
        if job_id is not None:
            body["job_id"] = job_id
        if new_settings is not None:
            body["new_settings"] = new_settings.as_dict()
        headers = {
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.2/jobs/update", body=body, headers=headers)

    def update_permissions(
        self, job_id: str, *, access_control_list: Optional[List[JobAccessControlRequest]] = None
    ) -> JobPermissions:
        """Updates the permissions on a job. Jobs can inherit permissions from their root object.

        :param job_id: str
          The job for which to get or manage permissions.
        :param access_control_list: List[:class:`JobAccessControlRequest`] (optional)

        :returns: :class:`JobPermissions`
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

        res = self._api.do("PATCH", f"/api/2.0/permissions/jobs/{job_id}", body=body, headers=headers)
        return JobPermissions.from_dict(res)


class PolicyComplianceForJobsAPI:
    """The compliance APIs allow you to view and manage the policy compliance status of jobs in your workspace.
    This API currently only supports compliance controls for cluster policies.

    A job is in compliance if its cluster configurations satisfy the rules of all their respective cluster
    policies. A job could be out of compliance if a cluster policy it uses was updated after the job was last
    edited. The job is considered out of compliance if any of its clusters no longer comply with their updated
    policies.

    The get and list compliance APIs allow you to view the policy compliance status of a job. The enforce
    compliance API allows you to update a job so that it becomes compliant with all of its policies."""

    def __init__(self, api_client):
        self._api = api_client

    def enforce_compliance(
        self, job_id: int, *, validate_only: Optional[bool] = None
    ) -> EnforcePolicyComplianceResponse:
        """Updates a job so the job clusters that are created when running the job (specified in `new_cluster`)
        are compliant with the current versions of their respective cluster policies. All-purpose clusters
        used in the job will not be updated.

        :param job_id: int
          The ID of the job you want to enforce policy compliance on.
        :param validate_only: bool (optional)
          If set, previews changes made to the job to comply with its policy, but does not update the job.

        :returns: :class:`EnforcePolicyComplianceResponse`
        """

        body = {}
        if job_id is not None:
            body["job_id"] = job_id
        if validate_only is not None:
            body["validate_only"] = validate_only
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/policies/jobs/enforce-compliance", body=body, headers=headers)
        return EnforcePolicyComplianceResponse.from_dict(res)

    def get_compliance(self, job_id: int) -> GetPolicyComplianceResponse:
        """Returns the policy compliance status of a job. Jobs could be out of compliance if a cluster policy
        they use was updated after the job was last edited and some of its job clusters no longer comply with
        their updated policies.

        :param job_id: int
          The ID of the job whose compliance status you are requesting.

        :returns: :class:`GetPolicyComplianceResponse`
        """

        query = {}
        if job_id is not None:
            query["job_id"] = job_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/policies/jobs/get-compliance", query=query, headers=headers)
        return GetPolicyComplianceResponse.from_dict(res)

    def list_compliance(
        self, policy_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[JobCompliance]:
        """Returns the policy compliance status of all jobs that use a given policy. Jobs could be out of
        compliance if a cluster policy they use was updated after the job was last edited and its job clusters
        no longer comply with the updated policy.

        :param policy_id: str
          Canonical unique identifier for the cluster policy.
        :param page_size: int (optional)
          Use this field to specify the maximum number of results to be returned by the server. The server may
          further constrain the maximum number of results returned in a single page.
        :param page_token: str (optional)
          A page token that can be used to navigate to the next page or previous page as returned by
          `next_page_token` or `prev_page_token`.

        :returns: Iterator over :class:`JobCompliance`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        if policy_id is not None:
            query["policy_id"] = policy_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/policies/jobs/list-compliance", query=query, headers=headers)
            if "jobs" in json:
                for v in json["jobs"]:
                    yield JobCompliance.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]
