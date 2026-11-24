"""Client support for accessing Temporal."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import (
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from enum import Enum

import temporalio.api.enums.v1
import temporalio.api.workflowservice.v1


@dataclass(frozen=True)
class WorkerBuildIdVersionSets:
    """Represents the sets of compatible Build ID versions associated with some Task Queue, as
    fetched by :py:meth:`Client.get_worker_build_id_compatibility`.
    """

    version_sets: Sequence[BuildIdVersionSet]
    """All version sets that were fetched for this task queue."""

    def default_set(self) -> BuildIdVersionSet:
        """Returns the default version set for this task queue."""
        return self.version_sets[-1]

    def default_build_id(self) -> str:
        """Returns the default Build ID for this task queue."""
        return self.default_set().default()

    @staticmethod
    def _from_proto(
        resp: temporalio.api.workflowservice.v1.GetWorkerBuildIdCompatibilityResponse,
    ) -> WorkerBuildIdVersionSets:
        return WorkerBuildIdVersionSets(
            version_sets=[
                BuildIdVersionSet(mvs.build_ids) for mvs in resp.major_version_sets
            ]
        )


@dataclass(frozen=True)
class BuildIdVersionSet:
    """A set of Build IDs which are compatible with each other."""

    build_ids: Sequence[str]
    """All Build IDs contained in the set."""

    def default(self) -> str:
        """Returns the default Build ID for this set."""
        return self.build_ids[-1]


class BuildIdOp(ABC):
    """Base class for Build ID operations as used by
    :py:meth:`Client.update_worker_build_id_compatibility`.
    """

    @abstractmethod
    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        """Returns a partial request with the operation populated. Caller must populate
        non-operation fields. This is done b/c there's no good way to assign a non-primitive message
        as the operation after initializing the request.
        """
        ...


@dataclass(frozen=True)
class BuildIdOpAddNewDefault(BuildIdOp):
    """Adds a new Build Id into a new set, which will be used as the default set for
    the queue. This means all new workflows will start on this Build Id.
    """

    build_id: str

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return (
            temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
                add_new_build_id_in_new_default_set=self.build_id
            )
        )


@dataclass(frozen=True)
class BuildIdOpAddNewCompatible(BuildIdOp):
    """Adds a new Build Id into an existing compatible set. The newly added ID becomes
    the default for that compatible set, and thus new workflow tasks for workflows which have been
    executing on workers in that set will now start on this new Build Id.
    """

    build_id: str
    """The Build Id to add to the compatible set."""

    existing_compatible_build_id: str
    """A Build Id which must already be defined on the task queue, and is used to find the
    compatible set to add the new id to.
    """

    promote_set: bool = False
    """If set to true, the targeted set will also be promoted to become the overall default set for
    the queue."""

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
            add_new_compatible_build_id=temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest.AddNewCompatibleVersion(
                new_build_id=self.build_id,
                existing_compatible_build_id=self.existing_compatible_build_id,
                make_set_default=self.promote_set,
            )
        )


@dataclass(frozen=True)
class BuildIdOpPromoteSetByBuildId(BuildIdOp):
    """Promotes a set of compatible Build Ids to become the current default set for the task queue.
    Any Build Id in the set may be used to target it.
    """

    build_id: str
    """A Build Id which must already be defined on the task queue, and is used to find the
    compatible set to promote."""

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return (
            temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
                promote_set_by_build_id=self.build_id
            )
        )


@dataclass(frozen=True)
class BuildIdOpPromoteBuildIdWithinSet(BuildIdOp):
    """Promotes a Build Id within an existing set to become the default ID for that set."""

    build_id: str

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return (
            temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
                promote_build_id_within_set=self.build_id
            )
        )


@dataclass(frozen=True)
class BuildIdOpMergeSets(BuildIdOp):
    """Merges two sets into one set, thus declaring all the Build Ids in both as compatible with one
    another. The default of the primary set is maintained as the merged set's overall default.
    """

    primary_build_id: str
    """A Build Id which and is used to find the primary set to be merged."""

    secondary_build_id: str
    """A Build Id which and is used to find the secondary set to be merged."""

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
            merge_sets=temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest.MergeSets(
                primary_set_build_id=self.primary_build_id,
                secondary_set_build_id=self.secondary_build_id,
            )
        )


@dataclass(frozen=True)
class WorkerTaskReachability:
    """Contains information about the reachability of some Build IDs"""

    build_id_reachability: Mapping[str, BuildIdReachability]
    """Maps Build IDs to information about their reachability"""

    @staticmethod
    def _from_proto(
        resp: temporalio.api.workflowservice.v1.GetWorkerTaskReachabilityResponse,
    ) -> WorkerTaskReachability:
        mapping = dict()
        for bid_reach in resp.build_id_reachability:
            tq_mapping = dict()
            unretrieved = set()
            for tq_reach in bid_reach.task_queue_reachability:
                if tq_reach.reachability == [
                    temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_UNSPECIFIED
                ]:
                    unretrieved.add(tq_reach.task_queue)
                    continue
                tq_mapping[tq_reach.task_queue] = [
                    TaskReachabilityType._from_proto(r) for r in tq_reach.reachability
                ]

            mapping[bid_reach.build_id] = BuildIdReachability(
                task_queue_reachability=tq_mapping,
                unretrieved_task_queues=frozenset(unretrieved),
            )

        return WorkerTaskReachability(build_id_reachability=mapping)


@dataclass(frozen=True)
class BuildIdReachability:
    """Contains information about the reachability of a specific Build ID"""

    task_queue_reachability: Mapping[str, Sequence[TaskReachabilityType]]
    """Maps Task Queue names to the reachability status of the Build ID on that queue. If the value
    is an empty list, the Build ID is not reachable on that queue.
    """

    unretrieved_task_queues: frozenset[str]
    """If any Task Queues could not be retrieved because the server limits the number that can be
    queried at once, they will be listed here.
    """


class TaskReachabilityType(Enum):
    """Enumerates how a task might reach certain kinds of workflows"""

    NEW_WORKFLOWS = 1
    EXISTING_WORKFLOWS = 2
    OPEN_WORKFLOWS = 3
    CLOSED_WORKFLOWS = 4

    @staticmethod
    def _from_proto(
        reachability: temporalio.api.enums.v1.TaskReachability.ValueType,
    ) -> TaskReachabilityType:
        if (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_NEW_WORKFLOWS
        ):
            return TaskReachabilityType.NEW_WORKFLOWS
        elif (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_EXISTING_WORKFLOWS
        ):
            return TaskReachabilityType.EXISTING_WORKFLOWS
        elif (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_OPEN_WORKFLOWS
        ):
            return TaskReachabilityType.OPEN_WORKFLOWS
        elif (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_CLOSED_WORKFLOWS
        ):
            return TaskReachabilityType.CLOSED_WORKFLOWS
        else:
            raise ValueError(f"Cannot convert reachability type: {reachability}")

    def _to_proto(self) -> temporalio.api.enums.v1.TaskReachability.ValueType:
        if self == TaskReachabilityType.NEW_WORKFLOWS:
            return (
                temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_NEW_WORKFLOWS
            )
        elif self == TaskReachabilityType.EXISTING_WORKFLOWS:
            return temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_EXISTING_WORKFLOWS
        elif self == TaskReachabilityType.OPEN_WORKFLOWS:
            return temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_OPEN_WORKFLOWS
        elif self == TaskReachabilityType.CLOSED_WORKFLOWS:
            return temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_CLOSED_WORKFLOWS
        else:
            return (
                temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_UNSPECIFIED
            )
