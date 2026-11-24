"""Worker using SDK Core. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import (
    TypeAlias,
)

import temporalio.bridge.client
import temporalio.bridge.proto
import temporalio.bridge.proto.activity_task
import temporalio.bridge.proto.nexus
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge
import temporalio.converter
import temporalio.converter._extstore
from temporalio.api.common.v1.message_pb2 import Payload
from temporalio.bridge._visitor_functions import PayloadSequence, VisitorFunctions
from temporalio.bridge.temporal_sdk_bridge import (
    CustomSlotSupplier as BridgeCustomSlotSupplier,
)
from temporalio.bridge.temporal_sdk_bridge import (
    PollShutdownError,  # type: ignore # noqa: F401
)
from temporalio.worker._command_aware_visitor import CommandAwarePayloadVisitor


@dataclass
class WorkerConfig:
    """Python representation of the Rust struct for configuring a worker."""

    namespace: str
    task_queue: str
    versioning_strategy: WorkerVersioningStrategy
    identity_override: str | None
    max_cached_workflows: int
    tuner: TunerHolder
    workflow_task_poller_behavior: PollerBehavior
    nonsticky_to_sticky_poll_ratio: float
    activity_task_poller_behavior: PollerBehavior
    no_remote_activities: bool
    task_types: WorkerTaskTypes
    sticky_queue_schedule_to_start_timeout_millis: int
    max_heartbeat_throttle_interval_millis: int
    default_heartbeat_throttle_interval_millis: int
    max_activities_per_second: float | None
    max_task_queue_activities_per_second: float | None
    max_eager_activity_reservations_per_workflow_task: int
    graceful_shutdown_period_millis: int
    nondeterminism_as_workflow_fail: bool
    nondeterminism_as_workflow_fail_for_types: set[str]
    nexus_task_poller_behavior: PollerBehavior
    plugins: Sequence[str]
    storage_drivers: set[str]
    disable_payload_error_limit: bool


@dataclass
class PollerBehaviorSimpleMaximum:
    """Python representation of the Rust struct for simple poller behavior."""

    simple_maximum: int


@dataclass
class PollerBehaviorAutoscaling:
    """Python representation of the Rust struct for autoscaling poller behavior."""

    minimum: int
    maximum: int
    initial: int


PollerBehavior: TypeAlias = PollerBehaviorSimpleMaximum | PollerBehaviorAutoscaling


@dataclass
class WorkerDeploymentVersion:
    """Python representation of the Rust struct for configuring a worker deployment version."""

    deployment_name: str
    build_id: str


@dataclass
class WorkerDeploymentOptions:
    """Python representation of the Rust struct for configuring a worker deployment options."""

    version: WorkerDeploymentVersion
    use_worker_versioning: bool
    default_versioning_behavior: int
    """An enums.v1.VersioningBehavior as an int"""


@dataclass
class WorkerVersioningStrategyNone:
    """Python representation of the Rust struct for configuring a worker versioning strategy None."""

    build_id_no_versioning: str


@dataclass
class WorkerVersioningStrategyLegacyBuildIdBased:
    """Python representation of the Rust struct for configuring a worker versioning strategy legacy Build ID-based."""

    build_id_with_versioning: str


WorkerVersioningStrategy: TypeAlias = (
    WorkerVersioningStrategyNone
    | WorkerDeploymentOptions
    | WorkerVersioningStrategyLegacyBuildIdBased
)


@dataclass
class ResourceBasedTunerConfig:
    """Python representation of the Rust struct for configuring a resource-based tuner."""

    target_memory_usage: float
    target_cpu_usage: float


@dataclass
class ResourceBasedSlotSupplier:
    """Python representation of the Rust struct for a resource-based slot supplier."""

    minimum_slots: int
    maximum_slots: int
    ramp_throttle_ms: int
    tuner_config: ResourceBasedTunerConfig


@dataclass(frozen=True)
class FixedSizeSlotSupplier:
    """Python representation of the Rust struct for a fixed-size slot supplier."""

    num_slots: int


SlotSupplier: TypeAlias = (
    FixedSizeSlotSupplier | ResourceBasedSlotSupplier | BridgeCustomSlotSupplier
)


@dataclass
class TunerHolder:
    """Python representation of the Rust struct for a tuner holder."""

    workflow_slot_supplier: SlotSupplier
    activity_slot_supplier: SlotSupplier
    local_activity_slot_supplier: SlotSupplier
    nexus_slot_supplier: SlotSupplier


@dataclass
class WorkerTaskTypes:
    """Python representation of the Rust struct for worker task types"""

    enable_workflows: bool
    enable_local_activities: bool
    enable_remote_activities: bool
    enable_nexus: bool


class Worker:
    """SDK Core worker."""

    @staticmethod
    def create(client: temporalio.bridge.client.Client, config: WorkerConfig) -> Worker:
        """Create a bridge worker from a bridge client."""
        return Worker(
            temporalio.bridge.temporal_sdk_bridge.new_worker(
                client._runtime._ref, client._ref, config
            )
        )

    @staticmethod
    def for_replay(
        runtime: temporalio.bridge.runtime.Runtime,
        config: WorkerConfig,
    ) -> tuple[Worker, temporalio.bridge.temporal_sdk_bridge.HistoryPusher]:
        """Create a bridge replay worker."""
        [
            replay_worker,
            pusher,
        ] = temporalio.bridge.temporal_sdk_bridge.new_replay_worker(
            runtime._ref, config
        )
        return Worker(replay_worker), pusher

    def __init__(self, ref: temporalio.bridge.temporal_sdk_bridge.WorkerRef) -> None:
        """Create SDK core worker from a bridge worker."""
        self._ref = ref

    async def validate(
        self,
    ) -> temporalio.bridge.proto.NamespaceInfo:
        """Validate the bridge worker."""
        return temporalio.bridge.proto.NamespaceInfo.FromString(
            await self._ref.validate()  # type: ignore[reportOptionalMemberAccess]
        )

    async def poll_workflow_activation(
        self,
    ) -> temporalio.bridge.proto.workflow_activation.WorkflowActivation:
        """Poll for a workflow activation."""
        return (
            temporalio.bridge.proto.workflow_activation.WorkflowActivation.FromString(
                await self._ref.poll_workflow_activation()  # type: ignore[reportOptionalMemberAccess]
            )
        )

    async def poll_activity_task(
        self,
    ) -> temporalio.bridge.proto.activity_task.ActivityTask:
        """Poll for an activity task."""
        return temporalio.bridge.proto.activity_task.ActivityTask.FromString(
            await self._ref.poll_activity_task()  # type: ignore[reportOptionalMemberAccess]
        )

    async def poll_nexus_task(
        self,
    ) -> temporalio.bridge.proto.nexus.NexusTask:
        """Poll for a nexus task."""
        return temporalio.bridge.proto.nexus.NexusTask.FromString(
            await self._ref.poll_nexus_task()  # type: ignore[reportOptionalMemberAccess]
        )

    async def complete_workflow_activation(
        self,
        comp: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        """Complete a workflow activation."""
        await self._ref.complete_workflow_activation(comp.SerializeToString())  # type: ignore[reportOptionalMemberAccess]

    async def complete_activity_task(
        self, comp: temporalio.bridge.proto.ActivityTaskCompletion
    ) -> None:
        """Complete an activity task."""
        await self._ref.complete_activity_task(comp.SerializeToString())  # type: ignore[reportOptionalMemberAccess]

    async def complete_nexus_task(
        self, comp: temporalio.bridge.proto.nexus.NexusTaskCompletion
    ) -> None:
        """Complete a nexus task."""
        await self._ref.complete_nexus_task(comp.SerializeToString())  # type: ignore[reportOptionalMemberAccess]

    def record_activity_heartbeat(
        self, comp: temporalio.bridge.proto.ActivityHeartbeat
    ) -> None:
        """Record an activity heartbeat."""
        self._ref.record_activity_heartbeat(comp.SerializeToString())  # type: ignore[reportOptionalMemberAccess]

    def request_workflow_eviction(self, run_id: str) -> None:
        """Request a workflow be evicted."""
        self._ref.request_workflow_eviction(run_id)  # type: ignore[reportOptionalMemberAccess]

    def replace_client(self, client: temporalio.bridge.client.Client) -> None:
        """Replace the worker client."""
        self._ref.replace_client(client._ref)  # type: ignore[reportOptionalMemberAccess]

    def initiate_shutdown(self) -> None:
        """Start shutdown of the worker."""
        self._ref.initiate_shutdown()  # type: ignore[reportOptionalMemberAccess]

    async def finalize_shutdown(self) -> None:
        """Finalize the worker.

        This will fail if shutdown hasn't completed fully due to internal
        reference count checks.
        """
        ref = self._ref
        self._ref = None
        await ref.finalize_shutdown()  # type: ignore[reportOptionalMemberAccess]


class _Visitor(VisitorFunctions):
    def __init__(
        self,
        f: Callable[[Sequence[Payload]], Awaitable[list[Payload]]],
    ):
        self._f = f

    async def visit_payload(self, payload: Payload) -> None:
        new_payload = (await self._f([payload]))[0]
        if new_payload is not payload:
            payload.CopyFrom(new_payload)

    async def visit_payloads(self, payloads: PayloadSequence) -> None:
        if len(payloads) == 0:
            return
        new_payloads = await self._f(payloads)
        if new_payloads is payloads:
            return
        del payloads[:]
        payloads.extend(new_payloads)


async def decode_activation(
    activation: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
    data_converter: temporalio.converter.DataConverter,
    decode_headers: bool,
    storage_concurrency_limit: int,
) -> temporalio.converter._extstore.StorageOperationMetrics:
    """Decode all payloads in the activation.

    Returns:
        Metrics from any external storage retrieval operations that occurred.
    """
    metrics = temporalio.converter._extstore.StorageOperationMetrics()
    with metrics.track():
        await CommandAwarePayloadVisitor(
            skip_search_attributes=True,
            skip_headers=not decode_headers,
            concurrency_limit=storage_concurrency_limit,
        ).visit(
            _Visitor(data_converter._external_retrieve_payload_sequence), activation
        )

    await CommandAwarePayloadVisitor(
        skip_search_attributes=True,
        skip_headers=not decode_headers,
    ).visit(_Visitor(data_converter._decode_payload_sequence), activation)

    return metrics


async def encode_completion(
    completion: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    data_converter: temporalio.converter.DataConverter,
    encode_headers: bool,
    storage_concurrency_limit: int,
) -> temporalio.converter._extstore.StorageOperationMetrics:
    """Encode all payloads in the completion.

    Returns:
        Metrics from any external storage store operations that occurred.
    """
    await CommandAwarePayloadVisitor(
        skip_search_attributes=True,
        skip_headers=not encode_headers,
    ).visit(
        _Visitor(data_converter._encode_payload_sequence),
        completion,
    )

    metrics = temporalio.converter._extstore.StorageOperationMetrics()
    with metrics.track():
        await CommandAwarePayloadVisitor(
            skip_search_attributes=True,
            skip_headers=not encode_headers,
            concurrency_limit=storage_concurrency_limit,
        ).visit(
            _Visitor(data_converter._external_store_payload_sequence),
            completion,
        )

    return metrics
