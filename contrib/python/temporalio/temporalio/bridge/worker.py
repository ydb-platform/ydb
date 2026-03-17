"""Worker using SDK Core. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import google.protobuf.internal.containers
from typing_extensions import TypeAlias

import temporalio.api.common.v1
import temporalio.api.history.v1
import temporalio.bridge.client
import temporalio.bridge.proto
import temporalio.bridge.proto.activity_task
import temporalio.bridge.proto.nexus
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge
import temporalio.converter
import temporalio.exceptions
from temporalio.bridge.temporal_sdk_bridge import (
    CustomSlotSupplier as BridgeCustomSlotSupplier,
)
from temporalio.bridge.temporal_sdk_bridge import PollShutdownError  # type: ignore


@dataclass
class WorkerConfig:
    """Python representation of the Rust struct for configuring a worker."""

    namespace: str
    task_queue: str
    versioning_strategy: WorkerVersioningStrategy
    identity_override: Optional[str]
    max_cached_workflows: int
    tuner: TunerHolder
    workflow_task_poller_behavior: PollerBehavior
    nonsticky_to_sticky_poll_ratio: float
    activity_task_poller_behavior: PollerBehavior
    no_remote_activities: bool
    sticky_queue_schedule_to_start_timeout_millis: int
    max_heartbeat_throttle_interval_millis: int
    default_heartbeat_throttle_interval_millis: int
    max_activities_per_second: Optional[float]
    max_task_queue_activities_per_second: Optional[float]
    graceful_shutdown_period_millis: int
    nondeterminism_as_workflow_fail: bool
    nondeterminism_as_workflow_fail_for_types: Set[str]
    nexus_task_poller_behavior: PollerBehavior


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


PollerBehavior: TypeAlias = Union[
    PollerBehaviorSimpleMaximum,
    PollerBehaviorAutoscaling,
]


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


WorkerVersioningStrategy: TypeAlias = Union[
    WorkerVersioningStrategyNone,
    WorkerDeploymentOptions,
    WorkerVersioningStrategyLegacyBuildIdBased,
]


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


SlotSupplier: TypeAlias = Union[
    FixedSizeSlotSupplier,
    ResourceBasedSlotSupplier,
    BridgeCustomSlotSupplier,
]


@dataclass
class TunerHolder:
    """Python representation of the Rust struct for a tuner holder."""

    workflow_slot_supplier: SlotSupplier
    activity_slot_supplier: SlotSupplier
    local_activity_slot_supplier: SlotSupplier


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
    ) -> Tuple[Worker, temporalio.bridge.temporal_sdk_bridge.HistoryPusher]:
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

    async def validate(self) -> None:
        """Validate the bridge worker."""
        await self._ref.validate()

    async def poll_workflow_activation(
        self,
    ) -> temporalio.bridge.proto.workflow_activation.WorkflowActivation:
        """Poll for a workflow activation."""
        return (
            temporalio.bridge.proto.workflow_activation.WorkflowActivation.FromString(
                await self._ref.poll_workflow_activation()
            )
        )

    async def poll_activity_task(
        self,
    ) -> temporalio.bridge.proto.activity_task.ActivityTask:
        """Poll for an activity task."""
        return temporalio.bridge.proto.activity_task.ActivityTask.FromString(
            await self._ref.poll_activity_task()
        )

    async def poll_nexus_task(
        self,
    ) -> temporalio.bridge.proto.nexus.NexusTask:
        """Poll for a nexus task."""
        return temporalio.bridge.proto.nexus.NexusTask.FromString(
            await self._ref.poll_nexus_task()
        )

    async def complete_workflow_activation(
        self,
        comp: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        """Complete a workflow activation."""
        await self._ref.complete_workflow_activation(comp.SerializeToString())

    async def complete_activity_task(
        self, comp: temporalio.bridge.proto.ActivityTaskCompletion
    ) -> None:
        """Complete an activity task."""
        await self._ref.complete_activity_task(comp.SerializeToString())

    async def complete_nexus_task(
        self, comp: temporalio.bridge.proto.nexus.NexusTaskCompletion
    ) -> None:
        """Complete a nexus task."""
        await self._ref.complete_nexus_task(comp.SerializeToString())

    def record_activity_heartbeat(
        self, comp: temporalio.bridge.proto.ActivityHeartbeat
    ) -> None:
        """Record an activity heartbeat."""
        self._ref.record_activity_heartbeat(comp.SerializeToString())

    def request_workflow_eviction(self, run_id: str) -> None:
        """Request a workflow be evicted."""
        self._ref.request_workflow_eviction(run_id)

    def replace_client(self, client: temporalio.bridge.client.Client) -> None:
        """Replace the worker client."""
        self._ref.replace_client(client._ref)

    def initiate_shutdown(self) -> None:
        """Start shutdown of the worker."""
        self._ref.initiate_shutdown()

    async def finalize_shutdown(self) -> None:
        """Finalize the worker.

        This will fail if shutdown hasn't completed fully due to internal
        reference count checks.
        """
        ref = self._ref
        self._ref = None
        await ref.finalize_shutdown()


# See https://mypy.readthedocs.io/en/stable/runtime_troubles.html#using-classes-that-are-generic-in-stubs-but-not-at-runtime
if TYPE_CHECKING:
    PayloadContainer: TypeAlias = (
        google.protobuf.internal.containers.RepeatedCompositeFieldContainer[
            temporalio.api.common.v1.Payload
        ]
    )
else:
    PayloadContainer: TypeAlias = (
        google.protobuf.internal.containers.RepeatedCompositeFieldContainer
    )


async def _apply_to_headers(
    headers: Mapping[str, temporalio.api.common.v1.Payload],
    cb: Callable[
        [Sequence[temporalio.api.common.v1.Payload]],
        Awaitable[List[temporalio.api.common.v1.Payload]],
    ],
) -> None:
    """Apply API payload callback to headers."""
    for payload in headers.values():
        new_payload = (await cb([payload]))[0]
        payload.CopyFrom(new_payload)


async def _decode_headers(
    headers: Mapping[str, temporalio.api.common.v1.Payload],
    codec: temporalio.converter.PayloadCodec,
) -> None:
    """Decode headers with the given codec."""
    return await _apply_to_headers(headers, codec.decode)


async def _encode_headers(
    headers: Mapping[str, temporalio.api.common.v1.Payload],
    codec: temporalio.converter.PayloadCodec,
) -> None:
    """Encode headers with the given codec."""
    return await _apply_to_headers(headers, codec.encode)


async def _apply_to_payloads(
    payloads: PayloadContainer,
    cb: Callable[
        [Sequence[temporalio.api.common.v1.Payload]],
        Awaitable[List[temporalio.api.common.v1.Payload]],
    ],
) -> None:
    """Apply API payload callback to payloads."""
    if len(payloads) == 0:
        return
    new_payloads = await cb(payloads)
    if new_payloads is payloads:
        return
    del payloads[:]
    # TODO(cretz): Copy too expensive?
    payloads.extend(new_payloads)


async def _apply_to_payload(
    payload: temporalio.api.common.v1.Payload,
    cb: Callable[
        [Sequence[temporalio.api.common.v1.Payload]],
        Awaitable[List[temporalio.api.common.v1.Payload]],
    ],
) -> None:
    """Apply API payload callback to payload."""
    new_payload = (await cb([payload]))[0]
    payload.CopyFrom(new_payload)


async def _decode_payloads(
    payloads: PayloadContainer,
    codec: temporalio.converter.PayloadCodec,
) -> None:
    """Decode payloads with the given codec."""
    return await _apply_to_payloads(payloads, codec.decode)


async def _decode_payload(
    payload: temporalio.api.common.v1.Payload,
    codec: temporalio.converter.PayloadCodec,
) -> None:
    """Decode a payload with the given codec."""
    return await _apply_to_payload(payload, codec.decode)


async def _encode_payloads(
    payloads: PayloadContainer,
    codec: temporalio.converter.PayloadCodec,
) -> None:
    """Encode payloads with the given codec."""
    return await _apply_to_payloads(payloads, codec.encode)


async def _encode_payload(
    payload: temporalio.api.common.v1.Payload,
    codec: temporalio.converter.PayloadCodec,
) -> None:
    """Decode a payload with the given codec."""
    return await _apply_to_payload(payload, codec.encode)


async def decode_activation(
    act: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
    codec: temporalio.converter.PayloadCodec,
    decode_headers: bool,
) -> None:
    """Decode the given activation with the codec."""
    for job in act.jobs:
        if job.HasField("query_workflow"):
            await _decode_payloads(job.query_workflow.arguments, codec)
            if decode_headers:
                await _decode_headers(job.query_workflow.headers, codec)
        elif job.HasField("resolve_activity"):
            if job.resolve_activity.result.HasField("cancelled"):
                await codec.decode_failure(
                    job.resolve_activity.result.cancelled.failure
                )
            elif job.resolve_activity.result.HasField("completed"):
                if job.resolve_activity.result.completed.HasField("result"):
                    await _decode_payload(
                        job.resolve_activity.result.completed.result, codec
                    )
            elif job.resolve_activity.result.HasField("failed"):
                await codec.decode_failure(job.resolve_activity.result.failed.failure)
        elif job.HasField("resolve_child_workflow_execution"):
            if job.resolve_child_workflow_execution.result.HasField("cancelled"):
                await codec.decode_failure(
                    job.resolve_child_workflow_execution.result.cancelled.failure
                )
            elif job.resolve_child_workflow_execution.result.HasField(
                "completed"
            ) and job.resolve_child_workflow_execution.result.completed.HasField(
                "result"
            ):
                await _decode_payload(
                    job.resolve_child_workflow_execution.result.completed.result, codec
                )
            elif job.resolve_child_workflow_execution.result.HasField("failed"):
                await codec.decode_failure(
                    job.resolve_child_workflow_execution.result.failed.failure
                )
        elif job.HasField("resolve_child_workflow_execution_start"):
            if job.resolve_child_workflow_execution_start.HasField("cancelled"):
                await codec.decode_failure(
                    job.resolve_child_workflow_execution_start.cancelled.failure
                )
        elif job.HasField("resolve_request_cancel_external_workflow"):
            if job.resolve_request_cancel_external_workflow.HasField("failure"):
                await codec.decode_failure(
                    job.resolve_request_cancel_external_workflow.failure
                )
        elif job.HasField("resolve_signal_external_workflow"):
            if job.resolve_signal_external_workflow.HasField("failure"):
                await codec.decode_failure(job.resolve_signal_external_workflow.failure)
        elif job.HasField("signal_workflow"):
            await _decode_payloads(job.signal_workflow.input, codec)
            if decode_headers:
                await _decode_headers(job.signal_workflow.headers, codec)
        elif job.HasField("initialize_workflow"):
            await _decode_payloads(job.initialize_workflow.arguments, codec)
            if decode_headers:
                await _decode_headers(job.initialize_workflow.headers, codec)
            if job.initialize_workflow.HasField("continued_failure"):
                await codec.decode_failure(job.initialize_workflow.continued_failure)
            for val in job.initialize_workflow.memo.fields.values():
                # This uses API payload not bridge payload
                new_payload = (await codec.decode([val]))[0]
                # Make a shallow copy, in case new_payload.metadata and val.metadata are
                # references to the same memory, e.g. decode() returns its input unchanged.
                new_metadata = dict(new_payload.metadata)
                val.metadata.clear()
                val.metadata.update(new_metadata)
                val.data = new_payload.data
        elif job.HasField("do_update"):
            await _decode_payloads(job.do_update.input, codec)
            if decode_headers:
                await _decode_headers(job.do_update.headers, codec)


async def encode_completion(
    comp: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    codec: temporalio.converter.PayloadCodec,
    encode_headers: bool,
) -> None:
    """Recursively encode the given completion with the codec."""
    if comp.HasField("failed"):
        await codec.encode_failure(comp.failed.failure)
    elif comp.HasField("successful"):
        for command in comp.successful.commands:
            if command.HasField("complete_workflow_execution"):
                if command.complete_workflow_execution.HasField("result"):
                    await _encode_payload(
                        command.complete_workflow_execution.result, codec
                    )
            elif command.HasField("continue_as_new_workflow_execution"):
                await _encode_payloads(
                    command.continue_as_new_workflow_execution.arguments, codec
                )
                if encode_headers:
                    await _encode_headers(
                        command.continue_as_new_workflow_execution.headers, codec
                    )
                for val in command.continue_as_new_workflow_execution.memo.values():
                    await _encode_payload(val, codec)
            elif command.HasField("fail_workflow_execution"):
                await codec.encode_failure(command.fail_workflow_execution.failure)
            elif command.HasField("respond_to_query"):
                if command.respond_to_query.HasField("failed"):
                    await codec.encode_failure(command.respond_to_query.failed)
                elif command.respond_to_query.HasField(
                    "succeeded"
                ) and command.respond_to_query.succeeded.HasField("response"):
                    await _encode_payload(
                        command.respond_to_query.succeeded.response, codec
                    )
            elif command.HasField("schedule_activity"):
                await _encode_payloads(command.schedule_activity.arguments, codec)
                if encode_headers:
                    await _encode_headers(command.schedule_activity.headers, codec)
            elif command.HasField("schedule_local_activity"):
                await _encode_payloads(command.schedule_local_activity.arguments, codec)
                if encode_headers:
                    await _encode_headers(
                        command.schedule_local_activity.headers, codec
                    )
            elif command.HasField("signal_external_workflow_execution"):
                await _encode_payloads(
                    command.signal_external_workflow_execution.args, codec
                )
                if encode_headers:
                    await _encode_headers(
                        command.signal_external_workflow_execution.headers, codec
                    )
            elif command.HasField("start_child_workflow_execution"):
                await _encode_payloads(
                    command.start_child_workflow_execution.input, codec
                )
                if encode_headers:
                    await _encode_headers(
                        command.start_child_workflow_execution.headers, codec
                    )
                for val in command.start_child_workflow_execution.memo.values():
                    await _encode_payload(val, codec)
            elif command.HasField("update_response"):
                if command.update_response.HasField("completed"):
                    await _encode_payload(command.update_response.completed, codec)
                elif command.update_response.HasField("rejected"):
                    await codec.encode_failure(command.update_response.rejected)
