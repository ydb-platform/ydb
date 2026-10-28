import collections.abc
import typing
from datetime import timedelta

import google.protobuf.duration_pb2

import temporalio.api.common.v1.message_pb2 as common_pb2
import temporalio.api.enums.v1.workflow_pb2 as workflow_enums_pb2
import temporalio.api.taskqueue.v1.message_pb2 as taskqueue_pb2
import temporalio.api.workflow.v1
import temporalio.common
import temporalio.converter
import temporalio.nexus.system


class SignalWithStartWorkflowModelRequest(typing.Protocol):
    namespace: str
    id: str


def retry_policy_from_proto(
    proto: common_pb2.RetryPolicy,
) -> temporalio.common.RetryPolicy:
    return temporalio.common.RetryPolicy.from_proto(proto)


def retry_policy_to_proto(
    retry_policy: temporalio.common.RetryPolicy,
) -> common_pb2.RetryPolicy:
    proto = common_pb2.RetryPolicy()
    retry_policy.apply_to_proto(proto)
    return proto


def workflow_function_name(
    value: str | collections.abc.Callable[..., collections.abc.Awaitable[object]],
) -> str:
    from temporalio.workflow import _Definition  # pyright: ignore[reportPrivateUsage]

    name, _result_type = _Definition.get_name_and_result_type(value)
    return name


def signal_function_to_proto(
    value: str | collections.abc.Callable[..., typing.Any],
) -> str:
    from temporalio.workflow import (
        _SignalDefinition,  # pyright: ignore[reportPrivateUsage]
    )

    return _SignalDefinition.must_name_from_fn_or_str(value)  # pyright: ignore[reportUnknownMemberType]


def workflow_type_to_proto(
    workflow_type: str
    | collections.abc.Callable[..., collections.abc.Awaitable[object]],
) -> common_pb2.WorkflowType:
    return common_pb2.WorkflowType(name=workflow_function_name(workflow_type))


def workflow_type_from_proto(
    proto: common_pb2.WorkflowType,
) -> str:
    return proto.name


def task_queue_from_proto(
    proto: taskqueue_pb2.TaskQueue,
) -> str:
    return proto.name


def task_queue_to_proto(
    task_queue: str,
) -> taskqueue_pb2.TaskQueue:
    return taskqueue_pb2.TaskQueue(name=task_queue)


def workflow_namespace() -> str:
    from temporalio.workflow import info

    return info().namespace


def signal_with_start_workflow_serialization_context(
    request: SignalWithStartWorkflowModelRequest,
) -> temporalio.converter.WorkflowSerializationContext:
    return temporalio.converter.WorkflowSerializationContext(
        namespace=request.namespace,
        workflow_id=request.id,
    )


def payloads_to_proto(
    values: collections.abc.Sequence[typing.Any],
) -> common_pb2.Payloads:
    return (
        temporalio.nexus.system._current_user_payload_converter().to_payloads_wrapper(
            values
        )
    )


def payloads_from_proto(
    proto: common_pb2.Payloads,
) -> list[object]:
    return list(
        temporalio.nexus.system._current_user_payload_converter().from_payloads_wrapper(
            proto
        )
    )


def _clone_payload(payload: common_pb2.Payload) -> common_pb2.Payload:
    clone = common_pb2.Payload()
    clone.CopyFrom(payload)
    return clone


def _value_to_payload(value: object | common_pb2.Payload) -> common_pb2.Payload:
    if isinstance(value, common_pb2.Payload):
        return _clone_payload(value)
    payloads = (
        temporalio.nexus.system._current_user_payload_converter().to_payloads_wrapper(
            [value]
        )
    )
    return _clone_payload(payloads.payloads[0])


def _payload_to_value(payload: common_pb2.Payload) -> object:
    wrapper = common_pb2.Payloads()
    wrapper.payloads.add().CopyFrom(payload)

    return typing.cast(
        object,
        temporalio.nexus.system._current_user_payload_converter().from_payloads_wrapper(
            wrapper
        )[0],
    )


def payload_from_proto(
    proto: common_pb2.Payload,
) -> object:
    return _payload_to_value(proto)


def payload_to_proto(
    payload: object,
) -> common_pb2.Payload:
    return _value_to_payload(payload)


def memo_from_proto(
    proto: common_pb2.Memo,
) -> collections.abc.Mapping[str, object]:
    return {key: _payload_to_value(value) for key, value in proto.fields.items()}


def memo_to_proto(
    memo: collections.abc.Mapping[str, object],
) -> common_pb2.Memo:
    message = common_pb2.Memo()
    for key, value in memo.items():
        message.fields[key].CopyFrom(_value_to_payload(value))
    return message


def duration_from_proto(proto: google.protobuf.duration_pb2.Duration) -> timedelta:
    return proto.ToTimedelta()


def duration_to_proto(
    duration: timedelta,
) -> google.protobuf.duration_pb2.Duration:
    proto = google.protobuf.duration_pb2.Duration()
    proto.FromTimedelta(duration)
    return proto


def workflow_id_reuse_policy_from_proto(
    policy: workflow_enums_pb2.WorkflowIdReusePolicy.ValueType,
) -> temporalio.common.WorkflowIDReusePolicy:
    return temporalio.common.WorkflowIDReusePolicy(int(policy))


def workflow_id_reuse_policy_to_proto(
    policy: temporalio.common.WorkflowIDReusePolicy,
) -> workflow_enums_pb2.WorkflowIdReusePolicy.ValueType:
    return typing.cast(workflow_enums_pb2.WorkflowIdReusePolicy.ValueType, int(policy))


def workflow_id_conflict_policy_from_proto(
    policy: workflow_enums_pb2.WorkflowIdConflictPolicy.ValueType,
) -> temporalio.common.WorkflowIDConflictPolicy:
    return temporalio.common.WorkflowIDConflictPolicy(int(policy))


def workflow_id_conflict_policy_to_proto(
    policy: temporalio.common.WorkflowIDConflictPolicy,
) -> workflow_enums_pb2.WorkflowIdConflictPolicy.ValueType:
    return typing.cast(
        workflow_enums_pb2.WorkflowIdConflictPolicy.ValueType, int(policy)
    )


def search_attributes_to_proto(
    search_attributes: temporalio.common.TypedSearchAttributes,
) -> common_pb2.SearchAttributes:
    proto = common_pb2.SearchAttributes()
    temporalio.converter.encode_search_attributes(search_attributes, proto)
    return proto


def search_attributes_from_proto(
    proto: common_pb2.SearchAttributes,
) -> temporalio.common.TypedSearchAttributes:
    return temporalio.converter.decode_typed_search_attributes(proto)


def priority_from_proto(
    proto: common_pb2.Priority,
) -> temporalio.common.Priority:
    return temporalio.common.Priority._from_proto(proto)  # pyright: ignore[reportPrivateUsage]


def priority_to_proto(
    priority: temporalio.common.Priority,
) -> common_pb2.Priority:
    return priority._to_proto()  # pyright: ignore[reportPrivateUsage]


def versioning_override_to_proto(
    versioning_override: temporalio.common.VersioningOverride,
) -> temporalio.api.workflow.v1.VersioningOverride:
    return versioning_override._to_proto()  # pyright: ignore[reportPrivateUsage]


def versioning_override_from_proto(
    proto: temporalio.api.workflow.v1.VersioningOverride,
) -> temporalio.common.VersioningOverride:
    if proto.HasField("pinned") and proto.pinned.HasField("version"):
        version = proto.pinned.version
        return temporalio.common.PinnedVersioningOverride(
            temporalio.common.WorkerDeploymentVersion(
                deployment_name=version.deployment_name,
                build_id=version.build_id,
            )
        )
    if proto.pinned_version:
        return temporalio.common.PinnedVersioningOverride(
            temporalio.common.WorkerDeploymentVersion.from_canonical_string(
                proto.pinned_version
            )
        )
    if proto.auto_upgrade:
        return temporalio.common.AutoUpgradeVersioningOverride()
    raise ValueError("unknown versioning override proto shape")
