"""Failure converters for converting exceptions to/from Temporal Failure protos."""

from __future__ import annotations

import dataclasses
import json
import traceback
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, ClassVar

import google.protobuf.json_format
import nexusrpc

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.failure.v1
import temporalio.exceptions
from temporalio.converter._payload_converter import PayloadConverter

logger = getLogger("temporalio.converter")

_TEMPORAL_FAILURE_PROTO_TYPE = "temporal.api.failure.v1.Failure"


class FailureConverter(ABC):
    """Base failure converter to/from errors.

    Note, for workflow exceptions, :py:attr:`to_failure` is only invoked if the
    exception is an instance of :py:class:`temporalio.exceptions.FailureError`.
    Users should extend :py:class:`temporalio.exceptions.ApplicationError` if
    they want a custom workflow exception to work with this class.
    """

    default: ClassVar[FailureConverter]
    """Default failure converter."""

    @abstractmethod
    def to_failure(
        self,
        exception: BaseException,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        """Convert the given exception to a Temporal failure.

        Users should make sure not to alter the ``exception`` input.

        Args:
            exception: The exception to convert.
            payload_converter: The payload converter to use if needed.
            failure: The failure to update with error information.
        """
        raise NotImplementedError

    @abstractmethod
    def from_failure(
        self,
        failure: temporalio.api.failure.v1.Failure,
        payload_converter: PayloadConverter,
    ) -> BaseException:
        """Convert the given Temporal failure to an exception.

        Users should make sure not to alter the ``failure`` input.

        Args:
            failure: The failure to convert.
            payload_converter: The payload converter to use if needed.

        Returns:
            Converted error.
        """
        raise NotImplementedError


class DefaultFailureConverter(FailureConverter):
    """Default failure converter.

    A singleton instance of this is available at
    :py:attr:`FailureConverter.default`.
    """

    def __init__(self, *, encode_common_attributes: bool = False) -> None:
        """Create the default failure converter.

        Args:
            encode_common_attributes: If ``True``, the message and stack trace
                of the failure will be moved into the encoded attribute section
                of the failure which can be encoded with a codec.
        """
        super().__init__()
        self._encode_common_attributes = encode_common_attributes

    def to_failure(
        self,
        exception: BaseException,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        """See base class."""
        # If already a failure error, use that
        if isinstance(exception, temporalio.exceptions.FailureError):
            self._error_to_failure(exception, payload_converter, failure)
        elif isinstance(exception, nexusrpc.HandlerError):
            self._nexus_handler_error_to_failure(exception, payload_converter, failure)
        else:
            # Convert to failure error
            failure_error = temporalio.exceptions.ApplicationError(
                str(exception),
                type=exception.__class__.__name__,
            )
            failure_error.__traceback__ = exception.__traceback__
            failure_error.__cause__ = exception.__cause__
            self._error_to_failure(failure_error, payload_converter, failure)
        # Encode common attributes if requested
        if self._encode_common_attributes:
            # Move message and stack trace to encoded attribute payload
            failure.encoded_attributes.CopyFrom(
                payload_converter.to_payloads(
                    [{"message": failure.message, "stack_trace": failure.stack_trace}]
                )[0]
            )
            failure.message = "Encoded failure"
            failure.stack_trace = ""

    def _error_to_failure(
        self,
        error: temporalio.exceptions.FailureError,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        # If there is an underlying proto already, just use that
        if error.failure:
            failure.CopyFrom(error.failure)
            return

        # Set message, stack, and cause. Obtaining cause follows rules from
        # https://docs.python.org/3/library/exceptions.html#exception-context
        failure.message = error.message
        if error.__traceback__:
            failure.stack_trace = "\n".join(traceback.format_tb(error.__traceback__))
        if error.__cause__:
            self.to_failure(error.__cause__, payload_converter, failure.cause)
        elif not error.__suppress_context__ and error.__context__:
            self.to_failure(error.__context__, payload_converter, failure.cause)

        # Set specific subclass values
        if isinstance(error, temporalio.exceptions.ApplicationError):
            failure.application_failure_info.SetInParent()
            if error.type:
                failure.application_failure_info.type = error.type
            failure.application_failure_info.non_retryable = error.non_retryable
            if error.details:
                failure.application_failure_info.details.CopyFrom(
                    payload_converter.to_payloads_wrapper(error.details)
                )
            if error.next_retry_delay:
                failure.application_failure_info.next_retry_delay.FromTimedelta(
                    error.next_retry_delay
                )
            if error.category:
                failure.application_failure_info.category = (
                    temporalio.api.enums.v1.ApplicationErrorCategory.ValueType(
                        error.category
                    )
                )
        elif isinstance(error, temporalio.exceptions.TimeoutError):
            failure.timeout_failure_info.SetInParent()
            failure.timeout_failure_info.timeout_type = (
                temporalio.api.enums.v1.TimeoutType.ValueType(error.type or 0)
            )
            if error.last_heartbeat_details:
                failure.timeout_failure_info.last_heartbeat_details.CopyFrom(
                    payload_converter.to_payloads_wrapper(error.last_heartbeat_details)
                )
        elif isinstance(error, temporalio.exceptions.CancelledError):
            failure.canceled_failure_info.SetInParent()
            if error.details:
                failure.canceled_failure_info.details.CopyFrom(
                    payload_converter.to_payloads_wrapper(error.details)
                )
        elif isinstance(error, temporalio.exceptions.TerminatedError):
            failure.terminated_failure_info.SetInParent()
        elif isinstance(error, temporalio.exceptions.ServerError):
            failure.server_failure_info.SetInParent()
            failure.server_failure_info.non_retryable = error.non_retryable
        elif isinstance(error, temporalio.exceptions.ActivityError):
            failure.activity_failure_info.SetInParent()
            failure.activity_failure_info.scheduled_event_id = error.scheduled_event_id
            failure.activity_failure_info.started_event_id = error.started_event_id
            failure.activity_failure_info.identity = error.identity
            failure.activity_failure_info.activity_type.name = error.activity_type
            failure.activity_failure_info.activity_id = error.activity_id
            failure.activity_failure_info.retry_state = (
                temporalio.api.enums.v1.RetryState.ValueType(error.retry_state or 0)
            )
        elif isinstance(error, temporalio.exceptions.ChildWorkflowError):
            failure.child_workflow_execution_failure_info.SetInParent()
            failure.child_workflow_execution_failure_info.namespace = error.namespace
            failure.child_workflow_execution_failure_info.workflow_execution.workflow_id = error.workflow_id
            failure.child_workflow_execution_failure_info.workflow_execution.run_id = (
                error.run_id
            )
            failure.child_workflow_execution_failure_info.workflow_type.name = (
                error.workflow_type
            )
            failure.child_workflow_execution_failure_info.initiated_event_id = (
                error.initiated_event_id
            )
            failure.child_workflow_execution_failure_info.started_event_id = (
                error.started_event_id
            )
            failure.child_workflow_execution_failure_info.retry_state = (
                temporalio.api.enums.v1.RetryState.ValueType(error.retry_state or 0)
            )
        elif isinstance(error, temporalio.exceptions.NexusOperationError):
            failure.nexus_operation_execution_failure_info.SetInParent()
            failure.nexus_operation_execution_failure_info.scheduled_event_id = (
                error.scheduled_event_id
            )
            failure.nexus_operation_execution_failure_info.endpoint = error.endpoint
            failure.nexus_operation_execution_failure_info.service = error.service
            failure.nexus_operation_execution_failure_info.operation = error.operation
            failure.nexus_operation_execution_failure_info.operation_token = (
                error.operation_token
            )

    def _nexus_handler_error_to_failure(
        self,
        error: nexusrpc.HandlerError,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        if error.original_failure:
            self._nexus_failure_to_temporal_failure(
                error.original_failure, error.retryable, failure
            )
        else:
            failure.message = error.message
            if stack_trace := error.stack_trace:
                failure.stack_trace = stack_trace
            elif tb := error.__traceback__:
                failure.stack_trace = "\n".join(traceback.format_tb(tb))
            if error.__cause__:
                self.to_failure(error.__cause__, payload_converter, failure.cause)
            failure.nexus_handler_failure_info.SetInParent()
            failure.nexus_handler_failure_info.type = error.type.name
            failure.nexus_handler_failure_info.retry_behavior = temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.ValueType(
                temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
                if error.retryable_override is True
                else temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
                if error.retryable_override is False
                else temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED
            )

    def _temporal_failure_to_nexus_failure(
        self, failure: temporalio.api.failure.v1.Failure
    ) -> nexusrpc.Failure:
        message, failure.message = failure.message, ""
        stack_trace, failure.stack_trace = failure.stack_trace, ""
        failure_dict = google.protobuf.json_format.MessageToDict(failure)
        failure.message = message
        failure.stack_trace = stack_trace
        return nexusrpc.Failure(
            message=message,
            stack_trace=stack_trace,
            metadata={
                "type": _TEMPORAL_FAILURE_PROTO_TYPE,
            },
            details=failure_dict,
        )

    def _nexus_failure_to_temporal_failure(
        self,
        failure: nexusrpc.Failure,
        retryable: bool,
        temporal_failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        if (
            failure.metadata
            and failure.metadata.get("type") == _TEMPORAL_FAILURE_PROTO_TYPE
        ):
            google.protobuf.json_format.ParseDict(
                dict(failure.details or {}), temporal_failure
            )
        else:
            temporal_failure.application_failure_info.SetInParent()
            temporal_failure.application_failure_info.type = "NexusFailure"
            temporal_failure.application_failure_info.non_retryable = not retryable
            temporal_failure.application_failure_info.details.SetInParent()
            temporal_failure.application_failure_info.details.payloads.append(
                temporalio.api.common.v1.Payload(
                    metadata={"encoding": b"json/plain"},
                    data=json.dumps(
                        dataclasses.replace(failure, message=""), separators=(",", ":")
                    ).encode("utf-8"),
                )
            )

        temporal_failure.message = failure.message
        temporal_failure.stack_trace = failure.stack_trace or ""

    def from_failure(
        self,
        failure: temporalio.api.failure.v1.Failure,
        payload_converter: PayloadConverter,
    ) -> BaseException:
        """See base class."""
        # If encoded attributes are present and have the fields we expect,
        # extract them
        if failure.HasField("encoded_attributes"):
            # Clone the failure to not mutate the incoming failure
            new_failure = temporalio.api.failure.v1.Failure()
            new_failure.CopyFrom(failure)
            failure = new_failure
            try:
                encoded_attributes: dict[str, Any] = payload_converter.from_payloads(
                    [failure.encoded_attributes]
                )[0]
                if isinstance(encoded_attributes, dict):
                    message = encoded_attributes.get("message")
                    if isinstance(message, str):
                        failure.message = message
                    stack_trace = encoded_attributes.get("stack_trace")
                    if isinstance(stack_trace, str):
                        failure.stack_trace = stack_trace
            except:
                pass

        err: temporalio.exceptions.FailureError | nexusrpc.HandlerError
        match failure.WhichOneof("failure_info"):
            case "application_failure_info":
                app_info = failure.application_failure_info
                err = temporalio.exceptions.ApplicationError(
                    failure.message or "Application error",
                    *payload_converter.from_payloads_wrapper(app_info.details),
                    type=app_info.type or None,
                    non_retryable=app_info.non_retryable,
                    next_retry_delay=app_info.next_retry_delay.ToTimedelta(),
                    category=temporalio.exceptions.ApplicationErrorCategory(
                        int(app_info.category)
                    ),
                )

            case "timeout_failure_info":
                timeout_info = failure.timeout_failure_info
                err = temporalio.exceptions.TimeoutError(
                    failure.message or "Timeout",
                    type=temporalio.exceptions.TimeoutType(
                        int(timeout_info.timeout_type)
                    )
                    if timeout_info.timeout_type
                    else None,
                    last_heartbeat_details=payload_converter.from_payloads_wrapper(
                        timeout_info.last_heartbeat_details
                    ),
                )

            case "canceled_failure_info":
                cancel_info = failure.canceled_failure_info
                err = temporalio.exceptions.CancelledError(
                    failure.message or "Cancelled",
                    *payload_converter.from_payloads_wrapper(cancel_info.details),
                )
            case "terminated_failure_info":
                err = temporalio.exceptions.TerminatedError(
                    failure.message or "Terminated"
                )

            case "server_failure_info":
                server_info = failure.server_failure_info
                err = temporalio.exceptions.ServerError(
                    failure.message or "Server error",
                    non_retryable=server_info.non_retryable,
                )

            case "activity_failure_info":
                act_info = failure.activity_failure_info
                err = temporalio.exceptions.ActivityError(
                    failure.message or "Activity error",
                    scheduled_event_id=act_info.scheduled_event_id,
                    started_event_id=act_info.started_event_id,
                    identity=act_info.identity,
                    activity_type=act_info.activity_type.name,
                    activity_id=act_info.activity_id,
                    retry_state=temporalio.exceptions.RetryState(
                        int(act_info.retry_state)
                    )
                    if act_info.retry_state
                    else None,
                )

            case "child_workflow_execution_failure_info":
                child_info = failure.child_workflow_execution_failure_info
                err = temporalio.exceptions.ChildWorkflowError(
                    failure.message or "Child workflow error",
                    namespace=child_info.namespace,
                    workflow_id=child_info.workflow_execution.workflow_id,
                    run_id=child_info.workflow_execution.run_id,
                    workflow_type=child_info.workflow_type.name,
                    initiated_event_id=child_info.initiated_event_id,
                    started_event_id=child_info.started_event_id,
                    retry_state=temporalio.exceptions.RetryState(
                        int(child_info.retry_state)
                    )
                    if child_info.retry_state
                    else None,
                )

            case "nexus_handler_failure_info":
                nexus_handler_failure_info = failure.nexus_handler_failure_info
                try:
                    _type = nexusrpc.HandlerErrorType[nexus_handler_failure_info.type]
                except KeyError:
                    logger.warning(
                        f"Unknown Nexus HandlerErrorType: {nexus_handler_failure_info.type}"
                    )
                    _type = nexusrpc.HandlerErrorType.INTERNAL

                retryable_override: bool | None
                match nexus_handler_failure_info.retry_behavior:
                    case temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
                        retryable_override = True
                    case temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
                        retryable_override = False
                    case _:
                        retryable_override = None

                err = nexusrpc.HandlerError(
                    failure.message or "Nexus handler error",
                    type=_type,
                    retryable_override=retryable_override,
                    stack_trace=failure.stack_trace if failure.stack_trace else None,
                    original_failure=self._temporal_failure_to_nexus_failure(failure),
                )

            case "nexus_operation_execution_failure_info":
                nexus_op_failure_info = failure.nexus_operation_execution_failure_info
                err = temporalio.exceptions.NexusOperationError(
                    failure.message or "Nexus operation error",
                    scheduled_event_id=nexus_op_failure_info.scheduled_event_id,
                    endpoint=nexus_op_failure_info.endpoint,
                    service=nexus_op_failure_info.service,
                    operation=nexus_op_failure_info.operation,
                    operation_token=nexus_op_failure_info.operation_token,
                )

            case "reset_workflow_failure_info" | None:
                err = temporalio.exceptions.FailureError(
                    failure.message or "Failure error",
                )

        if isinstance(err, temporalio.exceptions.FailureError):
            err._failure = failure
        if failure.HasField("cause"):
            err.__cause__ = self.from_failure(failure.cause, payload_converter)
        return err


class DefaultFailureConverterWithEncodedAttributes(DefaultFailureConverter):
    """Implementation of :py:class:`DefaultFailureConverter` which moves message
    and stack trace to encoded attributes subject to a codec.
    """

    def __init__(self) -> None:
        """Create a default failure converter with encoded attributes."""
        super().__init__(encode_common_attributes=True)
