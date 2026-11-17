"""Nexus worker"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from typing import (
    Any,
    NoReturn,
    ParamSpec,
    TypeGuard,
    TypeVar,
    cast,
)

import nexusrpc.handler
from nexusrpc import LazyValue
from nexusrpc.handler import CancelOperationContext, Handler, StartOperationContext

import temporalio.api.common.v1
import temporalio.api.nexus.v1
import temporalio.bridge.proto.nexus
import temporalio.bridge.worker
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.nexus
import temporalio.nexus.system
from temporalio.bridge._visitor import PayloadVisitor
from temporalio.bridge._visitor_functions import PayloadSequence, VisitorFunctions
from temporalio.bridge.worker import PollShutdownError
from temporalio.exceptions import (
    ApplicationError,
    CancelledError,
    FailureError,
    WorkflowAlreadyStartedError,
)
from temporalio.nexus import Info, logger
from temporalio.service import RPCError, RPCStatusCode

from ._interceptor import (
    ExecuteNexusOperationCancelInput,
    ExecuteNexusOperationStartInput,
    Interceptor,
    NexusOperationInboundInterceptor,
)

_TEMPORAL_FAILURE_PROTO_TYPE = "temporal.api.failure.v1.Failure"

_PAYLOAD_VALIDATION_ERROR_TYPE = "PayloadValidationError"
""":py:attr:`temporalio.exceptions.ApplicationError.type` a data converter uses to
say that it understood a Nexus operation's input but considers it invalid.

When non-retryable, such an error is reported as a
:py:attr:`nexusrpc.HandlerErrorType.BAD_REQUEST` handler error rather than as a
handler-side :py:attr:`nexusrpc.HandlerErrorType.INTERNAL` error.
"""


@dataclass
class _RunningNexusTask:
    task: asyncio.Task[Any]
    cancellation: _NexusTaskCancellation

    def cancel(self, reason: str):
        self.cancellation.cancel(reason)
        self.task.cancel()


class _NexusWorker:  # type:ignore[reportUnusedClass]
    def __init__(
        self,
        *,
        bridge_worker: Callable[[], temporalio.bridge.worker.Worker],
        client: temporalio.client.Client,
        namespace: str,
        task_queue: str,
        service_handlers: Sequence[Any],
        data_converter: temporalio.converter.DataConverter,
        interceptors: Sequence[Interceptor],
        metric_meter: temporalio.common.MetricMeter,
        executor: concurrent.futures.ThreadPoolExecutor | None,
    ) -> None:
        self._bridge_worker = bridge_worker
        self._client = client
        self._namespace = namespace
        self._task_queue = task_queue

        self._metric_meter = metric_meter
        middleware = _NexusMiddlewareForInterceptors(interceptors)

        # If an executor is provided, we wrap the executor with one that will
        # copy the contextvars.Context to the thread on submit
        handler_executor = _ContextPropagatingExecutor(executor) if executor else None
        self._handler = Handler(
            service_handlers, handler_executor, middleware=[middleware]
        )

        self._data_converter = data_converter

        self._running_tasks: dict[bytes, _RunningNexusTask] = {}
        self._fail_worker_exception_queue: asyncio.Queue[Exception] = asyncio.Queue()
        self._worker_shutdown_event: temporalio.common._CompositeEvent | None = None

    async def run(self) -> None:
        """Continually poll for Nexus tasks and dispatch to handlers."""

        async def raise_from_exception_queue() -> NoReturn:
            raise await self._fail_worker_exception_queue.get()

        exception_task = asyncio.create_task(raise_from_exception_queue())

        while True:
            try:
                poll_task = asyncio.create_task(self._bridge_worker().poll_nexus_task())
                await asyncio.wait(
                    [poll_task, exception_task], return_when=asyncio.FIRST_COMPLETED
                )
                if exception_task.done():
                    poll_task.cancel()
                    await exception_task
                nexus_task = await poll_task

                if nexus_task.HasField("task"):
                    task = nexus_task.task
                    request_deadline = (
                        nexus_task.request_deadline.ToDatetime().replace(
                            tzinfo=timezone.utc
                        )
                        if nexus_task.HasField("request_deadline")
                        else None
                    )
                    if task.request.HasField("start_operation"):
                        task_cancellation = _NexusTaskCancellation()
                        start_op_task = asyncio.create_task(
                            self._handle_start_operation_task(
                                task_token=task.task_token,
                                start_request=task.request.start_operation,
                                headers=dict(task.request.header),
                                task_cancellation=task_cancellation,
                                request_deadline=request_deadline,
                                endpoint=nexus_task.endpoint,
                            )
                        )
                        self._running_tasks[task.task_token] = _RunningNexusTask(
                            start_op_task, task_cancellation
                        )
                    elif task.request.HasField("cancel_operation"):
                        task_cancellation = _NexusTaskCancellation()
                        cancel_op_task = asyncio.create_task(
                            self._handle_cancel_operation_task(
                                task_token=task.task_token,
                                request=task.request.cancel_operation,
                                headers=dict(task.request.header),
                                task_cancellation=task_cancellation,
                                request_deadline=request_deadline,
                                endpoint=nexus_task.endpoint,
                            )
                        )
                        self._running_tasks[task.task_token] = _RunningNexusTask(
                            cancel_op_task, task_cancellation
                        )
                    else:
                        raise NotImplementedError(
                            f"Invalid Nexus task request: {task.request}"
                        )
                elif nexus_task.HasField("cancel_task"):
                    if running_task := self._running_tasks.get(
                        nexus_task.cancel_task.task_token
                    ):
                        reason = (
                            temporalio.bridge.proto.nexus.NexusTaskCancelReason.Name(
                                nexus_task.cancel_task.reason
                            )
                        )
                        running_task.cancel(reason)
                    else:
                        logger.debug(
                            f"Received cancel_task but no running task exists for "
                            f"task token: {nexus_task.cancel_task.task_token.decode()}"
                        )
                else:
                    raise NotImplementedError(f"Invalid Nexus task: {nexus_task}")

            except PollShutdownError:
                exception_task.cancel()
                return

            except Exception as err:
                raise RuntimeError("Nexus worker failed") from err

    def notify_shutdown(self) -> None:
        if self._worker_shutdown_event:
            self._worker_shutdown_event.set()

    # Only call this if run() raised an error
    async def drain_poll_queue(self) -> None:
        while True:
            try:
                # Take all tasks and say we can't handle them
                task = await self._bridge_worker().poll_nexus_task()
                completion = temporalio.bridge.proto.nexus.NexusTaskCompletion(
                    task_token=task.task.task_token
                )
                completion.error.failure.message = "Worker shutting down"
                await self._bridge_worker().complete_nexus_task(completion)
            except PollShutdownError:
                return

    # Only call this after run()/drain_poll_queue() have returned. This will not
    # raise an exception.
    async def wait_all_completed(self) -> None:
        running_tasks = [
            running_task.task for running_task in self._running_tasks.values()
        ]
        await asyncio.gather(*running_tasks, return_exceptions=True)

    # Task completion should never be dropped in case of cancellation.
    # The Rust future in core must complete for shutdown to happen without
    # hanging.
    async def _complete_task(
        self, completion: temporalio.bridge.proto.nexus.NexusTaskCompletion
    ):
        await asyncio.shield(self._bridge_worker().complete_nexus_task(completion))

    async def _encode_completion(
        self, completion: temporalio.bridge.proto.nexus.NexusTaskCompletion
    ) -> None:
        """Apply the payload codec then external storage to the completion's payloads."""
        dc = self._data_converter
        await PayloadVisitor(skip_search_attributes=True, skip_headers=True).visit(
            _PayloadTransformVisitor(dc._encode_payload_sequence), completion
        )
        await PayloadVisitor(skip_search_attributes=True).visit(
            _PayloadTransformVisitor(dc._external_store_payload_sequence),
            completion,
        )

    # TODO(nexus-preview): stack trace pruning. See sdk-typescript NexusHandler.execute
    # "Any call up to this function and including this one will be trimmed out of stack traces.""

    async def _handle_cancel_operation_task(
        self,
        task_token: bytes,
        request: temporalio.api.nexus.v1.CancelOperationRequest,
        headers: Mapping[str, str],
        task_cancellation: nexusrpc.handler.OperationTaskCancellation,
        request_deadline: datetime | None,
        endpoint: str,
    ) -> None:
        """Handle a cancel operation task.

        Attempt to execute the user cancel_operation method. Handle errors and send the
        task completion.
        """
        # Create the worker shutdown event if not created
        if not self._worker_shutdown_event:
            self._worker_shutdown_event = temporalio.common._CompositeEvent(
                thread_event=threading.Event(), async_event=asyncio.Event()
            )
        # TODO(nexus-prerelease): headers
        ctx = CancelOperationContext(
            service=request.service,
            operation=request.operation,
            headers=headers,
            task_cancellation=task_cancellation,
            request_deadline=request_deadline,
        )
        temporalio.nexus._operation_context._TemporalCancelOperationContext(
            info=lambda: Info(
                endpoint=endpoint,
                namespace=self._namespace,
                task_queue=self._task_queue,
            ),
            nexus_context=ctx,
            client=self._client,
            _runtime_metric_meter=self._metric_meter,
            _worker_shutdown_event=self._worker_shutdown_event,
        ).set()
        try:
            try:
                await self._handler.cancel_operation(ctx, request.operation_token)
                completion = temporalio.bridge.proto.nexus.NexusTaskCompletion(
                    task_token=task_token,
                    completed=temporalio.api.nexus.v1.Response(
                        cancel_operation=temporalio.api.nexus.v1.CancelOperationResponse()
                    ),
                )
                # No-op but keeps the cancel covered if it ever carries a payload.
                await self._encode_completion(completion)
            except asyncio.CancelledError:
                completion = temporalio.bridge.proto.nexus.NexusTaskCompletion(
                    task_token=task_token,
                    ack_cancel=task_cancellation.is_cancelled(),
                )
            except BaseException as err:
                logger.warning("Failed to execute Nexus cancel operation method")
                handler_error = _exception_to_handler_error(err)
                completion = temporalio.bridge.proto.nexus.NexusTaskCompletion(
                    task_token=task_token,
                )
                self._data_converter.failure_converter.to_failure(
                    handler_error,
                    self._data_converter.payload_converter,
                    completion.failure,
                )
                await self._encode_completion(completion)
            await self._complete_task(completion)
        except Exception:
            logger.exception("Failed to send Nexus task completion")
        finally:
            try:
                del self._running_tasks[task_token]
            except KeyError:
                logger.exception(
                    "Failed to remove task for completed Nexus cancel operation"
                )

    async def _handle_start_operation_task(
        self,
        task_token: bytes,
        start_request: temporalio.api.nexus.v1.StartOperationRequest,
        headers: Mapping[str, str],
        task_cancellation: nexusrpc.handler.OperationTaskCancellation,
        request_deadline: datetime | None,
        endpoint: str,
    ) -> None:
        """Handle a start operation task.

        Attempt to execute the user start_operation method and invoke the data converter
        on the result. Handle errors and send the task completion.
        """
        try:
            try:
                start_response = await self._start_operation(
                    start_request,
                    headers,
                    task_cancellation,
                    request_deadline,
                    endpoint,
                )
                completion = temporalio.bridge.proto.nexus.NexusTaskCompletion(
                    task_token=task_token,
                    completed=temporalio.api.nexus.v1.Response(
                        start_operation=start_response
                    ),
                )
                await self._encode_completion(completion)
            except asyncio.CancelledError:
                completion = temporalio.bridge.proto.nexus.NexusTaskCompletion(
                    task_token=task_token,
                    ack_cancel=task_cancellation.is_cancelled(),
                )
            except BaseException as err:
                logger.warning("Failed to execute Nexus start operation method")
                completion = temporalio.bridge.proto.nexus.NexusTaskCompletion(
                    task_token=task_token,
                )
                handler_error = _exception_to_handler_error(err)
                self._data_converter.failure_converter.to_failure(
                    handler_error,
                    self._data_converter.payload_converter,
                    completion.failure,
                )

                if isinstance(err, concurrent.futures.BrokenExecutor):
                    self._fail_worker_exception_queue.put_nowait(err)
                await self._encode_completion(completion)

            await self._complete_task(completion)
        except Exception:
            logger.exception("Failed to send Nexus task completion")
        finally:
            try:
                del self._running_tasks[task_token]
            except KeyError:
                logger.exception(
                    "Failed to remove task for completed Nexus start operation"
                )

    async def _start_operation(
        self,
        start_request: temporalio.api.nexus.v1.StartOperationRequest,
        headers: Mapping[str, str],
        cancellation: nexusrpc.handler.OperationTaskCancellation,
        request_deadline: datetime | None,
        endpoint: str,
    ) -> temporalio.api.nexus.v1.StartOperationResponse:
        """Invoke the Nexus handler's start_operation method and construct the StartOperationResponse.

        OperationError is handled by this function, since it results in a StartOperationResponse.

        All other exceptions are handled by a caller of this function.
        """
        # Create the worker shutdown event if not created
        if not self._worker_shutdown_event:
            self._worker_shutdown_event = temporalio.common._CompositeEvent(
                thread_event=threading.Event(), async_event=asyncio.Event()
            )
        ctx = StartOperationContext(
            service=start_request.service,
            operation=start_request.operation,
            headers=headers,
            request_id=start_request.request_id,
            callback_url=start_request.callback,
            inbound_links=[
                nexusrpc.Link(url=link.url, type=link.type)
                for link in start_request.links
            ],
            callback_headers=dict(start_request.callback_header),
            task_cancellation=cancellation,
            request_deadline=request_deadline,
        )
        temporalio.nexus._operation_context._TemporalStartOperationContext(
            nexus_context=ctx,
            client=self._client,
            info=lambda: Info(
                endpoint=endpoint,
                namespace=self._namespace,
                task_queue=self._task_queue,
            ),
            _runtime_metric_meter=self._metric_meter,
            _worker_shutdown_event=self._worker_shutdown_event,
        ).set()
        input = LazyValue(
            serializer=_NexusPayloadSerializer(
                data_converter=self._data_converter,
                payload=start_request.payload,
            ),
            headers={},
            stream=None,
        )
        try:
            result = await self._handler.start_operation(ctx, input)
            links = [
                temporalio.api.nexus.v1.Link(url=link.url, type=link.type)
                for link in ctx.outbound_links
            ]
            if isinstance(result, nexusrpc.handler.StartOperationResultAsync):
                return temporalio.api.nexus.v1.StartOperationResponse(
                    async_success=temporalio.api.nexus.v1.StartOperationResponse.Async(
                        operation_token=result.token,
                        links=links,
                    )
                )
            elif isinstance(result, nexusrpc.handler.StartOperationResultSync):
                [payload] = self._data_converter.payload_converter.to_payloads(
                    [result.value]
                )
                return temporalio.api.nexus.v1.StartOperationResponse(
                    sync_success=temporalio.api.nexus.v1.StartOperationResponse.Sync(
                        payload=payload,
                        links=links,
                    )
                )
            else:
                raise _exception_to_handler_error(
                    TypeError(
                        "Operation start method must return either "
                        "nexusrpc.handler.StartOperationResultSync or "
                        "nexusrpc.handler.StartOperationResultAsync."
                    )
                )
        except nexusrpc.OperationError as err:
            # Convert OperationError to a Temporal failure
            try:
                match err.state:
                    case nexusrpc.OperationErrorState.CANCELED:
                        raise CancelledError(err.message) from err.__cause__
                    case nexusrpc.OperationErrorState.FAILED:
                        raise ApplicationError(
                            message=err.message,
                            type="OperationError",
                            non_retryable=True,
                        ) from err.__cause__
            except FailureError as new_err:
                response = temporalio.api.nexus.v1.StartOperationResponse()
                self._data_converter.failure_converter.to_failure(
                    new_err,
                    self._data_converter.payload_converter,
                    response.failure,
                )
                return response


class _PayloadTransformVisitor(VisitorFunctions):
    """Adapts a payload-sequence transform for use with :class:`PayloadVisitor`."""

    def __init__(
        self,
        f: Callable[
            [Sequence[temporalio.api.common.v1.Payload]],
            Awaitable[list[temporalio.api.common.v1.Payload]],
        ],
    ) -> None:
        self._f = f

    async def visit_payload(self, payload: temporalio.api.common.v1.Payload) -> None:
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


def _is_payload_validation_error(err: BaseException) -> TypeGuard[ApplicationError]:
    """Whether err is a non-retryable :py:class:`ApplicationError` whose type is
    exactly :py:data:`_PAYLOAD_VALIDATION_ERROR_TYPE`.
    """
    return (
        isinstance(err, ApplicationError)
        and err.non_retryable
        and err.type == _PAYLOAD_VALIDATION_ERROR_TYPE
    )


@dataclass
class _NexusPayloadSerializer:
    data_converter: temporalio.converter.DataConverter
    payload: temporalio.api.common.v1.Payload

    async def serialize(self, value: Any) -> nexusrpc.Content:  # type:ignore[reportUnusedParameter]
        raise NotImplementedError(
            "The serialize method of the Serializer is not used by handlers"
        )

    async def deserialize(
        self,
        content: nexusrpc.Content,  # type:ignore[reportUnusedParameter]
        as_type: type[Any] | None = None,
    ) -> Any:
        dc = self.data_converter
        # The visitor mutates in place, so work on a copy to leave the request
        # payload untouched.
        payload = temporalio.api.common.v1.Payload()
        payload.CopyFrom(self.payload)
        try:
            await PayloadVisitor(skip_search_attributes=True).visit(
                _PayloadTransformVisitor(dc._external_retrieve_payload_sequence),
                payload,
            )
        except Exception as err:
            raise nexusrpc.HandlerError(
                "Failed to retrieve Nexus operation input from external storage",
                type=nexusrpc.HandlerErrorType.INTERNAL,
                retryable_override=True,
            ) from err

        try:
            await PayloadVisitor(skip_search_attributes=True, skip_headers=True).visit(
                _PayloadTransformVisitor(dc._decode_payload_sequence), payload
            )
        except Exception as err:
            if _is_payload_validation_error(err):
                # The data converter decoded the input and rejected it, so this
                # is the caller's fault rather than a handler-side error.
                raise nexusrpc.HandlerError(
                    "Invalid operation input",
                    type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                    retryable_override=False,
                ) from err
            raise nexusrpc.HandlerError(
                "Payload codec failed to decode Nexus operation input",
                type=nexusrpc.HandlerErrorType.INTERNAL,
            ) from err

        try:
            payload_converter = dc.payload_converter
            if temporalio.nexus.system._is_system_payload(payload):
                payload_converter = temporalio.nexus.system._get_payload_converter(
                    dc.payload_converter,
                    dc.failure_converter,
                )
            [input] = payload_converter.from_payloads(
                [payload],
                type_hints=[as_type] if as_type else None,
            )
            return input
        except Exception as err:
            if _is_payload_validation_error(err):
                # The data converter converted the input and rejected it, so
                # distinguish it from input that will never decode into the
                # expected type.
                raise nexusrpc.HandlerError(
                    "Invalid operation input",
                    type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                    retryable_override=False,
                ) from err
            raise nexusrpc.HandlerError(
                "Payload converter failed to decode Nexus operation input",
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                retryable_override=False,
            ) from err


def _exception_to_handler_error(err: BaseException) -> nexusrpc.HandlerError:
    # Based on sdk-typescript's convertKnownErrors:
    # https://github.com/temporalio/sdk-typescript/blob/nexus/packages/worker/src/nexus.ts
    if isinstance(err, nexusrpc.HandlerError):
        return err
    elif isinstance(err, ApplicationError):
        handler_err = nexusrpc.HandlerError(
            message="Handler failed with non-retryable application error",
            type=nexusrpc.HandlerErrorType.INTERNAL,
            retryable_override=not err.non_retryable,
        )
    elif isinstance(err, WorkflowAlreadyStartedError):
        handler_err = nexusrpc.HandlerError(
            err.message,
            type=nexusrpc.HandlerErrorType.INTERNAL,
            retryable_override=False,
        )
    elif isinstance(err, RPCError):
        if err.status == RPCStatusCode.INVALID_ARGUMENT:
            handler_err = nexusrpc.HandlerError(
                err.message,
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
            )
        elif err.status in [
            RPCStatusCode.ALREADY_EXISTS,
            RPCStatusCode.FAILED_PRECONDITION,
            RPCStatusCode.OUT_OF_RANGE,
        ]:
            handler_err = nexusrpc.HandlerError(
                err.message,
                type=nexusrpc.HandlerErrorType.INTERNAL,
                retryable_override=False,
            )
        elif err.status in [RPCStatusCode.ABORTED, RPCStatusCode.UNAVAILABLE]:
            handler_err = nexusrpc.HandlerError(
                err.message,
                type=nexusrpc.HandlerErrorType.UNAVAILABLE,
            )
        elif err.status in [
            RPCStatusCode.CANCELLED,
            RPCStatusCode.DATA_LOSS,
            RPCStatusCode.INTERNAL,
            RPCStatusCode.UNKNOWN,
            RPCStatusCode.UNAUTHENTICATED,
            RPCStatusCode.PERMISSION_DENIED,
        ]:
            # Note that UNAUTHENTICATED and PERMISSION_DENIED have Nexus error types but
            # we convert to internal because this is not a client auth error and happens
            # when the handler fails to auth with Temporal and should be considered
            # retryable.
            handler_err = nexusrpc.HandlerError(
                err.message, type=nexusrpc.HandlerErrorType.INTERNAL
            )
        elif err.status == RPCStatusCode.NOT_FOUND:
            handler_err = nexusrpc.HandlerError(
                err.message, type=nexusrpc.HandlerErrorType.NOT_FOUND
            )
        elif err.status == RPCStatusCode.RESOURCE_EXHAUSTED:
            handler_err = nexusrpc.HandlerError(
                err.message,
                type=nexusrpc.HandlerErrorType.RESOURCE_EXHAUSTED,
            )
        elif err.status == RPCStatusCode.UNIMPLEMENTED:
            handler_err = nexusrpc.HandlerError(
                err.message,
                type=nexusrpc.HandlerErrorType.NOT_IMPLEMENTED,
            )
        elif err.status == RPCStatusCode.DEADLINE_EXCEEDED:
            handler_err = nexusrpc.HandlerError(
                err.message,
                type=nexusrpc.HandlerErrorType.UPSTREAM_TIMEOUT,
            )
        else:
            handler_err = nexusrpc.HandlerError(
                f"Unhandled RPC error status: {err.status}",
                type=nexusrpc.HandlerErrorType.INTERNAL,
            )
    else:
        handler_err = nexusrpc.HandlerError(
            "Internal handler error", type=nexusrpc.HandlerErrorType.INTERNAL
        )
    handler_err.__cause__ = err
    return handler_err


class _NexusTaskCancellation(nexusrpc.handler.OperationTaskCancellation):
    def __init__(self):
        self._thread_evt = threading.Event()
        self._async_evt = asyncio.Event()
        self._lock = threading.Lock()
        self._reason: str | None = None

    def is_cancelled(self) -> bool:
        return self._thread_evt.is_set()

    def cancellation_reason(self) -> str | None:
        with self._lock:
            return self._reason

    def wait_until_cancelled_sync(self, timeout: float | None = None) -> bool:
        return self._thread_evt.wait(timeout)

    async def wait_until_cancelled(self) -> None:
        await self._async_evt.wait()

    def cancel(self, reason: str) -> bool:
        with self._lock:
            if self._thread_evt.is_set():
                return False
            self._reason = reason
            self._thread_evt.set()
            self._async_evt.set()
            return True


class _NexusOperationHandlerForInterceptor(
    nexusrpc.handler.MiddlewareSafeOperationHandler
):
    def __init__(self, next_interceptor: NexusOperationInboundInterceptor):
        self._next_interceptor = next_interceptor

    async def start(
        self, ctx: nexusrpc.handler.StartOperationContext, input: Any
    ) -> (
        nexusrpc.handler.StartOperationResultSync[Any]
        | nexusrpc.handler.StartOperationResultAsync
    ):
        return await self._next_interceptor.execute_nexus_operation_start(
            ExecuteNexusOperationStartInput(ctx, input)
        )

    async def cancel(
        self, ctx: nexusrpc.handler.CancelOperationContext, token: str
    ) -> None:
        return await self._next_interceptor.execute_nexus_operation_cancel(
            ExecuteNexusOperationCancelInput(ctx, token)
        )


class _NexusOperationInboundInterceptorImpl(NexusOperationInboundInterceptor):
    def __init__(self, handler: nexusrpc.handler.MiddlewareSafeOperationHandler):  # pyright: ignore[reportMissingSuperCall]
        self._handler = handler

    async def execute_nexus_operation_start(
        self, input: ExecuteNexusOperationStartInput
    ) -> (
        nexusrpc.handler.StartOperationResultSync[Any]
        | nexusrpc.handler.StartOperationResultAsync
    ):
        return await self._handler.start(input.ctx, input.input)

    async def execute_nexus_operation_cancel(
        self, input: ExecuteNexusOperationCancelInput
    ) -> None:
        return await self._handler.cancel(input.ctx, input.token)


class _NexusMiddlewareForInterceptors(nexusrpc.handler.OperationHandlerMiddleware):
    def __init__(self, interceptors: Sequence[Interceptor]) -> None:
        self._interceptors = interceptors

    def intercept(
        self,
        ctx: nexusrpc.handler.OperationContext,
        next: nexusrpc.handler.MiddlewareSafeOperationHandler,
    ) -> nexusrpc.handler.MiddlewareSafeOperationHandler:
        inbound = reduce(
            lambda impl, _next: _next.intercept_nexus_operation(impl),
            reversed(self._interceptors),
            cast(
                NexusOperationInboundInterceptor,
                _NexusOperationInboundInterceptorImpl(next),
            ),
        )

        return _NexusOperationHandlerForInterceptor(inbound)


_P = ParamSpec("_P")
_T = TypeVar("_T")


class _ContextPropagatingExecutor(concurrent.futures.Executor):
    def __init__(self, executor: concurrent.futures.ThreadPoolExecutor) -> None:
        self._executor = executor

    def submit(
        self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> concurrent.futures.Future[_T]:
        ctx = contextvars.copy_context()

        def wrapped(*a: _P.args, **k: _P.kwargs) -> _T:
            return ctx.run(fn, *a, **k)

        return self._executor.submit(wrapped, *args, **kwargs)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        return self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
