"""OpenTelemetry interceptor that creates/propagates spans."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    Type,
    cast,
)

import opentelemetry.baggage.propagation
import opentelemetry.context
import opentelemetry.context.context
import opentelemetry.propagators.composite
import opentelemetry.propagators.textmap
import opentelemetry.trace
import opentelemetry.trace.propagation.tracecontext
import opentelemetry.util.types
from typing_extensions import Protocol, TypeAlias, TypedDict

import temporalio.activity
import temporalio.api.common.v1
import temporalio.client
import temporalio.converter
import temporalio.exceptions
import temporalio.worker
import temporalio.workflow

# OpenTelemetry dynamically, lazily chooses its context implementation at
# runtime. When first accessed, they use pkg_resources.iter_entry_points + load.
# The load uses built-in open() which we don't allow in sandbox mode at runtime,
# only import time. Therefore if the first use of a OTel context is inside the
# sandbox, which it may be for a workflow worker, this will fail. So instead we
# eagerly reference it here to force loading at import time instead of lazily.
opentelemetry.context.get_current()

default_text_map_propagator = opentelemetry.propagators.composite.CompositePropagator(
    [
        opentelemetry.trace.propagation.tracecontext.TraceContextTextMapPropagator(),
        opentelemetry.baggage.propagation.W3CBaggagePropagator(),
    ]
)
"""Default text map propagator used by :py:class:`TracingInterceptor`."""

_CarrierDict: TypeAlias = Dict[str, opentelemetry.propagators.textmap.CarrierValT]


class TracingInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    """Interceptor that supports client and worker OpenTelemetry span creation
    and propagation.

    This should be created and used for ``interceptors`` on the
    :py:meth:`temporalio.client.Client.connect` call to apply to all client
    calls and worker calls using that client. To only apply to workers, set as
    worker creation option instead of in client.

    To customize the header key, text map propagator, or payload converter, a
    subclass of this and :py:class:`TracingWorkflowInboundInterceptor` should be
    created. In addition to customizing those attributes, the subclass of this
    class should return the workflow interceptor subclass from
    :py:meth:`workflow_interceptor_class`. That subclass should also set the
    custom attributes desired.
    """

    def __init__(  # type: ignore[reportMissingSuperCall]
        self,
        tracer: Optional[opentelemetry.trace.Tracer] = None,
        *,
        always_create_workflow_spans: bool = False,
    ) -> None:
        """Initialize a OpenTelemetry tracing interceptor.

        Args:
            tracer: The tracer to use. Defaults to
                :py:func:`opentelemetry.trace.get_tracer`.
            always_create_workflow_spans: When false, the default, spans are
                only created in workflows when an overarching span from the
                client is present. In cases of starting a workflow elsewhere,
                e.g. CLI or schedules, a client-created span is not present and
                workflow spans will not be created. Setting this to true will
                create spans in workflows no matter what, but there is a risk of
                them being orphans since they may not have a parent span after
                replaying.
        """
        self.tracer = tracer or opentelemetry.trace.get_tracer(__name__)
        # To customize any of this, users must subclass. We intentionally don't
        # accept this in the constructor because if they're customizing these
        # values, they'd also need to do it on the workflow side via subclassing
        # on that interceptor since they can't accept custom constructor values.
        self.header_key: str = "_tracer-data"
        self.text_map_propagator: opentelemetry.propagators.textmap.TextMapPropagator = default_text_map_propagator
        # TODO(cretz): Should I be using the configured one at the client and activity level?
        self.payload_converter = temporalio.converter.PayloadConverter.default
        self._always_create_workflow_spans = always_create_workflow_spans

    def intercept_client(
        self, next: temporalio.client.OutboundInterceptor
    ) -> temporalio.client.OutboundInterceptor:
        """Implementation of
        :py:meth:`temporalio.client.Interceptor.intercept_client`.
        """
        return _TracingClientOutboundInterceptor(next, self)

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        """Implementation of
        :py:meth:`temporalio.worker.Interceptor.intercept_activity`.
        """
        return _TracingActivityInboundInterceptor(next, self)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> Type[TracingWorkflowInboundInterceptor]:
        """Implementation of
        :py:meth:`temporalio.worker.Interceptor.workflow_interceptor_class`.
        """
        # Set the externs needed
        input.unsafe_extern_functions.update(
            {
                "__temporal_opentelemetry_completed_span": self._completed_workflow_span,
            }
        )
        return TracingWorkflowInboundInterceptor

    def _context_to_headers(
        self, headers: Mapping[str, temporalio.api.common.v1.Payload]
    ) -> Mapping[str, temporalio.api.common.v1.Payload]:
        carrier: _CarrierDict = {}
        self.text_map_propagator.inject(carrier)
        if carrier:
            headers = {
                **headers,
                self.header_key: self.payload_converter.to_payloads([carrier])[0],
            }
        return headers

    def _context_from_headers(
        self, headers: Mapping[str, temporalio.api.common.v1.Payload]
    ) -> Optional[opentelemetry.context.context.Context]:
        if self.header_key not in headers:
            return None
        header_payload = headers.get(self.header_key)
        if not header_payload:
            return None
        carrier: _CarrierDict = self.payload_converter.from_payloads([header_payload])[
            0
        ]
        if not carrier:
            return None
        return self.text_map_propagator.extract(carrier)

    @contextmanager
    def _start_as_current_span(
        self,
        name: str,
        *,
        attributes: opentelemetry.util.types.Attributes,
        input: Optional[_InputWithHeaders] = None,
        kind: opentelemetry.trace.SpanKind,
    ) -> Iterator[None]:
        with self.tracer.start_as_current_span(name, attributes=attributes, kind=kind):
            if input:
                input.headers = self._context_to_headers(input.headers)
            yield None

    def _completed_workflow_span(
        self, params: _CompletedWorkflowSpanParams
    ) -> Optional[_CarrierDict]:
        # Carrier to context, start span, set span as current on context,
        # context back to carrier

        # If the parent is missing and user hasn't said to always create, do not
        # create
        if params.parent_missing and not self._always_create_workflow_spans:
            return None

        # Extract the context
        context = self.text_map_propagator.extract(params.context)
        # Create link if there is a span present
        links: Optional[Sequence[opentelemetry.trace.Link]] = []
        if params.link_context:
            link_span = opentelemetry.trace.get_current_span(
                self.text_map_propagator.extract(params.link_context)
            )
            if link_span is not opentelemetry.trace.INVALID_SPAN:
                links = [opentelemetry.trace.Link(link_span.get_span_context())]

        # We start and end the span immediately because it is not replay-safe to
        # keep an unended long-running span. We set the end time the same as the
        # start time to make it clear it has no duration.
        span = self.tracer.start_span(
            params.name,
            context,
            attributes=params.attributes,
            links=links,
            start_time=params.time_ns,
            kind=params.kind,
        )
        context = opentelemetry.trace.set_span_in_context(span, context)
        if params.exception:
            span.record_exception(params.exception)
        span.end(end_time=params.time_ns)
        # Back to carrier
        carrier: _CarrierDict = {}
        self.text_map_propagator.inject(carrier, context)
        return carrier


class _TracingClientOutboundInterceptor(temporalio.client.OutboundInterceptor):
    def __init__(
        self, next: temporalio.client.OutboundInterceptor, root: TracingInterceptor
    ) -> None:
        super().__init__(next)
        self.root = root

    async def start_workflow(
        self, input: temporalio.client.StartWorkflowInput
    ) -> temporalio.client.WorkflowHandle[Any, Any]:
        prefix = (
            "StartWorkflow" if not input.start_signal else "SignalWithStartWorkflow"
        )
        with self.root._start_as_current_span(
            f"{prefix}:{input.workflow}",
            attributes={"temporalWorkflowID": input.id},
            input=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        ):
            return await super().start_workflow(input)

    async def query_workflow(self, input: temporalio.client.QueryWorkflowInput) -> Any:
        with self.root._start_as_current_span(
            f"QueryWorkflow:{input.query}",
            attributes={"temporalWorkflowID": input.id},
            input=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        ):
            return await super().query_workflow(input)

    async def signal_workflow(
        self, input: temporalio.client.SignalWorkflowInput
    ) -> None:
        with self.root._start_as_current_span(
            f"SignalWorkflow:{input.signal}",
            attributes={"temporalWorkflowID": input.id},
            input=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        ):
            return await super().signal_workflow(input)

    async def start_workflow_update(
        self, input: temporalio.client.StartWorkflowUpdateInput
    ) -> temporalio.client.WorkflowUpdateHandle[Any]:
        with self.root._start_as_current_span(
            f"StartWorkflowUpdate:{input.update}",
            attributes={"temporalWorkflowID": input.id},
            input=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        ):
            return await super().start_workflow_update(input)


class _TracingActivityInboundInterceptor(temporalio.worker.ActivityInboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
        root: TracingInterceptor,
    ) -> None:
        super().__init__(next)
        self.root = root

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        info = temporalio.activity.info()
        with self.root.tracer.start_as_current_span(
            f"RunActivity:{info.activity_type}",
            context=self.root._context_from_headers(input.headers),
            attributes={
                "temporalWorkflowID": info.workflow_id,
                "temporalRunID": info.workflow_run_id,
                "temporalActivityID": info.activity_id,
            },
            kind=opentelemetry.trace.SpanKind.SERVER,
        ):
            return await super().execute_activity(input)


class _InputWithHeaders(Protocol):
    headers: Mapping[str, temporalio.api.common.v1.Payload]


class _WorkflowExternFunctions(TypedDict):
    __temporal_opentelemetry_completed_span: Callable[
        [_CompletedWorkflowSpanParams], Optional[_CarrierDict]
    ]


@dataclass(frozen=True)
class _CompletedWorkflowSpanParams:
    context: _CarrierDict
    name: str
    attributes: opentelemetry.util.types.Attributes
    time_ns: int
    link_context: Optional[_CarrierDict]
    exception: Optional[Exception]
    kind: opentelemetry.trace.SpanKind
    parent_missing: bool


_interceptor_context_key = opentelemetry.context.create_key(
    "__temporal_opentelemetry_workflow_interceptor"
)


class TracingWorkflowInboundInterceptor(temporalio.worker.WorkflowInboundInterceptor):
    """Tracing interceptor for workflow calls.

    See :py:class:`TracingInterceptor` docs on why one might want to subclass
    this class.
    """

    @staticmethod
    def _from_context() -> Optional[TracingWorkflowInboundInterceptor]:
        ret = opentelemetry.context.get_value(_interceptor_context_key)
        if ret and isinstance(ret, TracingWorkflowInboundInterceptor):
            return ret
        return None

    def __init__(self, next: temporalio.worker.WorkflowInboundInterceptor) -> None:
        """Initialize a tracing workflow interceptor."""
        super().__init__(next)
        self._extern_functions = cast(
            _WorkflowExternFunctions, temporalio.workflow.extern_functions()
        )
        # To customize these, like the primary tracing interceptor, subclassing
        # must be used
        self.header_key: str = "_tracer-data"
        self.text_map_propagator: opentelemetry.propagators.textmap.TextMapPropagator = default_text_map_propagator
        # TODO(cretz): Should I be using the configured one for this workflow?
        self.payload_converter = temporalio.converter.PayloadConverter.default
        # This is the context for the overall workflow, lazily created
        self._workflow_context_carrier: Optional[_CarrierDict] = None

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        """Implementation of
        :py:meth:`temporalio.worker.WorkflowInboundInterceptor.init`.
        """
        super().init(_TracingWorkflowOutboundInterceptor(outbound, self))

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        """Implementation of
        :py:meth:`temporalio.worker.WorkflowInboundInterceptor.execute_workflow`.
        """
        with self._top_level_workflow_context(success_is_complete=True):
            # Entrypoint of workflow should be `server` in OTel
            self._completed_span(
                f"RunWorkflow:{temporalio.workflow.info().workflow_type}",
                kind=opentelemetry.trace.SpanKind.SERVER,
            )
            return await super().execute_workflow(input)

    async def handle_signal(self, input: temporalio.worker.HandleSignalInput) -> None:
        """Implementation of
        :py:meth:`temporalio.worker.WorkflowInboundInterceptor.handle_signal`.
        """
        # Create a span in the current context for the signal and link any
        # header given
        link_context_header = input.headers.get(self.header_key)
        link_context_carrier: Optional[_CarrierDict] = None
        if link_context_header:
            link_context_carrier = self.payload_converter.from_payloads(
                [link_context_header]
            )[0]
        with self._top_level_workflow_context(success_is_complete=False):
            self._completed_span(
                f"HandleSignal:{input.signal}",
                link_context_carrier=link_context_carrier,
                kind=opentelemetry.trace.SpanKind.SERVER,
            )
            await super().handle_signal(input)

    async def handle_query(self, input: temporalio.worker.HandleQueryInput) -> Any:
        """Implementation of
        :py:meth:`temporalio.worker.WorkflowInboundInterceptor.handle_query`.
        """
        # Only trace this if there is a header, and make that span the parent.
        # We do not put anything that happens in a query handler on the workflow
        # span.
        context_header = input.headers.get(self.header_key)
        context: opentelemetry.context.Context
        link_context_carrier: Optional[_CarrierDict] = None
        if context_header:
            context_carrier = self.payload_converter.from_payloads([context_header])[0]
            context = self.text_map_propagator.extract(context_carrier)
            # If there is a workflow span, use it as the link
            link_context_carrier = self._load_workflow_context_carrier()
        else:
            # Use an empty context
            context = opentelemetry.context.Context()

        # We need to put this interceptor on the context too
        context = self._set_on_context(context)
        # Run under context with new span
        token = opentelemetry.context.attach(context)
        try:
            # This won't be created if there was no context header
            self._completed_span(
                f"HandleQuery:{input.query}",
                link_context_carrier=link_context_carrier,
                # Create even on replay for queries
                new_span_even_on_replay=True,
                kind=opentelemetry.trace.SpanKind.SERVER,
            )
            return await super().handle_query(input)
        finally:
            opentelemetry.context.detach(token)

    def handle_update_validator(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> None:
        """Implementation of
        :py:meth:`temporalio.worker.WorkflowInboundInterceptor.handle_update_validator`.
        """
        link_context_header = input.headers.get(self.header_key)
        link_context_carrier: Optional[_CarrierDict] = None
        if link_context_header:
            link_context_carrier = self.payload_converter.from_payloads(
                [link_context_header]
            )[0]
        with self._top_level_workflow_context(success_is_complete=False):
            self._completed_span(
                f"ValidateUpdate:{input.update}",
                link_context_carrier=link_context_carrier,
                kind=opentelemetry.trace.SpanKind.SERVER,
            )
            super().handle_update_validator(input)

    async def handle_update_handler(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> Any:
        """Implementation of
        :py:meth:`temporalio.worker.WorkflowInboundInterceptor.handle_update_handler`.
        """
        link_context_header = input.headers.get(self.header_key)
        link_context_carrier: Optional[_CarrierDict] = None
        if link_context_header:
            link_context_carrier = self.payload_converter.from_payloads(
                [link_context_header]
            )[0]
        with self._top_level_workflow_context(success_is_complete=False):
            self._completed_span(
                f"HandleUpdate:{input.update}",
                link_context_carrier=link_context_carrier,
                kind=opentelemetry.trace.SpanKind.SERVER,
            )
            return await super().handle_update_handler(input)

    def _load_workflow_context_carrier(self) -> Optional[_CarrierDict]:
        if self._workflow_context_carrier:
            return self._workflow_context_carrier
        context_header = temporalio.workflow.info().headers.get(self.header_key)
        if not context_header:
            return None
        self._workflow_context_carrier = self.payload_converter.from_payloads(
            [context_header]
        )[0]
        return self._workflow_context_carrier

    @contextmanager
    def _top_level_workflow_context(
        self, *, success_is_complete: bool
    ) -> Iterator[None]:
        # Load context only if there is a carrier, otherwise use empty context
        context_carrier = self._load_workflow_context_carrier()
        context: opentelemetry.context.Context
        if context_carrier:
            context = self.text_map_propagator.extract(context_carrier)
        else:
            context = opentelemetry.context.Context()
        # We need to put this interceptor on the context too
        context = self._set_on_context(context)
        # Need to know whether completed and whether there was a fail-workflow
        # exception
        success = False
        exception: Optional[Exception] = None
        # Run under this context
        token = opentelemetry.context.attach(context)
        try:
            yield None
            success = True
        except temporalio.exceptions.FailureError as err:
            # We only record the failure errors since those are the only ones
            # that lead to workflow completions
            exception = err
            raise
        finally:
            # Create a completed span before detaching context
            if exception or (success and success_is_complete):
                self._completed_span(
                    f"CompleteWorkflow:{temporalio.workflow.info().workflow_type}",
                    exception=exception,
                    kind=opentelemetry.trace.SpanKind.INTERNAL,
                )
            opentelemetry.context.detach(token)

    def _context_to_headers(
        self, headers: Mapping[str, temporalio.api.common.v1.Payload]
    ) -> Mapping[str, temporalio.api.common.v1.Payload]:
        carrier: _CarrierDict = {}
        self.text_map_propagator.inject(carrier)
        return self._context_carrier_to_headers(carrier, headers)

    def _context_carrier_to_headers(
        self,
        carrier: _CarrierDict,
        headers: Mapping[str, temporalio.api.common.v1.Payload],
    ) -> Mapping[str, temporalio.api.common.v1.Payload]:
        if carrier:
            headers = {
                **headers,
                self.header_key: self.payload_converter.to_payloads([carrier])[0],
            }
        return headers

    def _completed_span(
        self,
        span_name: str,
        *,
        link_context_carrier: Optional[_CarrierDict] = None,
        add_to_outbound: Optional[_InputWithHeaders] = None,
        new_span_even_on_replay: bool = False,
        additional_attributes: opentelemetry.util.types.Attributes = None,
        exception: Optional[Exception] = None,
        kind: opentelemetry.trace.SpanKind = opentelemetry.trace.SpanKind.INTERNAL,
    ) -> None:
        # If we are replaying and they don't want a span on replay, no span
        if temporalio.workflow.unsafe.is_replaying() and not new_span_even_on_replay:
            return None

        # Create the span. First serialize current context to carrier.
        new_context_carrier: _CarrierDict = {}
        self.text_map_propagator.inject(new_context_carrier)
        # Invoke
        info = temporalio.workflow.info()
        attributes: Dict[str, opentelemetry.util.types.AttributeValue] = {
            "temporalWorkflowID": info.workflow_id,
            "temporalRunID": info.run_id,
        }
        if additional_attributes:
            attributes.update(additional_attributes)
        updated_context_carrier = self._extern_functions[
            "__temporal_opentelemetry_completed_span"
        ](
            _CompletedWorkflowSpanParams(
                context=new_context_carrier,
                name=span_name,
                # Always set span attributes as workflow ID and run ID
                attributes=attributes,
                time_ns=temporalio.workflow.time_ns(),
                link_context=link_context_carrier,
                exception=exception,
                kind=kind,
                parent_missing=opentelemetry.trace.get_current_span()
                is opentelemetry.trace.INVALID_SPAN,
            )
        )

        # Add to outbound if needed
        if add_to_outbound and updated_context_carrier:
            add_to_outbound.headers = self._context_carrier_to_headers(
                updated_context_carrier, add_to_outbound.headers
            )

    def _set_on_context(
        self, context: opentelemetry.context.Context
    ) -> opentelemetry.context.Context:
        return opentelemetry.context.set_value(_interceptor_context_key, self, context)


class _TracingWorkflowOutboundInterceptor(
    temporalio.worker.WorkflowOutboundInterceptor
):
    def __init__(
        self,
        next: temporalio.worker.WorkflowOutboundInterceptor,
        root: TracingWorkflowInboundInterceptor,
    ) -> None:
        super().__init__(next)
        self.root = root

    def continue_as_new(self, input: temporalio.worker.ContinueAsNewInput) -> NoReturn:
        # Put the current context on to the continue as new
        input.headers = self.root._context_to_headers(input.headers)
        super().continue_as_new(input)

    async def signal_child_workflow(
        self, input: temporalio.worker.SignalChildWorkflowInput
    ) -> None:
        # Create new span and put on outbound input
        self.root._completed_span(
            f"SignalChildWorkflow:{input.signal}",
            add_to_outbound=input,
            kind=opentelemetry.trace.SpanKind.SERVER,
        )
        await super().signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: temporalio.worker.SignalExternalWorkflowInput
    ) -> None:
        # Create new span and put on outbound input
        self.root._completed_span(
            f"SignalExternalWorkflow:{input.signal}",
            add_to_outbound=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        )
        await super().signal_external_workflow(input)

    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        # Create new span and put on outbound input
        self.root._completed_span(
            f"StartActivity:{input.activity}",
            add_to_outbound=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        )
        return super().start_activity(input)

    async def start_child_workflow(
        self, input: temporalio.worker.StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle:
        # Create new span and put on outbound input
        self.root._completed_span(
            f"StartChildWorkflow:{input.workflow}",
            add_to_outbound=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        )
        return await super().start_child_workflow(input)

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        # Create new span and put on outbound input
        self.root._completed_span(
            f"StartActivity:{input.activity}",
            add_to_outbound=input,
            kind=opentelemetry.trace.SpanKind.CLIENT,
        )
        return super().start_local_activity(input)


class workflow:
    """Contains static methods that are safe to call from within a workflow.

    .. warning::
        Using any other ``opentelemetry`` API could cause non-determinism.
    """

    def __init__(self) -> None:  # noqa: D107
        raise NotImplementedError

    @staticmethod
    def completed_span(
        name: str,
        *,
        attributes: opentelemetry.util.types.Attributes = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Create and end an OpenTelemetry span.

        Note, this will only create and record when the workflow is not
        replaying and if there is a current span (meaning the client started a
        span and this interceptor is configured on the worker and the span is on
        the context).

        There is currently no way to create a long-running span or to create a
        span that actually spans other code.

        Args:
            name: Name of the span.
            attributes: Attributes to set on the span if any. Workflow ID and
                run ID are automatically added.
            exception: Optional exception to record on the span.
        """
        interceptor = TracingWorkflowInboundInterceptor._from_context()
        if interceptor:
            interceptor._completed_span(
                name, additional_attributes=attributes, exception=exception
            )
