import warnings
from typing import (  # noqa
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Awaitable,
    Dict,
    List,
    Optional,
    Type,
)

from .context_managers import _ContextManager
from .helpers import Endpoint, TraceContext
from .mypy_types import OptBool, OptLoop
from .record import Record
from .sampler import Sampler, SamplerABC
from .span import NoopSpan, Span, SpanAbc
from .transport import StubTransport, Transport, TransportABC
from .utils import generate_random_64bit_string, generate_random_128bit_string


if TYPE_CHECKING:

    class _Base(AsyncContextManager["Tracer"]):
        pass


else:

    class _Base(AsyncContextManager):
        pass


class Tracer(_Base):
    def __init__(
        self,
        transport: TransportABC,
        sampler: SamplerABC,
        local_endpoint: Endpoint,
        ignored_exceptions: Optional[List[Type[Exception]]] = None,
    ) -> None:
        super().__init__()
        self._records: Dict[TraceContext, Record] = {}
        self._transport = transport
        self._sampler = sampler
        self._local_endpoint = local_endpoint
        self._ignored_exceptions = ignored_exceptions or []

    def new_trace(self, sampled: OptBool = None, debug: bool = False) -> SpanAbc:
        context = self._next_context(None, sampled=sampled, debug=debug)
        return self.to_span(context)

    def join_span(self, context: TraceContext) -> SpanAbc:
        new_context = context
        if context.sampled is None:
            sampled = self._sampler.is_sampled(context.trace_id)
            new_context = new_context._replace(sampled=sampled)
        else:
            new_context = new_context._replace(shared=True)
        return self.to_span(new_context)

    def new_child(self, context: TraceContext) -> SpanAbc:
        new_context = self._next_context(context)
        if not context.sampled:
            return NoopSpan(self, new_context, self._ignored_exceptions)
        return self.to_span(new_context)

    def to_span(self, context: TraceContext) -> SpanAbc:
        if not context.sampled:
            return NoopSpan(self, context, self._ignored_exceptions)

        record = Record(context, self._local_endpoint)
        self._records[context] = record
        return Span(self, context, record, self._ignored_exceptions)

    def _send(self, record: Record) -> None:
        self._records.pop(record.context, None)
        self._transport.send(record)

    def _next_context(
        self,
        context: Optional[TraceContext] = None,
        sampled: OptBool = None,
        debug: bool = False,
    ) -> TraceContext:
        span_id = generate_random_64bit_string()
        if context is not None:
            new_context = context._replace(
                span_id=span_id, parent_id=context.span_id, shared=False
            )
            return new_context

        trace_id = generate_random_128bit_string()
        if sampled is None:
            sampled = self._sampler.is_sampled(trace_id)

        new_context = TraceContext(
            trace_id=trace_id,
            parent_id=None,
            span_id=span_id,
            sampled=sampled,
            debug=debug,
            shared=False,
        )
        return new_context

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> "Tracer":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


def create(
    zipkin_address: str,
    local_endpoint: Endpoint,
    *,
    sample_rate: float = 0.01,
    send_interval: float = 5,
    loop: OptLoop = None,
    ignored_exceptions: Optional[List[Type[Exception]]] = None
) -> _ContextManager[Tracer]:
    if loop is not None:
        warnings.warn(
            "loop parameter is deprecated and ignored", DeprecationWarning, stacklevel=2
        )

    async def build_tracer() -> Tracer:
        sampler = Sampler(sample_rate=sample_rate)
        transport = Transport(zipkin_address, send_interval=send_interval)
        return Tracer(transport, sampler, local_endpoint, ignored_exceptions)

    result = _ContextManager(build_tracer())
    return result


def create_custom(
    local_endpoint: Endpoint,
    transport: Optional[TransportABC] = None,
    sampler: Optional[SamplerABC] = None,
    ignored_exceptions: Optional[List[Type[Exception]]] = None,
) -> _ContextManager[Tracer]:
    t = transport or StubTransport()
    sample_rate = 1  # sample everything
    s = sampler or Sampler(sample_rate=sample_rate)

    async def build_tracer() -> Tracer:
        return Tracer(t, s, local_endpoint, ignored_exceptions)

    result = _ContextManager(build_tracer())
    return result
