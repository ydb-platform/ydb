from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from threading import Lock
from typing import Callable, Literal

from opentelemetry import context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.sdk.trace.sampling import Sampler, TraceIdRatioBased
from typing_extensions import Self

from logfire._internal.constants import (
    ONE_SECOND_IN_NANOSECONDS,
    LevelName,
)
from logfire._internal.exporters.wrapper import WrapperSpanProcessor
from logfire.types import SpanLevel


@dataclass
class TraceBuffer:
    """Arguments of `SpanProcessor.on_start` and `SpanProcessor.on_end` for spans in a single trace.

    These are stored until either the trace is included by tail sampling or it's completed and discarded.
    """

    started: list[tuple[Span, context.Context | None]]
    ended: list[ReadableSpan]

    @cached_property
    def first_span(self) -> Span:
        return self.started[0][0]

    @cached_property
    def trace_id(self) -> int:
        span_context = self.first_span.context
        assert span_context is not None
        return span_context.trace_id


@dataclass
class TailSamplingSpanInfo:
    """Argument passed to the [`SamplingOptions.tail`][logfire.sampling.SamplingOptions.tail] callback."""

    span: ReadableSpan
    """
    Raw span object being started or ended.
    """

    context: context.Context | None
    """
    Second argument of [`SpanProcessor.on_start`][opentelemetry.sdk.trace.SpanProcessor.on_start] or `None` for `SpanProcessor.on_end`.
    """

    event: Literal['start', 'end']
    """
    `'start'` if the span is being started, `'end'` if it's being ended.
    """

    # Intentionally undocumented for now.
    buffer: TraceBuffer

    @property
    def level(self) -> SpanLevel:
        """The log level of the span."""
        return SpanLevel.from_span(self.span)

    @property
    def duration(self) -> float:
        """The time in seconds between the start of the trace and the start/end of this span."""
        # span.end_time and span.start_time are in nanoseconds and can be None.
        return (
            (self.span.end_time or self.span.start_time or 0) - (self.buffer.first_span.start_time or float('inf'))
        ) / ONE_SECOND_IN_NANOSECONDS


@dataclass
class SamplingOptions:
    """Options for [`logfire.configure(sampling=...)`][logfire.configure(sampling)].

    See the [sampling guide](https://logfire.pydantic.dev/docs/guides/advanced/sampling/).
    """

    head: float | Sampler = 1.0
    """
    Head sampling options.

    If it's a float, it should be a number between 0.0 and 1.0.
    This is the probability that an entire trace will randomly included.

    Alternatively you can pass a custom
    [OpenTelemetry `Sampler`](https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.sampling.html).
    """

    tail: Callable[[TailSamplingSpanInfo], float] | None = None
    """
    An optional tail sampling callback which will be called for every span.

    It should return a number between 0.0 and 1.0, the probability that the entire trace will be included.
    Use [`SamplingOptions.level_or_duration`][logfire.sampling.SamplingOptions.level_or_duration]
    for a common use case.

    Every span in a trace will be stored in memory until either the trace is included by tail sampling
    or it's completed and discarded, so large traces may consume a lot of memory.
    """

    @classmethod
    def level_or_duration(
        cls,
        *,
        head: float | Sampler = 1.0,
        level_threshold: LevelName | None = 'notice',
        duration_threshold: float | None = 5.0,
        background_rate: float = 0.0,
    ) -> Self:
        """Returns a `SamplingOptions` instance that tail samples traces based on their log level and duration.

        If a trace has at least one span/log that has a log level greater than or equal to `level_threshold`,
        or if the duration of the whole trace is greater than `duration_threshold` seconds,
        then the whole trace will be included.
        Otherwise, the probability is `background_rate`.

        The `head` parameter is the same as in the `SamplingOptions` constructor.
        """
        head_sample_rate = head if isinstance(head, (float, int)) else 1.0

        if not (0.0 <= background_rate <= head_sample_rate <= 1.0):
            raise ValueError('Invalid sampling rates, must be 0.0 <= background_rate <= head <= 1.0')

        def get_tail_sample_rate(span_info: TailSamplingSpanInfo) -> float:
            if duration_threshold is not None and span_info.duration > duration_threshold:
                return 1.0

            if level_threshold is not None and span_info.level >= level_threshold:
                return 1.0

            return background_rate

        return cls(head=head, tail=get_tail_sample_rate)


def check_trace_id_ratio(trace_id: int, rate: float) -> bool:
    # Based on TraceIdRatioBased.should_sample.
    return (trace_id & TraceIdRatioBased.TRACE_ID_LIMIT) < TraceIdRatioBased.get_bound_for_rate(rate)


class TailSamplingProcessor(WrapperSpanProcessor):
    """Passes spans to the wrapped processor if any span in a trace meets the sampling criteria."""

    def __init__(self, processor: SpanProcessor, get_tail_sample_rate: Callable[[TailSamplingSpanInfo], float]) -> None:
        super().__init__(processor)
        self.get_tail_sample_rate = get_tail_sample_rate

        # A TraceBuffer is typically created for each new trace.
        # If a span meets the sampling criteria, the buffer is dropped and all spans within are pushed
        # to the wrapped processor.
        # So when more spans arrive and there's no buffer, they get passed through immediately.
        self.traces: dict[int, TraceBuffer] = {}

        # Code that touches self.traces and its contents should be protected by this lock.
        self.lock = Lock()

    def on_start(self, span: Span, parent_context: context.Context | None = None) -> None:
        dropped = False
        buffer = None

        with self.lock:
            # span.context could supposedly be None, not sure how.
            if span.context:  # pragma: no branch
                trace_id = span.context.trace_id
                # If span.parent is None, it's the root span of a trace.
                if span.parent is None:
                    self.traces[trace_id] = TraceBuffer([], [])

                buffer = self.traces.get(trace_id)
                if buffer is not None:
                    # This trace's spans haven't met the criteria yet, so add this span to the buffer.
                    buffer.started.append((span, parent_context))
                    dropped = self.check_span(TailSamplingSpanInfo(span, parent_context, 'start', buffer))
                # The opposite case is handled outside the lock since it may take some time.

        # This code may take longer since it calls the wrapped processor which might do anything.
        # It shouldn't be inside the lock to avoid blocking other threads.
        # Since it's not in the lock, it shouldn't touch self.traces or its contents.
        if buffer is None:
            super().on_start(span, parent_context)
        elif dropped:
            self.push_buffer(buffer)

    def on_end(self, span: ReadableSpan) -> None:
        # This has a very similar structure and reasoning to on_start.

        dropped = False
        buffer = None

        with self.lock:
            if span.context:  # pragma: no branch
                trace_id = span.context.trace_id
                buffer = self.traces.get(trace_id)
                if buffer is not None:
                    buffer.ended.append(span)
                    dropped = self.check_span(TailSamplingSpanInfo(span, None, 'end', buffer))
                    if span.parent is None:
                        # This is the root span, so the trace is hopefully complete.
                        # Delete the buffer to save memory.
                        self.traces.pop(trace_id, None)

        if buffer is None:
            super().on_end(span)
        elif dropped:
            self.push_buffer(buffer)

    def check_span(self, span_info: TailSamplingSpanInfo) -> bool:
        """If the span meets the sampling criteria, drop the buffer and return True. Otherwise, return False."""
        sample_rate = self.get_tail_sample_rate(span_info)
        if sampled := check_trace_id_ratio(span_info.buffer.trace_id, sample_rate):
            self.drop_buffer(span_info.buffer)

        return sampled

    def drop_buffer(self, buffer: TraceBuffer) -> None:
        del self.traces[buffer.trace_id]

    def push_buffer(self, buffer: TraceBuffer) -> None:
        for started in buffer.started:
            super().on_start(*started)
        for span in buffer.ended:
            super().on_end(span)
