import time
from typing import Optional, Union

from annet import tracing
from annet.tracing import tracing_connector
from annet.types import GeneratorPartialRunArgs, GeneratorPerf

from .base import BaseGenerator


class GeneratorPerfMesurer:
    def __init__(
        self,
        gen: BaseGenerator,
        run_args: Optional[GeneratorPartialRunArgs] = None,
        trace_min_duration: tracing.MinDurationT = None,
    ):
        self._gen = gen
        self._run_args = run_args

        self._start_time: float = 0.0
        self._span_ctx = None
        self._span = None
        self._trace_min_duration = trace_min_duration

        self.last_result: Optional[GeneratorPerf] = None

    def start(self) -> None:
        self.last_result = None

        self._span_ctx = tracing_connector.get().start_as_current_span(
            "gen:call",
            tracer_name=self._gen.__class__.__module__,
            min_duration=self._trace_min_duration,
        )
        self._span = self._span_ctx.__enter__()  # pylint: disable=unnecessary-dunder-call

        if self._span:
            self._span.set_attributes({"generator.class": self._gen.__class__.__name__})
            if self._run_args:
                tracing_connector.get().set_device_attributes(
                    self._span,
                    self._run_args.device,
                )

        self._start_time = time.monotonic()

    def finish(self, exc_type=None, exc_val=None, exc_tb=None) -> GeneratorPerf:
        total = time.monotonic() - self._start_time
        self._span_ctx.__exit__(exc_type, exc_val, exc_tb)
        rt = self._gen.storage.flush_perf()

        meta = {}
        if tracing_connector.get().enabled:
            span_context = self._span.get_span_context()
            meta = {
                "span": {
                    "trace_id": str(span_context.trace_id),
                    "span_id": str(span_context.span_id),
                },
            }

        self.last_result = GeneratorPerf(total=total, rt=rt, meta=meta)
        return self.last_result

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish(exc_type, exc_val, exc_tb)
