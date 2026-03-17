import logging
from typing import TYPE_CHECKING, Any, Callable, Collection, List, Optional
from uuid import UUID

from opentelemetry import trace as trace_api
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.trace import Span
from wrapt import wrap_function_wrapper  # type: ignore

from openinference.instrumentation import OITracer, TraceConfig
from openinference.instrumentation.langchain.package import _instruments
from openinference.instrumentation.langchain.version import __version__

if TYPE_CHECKING:
    from langchain_core.callbacks import BaseCallbackManager
    from langchain_core.runnables.config import var_child_runnable_config  # noqa F401

    from openinference.instrumentation.langchain._tracer import OpenInferenceTracer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_MODULE = "langchain_core"


class LangChainInstrumentor(BaseInstrumentor):  # type: ignore
    """
    An instrumentor for LangChain
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        if not (tracer_provider := kwargs.get("tracer_provider")):
            tracer_provider = trace_api.get_tracer_provider()
        if not (config := kwargs.get("config")):
            config = TraceConfig()
        else:
            assert isinstance(config, TraceConfig)
        import langchain_core

        from openinference.instrumentation.langchain._tracer import OpenInferenceTracer

        tracer = OITracer(
            trace_api.get_tracer(__name__, __version__, tracer_provider),
            config=config,
        )
        self._tracer: Optional[OpenInferenceTracer] = OpenInferenceTracer(
            tracer,
            bool(kwargs.get("separate_trace_from_runtime_context")),
        )
        self._original_callback_manager_init = langchain_core.callbacks.BaseCallbackManager.__init__
        wrap_function_wrapper(
            module="langchain_core.callbacks",
            name="BaseCallbackManager.__init__",
            wrapper=_BaseCallbackManagerInit(self._tracer),
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        import langchain_core

        langchain_core.callbacks.BaseCallbackManager.__init__ = self._original_callback_manager_init  # type: ignore
        self._original_callback_manager_init = None  # type: ignore
        self._tracer = None

    def get_span(self, run_id: UUID) -> Optional[Span]:
        if not hasattr(self, "_tracer") or self._tracer is None:
            print("Missing `tracer` for spans; please run `LangChainInstrumentor().instrument()`")
            return None
        return self._tracer.get_span(run_id) if self._tracer else None

    def get_ancestors(self, run_id: UUID) -> List[Span]:
        if not hasattr(self, "_tracer") or self._tracer is None:
            print("Missing `tracer` for spans; please run `LangChainInstrumentor().instrument()`")
            return []
        ancestors: List[Span] = []
        tracer = self._tracer
        assert tracer

        run = tracer.run_map.get(str(run_id))
        if not run:
            return ancestors

        ancestor_run_id = run.parent_run_id  # start with the first ancestor

        while ancestor_run_id:
            span = self.get_span(ancestor_run_id)
            if span:
                ancestors.append(span)

            run = tracer.run_map.get(str(ancestor_run_id))
            if not run:
                break
            ancestor_run_id = run.parent_run_id
        return ancestors


class _BaseCallbackManagerInit:
    __slots__ = ("_tracer",)

    def __init__(self, tracer: "OpenInferenceTracer"):
        self._tracer = tracer

    def __call__(
        self,
        wrapped: Callable[..., None],
        instance: "BaseCallbackManager",
        args: Any,
        kwargs: Any,
    ) -> None:
        wrapped(*args, **kwargs)
        for handler in instance.inheritable_handlers:
            # Handlers may be copied when new managers are created, so we
            # don't want to keep adding. E.g. see the following location.
            # https://github.com/langchain-ai/langchain/blob/5c2538b9f7fb64afed2a918b621d9d8681c7ae32/libs/core/langchain_core/callbacks/manager.py#L1876  # noqa: E501
            if isinstance(handler, type(self._tracer)):
                break
        else:
            instance.add_handler(self._tracer, True)


def get_current_span() -> Optional[Span]:
    import langchain_core

    run_id: Optional[UUID] = None
    config = langchain_core.runnables.config.var_child_runnable_config.get()
    if not isinstance(config, dict):
        return None
    for v in config.values():
        if not isinstance(v, langchain_core.callbacks.BaseCallbackManager):
            continue
        if run_id := v.parent_run_id:
            break
    if not run_id:
        return None
    return LangChainInstrumentor().get_span(run_id)


def get_ancestor_spans() -> List[Span]:
    """
    Retrieve the ancestor spans for the current LangChain run.

    This function traverses the LangChain run tree from the current run's parent up to the root,
    collecting the spans associated with each ancestor run. The list is ordered from the immediate
    parent of the current run to the root of the run tree.
    """
    import langchain_core

    run_id: Optional[UUID] = None
    config = langchain_core.runnables.config.var_child_runnable_config.get()
    if not isinstance(config, dict):
        return []
    for v in config.values():
        if not isinstance(v, langchain_core.callbacks.BaseCallbackManager):
            continue
        if run_id := v.parent_run_id:
            break
    if not run_id:
        return []
    return LangChainInstrumentor().get_ancestors(run_id)
