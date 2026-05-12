from enum import IntEnum
import functools
from typing import Any, Callable, Dict, Optional, Type
from types import TracebackType


class TraceLevel(IntEnum):
    DEBUG = 0
    INFO = 1
    ERROR = 2
    NONE = 3


class _TracingCtx:
    def __init__(self, tracer: "Tracer", span_name: str) -> None:
        self._enabled = tracer._open_tracer is not None
        self._scope: Any = None
        self._tracer = tracer
        self._span_name = span_name

    def __enter__(self) -> "_TracingCtx":
        """
        Creates new span
        :return: self
        """
        if not self._enabled:
            return self
        self._scope = self._tracer._open_tracer.start_active_span(self._span_name)
        self._scope.span.set_baggage_item("ctx", self)
        self.trace(self._tracer._pre_tags)
        return self

    @property
    def enabled(self) -> bool:
        """
        :return: Is tracing enabled
        """
        return self._enabled

    def trace(self, tags: Dict[str, Any], trace_level: TraceLevel = TraceLevel.INFO) -> None:
        """
        Add tags to current span

        :param ydb.TraceLevel trace_level: level of tracing
        :param dict tags: Dict of tags
        """
        if self._tracer._verbose_level < trace_level:
            return
        if not self.enabled or self._scope is None:
            return
        for key, value in tags.items():
            self._scope.span.set_tag(key, value)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if not self.enabled:
            return
        if exc_val:
            self.trace(self._tracer._post_tags_err, trace_level=TraceLevel.ERROR)
            self._tracer._on_err(self, exc_type, exc_val, exc_tb)
        else:
            self.trace(self._tracer._post_tags_ok)
        self._scope.close()
        self._scope = None


def with_trace(span_name: Optional[str] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(f)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            name = span_name if span_name is not None else self.__class__.__name__ + "." + f.__name__
            with self.tracer.trace(name):
                return f(self, *args, **kwargs)

        return wrapper

    return decorator


def trace(tracer: "Tracer", tags: Dict[str, Any], trace_level: TraceLevel = TraceLevel.INFO) -> Optional[bool]:
    if tracer.enabled:
        scope = tracer._open_tracer.scope_manager.active
        if not scope:
            return False

        ctx = scope.span.get_baggage_item("ctx")
        if ctx is None:
            return False

        ctx.trace(tags, trace_level)
        return None
    return None


class Tracer:
    def __init__(self, tracer: Any) -> None:
        """
        Init an tracer to trace requests

        :param opentracing.Tracer tracer: opentracing.Tracer implementation. If None - tracing not enabled
        """
        self._open_tracer: Any = tracer
        self._pre_tags: Dict[str, Any] = {}
        self._post_tags_ok: Dict[str, Any] = {}
        self._post_tags_err: Dict[str, Any] = {}
        self._on_err: Callable[..., None] = lambda *args, **kwargs: None
        self._verbose_level: TraceLevel = TraceLevel.NONE

    @property
    def enabled(self) -> bool:
        return self._open_tracer is not None

    def trace(self, span_name: str) -> _TracingCtx:
        """
        Create tracing context

        :param str span_name:

        :return: A tracing context
        :rtype: _TracingCtx
        """
        return _TracingCtx(self, span_name)

    def with_pre_tags(self, tags: Dict[str, Any]) -> "Tracer":
        """
        Add `tags` to every span immediately after creation

        :param dict tags: tags dict

        :return: self
        """
        self._pre_tags = tags
        return self

    def with_post_tags(self, ok_tags: Dict[str, Any], err_tags: Dict[str, Any]) -> "Tracer":
        """
        Add some tags before span close

        :param ok_tags: Add this tags if no error raised
        :param err_tags: Add this tags if there is an exception

        :return: self
        """
        self._post_tags_ok = ok_tags
        self._post_tags_err = err_tags
        return self

    def with_on_error_callback(self, callee: Callable[..., None]) -> "Tracer":
        """
        Add an callback, that will be called if there is an exception in span

        :param callable[_TracingCtx, exc_type, exc_val, exc_tb] callee:

        :return: self
        """
        self._on_err = callee
        return self

    def with_verbose_level(self, level: TraceLevel) -> "Tracer":
        self._verbose_level = level
        return self

    @classmethod
    def default(cls, tracer: Any) -> "Tracer":
        """
        Create default tracer

        :param tracer:

        :return: new tracer
        """
        return (
            cls(tracer)
            .with_post_tags({"ok": True}, {"ok": False})
            .with_pre_tags({"started": True})
            .with_on_error_callback(_default_on_error_callback)
            .with_verbose_level(TraceLevel.INFO)
        )


def _default_on_error_callback(
    ctx: _TracingCtx,
    exc_type: Optional[Type[BaseException]],
    exc_val: Optional[BaseException],
    exc_tb: Optional[TracebackType],
) -> None:
    ctx.trace(
        {
            "error.type": exc_type.__name__ if exc_type else None,
            "error.value": exc_val,
            "error.traceback": exc_tb,
        },
        trace_level=TraceLevel.ERROR,
    )
