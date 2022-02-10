from enum import IntEnum
import functools


class TraceLevel(IntEnum):
    DEBUG = 0
    INFO = 1
    ERROR = 2
    NONE = 3


class _TracingCtx:
    def __init__(self, tracer, span_name):
        self._enabled = tracer._open_tracer is not None
        self._scope = None
        self._tracer = tracer
        self._span_name = span_name

    def __enter__(self):
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
    def enabled(self):
        """
        :return: Is tracing enabled
        """
        return self._enabled

    def trace(self, tags, trace_level=TraceLevel.INFO):
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

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.enabled:
            return
        if exc_val:
            self.trace(self._tracer._post_tags_err, trace_level=TraceLevel.ERROR)
            self._tracer._on_err(self, exc_type, exc_val, exc_tb)
        else:
            self.trace(self._tracer._post_tags_ok)
        self._scope.close()
        self._scope = None


def with_trace(span_name=None):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            name = (
                span_name
                if span_name is not None
                else self.__class__.__name__ + "." + f.__name__
            )
            with self.tracer.trace(name):
                return f(self, *args, **kwargs)

        return wrapper

    return decorator


def trace(tracer, tags, trace_level=TraceLevel.INFO):
    if tracer.enabled:
        scope = tracer._open_tracer.scope_manager.active
        if not scope:
            return False

        ctx = scope.span.get_baggage_item("ctx")
        if ctx is None:
            return False

        return ctx.trace(tags, trace_level)


class Tracer:
    def __init__(self, tracer):
        """
        Init an tracer to trace requests

        :param opentracing.Tracer tracer: opentracing.Tracer implementation. If None - tracing not enabled
        """
        self._open_tracer = tracer
        self._pre_tags = {}
        self._post_tags_ok = {}
        self._post_tags_err = {}
        self._on_err = lambda *args, **kwargs: None
        self._verbose_level = TraceLevel.NONE

    @property
    def enabled(self):
        return self._open_tracer is not None

    def trace(self, span_name):
        """
        Create tracing context

        :param str span_name:

        :return: A tracing context
        :rtype: _TracingCtx
        """
        return _TracingCtx(self, span_name)

    def with_pre_tags(self, tags):
        """
        Add `tags` to every span immediately after creation

        :param dict tags: tags dict

        :return: self
        """
        self._pre_tags = tags
        return self

    def with_post_tags(self, ok_tags, err_tags):
        """
        Add some tags before span close

        :param ok_tags: Add this tags if no error raised
        :param err_tags: Add this tags if there is an exception

        :return: self
        """
        self._post_tags_ok = ok_tags
        self._post_tags_err = err_tags
        return self

    def with_on_error_callback(self, callee):
        """
        Add an callback, that will be called if there is an exception in span

        :param callable[_TracingCtx, exc_type, exc_val, exc_tb] callee:

        :return: self
        """
        self._on_err = callee
        return self

    def with_verbose_level(self, level):
        self._verbose_level = level
        return self

    @classmethod
    def default(cls, tracer):
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


def _default_on_error_callback(ctx, exc_type, exc_val, exc_tb):
    ctx.trace(
        {
            "error.type": exc_type.__name__,
            "error.value": exc_val,
            "error.traceback": exc_tb,
        },
        trace_level=TraceLevel.ERROR,
    )
