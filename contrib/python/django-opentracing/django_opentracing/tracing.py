import opentracing
from opentracing.ext import tags
import six


class DjangoTracing(object):
    '''
    @param tracer the OpenTracing tracer to be used
    to trace requests using this DjangoTracing
    '''
    def __init__(self, tracer=None, start_span_cb=None):
        if start_span_cb is not None and not callable(start_span_cb):
            raise ValueError('start_span_cb is not callable')

        self._tracer_implementation = tracer
        self._start_span_cb = start_span_cb
        self._current_scopes = {}
        self._trace_all = False

    def _get_tracer_impl(self):
        return self._tracer_implementation

    @property
    def tracer(self):
        if self._tracer_implementation:
            return self._tracer_implementation
        else:
            return opentracing.tracer

    @property
    def _tracer(self):
        '''DEPRECATED'''
        return self.tracer

    def get_span(self, request):
        '''
        @param request
        Returns the span tracing this request
        '''
        scope = self._current_scopes.get(request, None)
        return None if scope is None else scope.span

    def trace(self, *attributes):
        '''
        Function decorator that traces functions such as Views
        @param attributes any number of HttpRequest attributes
        (strings) to be set as tags on the created span
        '''
        def decorator(view_func):
            # TODO: do we want to provide option of overriding
            # trace_all_requests so that they can trace certain attributes
            # of the request for just this request (this would require to
            # reinstate the name-mangling with a trace identifier, and another
            # settings key)

            def wrapper(request, *args, **kwargs):
                # if tracing all already, return right away.
                if self._trace_all:
                    return view_func(request)

                # otherwise, apply tracing.
                try:
                    self._apply_tracing(request, view_func, list(attributes))
                    r = view_func(request, *args, **kwargs)
                except Exception as exc:
                    self._finish_tracing(request, error=exc)
                    raise

                self._finish_tracing(request, r)
                return r

            return wrapper
        return decorator

    def _apply_tracing(self, request, view_func, attributes):
        '''
        Helper function to avoid rewriting for middleware and decorator.
        Returns a new span from the request with logged attributes and
        correct operation name from the view_func.
        '''
        # strip headers for trace info
        headers = {}
        for k, v in six.iteritems(request.META):
            k = k.lower().replace('_', '-')
            if k.startswith('http-'):
                k = k[5:]
            headers[k] = v

        # start new span from trace info
        operation_name = view_func.__name__
        try:
            span_ctx = self.tracer.extract(opentracing.Format.HTTP_HEADERS,
                                           headers)
            scope = self.tracer.start_active_span(operation_name,
                                                  child_of=span_ctx)
        except (opentracing.InvalidCarrierException,
                opentracing.SpanContextCorruptedException):
            scope = self.tracer.start_active_span(operation_name)

        # add span to current spans
        self._current_scopes[request] = scope

        # standard tags
        scope.span.set_tag(tags.COMPONENT, 'django')
        scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_SERVER)
        scope.span.set_tag(tags.HTTP_METHOD, request.method)
        scope.span.set_tag(tags.HTTP_URL, request.get_full_path())

        # log any traced attributes
        for attr in attributes:
            if hasattr(request, attr):
                payload = str(getattr(request, attr))
                if payload:
                    scope.span.set_tag(attr, payload)

        # invoke the start span callback, if any
        self._call_start_span_cb(scope.span, request)

        return scope

    def _finish_tracing(self, request, response=None, error=None):
        scope = self._current_scopes.pop(request, None)
        if scope is None:
            return

        if error is not None:
            scope.span.set_tag(tags.ERROR, True)
            scope.span.log_kv({
                'event': tags.ERROR,
                'error.object': error,
            })
        if response is not None:
            scope.span.set_tag(tags.HTTP_STATUS_CODE, response.status_code)

        scope.close()

    def _call_start_span_cb(self, span, request):
        if self._start_span_cb is None:
            return

        try:
            self._start_span_cb(span, request)
        except Exception:
            pass


def initialize_global_tracer(tracing):
    '''
    Initialisation as per https://github.com/opentracing/opentracing-python/blob/9f9ef02d4ef7863fb26d3534a38ccdccf245494c/opentracing/__init__.py#L36 # noqa

    Here the global tracer object gets initialised once from Django settings.
    '''
    if initialize_global_tracer.complete:
        return

    # DjangoTracing may be already relying on the global tracer,
    # hence check for a non-None value.
    tracer = tracing._tracer_implementation
    if tracer is not None:
        opentracing.tracer = tracer

    initialize_global_tracer.complete = True

initialize_global_tracer.complete = False
