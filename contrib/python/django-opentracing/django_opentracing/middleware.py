from django.conf import settings
from django.utils.module_loading import import_string

from .tracing import DjangoTracing
from .tracing import initialize_global_tracer

try:
    # Django >= 1.10
    from django.utils.deprecation import MiddlewareMixin
except ImportError:
    # Not required for Django <= 1.9, see:
    # https://docs.djangoproject.com/en/1.10/topics/http/middleware/#upgrading-pre-django-1-10-style-middleware
    MiddlewareMixin = object


class OpenTracingMiddleware(MiddlewareMixin):
    '''
    __init__() is only called once, no arguments, when the Web server
    responds to the first request
    '''
    def __init__(self, get_response=None):
        '''
        TODO: ANSWER Qs
        - Is it better to place all tracing info in the settings file,
          or to require a tracing.py file with configurations?
        - Also, better to have try/catch with empty tracer or just fail
          fast if there's no tracer specified
        '''
        self._init_tracing()
        self._tracing = settings.OPENTRACING_TRACING
        self.get_response = get_response

    def _init_tracing(self):
        if getattr(settings, 'OPENTRACING_TRACER', None) is not None:
            # Backwards compatibility.
            tracing = settings.OPENTRACING_TRACER
        elif getattr(settings, 'OPENTRACING_TRACING', None) is not None:
            tracing = settings.OPENTRACING_TRACING
        elif getattr(settings, 'OPENTRACING_TRACER_CALLABLE',
                     None) is not None:
            tracer_callable = settings.OPENTRACING_TRACER_CALLABLE
            tracer_parameters = getattr(settings,
                                        'OPENTRACING_TRACER_PARAMETERS',
                                        {})

            if not callable(tracer_callable):
                tracer_callable = import_string(tracer_callable)

            tracer = tracer_callable(**tracer_parameters)
            tracing = DjangoTracing(tracer)
        else:
            # Rely on the global Tracer.
            tracing = DjangoTracing()

        # trace_all defaults to True when used as middleware.
        tracing._trace_all = getattr(settings, 'OPENTRACING_TRACE_ALL', True)

        # set the start_span_cb hook, if any.
        tracing._start_span_cb = getattr(settings, 'OPENTRACING_START_SPAN_CB',
                                         None)

        # Normalize the tracing field in settings, including the old field.
        settings.OPENTRACING_TRACING = tracing
        settings.OPENTRACING_TRACER = tracing

        # Potentially set the global Tracer (unless we rely on it already).
        if getattr(settings, 'OPENTRACING_SET_GLOBAL_TRACER', False):
            initialize_global_tracer(tracing)

    def process_view(self, request, view_func, view_args, view_kwargs):
        # determine whether this middleware should be applied
        # NOTE: if tracing is on but not tracing all requests, then the tracing
        # occurs through decorator functions rather than middleware
        if not self._tracing._trace_all:
            return None

        if hasattr(settings, 'OPENTRACING_TRACED_ATTRIBUTES'):
            traced_attributes = getattr(settings,
                                        'OPENTRACING_TRACED_ATTRIBUTES')
        else:
            traced_attributes = []
        self._tracing._apply_tracing(request, view_func, traced_attributes)

    def process_exception(self, request, exception):
        self._tracing._finish_tracing(request, error=exception)

    def process_response(self, request, response):
        self._tracing._finish_tracing(request, response=response)
        return response
