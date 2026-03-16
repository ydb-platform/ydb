from django.test import SimpleTestCase, Client, override_settings
from django.conf import settings
import mock
import opentracing
from opentracing.ext import tags
from opentracing.mocktracer import MockTracer
from opentracing.scope_managers import ThreadLocalScopeManager

from django_opentracing import OpenTracingMiddleware
from django_opentracing import DjangoTracing
from django_opentracing import DjangoTracer
from django_opentracing.tracing import initialize_global_tracer


from django.core.wsgi import get_wsgi_application  # FIX_FOR_YANDEX
application = get_wsgi_application()  # FIX_FOR_YANDEX


def start_span_cb(span, request):
    span.set_tag(tags.COMPONENT, 'customvalue')


def start_span_cb_error(span, request):
    raise RuntimeError()


class TestDjangoOpenTracingMiddleware(SimpleTestCase):

    def setUp(self):
        settings.OPENTRACING_TRACING._tracer.reset()

    def test_middleware_untraced(self):
        client = Client()
        response = client.get('/untraced/')
        assert response['numspans'] == '1'
        assert len(settings.OPENTRACING_TRACING._current_scopes) == 0
        assert len(settings.OPENTRACING_TRACING.tracer.finished_spans()) == 1

    @override_settings(OPENTRACING_TRACE_ALL=False)
    def test_middleware_untraced_no_trace_all(self):
        client = Client()
        response = client.get('/untraced/')
        assert response['numspans'] == '0'
        assert len(settings.OPENTRACING_TRACING._current_scopes) == 0
        assert len(settings.OPENTRACING_TRACING.tracer.finished_spans()) == 0

    def test_middleware_traced(self):
        client = Client()
        response = client.get('/traced/')
        assert response['numspans'] == '1'
        assert len(settings.OPENTRACING_TRACING._current_scopes) == 0
        assert len(settings.OPENTRACING_TRACING.tracer.finished_spans()) == 1

    @override_settings(OPENTRACING_TRACE_ALL=False)
    def test_middleware_traced_no_trace_all(self):
        client = Client()
        response = client.get('/traced/')
        assert response['numspans'] == '1'
        assert len(settings.OPENTRACING_TRACING._current_scopes) == 0
        assert len(settings.OPENTRACING_TRACING.tracer.finished_spans()) == 1

    def test_middleware_traced_tags(self):
        self.verify_traced_tags()

    @override_settings(OPENTRACING_TRACE_ALL=False)
    def test_middleware_traced_tags_decorated(self):
        self.verify_traced_tags()

    def verify_traced_tags(self):
        client = Client()
        client.get('/traced/')

        spans = settings.OPENTRACING_TRACING._tracer.finished_spans()
        assert len(spans) == 1
        assert spans[0].tags.get(tags.COMPONENT, None) == 'django'
        assert spans[0].tags.get(tags.HTTP_METHOD, None) == 'GET'
        assert spans[0].tags.get(tags.HTTP_STATUS_CODE, None) == 200
        assert spans[0].tags.get(tags.SPAN_KIND, None) == tags.SPAN_KIND_RPC_SERVER

    def test_middleware_traced_with_attrs(self):
        client = Client()
        response = client.get('/traced_with_attrs/')
        assert response['numspans'] == '1'
        assert len(settings.OPENTRACING_TRACING._current_scopes) == 0

    @override_settings(OPENTRACING_TRACE_ALL=False)
    def test_middleware_traced_with_arg_decorated(self):
        client = Client()
        response = client.get('/traced_with_arg/7/')
        assert response['numspans'] == '1'
        assert response['arg'] == '7'
        assert len(settings.OPENTRACING_TRACING._current_scopes) == 0

    def test_middleware_traced_with_error(self):
        self.verify_traced_with_error()

    @override_settings(OPENTRACING_TRACE_ALL=False)
    def test_middleware_traced_with_error_decorated(self):
        self.verify_traced_with_error()

    def verify_traced_with_error(self):
        client = Client()
        with self.assertRaises(ValueError):
            client.get('/traced_with_error/')
        
        spans = settings.OPENTRACING_TRACING._tracer.finished_spans()
        assert len(spans) == 1
        assert spans[0].tags.get(tags.ERROR, False) is True

        assert len(spans[0].logs) == 1
        assert spans[0].logs[0].key_values.get('event', None) is 'error'
        assert isinstance(
            spans[0].logs[0].key_values.get('error.object', None),
            ValueError
        )

    @override_settings(OPENTRACING_START_SPAN_CB=start_span_cb)
    def test_middleware_traced_start_span_cb(self):
        client = Client()
        client.get('/traced/')

        spans = settings.OPENTRACING_TRACING._tracer.finished_spans()
        assert len(spans) == 1
        assert spans[0].tags.get(tags.COMPONENT, None) is 'customvalue'

    @override_settings(OPENTRACING_START_SPAN_CB=start_span_cb_error)
    def test_middleware_traced_start_span_cb_error(self):
        client = Client()
        client.get('/traced/')

        spans = settings.OPENTRACING_TRACING._tracer.finished_spans()
        assert len(spans) == 1  # Span finished properly.

    def test_middleware_traced_scope(self):
        client = Client()
        response = client.get('/traced_scope/')
        assert response['active_span'] is not None
        assert response['request_span'] == response['active_span']


@override_settings()
class TestDjangoOpenTracingMiddlewareInitialization(SimpleTestCase):

    def setUp(self):
        for m in ['OPENTRACING_TRACING',
                  'OPENTRACING_TRACER',
                  'OPENTRACING_TRACER_CALLABLE',
                  'OPENTRACING_TRACER_PARAMETERS']:
            try:
                delattr(settings, m)
            except AttributeError:
                pass

        initialize_global_tracer.complete = False

    def test_tracer_deprecated(self):
        tracing = DjangoTracer()
        settings.OPENTRACING_TRACER = tracing
        OpenTracingMiddleware()
        assert getattr(settings, 'OPENTRACING_TRACER', None) is tracing
        assert getattr(settings, 'OPENTRACING_TRACING', None) is tracing

    def test_tracing(self):
        tracing = DjangoTracing()
        settings.OPENTRACING_TRACING = tracing
        OpenTracingMiddleware()
        assert getattr(settings, 'OPENTRACING_TRACING', None) is tracing

    def test_tracer_callable(self):
        settings.OPENTRACING_TRACER_CALLABLE = MockTracer
        settings.OPENTRACING_TRACER_PARAMETERS = {
            'scope_manager': ThreadLocalScopeManager()
        }
        OpenTracingMiddleware()
        assert getattr(settings, 'OPENTRACING_TRACING', None) is not None
        assert isinstance(settings.OPENTRACING_TRACING.tracer, MockTracer)

    def test_tracer_callable_str(self):
        settings.OPENTRACING_TRACER_CALLABLE = 'opentracing.mocktracer.MockTracer'
        settings.OPENTRACING_TRACER_PARAMETERS = {
            'scope_manager': ThreadLocalScopeManager()
        }
        OpenTracingMiddleware()
        assert getattr(settings, 'OPENTRACING_TRACING', None) is not None
        assert isinstance(settings.OPENTRACING_TRACING.tracer, MockTracer)

    def test_tracing_none(self):
        OpenTracingMiddleware()
        assert getattr(settings, 'OPENTRACING_TRACING', None) is not None
        assert settings.OPENTRACING_TRACING.tracer is opentracing.tracer
        assert settings.OPENTRACING_TRACING._get_tracer_impl() is None

    def test_set_global_tracer(self):
        tracer = MockTracer()
        settings.OPENTRACING_TRACING = DjangoTracing(tracer)
        settings.OPENTRACING_SET_GLOBAL_TRACER = True
        with mock.patch('opentracing.tracer'):
            OpenTracingMiddleware()
            assert opentracing.tracer is tracer

        settings.OPENTRACING_SET_GLOBAL_TRACER = False
        with mock.patch('opentracing.tracer'):
            OpenTracingMiddleware()
            assert opentracing.tracer is not tracer

    def test_set_global_tracer_no_tracing(self):
        settings.OPENTRACING_SET_GLOBAL_TRACER = True
        with mock.patch('opentracing.tracer'):
            OpenTracingMiddleware()
            assert getattr(settings, 'OPENTRACING_TRACING', None) is not None
            assert settings.OPENTRACING_TRACING.tracer is opentracing.tracer
            assert settings.OPENTRACING_TRACING._get_tracer_impl() is None
