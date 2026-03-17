from django.http import HttpResponse
from django.conf import settings

tracing = settings.OPENTRACING_TRACING


def index(request):
    return HttpResponse("index")


@tracing.trace('path', 'scheme', 'fake_setting')
def traced_func_with_attrs(request):
    currentSpanCount = len(settings.OPENTRACING_TRACING._current_scopes)
    response = HttpResponse()
    response['numspans'] = currentSpanCount
    return response


@tracing.trace()
def traced_func(request):
    currentSpanCount = len(settings.OPENTRACING_TRACING._current_scopes)
    response = HttpResponse()
    response['numspans'] = currentSpanCount
    return response


@tracing.trace()
def traced_func_with_arg(request, arg):
    currentSpanCount = len(settings.OPENTRACING_TRACING._current_scopes)
    response = HttpResponse()
    response['numspans'] = currentSpanCount
    response['arg'] = arg
    return response


@tracing.trace()
def traced_func_with_error(request):
    raise ValueError('key')


def untraced_func(request):
    currentSpanCount = len(settings.OPENTRACING_TRACING._current_scopes)
    response = HttpResponse()
    response['numspans'] = currentSpanCount
    return response


@tracing.trace()
def traced_scope_func(request):
    response = HttpResponse()
    response['active_span'] = tracing._tracer.active_span
    response['request_span'] = tracing.get_span(request)
    return response
