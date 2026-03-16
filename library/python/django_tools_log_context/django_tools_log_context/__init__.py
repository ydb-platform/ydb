# coding: utf-8

from __future__ import unicode_literals

from .http import HttpRequestContext
from .profiler import execution_profiler, request_profiler

default_app_config = 'django_tools_log_context.apps.DjangoToolsLogContextConfig'

request_context = HttpRequestContext


class RequestLogContext(HttpRequestContext):
    def __init__(self, request, **kwargs):
        threshold = kwargs.pop('threshold', 0)
        self.profiler = request_profiler(request, threshold)
        super(RequestLogContext, self).__init__(request, **kwargs)

    def __enter__(self):
        super(RequestLogContext, self).__enter__()
        self.profiler.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.profiler.__exit__(*args, **kwargs)
        super(RequestLogContext, self).__exit__(*args, **kwargs)


request_log_context = RequestLogContext
