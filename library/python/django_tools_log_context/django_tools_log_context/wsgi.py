# coding: utf-8
from __future__ import absolute_import, unicode_literals

from functools import wraps

import django
from django.core.handlers.wsgi import WSGIHandler
from django_tools_log_context import request_log_context


class LogContextWSGIHandler(WSGIHandler):
    """Custom WSGIHangler to wrap views with request_log_context"""
    def make_view_atomic(self, view):
        view = super(LogContextWSGIHandler, self).make_view_atomic(view)

        @wraps(view)
        def wrapped(request, *args, **kwargs):
            with request_log_context(request, endpoint=view, **kwargs):
                return view(request, *args, **kwargs)

        return wrapped


def log_context_get_wsgi_application():
    django.setup(set_prefix=False)
    return LogContextWSGIHandler()
