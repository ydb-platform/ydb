# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import logging
import re
import six
import traceback
from functools import wraps
from monotonic import monotonic
from ylog.context import log_context

if six.PY2:
    from urlparse import urlparse
elif six.PY3:
    from urllib.parse import urlparse

from django.conf import settings
from django.utils.encoding import force_str
from .state import get_state
from .utils import dynamic_import


module_path_map = {
    module_path: None
    for module_path in settings.TOOLS_LOG_CONTEXT_REQUESTS_MODULES
}


def enable_instrumentation():
    for module_path, send_ref in module_path_map.items():
        if send_ref is None:
            session_class = dynamic_import(module_path, 'Session')
            if session_class is not None:
                session_send = getattr(session_class, 'send')
                setattr(session_class, 'send', fake_session_send(session_send))
                module_path_map[module_path] = session_send


def disable_instrumentation():
    for module_path, send_ref in module_path_map.items():
        if send_ref is not None:
            session_class = dynamic_import(module_path, 'Session')
            if session_class is not None:
                setattr(session_class, 'send', send_ref)
                module_path_map[module_path] = None


def fake_session_send(old_session_send):
    sessionid_re = re.compile(r'(sessionid|ts_sign|oauth_token)=([^&]+)', re.IGNORECASE)
    state = get_state()
    logger = logging.getLogger(__name__)

    @wraps(old_session_send)
    def wrapped(self, request, **kwargs):
        from django.conf import settings
        response = None
        start_time = monotonic()
        try:
            response = old_session_send(self, request, **kwargs)
            return response
        finally:
            duration = (monotonic() - start_time) * 1000
            elapsed = getattr(response, 'elapsed', None)
            if elapsed:
                duration = elapsed.total_seconds() * 1000

            state.add_requests_time(duration)

            if settings.TOOLS_LOG_CONTEXT_ENABLE_HTTP_TRACKING and state.is_enabled():
                url = sessionid_re.sub(r'\1=xxxxx', request.url)
                params = {}
                status_code = getattr(response, 'status_code', None)
                parsed_url = urlparse(url)
                profiling = {
                    'method': request.method,
                    'status_code': status_code,
                    'hostname': parsed_url.hostname,
                    'path': parsed_url.path,
                    'query': parsed_url.query,
                    'vendor': 'requests',
                    'query_to_analyse': '%s %s' % (request.method, parsed_url._replace(query='', path='').geturl()),
                    'content': None,
                }
                if settings.TOOLS_LOG_CONTEXT_RESPONSE_MAX_SIZE > 0:
                    if status_code not in (None, 200, 404):
                        content = force_str(getattr(response, 'content', ''))
                        profiling['content'] = content[:settings.TOOLS_LOG_CONTEXT_RESPONSE_MAX_SIZE]

                if settings.TOOLS_LOG_CONTEXT_ENABLE_STACKTRACES:
                    profiling['stacktrace'] = ''.join(i.decode('utf-8') if six.PY2 else i for i in traceback.format_stack()[:-1])

                params['profiling'] = profiling

                log_level = logging.INFO
                if status_code:
                    if status_code >= 500:
                        log_level = logging.ERROR
                    elif settings.TOOLS_LOG_CONTEXT_ENABLE_HTTP_400_WARNING and status_code >= 400:
                        log_level = logging.WARNING

                with log_context(execution_time=int(duration), **params):
                    logger.log(log_level, '(%.3f msec) %s %s', duration, request.method, url)
    return wrapped
