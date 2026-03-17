# coding: utf-8

from __future__ import unicode_literals

import json
import logging
import time
import uuid
import six

from ylog.context import log_context, get_log_context, put_to_context, pop_from_context
from django.db import connections
from django.conf import settings
from django.utils.encoding import force_bytes
from cached_property import cached_property
from monotonic import monotonic
from requests.sessions import Session
from .state import get_state

try:
    from uwsgi import set_logvar as uwsgi_set_logvar
    uwsgi_loaded = True
except ImportError:
    uwsgi_set_logvar = lambda key, value: None
    uwsgi_loaded = False


def flat_dict(d, parent_keys, result):
    if isinstance(d, dict):
        for key, value in six.iteritems(d):
            flat_dict(value, parent_keys + [key], result)
    else:
        result.append(('.'.join(parent_keys), d))


class DjangoOrmMixin(object):
    def _dump_execution_info(self, state):
        if settings.TOOLS_LOG_CONTEXT_ENABLE_DB_TRACKING:
            with log_context(execution_time=int(state.sql_time)):
                self.log.info('%s executed %s sql queries in %.3f msec', self.code_block_name, state.sql_count, state.sql_time)
        return super(DjangoOrmMixin, self)._dump_execution_info(state)


class RequestsMixin(object):
    def _dump_execution_info(self, state):
        if settings.TOOLS_LOG_CONTEXT_ENABLE_HTTP_TRACKING:
            with log_context(execution_time=int(state.requests_time)):
                self.log.info('%s executed %s http requests in %.3f msec', self.code_block_name, state.requests_count, state.requests_time)
        return super(RequestsMixin, self)._dump_execution_info(state)


class RedisMixin(object):
    def _dump_execution_info(self, state):
        if settings.TOOLS_LOG_CONTEXT_ENABLE_REDIS_TRACKING:
            with log_context(execution_time=int(state.redis_time)):
                self.log.info('%s executed %s redis commands in %.3f msec', self.code_block_name, state.redis_count, state.redis_time)
        return super(RedisMixin, self)._dump_execution_info(state)


class BaseExecutionProfiler(object):
    started_at = None
    code_block_name = None

    connection_names = ['default']

    def __init__(self, code_block_name, code_block_type, threshold):
        """
        @param code_block_name: имя блока, например "GET /homepage", "my_python_function"
        @param code_block_type: тип, например "http request", "custom function" итп
        @param threshold: msec
        """
        self.code_block_name = code_block_name
        self.code_block_type = code_block_type
        self.threshold = threshold

    @cached_property
    def log(self):
        return logging.getLogger(__name__)

    def _dump_execution_info(self, state):
        pass

    def __enter__(self):
        self.started_at = monotonic()
        put_to_context('profiling_uuid', uuid.uuid1().hex)
        state = get_state()
        state.put_state()

    def __exit__(self, exc_type, exc_val, exc_tb):
        profiling_uuid = pop_from_context('profiling_uuid')
        execution_time = (monotonic() - self.started_at) * 1000
        state = get_state()
        with log_context(execution_time=int(execution_time), profiling_uuid=profiling_uuid):
            if execution_time > self.threshold:
                self._dump_execution_info(state.pop_state())
            log_method = self.log.info
            ctx = {}
            if exc_type:
                ctx['unhandled_exception'] = dict(type=repr(exc_type), value=repr(exc_val))
                log_method = self.log.error
            # "profiling" это блок который присутствует только в финальной записи
            # это требование нужно для того чтобы в YQL выбирать было удобно.
            # запрос предполагается строить как WHERE profiling/final=1
            ctx['profiling'] = dict(
                final=1,
                block_name=self.code_block_name,
                block_type=self.code_block_type,
            )

            if uwsgi_loaded:
                uwsgi_log_context = []
                flat_dict(get_log_context(), [], uwsgi_log_context)
                for key, value in uwsgi_log_context:
                    uwsgi_set_logvar(force_bytes(key), force_bytes(value))

            with log_context(**ctx):
                kwargs = {}
                if exc_type is not None:
                    kwargs['exc_info'] = (exc_type, exc_val, exc_tb)
                log_method(
                    '%s %s finished in %.3f msec',
                    self.code_block_type, self.code_block_name, execution_time,
                    **kwargs
                )
        return False


class ExecutionProfiler(DjangoOrmMixin, RequestsMixin, RedisMixin, BaseExecutionProfiler):
    pass


class RequestExecutionProfiler(ExecutionProfiler):
    @staticmethod
    def request_repr(http_request):
        return '{method}: {path}'.format(
            method=http_request.method,
            path=http_request.path,
        )

    def __init__(self, request, threshold):
        super(RequestExecutionProfiler, self).__init__(
            self.request_repr(request),
            code_block_type='http request',
            threshold=threshold,
        )


execution_profiler = ExecutionProfiler
request_profiler = RequestExecutionProfiler
