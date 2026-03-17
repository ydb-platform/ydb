# coding: utf-8

from __future__ import unicode_literals

import logging
from uuid import uuid4

from ylog.context import log_context, get_log_context

from .profiler import execution_profiler


log = logging.getLogger(__name__)


class CtxAwareMixin(object):
    abstract = True
    _execution_threshold = 60 * 1000  # 1 min

    def apply_async(self, args=None, kwargs=None, task_id=None, *positional, **keyword):
        task_id = task_id or str(uuid4())
        log_context_data = get_log_context()
        headers = keyword.get('headers') or {}
        if log_context_data:
            headers['log_context'] = log_context_data
        if headers:
            keyword['headers'] = headers
        log.info('Putting in a queue celery task %s[%s]', self.name, task_id)
        return super(CtxAwareMixin, self).apply_async(args, kwargs, task_id, *positional, **keyword)

    def __call__(self, *args, **kwargs):
        log_context_data = getattr(self.request, 'log_context', None)
        if log_context_data is None:
            headers = self.request.headers or {}
            log_context_data = headers.get('log_context', {})
        log_context_data['celery_task'] = {'name': self.name, 'id': self.request.id}
        with log_context(**log_context_data):
            with execution_profiler(self.name, code_block_type='celery task', threshold=self._execution_threshold):
                log.info('Started celery task %s[%s]', self.name, self.request.id)
                return super(CtxAwareMixin, self).__call__(*args, **kwargs)
