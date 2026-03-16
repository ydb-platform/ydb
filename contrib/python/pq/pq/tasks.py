# -*- coding: utf-8 -*-
from logging import getLogger
from functools import wraps
from pydoc import locate
from . import (
    PQ as BasePQ,
    Queue as BaseQueue,
)


def task(
    queue,
    schedule_at=None,
    expected_at=None,
    max_retries=0,
    retry_in='30s',
):
    def decorator(f):
        f._path = "%s.%s" % (f.__module__, f.__name__)
        f._max_retries = max_retries
        f._retry_in = retry_in

        queue.handler_registry[f._path] = f

        @wraps(f)
        def wrapper(*args, **kwargs):
            _schedule_at = kwargs.pop('_schedule_at', None)
            _expected_at = kwargs.pop('_expected_at', None)

            put_kwargs = dict(
                schedule_at=_schedule_at or schedule_at,
                expected_at=_expected_at or expected_at,
            )

            return queue.put(
                dict(
                    function=f._path,
                    args=args,
                    kwargs=kwargs,
                    retried=0,
                    retry_in=f._retry_in,
                    max_retries=f._max_retries,
                ),
                **put_kwargs
            )

        return wrapper

    return decorator


class Queue(BaseQueue):
    handler_registry = dict()
    logger = getLogger('pq.tasks')

    def fail(self, job, data, e=None):
        retried = data['retried']

        if data.get('max_retries', 0) > retried:
            data.update(dict(
                retried=retried + 1,
            ))
            id = self.put(data, schedule_at=data['retry_in'])
            self.logger.info("Rescheduled %r as `%s`" % (job, id))

            return False

        self.logger.warning("Failed to perform job %r :" % job)
        self.logger.exception(e)

        return False

    def perform(self, job):
        data = job.data
        function_path = data['function']

        f = self.handler_registry.get(function_path)

        if function_path not in self.handler_registry:
            f = self.handler_registry[function_path] = locate(function_path)

        if f is None:
            return self.fail(job, data, KeyError(
                "Job handler `%s` not found." % function_path,
            ))

        try:
            f(job.id, *data['args'], **data['kwargs'])
            return True

        except Exception as e:
            return self.fail(job, data, e)

    task = task

    def work(self, burst=False):
        """Starts processing jobs."""
        self.logger.info('`%s` starting to perform jobs' % self.name)

        for job in self:
            if job is None:
                if burst:
                    return

                continue

            self.perform(job)


class PQ(BasePQ):
    queue_class = Queue
