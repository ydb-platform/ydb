"""Model managers."""

import warnings
from functools import wraps
from itertools import count

from celery.utils.time import maybe_timedelta
from django.conf import settings
from django.db import connections, models, router, transaction

from .utils import now

W_ISOLATION_REP = """
Polling results with transaction isolation level 'repeatable-read'
within the same transaction may give outdated results.

Be sure to commit the transaction for each poll iteration.
"""


class TxIsolationWarning(UserWarning):
    """Warning emitted if the transaction isolation level is suboptimal."""


def transaction_retry(max_retries=1):
    """Decorate a function to retry database operations.

    For functions doing database operations, adding
    retrying if the operation fails.

    Keyword Arguments:
        max_retries (int): Maximum number of retries.  Default one retry.

    """
    def _outer(fun):

        @wraps(fun)
        def _inner(*args, **kwargs):
            _max_retries = kwargs.pop('exception_retry_count', max_retries)
            for retries in count(0):
                try:
                    return fun(*args, **kwargs)
                except Exception:   # pragma: no cover
                    # Depending on the database backend used we can experience
                    # various exceptions. E.g. psycopg2 raises an exception
                    # if some operation breaks the transaction, so saving
                    # the task result won't be possible until we rollback
                    # the transaction.
                    if retries >= _max_retries:
                        raise
        return _inner

    return _outer


class ResultManager(models.Manager):
    """Generic manager for celery results."""

    def warn_if_repeatable_read(self):
        if 'mysql' in self.current_engine().lower():
            cursor = self.connection_for_read().cursor()
            # MariaDB and MySQL since 8.0 have different transaction isolation
            # variables: the former has tx_isolation, while the latter has
            # transaction_isolation
            if cursor.execute("SHOW VARIABLES WHERE variable_name IN "
                              "('tx_isolation', 'transaction_isolation');"):
                isolation = cursor.fetchone()[1]
                if isolation == 'REPEATABLE-READ':
                    warnings.warn(TxIsolationWarning(W_ISOLATION_REP.strip()))

    def connection_for_write(self):
        return connections[router.db_for_write(self.model)]

    def connection_for_read(self):
        return connections[self.db]

    def current_engine(self):
        try:
            return settings.DATABASES[self.db]['ENGINE']
        except AttributeError:
            return settings.DATABASE_ENGINE

    def get_all_expired(self, expires):
        """Get all expired results."""
        return self.filter(date_done__lt=now() - maybe_timedelta(expires))

    def delete_expired(self, expires):
        """Delete all expired results."""
        with transaction.atomic(using=self.db):
            self.get_all_expired(expires).delete()


class TaskResultManager(ResultManager):
    """Manager for :class:`~.models.TaskResult` models."""

    _last_id = None

    def get_task(self, task_id):
        """Get result for task by ``task_id``.

        Keyword Arguments:
            exception_retry_count (int): How many times to retry by
                transaction rollback on exception.  This could
                happen in a race condition if another worker is trying to
                create the same task.  The default is to retry once.

        """
        try:
            return self.get(task_id=task_id)
        except self.model.DoesNotExist:
            if self._last_id == task_id:
                self.warn_if_repeatable_read()
            self._last_id = task_id
            return self.model(task_id=task_id)

    @transaction_retry(max_retries=2)
    def store_result(self, content_type, content_encoding,
                     task_id, result, status,
                     traceback=None, meta=None,
                     periodic_task_name=None,
                     task_name=None, task_args=None, task_kwargs=None,
                     worker=None, using=None, **kwargs):
        """Store the result and status of a task.

        Arguments:
            content_type (str): Mime-type of result and meta content.
            content_encoding (str): Type of encoding (e.g. binary/utf-8).
            task_id (str): Id of task.
            periodic_task_name (str): Celery Periodic task name.
            task_name (str): Celery task name.
            task_args (str): Task arguments.
            task_kwargs (str): Task kwargs.
            result (str): The serialized return value of the task,
                or an exception instance raised by the task.
            status (str): Task status.  See :mod:`celery.states` for a list of
                possible status values.
            worker (str): Worker that executes the task.
            using (str): Django database connection to use.
            traceback (str): The traceback string taken at the point of
                exception (only passed if the task failed).
            meta (str): Serialized result meta data (this contains e.g.
                children).

        Keyword Arguments:
            exception_retry_count (int): How many times to retry by
                transaction rollback on exception.  This could
                happen in a race condition if another worker is trying to
                create the same task.  The default is to retry twice.

        """
        fields = {
            'status': status,
            'result': result,
            'traceback': traceback,
            'meta': meta,
            'content_encoding': content_encoding,
            'content_type': content_type,
            'periodic_task_name': periodic_task_name,
            'task_name': task_name,
            'task_args': task_args,
            'task_kwargs': task_kwargs,
            'worker': worker
        }
        if 'date_started' in kwargs:
            fields['date_started'] = kwargs['date_started']

        obj, created = self.using(using).get_or_create(task_id=task_id,
                                                       defaults=fields)
        if not created:
            for k, v in fields.items():
                setattr(obj, k, v)
            obj.save(using=using)
        return obj


class GroupResultManager(ResultManager):
    """Manager for :class:`~.models.GroupResult` models."""

    _last_id = None

    def get_group(self, group_id):
        """Get result for group by ``group_id``.

        Keyword Arguments:
            exception_retry_count (int): How many times to retry by
                transaction rollback on exception.  This could
                happen in a race condition if another worker is trying to
                create the same task.  The default is to retry once.

        """
        try:
            return self.get(group_id=group_id)
        except self.model.DoesNotExist:
            if self._last_id == group_id:
                self.warn_if_repeatable_read()
            self._last_id = group_id
            return self.model(group_id=group_id)

    @transaction_retry(max_retries=2)
    def store_group_result(self, content_type, content_encoding,
                           group_id, result, using=None):
        fields = {
            'result': result,
            'content_encoding': content_encoding,
            'content_type': content_type,
        }

        if not using:
            using = self.db

        obj, created = self.using(using).get_or_create(group_id=group_id,
                                                       defaults=fields)
        if not created:
            for k, v in fields.items():
                setattr(obj, k, v)
            obj.save(using=self.db)
        return obj
