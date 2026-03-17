# -*- coding: utf-8 -*-
import os
import json
import sys

from contextlib import contextmanager
from select import select
from logging import getLogger
from hashlib import md5
from weakref import WeakValueDictionary

from .utils import (
    Literal, prepared, transaction, convert_time_spec, utc_format
)

import library.python.resource as rs


__title__ = 'pq'
__version__ = '1.9.1'
__author__ = 'Malthe Borch'
__license__ = 'BSD'


if sys.version_info[0] == 2:
    import cPickle as pickle
else:
    import pickle as pickle


class PQ(object):
    """Convenient queue manager."""

    table = 'queue'

    queue_class = None

    template_path = os.path.dirname(__file__)

    def __init__(self, *args, **kwargs):
        queue_class = kwargs.pop('queue_class', None)
        if queue_class is not None:
            self.queue_class = queue_class
        self.params = args, kwargs
        self.queues = WeakValueDictionary()

    def __getitem__(self, name):
        try:
            return self.queues[name]
        except KeyError:
            factory = self.queue_class
            if factory is None:
                factory = Queue
            return self.queues.setdefault(
                name, factory(name, *self.params[0], **self.params[1])
            )

    def close(self):
        self[''].close()

    def create(self):
        queue = self['']

        sql = rs.resfs_read('contrib/python/pq/pq/create.sql')

        with queue._transaction() as cursor:
            cursor.execute(
                sql, {
                    'name': Literal(queue.name),
                    'table': queue.table,
                }
            )


class QueueIterator(object):
    """Returns a queue iterator.

    If the `timeout` attribute is set, then the iterator uses the
    smallest value of the time until next scheduled item is available
    and this setting.
    """

    timeout = None

    def __init__(self, queue):
        self.queue = queue

    def __iter__(self):
        return self

    def __next__(self):
        ''' Python 3 iterator.
        '''
        return self.queue.get(timeout=self.timeout)

    def next(self):
        ''' Python 2 iterator.
        '''
        return QueueIterator.__next__(self)


class Queue(object):
    """Simple thread-safe transactional queue."""

    # This timeout is used during iteration. If the timeout elapses
    # and no item was pulled from the queue, the iteration loop
    # returns ``None``.
    timeout = 1
    last_timeout = None

    # Keyword arguments passed when creating a new cursor.
    cursor_kwargs = {}

    logger = getLogger('pq')

    converters = {
        'pickle': (
            #
            # Pickle protocol 0 claims to be ASCII, but it outputs
            # characters outside of range(128). So, we treat it
            # like Latin-1 so that it can encoded into JSON UTF-8.
            #
            lambda data: pickle.dumps(data, 0).decode('latin-1'),
            lambda data: pickle.loads(data.encode('latin-1'))
            ),
    }

    dumps = loads = staticmethod(lambda data: data)

    encode = staticmethod(json.dumps)
    decode = staticmethod(json.loads)

    ctx = cursor = None

    def __init__(self, name, conn=None, pool=None, table='queue', schema=None, **kwargs):
        self.conn = conn
        self.pool = pool

        if '/' in name:
            name, key = name.rsplit('/', 1)
            self.dumps, self.loads = self.converters[key]

        self.name = name
        self.table = Literal((schema + "." if schema else "") + table)

        # Set additional options.
        self.__dict__.update(kwargs)

    def __enter__(self):
        self.ctx = self._transaction()
        self.cursor = self.ctx.__enter__()
        return self.cursor

    def __exit__(self, *args):
        try:
            self.ctx.__exit__(*args)
        finally:
            del self.cursor
            del self.ctx

    def __iter__(self):
        return QueueIterator(self)

    def __len__(self):
        with self._transaction() as cursor:
            return self._count(cursor)

    def close(self):
        """Close the queue connection."""

        if self.conn is not None:
            self.conn.close()
        else:
            self.pool.closeall()

    def get(self, block=True, timeout=None):
        """Pull item from queue."""

        self.timeout = timeout or self.timeout

        while True:
            with self._transaction() as cursor:
                (
                    job_id,
                    data,
                    size,
                    enqueued_at,
                    schedule_at,
                    expected_at,
                    seconds,
                ) = self._pull_item(
                    cursor, block
                )
                self.last_timeout = (
                    seconds or self.last_timeout or self.timeout
                )

            if data is not None:
                # Reset the timeout if there's no estimation
                if seconds is None or seconds < 0:
                    self.last_timeout = self.timeout

                decoded = self.decode(data)

                return Job(
                    job_id, self.loads(decoded), size,
                    enqueued_at, schedule_at, expected_at, self.update
                )

            if not block:
                return

            self.last_timeout = min(self.last_timeout, self.timeout)

            if not self._select(self.last_timeout):
                block = False

    def put(self, data, schedule_at=None, expected_at=None):
        """Put item into queue.

        If `schedule_at` is provided, the item is not dequeued until
        the provided time.

        The argument may be specified as one of the following:

        - a `datetime` object
        - a `timedelta` object
        - a string on the form '%d(s|m|h|d)`.

        In the last form, the unit indicated is seconds, minutes,
        hours and days, respectively.
        """

        schedule_at = convert_time_spec(schedule_at)
        expected_at = convert_time_spec(expected_at)

        with self._transaction() as cursor:
            return self._put_item(
                cursor, self.encode(self.dumps(data)),
                utc_format(schedule_at) if schedule_at is not None else None,
                utc_format(expected_at) if expected_at is not None else None,
            )

    def update(self, job_id, data):
        """Update job data."""

        with self._transaction() as cursor:
            return self._update_item(
                cursor, job_id, self.encode(self.dumps(data))
            )

    def clear(self):
        with self._transaction() as cursor:
            cursor.execute(
                "DELETE FROM %s WHERE q_name = %s", (
                    self.table,
                    self.name
                )
            )

    @contextmanager
    def _conn(self):
        if self.pool:
            conn = self.pool.getconn()
            try:
                yield conn
            finally:
                self.pool.putconn(conn)
        else:
            yield self.conn

    def _listen(self, cursor):
        name = self.name
        if len(name) > 63:
            digest = md5(name.encode()).hexdigest()
            name = "pq_" + digest
        cursor.execute('LISTEN "%s"', (Literal(name), ))

    @prepared
    def _put_item(self, cursor):
        """Puts a single item into the queue.

            INSERT INTO %(table)s (q_name, data, schedule_at, expected_at)
            VALUES (%(name)s, $1, $2, $3) RETURNING id

        This method expects a string argument which is the item data
        and the scheduling timestamp.
        """

        return cursor.fetchone()[0]

    @prepared
    def _update_item(self, cursor):
        """Updates a single item into the queue.

            UPDATE %(table)s SET data = $2 WHERE id = $1
            RETURNING length(data::text)

        """

        return cursor.fetchone()[0]

    @prepared
    def _pull_item(self, cursor, blocking):
        """Return a single item from the queue.

        Priority is given to items that are ready for work and
        requested earliest in terms of their expected time.

        This method uses the following query:

            WITH
              selected AS (
                SELECT * FROM %(table)s
                WHERE
                  q_name = %(name)s AND
                  dequeued_at IS NULL
                ORDER BY schedule_at nulls first, expected_at nulls last, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
              ),
              updated AS (
                UPDATE %(table)s AS t SET dequeued_at = current_timestamp
                FROM selected
                WHERE
                  t.id = selected.id AND
                  (t.schedule_at <= now() OR t.schedule_at is NULL)
                RETURNING t.data, length(t.data::text) AS length
              )
            SELECT
              id,
              (SELECT data::text FROM updated),
              (SELECT length FROM updated),
              enqueued_at AT TIME ZONE 'utc' AS enqueued_at,
              schedule_at AT TIME ZONE 'utc' AS schedule_at,
              expected_at AT TIME ZONE 'utc' AS expected_at,
              (date_part(
                'second', (
                  (SELECT schedule_at - now() FROM selected))))
            FROM selected

        If `blocking` is set, the item blocks until an item is ready
        or the timeout has been reached.

        """

        row = cursor.fetchone()
        if row is None:
            if blocking:
                self._listen(cursor)

            return None, None, None, None, None, None, None

        return row

    @prepared
    def _count(self, cursor):
        """Return number of items in queue.

            SELECT COUNT(*) FROM %(table)s
            WHERE q_name = %(name)s AND dequeued_at IS NULL
              AND (schedule_at IS NULL OR schedule_at <= NOW())

        """

        return cursor.fetchone()[0]

    def _select(self, timeout):
        with self._conn() as conn:
            r, w, x = select([conn], [], [], timeout)
        has_data = bool(r or w or x)
        if not has_data:
            self.logger.debug("timeout (%.3f seconds)." % timeout)
        return has_data

    @contextmanager
    def _transaction(self):
        if self.cursor is not None:
            self.cursor.execute("SAVEPOINT pq")
            try:
                yield self.cursor
            except:
                self.cursor.execute("ROLLBACK TO SAVEPOINT pq")
                raise
            self.cursor.execute("RELEASE SAVEPOINT pq")
            return

        with self._conn() as conn, transaction(conn, **self.cursor_kwargs) \
                as cursor:
            yield cursor


class Job(object):
    """An item in the queue."""

    __slots__ = (
        "_data", "_size", "_update", "id", "enqueued_at", "schedule_at",
        "expected_at",
    )

    def __init__(
        self,
        job_id,
        data,
        size,
        enqueued_at,
        schedule_at,
        expected_at,
        update
    ):
        self._data = data
        self._size = size
        self._update = update
        self.id = job_id
        self.enqueued_at = enqueued_at
        self.schedule_at = schedule_at
        self.expected_at = expected_at

    def __repr__(self):
        cls = type(self)
        return (
            '<%s.%s id=%d size=%d enqueued_at=%r '
            'schedule_at=%r expected_at=%r>' % (
                cls.__module__,
                cls.__name__,
                self.id,
                self.size,
                utc_format(self.enqueued_at),
                utc_format(self.schedule_at) if self.schedule_at else None,
                utc_format(self.expected_at) if self.expected_at else None,
            )
        ).replace("'", '"')

    @property
    def size(self):
        return self._size

    def get_data(self):
        return self._data

    def set_data(self, data):
        self._size = self._update(self.id, data)
        self._data = data

    data = property(get_data, set_data)

    del get_data
    del set_data
