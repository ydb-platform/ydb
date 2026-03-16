PQ
**

A transactional queue system for PostgreSQL written in Python.

.. figure:: https://pq.readthedocs.org/en/latest/_static/intro.svg
   :alt: PQ does the job!

It allows you to push and pop items in and out of a queue in various
ways and also provides two scheduling options: delayed processing and
prioritization.

The system uses a single table that holds all jobs across queues; the
specifics are easy to customize.

The system currently supports only the `psycopg2
<https://pypi.python.org/pypi/psycopg2>`_ database driver - or
`psycopg2cffi <https://pypi.python.org/pypi/psycopg2cffi>`_ for PyPy.

The basic queue implementation is similar to Ryan Smith's
`queue_classic <https://github.com/ryandotsmith/queue_classic>`_
library written in Ruby, but uses `SKIP LOCKED
<https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/>`_
for concurrency control.

In terms of performance, the implementation clock in at about 1,000
operations per second. Using the `PyPy <http://pypy.org/>`_
interpreter, this scales linearly with the number of cores available.


Getting started
===============

All functionality is encapsulated in a single class ``PQ``.

     ``class PQ(conn=None, pool=None, table="queue", schema=None)``

The optional ``schema`` argument can be used to qualify the table with
a schema if necessary.

Example usage:

.. code-block:: python

    from psycopg2 import connect
    from pq import PQ

    conn = connect('dbname=example user=postgres')
    pq = PQ(conn)

For multi-threaded operation, use a connection pool such as
``psycopg2.pool.ThreadedConnectionPool``.

You probably want to make sure your database is created with the
``utf-8`` encoding.

To create and configure the queue table, call the ``create()`` method.

.. code-block:: python

    pq.create()


Queues
======

The ``pq`` object exposes queues through Python's dictionary
interface:

.. code-block:: python

    queue = pq['apples']

The ``queue`` object provides ``get`` and ``put`` methods as explained
below, and in addition, it also works as a context manager where it
manages a transaction:

.. code-block:: python

    with queue as cursor:
        ...

The statements inside the context manager are either committed as a
transaction or rejected, atomically. This is useful when a queue is
used to manage jobs because it allows you to retrieve a job from the
queue, perform a job and write a result, with transactional
semantics.

Methods
=======

Use the ``put(data)`` method to insert an item into the queue. It
takes a JSON-compatible object such as a Python dictionary:

.. code-block:: python

    queue.put({'kind': 'Cox'})
    queue.put({'kind': 'Arthur Turner'})
    queue.put({'kind': 'Golden Delicious'})

Items are pulled out of the queue using ``get(block=True)``. The
default behavior is to block until an item is available with a default
timeout of one second after which a value of ``None`` is returned.

.. code-block:: python

    def eat(kind):
        print 'umm, %s apples taste good.' % kind

    job = queue.get()
    eat(**job.data)

The ``job`` object provides additional metadata in addition to the
``data`` attribute as illustrated by the string representation:

    >>> job
    <pq.Job id=77709 size=1 enqueued_at="2014-02-21T16:22:06Z" schedule_at=None>

The ``get`` operation is also available through iteration:

.. code-block:: python

    for job in queue:
        if job is None:
            break

        eat(**job.data)

The iterator blocks if no item is available. Again, there is a default
timeout of one second, after which the iterator yields a value of
``None``.

An application can then choose to break out of the loop, or wait again
for an item to be ready.

.. code-block:: python

    for job in queue:
        if job is not None:
            eat(**job.data)

        # This is an infinite loop!


Scheduling
==========

Items can be scheduled such that they're not pulled until a later
time:

.. code-block:: python

    queue.put({'kind': 'Cox'}, '5m')

In this example, the item is ready for work five minutes later. The
method also accepts ``datetime`` and ``timedelta`` objects.


Priority
========

If some items are more important than others, a time expectation can
be expressed:

.. code-block:: python

    queue.put({'kind': 'Cox'}, expected_at='5m')

This tells the queue processor to give priority to this item over an
item expected at a later time, and conversely, to prefer an item with
an earlier expected time. Note that items without a set priority are
pulled last.

The scheduling and priority options can be combined:

.. code-block:: python

    queue.put({'kind': 'Cox'}, '1h', '2h')

This item won't be pulled out until after one hour, and even then,
it's only processed subject to it's priority of two hours.


Encoding and decoding
=====================

The task data is encoded and decoded into JSON using the built-in
`json` module. If you want to use a different implementation or need
to configure this, pass `encode` and/or `decode` arguments to the `PQ`
constructor.


Pickles
=======

If a queue name is provided as ``<name>/pickle``
(e.g. ``'jobs/pickle'``), items are automatically pickled and
unpickled using Python's built-in ``cPickle`` module:

.. code-block:: python

    queue = pq['apples/pickle']

    class Apple(object):
        def __init__(self, kind):
           self.kind = kind

    queue.put(Apple('Cox'))

This allows you to store most objects without having to add any
further serialization code.

The old pickle protocol ``0`` is used to ensure the pickled data is
encoded as ``ascii`` which should be compatible with any database
encoding. Note that the pickle data is still wrapped as a JSON string at the
database level.

While using the pickle protocol is an easy way to serialize objects,
for advanced users t might be better to use JSON serialization
directly on the objects, using for example the object hook mechanism
in the built-in `json` module or subclassing
`JSONEncoder <https://docs.python.org/2/library/json.html#json.JSONEncoder>`.


Tasks
=====

``pq`` comes with a higher level ``API`` that helps to manage ``tasks``.


.. code-block:: python

    from pq.tasks import PQ

    pq = PQ(...)

    queue = pq['default']

    @queue.task(schedule_at='1h')
    def eat(job_id, kind):
        print 'umm, %s apples taste good.' % kind

    eat('Cox')

    queue.work()


``tasks``'s ``jobs`` can optionally be re-scheduled on failure:

.. code-block:: python

    @queue.task(schedule_at='1h', max_retries=2, retry_in='10s')
    def eat(job_id, kind):
        # ...


Time expectations can be overriden at ``task`` call:

.. code-block:: python

    eat('Cox', _expected_at='2m', _schedule_at='1m')


** NOTE ** First positional argument is id of job. It's PK of record in PostgreSQL.

Thread-safety
=============

All objects are thread-safe as long as a connection pool is provided
where each thread receives its own database connection.
