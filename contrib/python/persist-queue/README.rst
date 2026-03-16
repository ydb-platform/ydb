persist-queue - A thread-safe, disk-based queue for Python
==========================================================

.. image:: https://img.shields.io/circleci/project/github/peter-wangxu/persist-queue/master.svg?label=Linux%20%26%20Mac
    :target: https://circleci.com/gh/peter-wangxu/persist-queue

.. image:: https://img.shields.io/appveyor/ci/peter-wangxu/persist-queue/master.svg?label=Windows
    :target: https://ci.appveyor.com/project/peter-wangxu/persist-queue

.. image:: https://img.shields.io/codecov/c/github/peter-wangxu/persist-queue/master.svg
    :target: https://codecov.io/gh/peter-wangxu/persist-queue

.. image:: https://img.shields.io/pypi/v/persist-queue.svg
    :target: https://pypi.python.org/pypi/persist-queue

.. image:: https://img.shields.io/pypi/pyversions/persist-queue
   :alt: PyPI - Python Version

Overview
--------

``persist-queue`` implements file-based and SQLite3-based persistent queues for Python. 
It provides thread-safe, disk-based queue implementations that survive process crashes 
and restarts.

By default, *persist-queue* use *pickle* object serialization module to support object instances.
Most built-in type, like `int`, `dict`, `list` are able to be persisted by `persist-queue` directly, to support customized objects,
please refer to `Pickling and unpickling extension types(Python2) <https://docs.python.org/2/library/pickle.html#pickling-and-unpickling-normal-class-instances>`_
and `Pickling Class Instances(Python3) <https://docs.python.org/3/library/pickle.html#pickling-class-instances>`_

This project is based on the achievements of `python-pqueue <https://github.com/balena/python-pqueue>`_
and `queuelib <https://github.com/scrapy/queuelib>`_

Key Features
^^^^^^^^^^^^

* **Disk-based**: Each queued item is stored on disk to survive crashes
* **Thread-safe**: Supports multi-threaded producers and consumers
* **Recoverable**: Items can be read after process restart
* **Green-compatible**: Works with ``greenlet`` or ``eventlet`` environments
* **Multiple serialization**: Supports pickle (default), msgpack, cbor, and json
* **Async support**: Provides async versions of all queue types (v1.1.0+)

Supported Queue Types
^^^^^^^^^^^^^^^^^^^^^

**File-based Queues:**

* ``Queue`` - Basic file-based FIFO queue
* ``AsyncQueue`` - Async file-based queue (v1.1.0+)

**SQLite-based Queues:**

* ``SQLiteQueue`` / ``FIFOSQLiteQueue`` - FIFO SQLite queue
* ``FILOSQLiteQueue`` - FILO SQLite queue
* ``UniqueQ`` - Unique items only queue
* ``PriorityQueue`` - Priority-based queue
* ``SQLiteAckQueue`` - Acknowledgment-based queue
* ``AsyncSQLiteQueue`` - Async SQLite queue (v1.1.0+)

**Other:**

* ``PDict`` - Persistent dictionary
* ``MySQLQueue`` - MySQL-based queue (requires extra dependencies)

Installation
------------

Basic Installation
^^^^^^^^^^^^^^^^^^

.. code-block:: console

    pip install persist-queue

With Extra Features
^^^^^^^^^^^^^^^^^^^

.. code-block:: console

    # For msgpack, cbor, and MySQL support
    pip install "persist-queue[extra]"
    
    # For async support (requires Python 3.7+)
    pip install "persist-queue[async]"
    
    # For all features
    pip install "persist-queue[extra,async]"

From Source
^^^^^^^^^^^

.. code-block:: console

    git clone https://github.com/peter-wangxu/persist-queue
    cd persist-queue
    python setup.py install

Requirements
------------

* Python 3.5 or newer (Python 2 support dropped in v1.0.0)
* Full support for Linux, macOS, and Windows
* For async features: Python 3.7+ with aiofiles and aiosqlite
* For MySQL queues: DBUtils and PyMySQL

Quick Start
-----------

Basic File Queue
^^^^^^^^^^^^^^^^

.. code-block:: python

    from persistqueue import Queue
    
    # Create a queue
    q = Queue("my_queue_path")
    
    # Add items
    q.put("item1")
    q.put("item2")
    
    # Get items
    item = q.get()
    print(item)  # "item1"
    
    # Mark as done
    q.task_done()

SQLite Queue
^^^^^^^^^^^^

.. code-block:: python

    import persistqueue
    
    # Create SQLite queue
    q = persistqueue.SQLiteQueue('my_queue.db', auto_commit=True)
    
    # Add items
    q.put('data1')
    q.put('data2')
    
    # Get items
    item = q.get()
    print(item)  # "data1"

MySQL Queue
^^^^^^^^^^^

.. code-block:: python

    import persistqueue
    
    # Create MySQL queue
    q = persistqueue.MySQLQueue(
        host='localhost',
        port=3306,
        user='username',
        password='password',
        database='testdb',
        table_name='my_queue'
    )
    
    # Add items
    q.put('data1')
    q.put('data2')
    
    # Get items
    item = q.get()
    print(item)  # "data1"
    
    # Mark as done
    q.task_done()

Async Queue (v1.1.0+)
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import asyncio
    from persistqueue import AsyncQueue
    
    async def main():
        async with AsyncQueue("/path/to/queue") as queue:
            await queue.put("async item")
            item = await queue.get()
            await queue.task_done()
    
    asyncio.run(main())

Examples
--------

File-based Queue
^^^^^^^^^^^^^^^^

.. code-block:: python

    >>> from persistqueue import Queue
    >>> q = Queue("mypath")
    >>> q.put('a')
    >>> q.put('b')
    >>> q.put('c')
    >>> q.get()
    'a'
    >>> q.task_done()

SQLite3-based Queue
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    >>> import persistqueue
    >>> q = persistqueue.SQLiteQueue('mypath', auto_commit=True)
    >>> q.put('str1')
    >>> q.put('str2')
    >>> q.put('str3')
    >>> q.get()
    'str1'
    >>> del q

Priority Queue
^^^^^^^^^^^^^^

.. code-block:: python

    >>> import persistqueue
    >>> q = persistqueue.PriorityQueue('mypath')
    >>> q.put('low', priority=10)
    >>> q.put('high', priority=1)
    >>> q.put('mid', priority=5)
    >>> q.get()
    'high'
    >>> q.get()
    'mid'
    >>> q.get()
    'low'

Unique Queue
^^^^^^^^^^^^

.. code-block:: python

    >>> import persistqueue
    >>> q = persistqueue.UniqueQ('mypath')
    >>> q.put('str1')
    >>> q.put('str1')  # Duplicate ignored
    >>> q.size
    1
    >>> q.put('str2')
    >>> q.size
    2

Acknowledgment Queue
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    >>> import persistqueue
    >>> ackq = persistqueue.SQLiteAckQueue('path')
    >>> ackq.put('str1')
    >>> item = ackq.get()
    >>> # Process the item
    >>> ackq.ack(item)  # Mark as completed
    >>> # Or if processing failed:
    >>> ackq.nack(item)  # Mark for retry
    >>> ackq.ack_failed(item)  # Mark as failed

MySQL Queue
^^^^^^^^^^^

.. code-block:: python

    >>> import persistqueue
    >>> q = persistqueue.MySQLQueue(
    ...     host='localhost',
    ...     port=3306,
    ...     user='testuser',
    ...     password='testpass',
    ...     database='testdb',
    ...     table_name='test_queue'
    ... )
    >>> q.put('item1')
    >>> q.put('item2')
    >>> q.put('item3')
    >>> q.get()
    'item1'
    >>> q.task_done()
    >>> q.get()
    'item2'
    >>> q.task_done()
    >>> q.size
    1

Async Queue (v1.1.0+)
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import asyncio
    from persistqueue import AsyncQueue, AsyncSQLiteQueue

    async def example():
        # File-based async queue
        async with AsyncQueue("/path/to/queue") as queue:
            await queue.put("data item")
            item = await queue.get()
            await queue.task_done()
        
        # SQLite-based async queue
        async with AsyncSQLiteQueue("/path/to/queue.db") as queue:
            item_id = await queue.put({"key": "value"})
            item = await queue.get()
            await queue.update({"key": "new_value"}, item_id)
            await queue.task_done()

    asyncio.run(example())

Persistent Dictionary
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    >>> from persistqueue import PDict
    >>> q = PDict("testpath", "testname")
    >>> q['key1'] = 123
    >>> q['key2'] = 321
    >>> q['key1']
    123
    >>> len(q)
    2
    >>> del q['key1']
    >>> q['key1']
    KeyError: 'Key: key1 not exists.'

Multi-threading Usage
---------------------

SQLite3-based Queue
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from persistqueue import FIFOSQLiteQueue
    from threading import Thread

    q = FIFOSQLiteQueue(path="./test", multithreading=True)

    def worker():
        while True:
            item = q.get()
            do_work(item)

    for i in range(num_worker_threads):
         t = Thread(target=worker)
         t.daemon = True
         t.start()

    for item in source():
        q.put(item)

    q.join()  # Block until all tasks are done

File-based Queue
^^^^^^^^^^^^^^^^

.. code-block:: python

    from persistqueue import Queue
    from threading import Thread

    q = Queue()

    def worker():
        while True:
            item = q.get()
            do_work(item)
            q.task_done()

    for i in range(num_worker_threads):
         t = Thread(target=worker)
         t.daemon = True
         t.start()

    for item in source():
        q.put(item)

    q.join()  # Block until all tasks are done

MySQL Queue
^^^^^^^^^^^

.. code-block:: python

    from persistqueue import MySQLQueue
    from threading import Thread

    q = MySQLQueue(
        host='localhost',
        port=3306,
        user='username',
        password='password',
        database='testdb',
        table_name='my_queue'
    )

    def worker():
        while True:
            item = q.get()
            do_work(item)
            q.task_done()

    for i in range(num_worker_threads):
         t = Thread(target=worker)
         t.daemon = True
         t.start()

    for item in source():
        q.put(item)

    q.join()  # Block until all tasks are done

Serialization Options
---------------------

persist-queue supports multiple serialization protocols:

.. code-block:: python

    >>> from persistqueue import Queue
    >>> from persistqueue import serializers
    
    # Pickle (default)
    >>> q = Queue('mypath', serializer=serializers.pickle)
    
    # MessagePack
    >>> q = Queue('mypath', serializer=serializers.msgpack)
    
    # CBOR
    >>> q = Queue('mypath', serializer=serializers.cbor2)
    
    # JSON
    >>> q = Queue('mypath', serializer=serializers.json)

Performance
-----------

Benchmark Results (1000 items)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Windows (Windows 10, SATA3 SSD, 16GB RAM)**

+---------------+---------+-------------------------+----------------------------+
|               | Write   | Write/Read(1 task_done) | Write/Read(many task_done) |
+---------------+---------+-------------------------+----------------------------+
| SQLite3 Queue | 1.8880  | 2.0290                  | 3.5940                     |
+---------------+---------+-------------------------+----------------------------+
| File Queue    | 4.9520  | 5.0560                  | 8.4900                     |
+---------------+---------+-------------------------+----------------------------+

Benchmarking
------------

You can easily benchmark the performance of all queue types (including async) using the built-in tool:

**Run with tox:**

.. code-block:: console

    tox -e bench -- rst

**Or run directly:**

.. code-block:: console

    python benchmark/run_benchmark.py 1000 rst

- The first argument is the number of items to test (default: 1000)
- The second argument is the output format: `rst` (for reStructuredText table), `console`, or `json`

**Example output (rst):**

.. code-block:: text

    +--------------------+--------------------+--------------------+--------------------+
    | Queue Type         | Write              | Write/Read(1 task_done) | Write/Read(many task_done) |
    +--------------------+--------------------+--------------------+--------------------+
    | File Queue         | 0.0481             | 0.0299             | 0.0833             |
    | AsyncSQLiteQueue   | 0.2664             | 0.5353             | 0.5508             |
    | AsyncFileQueue     | 0.1333             | 0.1500             | 0.2337             |
    +--------------------+--------------------+--------------------+--------------------+

This makes it easy to compare the performance of sync and async queues on your platform.

Performance Tips
^^^^^^^^^^^^^^^^

* **WAL Mode**: SQLite3 queues use WAL mode by default for better performance
* **auto_commit=False**: Use for batch operations, call ``task_done()`` to persist
* **Protocol Selection**: Automatically selects optimal pickle protocol
* **Windows**: File queue performance improved 3-4x since v0.4.1
* **MySQL Connection Pooling**: MySQL queues use connection pooling for better performance

Testing
-------

Run tests using tox:

.. code-block:: console

    # Run tests for specific Python version
    tox -e py312
    
    # Run code style checks
    tox -e pep8
    
    # Generate coverage report
    tox -e cover

Development
-----------

Install development dependencies:

.. code-block:: console

    pip install -r test-requirements.txt
    pip install -r extra-requirements.txt

Run benchmarks:

.. code-block:: console

    python benchmark/run_benchmark.py 1000

Release Notes
-------------

For detailed information about recent changes and updates, see:

* `Release Notes for v1.1 <docs/RELEASE_NOTES/releasenote-1.1.txt>`_ - Major update with async queue enhancements and pytest migration

Known Issues
------------

* **Windows File Queue**: Atomic operations are experimental. Critical data may become unreadable during ``task_done()`` failures
* **MySQL Tests**: Require local MySQL service, otherwise skipped automatically
* **Async Features**: Require Python 3.7+ and asyncio support

Troubleshooting
---------------

**Database Locked Error**
^^^^^^^^^^^^^^^^^^^^^^^^^

If you get ``sqlite3.OperationalError: database is locked``:

* Increase the ``timeout`` parameter when creating the queue
* Ensure you're using ``multithreading=True`` for multi-threaded access

**MySQL Connection Issues**
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you get MySQL connection errors:

* Verify MySQL server is running and accessible
* Check connection parameters (host, port, user, password)
* Ensure the database exists and user has proper permissions
* For connection pool issues, try increasing ``max_connections`` parameter

**Thread Safety Issues**
^^^^^^^^^^^^^^^^^^^^^^^^

* Make sure to set ``multithreading=True`` when initializing SQLite queues
* SQLite3 queues are thoroughly tested in multi-threading environments
* MySQL queues are thread-safe by default

**Import Errors**
^^^^^^^^^^^^^^^^^

* For async features: Install with ``pip install "persist-queue[async]"``
* For MySQL support: Install with ``pip install "persist-queue[extra]"``

Community
---------

* **Slack**: Join `persist-queue <https://join.slack.com/t/persist-queue/shared_invite/enQtOTM0MDgzNTQ0MDg3LTNmN2IzYjQ1MDc0MDYzMjI4OGJmNmVkNWE3ZDBjYzg5MDc0OWUzZDJkYTkwODdkZmYwODdjNjUzMTk3MWExNDE>`_ channel
* **GitHub**: `Repository <https://github.com/peter-wangxu/persist-queue>`_
* **PyPI**: `Package <https://pypi.python.org/pypi/persist-queue>`_

Contributing
------------

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests to cover your changes
5. Submit a pull request with a clear title and description

License
-------

`BSD License <LICENSE>`_

Contributors
------------

`View Contributors <https://github.com/peter-wangxu/persist-queue/graphs/contributors>`_