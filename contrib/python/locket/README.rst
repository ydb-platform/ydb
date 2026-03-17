locket.py: File-based locks for Python on Linux and Windows
===========================================================

Locket implements a file-based lock that can be used by multiple processes provided they use the same path.

.. code-block:: python

    import locket

    # Wait for lock
    with locket.lock_file("path/to/lock/file"):
        perform_action()

    # Raise LockError if lock cannot be acquired immediately
    with locket.lock_file("path/to/lock/file", timeout=0):
        perform_action()

    # Raise LockError if lock cannot be acquired after thirty seconds
    with locket.lock_file("path/to/lock/file", timeout=30):
        perform_action()

    # Without context managers:
    lock = locket.lock_file("path/to/lock/file")
    try:
        lock.acquire()
        perform_action()
    finally:
        lock.release()

Locks largely behave as (non-reentrant) ``Lock`` instances from the ``threading``
module in the standard library. Specifically, their behaviour is:

* Locks are uniquely identified by the file being locked,
  both in the same process and across different processes.

* Locks are either in a locked or unlocked state.

* When the lock is unlocked, calling ``acquire()`` returns immediately and changes
  the lock state to locked.

* When the lock is locked, calling ``acquire()`` will block until the lock state
  changes to unlocked, or until the timeout expires.

* If a process holds a lock, any thread in that process can call ``release()`` to
  change the state to unlocked.

* Calling ``release()`` on an unlocked lock raises ``LockError``.

* Behaviour of locks after ``fork`` is undefined.

Installation
------------

.. code-block:: sh

    pip install locket
