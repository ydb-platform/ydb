psycogreen -- psycopg2 integration with coroutine libraries
===========================================================

The `psycogreen`_ package enables psycopg2 to work with coroutine libraries,
using asynchronous calls internally but offering a blocking interface so that
regular code can run unmodified.

`Psycopg`_ offers `coroutines support`__ since release 2.2. Because the main
module is a C extension it cannot be monkey-patched to become
coroutine-friendly. Instead it exposes `a hook`__ that coroutine libraries can
use to install a function integrating with their event scheduler. Psycopg will
call the function whenever it executes a libpq call that may block.
`psycogreen` is a collection of "wait callbacks" useful to integrate Psycopg
with different coroutine libraries.

.. _psycogreen: https://github.com/psycopg/psycogreen
.. _Psycopg: http://initd.org/psycopg/
.. __: http://initd.org/psycopg/docs/advanced.html#support-to-coroutine-libraries
.. __: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.set_wait_callback


Installation
------------

``pip install psycogreen``


Module ``psycogreen.eventlet``
------------------------------

`Eventlet`_ currently supports Psycopg out-of-the-box and ``psycogreen`` is not
necessary. See `the documentation`__ for patching instruction.

.. _Eventlet: http://eventlet.net/
.. __: http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library

If for any reason you want to avoid using Eventlet monkeypatching you can use
``psycogreen.eventlet.patch_psycopg()``.

Function ``psycogreen.eventlet.patch_psycopg()``
    Enable async processing in Psycopg integrated with the Eventlet events
    loop.  It is performed by registering ``eventlet_wait_callback()`` as
    psycopg2 wait callback.

Function ``psycogreen.eventlet.eventlet_wait_callback(conn)``
    A wait callback integrating with Eventlet events loop.

An example script showing concurrent usage of ``psycopg2`` with ``urlopen()``
with Eventlet is available in |tests/test_eventlet.py|__.

.. |tests/test_eventlet.py| replace:: ``tests/test_eventlet.py``
.. __: https://github.com/psycopg/psycogreen/blob/master/tests/test_eventlet.py


Module ``psycogreen.gevent``
----------------------------

In order to use psycopg2 asynchronously with `gevent`_ you can use
``psycogreen.gevent.patch_psycopg()``.

Function ``psycogreen.gevent.patch_psycopg()``
    Enable async processing in Psycopg integrated with the gevent events
    loop.  It is performed by registering ``gevent_wait_callback()`` as
    psycopg2 wait callback.

Function ``psycogreen.gevent.gevent_wait_callback(conn)``
    A wait callback integrating with gevent events loop.

An example script showing concurrent usage of ``psycopg2`` with ``urlopen()``
with gevent is available in |tests/test_gevent.py|__.

.. _gevent: http://www.gevent.org/
.. |tests/test_gevent.py| replace:: ``tests/test_gevent.py``
.. __: https://github.com/psycopg/psycogreen/blob/master/tests/test_gevent.py


uWSGI green threads
-------------------

Roberto De Ioris is writing uGreen__, a green thread implementation on top of
the `uWSGI async platform`__.

.. __: http://projects.unbit.it/uwsgi/wiki/uGreen
.. __: http://projects.unbit.it/uwsgi/

He has performed some tests using both `psycopg2 async support`__ and
`psycopg2 green support`__ and has reported no problem in their stress tests
with both the async styles.

.. __: http://projects.unbit.it/uwsgi/browser/tests/psycopg2_green.py
.. __: http://projects.unbit.it/uwsgi/browser/tests/psycogreen_green.py

