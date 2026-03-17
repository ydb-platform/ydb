OpenTelemetry asyncio Instrumentation
======================================

|pypi|

.. |pypi| image:: https://badge.fury.io/py/opentelemetry-instrumentation-asyncio.svg
   :target: https://pypi.org/project/opentelemetry-instrumentation-asyncio/

AsyncioInstrumentor: Tracing Requests Made by the Asyncio Library


The opentelemetry-instrumentation-asyncio package allows tracing asyncio applications.
It also includes metrics for duration and counts of coroutines and futures. Metrics are generated even if coroutines are not traced.


Set the names of coroutines you want to trace.
-------------------------------------------------
.. code:: bash

    export OTEL_PYTHON_ASYNCIO_COROUTINE_NAMES_TO_TRACE=coro_name,coro_name2,coro_name3

If you want to trace specific blocking functions executed with the ``to_thread`` function of asyncio, set the name of the functions in ``OTEL_PYTHON_ASYNCIO_TO_THREAD_FUNCTION_NAMES_TO_TRACE``.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
.. code:: bash

    export OTEL_PYTHON_ASYNCIO_TO_THREAD_FUNCTION_NAMES_TO_TRACE=func_name,func_name2,func_name3

You can enable tracing futures with ``OTEL_PYTHON_ASYNCIO_FUTURE_TRACE_ENABLED``
-----------------------------------------------------------------------------------------------
.. code:: bash

    export OTEL_PYTHON_ASYNCIO_FUTURE_TRACE_ENABLED=true

Run instrumented application
-----------------------------
1. coroutine
--------------------
.. code:: python

    # export OTEL_PYTHON_ASYNCIO_COROUTINE_NAMES_TO_TRACE=sleep

    import asyncio
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

    AsyncioInstrumentor().instrument()

    async def main():
        await asyncio.create_task(asyncio.sleep(0.1))

    asyncio.run(main())

2. future
--------------------
.. code:: python

    # export OTEL_PYTHON_ASYNCIO_FUTURE_TRACE_ENABLED=true

    import asyncio
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

    AsyncioInstrumentor().instrument()

    loop = asyncio.get_event_loop()

    future = asyncio.Future()
    future.set_result(1)
    task = asyncio.ensure_future(future)
    loop.run_until_complete(task)

3. to_thread
--------------------
.. code:: python

    # export OTEL_PYTHON_ASYNCIO_TO_THREAD_FUNCTION_NAMES_TO_TRACE=func

    import asyncio
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

    AsyncioInstrumentor().instrument()

    async def main():
        await asyncio.to_thread(func)

    def func():
        pass

    asyncio.run(main())


asyncio metric types
----------------------

* `asyncio.process.duration` (seconds) - Duration of asyncio process
* `asyncio.process.count` (count) - Number of asyncio process


API
---



Installation
------------

::

    pip install opentelemetry-instrumentation-asyncio


References
-----------

* `OpenTelemetry asyncio/ Tracing <https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/asyncio/asyncio.html>`_
* `OpenTelemetry Project <https://opentelemetry.io/>`_
* `OpenTelemetry Python Examples <https://github.com/open-telemetry/opentelemetry-python/tree/main/docs/examples>`_
