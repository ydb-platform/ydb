OpenTelemetry threading Instrumentation
=======================================

|pypi|

.. |pypi| image:: https://badge.fury.io/py/opentelemetry-instrumentation-threading.svg
   :target: https://pypi.org/project/opentelemetry-instrumentation-threading/

This library provides instrumentation for the `threading` module to ensure that
the OpenTelemetry context is propagated across threads. It is important to note
that this instrumentation does not produce any telemetry data on its own. It
merely ensures that the context is correctly propagated when threads are used.

Installation
------------

::

    pip install opentelemetry-instrumentation-threading

References
----------

* `OpenTelemetry Threading Tracing <https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/threading/threading.html>`_
* `OpenTelemetry Project <https://opentelemetry.io/>`_
