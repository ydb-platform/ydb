OpenTelemetry Prometheus Exporter
=================================

|pypi|

.. |pypi| image:: https://badge.fury.io/py/opentelemetry-exporter-prometheus.svg
   :target: https://pypi.org/project/opentelemetry-exporter-prometheus/

This library allows to export metrics data to `Prometheus <https://prometheus.io/>`_.

Installation
------------

::

     pip install opentelemetry-exporter-prometheus

Limitations
-----------

* No multiprocessing support: The Prometheus exporter is not designed to operate in multiprocessing environments (see `#3747 <https://github.com/open-telemetry/opentelemetry-python/issues/3747>`_).

References
----------

* `OpenTelemetry Prometheus Exporter <https://opentelemetry-python.readthedocs.io/en/latest/exporter/prometheus/prometheus.html>`_
* `Prometheus <https://prometheus.io/>`_
* `OpenTelemetry Project <https://opentelemetry.io/>`_
