=======================
drf-spectacular-sidecar
=======================

|pypi-version| |pypi-dl|

Serve self-contained distribution builds of `Swagger UI`_ and `Redoc`_ with `Django`_ either via `runserver`_ or `collectstatic`_.

This Django app is an optional addition to `drf-spectacular`_, but does not depend on it. It may also be used independently.

* `Swagger UI`_ version ``5.31.0`` (`npm <https://www.npmjs.com/package/swagger-ui-dist>`__)
* `Redoc`_ version ``2.5.2`` (`npm <https://www.npmjs.com/package/redoc>`__)

This is a self-updating and self-publishing repository that looks for updates on the 1st of every month.
The distribution files are sourced from npm via `jsdelivr`_, validated, packaged and uploaded to `PyPI`_.

Installation
------------

.. code:: bash

    $ pip install drf-spectacular-sidecar

The package needs to be registered to allow Django to discover the static files.

.. code:: python

    INSTALLED_APPS = [
        # ALL YOUR APPS
        'drf_spectacular_sidecar',
    ]

Requirements
------------

Django >= 2.2

Licenses
--------

Provided by `T. Franzel <https://github.com/tfranzel>`_. `Licensed under 3-Clause BSD <https://github.com/tfranzel/drf-spectacular-sidecar/blob/master/LICENSE>`_.

This package includes distribution builds of

* `Swagger UI`_: The `original license (Apache 2.0) <https://github.com/swagger-api/swagger-ui/blob/master/LICENSE>`_ and copyright apply to those files.
* `Redoc`_: The `original license (MIT) <https://github.com/Redocly/redoc/blob/master/LICENSE>`_ and copyright apply to those files.


.. |pypi-version| image:: https://img.shields.io/pypi/v/drf-spectacular-sidecar.svg
   :target: https://pypi.org/project/drf-spectacular-sidecar/
.. |pypi-dl| image:: https://img.shields.io/pypi/dm/drf-spectacular-sidecar
   :target: https://pypi.org/project/drf-spectacular-sidecar/

.. _PyPI: https://pypi.org/project/drf-spectacular-sidecar/
.. _jsdelivr: https://www.jsdelivr.com/
.. _Django: https://www.djangoproject.com/
.. _drf-spectacular: https://github.com/tfranzel/drf-spectacular
.. _Redoc: https://github.com/Redocly/redoc
.. _Swagger UI: https://github.com/swagger-api/swagger-ui
.. _collectstatic: https://docs.djangoproject.com/en/3.2/ref/contrib/staticfiles/#collectstatic
.. _runserver: https://docs.djangoproject.com/en/3.2/ref/contrib/staticfiles/#runserver
