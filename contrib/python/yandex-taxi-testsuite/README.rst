Yandex Taxi Testsuite
=====================

.. image:: https://badge.fury.io/py/yandex-taxi-testsuite.svg
    :target: https://pypi.python.org/pypi/yandex-taxi-testsuite
.. image:: https://github.com/yandex/yandex-taxi-testsuite/actions/workflows/ci.yml/badge.svg?branch=develop
    :target: https://github.com/yandex/yandex-taxi-testsuite/actions/workflows/ci.yml?branch=develop
.. image:: https://github.com/yandex/yandex-taxi-testsuite/actions/workflows/macos-ci.yml/badge.svg?branch=develop
    :target: https://github.com/yandex/yandex-taxi-testsuite/actions/workflows/macos-ci.yml?branch=develop


What is testsuite
-----------------

Testsuite is a microservice-oriented test framework written in Python based on
pytest_.

Testsuite is written and supported by Yandex.Taxi_, and is used to test
Yandex.Taxi microservices written in C++ and Python.

The principal suggested approach to testing - although not the only one - is
black box, when the service is tested through http calls.

Direct read and write access from test to database is supported to enable
precondition setup and result assertions.

Installation
------------

Installation using pip_:

.. code-block:: sh

   pip3 install yandex-taxi-testsuite

   # testsuite with mongodb support
   pip3 install yandex-taxi-testsuite[mongodb]

   # testsuite with postgresql support
   pip3 install yandex-taxi-testsuite[postgresql]
   pip3 install yandex-taxi-testsuite[postgresql-binary]

   # testsuite with redis support
   pip3 install yandex-taxi-testsuite[redis]

   # testsuite with mysql support
   pip3 install yandex-taxi-testsuite[mysql]

   # testsuite with clickhouse support
   pip3 install yandex-taxi-testsuite[clickhouse]

   # testsuite with rabbitmq support
   pip3 install yandex-taxi-testsuite[rabbitmq]

   # testsuite with kafka support
   pip3 install yandex-taxi-testsuite[kafka]

Supported databases
-------------------

Out-of-the-box testsuite supports the following databases:

* PostgreSQL
* MongoDB
* Redis/Valkey
* MySQL/MariaDB 10+
* ClickHouse
* RabbitMQ

Supported operating systems
---------------------------

Testsuite runs on GNU/Linux and macOS operating systems.

Principle of operation
----------------------

Testsuite sets up the environment for the service being tested:

* Testsuite starts any required databases (postgresql, mongo, redis).
* Before each test, testsuite fills the database with test data.
* Testsuite starts its own web server (mockserver), which mimics (mocks)
  microservices other than the one being tested.

Testsuite starts the microservice being tested in a separate process.

Testsuite then runs tests.

A test performs http requests to the service and verifies that the requests
were processed properly.

A test may check the results of an http call by looking directly into the
service's database.

A test may check whether the service has made calls to external services,
as well as the order of calls and the data that was sent and received.

A test may check the internal state of the service as represented by the data
the service sent to the test with the testpoint mechanism.

Source code
-----------

Testsuite open-source edition code is available
`here <https://github.com/yandex/yandex-taxi-testsuite>`_.

Documentation
-------------

For full documentation, including installation, tutorials,
please see https://yandex.github.io/yandex-taxi-testsuite/.

Running testsuite
-----------------

self-tests:

.. code-block:: sh

   pytest3 ./tests

tests of example services:

.. code-block:: sh

   cd docs/examples && make

Development
-----------

Setup virtual env
~~~~~~~~~~~~~~~~~

In order to test your modifications it's useful to run testsuite inside
virtualenv. Use the following command to create developer's venv:

.. code-block:: sh

   make setup-dev-venv

Virtualenv will be created in `.venv-dev` directory.

Code format and linters
~~~~~~~~~~~~~~~~~~~~~~~

Auto format source code:

.. code-block:: sh

   make venv-format

Run linters:

.. code-block:: sh

   make venv-check-linters
   make venv-check-mypy

You can also add pre-commit hook which will run ruff and linters for you:

.. code-block:: sh

   make install-pre-commit-hooks

Running tests
~~~~~~~~~~~~~

You can run tests using Makefile:

.. code-block:: sh

   make venv-tests

Or directly using pytest:

.. code-block:: sh

   make setup-dev-venv                  # Setup virtual env first
   . .venv-dev/bin/activate             # Activate virtualenv
   pytest -vv tests/plugins/mockserver  # Finally run pytest

Building documentation
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: sh

   make build-docs

Contributing
~~~~~~~~~~~~

* Fork project
* Write code in your own branch
* Test your code
* Run linters and code formatters
* Add new changelog_ entry
* Create pull request on github

.. _Yandex.Taxi: https://taxi.yandex.com/company/
.. _pytest: https://pytest.org/
.. _pip: https://pypi.org/project/yandex-taxi-testsuite/
.. _changelog: https://github.com/yandex/yandex-taxi-testsuite/tree/develop/docs/changelog.rst
