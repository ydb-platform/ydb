Makefile documentation
======================

Makefile is used to automate Pyroute2 deployment and test
processes. Mostly, it is but a collection of common commands.


target: clean
-------------

Clean up the repo directory from the built documentation,
collected coverage data, compiled bytecode etc.

target: docs
------------

Build documentation. Requires `sphinx`.

target: test
------------

NB: nosetests CI will be obsoleted as soon as the new CI (pytest/tox)
will be ready.

Run tests against current code. Command line options:

* python -- path to the Python to use
* nosetests -- path to nosetests to use
* wlevel -- the Python -W level
* coverage -- set `coverage=html` to get coverage report
* pdb -- set `pdb=true` to launch pdb on errors
* module -- run only specific test module
* skip -- skip tests by pattern
* loop -- number of test iterations for each module
* report -- url to submit reports to (see tests/collector.py)
* worker -- the worker id

To run the full test cycle on the project, using a specific
python, making html coverage report::

    $ sudo make test python=python3 coverage=html

To run a specific test module::

    $ sudo make test module=general:test_ipdb.py:TestExplicit

The module parameter syntax::

    ## module=package[:test_file.py[:TestClass[.test_case]]]

    $ sudo make test module=lnst
    $ sudo make test module=general:test_ipr.py
    $ sudo make test module=general:test_ipdb.py:TestExplicit

There are several test packages:

* general -- common functional tests
* eventlet -- Neutron compatibility tests
* lnst -- LNST compatibility tests

For each package a new Python instance is launched, keep that
in mind since it affects the code coverage collection.

It is possible to skip tests by a pattern::

    $ sudo make test skip=test_stress

To run tests in a loop, use the loop parameter::

    $ sudo make test loop=10

For every iteration the code will be packed again with `make dist`
and checked against PEP8.

All the statistic may be collected with a simple web-script, see
`tests/collector.py` (requires the bottle framework). To retrieve
the collected data one can use curl::

    $ sudo make test report=http://localhost:8080/v1/report/
    $ curl http://localhost:8080/v1/report/ | python -m json.tool

target: pytest
--------------

Run the pytest CI. Specific options:

* coverage -- set `coverage=true` to create the coverage report
* pdb -- set `pdb=true` to run pdb in the case of test failure
* skipdb -- skip tests that use a specific DB, `postgres` or `sqlite3`
* dbname -- set the PostgreSQL DB name (if used)

The NDB module uses a DB as the storage, it may be SQLite3 or PostgreSQL.
By default it uses in-memory SQLite3, but tests cover all the providers
as the SQL code may differ. One can skip DB-specific tests by setting
the `skipdb` option.

target: dist
------------

Make Python distribution package. Command line options:

* python -- the Python to use

target: install
---------------

Build and install the package into the system. Command line options:

* python -- the Python to use
* root -- root install directory
* lib -- where to install lib files

target: develop
---------------

Build the package and deploy the egg-link with setuptools. No code
will be deployed into the system directories, but instead the local
package directory will be visible to the python. In that case one
can change the code locally and immediately test it system-wide
without running `make install`.

* python -- the Python to use

other targets
-------------

Other targets are either utility targets to be used internally,
or hooks for related projects. You can safely ignore them.
