.. devcontribute:

Project Contribution Guide
==========================

Setup the Environment
---------------------

.. code-block:: bash

    # Make sure you have the following installed:
    #   bash
    #   git
    #   python
    #   GNU make, sed, awk

    # Clone the repository
    git clone ${pyroute2_git_url}
    cd pyroute2

    # Run the test suite
    sudo make test


Test-Driven Development
-----------------------

It is best practice that every change is covered by tests. To make
things easier, write the tests first and then implement your feature.

The test modules are located in the `/tests/` folder and are run
using nox. You can add your tests to an existing test module or create
your own module if it requires a specific environment that is not yet
covered. In that case, add a new session to `noxfile.py`.

The project’s convention is to use pytest, but it is not a strict
requirement. If testing your feature requires a specific framework or
uses the standard library’s `unittest` module, it can be discussed
in the pull request.

However, tests must always be run using a nox session.


Code Linting
------------

The code must be formatted according to the project’s code style.
The simplest way is to run `make format`, which will automatically
reformat the code.

.. note::
   The code must pass `make format` in order to be merged.

In addition to code style checks, additional linting is performed.
It is best practice to add type annotations and ensure that the
modified code passes mypy linting. If you do so, add the affected
file or module to `.mypy-check-paths` so that subsequent changes
will not undo your work.


Run Specific Test Modules
--------------------------

It is possible to run a specific test session instead of running
all default sessions:

.. code-block:: bash

    make test session=unit             # run only tests/test_unit
    make test session=core-python3.14  # run tests/test_core with Python 3.14

See `noxfile.py` for the defined sessions and the Python version matrix.

Each test session is run in a separate Python process within its own
virtual environment, while test modules within the same session share
the process and virtual environment.

Thus, testing integration with libraries such as eventlet, or testing
multiprocessing start methods (spawn, fork, etc.), requires a separate
session since such process setup can only be performed once.

To run a specific test module within a session, use the `noxconfig` argument:

.. code-block:: bash

    # Run tests/test_linux/test_ndb/test_mpls.py
    make test session=linux-python3.14 \
        noxconfig='{"sub": "test_ndb/test_mpls.py"}'


Test Session Configuration
---------------------------

Test session configuration can be provided via the `noxconfig` argument
in JSON format. Available options:

**coverage** (bool, default: false)
  Generate a coverage report.

**exitfirst** (bool, default: true)
  Exit the session after the first failure (true) or run all tests and
  report failures at the end (false).

**fail_on_warnings** (bool, default: false)
  Fail on Python warnings, such as DeprecationWarning.

**fast** (bool, default: false)
  Reuse an existing virtual environment for the session (true) or purge
  the virtual environment and create a new one from scratch (false).
  This option might be useful when running tests offline, but it may
  leave artifacts in the source tree. It is disabled by default and
  requires at least one run with `"fast": false` to properly set up
  the virtual environment. Use with caution.

**pdb** (bool, default: false)
  Launch the pdb debugger shell on errors.

**sub** (str, default: none)
  Select the module to run within the session.

**test_prefix** (str, default: "tests")
  Override the test sessions directory.

**timeout** (int, default: 60)
  Test case timeout in seconds.

**verbose** (bool, default: true)
  If true, print every test case name and description (if available);
  if false, print only a dot for each test case.

**Example:**

.. code-block:: bash

    make test \
        session=core-python3.14 \
        noxconfig='{ \
            "exitfirst": false, \
            "verbose": false, \
            "timeout": 10, \
            "pdb": true, \
            "sub": "test_plan9/test_basic.py" \
        }'


Running with nox
----------------

The `make test` command creates a virtual environment and installs nox
within it. However, you can also use your system’s installation of nox.
The previous example would then look as follows:

.. code-block:: bash

    nox \
        -e core-python3.14 -- \
        '{ \
            "exitfirst": false, \
            "verbose": false, \
            "timeout": 10, \
            "pdb": true, \
            "sub": "test_plan9/test_basic.py" \
        }'


Project Dependencies
--------------------

The project is designed to work with only the standard library, which
is a strict requirement. Some embedded environments even strip the
standard library, removing modules such as sqlite3.

To support such environments, the project provides two packages: `pyroute2`
and `pyroute2.minimal`. The latter offers a minimal distribution without
modules such as sqlite3 or pickle.

The `pyroute2` and `pyroute2.minimal` packages are mutually exclusive.
Each provides its own PyPI package.


Submit a Pull Request
---------------------

The primary repository for the project is on GitHub. All pull requests
are welcome.

The code must meet the following requirements:

- The library must work on Python ≥ 3.9.
- The code must pass `make format`.
- The code must not break existing unit and functional tests (run via `sudo make test`).
- The use of ctypes must not cause the library to fail on SELinux.


IRC Channel
-----------
The project has an IRC channel, **#pyroute2**, on the Libera.Chat network.
While the team is not guaranteed to be online 24/7, you can still discuss
your questions there.
