=========================================
 aiosmtpd - An asyncio based SMTP server
=========================================

| |github license| |_| |PyPI Version| |_| |PyPI Python| |_| |PyPI PythonImpl|
| |GA badge| |_| |CodeQL badge| |_| |codecov| |_| |readthedocs|
|
| |GH Discussions|
|

.. |_| unicode:: 0xA0
   :trim:
.. |github license| image:: https://img.shields.io/github/license/aio-libs/aiosmtpd?logo=Open+Source+Initiative&logoColor=0F0
   :target: https://github.com/aio-libs/aiosmtpd/blob/master/LICENSE
   :alt: Project License on GitHub
.. |PyPI Version| image:: https://img.shields.io/pypi/v/aiosmtpd?logo=pypi&logoColor=yellow
   :target: https://pypi.org/project/aiosmtpd/
   :alt: PyPI Package
.. |PyPI Python| image:: https://img.shields.io/pypi/pyversions/aiosmtpd?logo=python&logoColor=yellow
   :target: https://pypi.org/project/aiosmtpd/
   :alt: Supported Python Versions
.. |PyPI PythonImpl| image:: https://img.shields.io/pypi/implementation/aiosmtpd?logo=python
   :target: https://pypi.org/project/aiosmtpd/
   :alt: Supported Python Implementations
.. .. For |GA badge|, don't forget to check actual workflow name in unit-testing-and-coverage.yml
.. |GA badge| image:: https://github.com/aio-libs/aiosmtpd/workflows/aiosmtpd%20CI/badge.svg
   :target: https://github.com/aio-libs/aiosmtpd/actions/workflows/unit-testing-and-coverage.yml
   :alt: GitHub CI status
.. |CodeQL badge| image:: https://github.com/aio-libs/aiosmtpd/workflows/CodeQL/badge.svg
   :target: https://github.com/aio-libs/aiosmtpd/actions/workflows/codeql.yml
   :alt: CodeQL status
.. |codecov| image:: https://codecov.io/github/aio-libs/aiosmtpd/coverage.svg?branch=master
   :target: https://codecov.io/github/aio-libs/aiosmtpd?branch=master
   :alt: Code Coverage
.. |readthedocs| image:: https://img.shields.io/readthedocs/aiosmtpd?logo=Read+the+Docs&logoColor=white
   :target: https://aiosmtpd.readthedocs.io/en/latest/
   :alt: Documentation Status
.. |GH Discussions| image:: https://img.shields.io/github/discussions/aio-libs/aiosmtpd?logo=github&style=social
   :target: https://github.com/aio-libs/aiosmtpd/discussions
   :alt: GitHub Discussions

The Python standard library includes a basic |SMTP|_ server in the |smtpd|_ module,
based on the old asynchronous libraries |asyncore|_ and |asynchat|_.
These modules are quite old and are definitely showing their age;
``asyncore`` and ``asynchat`` are difficult APIs to work with, understand, extend, and fix.
(And have been deprecated since Python 3.6, and will be removed in Python 3.12.)

With the introduction of the |asyncio|_ module in Python 3.4,
a much better way of doing asynchronous I/O is now available.
It seems obvious that an asyncio-based version of the SMTP and related protocols are needed for Python 3.
This project brings together several highly experienced Python developers collaborating on this reimplementation.

This package provides such an implementation of both the SMTP and LMTP protocols.

Full documentation is available on |aiosmtpd rtd|_


Requirements
============

Supported Platforms
-----------------------

``aiosmtpd`` has been tested on **CPython**>=3.8 and |PyPy|_>=3.8
for the following platforms (in alphabetical order):

* Cygwin (as of 2022-12-22, only for CPython 3.8, and 3.9)
* MacOS 11 and 12
* Ubuntu 18.04
* Ubuntu 20.04
* Ubuntu 22.04
* Windows 10
* Windows Server 2019
* Windows Server 2022

``aiosmtpd`` *probably* can run on platforms not listed above,
but we cannot provide support for unlisted platforms.

.. |PyPy| replace:: **PyPy**
.. _`PyPy`: https://www.pypy.org/


Installation
============

Install as usual with ``pip``::

    pip install aiosmtpd

If you receive an error message ``ModuleNotFoundError: No module named 'public'``,
it likely means your ``setuptools`` is too old;
try to upgrade ``setuptools`` to at least version ``46.4.0``
which had `implemented a fix for this issue`_.

.. _`implemented a fix for this issue`: https://setuptools.readthedocs.io/en/latest/history.html#v46-4-0


Project details
===============

As of 2016-07-14, aiosmtpd has been put under the |aiolibs|_ umbrella project
and moved to GitHub.

* Project home: https://github.com/aio-libs/aiosmtpd
* PyPI project page: https://pypi.org/project/aiosmtpd/
* Report bugs at: https://github.com/aio-libs/aiosmtpd/issues
* Git clone: https://github.com/aio-libs/aiosmtpd.git
* Documentation: http://aiosmtpd.readthedocs.io/
* StackOverflow: https://stackoverflow.com/questions/tagged/aiosmtpd

The best way to contact the developers is through the GitHub links above.
You can also request help by submitting a question on StackOverflow.


Building
========

You can install this package in a virtual environment like so::

    $ python3 -m venv /path/to/venv
    $ source /path/to/venv/bin/activate
    $ python setup.py install

This will give you a command line script called ``aiosmtpd`` which implements the
SMTP server.  Use ``aiosmtpd --help`` for a quick reference.

You will also have access to the ``aiosmtpd`` library, which you can use as a
testing environment for your SMTP clients.  See the documentation links above
for details.


Developing
==========

You'll need the `tox <https://pypi.python.org/pypi/tox>`__ tool to run the
test suite for Python 3.  Once you've got that, run::

    $ tox

Individual tests can be run like this::

    $ tox -- <testname>

where ``<testname>`` is the "node id" of the test case to run, as explained
in `the pytest documentation`_. The command above will run that one test case
against all testenvs defined in ``tox.ini`` (see below).

If you want test to stop as soon as it hit a failure, use the ``-x``/``--exitfirst``
option::

    $ tox -- -x

You can also add the ``-s``/``--capture=no`` option to show output, e.g.::

    $ tox -e py311-nocov -- -s

and these options can be combined::

    $ tox -e py311-nocov -- -x -s <testname>

(The ``-e`` parameter is explained in the next section about 'testenvs'.
In general, you'll want to choose the ``nocov`` testenvs if you want to show output,
so you can see which test is generating which output.)


Supported 'testenvs'
------------------------

In general, the ``-e`` parameter to tox specifies one (or more) **testenv**
to run (separate using comma if more than one testenv). The following testenvs
have been configured and tested:

* ``{py38,py39,py310,py311,py312,pypy3,pypy37,pypy38,pypy39}-{nocov,cov,diffcov,profile}``

  Specifies the interpreter to run and the kind of testing to perform.

  - ``nocov`` = no coverage testing. Tests will run verbosely.
  - ``cov`` = with coverage testing. Tests will run in brief mode
    (showing a single character per test run)
  - ``diffcov`` = with diff-coverage report (showing difference in
    coverage compared to previous commit). Tests will run in brief mode
  - ``profile`` = no coverage testing, but code profiling instead.
    This must be **invoked manually** using the ``-e`` parameter

  **Note 1:** As of 2021-02-23,
  only the ``{py38,py39}-{nocov,cov}`` combinations work on **Cygwin**.

  **Note 2:** It is also possible to use whatever Python version is used when
  invoking ``tox`` by using the ``py`` target, but you must explicitly include
  the type of testing you want. For example::

    $ tox -e "py-{nocov,cov,diffcov}"

  (Don't forget the quotes if you want to use braces!)

  You might want to do this for CI platforms where the exact Python version
  is pre-prepared, such as Travis CI or |GitHub Actions|_; this will definitely
  save some time during tox's testenv prepping.

  For all testenv combinations except diffcov,
  |bandit|_ security check will also be run prior to running pytest.

.. _bandit: https://github.com/PyCQA/bandit
.. |bandit| replace:: ``bandit``


* ``qa``

  Performs |flake8|_ code style checking,
  and |flake8-bugbear|_ design checking.

  In addition, some tests to help ensure that ``aiosmtpd`` is *releasable* to PyPI are also run.

.. _flake8: https://flake8.pycqa.org/en/latest/
.. |flake8| replace:: ``flake8``
.. _flake8-bugbear: https://github.com/PyCQA/flake8-bugbear
.. |flake8-bugbear| replace:: ``flake8-bugbear``

* ``docs``

  Builds **HTML documentation** and **manpage** using Sphinx.
  A `pytest doctest`_ will run prior to actual building of the documentation.

* ``static``

  Performs a **static type checking** using ``pytype``.

  **Note 1:** Please ensure that `all pytype dependencies`_ have been installed before
  executing this testenv.

  **Note 2:** This testenv will be _SKIPPED_ on Windows,
  because ``pytype`` currently cannot run on Windows.

  **Note 3:** This testenv does NOT work on **Cygwin**.

.. _`all pytype dependencies`: https://github.com/google/pytype/blob/2021.02.09/CONTRIBUTING.md#pytype-dependencies


Environment Variables
-------------------------

``ASYNCIO_CATCHUP_DELAY``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Due to how asyncio event loop works, some actions do not instantly get
    responded to. This is especially so on slower / overworked systems.
    In consideration of such situations, some test cases invoke a slight
    delay to let the event loop catch up.

    Defaults to `0.1` and can be set to any float value you want.


Different Python Versions
-----------------------------

The tox configuration files have been created to cater for more than one
Python versions `safely`: If an interpreter is not found for a certain
Python version, tox will skip that whole testenv.

However, with a little bit of effort, you can have multiple Python interpreter
versions on your system by using ``pyenv``. General steps:

1. Install ``pyenv`` from https://github.com/pyenv/pyenv#installation

2. Install ``tox-pyenv`` from https://pypi.org/project/tox-pyenv/

3. Using ``pyenv``, install the Python versions you want to test on

4. Create a ``.python-version`` file in the root of the repo, listing the
   Python interpreter versions you want to make available to tox (see pyenv's
   documentation about this file)

   **Tip:** The 1st line of ``.python-version`` indicates your *preferred* Python version
   which will be used to run tox.

5. Invoke tox with the option ``--tox-pyenv-no-fallback`` (see tox-pyenv's
   documentation about this option)


``housekeep.py``
----------------

If you ever need to 'reset' your repo, you can use the ``housekeep.py`` utility
like so::

    $ python housekeep.py superclean

It is *strongly* recommended to NOT do superclean too often, though.
Every time you invoke ``superclean``,
tox will have to recreate all its testenvs,
and this will make testing *much* longer to finish.

``superclean`` is typically only needed when you switch branches,
or if you want to really ensure that artifacts from previous testing sessions
won't interfere with your next testing sessions.

For example, you want to force Sphinx to rebuild all documentation.
Or, you're sharing a repo between environments (say, PSCore and Cygwin)
and the cached Python bytecode messes up execution
(e.g., sharing the exact same directory between Windows PowerShell and Cygwin
will cause problems as Python becomes confused about the locations of the source code).


Signing Keys
============

Starting version 1.3.1,
files provided through `PyPI`_ or `GitHub Releases`_
will be signed using one of the following GPG Keys:

+-------------------------+----------------+----------------------------------+
| GPG Key ID              | Owner          | Email                            |
+=========================+================+==================================+
| ``5D60 CE28 9CD7 C258`` | Pandu E POLUAN | pepoluan at gmail period com     |
+-------------------------+----------------+----------------------------------+
| ``5555 A6A6 7AE1 DC91`` | Pandu E POLUAN | pepoluan at gmail period com     |
+-------------------------+----------------+----------------------------------+
| ``E309 FD82 73BD 8465`` | Wayne Werner   | waynejwerner at gmail period com |
+-------------------------+----------------+----------------------------------+
| ``5FE9 28CD 9626 CE2B`` | Sam Bull       | sam at sambull period org        |
+-------------------------+----------------+----------------------------------+



.. _PyPI: https://pypi.org/project/aiosmtpd/
.. _`GitHub Releases`: https://github.com/aio-libs/aiosmtpd/releases


License
=======

``aiosmtpd`` is released under the Apache License version 2.0.


.. _`GitHub Actions`: https://docs.github.com/en/free-pro-team@latest/actions/guides/building-and-testing-python#running-tests-with-tox
.. |GitHub Actions| replace:: **GitHub Actions**
.. _`pytest doctest`: https://docs.pytest.org/en/stable/doctest.html
.. _`the pytest documentation`: https://docs.pytest.org/en/stable/usage.html#specifying-tests-selecting-tests
.. _`aiosmtpd rtd`: https://aiosmtpd.readthedocs.io
.. |aiosmtpd rtd| replace:: **aiosmtpd.readthedocs.io**
.. _`SMTP`: https://tools.ietf.org/html/rfc5321
.. |SMTP| replace:: **SMTP**
.. _`smtpd`: https://docs.python.org/3/library/smtpd.html
.. |smtpd| replace:: **smtpd**
.. _`asyncore`: https://docs.python.org/3/library/asyncore.html
.. |asyncore| replace:: ``asyncore``
.. _`asynchat`: https://docs.python.org/3/library/asynchat.html
.. |asynchat| replace:: ``asynchat``
.. _`asyncio`: https://docs.python.org/3/library/asyncio.html
.. |asyncio| replace:: ``asyncio``
.. _`aiolibs`: https://github.com/aio-libs
.. |aiolibs| replace:: **aio-libs**
