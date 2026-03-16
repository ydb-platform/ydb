.. -*- mode: rst; coding: utf-8 -*-

==============================================================================
pytest-twisted - test twisted code with pytest
==============================================================================

|PyPI| |Pythons| |Travis| |AppVeyor| |Actions| |Black|

:Authors: Ralf Schmitt, Kyle Altendorf, Victor Titor
:Version: 1.14.3
:Date:    2024-08-21
:Download: https://pypi.python.org/pypi/pytest-twisted#downloads
:Code: https://github.com/pytest-dev/pytest-twisted


pytest-twisted is a plugin for pytest, which allows to test code,
which uses the twisted framework. test functions can return Deferred
objects and pytest will wait for their completion with this plugin.


NOTICE: Python 3.8 with asyncio support
=======================================

In Python 3.8, asyncio changed the default loop implementation to use
their proactor.  The proactor does not implement some methods used by
Twisted's asyncio support.  The result is a ``NotImplementedError``
exception such as below.

.. code-block:: pytb

    <snip>
      File "c:\projects\pytest-twisted\.tox\py38-asyncioreactor\lib\site-packages\twisted\internet\asyncioreactor.py", line 320, in install
        reactor = AsyncioSelectorReactor(eventloop)
      File "c:\projects\pytest-twisted\.tox\py38-asyncioreactor\lib\site-packages\twisted\internet\asyncioreactor.py", line 69, in __init__
        super().__init__()
      File "c:\projects\pytest-twisted\.tox\py38-asyncioreactor\lib\site-packages\twisted\internet\base.py", line 571, in __init__
        self.installWaker()
      File "c:\projects\pytest-twisted\.tox\py38-asyncioreactor\lib\site-packages\twisted\internet\posixbase.py", line 286, in installWaker
        self.addReader(self.waker)
      File "c:\projects\pytest-twisted\.tox\py38-asyncioreactor\lib\site-packages\twisted\internet\asyncioreactor.py", line 151, in addReader
        self._asyncioEventloop.add_reader(fd, callWithLogger, reader,
      File "C:\Python38-x64\Lib\asyncio\events.py", line 501, in add_reader
        raise NotImplementedError
    NotImplementedError

The previous default, the selector loop, still works but you have to
explicitly set it and do so early. The following ``conftest.py`` is provided
for reference.

.. code-block:: python3

    import sys

    import pytest
    import pytest_twisted


    @pytest.hookimpl(tryfirst=True)
    def pytest_configure(config):
        # https://twistedmatrix.com/trac/ticket/9766
        # https://github.com/pytest-dev/pytest-twisted/issues/80

        if (
            config.getoption("reactor", "default") == "asyncio"
            and sys.platform == 'win32'
            and sys.version_info >= (3, 8)
        ):
            import asyncio

            selector_policy = asyncio.WindowsSelectorEventLoopPolicy()
            asyncio.set_event_loop_policy(selector_policy)


Python 2 support plans
======================

At some point it may become impractical to retain Python 2 support.
Given the small size and very low amount of development it seems
likely that this will not be a near term issue.  While I personally
have no need for Python 2 support I try to err on the side of being
helpful so support will not be explicitly removed just to not have to
think about it.  If major issues are reported and neither myself nor
the community have time to resolve them then options will be
considered.


Installation
============
Install the plugin as below.

.. code-block:: sh

    pip install pytest-twisted


Using the plugin
================

The plugin is available after installation and can be disabled using
``-p no:twisted``.

By default ``twisted.internet.default`` is used to install the reactor.
This creates the same reactor that ``import twisted.internet.reactor``
would.  Alternative reactors can be specified using the ``--reactor``
option.  This presently supports ``qt5reactor`` for use with ``pyqt5``
and ``pytest-qt`` as well as ``asyncio``. This `guide`_ describes how to add
support for a new reactor.

The reactor is automatically created prior to the first test but can
be explicitly installed earlier by calling
``pytest_twisted.init_default_reactor()`` or the corresponding function
for the desired alternate reactor.


inlineCallbacks
===============
Using ``twisted.internet.defer.inlineCallbacks`` as a decorator for test
functions, which use fixtures, does not work. Please use
``pytest_twisted.inlineCallbacks`` instead.

.. code-block:: python

  @pytest_twisted.inlineCallbacks
  def test_some_stuff(tmpdir):
      res = yield threads.deferToThread(os.listdir, tmpdir.strpath)
      assert res == []


ensureDeferred
==============
Using ``twisted.internet.defer.ensureDeferred`` as a decorator for test
functions, which use fixtures, does not work. Please use
``pytest_twisted.ensureDeferred`` instead.

.. code-block:: python

  @pytest_twisted.ensureDeferred
  async def test_some_stuff(tmpdir):
      res = await threads.deferToThread(os.listdir, tmpdir.strpath)
      assert res == []


Waiting for deferreds in fixtures
=================================
``pytest_twisted.blockon`` allows fixtures to wait for deferreds.

.. code-block:: python

  @pytest.fixture
  def val():
      d = defer.Deferred()
      reactor.callLater(1.0, d.callback, 10)
      return pytest_twisted.blockon(d)


async/await fixtures
====================
``async``/``await`` fixtures can be used along with ``yield`` for normal
pytest fixture semantics of setup, value, and teardown.  At present only
function and module scope are supported.

.. code-block:: python

  # No yield (coroutine function)
  #   -> use pytest_twisted.async_fixture()
  @pytest_twisted.async_fixture()
  async def foo():
      d = defer.Deferred()
      reactor.callLater(0.01, d.callback, 42)
      value = await d
      return value

  # With yield (asynchronous generator)
  #   -> use pytest_twisted.async_yield_fixture()
  @pytest_twisted.async_yield_fixture()
  async def foo_with_teardown():
      d1, d2 = defer.Deferred(), defer.Deferred()
      reactor.callLater(0.01, d1.callback, 42)
      reactor.callLater(0.02, d2.callback, 37)
      value = await d1
      yield value
      await d2


Hypothesis
==========
pytest-twisted can be used with Hypothesis.

.. code-block:: python

   @hypothesis.given(x=hypothesis.strategies.integers())
   @pytest_twisted.ensureDeferred
   async def test_async(x):
       assert isinstance(x, int)


The twisted greenlet
====================
Some libraries (e.g. corotwine) need to know the greenlet, which is
running the twisted reactor. It's available from the
``twisted_greenlet`` fixture. The following code can be used to make
corotwine work with pytest-twisted.

.. code-block:: python

  @pytest.fixture(scope="session", autouse=True)
  def set_MAIN(request, twisted_greenlet):
      from corotwine import protocol
      protocol.MAIN = twisted_greenlet


That's (almost) all.


Deprecations
============

----
v1.9
----

``pytest.blockon``
    Use ``pytest_twisted.blockon``
``pytest.inlineCallbacks``
    Use ``pytest_twisted.inlineCallbacks``


.. |PyPI| image:: https://img.shields.io/pypi/v/pytest-twisted.svg
   :alt: PyPI version
   :target: https://pypi.python.org/pypi/pytest-twisted

.. |Pythons| image:: https://img.shields.io/pypi/pyversions/pytest-twisted.svg
   :alt: Supported Python versions
   :target: https://pypi.python.org/pypi/pytest-twisted

.. |Travis| image:: https://travis-ci.org/pytest-dev/pytest-twisted.svg?branch=master
   :alt: Travis build status
   :target: https://travis-ci.org/pytest-dev/pytest-twisted

.. |AppVeyor| image:: https://ci.appveyor.com/api/projects/status/eb1vp9hysp463c66/branch/master?svg=true
   :alt: AppVeyor build status
   :target: https://ci.appveyor.com/project/pytestbot/pytest-twisted

.. |Actions| image:: https://img.shields.io/github/workflow/status/pytest-dev/pytest-twisted/CI/master?logo=GitHub-Actions
   :alt: GitHub Actions build status
   :target: https://github.com/pytest-dev/pytest-twisted/actions?query=branch%3Amaster

.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :alt: Black code style
   :target: https://github.com/ambv/black

.. _guide: CONTRIBUTING.rst
