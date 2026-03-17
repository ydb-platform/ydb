pytest-rerunfailures
====================

.. START-SHORT-DESCRIPTION

pytest-rerunfailures is a plugin for `pytest <https://pytest.org>`_ that
re-runs tests to eliminate intermittent failures.

.. END-SHORT-DESCRIPTION

.. image:: https://img.shields.io/badge/license-MPL%202.0-blue.svg
   :target: https://github.com/pytest-dev/pytest-rerunfailures/blob/master/LICENSE
   :alt: License
.. image:: https://img.shields.io/pypi/v/pytest-rerunfailures.svg
   :target: https://pypi.python.org/pypi/pytest-rerunfailures/
   :alt: PyPI
.. image:: https://github.com/pytest-dev/pytest-rerunfailures/workflows/Test/badge.svg
   :target: https://github.com/pytest-dev/pytest-rerunfailures/actions
   :alt: GitHub Actions
.. image:: https://readthedocs.org/projects/pytest-rerunfailures/badge/?version=latest
    :target: https://pytest-rerunfailures.readthedocs.io/latest/?badge=latest
    :alt: Documentation Status

.. START-INSTALLATION

Requirements
------------

You will need the following prerequisites in order to use pytest-rerunfailures:

- Python 3.10+ or PyPy3
- pytest 8.0 or newer

This plugin can recover from a hard crash with the following optional
prerequisites:

- pytest-xdist 2.3.0 or newer

This package is currently tested against the last 5 minor pytest releases. In
case you work with an older version of pytest you should consider updating or
use one of the earlier versions of this package.

Installation
------------

To install pytest-rerunfailures:

.. code-block:: bash

  $ pip install pytest-rerunfailures

.. END-INSTALLATION

Recover from hard crashes
-------------------------

If one or more tests trigger a hard crash (for example: segfault), this plugin
will ordinarily be unable to rerun the test. However, if a compatible version of
pytest-xdist is installed, and the tests are run within pytest-xdist using the ``-n``
flag, this plugin will be able to rerun crashed tests, assuming the workers and
controller are on the same LAN (this assumption is valid for almost all cases
because most of the time the workers and controller are on the same computer).
If this assumption is not the case, then this functionality may not operate.

Re-run all failures
-------------------

To re-run all test failures, use the ``--reruns`` command line option with the
maximum number of times you'd like the tests to run:

.. code-block:: bash

  $ pytest --reruns 5

Failed fixture or setup_class will also be re-executed.

To add a delay time between re-runs use the ``--reruns-delay`` command line
option with the amount of seconds that you would like wait before the next
test re-run is launched:

.. code-block:: bash

   $ pytest --reruns 5 --reruns-delay 1

Re-run all failures matching certain expressions
------------------------------------------------

To re-run only those failures that match a certain list of expressions, use the
``--only-rerun`` flag and pass it a regular expression. For example,
the following would only rerun those errors that match ``AssertionError``:

.. code-block:: bash

   $ pytest --reruns 5 --only-rerun AssertionError

Passing the flag multiple times accumulates the arguments, so the following
would only rerun those errors that match ``AssertionError`` or ``ValueError``:

.. code-block:: bash

   $ pytest --reruns 5 --only-rerun AssertionError --only-rerun ValueError

Re-run all failures other than matching certain expressions
-----------------------------------------------------------

To re-run only those failures that do not match a certain list of expressions, use the
``--rerun-except`` flag and pass it a regular expression. For example,
the following would only rerun errors other than that match ``AssertionError``:

.. code-block:: bash

   $ pytest --reruns 5 --rerun-except AssertionError

Passing the flag multiple times accumulates the arguments, so the following
would only rerun those errors that does not match with ``AssertionError`` or ``OSError``:

.. code-block:: bash

   $ pytest --reruns 5 --rerun-except AssertionError --rerun-except OSError

Re-run individual failures
--------------------------

To mark individual tests as flaky, and have them automatically re-run when they
fail, add the ``flaky`` mark with the maximum number of times you'd like the
test to run:

.. code-block:: python

  @pytest.mark.flaky(reruns=5)
  def test_example():
      import random
      assert random.choice([True, False])

Note that when teardown fails, two reports are generated for the case, one for
the test case and the other for the teardown error.

You can also specify the re-run delay time in the marker:

.. code-block:: python

  @pytest.mark.flaky(reruns=5, reruns_delay=2)
  def test_example():
      import random
      assert random.choice([True, False])

You can also specify an optional ``condition`` in the re-run marker:

.. code-block:: python

   @pytest.mark.flaky(reruns=5, condition=sys.platform.startswith("win32"))
   def test_example():
      import random
      assert random.choice([True, False])

Exception filtering can be accomplished by specifying regular expressions for
``only_rerun`` and ``rerun_except``. They override the ``--only-rerun`` and
``--rerun-except`` command line arguments, respectively.

Arguments can be a single string:

.. code-block:: python

   @pytest.mark.flaky(rerun_except="AssertionError")
   def test_example():
       raise AssertionError()

Or a list of strings:

.. code-block:: python

   @pytest.mark.flaky(only_rerun=["AssertionError", "ValueError"])
   def test_example():
       raise AssertionError()


You can use ``@pytest.mark.flaky(condition)`` similarly as ``@pytest.mark.skipif(condition)``, see `pytest-mark-skipif <https://docs.pytest.org/en/6.2.x/reference.html#pytest-mark-skipif>`_

.. code-block:: python

    @pytest.mark.flaky(reruns=2,condition="sys.platform.startswith('win32')")
    def test_example():
        import random
        assert random.choice([True, False])
    # totally same as the above
    @pytest.mark.flaky(reruns=2,condition=sys.platform.startswith("win32"))
    def test_example():
      import random
      assert random.choice([True, False])

Note that the test will re-run for any ``condition`` that is truthy.

Force rerun count
-----------------

To force a specific re-run count globally, irrespective of the number
of re-runs specified in test markers, pass ``--force-reruns``:

.. code-block:: bash

   $ pytest --force-reruns 5

Output
------

Here's an example of the output provided by the plugin when run with
``--reruns 2`` and ``-r aR``::

  test_report.py RRF

  ================================== FAILURES ==================================
  __________________________________ test_fail _________________________________

      def test_fail():
  >       assert False
  E       assert False

  test_report.py:9: AssertionError
  ============================ rerun test summary info =========================
  RERUN test_report.py::test_fail
  RERUN test_report.py::test_fail
  ============================ short test summary info =========================
  FAIL test_report.py::test_fail
  ======================= 1 failed, 2 rerun in 0.02 seconds ====================

Note that output will show all re-runs. Tests that fail on all the re-runs will
be marked as failed.

.. START-COMPATIBILITY

Compatibility
-------------

* This plugin is *not* compatible with pytest-xdist's --looponfail flag.
* This plugin is *not* compatible with the core --pdb flag.
* This plugin is *not* compatible with the plugin
  `flaky <https://pypi.org/project/flaky/>`_, you can only have
  ``pytest-rerunfailures`` or ``flaky`` but not both.

.. END-COMPATIBILITY

.. START-PRIORITY

Priority
-------------

You can specify arguments in three places. So if you set the number of reruns in all three,
which one takes priority?

* Top priority is the marker, such as ``@pytest.mark.flaky(reruns=1)``
* Second priority is what's specified on the command line, like ``--reruns=2``
* Last priority is the ``pyproject.toml`` (or ``pytest.ini``) file setting, like ``reruns = 3``

Additionally, all three can be overridden by passing ``--force-reruns`` argument
on the command line.

.. END-PRIORITY


.. START-CONTRIBUTING

Resources
---------

- `Issue Tracker <https://github.com/pytest-dev/pytest-rerunfailures/issues>`_
- `Code <https://github.com/pytest-dev/pytest-rerunfailures/>`_

Development
-----------

* Test execution count can be retrieved from the ``execution_count`` attribute
  in test ``item``'s object. Example:

  .. code-block:: python

    @hookimpl(tryfirst=True)
    def pytest_runtest_makereport(item, call):
        print(item.execution_count)

.. END-CONTRIBUTING
