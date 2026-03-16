pytest-forked: run each test in a forked subprocess
====================================================


.. warning::

	this is a extraction of the xdist --forked module,
	future maintenance beyond the bare minimum is not planned until a new maintainer is found.


This plugin **does not work on Windows** because there's no ``fork`` support.


* ``--forked``: run each test in a forked
  subprocess to survive ``SEGFAULTS`` or otherwise dying processes.

|python| |version| |ci| |pre-commit| |black|

.. |version| image:: http://img.shields.io/pypi/v/pytest-forked.svg
  :target: https://pypi.python.org/pypi/pytest-forked

.. |ci| image:: https://github.com/pytest-dev/pytest-forked/workflows/build/badge.svg
  :target: https://github.com/pytest-dev/pytest-forked/actions

.. |python| image:: https://img.shields.io/pypi/pyversions/pytest-forked.svg
  :target: https://pypi.python.org/pypi/pytest-forked/

.. |black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
  :target: https://github.com/ambv/black

.. |pre-commit| image:: https://results.pre-commit.ci/badge/github/pytest-dev/pytest-forked/master.svg
   :target: https://results.pre-commit.ci/latest/github/pytest-dev/pytest-forked/master

Installation
-----------------------

Install the plugin with::

    pip install pytest-forked

or use the package in develope/in-place mode with
a checkout of the `pytest-forked repository`_ ::

   pip install -e .


Usage examples
---------------------

If you have tests involving C or C++ libraries you might have to deal
with tests crashing the process.  For this case you may use the boxing
options::

    pytest --forked

which will run each test in a subprocess and will report if a test
crashed the process.  You can also combine this option with
running multiple processes via pytest-xdist to speed up the test run
and use your CPU cores::

    pytest -n3 --forked

this would run 3 testing subprocesses in parallel which each
create new forked subprocesses for each test.


You can also fork for individual tests::

    @pytest.mark.forked
    def test_with_leaky_state():
        run_some_monkey_patches()


This test will be unconditionally boxed, regardless of CLI flag.


.. _`pytest-forked repository`: https://github.com/pytest-dev/pytest-forked
