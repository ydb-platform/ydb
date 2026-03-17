===============
pytest-randomly
===============

.. image:: https://img.shields.io/github/actions/workflow/status/pytest-dev/pytest-randomly/main.yml.svg?branch=main&style=for-the-badge
   :target: https://github.com/pytest-dev/pytest-randomly/actions?workflow=CI

.. image:: https://img.shields.io/badge/Coverage-100%25-success?style=for-the-badge
  :target: https://github.com/pytest-dev/pytest-randomly/actions?workflow=CI

.. image:: https://img.shields.io/pypi/v/pytest-randomly.svg?style=for-the-badge
   :target: https://pypi.org/project/pytest-randomly/

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg?style=for-the-badge
   :target: https://github.com/psf/black

.. image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=for-the-badge
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit

Pytest plugin to randomly order tests and control ``random.seed``.

----

**Testing a Django project?**
Check out my book `Speed Up Your Django Tests <https://adamchainz.gumroad.com/l/suydt>`__ which covers loads of ways to write faster, more accurate tests.

----

Features
========

All of these features are on by default but can be disabled with flags.

* Randomly shuffles the order of test items. This is done first at the level of
  modules, then at the level of test classes (if you have them), then at the
  order of functions. This also works with things like doctests.

* Generates a base random seed or accepts one for reproduction with ``--randomly-seed``.
  The base random seed is printed at the start of the test run, and can be passed in to repeat a failure caused by test ordering or random data.

* At the start of the test run, and before each test setup, run, and teardown, it resets Python’s global random seed to a fixed value, using |random.seed()|__.
  The fixed value is derived from the base random seed, the pytest test ID, and an offset for setup or teardown.
  This ensures each test gets a different but repeatable random seed.

  .. |random.seed()| replace:: ``random.seed()``
  __ https://docs.python.org/3/library/random.html#random.seed

* pytest-randomly also resets several libraries’ random states at the start of
  every test, if they are installed:

  * `factory boy <https://factoryboy.readthedocs.io/en/latest/reference.html>`__

  * `Faker <https://pypi.org/project/faker>`__

    The ``faker`` pytest fixture is also affected, as pytest-randomly defines |the faker_seed fixture|__.

    .. |the faker_seed fixture| replace:: the ``faker_seed`` fixture
    __ https://faker.readthedocs.io/en/master/pytest-fixtures.html#seeding-configuration

  * `Model Bakery <https://model-bakery.readthedocs.io/en/latest/>`__

  * `NumPy <https://www.numpy.org/>`_

    Only its `legacy random state <https://numpy.org/doc/stable/reference/random/legacy.html>`__ is affected.

* If additional random generators are used, they can be registered under the
  ``pytest_randomly.random_seeder``
  `entry point <https://packaging.python.org/specifications/entry-points/>`_ and
  will have their seed reset at the start of every test. Register a function
  that takes the current seed value.

* Works with `pytest-xdist <https://pypi.org/project/pytest-xdist/>`__.

About
=====

Randomness in testing can be quite powerful to discover hidden flaws in the
tests themselves, as well as giving a little more coverage to your system.

By randomly ordering the tests, the risk of surprising inter-test dependencies
is reduced - a technique used in many places, for example Google's C++ test
runner `googletest
<https://code.google.com/p/googletest/wiki/V1_5_AdvancedGuide#Shuffling_the_Tests>`_.
Research suggests that "dependent tests do exist in practice" and a random
order of test executions can effectively detect such dependencies [1]_.
Alternatively, a reverse order of test executions, as provided by `pytest-reverse
<https://github.com/adamchainz/pytest-reverse>`__, may find less dependent
tests but can achieve a better benefit/cost ratio.

By resetting the random seed to a repeatable number for each test, tests can
create data based on random numbers and yet remain repeatable, for example
factory boy's fuzzy values. This is good for ensuring that tests specify the
data they need and that the tested system is not affected by any data that is
filled in randomly due to not being specified.

I have written a `blog post covering the history of
pytest-randomly <https://adamj.eu/tech/2018/01/08/pytest-randomly-history/>`__,
including how it started life as the nose plugin
`nose-randomly <https://github.com/adamchainz/nose-randomly>`__.

Additionally, I appeared on the Test and Code podcast to `talk about
pytest-randomly <https://testandcode.com/128>`__.

Installation
============

Install with:

.. code-block:: bash

    python -m pip install pytest-randomly

Python 3.9 to 3.14 supported.

Usage
=====

Pytest will automatically find the plugin and use it when you run ``pytest``.
The output will start with an extra line that tells you the random seed that is
being used:

.. code-block:: bash

    $ pytest
    ...
    platform darwin -- Python ...
    Using --randomly-seed=1553614239
    ...

If the tests fail due to ordering or randomly created data, you can restart
them with that seed using the flag as suggested:

.. code-block:: bash

    pytest --randomly-seed=1234

Or more conveniently, use the special value ``last``:

.. code-block:: bash

    pytest --randomly-seed=last

(This only works if pytest’s cacheprovider plugin has not been disabled.)

Since the ordering is by module, then by class, you can debug inter-test
pollution failures by narrowing down which tests are being run to find the bad
interaction by rerunning just the module/class:

.. code-block:: bash

    pytest --randomly-seed=1234 tests/module_that_failed/

You can disable behaviours you don't like with the following flags:

* ``--randomly-dont-reset-seed`` - turn off the reset of ``random.seed()`` at
  the start of every test
* ``--randomly-dont-reorganize`` - turn off the shuffling of the order of tests

The plugin appears to Pytest with the name 'randomly'. To disable it
altogether, you can use the ``-p`` argument, for example:

.. code-block:: sh

    pytest -p no:randomly

Avoid reordering some tests
---------------------------

To fix the order of some tests, use the ``pytest-order`` plugin.
See its `documentation section <https://pytest-order.readthedocs.io/en/latest/other_plugins.html#pytest-randomly>`__ on usage with pytest-randomly.

Entry Point
===========

If you're using a different randomness generator in your third party package,
you can register an entrypoint to be called every time ``pytest-randomly``
reseeds. Implement the entrypoint ``pytest_randomly.random_seeder``, referring
to a function/callable that takes one argument, the new seed (int).

For example in your ``setup.cfg``:

.. code-block:: ini

    [options.entry_points]
    pytest_randomly.random_seeder =
        mypackage = mypackage.reseed

Then implement ``reseed(new_seed)``.

References
==========

.. [1] Sai Zhang, Darioush Jalali, Jochen Wuttke, Kıvanç Muşlu, Wing Lam, Michael D. Ernst, and David Notkin. 2014. Empirically revisiting the test independence assumption. In Proceedings of the 2014 International Symposium on Software Testing and Analysis (ISSTA 2014). Association for Computing Machinery, New York, NY, USA, 385–396. doi:https://doi.org/10.1145/2610384.2610404
