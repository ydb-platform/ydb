Cerberus |latest-version|
=========================
|build-status| |python-support| |black|

Cerberus is a lightweight and extensible data validation library for Python.

.. code-block:: python

    >>> v = Validator({'name': {'type': 'string'}})
    >>> v.validate({'name': 'john doe'})
    True


Features
--------

Cerberus provides type checking and other base functionality out of the box and
is designed to be non-blocking and easily and widely extensible, allowing for
custom validation. It has no dependencies, but has the potential to become
yours.


Versioning & Interpreter support
--------------------------------

The Cerberus `1.x` versions can be used with Python 2 while version `2.0` and
later rely on Python 3 features.

Starting with Cerberus 1.2, it is maintained according to
`semantic versioning`_. So, a major release sheds off the old and defines a
space for the new, minor releases ship further new features and improvements
(you now the drill, new bugs are inevitable too), and micro releases polish a
definite amount of features to glory.

We intend to test Cerberus against all CPython interpreters at least until half
a year after their `end of life`_ and against the most recent PyPy interpreter
as a requirement for a release. If you still need to use it with a potential
security hole in your setup, it should most probably work with the latest
minor version branch from the time when the interpreter was still tested.
Subsequent minor versions have good chances as well. In any case, you are
advised to run the contributed test suite on your target system.


Funding
-------

Cerberus is an open source, collaboratively funded project. If you run a
business and are using Cerberus in a revenue-generating product, it would
make business sense to sponsor its development: it ensures the project that
your product relies on stays healthy and actively maintained. Individual users
are also welcome to make a recurring pledge or a one time donation if Cerberus
has helped you in your work or personal projects.

Every single sign-up makes a significant impact towards making Eve possible. To
learn more, check out our `funding page`_.


Documentation
-------------

Complete documentation is available at http://docs.python-cerberus.org


Installation
------------

Cerberus is on PyPI_, so all you need to do is:

.. code-block:: console

    $ pip install cerberus


Testing
-------

Just run:

.. code-block:: console

    $ python setup.py test

Or you can use tox to run the tests under all supported Python versions. Make
sure the required python versions are installed and run:

.. code-block:: console

    $ pip install tox  # first time only
    $ tox


Contributing
------------

Please see the `Contribution Guidelines`_.


Copyright
---------

Cerberus is an open source project by `Nicola Iarocci`_. See the license_ file
for more information.


.. _Contribution Guidelines: https://github.com/pyeve/cerberus/blob/master/CONTRIBUTING.rst
.. _end of life: https://devguide.python.org/#status-of-python-branches
.. _funding page: http://docs.python-cerberus.org/en/latest/funding.html
.. _license: https://github.com/pyeve/cerberus/blob/master/LICENSE
.. _Nicola Iarocci: https://nicolaiarocci.com/
.. _PyPI: https://pypi.python.org/
.. _semantic versioning: https://semver.org/

.. |black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :alt: Black code style
   :target: https://black.readthedocs.io/
.. |build-status| image:: https://travis-ci.org/pyeve/cerberus.svg?branch=master
   :alt: Build status
   :target: https://travis-ci.org/pyeve/cerberus
.. |latest-version| image:: https://img.shields.io/pypi/v/cerberus.svg
   :alt: Latest version on PyPI
   :target: https://pypi.org/project/cerberus
.. |license| image:: https://img.shields.io/pypi/l/cerberus.svg
   :alt: Software license
   :target: https://github.com/pyeve/cerberus/blob/master/LICENSE
.. |python-support| image:: https://img.shields.io/pypi/pyversions/cerberus.svg
   :target: https://pypi.python.org/pypi/cerberus
   :alt: Python versions
