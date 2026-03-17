.. image:: https://img.shields.io/pypi/v/pytest-django.svg?style=flat
    :alt: PyPI Version
    :target: https://pypi.python.org/pypi/pytest-django

.. image:: https://img.shields.io/pypi/pyversions/pytest-django.svg
    :alt: Supported Python versions
    :target: https://pypi.python.org/pypi/pytest-django

.. image:: https://travis-ci.org/pytest-dev/pytest-django.svg?branch=master
    :alt: Build Status
    :target: https://travis-ci.org/pytest-dev/pytest-django

.. image:: https://img.shields.io/codecov/c/github/pytest-dev/pytest-django.svg?style=flat
    :alt: Coverage
    :target: https://codecov.io/gh/pytest-dev/pytest-django

Welcome to pytest-django!
=========================

pytest-django allows you to test your Django project/applications with the
`pytest testing tool <https://pytest.org/>`_.

* `Quick start / tutorial
  <https://pytest-django.readthedocs.io/en/latest/tutorial.html>`_
* `Changelog <https://pytest-django.readthedocs.io/en/latest/changelog.html>`_
* Full documentation: https://pytest-django.readthedocs.io/en/latest/
* `Contribution docs
  <https://pytest-django.readthedocs.io/en/latest/contributing.html>`_
* Version compatibility:

  * Django: 1.8-1.11, 2.0-2.2,
    and latest master branch (compatible at the time of each release)
  * Python: CPython 2.7, 3.4-3.7 or PyPy 2, 3
  * pytest: >=3.6

* Licence: BSD
* Project maintainers: Andreas Pelme, Floris Bruynooghe and Daniel Hahler
* `All contributors <https://github.com/pytest-dev/pytest-django/contributors>`_
* GitHub repository: https://github.com/pytest-dev/pytest-django
* `Issue tracker <http://github.com/pytest-dev/pytest-django/issues>`_
* `Python Package Index (PyPI) <https://pypi.python.org/pypi/pytest-django/>`_

Install pytest-django
---------------------

::

    pip install pytest-django

Why would I use this instead of Django's `manage.py test` command?
------------------------------------------------------------------

Running your test suite with pytest-django allows you to tap into the features
that are already present in pytest. Here are some advantages:

* `Manage test dependencies with pytest fixtures. <https://pytest.org/en/latest/fixture.html>`_
* Less boilerplate tests: no need to import unittest, create a subclass with methods. Write tests as regular functions.
* Database re-use: no need to re-create the test database for every test run.
* Run tests in multiple processes for increased speed (with the pytest-xdist plugin).
* Make use of other `pytest plugins <https://pytest.org/en/latest/plugins.html>`_.
* Works with both worlds: Existing unittest-style TestCase's still work without any modifications.

See the `pytest documentation <https://pytest.org/en/latest/>`_ for more information on pytest itself.
