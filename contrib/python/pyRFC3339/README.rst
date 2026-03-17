Description
===========

.. image:: https://github.com/kurtraschke/pyRFC3339/actions/workflows/python.yml/badge.svg
    :target: https://github.com/kurtraschke/pyRFC3339/actions/workflows/python.yml
    :alt: Build Status

.. image:: https://readthedocs.org/projects/pyrfc3339/badge/?version=latest
    :target: https://pyrfc3339.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

pyRFC3339 parses and generates :RFC:`3339`-compliant timestamps using `Python <https://www.python.org/>`_ `datetime.datetime <https://docs.python.org/3/library/datetime.html#datetime-objects>`_ objects.

>>> from pyrfc3339 import generate, parse
>>> from datetime import datetime, timezone
>>> generate(datetime.now(timezone.utc)) #doctest:+ELLIPSIS
'...T...Z'
>>> parse('2009-01-01T10:01:02Z')
datetime.datetime(2009, 1, 1, 10, 1, 2, tzinfo=datetime.timezone.utc)
>>> parse('2009-01-01T14:01:02-04:00')
datetime.datetime(2009, 1, 1, 14, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=72000)))

Installation
============

To install the latest version from `PyPI <https://pypi.org/>`_:

``$ pip install pyRFC3339``

To install the latest development version:

``$ pip install https://github.com/kurtraschke/pyRFC3339/tarball/main#egg=pyRFC3339-dev``

Tests as well as enforcement of code style, formatting, and type safety are run with `tox <https://tox.wiki/>`_:

``$ tox``

To build the documentation with Sphinx:

``$ tox -e docs``

The documentation is also available online at:

https://pyrfc3339.readthedocs.io/
