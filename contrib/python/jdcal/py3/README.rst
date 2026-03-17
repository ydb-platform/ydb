jdcal
=====

.. _TPM: http://www.sal.wisc.edu/~jwp/astro/tpm/tpm.html
.. _Jeffrey W. Percival: http://www.sal.wisc.edu/~jwp/
.. _IAU SOFA: http://www.iausofa.org/
.. _pip: https://pypi.org/project/pip/
.. _easy_install: https://setuptools.readthedocs.io/en/latest/easy_install.html

.. image:: https://travis-ci.org/phn/jdcal.svg?branch=master
    :target: https://travis-ci.org/phn/jdcal


This module contains functions for converting between Julian dates and
calendar dates.

A function for converting Gregorian calendar dates to Julian dates, and
another function for converting Julian calendar dates to Julian dates
are defined. Two functions for the reverse calculations are also
defined.

Different regions of the world switched to Gregorian calendar from
Julian calendar on different dates. Having separate functions for Julian
and Gregorian calendars allow maximum flexibility in choosing the
relevant calendar.

Julian dates are stored in two floating point numbers (double).  Julian
dates, and Modified Julian dates, are large numbers. If only one number
is used, then the precision of the time stored is limited. Using two
numbers, time can be split in a manner that will allow maximum
precision. For example, the first number could be the Julian date for
the beginning of a day and the second number could be the fractional
day. Calculations that need the latter part can now work with maximum
precision.

All the above functions are "proleptic". This means that they work for
dates on which the concerned calendar is not valid. For example,
Gregorian calendar was not used prior to around October 1582.

A function to test if a given Gregorian calendar year is a leap year is
also defined.

Zero point of Modified Julian Date (MJD) and the MJD of 2000/1/1
12:00:00 are also given as module level constants.

Examples
--------

Some examples are given below. For more information see
https://oneau.wordpress.com/2011/08/30/jdcal/.

Gregorian calendar:

.. code-block:: python

    >>> from jdcal import gcal2jd, jd2gcal
    >>> gcal2jd(2000,1,1)
    (2400000.5, 51544.0)
    >>> 2400000.5 + 51544.0 + 0.5
    2451545.0

    >>> gcal2jd(2000,2,30)
    (2400000.5, 51604.0)
    >>> gcal2jd(2000,3,1)
    (2400000.5, 51604.0)
    >>> gcal2jd(2001,2,30)
    (2400000.5, 51970.0)
    >>> gcal2jd(2001,3,2)
    (2400000.5, 51970.0)

    >>> jd2gcal(*gcal2jd(2000,1,1))
    (2000, 1, 1, 0.0)
    >>> jd2gcal(*gcal2jd(1950,1,1))
    (1950, 1, 1, 0.0)

    >>> gcal2jd(2000,1,1)
    (2400000.5, 51544.0)
    >>> jd2gcal(2400000.5, 51544.0)
    (2000, 1, 1, 0.0)
    >>> jd2gcal(2400000.5, 51544.5)
    (2000, 1, 1, 0.5)
    >>> jd2gcal(2400000.5, 51544.245)
    (2000, 1, 1, 0.24500000000261934)
    >>> jd2gcal(2400000.5, 51544.1)
    (2000, 1, 1, 0.099999999998544808)
    >>> jd2gcal(2400000.5, 51544.75)
    (2000, 1, 1, 0.75)

Julian calendar:

.. code-block:: python

    >>> jd2jcal(*jcal2jd(2000, 1, 1))
    (2000, 1, 1, 0.0)
    >>> jd2jcal(*jcal2jd(-4000, 10, 11))
    (-4000, 10, 11, 0.0)

Gregorian leap year:

.. code-block:: python

    >>> from jdcal import is_leap
    >>> is_leap(2000)
    True
    >>> is_leap(2100)
    False

JD for zero point of MJD, and MJD for JD2000.0:

.. code-block:: python

    >>> from jdcal import MJD_0, MJD_JD2000
    >>> print MJD_0
    2400000.5
    >>> print MJD_JD2000
    51544.5


Installation
------------

The module can be installed using `pip`_ or `easy_install`_::

  $ pip install jdcal

or,

::

  $ easy_install jdcal


Tests are in ``test_jdcal.py``.

Credits
--------

1. A good amount of the code is based on the excellent `TPM`_ C library
   by `Jeffrey W. Percival`_.
2. The inspiration to split Julian dates into two numbers came from the
   `IAU SOFA`_ C library. No code or algorithm from the SOFA library is
   used in `jdcal`.

License
-------

Released under BSD; see LICENSE.txt.

For comments and suggestions, email to user `prasanthhn` in the `gmail.com`
domain.
