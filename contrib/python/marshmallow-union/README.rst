========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|

.. |docs| image:: https://readthedocs.org/projects/python-marshmallow-union/badge/?style=flat
    :target: https://readthedocs.org/projects/python-marshmallow-union
    :alt: Documentation Status


.. |travis| image:: https://travis-ci.org/adamboche/python-marshmallow-union.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/adamboche/python-marshmallow-union

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/adamboche/python-marshmallow-union?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/adamboche/python-marshmallow-union

.. |codecov| image:: https://codecov.io/github/adamboche/python-marshmallow-union/coverage.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/adamboche/python-marshmallow-union

.. |version| image:: https://img.shields.io/pypi/v/marshmallow-union.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/pypi/marshmallow-union

.. |commits-since| image:: https://img.shields.io/github/commits-since/adamboche/python-marshmallow-union/v0.1.15.svg
    :alt: Commits since latest release
    :target: https://github.com/adamboche/python-marshmallow-union/compare/v0.1.15..master

.. |wheel| image:: https://img.shields.io/pypi/wheel/marshmallow-union.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/pypi/marshmallow-union

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/marshmallow-union.svg
    :alt: Supported versions
    :target: https://pypi.org/pypi/marshmallow-union

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/marshmallow-union.svg
    :alt: Supported implementations
    :target: https://pypi.org/pypi/marshmallow-union


.. end-badges

Union fields for marshmallow.

* Free software: MIT license


Warning
===========

   This library works by trying a list of fields one by one, and (de)serializing with the first one not to raise an error.
   The type of the values is not taken into account, so if one of the fields in the union accepts values of an unexpected type,
   they will be used for serialization. This can lead to a surprising behavior, because :
   
   .. code-block:: python
   
       u = Union(fields=[fields.Integer(), fields.String()]) # the Integer field accepts string representations of integers
       type(u.deserialize('0')) # -> int
       
   If you want to have precise control of which field will be used for which value, you can use `marshmallow-polyfield <https://github.com/Bachmann1234/marshmallow-polyfield/>`_ instead of this library.



Documentation
=============


https://python-marshmallow-union.readthedocs.io/
