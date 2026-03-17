|Travis|_ |Coveralls|_ |Docs|_ |PyPI|_

.. |Travis| image:: https://api.travis-ci.org/python-excel/xlutils.svg?branch=master
.. _Travis: https://travis-ci.org/python-excel/xlutils

.. |Coveralls| image:: https://coveralls.io/repos/python-excel/xlutils/badge.svg?branch=master
.. _Coveralls: https://coveralls.io/r/python-excel/xlutils?branch=master

.. |Docs| image:: https://readthedocs.org/projects/xlutils/badge/?version=latest
.. _Docs: http://xlutils.readthedocs.org/en/latest/

.. |PyPI| image:: https://badge.fury.io/py/xlutils.svg
.. _PyPI: https://badge.fury.io/py/xlutils
    
xlutils
=======

This package provides a collection of utilities for working with Excel
files. Since these utilities may require either or both of the xlrd
and xlwt packages, they are collected together here, separate from either
package.

Currently available are:

**xlutils.copy**
  Tools for copying xlrd.Book objects to xlwt.Workbook objects.

**xlutils.display**
  Utility functions for displaying information about xlrd-related
  objects in a user-friendly and safe fashion.

**xlutils.filter**
  A mini framework for splitting and filtering Excel files into new
  Excel files.

**xlutils.margins**
  Tools for finding how much of an Excel file contains useful data.

**xlutils.save**
  Tools for serializing xlrd.Book objects back to Excel files.

**xlutils.styles**
  Tools for working with formatting information expressed in styles.

Installation
============

Do the following in your virtualenv::

  pip install xlutils

Documentation
=============

The latest documentation can also be found at:
http://xlutils.readthedocs.org/en/latest/

Problems?
=========
Try the following in this order:

- Read the source

- Ask a question on http://groups.google.com/group/python-excel/

Licensing
=========

Copyright (c) 2008-2015 Simplistix Ltd.
See docs/license.txt for details.
