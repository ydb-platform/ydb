xlrd
====

|Build Status|_ |Coverage Status|_ |Documentation|_ |PyPI version|_

.. |Build Status| image:: https://circleci.com/gh/python-excel/xlrd/tree/master.svg?style=shield
.. _Build Status: https://circleci.com/gh/python-excel/xlrd/tree/master

.. |Coverage Status| image:: https://codecov.io/gh/python-excel/xlrd/branch/master/graph/badge.svg?token=lNSqwBBbvk
.. _Coverage Status: https://codecov.io/gh/python-excel/xlrd

.. |Documentation| image:: https://readthedocs.org/projects/xlrd/badge/?version=latest
.. _Documentation: http://xlrd.readthedocs.io/en/latest/?badge=latest

.. |PyPI version| image:: https://badge.fury.io/py/xlrd.svg
.. _PyPI version: https://badge.fury.io/py/xlrd


xlrd is a library for reading data and formatting information from Excel
files in the historical ``.xls`` format.

.. warning::

  This library will no longer read anything other than ``.xls`` files. For
  alternatives that read newer file formats, please see http://www.python-excel.org/.

The following are also not supported but will safely and reliably be ignored:

*   Charts, Macros, Pictures, any other embedded object, **including** embedded worksheets.
*   VBA modules
*   Formulas, but results of formula calculations are extracted.
*   Comments
*   Hyperlinks
*   Autofilters, advanced filters, pivot tables, conditional formatting, data validation

Password-protected files are not supported and cannot be read by this library.

Quick start:

.. code-block:: bash

    pip install xlrd
    
.. code-block:: python

    import xlrd
    book = xlrd.open_workbook("myfile.xls")
    print("The number of worksheets is {0}".format(book.nsheets))
    print("Worksheet name(s): {0}".format(book.sheet_names()))
    sh = book.sheet_by_index(0)
    print("{0} {1} {2}".format(sh.name, sh.nrows, sh.ncols))
    print("Cell D30 is {0}".format(sh.cell_value(rowx=29, colx=3)))
    for rx in range(sh.nrows):
        print(sh.row(rx))

From the command line, this will show the first, second and last rows of each sheet in each file:

.. code-block:: bash

    python PYDIR/scripts/runxlrd.py 3rows *blah*.xls
