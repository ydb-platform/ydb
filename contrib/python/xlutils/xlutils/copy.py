# Copyright (c) 2009-2012 Simplistix Ltd
#
# This Software is released under the MIT License:
# http://www.opensource.org/licenses/mit-license.html
# See license.txt for more details.

from xlutils.filter import process,XLRDReader,XLWTWriter

def copy(wb):
    """
    Copy an :class:`xlrd.Book` into an :class:`xlwt.Workbook` preserving as much
    information from the source object as possible.

    See the :doc:`copy` documentation for an example.
    """
    w = XLWTWriter()
    process(
        XLRDReader(wb,'unknown.xls'),
        w
        )
    return w.output[0][1]
