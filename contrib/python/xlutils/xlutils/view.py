# Copyright (c) 2013 Simplistix Ltd
#
# This Software is released under the MIT License:
# http://www.opensource.org/licenses/mit-license.html
# See license.txt for more details.

from datetime import datetime, time
from xlrd import open_workbook, XL_CELL_DATE, xldate_as_tuple
from xlwt.Utils import col_by_name

from .compat import xrange

class Index(object):
    def __init__(self, name):
        self.name = name
        
class Row(Index):
    """
    A one-based, end-inclusive row index for use in slices,
    eg:: ``[Row(1):Row(2), :]``
    """
    def __index__(self):
        return int(self.name) - 1
    
class Col(Index):
    """
    An end-inclusive column label index for use in slices,
    eg: ``[:, Col('A'), Col('B')]``
    """
    def __index__(self):
        return col_by_name(self.name)
    
class SheetView(object):
    """
    A view on a sheet in a workbook. Should be created by indexing a
    :class:`View`.
    
    These can be sliced to create smaller views.
    
    Views can be iterated over to return a set of iterables, one for each row
    in the view. Data is returned as in the cell values with the exception of
    dates and times which are converted into :class:`~datetime.datetime`
    instances.
    """
    def __init__(self, book, sheet, row_slice=None, col_slice=None):
        #: The workbook used by this view.
        self.book = book
        #: The sheet in the workbook used by this view.
        self.sheet = sheet
        for name, source in (('rows', row_slice), ('cols', col_slice)):
            start = 0
            stop = max_n = getattr(self.sheet, 'n'+name)
            if isinstance(source, slice):
                if source.start is not None:
                    start_val = source.start
                    if isinstance(start_val, Index):
                        start_val = start_val.__index__()
                    if start_val <  0:
                        start = max(0, max_n + start_val)
                    elif start_val > 0:
                        start = min(max_n, start_val)
                if source.stop is not None:
                    stop_val = source.stop
                    if isinstance(stop_val, Index):
                        stop_val = stop_val.__index__() + 1
                    if stop_val <  0:
                        stop = max(0, max_n + stop_val)
                    elif stop_val > 0:
                        stop = min(max_n, stop_val)
            setattr(self, name, xrange(start, stop))

    def __row(self, rowx):
        for colx in self.cols:
            value = self.sheet.cell_value(rowx, colx)
            if self.sheet.cell_type(rowx, colx) == XL_CELL_DATE:
                date_parts = xldate_as_tuple(value, self.book.datemode)
                # Times come out with a year of 0.
                if date_parts[0]:
                    value = datetime(*date_parts)
                else:
                    value = time(*date_parts[3:])
            yield value
            
    def __iter__(self):
        for rowx in self.rows:
            yield self.__row(rowx)

    def __getitem__(self, slices):
        assert isinstance(slices, tuple)
        assert len(slices)==2
        return self.__class__(self.book, self.sheet, *slices)
        

class View(object):
    """
    A view wrapper around a :class:`~xlrd.Book` that allows for easy
    iteration over the data in a group of cells.

    :param path: The path of the .xls from which to create views.
    :param class_: An class to use instead of :class:`SheetView` for views of sheets.
    """

    #: This can be replaced in a sub-class to use something other than
    #: :class:`SheetView` for the views of sheets returned.
    class_ = SheetView

    def __init__(self, path, class_=None):
        self.class_ = class_ or self.class_
        self.book = open_workbook(path, formatting_info=0, on_demand=True)

    def __getitem__(self, item):
        """
        Returns of a view of a sheet in the workbook this view is created for.
        
        :param item: either zero-based integer index or a sheet name.
        """
        if isinstance(item, int):
            sheet = self.book.sheet_by_index(item)
        else:
            sheet = self.book.sheet_by_name(item)
        return self.class_(self.book, sheet)

class CheckSheet(SheetView):
    """
    A special sheet view for use in automated tests.
    """
    
    def compare(self, *expected):
        """
        Call to check whether this view contains the expected data.
        If it does not, a descriptive :class:`AssertionError` will
        be raised. Requires
        `testfixtures <http://www.simplistix.co.uk/software/python/testfixtures>`__.

        :param expected: tuples containing the data that should be
                         present in this view.
        """
        actual = []
        for row in self:
            actual.append(tuple(row))

        # late import in case testfixtures isn't around!
        from testfixtures import compare as _compare
        _compare(expected, actual=tuple(actual))

class CheckerView(View):
    """
    A special subclass of :class:`View` for use in automated tests when you
    want to check the contents of a generated spreadsheet.

    Views of sheets are returned as :class:`CheckSheet` instances which have a
    handy :meth:`~CheckSheet.compare` method.
    """
    class_ = CheckSheet
