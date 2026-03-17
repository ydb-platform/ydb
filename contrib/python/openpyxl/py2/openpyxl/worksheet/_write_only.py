from __future__ import absolute_import
# Copyright (c) 2010-2019 openpyxl


"""Write worksheets to xml representations in an optimized way"""

from inspect import isgenerator

from openpyxl.cell import Cell, WriteOnlyCell
from openpyxl.workbook.child import _WorkbookChild
from .worksheet import Worksheet
from openpyxl.utils.exceptions import WorkbookAlreadySaved

from ._writer import WorksheetWriter


class WriteOnlyWorksheet(_WorkbookChild):
    """
    Streaming worksheet. Optimised to reduce memory by writing rows just in
    time.
    Cells can be styled and have comments Styles for rows and columns
    must be applied before writing cells
    """

    __saved = False
    _writer = None
    _rows = None
    _rel_type = Worksheet._rel_type
    _path = Worksheet._path
    mime_type = Worksheet.mime_type


    def __init__(self, parent, title):
        super(WriteOnlyWorksheet, self).__init__(parent, title)
        self._max_col = 0
        self._max_row = 0

        # Methods from Worksheet
        self._add_row = Worksheet._add_row.__get__(self)
        self._add_column = Worksheet._add_column.__get__(self)
        self.add_chart = Worksheet.add_chart.__get__(self)
        self.add_image = Worksheet.add_image.__get__(self)
        self.add_table = Worksheet.add_table.__get__(self)

        setup = Worksheet._setup.__get__(self)
        setup()

        self.print_titles = Worksheet.print_titles.__get__(self)
        self.sheet_view = Worksheet.sheet_view.__get__(self)


    @property
    def freeze_panes(self):
        return Worksheet.freeze_panes.__get__(self)


    @freeze_panes.setter
    def freeze_panes(self, value):
        Worksheet.freeze_panes.__set__(self, value)


    @property
    def print_title_cols(self):
        return Worksheet.print_title_cols.__get__(self)


    @print_title_cols.setter
    def print_title_cols(self, value):
        Worksheet.print_title_cols.__set__(self, value)


    @property
    def print_title_rows(self):
        return Worksheet.print_title_rows.__get__(self)


    @print_title_rows.setter
    def print_title_rows(self, value):
        Worksheet.print_title_rows.__set__(self, value)


    @property
    def print_area(self):
        return Worksheet.print_area.__get__(self)


    @print_area.setter
    def print_area(self, value):
        Worksheet.print_area.__set__(self, value)


    @property
    def closed(self):
        return self.__saved


    def _write_rows(self):
        """
        Send rows to the writer's stream
        """
        try:
            xf = self._writer.xf.send(True)
        except StopIteration:
            self._already_saved()

        with xf.element("sheetData"):
            row_idx = 1
            try:
                while True:
                    row = (yield)
                    row = self._values_to_row(row, row_idx)
                    self._writer.write_row(xf, row, row_idx)
                    row_idx += 1
            except GeneratorExit:
                pass

        self._writer.xf.send(None)


    def _get_writer(self):
        if self._writer is None:
            self._writer = WorksheetWriter(self)
            self._writer.write_top()


    def close(self):
        if self.__saved:
            self._already_saved()

        self._get_writer()

        if self._rows is None:
            self._writer.write_rows()
        else:
            self._rows.close()

        self._writer.write_tail()

        self._writer.close()
        self.__saved = True


    def append(self, row):
        """
        :param row: iterable containing values to append
        :type row: iterable
        """

        if (not isgenerator(row) and
            not isinstance(row, (list, tuple, range))
            ):
            self._invalid_row(row)

        self._get_writer()

        if self._rows is None:
            self._rows = self._write_rows()
            next(self._rows)

        self._rows.send(row)


    def _values_to_row(self, values, row_idx):
        """
        Convert whatever has been appended into a form suitable for work_rows
        """
        cell = WriteOnlyCell(self)

        for col_idx, value in enumerate(values, 1):
            if value is None:
                continue
            try:
                cell.value = value
            except ValueError:
                if isinstance(value, Cell):
                    cell = value
                else:
                    raise ValueError

            cell.column = col_idx
            cell.row = row_idx

            if cell.hyperlink is not None:
                cell.hyperlink.ref = cell.coordinate

            yield cell

            # reset cell if style applied
            if cell.has_style or cell.hyperlink:
                cell = WriteOnlyCell(self)


    def _already_saved(self):
        raise WorkbookAlreadySaved('Workbook has already been saved and cannot be modified or saved anymore.')


    def _invalid_row(self, iterable):
        raise TypeError('Value must be a list, tuple, range or a generator Supplied value is {0}'.format(
            type(iterable))
                        )
