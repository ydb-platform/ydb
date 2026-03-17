""" Tablib - XLS Support.
"""
import datetime
import re
from io import BytesIO

import xlrd
import xlwt
from xlrd.xldate import xldate_as_datetime

import tablib

# special styles
wrap = xlwt.easyxf("alignment: wrap on")
bold = xlwt.easyxf("font: bold on")
datetime_style = xlwt.easyxf(num_format_str='M/D/YY h:mm')
date_style = xlwt.easyxf(num_format_str='M/D/YY')
time_style = xlwt.easyxf(num_format_str='h:mm:ss')

INVALID_TITLE_REGEX = re.compile(r'[\\*?:/\[\]]')


def safe_xls_sheet_title(s, replace="-"):
    return re.sub(INVALID_TITLE_REGEX, replace, s)[:31]


class XLSFormat:
    title = 'xls'
    extensions = ('xls',)

    @classmethod
    def detect(cls, stream):
        """Returns True if given stream is a readable excel file."""
        try:
            xlrd.open_workbook(file_contents=stream)
            return True
        except Exception:
            pass
        try:
            xlrd.open_workbook(file_contents=stream.read())
            return True
        except Exception:
            pass
        try:
            xlrd.open_workbook(filename=stream)
            return True
        except Exception:
            return False

    @classmethod
    def export_set(cls, dataset, invalid_char_subst="-"):
        """Returns XLS representation of Dataset.

        If ``dataset.title`` contains characters which are
        considered invalid for an XLSX/XLS file sheet name
        (https://web.archive.org/web/20230323081941/https://www.excelcodex.com/2012/06/worksheets-naming-conventions/),
        they will be replaced with ``invalid_char_subst``.

        """

        wb = xlwt.Workbook(encoding='utf8')
        ws = wb.add_sheet(
            safe_xls_sheet_title(dataset.title, invalid_char_subst)
            if dataset.title
            else 'Tablib Dataset'
        )

        cls.dset_sheet(dataset, ws)

        stream = BytesIO()
        wb.save(stream)
        return stream.getvalue()

    @classmethod
    def export_book(cls, databook, invalid_char_subst="-"):
        """Returns XLS representation of DataBook."""

        wb = xlwt.Workbook(encoding='utf8')

        for i, dset in enumerate(databook._datasets):
            ws = wb.add_sheet(safe_xls_sheet_title(
                dset.title, invalid_char_subst)
                if dset.title else f"Sheet{i}"
            )

            cls.dset_sheet(dset, ws)

        stream = BytesIO()
        wb.save(stream)
        return stream.getvalue()

    @classmethod
    def import_set(cls, dset, in_stream, headers=True, skip_lines=0):
        """Returns databook from XLS stream."""

        dset.wipe()

        xls_book = xlrd.open_workbook(file_contents=in_stream.read())
        sheet = xls_book.sheet_by_index(0)

        dset.title = sheet.name

        def cell_value(value, type_):
            if type_ == xlrd.XL_CELL_ERROR:
                return xlrd.error_text_from_code[value]
            elif type_ == xlrd.XL_CELL_DATE:
                return xldate_as_datetime(value, xls_book.datemode)
            return value

        for i in range(sheet.nrows):
            if i < skip_lines:
                continue
            if i == skip_lines and headers:
                dset.headers = sheet.row_values(i)
            else:
                dset.append([
                    cell_value(val, typ)
                    for val, typ in zip(sheet.row_values(i), sheet.row_types(i))
                ])

    @classmethod
    def import_book(cls, dbook, in_stream, headers=True):
        """Returns databook from XLS stream."""

        dbook.wipe()

        xls_book = xlrd.open_workbook(file_contents=in_stream.read())

        for sheet in xls_book.sheets():
            data = tablib.Dataset()
            data.title = sheet.name

            for i in range(sheet.nrows):
                if i == 0 and headers:
                    data.headers = sheet.row_values(0)
                else:
                    data.append(sheet.row_values(i))

            dbook.add_sheet(data)

    @classmethod
    def dset_sheet(cls, dataset, ws):
        """Completes given worksheet from given Dataset."""
        _package = dataset._package(dicts=False)

        for i, sep in enumerate(dataset._separators):
            _offset = i
            _package.insert((sep[0] + _offset), (sep[1],))

        for i, row in enumerate(_package):
            for j, col in enumerate(row):

                # bold headers
                if (i == 0) and dataset.headers:
                    ws.write(i, j, col, bold)

                    # frozen header row
                    ws.panes_frozen = True
                    ws.horz_split_pos = 1

                # bold separators
                elif len(row) < dataset.width:
                    ws.write(i, j, col, bold)

                # format date types
                elif isinstance(col, datetime.datetime):
                    ws.write(i, j, col, datetime_style)
                elif isinstance(col, datetime.date):
                    ws.write(i, j, col, date_style)
                elif isinstance(col, datetime.time):
                    ws.write(i, j, col, time_style)
                # wrap the rest
                else:
                    try:
                        if '\n' in col:
                            ws.write(i, j, col, wrap)
                        else:
                            ws.write(i, j, col)
                    except TypeError:
                        ws.write(i, j, col)
