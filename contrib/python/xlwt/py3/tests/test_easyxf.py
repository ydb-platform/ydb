import unittest
import filecmp
import datetime

from .utils import in_tst_dir, in_tst_output_dir

import xlwt as xlwt
ezxf = xlwt.easyxf

def write_xls(file_name, sheet_name, headings, data, heading_xf, data_xfs):
    book = xlwt.Workbook()
    sheet = book.add_sheet(sheet_name)
    rowx = 0
    for colx, value in enumerate(headings):
        sheet.write(rowx, colx, value, heading_xf)
    sheet.set_panes_frozen(True) # frozen headings instead of split panes
    sheet.set_horz_split_pos(rowx+1) # in general, freeze after last heading row
    sheet.set_remove_splits(True) # if user does unfreeze, don't leave a split there
    for row in data:
        rowx += 1
        for colx, value in enumerate(row):
            sheet.write(rowx, colx, value, data_xfs[colx])
    book.save(file_name)

EXAMPLE_XLS = 'xlwt_easyxf_simple_demo.xls'

class TestUnicode0(unittest.TestCase):
    def create_example_xls(self, filename):
        mkd = datetime.date
        hdngs = ['Date', 'Stock Code', 'Quantity', 'Unit Price', 'Value', 'Message']
        kinds =  'date    text          int         price         money    text'.split()
        data = [
            [mkd(2007, 7, 1), 'ABC', 1000, 1.234567, 1234.57, ''],
            [mkd(2007, 12, 31), 'XYZ', -100, 4.654321, -465.43, 'Goods returned'],
            ] + [
                [mkd(2008, 6, 30), 'PQRCD', 100, 2.345678, 234.57, ''],
            ] * 100

        heading_xf = ezxf('font: bold on; align: wrap on, vert centre, horiz center')
        kind_to_xf_map = {
            'date': ezxf(num_format_str='yyyy-mm-dd'),
            'int': ezxf(num_format_str='#,##0'),
            'money': ezxf('font: italic on; pattern: pattern solid, fore-colour grey25',
                num_format_str='$#,##0.00'),
            'price': ezxf(num_format_str='#0.000000'),
            'text': ezxf(),
            }
        data_xfs = [kind_to_xf_map[k] for k in kinds]
        write_xls(filename, 'Demo', hdngs, data, heading_xf, data_xfs)

    def test_example_xls(self):
        self.create_example_xls(in_tst_output_dir(EXAMPLE_XLS))
        self.assertTrue(filecmp.cmp(in_tst_dir(EXAMPLE_XLS),
                                    in_tst_output_dir(EXAMPLE_XLS),
                                    shallow=False))
