# Portions Copyright (C) 2010, Manfred Moitzi under a BSD licence

import unittest

import xlrd
from xlrd.timemachine import UNICODE_LITERAL

from .base import from_this_dir


class TestCell(unittest.TestCase):

    def setUp(self):
        self.book = xlrd.open_workbook(from_this_dir('profiles.xls'), formatting_info=True)
        self.sheet = self.book.sheet_by_name('PROFILEDEF')

    def test_empty_cell(self):
        sheet = self.book.sheet_by_name('TRAVERSALCHAINAGE')
        cell = sheet.cell(0, 0)
        self.assertEqual(cell.ctype, xlrd.book.XL_CELL_EMPTY)
        self.assertEqual(cell.value, '')
        self.assertEqual(type(cell.value), type(UNICODE_LITERAL('')))
        self.assertTrue(cell.xf_index > 0)

    def test_string_cell(self):
        cell = self.sheet.cell(0, 0)
        self.assertEqual(cell.ctype, xlrd.book.XL_CELL_TEXT)
        self.assertEqual(cell.value, 'PROFIL')
        self.assertEqual(type(cell.value), type(UNICODE_LITERAL('')))
        self.assertTrue(cell.xf_index > 0)

    def test_number_cell(self):
        cell = self.sheet.cell(1, 1)
        self.assertEqual(cell.ctype, xlrd.book.XL_CELL_NUMBER)
        self.assertEqual(cell.value, 100)
        self.assertTrue(cell.xf_index > 0)

    def test_calculated_cell(self):
        sheet2 = self.book.sheet_by_name('PROFILELEVELS')
        cell = sheet2.cell(1, 3)
        self.assertEqual(cell.ctype, xlrd.book.XL_CELL_NUMBER)
        self.assertAlmostEqual(cell.value, 265.131, places=3)
        self.assertTrue(cell.xf_index > 0)

    def test_merged_cells(self):
        book = xlrd.open_workbook(from_this_dir('xf_class.xls'), formatting_info=True)
        sheet3 = book.sheet_by_name('table2')
        row_lo, row_hi, col_lo, col_hi = sheet3.merged_cells[0]
        self.assertEqual(sheet3.cell(row_lo, col_lo).value, 'MERGED')
        self.assertEqual((row_lo, row_hi, col_lo, col_hi), (3, 7, 2, 5))

    def test_merged_cells_xlsx(self):
        book = xlrd.open_workbook(from_this_dir('merged_cells.xlsx'))

        sheet1 = book.sheet_by_name('Sheet1')
        expected = []
        got = sheet1.merged_cells
        self.assertEqual(expected, got)

        sheet2 = book.sheet_by_name('Sheet2')
        expected = [(0, 1, 0, 2)]
        got = sheet2.merged_cells
        self.assertEqual(expected, got)

        sheet3 = book.sheet_by_name('Sheet3')
        expected = [(0, 1, 0, 2), (0, 1, 2, 4), (1, 4, 0, 2), (1, 9, 2, 4)]
        got = sheet3.merged_cells
        self.assertEqual(expected, got)

        sheet4 = book.sheet_by_name('Sheet4')
        expected = [(0, 1, 0, 2), (2, 20, 0, 1), (1, 6, 2, 5)]
        got = sheet4.merged_cells
        self.assertEqual(expected, got)
