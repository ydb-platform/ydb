# Portions Copyright (C) 2010, Manfred Moitzi under a BSD licence

import types
from unittest import TestCase

import xlrd
from xlrd.timemachine import xrange

from .base import from_this_dir

SHEETINDEX = 0
NROWS = 15
NCOLS = 13

ROW_ERR = NROWS + 10
COL_ERR = NCOLS + 10


class TestSheet(TestCase):

    sheetnames = ['PROFILEDEF', 'AXISDEF', 'TRAVERSALCHAINAGE',
                  'AXISDATUMLEVELS', 'PROFILELEVELS']

    def setUp(self):
        self.book = xlrd.open_workbook(from_this_dir('profiles.xls'), formatting_info=True)

    def check_sheet_function(self, function):
        self.assertTrue(function(0, 0))
        self.assertTrue(function(NROWS-1, NCOLS-1))

    def check_sheet_function_index_error(self, function):
        self.assertRaises(IndexError, function, ROW_ERR, 0)
        self.assertRaises(IndexError, function, 0, COL_ERR)

    def check_col_slice(self, col_function):
        _slice = col_function(0, 2, NROWS-2)
        self.assertEqual(len(_slice), NROWS-4)

    def check_row_slice(self, row_function):
        _slice = row_function(0, 2, NCOLS-2)
        self.assertEqual(len(_slice), NCOLS-4)

    def test_nrows(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.assertEqual(sheet.nrows, NROWS)

    def test_ncols(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.assertEqual(sheet.ncols, NCOLS)

    def test_cell(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.assertNotEqual(xlrd.empty_cell, sheet.cell(0, 0))
        self.assertNotEqual(xlrd.empty_cell, sheet.cell(NROWS-1, NCOLS-1))

    def test_cell_error(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_sheet_function_index_error(sheet.cell)

    def test_cell_type(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_sheet_function(sheet.cell_type)

    def test_cell_type_error(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_sheet_function_index_error(sheet.cell_type)

    def test_cell_value(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_sheet_function(sheet.cell_value)

    def test_cell_value_error(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_sheet_function_index_error(sheet.cell_value)

    def test_cell_xf_index(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_sheet_function(sheet.cell_xf_index)

    def test_cell_xf_index_error(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_sheet_function_index_error(sheet.cell_xf_index)

    def test_col(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        col = sheet.col(0)
        self.assertEqual(len(col), NROWS)

    def test_row(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        row = sheet.row(0)
        self.assertEqual(len(row), NCOLS)

    def test_get_rows(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        rows = sheet.get_rows()
        self.assertTrue(isinstance(rows, types.GeneratorType), True)
        self.assertEqual(len(list(rows)), sheet.nrows)

    def test_col_slice(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_col_slice(sheet.col_slice)

    def test_col_types(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_col_slice(sheet.col_types)

    def test_col_values(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_col_slice(sheet.col_values)

    def test_row_slice(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_row_slice(sheet.row_slice)

    def test_row_types(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_row_slice(sheet.col_types)

    def test_row_values(self):
        sheet = self.book.sheet_by_index(SHEETINDEX)
        self.check_col_slice(sheet.row_values)


class TestSheetRagged(TestCase):

    def test_read_ragged(self):
        book = xlrd.open_workbook(from_this_dir('ragged.xls'), ragged_rows=True)
        sheet = book.sheet_by_index(0)
        self.assertEqual(sheet.row_len(0), 3)
        self.assertEqual(sheet.row_len(1), 2)
        self.assertEqual(sheet.row_len(2), 1)
        self.assertEqual(sheet.row_len(3), 4)
        self.assertEqual(sheet.row_len(4), 4)


class TestMergedCells(TestCase):

    def test_tidy_dimensions(self):
        book = xlrd.open_workbook(from_this_dir('merged_cells.xlsx'))
        for sheet in book.sheets():
            for rowx in xrange(sheet.nrows):
                self.assertEqual(sheet.row_len(rowx), sheet.ncols)
