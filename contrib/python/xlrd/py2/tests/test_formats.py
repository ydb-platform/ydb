# -*- coding: utf-8 -*-
# Portions Copyright (C) 2010, Manfred Moitzi under a BSD licence

import sys
from unittest import TestCase

import xlrd

from .base import from_this_dir

if sys.version_info[0] >= 3:
    def u(s): return s
else:
    def u(s):
        return s.decode('utf-8')


class TestCellContent(TestCase):

    def setUp(self):
        self.book = xlrd.open_workbook(from_this_dir('Formate.xls'), formatting_info=True)
        self.sheet = self.book.sheet_by_name(u('Blätt1'))

    def test_text_cells(self):
        for row, name in enumerate([u('Huber'), u('Äcker'), u('Öcker')]):
            cell = self.sheet.cell(row, 0)
            self.assertEqual(cell.ctype, xlrd.book.XL_CELL_TEXT)
            self.assertEqual(cell.value, name)
            self.assertTrue(cell.xf_index > 0)

    def test_date_cells(self):
        # see also 'Dates in Excel spreadsheets' in the documentation
        # convert: xldate_as_tuple(float, book.datemode) -> (year, month,
        # day, hour, minutes, seconds)
        for row, date in [(0, 2741.), (1, 38406.), (2, 32266.)]:
            cell = self.sheet.cell(row, 1)
            self.assertEqual(cell.ctype, xlrd.book.XL_CELL_DATE)
            self.assertEqual(cell.value, date)
            self.assertTrue(cell.xf_index > 0)

    def test_time_cells(self):
        # see also 'Dates in Excel spreadsheets' in the documentation
        # convert: xldate_as_tuple(float, book.datemode) -> (year, month,
        # day, hour, minutes, seconds)
        for row, time in [(3, .273611), (4, .538889), (5, .741123)]:
            cell = self.sheet.cell(row, 1)
            self.assertEqual(cell.ctype, xlrd.book.XL_CELL_DATE)
            self.assertAlmostEqual(cell.value, time, places=6)
            self.assertTrue(cell.xf_index > 0)

    def test_percent_cells(self):
        for row, time in [(6, .974), (7, .124)]:
            cell = self.sheet.cell(row, 1)
            self.assertEqual(cell.ctype, xlrd.book.XL_CELL_NUMBER)
            self.assertAlmostEqual(cell.value, time, places=3)
            self.assertTrue(cell.xf_index > 0)

    def test_currency_cells(self):
        for row, time in [(8, 1000.30), (9, 1.20)]:
            cell = self.sheet.cell(row, 1)
            self.assertEqual(cell.ctype, xlrd.book.XL_CELL_NUMBER)
            self.assertAlmostEqual(cell.value, time, places=2)
            self.assertTrue(cell.xf_index > 0)

    def test_get_from_merged_cell(self):
        sheet = self.book.sheet_by_name(u('ÖÄÜ'))
        cell = sheet.cell(2, 2)
        self.assertEqual(cell.ctype, xlrd.book.XL_CELL_TEXT)
        self.assertEqual(cell.value, 'MERGED CELLS')
        self.assertTrue(cell.xf_index > 0)

    def test_ignore_diagram(self):
        sheet = self.book.sheet_by_name(u('Blätt3'))
        cell = sheet.cell(0, 0)
        self.assertEqual(cell.ctype, xlrd.book.XL_CELL_NUMBER)
        self.assertEqual(cell.value, 100)
        self.assertTrue(cell.xf_index > 0)
