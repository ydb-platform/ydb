from unittest import TestCase

from xlrd import open_workbook
from xlrd.biffh import XL_CELL_TEXT

from .base import from_this_dir


class TestMissingRecords(TestCase):

    def setUp(self):
        path = from_this_dir('biff4_no_format_no_window2.xls')
        self.book = open_workbook(path)
        self.sheet = self.book.sheet_by_index(0)

    def test_default_format(self):
        cell = self.sheet.cell(0, 0)
        self.assertEqual(cell.ctype, XL_CELL_TEXT)

    def test_default_window2_options(self):
        self.assertEqual(self.sheet.cached_page_break_preview_mag_factor, 0)
        self.assertEqual(self.sheet.cached_normal_view_mag_factor, 0)
