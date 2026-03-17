# -*- coding: utf-8 -*-

import sys
import os
import unittest
import filecmp
from datetime import datetime

from .utils import in_tst_dir, in_tst_output_dir

import xlwt

LOREM_IPSUM = u'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'


class TestSimple(unittest.TestCase):

    def create_simple_xls(self, **kw):
        font0 = xlwt.Font()
        font0.name = 'Times New Roman'
        font0.colour_index = 2
        font0.bold = True

        style0 = xlwt.XFStyle()
        style0.font = font0

        style1 = xlwt.XFStyle()
        style1.num_format_str = 'D-MMM-YY'

        wb = xlwt.Workbook(**kw)
        ws = wb.add_sheet('A Test Sheet')

        ws.write(0, 0, 'Test', style0)
        ws.write(1, 0, datetime(2010, 12, 5), style1)
        ws.write(2, 0, 1)
        ws.write(2, 1, 1)
        ws.write(2, 2, xlwt.Formula("A3+B3"))
        return wb, ws

    def test_create_simple_xls(self):
        wb, _ = self.create_simple_xls()
        wb.save(in_tst_output_dir('simple.xls'))
        self.assertTrue(filecmp.cmp(in_tst_dir('simple.xls'),
                                    in_tst_output_dir('simple.xls'),
                                    shallow=False))

    def test_create_less_simple_xls(self):
        wb, ws = self.create_simple_xls()
        more_content=[
            [
                u'A{0}'.format(i),
                u'Zażółć gęślą jaźń {0} {1}'.format(i, LOREM_IPSUM),
            ]
            for idx, i in enumerate(range(1000, 1050))
        ]
        for r_idx, content_row in enumerate(more_content, 3):
            for c_idx, cell in enumerate(content_row):
                ws.write(r_idx, c_idx, cell)
        wb.save(in_tst_output_dir('less_simple.xls'))
        self.assertTrue(filecmp.cmp(in_tst_dir('less_simple.xls'),
                                    in_tst_output_dir('less_simple.xls'),
                                    shallow=False))

    def test_font_compression(self):
        wb, ws = self.create_simple_xls(style_compression = 2)
        wb.save(in_tst_output_dir('simple.xls'), )

