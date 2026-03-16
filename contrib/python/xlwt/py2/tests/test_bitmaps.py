# -*- coding: utf-8 -*-

import sys
import os
import unittest
import filecmp
from datetime import datetime

from .utils import in_tst_dir, in_tst_output_dir

import xlwt

class TestBitmaps(unittest.TestCase):
    def test_create_bitmap_from_memory(self):
        book = xlwt.Workbook()
        sheet = book.add_sheet('bitmap test')
        
        with open (in_tst_dir("python.bmp"), "rb") as bmpfile:
            bmpdata = bmpfile.read()
            sheet.insert_bitmap_data(bmpdata, 1, 1)
        
        book.save(in_tst_output_dir('bitmaps.xls'))
        
        self.assertTrue(filecmp.cmp(in_tst_dir('bitmaps.xls'),
                                    in_tst_output_dir('bitmaps.xls'),
                                    shallow=False))

    def test_create_bitmap_from_file(self):
        book = xlwt.Workbook()
        sheet = book.add_sheet('bitmap test')
        
        sheet.insert_bitmap(in_tst_dir("python.bmp"), 1, 1)
        
        book.save(in_tst_output_dir('bitmaps.xls'))
        
        self.assertTrue(filecmp.cmp(in_tst_dir('bitmaps.xls'),
                                    in_tst_output_dir('bitmaps.xls'),
                                    shallow=False))

if __name__=='__main__':
    unittest.main()
