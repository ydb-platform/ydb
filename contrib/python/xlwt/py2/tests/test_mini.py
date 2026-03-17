import sys
import os
import unittest
import filecmp

from .utils import in_tst_dir, in_tst_output_dir

import xlwt

def from_tst_dir(filename):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

class TestMini(unittest.TestCase):
    def test_create_mini_xls(self):
        book = xlwt.Workbook()
        sheet = book.add_sheet('xlwt was here')
        book.save(in_tst_output_dir('mini.xls'))

        self.assertTrue(filecmp.cmp(in_tst_dir('mini.xls'),
                                    in_tst_output_dir('mini.xls'),
                                    shallow=False))
