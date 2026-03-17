# coding:utf-8

from io import BytesIO
import unittest

import xlwt

class TestSharedStringTable(unittest.TestCase):
    def test_shared_string_table(self):
        expected_result = b'\xfc\x00\x11\x00\x01\x00\x00\x00\x01\x00\x00\x00\x03\x00\x01\x1e\x04;\x04O\x04'
        string_record = xlwt.BIFFRecords.SharedStringTable(encoding='cp1251')
        string_record.add_str(u'Оля')
        self.assertEqual(expected_result, string_record.get_biff_record())

class TestIntersheetsRef(unittest.TestCase):
    def test_intersheets_ref(self):
        book = xlwt.Workbook()
        sheet_a = book.add_sheet('A')
        sheet_a.write(0, 0, 'A1')
        sheet_a.write(0, 1, 'A2')
        sheet_b = book.add_sheet('B')
        sheet_b.write(0, 0, xlwt.Formula("'A'!$A$1&'A'!$A$2"))
        out = BytesIO()
        book.save(out)
