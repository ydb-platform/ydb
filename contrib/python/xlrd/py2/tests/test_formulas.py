# -*- coding: utf-8 -*-
# Portions Copyright (C) 2010, Manfred Moitzi under a BSD licence

from unittest import TestCase

import xlrd

from .base import from_this_dir

try:
    ascii
except NameError:
    # For Python 2
    def ascii(s):
        a = repr(s)
        if a.startswith(('u"', "u'")):
            a = a[1:]
        return a

class TestFormulas(TestCase):

    def setUp(self):
        book = xlrd.open_workbook(from_this_dir('formula_test_sjmachin.xls'))
        self.sheet = book.sheet_by_index(0)

    def get_value(self, col, row):
        return ascii(self.sheet.col_values(col)[row])

    def test_cell_B2(self):
        self.assertEqual(
            self.get_value(1, 1),
            r"'\u041c\u041e\u0421\u041a\u0412\u0410 \u041c\u043e\u0441\u043a\u0432\u0430'",
        )

    def test_cell_B3(self):
        self.assertEqual(self.get_value(1, 2), '0.14285714285714285')

    def test_cell_B4(self):
        self.assertEqual(self.get_value(1, 3), "'ABCDEF'")

    def test_cell_B5(self):
        self.assertEqual(self.get_value(1, 4), "''")

    def test_cell_B6(self):
        self.assertEqual(self.get_value(1, 5), '1')

    def test_cell_B7(self):
        self.assertEqual(self.get_value(1, 6), '7')

    def test_cell_B8(self):
        self.assertEqual(
            self.get_value(1, 7),
            r"'\u041c\u041e\u0421\u041a\u0412\u0410 \u041c\u043e\u0441\u043a\u0432\u0430'",
        )

class TestNameFormulas(TestCase):

    def setUp(self):
        book = xlrd.open_workbook(from_this_dir('formula_test_names.xls'))
        self.sheet = book.sheet_by_index(0)

    def get_value(self, col, row):
        return ascii(self.sheet.col_values(col)[row])

    def test_unaryop(self):
        self.assertEqual(self.get_value(1, 1), '-7.0')

    def test_attrsum(self):
        self.assertEqual(self.get_value(1, 2), '4.0')

    def test_func(self):
        self.assertEqual(self.get_value(1, 3), '6.0')

    def test_func_var_args(self):
        self.assertEqual(self.get_value(1, 4), '3.0')

    def test_if(self):
        self.assertEqual(self.get_value(1, 5), "'b'")

    def test_choose(self):
        self.assertEqual(self.get_value(1, 6), "'C'")
