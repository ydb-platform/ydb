import os
import unittest
import filecmp

import xlwt

from .utils import in_tst_dir, in_tst_output_dir

def create_example_xls(filename):
    w = xlwt.Workbook()
    ws1 = w.add_sheet(u'\N{GREEK SMALL LETTER ALPHA}\N{GREEK SMALL LETTER BETA}\N{GREEK SMALL LETTER GAMMA}')

    ws1.write(0, 0, u'\N{GREEK SMALL LETTER ALPHA}\N{GREEK SMALL LETTER BETA}\N{GREEK SMALL LETTER GAMMA}')
    ws1.write(1, 1, u'\N{GREEK SMALL LETTER DELTA}x = 1 + \N{GREEK SMALL LETTER DELTA}')

    ws1.write(2,0, u'A\u2262\u0391.')     # RFC2152 example
    ws1.write(3,0, u'Hi Mom -\u263a-!')   # RFC2152 example
    ws1.write(4,0, u'\u65E5\u672C\u8A9E') # RFC2152 example
    ws1.write(5,0, u'Item 3 is \u00a31.') # RFC2152 example
    ws1.write(8,0, u'\N{INTEGRAL}')       # RFC2152 example

    w.add_sheet(u'A\u2262\u0391.')     # RFC2152 example
    w.add_sheet(u'Hi Mom -\u263a-!')   # RFC2152 example
    one_more_ws = w.add_sheet(u'\u65E5\u672C\u8A9E') # RFC2152 example
    w.add_sheet(u'Item 3 is \u00a31.') # RFC2152 example

    one_more_ws.write(0, 0, u'\u2665\u2665')

    w.add_sheet(u'\N{GREEK SMALL LETTER ETA WITH TONOS}')
    w.save(filename)

EXAMPLE_XLS = 'unicode1.xls'

class TestUnicode1(unittest.TestCase):

    def test_example_xls(self):
        create_example_xls(in_tst_output_dir(EXAMPLE_XLS))
        self.assertTrue(filecmp.cmp(in_tst_dir(EXAMPLE_XLS),
                                    in_tst_output_dir(EXAMPLE_XLS),
                                    shallow=False))
