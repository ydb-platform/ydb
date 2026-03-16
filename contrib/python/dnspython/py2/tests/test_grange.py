# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2007, 2009-2011 Nominum, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND NOMINUM DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import sys
sys.path.insert(0, '../')

import unittest

import dns
import dns.exception
import dns.grange


class GRangeTestCase(unittest.TestCase):

    def testFromText1(self):
        start, stop, step = dns.grange.from_text('1-1')
        self.assertEqual(start, 1)
        self.assertEqual(stop, 1)
        self.assertEqual(step, 1)

    def testFromText2(self):
        start, stop, step = dns.grange.from_text('1-4')
        self.assertEqual(start, 1)
        self.assertEqual(stop, 4)
        self.assertEqual(step, 1)

    def testFromText3(self):
        start, stop, step = dns.grange.from_text('4-255')
        self.assertEqual(start, 4)
        self.assertEqual(stop, 255)
        self.assertEqual(step, 1)

    def testFromText4(self):
        start, stop, step = dns.grange.from_text('1-1/1')
        self.assertEqual(start, 1)
        self.assertEqual(stop, 1)
        self.assertEqual(step, 1)

    def testFromText5(self):
        start, stop, step = dns.grange.from_text('1-4/2')
        self.assertEqual(start, 1)
        self.assertEqual(stop, 4)
        self.assertEqual(step, 2)

    def testFromText6(self):
        start, stop, step = dns.grange.from_text('4-255/77')
        self.assertEqual(start, 4)
        self.assertEqual(stop, 255)
        self.assertEqual(step, 77)

    def testFailFromText1(self):
        def bad():
            start = 2
            stop = 1
            step = 1
            dns.grange.from_text('%d-%d/%d' % (start, stop, step))
        self.assertRaises(AssertionError, bad)

    def testFailFromText2(self):
        def bad():
            start = '-1'
            stop = 3
            step = 1
            dns.grange.from_text('%s-%d/%d' % (start, stop, step))
        self.assertRaises(dns.exception.SyntaxError, bad)

    def testFailFromText3(self):
        def bad():
            start = 1
            stop = 4
            step = '-2'
            dns.grange.from_text('%d-%d/%s' % (start, stop, step))
        self.assertRaises(dns.exception.SyntaxError, bad)

if __name__ == '__main__':
    unittest.main()
