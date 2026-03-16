# Copyright (C) 2014 Red Hat, Inc.
# Author: Petr Spacek <pspacek@redhat.com>
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND RED HAT DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import unittest

import dns.rrset
import dns.rdtypes.ANY.LOC

class RdtypeAnyLocTestCase(unittest.TestCase):

    def testEqual1(self):
        '''Test default values for size, horizontal and vertical precision.'''
        r1 = dns.rrset.from_text('foo', 300, 'IN', 'LOC',
                                 '49 11 42.400 N 16 36 29.600 E 227.64m')
        r2 = dns.rrset.from_text('FOO', 600, 'in', 'loc',
                                 '49 11 42.400 N 16 36 29.600 E 227.64m '
                                 '1.00m 10000.00m 10.00m')
        self.failUnless(r1 == r2, '"{}" != "{}"'.format(r1, r2))

    def testEqual2(self):
        '''Test default values for size, horizontal and vertical precision.'''
        r1 = dns.rdtypes.ANY.LOC.LOC(1, 29, (49, 11, 42, 400, 1),
                                     (16, 36, 29, 600, 1),
                                     22764.0) # centimeters
        r2 = dns.rdtypes.ANY.LOC.LOC(1, 29, (49, 11, 42, 400, 1),
                                     (16, 36, 29, 600, 1),
                                     22764.0, # centimeters
                                     100.0, 1000000.00, 1000.0)  # centimeters
        self.failUnless(r1 == r2, '"{}" != "{}"'.format(r1, r2))

    def testEqual3(self):
        '''Test size, horizontal and vertical precision parsers: 100 cm == 1 m.

        Parsers in from_text() and __init__() have to produce equal results.'''
        r1 = dns.rdtypes.ANY.LOC.LOC(1, 29, (49, 11, 42, 400, 1),
                                     (16, 36, 29, 600, 1), 22764.0,
                                     200.0, 1000.00, 200.0)      # centimeters
        r2 = dns.rrset.from_text('FOO', 600, 'in', 'loc',
                                 '49 11 42.400 N 16 36 29.600 E 227.64m '
                                 '2.00m 10.00m 2.00m')[0]
        self.failUnless(r1 == r2, '"{}" != "{}"'.format(r1, r2))

    def testEqual4(self):
        '''Test size, horizontal and vertical precision parsers without unit.

        Parsers in from_text() and __init__() have produce equal result
        for values with and without trailing "m".'''
        r1 = dns.rdtypes.ANY.LOC.LOC(1, 29, (49, 11, 42, 400, 1),
                                     (16, 36, 29, 600, 1), 22764.0,
                                     200.0, 1000.00, 200.0)      # centimeters
        r2 = dns.rrset.from_text('FOO', 600, 'in', 'loc',
                                 '49 11 42.400 N 16 36 29.600 E 227.64 '
                                 '2 10 2')[0] # meters without explicit unit
        self.failUnless(r1 == r2, '"{}" != "{}"'.format(r1, r2))

if __name__ == '__main__':
    unittest.main()
