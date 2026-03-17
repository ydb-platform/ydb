#!/usr/bin/env python

"""Unit tests for M2Crypto.ASN1.

Copyright (c) 2005 Open Source Applications Foundation. All rights reserved."""

import datetime
import time

from M2Crypto import ASN1, m2, util
from tests import unittest


class ASN1TestCase(unittest.TestCase):

    def test_Integer(self):
        pass  # XXX Dunno how to test

    def test_BitSTring(self):
        pass  # XXX Dunno how to test

    def test_String(self):
        asn1ptr = m2.asn1_string_new()
        # FIXME this is probably wrong ... asn1_string_set should have
        # Python string as its parameter.
        text = b'hello there'
        # In RFC2253 format:
        # #040B68656C6C6F207468657265
        #      h e l l o   t h e r e
        m2.asn1_string_set(asn1ptr, text)
        a = ASN1.ASN1_String(asn1ptr, 1)
        self.assertEqual(a.as_text(), 'hello there', a.as_text())
        self.assertEqual(a.as_text(flags=m2.ASN1_STRFLGS_RFC2253),
                         '#040B68656C6C6F207468657265',
                         a.as_text(flags=m2.ASN1_STRFLGS_RFC2253))
        self.assertEqual(a.as_text(), str(a))

    def test_Object(self):
        pass  # XXX Dunno how to test

    @unittest.skipIf(util.is_32bit(), 'Skip on 32bit architectures.')
    def test_TIME(self):
        asn1 = ASN1.ASN1_TIME()
        self.assertEqual(str(asn1), 'Bad time value')

        format = '%b %d %H:%M:%S %Y GMT'
        utcformat = '%y%m%d%H%M%SZ'

        s = '990807053011Z'
        asn1.set_string(s)
        self.assertEqual(str(asn1), 'Aug  7 05:30:11 1999 GMT')
        t1 = time.strptime(str(asn1), format)
        t2 = time.strptime(s, utcformat)
        self.assertEqual(t1, t2)

        asn1.set_time(500)
        self.assertEqual(str(asn1), 'Jan  1 00:08:20 1970 GMT')
        t1 = time.strftime(format, time.strptime(str(asn1), format))
        t2 = time.strftime(format, time.gmtime(500))
        self.assertEqual(t1, t2)

        t = int(time.time()) + time.timezone
        asn1.set_time(t)
        t1 = time.strftime(format, time.strptime(str(asn1), format))
        t2 = time.strftime(format, time.gmtime(t))
        self.assertEqual(t1, t2)

    @unittest.skipIf(util.is_32bit(), 'Skip on 32bit architectures.')
    def test_UTCTIME(self):
        asn1 = ASN1.ASN1_UTCTIME()
        self.assertEqual(str(asn1), 'Bad time value')

        format = '%b %d %H:%M:%S %Y GMT'
        utcformat = '%y%m%d%H%M%SZ'

        s = '990807053011Z'
        asn1.set_string(s)
        # self.assertEqual(str(asn1), 'Aug  7 05:30:11 1999 GMT')
        t1 = time.strptime(str(asn1), format)
        t2 = time.strptime(s, utcformat)
        self.assertEqual(t1, t2)

        asn1.set_time(500)
        # self.assertEqual(str(asn1), 'Jan  1 00:08:20 1970 GMT')
        t1 = time.strftime(format, time.strptime(str(asn1), format))
        t2 = time.strftime(format, time.gmtime(500))
        self.assertEqual(t1, t2)

        t = int(time.time()) + time.timezone
        asn1.set_time(t)
        t1 = time.strftime(format, time.strptime(str(asn1), format))
        t2 = time.strftime(format, time.gmtime(t))
        self.assertEqual(t1, t2)

    @unittest.skipIf(util.is_32bit(), 'Skip on 32bit architectures.')
    def test_TIME_datetime(self):
        asn1 = ASN1.ASN1_TIME()
        # Test get_datetime and set_datetime
        t = time.time()
        dt = datetime.datetime.fromtimestamp(int(t))
        udt = dt.replace(tzinfo=ASN1.LocalTimezone()).astimezone(ASN1.UTC)
        asn1.set_time(int(t))
        t1 = str(asn1)
        asn1.set_datetime(dt)
        t2 = str(asn1)
        self.assertEqual(t1, t2)
        self.assertEqual(str(udt), str(asn1.get_datetime()))

        dt = dt.replace(tzinfo=ASN1.LocalTimezone())
        asn1.set_datetime(dt)
        t2 = str(asn1)
        self.assertEqual(t1, t2)
        self.assertEqual(str(udt), str(asn1.get_datetime()))

        dt = dt.astimezone(ASN1.UTC)
        asn1.set_datetime(dt)
        t2 = str(asn1)
        self.assertEqual(t1, t2)
        self.assertEqual(str(udt), str(asn1.get_datetime()))

    @unittest.skipIf(util.is_32bit(), 'Skip on 32bit architectures.')
    def test_UTCTIME_datetime(self):
        asn1 = ASN1.ASN1_UTCTIME()
        # Test get_datetime and set_datetime
        t = time.time()
        dt = datetime.datetime.fromtimestamp(int(t))
        udt = dt.replace(tzinfo=ASN1.LocalTimezone()).astimezone(ASN1.UTC)
        asn1.set_time(int(t))
        t1 = str(asn1)
        asn1.set_datetime(dt)
        t2 = str(asn1)
        self.assertEqual(t1, t2)
        self.assertEqual(str(udt), str(asn1.get_datetime()))

        dt = dt.replace(tzinfo=ASN1.LocalTimezone())
        asn1.set_datetime(dt)
        t2 = str(asn1)
        self.assertEqual(t1, t2)
        self.assertEqual(str(udt), str(asn1.get_datetime()))

        dt = dt.astimezone(ASN1.UTC)
        asn1.set_datetime(dt)
        t2 = str(asn1)
        self.assertEqual(t1, t2)
        self.assertEqual(str(udt), str(asn1.get_datetime()))


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(ASN1TestCase)


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
