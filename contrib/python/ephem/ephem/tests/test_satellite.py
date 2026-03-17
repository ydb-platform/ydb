# -*- coding: utf-8 -*-

import ephem, unittest

tle_lines = (
    'ISS (ZARYA)             ',
    '1 25544U 98067A   09119.77864163  .00009789  00000-0  76089-4 0  7650',
    '2 25544  51.6397 195.1243 0008906 304.8273 151.9344 15.72498628598335',
    )

if not hasattr(unittest.TestCase, 'assertRaisesRegex'):
    setattr(unittest.TestCase, 'assertRaisesRegex',
            unittest.TestCase.assertRaisesRegexp)

class SatelliteTests(unittest.TestCase):
    def setUp(self):
        self.iss = ephem.readtle(*tle_lines)
        self.atlanta = ephem.city('Atlanta')

    def test_TLE_checksum(self):
        lines = list(tle_lines)
        lines[1] = lines[1][:-1] + '1'
        expected = 'incorrect TLE checksum at end of line'
        self.assertRaisesRegex(ValueError, expected, ephem.readtle, *lines)

    def test_normal_methods(self):
        for which in ['previous', 'next']:
            for event in ['transit', 'antitransit', 'rising', 'setting']:
                method_name = which + '_' + event
                method = getattr(self.atlanta, method_name)
                self.assertRaises(TypeError, method, self.iss)

    def test_attribute_values_and_roundtrips(self):
        def check():
            self.assertEqual(self.iss.epoch, 39931.27864163)
            self.assertEqual(self.iss.n, 15.72498628)
            self.assertEqual(self.iss.inc, 51.63970184326172)
            self.assertEqual(self.iss.raan, 195.12429809570312)
            self.assertEqual(self.iss.e, 0.0008905999711714685)
            self.assertEqual(self.iss.ap, 304.8273010253906)
            self.assertEqual(self.iss.M, 151.9344024658203)
            self.assertEqual(self.iss.decay, 9.788999886950478e-05)
            self.assertEqual(self.iss.drag, 7.60890034143813e-05)
            self.assertEqual(self.iss.orbit, 59833)

        check()

        self.iss.epoch = self.iss.epoch
        self.iss.n = self.iss.n
        self.iss.inc = self.iss.inc
        self.iss.raan = self.iss.raan
        self.iss.e = self.iss.e
        self.iss.ap = self.iss.ap
        self.iss.M = self.iss.M
        self.iss.decay = self.iss.decay
        self.iss.drag = self.iss.drag
        self.iss.orbit = self.iss.orbit

        check()

    def test_next_pass(self):
        iss = self.iss
        self.atlanta.date = '2009/4/30'
        rt, raz, tt, talt, st, saz = self.atlanta.next_pass(iss)

        # Calsky says (using EST, and probably a different horizon):
        #  Rise(invis.)  1h02m17s  --.-mag  az:192.0° SSW
        #  Culmination   1h06m39s  --.-mag  az:128.2° SE   h:16.0°
        #  Set (invis.)  1h11m04s  --.-m  az: 64.9° ENE

        self.assertAlmostEqual(ephem.Date('2009/4/30 5:02:17'), rt, 3)
        self.assertAlmostEqual(ephem.Date('2009/4/30 5:06:39'), tt, 3)
        self.assertAlmostEqual(ephem.Date('2009/4/30 5:11:04'), st, 3)

        self.assertAlmostEqual(ephem.degrees('192.0'), raz, 1)
        self.assertAlmostEqual(ephem.degrees('16.0'), talt, 1)
        self.assertAlmostEqual(ephem.degrees('64.9'), saz, 1)

    def test_next_pass_consecutive(self):
        # Issue #63
        iss = self.iss
        # At this time, the ISS is already above the horizon
        self.atlanta.date = '2009/4/29 15:51:00'
        rt, raz, tt, talt, st, saz = self.atlanta.next_pass(iss)

        self.assertAlmostEqual(ephem.Date('2009/4/30 5:02:17'), rt, 3)
        self.assertAlmostEqual(ephem.Date('2009/4/30 5:06:39'), tt, 3)
        self.assertAlmostEqual(ephem.Date('2009/4/30 5:11:04'), st, 3)

        self.assertAlmostEqual(ephem.degrees('192.0'), raz, 1)
        self.assertAlmostEqual(ephem.degrees('16.0'), talt, 1)
        self.assertAlmostEqual(ephem.degrees('64.9'), saz, 1)

    def test_next_pass_notsinglepass(self):
        # Issue #63
        iss = self.iss
        # At this time, the ISS is already above the horizon
        self.atlanta.date = '2009/4/29 15:51:00'
        rt, raz, tt, talt, st, saz = self.atlanta.next_pass(iss, singlepass=False)

        self.assertAlmostEqual(ephem.Date('2009/4/30 5:02:17'), rt, 3)
        self.assertAlmostEqual(ephem.Date('2009/4/29 15:51:35'), tt, 3)
        self.assertAlmostEqual(ephem.Date('2009/4/29 15:54:01'), st, 3)

        self.assertAlmostEqual(ephem.degrees('192.0'), raz, 1)
        self.assertAlmostEqual(ephem.degrees('2.1'), talt, 1)
        self.assertAlmostEqual(ephem.degrees('209.0'), saz, 1)

    def test_more_than_one_year_before_TLE(self):
        self.assertRaises(ValueError, self.iss.compute, '2008/4/28')

    def test_more_than_one_year_after_TLE(self):
        self.assertRaises(ValueError, self.iss.compute, '2010/4/30 20:00')
