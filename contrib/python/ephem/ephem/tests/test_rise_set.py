import ephem
import unittest

METHODS = (
    ephem.Observer.previous_rising,
    ephem.Observer.previous_setting,
    ephem.Observer.next_rising,
    ephem.Observer.next_setting,
)

tle_lines = (
    'ISS (ZARYA)',
    '1 25544U 98067A   21339.43187394  .00003128  00000+0  65063-4 0  9994',
    '2 25544  51.6429 215.9885 0004097 274.3592 259.8366 15.48933952315130',
)

class HourAngleTests(unittest.TestCase):
    # Yes, maybe an odd place to put a body attribute test; but HA was
    # added to support the new rising and setting logic, so here it is.

    o = ephem.Observer()
    o.date = '2021/12/5 13:56'

    def test_fixed_ha(self):  # obj_fixed() in circum.c
        b = ephem.star('Rigel')
        b.compute(self.o)
        self.assertNotEqual(b.ha, 0.0)

    def test_cir_pos_ha(self):  # cir_pos() in circum.c
        b = ephem.Mars()
        b.compute(self.o)
        self.assertNotEqual(b.ha, 0.0)

    def test_earth_satellite_ha(self):  # earthsat.c
        b = ephem.readtle(*tle_lines)
        b.compute(self.o)
        self.assertNotEqual(b.ha, 0.0)

    # Ignoring "plmoon.c" case for now, as PyEphem objects like
    # Callisto() seem to lack the new "ha" attribute anyway.

class RiseSetTests(unittest.TestCase):
    maxDiff = 10000

    def test_never_up(self):
        m = ephem.Moon()
        o = ephem.Observer()
        o.lon = '0'
        o.date = '2021/11/11'

        # First, two clear-cut cases.
        o.lat = '+40'
        self.assertEqual(str(o.next_rising(m)), '2021/11/11 13:22:31')

        o.lat = '+80'
        self.assertRaises(ephem.NeverUpError, o.next_rising, m)

        # Now, an edge case: although the Moon starts the day too far
        # south to rise, it moves north over the first few hours and
        # winds up rising.
        o.lat = '+70'
        t = o.next_rising(m)
        self.assertEqual(str(o.next_rising(m)), '2021/11/11 17:13:03')

    def test_always_up(self):
        m = ephem.Moon()
        o = ephem.Observer()
        o.lon = '0'
        o.date = '2022/9/7'

        o.lat = '-60'
        self.assertEqual(str(o.next_rising(m)), '2022/9/7 12:11:26')

        o.lat = '-70'
        self.assertRaises(ephem.AlwaysUpError, o.next_rising, m)

    def test_raises_error_for_NaN(self):
        m = ephem.Moon()
        o = ephem.Observer()
        o.date = float('nan')
        with self.assertRaises(ValueError):
            o.next_rising(m)

    def test_avoids_infinite_loop(self):
        observer = ephem.Observer()
        observer.lat = '69.043'
        observer.lon = '20.851'
        observer.elev = 1009
        observer.date = '2023/5/20 22:20'
        sun = ephem.Sun()
        t = observer.next_rising(sun)  # used to be an infinite loop

        # This is probably not an accurate value, as we broke out of the
        # loop without a solution, but let's at include it in the test
        # anyway, so that we don't change the result in the future
        # without at least knowing it.
        self.assertEqual(str(t), '2023/5/20 22:33:50')

    def test_sun(self):
        s = ephem.Sun()
        o = ephem.Observer()
        o.lat = '36.4072'
        o.lon = '-105.5734'
        o.date = '2021/11/24'
        expected = """\
0.0 0.0 False
2021/11/23 13:51:09 previous_rising
2021/11/23 23:46:11 previous_setting
2021/11/24 13:52:08 next_rising
2021/11/24 23:45:47 next_setting

0.0 0.0 True
2021/11/23 13:52:38 previous_rising
2021/11/23 23:44:41 previous_setting
2021/11/24 13:53:38 next_rising
2021/11/24 23:44:17 next_setting

0.0 1010.0 False
2021/11/23 13:47:44 previous_rising
2021/11/23 23:49:36 previous_setting
2021/11/24 13:48:43 next_rising
2021/11/24 23:49:12 next_setting

0.0 1010.0 True
2021/11/23 13:49:33 previous_rising
2021/11/23 23:47:46 previous_setting
2021/11/24 13:50:32 next_rising
2021/11/24 23:47:22 next_setting

-0.8333 0.0 False
2021/11/23 13:46:34 previous_rising
2021/11/23 23:50:46 previous_setting
2021/11/24 13:47:33 next_rising
2021/11/24 23:50:22 next_setting

-0.8333 0.0 True
2021/11/23 13:48:03 previous_rising
2021/11/23 23:49:17 previous_setting
2021/11/24 13:49:02 next_rising
2021/11/24 23:48:53 next_setting

-0.8333 1010.0 False
2021/11/23 13:41:44 previous_rising
2021/11/23 23:55:36 previous_setting
2021/11/24 13:42:42 next_rising
2021/11/24 23:55:13 next_setting

-0.8333 1010.0 True
2021/11/23 13:43:44 previous_rising
2021/11/23 23:53:35 previous_setting
2021/11/24 13:44:43 next_rising
2021/11/24 23:53:12 next_setting
"""
        expected = expected.splitlines()
        actual = self._generate_report(o, s)
        for n, (expected, actual) in enumerate(zip(expected, actual), 1):
            self.assertEqual(expected, actual, 'Line {}'.format(n))

    def test_moon(self):
        m = ephem.Moon()
        o = ephem.Observer()
        o.lat = '36.4072'
        o.lon = '-105.5734'
        o.date = '2021/11/24'
        expected = """\
0.0 0.0 False
2021/11/23 02:21:39 previous_rising
2021/11/23 17:34:47 previous_setting
2021/11/24 03:15:45 next_rising
2021/11/24 18:18:46 next_setting

0.0 0.0 True
2021/11/23 02:23:09 previous_rising
2021/11/23 17:33:17 previous_setting
2021/11/24 03:17:15 next_rising
2021/11/24 18:17:18 next_setting

0.0 1010.0 False
2021/11/23 02:17:52 previous_rising
2021/11/23 17:38:32 previous_setting
2021/11/24 03:12:00 next_rising
2021/11/24 18:22:25 next_setting

0.0 1010.0 True
2021/11/23 02:19:43 previous_rising
2021/11/23 17:36:41 previous_setting
2021/11/24 03:13:51 next_rising
2021/11/24 18:20:36 next_setting

-0.8333 0.0 False
2021/11/23 02:16:31 previous_rising
2021/11/23 17:39:52 previous_setting
2021/11/24 03:10:40 next_rising
2021/11/24 18:23:43 next_setting

-0.8333 0.0 True
2021/11/23 02:18:02 previous_rising
2021/11/23 17:38:21 previous_setting
2021/11/24 03:12:11 next_rising
2021/11/24 18:22:14 next_setting

-0.8333 1010.0 False
2021/11/23 02:11:04 previous_rising
2021/11/23 17:45:16 previous_setting
2021/11/24 03:05:16 next_rising
2021/11/24 18:28:58 next_setting

-0.8333 1010.0 True
2021/11/23 02:13:10 previous_rising
2021/11/23 17:43:11 previous_setting
2021/11/24 03:07:21 next_rising
2021/11/24 18:26:56 next_setting
"""
        expected = expected.splitlines()
        actual = self._generate_report(o, m)
        for n, (expected, actual) in enumerate(zip(expected, actual), 1):
            self.assertEqual(expected, actual, 'Line {}'.format(n))

    def _generate_report(self, o, body):
        for horizon in 0.0, '-0.8333':
            for pressure in 0.0, 1010.0:
                for use_center in False, True:
                    o.horizon = horizon
                    o.pressure = pressure
                    yield '{} {} {}'.format(horizon, pressure, use_center)
                    for method in METHODS:
                        d = method(o, body, use_center=use_center)
                        yield '{} {}'.format(d, method.__name__)
                    yield ''
