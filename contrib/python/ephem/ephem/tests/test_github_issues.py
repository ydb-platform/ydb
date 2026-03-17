try:
    from unittest2 import TestCase
except:
    from unittest import TestCase
import datetime
import ephem
import math
import os

class GitHubIssues(TestCase):

    def test_github_14(self):
        iss = ephem.readtle(
            'ISS (ZARYA)',
            '1 25544U 98067A   03097.78853147  .00021906 '
            ' 00000-0  28403-3 0  8652',
            '2 25544  51.6361  13.7980 0004256  35.6671  '
            '59.2566 15.58778559250029',
            )
        gatech = ephem.Observer()
        gatech.lon, gatech.lat = '-84.39733', '33.775867'
        gatech.date = '2003/3/23'
        iss.compute(gatech)
        self.assertEqual(str(iss.a_ra), '8:50:10.99')
        self.assertEqual(str(iss.g_ra), '6:54:40.64')
        self.assertEqual(str(iss.ra), '8:50:16.76')

    def test_github_24(self):
        boston = ephem.Observer()
        boston.lat = '42:21:24'
        boston.lon = '-71:03:25'
        boston.elevation = 6.4
        boston.pressure = 0.0
        boston.temp = 10.0
        boston.date = '2014/5/29 00:00:00'
        mars = ephem.Mars(boston)
        pa = mars.parallactic_angle()
        # The exact value that XEphem computes for this circumstance:
        self.assertEqual('%.2f' % (pa / ephem.degree), '-13.62')

    def test_github_25(self):
        tle=[
      "OBJECT L",
      "1 39276U 13055L   13275.56815576  .28471697  00000-0  53764+0 0    92",
      "2 39276 080.9963 312.3419 0479021 141.2713 225.6381 14.71846910   412",
        ]
        fenton = ephem.Observer()
        fenton.long = '-106.673887'
        fenton.lat = '35.881226'
        fenton.elevation = 2656
        fenton.horizon = math.radians(10)
        fenton.compute_pressure()
        sat = ephem.readtle(*tle)
        fenton.date = datetime.datetime(2013,10,6,0,0,0,0)
        sat.compute(fenton)

        # The issue was that this calculation froze, so we are happy
        # either with an exception or the right answer.
        try:
            result = sat.neverup
        except RuntimeError:
            pass
        else:
            self.assertEqual(result, False)

    def test_github_28(self):
        tle = [
       '1 39519U 98067DM  14145.55394724  .00421800  00000-0  16480-2 0  2721',
       '2 39519 051.6371 202.2821 0012663 000.6767 359.4251 15.85259233 15944',
        ]
        date = "2014-01-04 00:00:00"

        sat = ephem.readtle("Satellite", tle[0], tle[1])
        sat.compute(date)

        self.assertRaises(RuntimeError, lambda: sat.sublat)

    def test_github_31(self):
        position = (4.116325133165859, 0.14032240860186646)
        self.assertEqual(ephem.separation(position, position), 0.0)

    def test_github_41(self):
        g = ephem.Galactic('45:00', '55:00')
        self.assertEqual(str(g.long), '45:00:00.0')
        g.long = ephem.degrees('-35')
        self.assertEqual(str(g.long), '-35:00:00.0')

    # On Windows, the following test reports:
    # AssertionError: 0.00017109312466345727 != 0.0296238418669 within 10 places

    def test_github_58(self):
        t = ephem.readdb("P10frjh,e,7.43269,35.02591,162.97669,"
                         "0.6897594,1.72051182,0.5475395,"
                         "195.80709,10/10/2014,2000,H26.4,0.15")
        t.compute('2014/10/27')
        self.assertAlmostEqual(t.earth_distance, 0.0296238474547863, 10)
        self.assertAlmostEqual(t.mag, 20.23, 2)
        self.assertAlmostEqual(t.phase, 91.1195, 2)
        self.assertAlmostEqual(t.radius, 0, 2)
        self.assertAlmostEqual(t.size, 0.0010345211485400796, 12)

    def test_github_64(self):
        sun = ephem.Sun()
        pole = ephem.Observer()
        pole.lon = '0.0'
        pole.lat = '90.0'
        pole.elevation = 0.0

        pole.date = '2009/4/30'
        with self.assertRaises(ephem.AlwaysUpError):
            pole.previous_rising(sun)
        self.assertEqual(str(pole.date), '2009/4/30 00:00:00')

    def test_github_193(self):
        ic = ephem.readdb('IC1101,f,15:10:56.10,+05:44:41.2,0,2000')
        ic.compute()
        c = ephem.constellation(ic)
        self.assertEqual(c, ('Vir', 'Virgo'))

    def test_github_196(self):
        line = ('2010 LG61,e,123.8859,317.3744,352.1688,7.366687,'
                '0.0492942,0.81371070,163.4277,04/27.0/2019,2000,H,0.15')
        elems = ephem.readdb(line)
        self.assertEqual(str(elems._H), 'nan')
