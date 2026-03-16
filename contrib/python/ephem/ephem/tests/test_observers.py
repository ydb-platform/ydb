#!/usr/bin/env python

from copy import copy
from ephem import Observer, city
from unittest import TestCase

class ObserverTests(TestCase):
    def test_longitude_constructor_does_not_hang(self):  # GitHub #207
        observer = Observer()
        observer.lon = '-113.78401934344532'

    def test_lon_can_also_be_called_long(self):
        o = Observer()
        o.lon = 3.0
        self.assertEqual(o.long, 3.0)
        o.long = 6.0
        self.assertEqual(o.lon, 6.0)

    def test_pressure_at_sea_level(self):
        o = Observer()
        o.elevation = 0
        o.compute_pressure()
        self.assertEqual(o.pressure, 1013.25)

    def test_pressure_at_11km(self):
        o = Observer()
        o.elevation = 11e3
        o.compute_pressure()
        assert 226.31 < o.pressure < 226.33

    def test_observer_copy(self):
        c = city('Boston')
        c.date = '2015/5/30 10:09'
        d = c.copy()
        self._check_observer(c, d)
        d = copy(c)
        self._check_observer(c, d)

    def _check_observer(self, c, d):
        assert c is not d
        self.assertEqual(c.date, d.date)
        self.assertEqual(c.lat, d.lat)
        self.assertEqual(c.lon, d.lon)
        self.assertEqual(c.elev, d.elev)
        self.assertEqual(c.horizon, d.horizon)
        self.assertEqual(c.epoch, d.epoch)
        self.assertEqual(c.temp, d.temp)
        self.assertEqual(c.pressure, d.pressure)
