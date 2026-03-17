# -*- coding: utf-8 -*-


# PyMeeus: Python module implementing astronomical algorithms.
# Copyright (C) 2018  Dagoberto Salazar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


from pymeeus.base import TOL
from pymeeus.Jupiter import Jupiter
from pymeeus.Epoch import Epoch


# Jupiter class

def test_jupiter_geometric_heliocentric_position():
    """Tests the geometric_heliocentric_position() method of Jupiter class"""

    epoch = Epoch(2018, 10, 27.0)
    lon, lat, r = Jupiter.geometric_heliocentric_position(epoch)

    assert abs(round(lon.to_positive(), 4) - 241.5873) < TOL, \
        "ERROR: 1st geometric_heliocentric_position() test doesn't match"

    assert abs(round(lat, 4) - 0.8216) < TOL, \
        "ERROR: 2nd geometric_heliocentric_position() test doesn't match"

    assert abs(round(r, 5) - 5.36848) < TOL, \
        "ERROR: 3rd geometric_heliocentric_position() test doesn't match"


def test_jupiter_orbital_elements_mean_equinox():
    """Tests the orbital_elements_mean_equinox() method of Jupiter class"""

    epoch = Epoch(2065, 6, 24.0)
    l, a, e, i, ome, arg = Jupiter.orbital_elements_mean_equinox(epoch)

    assert abs(round(l, 6) - 222.433723) < TOL, \
        "ERROR: 1st orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(a, 8) - 5.20260333) < TOL, \
        "ERROR: 2nd orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(e, 7) - 0.0486046) < TOL, \
        "ERROR: 3rd orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(i, 6) - 1.29967) < TOL, \
        "ERROR: 4th orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(ome, 5) - 101.13309) < TOL, \
        "ERROR: 5th orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(arg, 6) - (-85.745532)) < TOL, \
        "ERROR: 6th orbital_elements_mean_equinox() test doesn't match"


def test_jupiter_orbital_elements_j2000():
    """Tests the orbital_elements_j2000() method of Jupiter class"""

    epoch = Epoch(2065, 6, 24.0)
    l, a, e, i, ome, arg = Jupiter.orbital_elements_j2000(epoch)

    assert abs(round(l, 6) - 221.518802) < TOL, \
        "ERROR: 1st orbital_elements_j2000() test doesn't match"

    assert abs(round(a, 8) - 5.20260333) < TOL, \
        "ERROR: 2nd orbital_elements_j2000() test doesn't match"

    assert abs(round(e, 7) - 0.0486046) < TOL, \
        "ERROR: 3rd orbital_elements_j2000() test doesn't match"

    assert abs(round(i, 6) - 1.30198) < TOL, \
        "ERROR: 4th orbital_elements_j2000() test doesn't match"

    assert abs(round(ome, 5) - 100.58051) < TOL, \
        "ERROR: 5th orbital_elements_j2000() test doesn't match"

    assert abs(round(arg, 6) - (-86.107875)) < TOL, \
        "ERROR: 6th orbital_elements_j2000() test doesn't match"


def test_jupiter_geocentric_position():
    """Tests the geocentric_position() method of Jupiter class"""

    epoch = Epoch(1992, 12, 20.0)
    ra, dec, elon = Jupiter.geocentric_position(epoch)

    assert ra.ra_str(n_dec=1) == "12h 47' 9.6''", \
        "ERROR: 1st geocentric_position() test doesn't match"

    assert dec.dms_str(n_dec=1) == "-3d 41' 55.3''", \
        "ERROR: 2nd geocentric_position() test doesn't match"

    assert elon.dms_str(n_dec=1) == "76d 2' 26.0''", \
        "ERROR: 3rd geocentric_position() test doesn't match"


def test_jupiter_conjunction():
    """Tests the conjunction() method of Jupiter class"""

    epoch = Epoch(1993, 10, 1.0)
    conjunction = Jupiter.conjunction(epoch)
    y, m, d = conjunction.get_date()

    assert abs(round(y, 0) - 1993) < TOL, \
        "ERROR: 1st conjunction() test doesn't match"

    assert abs(round(m, 0) - 10) < TOL, \
        "ERROR: 2nd conjunction() test doesn't match"

    assert abs(round(d, 4) - 18.3341) < TOL, \
        "ERROR: 3rd conjunction() test doesn't match"


def test_jupiter_opposition():
    """Tests the opposition() method of Jupiter class"""

    epoch = Epoch(-6, 9, 1.0)
    oppo = Jupiter.opposition(epoch)
    y, m, d = oppo.get_date()

    assert abs(round(y, 0) - (-6)) < TOL, \
        "ERROR: 1st opposition() test doesn't match"

    assert abs(round(m, 0) - 9) < TOL, \
        "ERROR: 2nd opposition() test doesn't match"

    assert abs(round(d, 4) - 15.2865) < TOL, \
        "ERROR: 3rd opposition() test doesn't match"


def test_jupiter_station_longitude_1():
    """Tests the station_longitude_1() method of Jupiter class"""

    epoch = Epoch(2018, 11, 1.0)
    sta1 = Jupiter.station_longitude_1(epoch)
    y, m, d = sta1.get_date()

    assert abs(round(y, 0) - 2018) < TOL, \
        "ERROR: 1st station_longitude_1() test doesn't match"

    assert abs(round(m, 0) - 3) < TOL, \
        "ERROR: 2nd station_longitude_1() test doesn't match"

    assert abs(round(d, 4) - 9.1288) < TOL, \
        "ERROR: 3rd station_longitude_1() test doesn't match"


def test_jupiter_station_longitude_2():
    """Tests the station_longitude_2() method of Jupiter class"""

    epoch = Epoch(2018, 11, 1.0)
    sta2 = Jupiter.station_longitude_2(epoch)
    y, m, d = sta2.get_date()

    assert abs(round(y, 0) - 2018) < TOL, \
        "ERROR: 1st station_longitude_2() test doesn't match"

    assert abs(round(m, 0) - 7) < TOL, \
        "ERROR: 2nd station_longitude_2() test doesn't match"

    assert abs(round(d, 4) - 10.6679) < TOL, \
        "ERROR: 3rd station_longitude_2() test doesn't match"


def test_jupiter_perihelion_aphelion():
    """Tests the perihelion_aphelion() method of Jupiter class"""

    epoch = Epoch(2019, 2, 23.0)
    e = Jupiter.perihelion_aphelion(epoch)
    y, m, d, h, mi, s = e.get_full_date()

    assert abs(y - 2023) < TOL, \
        "ERROR: 1st perihelion_aphelion() test doesn't match"

    assert abs(m - 1) < TOL, \
        "ERROR: 2nd perihelion_aphelion() test doesn't match"

    assert abs(d - 20) < TOL, \
        "ERROR: 3rd perihelion_aphelion() test doesn't match"

    assert abs(h - 11) < TOL, \
        "ERROR: 4th perihelion_aphelion() test doesn't match"

    epoch = Epoch(1981, 6, 1.0)
    e = Jupiter.perihelion_aphelion(epoch, perihelion=False)
    y, m, d, h, mi, s = e.get_full_date()

    assert abs(y - 1981) < TOL, \
        "ERROR: 5th perihelion_aphelion() test doesn't match"

    assert abs(m - 7) < TOL, \
        "ERROR: 6th perihelion_aphelion() test doesn't match"

    assert abs(d - 28) < TOL, \
        "ERROR: 7th perihelion_aphelion() test doesn't match"

    assert abs(h - 6) < TOL, \
        "ERROR: 8th perihelion_aphelion() test doesn't match"


def test_jupiter_passage_nodes():
    """Tests the passage_nodes() method of Jupiter class"""

    epoch = Epoch(2019, 1, 1)
    time, r = Jupiter.passage_nodes(epoch)
    y, m, d = time.get_date()
    d = round(d, 1)
    r = round(r, 4)

    assert abs(y - 2025) < TOL, \
        "ERROR: 1st passage_nodes() test doesn't match"

    assert abs(m - 9) < TOL, \
        "ERROR: 2nd passage_nodes() test doesn't match"

    assert abs(d - 15.6) < TOL, \
        "ERROR: 3rd passage_nodes() test doesn't match"

    assert abs(r - 5.1729) < TOL, \
        "ERROR: 4th passage_nodes() test doesn't match"
