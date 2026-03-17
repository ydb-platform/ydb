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
from pymeeus.Uranus import Uranus
from pymeeus.Epoch import Epoch


# Uranus class

def test_uranus_geometric_heliocentric_position():
    """Tests the geometric_heliocentric_position() method of Uranus class"""

    epoch = Epoch(2018, 10, 27.0)
    lon, lat, r = Uranus.geometric_heliocentric_position(epoch)

    assert abs(round(lon.to_positive(), 4) - 30.5888) < TOL, \
        "ERROR: 1st geometric_heliocentric_position() test doesn't match"

    assert abs(round(lat, 4) - (-0.5315)) < TOL, \
        "ERROR: 2nd geometric_heliocentric_position() test doesn't match"

    assert abs(round(r, 5) - 19.86964) < TOL, \
        "ERROR: 3rd geometric_heliocentric_position() test doesn't match"


def test_uranus_orbital_elements_mean_equinox():
    """Tests the orbital_elements_mean_equinox() method of Uranus class"""

    epoch = Epoch(2065, 6, 24.0)
    l, a, e, i, ome, arg = Uranus.orbital_elements_mean_equinox(epoch)

    assert abs(round(l, 6) - 235.517526) < TOL, \
        "ERROR: 1st orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(a, 8) - 19.21844604) < TOL, \
        "ERROR: 2nd orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(e, 7) - 0.0463634) < TOL, \
        "ERROR: 3rd orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(i, 6) - 0.77372) < TOL, \
        "ERROR: 4th orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(ome, 5) - 74.34776) < TOL, \
        "ERROR: 5th orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(arg, 6) - 99.630865) < TOL, \
        "ERROR: 6th orbital_elements_mean_equinox() test doesn't match"


def test_uranus_orbital_elements_j2000():
    """Tests the orbital_elements_j2000() method of Uranus class"""

    epoch = Epoch(2065, 6, 24.0)
    l, a, e, i, ome, arg = Uranus.orbital_elements_j2000(epoch)

    assert abs(round(l, 6) - 234.602641) < TOL, \
        "ERROR: 1st orbital_elements_j2000() test doesn't match"

    assert abs(round(a, 8) - 19.21844604) < TOL, \
        "ERROR: 2nd orbital_elements_j2000() test doesn't match"

    assert abs(round(e, 7) - 0.0463634) < TOL, \
        "ERROR: 3rd orbital_elements_j2000() test doesn't match"

    assert abs(round(i, 6) - 0.772094) < TOL, \
        "ERROR: 4th orbital_elements_j2000() test doesn't match"

    assert abs(round(ome, 5) - 74.05468) < TOL, \
        "ERROR: 5th orbital_elements_j2000() test doesn't match"

    assert abs(round(arg, 6) - 99.009058) < TOL, \
        "ERROR: 6th orbital_elements_j2000() test doesn't match"


def test_uranus_geocentric_position():
    """Tests the geocentric_position() method of Uranus class"""

    epoch = Epoch(1992, 12, 20.0)
    ra, dec, elon = Uranus.geocentric_position(epoch)

    assert ra.ra_str(n_dec=1) == "19h 13' 48.7''", \
        "ERROR: 1st geocentric_position() test doesn't match"

    assert dec.dms_str(n_dec=1) == "-22d 46' 13.0''", \
        "ERROR: 2nd geocentric_position() test doesn't match"

    assert elon.dms_str(n_dec=1) == "18d 44' 18.7''", \
        "ERROR: 3rd geocentric_position() test doesn't match"


def test_uranus_conjunction():
    """Tests the conjunction() method of Uranus class"""

    epoch = Epoch(1993, 10, 1.0)
    conjunction = Uranus.conjunction(epoch)
    y, m, d = conjunction.get_date()

    assert abs(round(y, 0) - 1994) < TOL, \
        "ERROR: 1st conjunction() test doesn't match"

    assert abs(round(m, 0) - 1) < TOL, \
        "ERROR: 2nd conjunction() test doesn't match"

    assert abs(round(d, 4) - 12.7365) < TOL, \
        "ERROR: 3rd conjunction() test doesn't match"


def test_uranus_opposition():
    """Tests the opposition() method of Uranus class"""

    epoch = Epoch(1780, 12, 1.0)
    oppo = Uranus.opposition(epoch)
    y, m, d = oppo.get_date()

    assert abs(round(y, 0) - 1780) < TOL, \
        "ERROR: 1st opposition() test doesn't match"

    assert abs(round(m, 0) - 12) < TOL, \
        "ERROR: 2nd opposition() test doesn't match"

    assert abs(round(d, 4) - 17.5998) < TOL, \
        "ERROR: 3rd opposition() test doesn't match"


def test_uranus_perihelion_aphelion():
    """Tests the perihelion_aphelion() method of Uranus class"""

    epoch = Epoch(1960, 1, 1.0)
    e = Uranus.perihelion_aphelion(epoch)
    y, m, d, h, mi, s = e.get_full_date()

    assert abs(y - 1966) < TOL, \
        "ERROR: 1st perihelion_aphelion() test doesn't match"

    assert abs(m - 5) < TOL, \
        "ERROR: 2nd perihelion_aphelion() test doesn't match"

    assert abs(d - 18) < TOL, \
        "ERROR: 3rd perihelion_aphelion() test doesn't match"

    epoch = Epoch(2009, 1, 1.0)
    e = Uranus.perihelion_aphelion(epoch, perihelion=False)
    y, m, d, h, mi, s = e.get_full_date()

    assert abs(y - 2009) < TOL, \
        "ERROR: 4th perihelion_aphelion() test doesn't match"

    assert abs(m - 2) < TOL, \
        "ERROR: 5th perihelion_aphelion() test doesn't match"

    assert abs(d - 23) < TOL, \
        "ERROR: 6th perihelion_aphelion() test doesn't match"


def test_uranus_passage_nodes():
    """Tests the passage_nodes() method of Uranus class"""

    epoch = Epoch(2019, 1, 1)
    time, r = Uranus.passage_nodes(epoch)
    y, m, d = time.get_date()
    d = round(d, 1)
    r = round(r, 4)

    assert abs(y - 2028) < TOL, \
        "ERROR: 1st passage_nodes() test doesn't match"

    assert abs(m - 8) < TOL, \
        "ERROR: 2nd passage_nodes() test doesn't match"

    assert abs(d - 23.2) < TOL, \
        "ERROR: 3rd passage_nodes() test doesn't match"

    assert abs(r - 19.3201) < TOL, \
        "ERROR: 4th passage_nodes() test doesn't match"
