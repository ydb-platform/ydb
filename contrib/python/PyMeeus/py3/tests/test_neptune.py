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
from pymeeus.Neptune import Neptune
from pymeeus.Epoch import Epoch


# Neptune class

def test_neptune_geometric_heliocentric_position():
    """Tests the geometric_heliocentric_position() method of Neptune class"""

    epoch = Epoch(2018, 10, 27.0)
    lon, lat, r = Neptune.geometric_heliocentric_position(epoch)

    assert abs(round(lon.to_positive(), 4) - 345.3776) < TOL, \
        "ERROR: 1st geometric_heliocentric_position() test doesn't match"

    assert abs(round(lat, 4) - (-0.9735)) < TOL, \
        "ERROR: 2nd geometric_heliocentric_position() test doesn't match"

    assert abs(round(r, 5) - 29.93966) < TOL, \
        "ERROR: 3rd geometric_heliocentric_position() test doesn't match"


def test_neptune_orbital_elements_mean_equinox():
    """Tests the orbital_elements_mean_equinox() method of Neptune class"""

    epoch = Epoch(2065, 6, 24.0)
    l, a, e, i, ome, arg = Neptune.orbital_elements_mean_equinox(epoch)

    assert abs(round(l, 6) - 88.321947) < TOL, \
        "ERROR: 1st orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(a, 8) - 30.11038676) < TOL, \
        "ERROR: 2nd orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(e, 7) - 0.0094597) < TOL, \
        "ERROR: 3rd orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(i, 6) - 1.763855) < TOL, \
        "ERROR: 4th orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(ome, 5) - 132.46986) < TOL, \
        "ERROR: 5th orbital_elements_mean_equinox() test doesn't match"

    assert abs(round(arg, 6) - (-83.415521)) < TOL, \
        "ERROR: 6th orbital_elements_mean_equinox() test doesn't match"


def test_neptune_orbital_elements_j2000():
    """Tests the orbital_elements_j2000() method of Neptune class"""

    epoch = Epoch(2065, 6, 24.0)
    l, a, e, i, ome, arg = Neptune.orbital_elements_j2000(epoch)

    assert abs(round(l, 6) - 87.407029) < TOL, \
        "ERROR: 1st orbital_elements_j2000() test doesn't match"

    assert abs(round(a, 8) - 30.11038676) < TOL, \
        "ERROR: 2nd orbital_elements_j2000() test doesn't match"

    assert abs(round(e, 7) - 0.0094597) < TOL, \
        "ERROR: 3rd orbital_elements_j2000() test doesn't match"

    assert abs(round(i, 6) - 1.770101) < TOL, \
        "ERROR: 4th orbital_elements_j2000() test doesn't match"

    assert abs(round(ome, 5) - 131.74402) < TOL, \
        "ERROR: 5th orbital_elements_j2000() test doesn't match"

    assert abs(round(arg, 6) - (-83.6046)) < TOL, \
        "ERROR: 6th orbital_elements_j2000() test doesn't match"


def test_neptune_geocentric_position():
    """Tests the geocentric_position() method of Neptune class"""

    epoch = Epoch(1992, 12, 20.0)
    ra, dec, elon = Neptune.geocentric_position(epoch)

    assert ra.ra_str(n_dec=1) == "19h 17' 14.5''", \
        "ERROR: 1st geocentric_position() test doesn't match"

    assert dec.dms_str(n_dec=1) == "-21d 34' 15.1''", \
        "ERROR: 2nd geocentric_position() test doesn't match"

    assert elon.dms_str(n_dec=1) == "19d 44' 59.6''", \
        "ERROR: 3rd geocentric_position() test doesn't match"


def test_neptune_conjunction():
    """Tests the conjunction() method of Neptune class"""

    epoch = Epoch(1993, 10, 1.0)
    conjunction = Neptune.conjunction(epoch)
    y, m, d = conjunction.get_date()

    assert abs(round(y, 0) - 1994) < TOL, \
        "ERROR: 1st conjunction() test doesn't match"

    assert abs(round(m, 0) - 1) < TOL, \
        "ERROR: 2nd conjunction() test doesn't match"

    assert abs(round(d, 4) - 11.3057) < TOL, \
        "ERROR: 3rd conjunction() test doesn't match"


def test_neptune_opposition():
    """Tests the opposition() method of Neptune class"""

    epoch = Epoch(1846, 8, 1)
    oppo = Neptune.opposition(epoch)
    y, m, d = oppo.get_date()

    assert abs(round(y, 0) - 1846) < TOL, \
        "ERROR: 1st opposition() test doesn't match"

    assert abs(round(m, 0) - 8) < TOL, \
        "ERROR: 2nd opposition() test doesn't match"

    assert abs(round(d, 4) - 20.1623) < TOL, \
        "ERROR: 3rd opposition() test doesn't match"
