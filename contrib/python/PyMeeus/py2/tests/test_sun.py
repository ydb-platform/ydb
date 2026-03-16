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
from pymeeus.Sun import Sun
from pymeeus.Epoch import Epoch


# Sun module

def test_sun_true_longitude_coarse():
    """Tests the true_longitude_coarse() method of Sun class"""

    epoch = Epoch(1992, 10, 13)
    true_lon, r = Sun.true_longitude_coarse(epoch)

    assert true_lon.dms_str(n_dec=0) == "199d 54' 36.0''", \
        "ERROR: 1st true_longitude_coarse() test, 'true_lon' doesn't match"

    assert abs(round(r, 5) - 0.99766) < TOL, \
        "ERROR: 2nd true_longitude_coarse() test, 'r' value doesn't match"


def test_sun_apparent_longitude_coarse():
    """Tests apparent_longitude_coarse() method of Sun class"""

    epoch = Epoch(1992, 10, 13)
    alon, r = Sun.apparent_longitude_coarse(epoch)

    assert alon.dms_str(n_dec=0) == "199d 54' 32.0''", \
        "ERROR: 1st apparent_longitude_coarse() test, 'alon' doesn't match"


def test_sun_apparent_rightascension_declination_coarse():
    """Tests apparent_rightascension_declination_coarse() method of Sun
    class"""

    epoch = Epoch(1992, 10, 13)
    ra, delta, r = Sun.apparent_rightascension_declination_coarse(epoch)

    assert ra.ra_str(n_dec=1) == "13h 13' 31.4''", \
        "ERROR: 1st rightascension_declination_coarse() test doesn't match"

    assert delta.dms_str(n_dec=0) == "-7d 47' 6.0''", \
        "ERROR: 2nd rightascension_declination_coarse() test doesn't match"


def test_sun_geometric_geocentric_position():
    """Tests the geometric_geocentric_position() method of Sun class"""

    epoch = Epoch(1992, 10, 13.0)
    lon, lat, r = Sun.geometric_geocentric_position(epoch, tofk5=False)

    assert abs(round(lon.to_positive(), 6) - 199.907297) < TOL, \
        "ERROR: 1st geometric_geocentric_position() test, 'lon' doesn't match"

    assert lat.dms_str(n_dec=3) == "0.744''", \
        "ERROR: 2nd geometric_geocentric_position() test, 'lat' doesn't match"

    assert abs(round(r, 8) - 0.99760852) < TOL, \
        "ERROR: 3rd geometric_geocentric_position() test, 'r' doesn't match"


def test_sun_apparent_geocentric_position():
    """Tests the apparent_geocentric_position() method of Sun class"""

    epoch = Epoch(1992, 10, 13.0)
    lon, lat, r = Sun.apparent_geocentric_position(epoch)

    assert lon.to_positive().dms_str(n_dec=3) == "199d 54' 21.548''", \
        "ERROR: 1st apparent_geocentric_position() test, 'lon' doesn't match"

    assert lat.dms_str(n_dec=3) == "0.721''", \
        "ERROR: 2nd apparent_geocentric_position() test, 'lat' doesn't match"

    assert abs(round(r, 8) - 0.99760852) < TOL, \
        "ERROR: 3rd apparent_geocentric_position() test, 'r' doesn't match"


def test_rectangular_coordinates_mean_equinox():
    """Tests rectangular_coordinates_mean_equinox() method of Sun class"""

    epoch = Epoch(1992, 10, 13.0)
    x, y, z = Sun.rectangular_coordinates_mean_equinox(epoch)

    assert abs(round(x, 7) - (-0.9379963)) < TOL, \
        "ERROR: 1st rectangular_coordinates_mean_equinox(), 'x' doesn't match"

    assert abs(round(y, 6) - (-0.311654)) < TOL, \
        "ERROR: 2nd rectangular_coordinates_mean_equinox(), 'y' doesn't match"

    assert abs(round(z, 7) - (-0.1351207)) < TOL, \
        "ERROR: 3rd rectangular_coordinates_mean_equinox(), 'z' doesn't match"


def test_rectangular_coordinates_j2000():
    """Tests rectangular_coordinates_j2000() method of Sun class"""

    epoch = Epoch(1992, 10, 13.0)
    x, y, z = Sun.rectangular_coordinates_j2000(epoch)

    assert abs(round(x, 8) - (-0.93740485)) < TOL, \
        "ERROR: 1st rectangular_coordinates_j2000() test, 'x' doesn't match"

    assert abs(round(y, 8) - (-0.3131474)) < TOL, \
        "ERROR: 2nd rectangular_coordinates_j2000() test, 'y' doesn't match"

    assert abs(round(z, 8) - (-0.13577045)) < TOL, \
        "ERROR: 3rd rectangular_coordinates_j2000() test, 'z' doesn't match"


def test_rectangular_coordinates_b1950():
    """Tests rectangular_coordinates_b1950() method of Sun class"""

    epoch = Epoch(1992, 10, 13.0)
    x, y, z = Sun.rectangular_coordinates_b1950(epoch)

    assert abs(round(x, 8) - (-0.94149557)) < TOL, \
        "ERROR: 1st rectangular_coordinates_b1950() test, 'x' doesn't match"

    assert abs(round(y, 8) - (-0.30259922)) < TOL, \
        "ERROR: 2nd rectangular_coordinates_b1950() test, 'y' doesn't match"

    assert abs(round(z, 8) - (-0.11578695)) < TOL, \
        "ERROR: 3rd rectangular_coordinates_b1950() test, 'z' doesn't match"


def test_rectangular_coordinates_equinox():
    """Tests rectangular_coordinates_equinox() method of Sun class"""

    epoch = Epoch(1992, 10, 13.0)
    e_equinox = Epoch(2467616.0)
    x, y, z = Sun.rectangular_coordinates_equinox(epoch, e_equinox)

    assert abs(round(x, 8) - (-0.93368986)) < TOL, \
        "ERROR: 1st rectangular_coordinates_equinox() test, 'x' doesn't match"

    assert abs(round(y, 8) - (-0.32235085)) < TOL, \
        "ERROR: 2nd rectangular_coordinates_equinox() test, 'y' doesn't match"

    assert abs(round(z, 8) - (-0.13977098)) < TOL, \
        "ERROR: 3rd rectangular_coordinates_equinox() test, 'z' doesn't match"


def test_sun_get_equinox_solstice():
    """Tests the get_equinox_solstice() method of Sun class"""

    epoch = Sun.get_equinox_solstice(1962, target="summer")
    y, m, d, h, mi, s = epoch.get_full_date()
    st = "{}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s, 0))

    assert st == "1962/6/21 21:24:42.0", \
        "ERROR: 1st get_equinox_solstice() test, time stamp doesn't match"


def test_sun_equation_of_time():
    """Tests the equation_of_time() method of Sun class"""

    epoch = Epoch(1992, 10, 13.0)
    m, s = Sun.equation_of_time(epoch)

    assert abs(m) - 13 < TOL, \
        "ERROR: 1st equation_of_time() test, 'm' doesn't match"

    assert abs(round(s, 1)) - 42.6 < TOL, \
        "ERROR: 2nd equation_of_time() test, 's' doesn't match"


def test_sun_ephemeris_physical_observations():
    """Tests the ephemeris_physical_observations() method of Sun class"""

    epoch = Epoch(1992, 10, 13)
    p, b0, l0 = Sun.ephemeris_physical_observations(epoch)

    assert abs(round(p, 2)) - 26.27 < TOL, \
        "ERROR: 1st ephemeris_physical_observations() test, 'p' doesn't match"

    assert abs(round(b0, 2)) - 5.99 < TOL, \
        "ERROR: 2nd ephemeris_physical_observations() test, 'b0' doesn't match"

    assert abs(round(l0, 2)) - 238.63 < TOL, \
        "ERROR: 3rd ephemeris_physical_observations() test, 'l0' doesn't match"
