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
from pymeeus.Angle import Angle
from pymeeus.Minor import Minor
from pymeeus.Epoch import Epoch


# Minor class


def test_minor_geocentric_position():
    """Tests the geocentric_position() method of Minor class"""

    a = 2.2091404
    e = 0.8502196
    q = a * (1.0 - e)
    i = Angle(11.94524)
    omega = Angle(334.75006)
    w = Angle(186.23352)
    t = Epoch(1990, 10, 28.54502)
    epoch = Epoch(1990, 10, 6.0)
    minor = Minor(q, e, i, omega, w, t)
    ra, dec, elong = minor.geocentric_position(epoch)

    assert ra.ra_str(n_dec=1) == "10h 34' 13.7''", \
        "ERROR: 1st geocentric_position() test doesn't match"

    assert dec.dms_str(n_dec=0) == "19d 9' 32.0''", \
        "ERROR: 2nd geocentric_position() test doesn't match"

    assert abs(round(elong, 2) - 40.51) < TOL, \
        "ERROR: 3rd geocentric_position() test doesn't match"


def test_minor_heliocentric_ecliptical_position():
    """Tests the heliocentric_ecliptical_position() method of Minor class"""

    a = 2.2091404
    e = 0.8502196
    q = a * (1.0 - e)
    i = Angle(11.94524)
    omega = Angle(334.75006)
    w = Angle(186.23352)
    t = Epoch(1990, 10, 28.54502)
    epoch = Epoch(1990, 10, 6.0)
    minor = Minor(q, e, i, omega, w, t)
    lon, lat = minor.heliocentric_ecliptical_position(epoch)

    assert lon.dms_str(n_dec=1) == "66d 51' 57.8''", \
        "ERROR: 1st heliocentric_ecliptical_position() test doesn't match"

    assert lat.dms_str(n_dec=1) == "11d 56' 14.4''", \
        "ERROR: 2nd heliocentric_ecliptical_position() test doesn't match"
