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
from pymeeus.Moon import Moon
from pymeeus.Epoch import Epoch


# Moon class

def test_moon_geocentric_ecliptical_pos():
    """Tests the method 'geocentric_ecliptical_pos()' of Moon class"""

    epoch = Epoch(1992, 4, 12.0)
    Lambda, Beta, Delta, ppi = Moon.geocentric_ecliptical_pos(epoch)
    Lambda = round(Lambda, 6)
    Beta = round(Beta, 6)
    Delta = round(Delta, 1)
    ppi = round(ppi, 6)

    assert abs(Lambda - 133.162655) < TOL, \
        "ERROR: 1st 'geocentric_ecliptical_pos()' test, 'Lambda' value doesn't\
            match"

    assert abs(Beta - (-3.229126)) < TOL, \
        "ERROR: 2nd 'geocentric_ecliptical_pos()' test, 'Beta' value doesn't\
            match"

    assert abs(Delta - 368409.7) < TOL, \
        "ERROR: 3rd 'geocentric_ecliptical_pos()' test, 'Delta' value doesn't\
            match"

    assert abs(ppi - 0.991990) < TOL, \
        "ERROR: 4th 'geocentric_ecliptical_pos()' test, 'ppi' value doesn't\
            match"


def test_moon_apparent_ecliptical_pos():
    """Tests the method 'apparent_ecliptical_pos()' of Moon class"""

    epoch = Epoch(1992, 4, 12.0)
    Lambda, Beta, Delta, ppi = Moon.apparent_ecliptical_pos(epoch)
    Lambda = round(Lambda, 5)
    Beta = round(Beta, 6)
    Delta = round(Delta, 1)
    ppi = round(ppi, 6)

    assert abs(Lambda - 133.16726) < TOL, \
        "ERROR: 1st 'apparent_ecliptical_pos()' test, 'Lambda' value doesn't\
            match"

    assert abs(Beta - (-3.229126)) < TOL, \
        "ERROR: 2nd 'apparent_ecliptical_pos()' test, 'Beta' value doesn't\
            match"

    assert abs(Delta - 368409.7) < TOL, \
        "ERROR: 3rd 'apparent_ecliptical_pos()' test, 'Delta' value doesn't\
            match"

    assert abs(ppi - 0.991990) < TOL, \
        "ERROR: 4th 'apparent_ecliptical_pos()' test, 'ppi' value doesn't\
            match"


def test_moon_apparent_equatorial_pos():
    """Tests the method 'apparent_equatorial_pos()' of Moon class"""

    epoch = Epoch(1992, 4, 12.0)
    ra, dec, Delta, ppi = Moon.apparent_equatorial_pos(epoch)
    ra = round(ra, 6)
    dec = round(dec, 6)
    Delta = round(Delta, 1)
    ppi = round(ppi, 6)

    assert abs(ra - 134.688469) < TOL, \
        "ERROR: 1st 'apparent_equatorial_pos()' test, 'ra' value doesn't\
            match"

    assert abs(dec - 13.768367) < TOL, \
        "ERROR: 2nd 'apparent_equatorial_pos()' test, 'dec' value doesn't\
            match"

    assert abs(Delta - 368409.7) < TOL, \
        "ERROR: 3rd 'apparent_equatorial_pos()' test, 'Delta' value doesn't\
            match"

    assert abs(ppi - 0.991990) < TOL, \
        "ERROR: 4th 'apparent_equatorial_pos()' test, 'ppi' value doesn't\
            match"


def test_moon_longitude_mean_ascending_node():
    """Tests the method 'longitude_mean_ascending_node()' of Moon class"""

    epoch = Epoch(1913, 5, 27.0)
    Omega1 = Moon.longitude_mean_ascending_node(epoch)
    Omega1 = round(Omega1, 1)
    epoch = Epoch(2043, 9, 10.0)
    Omega2 = Moon.longitude_mean_ascending_node(epoch)
    Omega2 = round(Omega2, 1)
    epoch = Epoch(1959, 12, 7.0)
    Omega3 = Moon.longitude_mean_ascending_node(epoch)
    Omega3 = round(Omega3, 1)
    epoch = Epoch(2108, 11, 3.0)
    Omega4 = Moon.longitude_mean_ascending_node(epoch)
    Omega4 = round(Omega4, 1)

    assert abs(Omega1 - 0.0) < TOL, \
        "ERROR: 1st 'longitude_mean_ascending_node()' test, 'Omega1' value\
            doesn't match"

    assert abs(Omega2 - 0.0) < TOL, \
        "ERROR: 2nd 'longitude_mean_ascending_node()' test, 'Omega2' value\
            doesn't match"

    assert abs(Omega3 - 180.0) < TOL, \
        "ERROR: 3rd 'longitude_mean_ascending_node()' test, 'Omega3' value\
            doesn't match"

    assert abs(Omega4 - 180.0) < TOL, \
        "ERROR: 4th 'longitude_mean_ascending_node()' test, 'Omega4' value\
            doesn't match"


def test_moon_longitude_true_ascending_node():
    """Tests the method 'longitude_true_ascending_node()' of Moon class"""

    epoch = Epoch(1913, 5, 27.0)
    Omega = Moon.longitude_true_ascending_node(epoch)
    Omega = round(Omega, 4)

    assert abs(Omega - 0.8763) < TOL, \
        "ERROR: 1st 'longitude_true_ascending_node()' test, 'Omega' value\
            doesn't match"


def test_moon_longitude_mean_perigee_node():
    """Tests the method 'longitude_mean_perigee()' of Moon class"""

    epoch = Epoch(2021, 3, 5.0)
    Pi = Moon.longitude_mean_perigee(epoch)
    Pi = round(Pi, 5)

    assert abs(Pi - 224.89194) < TOL, \
        "ERROR: 1st 'longitude_mean_perigee()' test, 'Pi' value doesn't match"


def test_moon_illuminated_fraction_disk():
    """Tests the method 'illuminated_fraction_disk()' of Moon class"""

    epoch = Epoch(1992, 4, 12.0)
    k = Moon.illuminated_fraction_disk(epoch)
    k = round(k, 2)

    assert abs(k - 0.68) < TOL, \
        "ERROR: 1st 'illuminated_fraction_disk()' test, 'k' value doesn't\
            match"


def test_moon_position_bright_limb():
    """Tests the method 'position_bright_limb()' of Moon class"""

    epoch = Epoch(1992, 4, 12.0)
    xi = Moon.position_bright_limb(epoch)
    xi = round(xi, 1)

    assert abs(xi - 285.0) < TOL, \
        "ERROR: 1st 'position_bright_limb()' test, 'xi' value doesn't match"


def test_moon_phase():
    """Tests the method 'moon_phase()' of Moon class"""

    epoch = Epoch(1977, 2, 15.0)
    new_moon = Moon.moon_phase(epoch, target="new")
    y, m, d, h, mi, s = new_moon.get_full_date()
    res = (str(y) + "/" + str(m) + "/" + str(d) + " " + str(h) + ":" + str(mi)
           + ":" + str(round(s, 0)))

    assert res == "1977/2/18 3:37:42.0", \
        "ERROR: 1st 'moon_phase()' test, 'res' value doesn't match"

    epoch = Epoch(2044, 1, 1.0)
    new_moon = Moon.moon_phase(epoch, target="last")
    y, m, d, h, mi, s = new_moon.get_full_date()
    res = (str(y) + "/" + str(m) + "/" + str(d) + " " + str(h) + ":" + str(mi)
           + ":" + str(round(s, 0)))

    assert res == "2044/1/21 23:48:17.0", \
        "ERROR: 2nd 'moon_phase()' test, 'res' value doesn't match"


def test_moon_perigee_apogee():
    """Tests the method 'moon_perigee_apogee()' of Moon class"""

    epoch = Epoch(1988, 10, 1.0)
    apogee, parallax = Moon.moon_perigee_apogee(epoch, target="apogee")
    y, m, d, h, mi, s = apogee.get_full_date()
    apo = (str(y) + "/" + str(m) + "/" + str(d) + " " + str(h) + ":" + str(mi))
    para = parallax.dms_str(n_dec=3)

    assert apo == "1988/10/7 20:30", \
        "ERROR: 1st 'moon_perigee_apogee()' test, 'apo' value doesn't match"

    assert para == "54' 0.679''", \
        "ERROR: 2nd 'moon_perigee_apogee()' test, 'para' value doesn't match"


def test_moon_passage_nodes():
    """Tests the method 'moon_passage_nodes()' of Moon class"""

    epoch = Epoch(1987, 5, 15.0)
    passage = Moon.moon_passage_nodes(epoch, target="ascending")
    y, m, d, h, mi, s = passage.get_full_date()
    mi = round(mi + s / 60.0, 0)
    pas = (str(y) + "/" + str(m) + "/" + str(d) + " " + str(h) + ":" + str(mi))

    assert pas == "1987/5/23 6:26.0", \
        "ERROR: 1st 'moon_passage_nodes()' test, 'pas' value doesn't match"


def test_moon_maximum_declination():
    """Tests the method 'moon_maximum_declination()' of Moon class"""

    epoch = Epoch(1988, 12, 15.0)
    epo, dec = Moon.moon_maximum_declination(epoch)
    y, m, d, h, mi, s = epo.get_full_date()
    epochstr = "{}/{}/{} {}:0{}".format(y, m, d, h, mi)
    decli = "{}".format(dec.dms_str(n_dec=0))

    assert epochstr == "1988/12/22 20:01", \
        "ERROR: 1st 'moon_maximum_declination()' test, 'epochstr' value "\
        + "doesn't match"

    assert decli == "28d 9' 22.0''", \
        "ERROR: 2nd 'moon_maximum_declination()' test, 'decli' value doesn't "\
        + "match"

    epoch = Epoch(2049, 4, 15.0)
    epo, dec = Moon.moon_maximum_declination(epoch, target='southern')
    y, m, d, h, mi, s = epo.get_full_date()
    epochstr = "{}/{}/{} {}:{}".format(y, m, d, h, mi)
    decli = "{}".format(dec.dms_str(n_dec=0))

    assert epochstr == "2049/4/21 14:0", \
        "ERROR: 3rd 'moon_maximum_declination()' test, 'epochstr' value "\
        + "doesn't match"

    assert decli == "-22d 8' 18.0''", \
        "ERROR: 4th 'moon_maximum_declination()' test, 'decli' value doesn't "\
        + "match"

    epoch = Epoch(-4, 3, 15.0)
    epo, dec = Moon.moon_maximum_declination(epoch, target='northern')
    y, m, d, h, mi, s = epo.get_full_date()
    epochstr = "{}/{}/{} {}h".format(y, m, d, h)
    decli = "{}".format(dec.dms_str(n_dec=0))

    assert epochstr == "-4/3/16 15h", \
        "ERROR: 5th 'moon_maximum_declination()' test, 'epochstr' value "\
        + "doesn't match"

    assert decli == "28d 58' 26.0''", \
        "ERROR: 6th 'moon_maximum_declination()' test, 'decli' value doesn't "\
        + "match"


def test_moon_librations():
    """Tests the method 'moon_librations()' of Moon class"""

    epoch = Epoch(1992, 4, 12.0)
    lopt, bopt, lphys, bphys, ltot, btot = Moon.moon_librations(epoch)

    assert abs(round(lopt, 3) - (-1.206)) < TOL, \
        "ERROR: 1st 'moon_librations()' test, 'lopt' value "\
        + "doesn't match"

    assert abs(round(bopt, 3) - 4.194) < TOL, \
        "ERROR: 2nd 'moon_librations()' test, 'bopt' value doesn't "\
        + "match"

    assert abs(round(lphys, 3) - (-0.025)) < TOL, \
        "ERROR: 3rd 'moon_librations()' test, 'lphys' value "\
        + "doesn't match"

    assert abs(round(bphys, 3) - 0.006) < TOL, \
        "ERROR: 4th 'moon_librations()' test, 'bphys' value doesn't "\
        + "match"

    assert abs(round(ltot, 2) - (-1.23)) < TOL, \
        "ERROR: 5th 'moon_librations()' test, 'ltot' value "\
        + "doesn't match"

    assert abs(round(btot, 3) - 4.2) < TOL, \
        "ERROR: 6th 'moon_librations()' test, 'btot' value doesn't "\
        + "match"


def test_moon_position_angle_axis():
    """Tests the method 'moon_position_angle_axis()' of Moon class"""

    epoch = Epoch(1992, 4, 12.0)
    p = Moon.moon_position_angle_axis(epoch)

    assert abs(round(p, 2) - 15.08) < TOL, \
        "ERROR: 1st 'moon_position_angle_axis()' test, 'p' value "\
        + "doesn't match"
