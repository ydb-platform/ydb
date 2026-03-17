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


from math import cos

from pymeeus.base import TOL
from pymeeus.Coordinates import mean_obliquity, true_obliquity, \
    nutation_longitude, nutation_obliquity, precession_equatorial, \
    precession_ecliptical, motion_in_space, equatorial2ecliptical, \
    ecliptical2equatorial, equatorial2horizontal, horizontal2equatorial, \
    equatorial2galactic, galactic2equatorial, ecliptic_horizon, \
    parallactic_angle, ecliptic_equator, diurnal_path_horizon, \
    times_rise_transit_set, refraction_apparent2true, \
    refraction_true2apparent, angular_separation, \
    minimum_angular_separation, relative_position_angle, \
    planetary_conjunction, planet_star_conjunction, planet_stars_in_line, \
    straight_line, circle_diameter, apparent_position, \
    orbital_equinox2equinox, kepler_equation, velocity, \
    velocity_perihelion, velocity_aphelion, length_orbit, \
    passage_nodes_elliptic, passage_nodes_parabolic, phase_angle, \
    illuminated_fraction
from pymeeus.Angle import Angle
from pymeeus.Epoch import Epoch, JDE2000


# Coordinates module

def test_coordinates_mean_obliquity():
    """Tests the mean_obliquity() method of Coordinates module"""

    e0 = mean_obliquity(1987, 4, 10)
    a = e0.dms_tuple()
    assert abs(a[0] - 23.0) < TOL, \
        "ERROR: 1st mean_obliquity() test, 'degrees' value doesn't match"

    assert abs(a[1] - 26.0) < TOL, \
        "ERROR: 2nd mean_obliquity() test, 'minutes' value doesn't match"

    assert abs(round(a[2], 3) - 27.407) < TOL, \
        "ERROR: 3rd mean_obliquity() test, 'seconds value doesn't match"

    assert abs(a[3] - 1.0) < TOL, \
        "ERROR: 4th mean_obliquity() test, 'sign' value doesn't match"


def test_coordinates_true_obliquity():
    """Tests the true_obliquity() method of Coordinates module"""

    epsilon = true_obliquity(1987, 4, 10)
    a = epsilon.dms_tuple()
    assert abs(a[0] - 23.0) < TOL, \
        "ERROR: 1st true_obliquity() test, 'degrees' value doesn't match"

    assert abs(a[1] - 26.0) < TOL, \
        "ERROR: 2nd true_obliquity() test, 'minutes' value doesn't match"

    assert abs(round(a[2], 3) - 36.849) < TOL, \
        "ERROR: 3rd true_obliquity() test, 'seconds value doesn't match"

    assert abs(a[3] - 1.0) < TOL, \
        "ERROR: 4th true_obliquity() test, 'sign' value doesn't match"


def test_coordinates_nutation_longitude():
    """Tests the nutation_longitude() method of Coordinates module"""

    dpsi = nutation_longitude(1987, 4, 10)
    a = dpsi.dms_tuple()
    assert abs(a[0] - 0.0) < TOL, \
        "ERROR: 1st nutation_longitude() test, 'degrees' value doesn't match"

    assert abs(a[1] - 0.0) < TOL, \
        "ERROR: 2nd nutation_longitude() test, 'minutes' value doesn't match"

    assert abs(round(a[2], 3) - 3.788) < TOL, \
        "ERROR: 3rd nutation_longitude() test, 'seconds value doesn't match"

    assert abs(a[3] - (-1.0)) < TOL, \
        "ERROR: 4th nutation_longitude() test, 'sign' value doesn't match"


def test_coordinates_nutation_obliquity():
    """Tests the nutation_obliquity() method of Coordinates module"""

    depsilon = nutation_obliquity(1987, 4, 10)
    a = depsilon.dms_tuple()
    assert abs(a[0] - 0.0) < TOL, \
        "ERROR: 1st nutation_obliquity() test, 'degrees' value doesn't match"

    assert abs(a[1] - 0.0) < TOL, \
        "ERROR: 2nd nutation_obliquity() test, 'minutes' value doesn't match"

    assert abs(round(a[2], 3) - 9.443) < TOL, \
        "ERROR: 3rd nutation_obliquity() test, 'seconds value doesn't match"

    assert abs(a[3] - 1.0) < TOL, \
        "ERROR: 4th nutation_obliquity() test, 'sign' value doesn't match"


def test_coordinates_precession_equatorial():
    """Tests the precession_equatorial() method of Coordinates module"""

    start_epoch = JDE2000
    final_epoch = Epoch(2028, 11, 13.19)
    alpha0 = Angle(2, 44, 11.986, ra=True)
    delta0 = Angle(49, 13, 42.48)
    pm_ra = Angle(0, 0, 0.03425, ra=True)
    pm_dec = Angle(0, 0, -0.0895)

    alpha, delta = precession_equatorial(start_epoch, final_epoch, alpha0,
                                         delta0, pm_ra, pm_dec)

    assert alpha.ra_str(False, 3) == "2:46:11.331", \
        "ERROR: 1st precession_equatorial test, right ascension doesn't match"

    assert delta.dms_str(False, 2) == "49:20:54.54", \
        "ERROR: 2nd precession_equatorial() test, 'declination' doesn't match"


def test_coordinates_precession_ecliptical():
    """Tests the precession_ecliptical() method of Coordinates module"""

    start_epoch = JDE2000
    final_epoch = Epoch(-214, 6, 30.0)
    lon0 = Angle(149.48194)
    lat0 = Angle(1.76549)

    lon, lat = precession_ecliptical(start_epoch, final_epoch, lon0, lat0)

    assert abs(round(lon(), 3) - 118.704) < TOL, \
        "ERROR: 1st precession_ecliptical() test, 'longitude' doesn't match"

    assert abs(round(lat(), 3) - 1.615) < TOL, \
        "ERROR: 2nd precession_ecliptical() test, 'latitude' doesn't match"


def test_coordinates_motion_in_space():
    """Tests the motion_in_space() method of Coordinates module"""

    ra = Angle(6, 45, 8.871, ra=True)
    dec = Angle(-16.716108)
    pm_ra = Angle(0, 0, -0.03847, ra=True)
    pm_dec = Angle(0, 0, -1.2053)
    dist = 2.64
    vel = -7.6

    alpha, delta = motion_in_space(ra, dec, dist, vel, pm_ra, pm_dec, -2000.0)

    assert alpha.ra_str(False, 2) == "6:46:25.09", \
        "ERROR: 1st motion_in_space() test, 'right ascension' doesn't match"

    assert delta.dms_str(False, 1) == "-16:3:0.8", \
        "ERROR: 2nd motion_in_space() test, 'declination' doesn't match"

    alpha, delta = motion_in_space(ra, dec, dist, vel, pm_ra, pm_dec, -3000.0)

    assert alpha.ra_str(False, 2) == "6:47:2.67", \
        "ERROR: 3rd motion_in_space() test, 'right ascension' doesn't match"

    assert delta.dms_str(False, 1) == "-15:43:12.3", \
        "ERROR: 4th motion_in_space() test, 'declination' doesn't match"

    alpha, delta = motion_in_space(ra, dec, dist, vel, pm_ra, pm_dec, -12000.0)

    assert alpha.ra_str(False, 2) == "6:52:25.72", \
        "ERROR: 5th motion_in_space() test, 'right ascension' doesn't match"

    assert delta.dms_str(False, 1) == "-12:50:6.7", \
        "ERROR: 6th motion_in_space() test, 'declination' doesn't match"


def test_coordinates_equatorial2ecliptical():
    """Tests the equatorial2ecliptical() method of Coordinates module"""

    ra = Angle(7, 45, 18.946, ra=True)
    dec = Angle(28, 1, 34.26)
    epsilon = Angle(23.4392911)
    lon, lat = equatorial2ecliptical(ra, dec, epsilon)

    assert abs(round(lon(), 5) - 113.21563) < TOL, \
        "ERROR: 1st equatorial2ecliptical() test, 'longitude' doesn't match"

    assert abs(round(lat(), 5) - 6.68417) < TOL, \
        "ERROR: 2nd equatorial2ecliptical() test, 'latitude' doesn't match"


def test_coordinates_ecliptical2equatorial():
    """Tests the ecliptical2equatorial() method of Coordinates module"""

    lon = Angle(113.21563)
    lat = Angle(6.68417)
    epsilon = Angle(23.4392911)
    ra, dec = ecliptical2equatorial(lon, lat, epsilon)

    assert ra.ra_str(n_dec=3) == "7h 45' 18.946''", \
        "ERROR: 1st ecliptical2equatorial() test, 'ra' doesn't match"

    assert dec.dms_str(n_dec=2) == "28d 1' 34.26''", \
        "ERROR: 2nd ecliptical2equatorial() test, 'declination' doesn't match"


def test_coordinates_equatorial2horizontal():
    """Tests the equatorial2horizontal() method of Coordinates module"""

    lon = Angle(77, 3, 56)
    lat = Angle(38, 55, 17)
    ra = Angle(23, 9, 16.641, ra=True)
    dec = Angle(-6, 43, 11.61)
    theta0 = Angle(8, 34, 57.0896, ra=True)
    eps = Angle(23, 26, 36.87)
    delta = Angle(0, 0, ((-3.868*cos(eps.rad()))/15.0), ra=True)
    theta0 += delta
    h = theta0 - lon - ra
    azi, ele = equatorial2horizontal(h, dec, lat)

    assert abs(round(azi, 3) - 68.034) < TOL, \
        "ERROR: 1st equatorial2horizontal() test, 'azimuth' doesn't match"

    assert abs(round(ele, 3) - 15.125) < TOL, \
        "ERROR: 2nd equatorial2horizontal() test, 'elevation' doesn't match"


def test_coordinates_horizontal2equatorial():
    """Tests the horizontal2equatorial() method of Coordinates module"""

    azi = Angle(68.0337)
    ele = Angle(15.1249)
    lat = Angle(38, 55, 17)
    h, dec = horizontal2equatorial(azi, ele, lat)

    assert abs(round(h, 4) - 64.3521) < TOL, \
        "ERROR: 1st horizontal2equatorial() test, 'hour angle' doesn't match"

    assert dec.dms_str(n_dec=0) == "-6d 43' 12.0''", \
        "ERROR: 2nd horizontal2equatorial() test, 'declination' match"


def test_coordinates_equatorial2galactic():
    """Tests the equatorial2galactic() method of Coordinates module"""

    ra = Angle(17, 48, 59.74, ra=True)
    dec = Angle(-14, 43, 8.2)
    lon, lat = equatorial2galactic(ra, dec)

    assert abs(round(lon, 4) - 12.9593) < TOL, \
        "ERROR: 1st equatorial2galactic() test, 'longitude' doesn't match"

    assert abs(round(lat, 4) - 6.0463) < TOL, \
        "ERROR: 2nd equatorial2galactic() test, 'latitude' doesn't match"


def test_coordinates_galactic2equatorial():
    """Tests the galactic2equatorial() method of Coordinates module"""

    lon = Angle(12.9593)
    lat = Angle(6.0463)
    ra, dec = galactic2equatorial(lon, lat)

    assert ra.ra_str(n_dec=1) == "17h 48' 59.7''", \
        "ERROR: 1st galactic2equatorial() test, 'ra' doesn't match"

    assert dec.dms_str(n_dec=0) == "-14d 43' 8.0''", \
        "ERROR: 2nd galactic2equatorial() test, 'declination' doesn't match"


def test_coordinates_parallactic_angle():
    """Tests the parallactic_angle() method of Coordinates module"""

    hour_angle = Angle(0.0)
    declination = Angle(45.0)
    latitude = Angle(50.0)
    q = parallactic_angle(hour_angle, declination, latitude)

    assert q.dms_str(n_dec=1) == "0d 0' 0.0''", \
        "ERROR: 1st parallactic_angle() test, 'lon1' doesn't match"


def test_coordinates_ecliptic_horizon():
    """Tests the ecliptic_horizon() method of Coordinates module"""

    sidereal_time = Angle(5.0, ra=True)
    lat = Angle(51.0)
    epsilon = Angle(23.44)
    lon1, lon2, i = ecliptic_horizon(sidereal_time, lat, epsilon)

    assert lon1.dms_str(n_dec=1) == "169d 21' 29.9''", \
        "ERROR: 1st ecliptic_horizon() test, 'lon1' doesn't match"

    assert lon2.dms_str(n_dec=1) == "349d 21' 29.9''", \
        "ERROR: 2nd ecliptic_horizon() test, 'lon2' doesn't match"

    assert abs(round(i, 0) - 62.0) < TOL, \
        "ERROR: 3rd ecliptic_horizon() test, 'i' angle doesn't match"


def test_coordinates_ecliptic_equator():
    """Tests the ecliptic_equator() method of Coordinates module"""

    lon = Angle(0.0)
    lat = Angle(0.0)
    eps = Angle(23.5)
    ang_ecl_equ = ecliptic_equator(lon, lat, eps)

    assert ang_ecl_equ.dms_str(n_dec=1) == "156d 30' 0.0''", \
        "ERROR: 1st ecliptic_equator() test, 'ang_ecl_equ' doesn't match"


def test_coordinates_diurnal_path_horizon():
    """Tests the diurnal_path_horizon() method of Coordinates module"""

    dec = Angle(23.44)
    lat = Angle(40.0)
    j = diurnal_path_horizon(dec, lat)

    assert j.dms_str(n_dec=1) == "45d 31' 28.4''", \
        "ERROR: 1st diurnal_path_horizon() test, 'j' angle doesn't match"


def test_coordinates_times_rise_transit_set():
    """Tests the times_rise_transit_set() method of Coordinates module"""

    longitude = Angle(71, 5, 0.0)
    latitude = Angle(42, 20, 0.0)
    alpha1 = Angle(2, 42, 43.25, ra=True)
    delta1 = Angle(18, 2, 51.4)
    alpha2 = Angle(2, 46, 55.51, ra=True)
    delta2 = Angle(18, 26, 27.3)
    alpha3 = Angle(2, 51, 7.69, ra=True)
    delta3 = Angle(18, 49, 38.7)
    h0 = Angle(-0.5667)
    delta_t = 56.0
    theta0 = Angle(11, 50, 58.1, ra=True)
    rising, transit, setting = times_rise_transit_set(longitude, latitude,
                                                      alpha1, delta1,
                                                      alpha2, delta2,
                                                      alpha3, delta3, h0,
                                                      delta_t, theta0)

    assert abs(round(rising, 4) - 12.4238) < TOL, \
        "ERROR: 1st times_rise_transit_set() test, 'rising' time doesn't match"

    assert abs(round(transit, 3) - 19.675) < TOL, \
        "ERROR: 2nd times_rise_transit_set() test, 'transit' doesn't match"

    assert abs(round(setting, 3) - 2.911) < TOL, \
        "ERROR: 3rd times_rise_transit_set() test, 'setting' doesn't match"


def test_coordinates_refraction_apparent2true():
    """Tests the refraction_apparent2true() method of Coordinates module"""

    apparent_elevation = Angle(0, 30, 0.0)
    true = refraction_apparent2true(apparent_elevation)

    assert true.dms_str(n_dec=1) == "1' 14.7''", \
        "ERROR: 1st refraction_apparent2true() test, 'true' doesn't match"


def test_coordinates_refraction_true2apparent():
    """Tests the refraction_true2apparent() method of Coordinates module"""

    true_elevation = Angle(0, 33, 14.76)
    apparent = refraction_true2apparent(true_elevation)

    assert apparent.dms_str(n_dec=2) == "57' 51.96''", \
        "ERROR: 1st refraction_true2apparent() test, 'apparent' doesn't match"


def test_coordinates_angular_separation():
    """Tests the angular_separation() method of Coordinates module"""

    alpha1 = Angle(14, 15, 39.7, ra=True)
    delta1 = Angle(19, 10, 57.0)
    alpha2 = Angle(13, 25, 11.6, ra=True)
    delta2 = Angle(-11, 9, 41.0)
    sep_ang = angular_separation(alpha1, delta1, alpha2, delta2)

    assert abs(round(sep_ang, 3) - 32.793) < TOL, \
        "ERROR: 1st angular_separation() test, 'sep_ang' value doesn't match"


def test_coordinates_minimum_angular_separation():
    """Tests the minimum_angular_separation() method of Coordinates module"""

    alpha1_1 = Angle(10, 29, 44.27, ra=True)
    delta1_1 = Angle(11, 2, 5.9)
    alpha2_1 = Angle(10, 33, 29.64, ra=True)
    delta2_1 = Angle(10, 40, 13.2)
    alpha1_2 = Angle(10, 36, 19.63, ra=True)
    delta1_2 = Angle(10, 29, 51.7)
    alpha2_2 = Angle(10, 33, 57.97, ra=True)
    delta2_2 = Angle(10, 37, 33.4)
    alpha1_3 = Angle(10, 43, 1.75, ra=True)
    delta1_3 = Angle(9, 55, 16.7)
    alpha2_3 = Angle(10, 34, 26.22, ra=True)
    delta2_3 = Angle(10, 34, 53.9)
    n, d = minimum_angular_separation(alpha1_1, delta1_1, alpha1_2, delta1_2,
                                      alpha1_3, delta1_3, alpha2_1, delta2_1,
                                      alpha2_2, delta2_2, alpha2_3, delta2_3)

    assert abs(round(n, 6) + 0.370726) < TOL, \
        "ERROR: 1st minimum_angular_separation() test, 'n' value doesn't match"

    assert d.dms_str(n_dec=0) == "3' 44.0''", \
        "ERROR: 2nd minimum_angular_separation() test, 'd' value doesn't match"


def test_coordinates_relative_position_angle():
    """Tests the relative_position_angle() method of Coordinates module"""

    alpha1 = Angle(14, 15, 39.7, ra=True)
    delta1 = Angle(19, 10, 57.0)
    alpha2 = Angle(14, 15, 39.7, ra=True)
    delta2 = Angle(-11, 9, 41.0)
    pos_ang = relative_position_angle(alpha1, delta1, alpha2, delta2)

    assert abs(round(pos_ang, 1) - 0.0) < TOL, \
        "ERROR: 1st relative_position_angle() test, 'pos_ang' doesn't match"

    alpha1 = Angle(14, 15, 39.7, ra=True)
    delta1 = Angle(-19, 10, 57.0)
    alpha2 = Angle(14, 15, 39.7, ra=True)
    delta2 = Angle(11, 9, 41.0)
    pos_ang = relative_position_angle(alpha1, delta1, alpha2, delta2)

    assert abs(round(pos_ang, 1) - 180.0) < TOL, \
        "ERROR: 2nd relative_position_angle() test, 'pos_ang' doesn't match"


def test_coordinates_planetary_conjunction():
    """Tests the planetary_conjunction() method of Coordinates module"""

    alpha1_1 = Angle(10, 24, 30.125, ra=True)
    delta1_1 = Angle(6, 26, 32.05)
    alpha1_2 = Angle(10, 25,  0.342, ra=True)
    delta1_2 = Angle(6, 10, 57.72)
    alpha1_3 = Angle(10, 25, 12.515, ra=True)
    delta1_3 = Angle(5, 57, 33.08)
    alpha1_4 = Angle(10, 25,  6.235, ra=True)
    delta1_4 = Angle(5, 46, 27.07)
    alpha1_5 = Angle(10, 24, 41.185, ra=True)
    delta1_5 = Angle(5, 37, 48.45)
    alpha2_1 = Angle(10, 27, 27.175, ra=True)
    delta2_1 = Angle(4,  4, 41.83)
    alpha2_2 = Angle(10, 26, 32.410, ra=True)
    delta2_2 = Angle(3, 55, 54.66)
    alpha2_3 = Angle(10, 25, 29.042, ra=True)
    delta2_3 = Angle(3, 48,  3.51)
    alpha2_4 = Angle(10, 24, 17.191, ra=True)
    delta2_4 = Angle(3, 41, 10.25)
    alpha2_5 = Angle(10, 22, 57.024, ra=True)
    delta2_5 = Angle(3, 35, 16.61)
    alpha1_list = [alpha1_1, alpha1_2, alpha1_3, alpha1_4, alpha1_5]
    delta1_list = [delta1_1, delta1_2, delta1_3, delta1_4, delta1_5]
    alpha2_list = [alpha2_1, alpha2_2, alpha2_3, alpha2_4, alpha2_5]
    delta2_list = [delta2_1, delta2_2, delta2_3, delta2_4, delta2_5]
    pc = planetary_conjunction(alpha1_list, delta1_list,
                               alpha2_list, delta2_list)

    assert abs(round(pc[0], 5) - 0.23797) < TOL, \
        "ERROR: 1st planetary_conjunction() test, 'pc[0]' doesn't match"

    assert pc[1].dms_str(n_dec=1) == "2d 8' 21.8''", \
        "ERROR: 2nd planetary_conjunction() test, 'pc[1]' doesn't match"


def test_coordinates_planet_star_conjunction():
    """Tests the planet_star_conjunction() method of Coordinates module"""

    alpha_1 = Angle(15,  3, 51.937, ra=True)
    delta_1 = Angle(-8, 57, 34.51)
    alpha_2 = Angle(15,  9, 57.327, ra=True)
    delta_2 = Angle(-9,  9,  3.88)
    alpha_3 = Angle(15, 15, 37.898, ra=True)
    delta_3 = Angle(-9, 17, 37.94)
    alpha_4 = Angle(15, 20, 50.632, ra=True)
    delta_4 = Angle(-9, 23, 16.25)
    alpha_5 = Angle(15, 25, 32.695, ra=True)
    delta_5 = Angle(-9, 26,  1.01)
    alpha_star = Angle(15, 17, 0.446, ra=True)
    delta_star = Angle(-9, 22, 58.47)
    alpha_list = [alpha_1, alpha_2, alpha_3, alpha_4, alpha_5]
    delta_list = [delta_1, delta_2, delta_3, delta_4, delta_5]
    pc = planet_star_conjunction(alpha_list, delta_list,
                                 alpha_star, delta_star)

    assert abs(round(pc[0], 4) - 0.2551) < TOL, \
        "ERROR: 1st planet_star_conjunction() test, 'pc[0]' doesn't match"

    assert pc[1].dms_str(n_dec=0) == "3' 38.0''", \
        "ERROR: 2nd planet_star_conjunction() test, 'pc[1]' doesn't match"


def test_coordinates_planet_stars_in_line():
    """Tests the planet_stars_in_line() method of Coordinates module"""

    alpha_1 = Angle(7, 55, 55.36, ra=True)
    delta_1 = Angle(21, 41,  3.0)
    alpha_2 = Angle(7, 58, 22.55, ra=True)
    delta_2 = Angle(21, 35, 23.4)
    alpha_3 = Angle(8,  0, 48.99, ra=True)
    delta_3 = Angle(21, 29, 38.2)
    alpha_4 = Angle(8,  3, 14.66, ra=True)
    delta_4 = Angle(21, 23, 47.5)
    alpha_5 = Angle(8,  5, 39.54, ra=True)
    delta_5 = Angle(21, 17, 51.4)
    alpha_star1 = Angle(7, 34, 16.40, ra=True)
    delta_star1 = Angle(31, 53, 51.2)
    alpha_star2 = Angle(7, 45,  0.10, ra=True)
    delta_star2 = Angle(28,  2, 12.5)
    alpha_list = [alpha_1, alpha_2, alpha_3, alpha_4, alpha_5]
    delta_list = [delta_1, delta_2, delta_3, delta_4, delta_5]
    n = planet_stars_in_line(alpha_list, delta_list, alpha_star1, delta_star1,
                             alpha_star2, delta_star2)

    assert abs(round(n, 4) - 0.2233) < TOL, \
        "ERROR: 1st planet_stars_in_line() test, 'n' value doesn't match"


def test_coordinates_straight_line():
    """Tests the straight_line() method of Coordinates module"""

    alpha1 = Angle(5, 32,  0.40, ra=True)
    delta1 = Angle(0, -17, 56.9)
    alpha2 = Angle(5, 36, 12.81, ra=True)
    delta2 = Angle(-1, 12,  7.0)
    alpha3 = Angle(5, 40, 45.52, ra=True)
    delta3 = Angle(-1, 56, 33.3)
    psi, omega = straight_line(alpha1, delta1, alpha2, delta2, alpha3, delta3)

    assert psi.dms_str(n_dec=0) == "7d 31' 1.0''", \
        "ERROR: 1st straight_line() test, 'psi' value doesn't match"

    assert omega.dms_str(n_dec=0) == "-5' 24.0''", \
        "ERROR: 2nd straight_line() test, 'omega' value doesn't match"


def test_coordinates_circle_diameter():
    """Tests the circle_diameter() method of Coordinates module"""

    alpha1 = Angle(12, 41,  8.63, ra=True)
    delta1 = Angle(-5, 37, 54.2)
    alpha2 = Angle(12, 52,  5.21, ra=True)
    delta2 = Angle(-4, 22, 26.2)
    alpha3 = Angle(12, 39, 28.11, ra=True)
    delta3 = Angle(-1, 50,  3.7)
    d = circle_diameter(alpha1, delta1, alpha2, delta2, alpha3, delta3)

    assert d.dms_str(n_dec=0) == "4d 15' 49.0''", \
        "ERROR: 1st circle_diameter() test, 'd' value doesn't match"

    alpha1 = Angle(9,  5, 41.44, ra=True)
    delta1 = Angle(18, 30, 30.0)
    alpha2 = Angle(9,  9, 29.0, ra=True)
    delta2 = Angle(17, 43, 56.7)
    alpha3 = Angle(8, 59, 47.14, ra=True)
    delta3 = Angle(17, 49, 36.8)
    d = circle_diameter(alpha1, delta1, alpha2, delta2, alpha3, delta3)

    assert d.dms_str(n_dec=0) == "2d 18' 38.0''", \
        "ERROR: 2nd circle_diameter() test, 'd' value doesn't match"


def test_coordinates_apparent_position():
    """Tests the apparent_position() method of Coordinates module"""

    epoch = Epoch(2028, 11, 13.19)
    alpha = Angle(2, 46, 11.331, ra=True)
    delta = Angle(49, 20, 54.54)
    sun_lon = Angle(231.328)
    app_alpha, app_delta = apparent_position(epoch, alpha, delta, sun_lon)

    assert app_alpha.ra_str(n_dec=2) == "2h 46' 14.39''", \
        "ERROR: 1st apparent_position() test, 'app_alpha' value doesn't match"

    assert app_delta.dms_str(n_dec=2) == "49d 21' 7.45''", \
        "ERROR: 2nd apparent_position() test, 'app_delta' value doesn't match"


def test_coordinates_orbital_equinox2equinox():
    """Tests the orbital_equinox2equinox() method of Coordinates module"""

    epoch0 = Epoch(2358042.5305)
    epoch = Epoch(2433282.4235)
    i0 = Angle(47.122)
    arg0 = Angle(151.4486)
    lon0 = Angle(45.7481)
    i1, arg1, lon1 = orbital_equinox2equinox(epoch0, epoch, i0, arg0, lon0)

    assert abs(round(i1(), 3) - 47.138) < TOL, \
        "ERROR: 1st orbital_equinox2equinox() test, 'i1' value doesn't match"

    assert abs(round(arg1(), 4) - 151.4782) < TOL, \
        "ERROR: 2nd orbital_equinox2equinox() test, 'arg1' value doesn't match"

    assert abs(round(lon1(), 4) - 48.6037) < TOL, \
        "ERROR: 3rd orbital_equinox2equinox() test, 'lon1' value doesn't match"


def test_coordinates_kepler_equation():
    """Tests the kepler_equation() method of Coordinates module"""

    e1, v1 = kepler_equation(0.1, Angle(5.0))
    e2, v2 = kepler_equation(0.99, Angle(1.0))
    e3, v3 = kepler_equation(0.99, Angle(0.2, radians=True))

    assert abs(round(e1(), 6) - 5.554589) < TOL, \
        "ERROR: 1st kepler_equation() test, 'e1' value doesn't match"

    assert abs(round(v1(), 6) - 6.139762) < TOL, \
        "ERROR: 2nd kepler_equation() test, 'v1' value doesn't match"

    assert abs(round(e2(), 6) - 24.725822) < TOL, \
        "ERROR: 3rd kepler_equation() test, 'e2' value doesn't match"

    assert abs(round(v2(), 6) - 144.155952) < TOL, \
        "ERROR: 4th kepler_equation() test, 'v2' value doesn't match"

    assert abs(round(e3(), 8) - 61.13444578) < TOL, \
        "ERROR: 5th kepler_equation() test, 'e3' value doesn't match"

    assert abs(round(v3(), 6) - 166.311977) < TOL, \
        "ERROR: 6th kepler_equation() test, 'v3' value doesn't match"


def test_coordinates_velocity():
    """Tests the velocity() function of Coordinates module"""

    r = 1.0
    a = 17.9400782
    v = velocity(r, a)

    assert abs(round(v, 2) - 41.53) < TOL, \
        "ERROR: 1st velocity() test, value doesn't match"


def test_coordinates_velocity_perihelion():
    """Tests the velocity_perihelion() function of Coordinates module"""

    a = 17.9400782
    e = 0.96727426
    vp = velocity_perihelion(e, a)

    assert abs(round(vp, 2) - 54.52) < TOL, \
        "ERROR: 1st velocity_perihelion() test, value doesn't match"


def test_coordinates_velocity_aphelion():
    """Tests the velocity_aphelion() function of Coordinates module"""

    a = 17.9400782
    e = 0.96727426
    va = velocity_aphelion(e, a)

    assert abs(round(va, 2) - 0.91) < TOL, \
        "ERROR: 1st velocity_aphelion() test, value doesn't match"


def test_coordinates_length_orbit():
    """Tests the length_orbit() function of Coordinates module"""

    a = 17.9400782
    e = 0.96727426
    length = length_orbit(e, a)

    assert abs(round(length, 2) - 77.06) < TOL, \
        "ERROR: 1st length_orbit() test, value doesn't match"


def test_coordinates_passage_nodes_elliptic():
    """Tests the passage_nodes_elliptic() function of Coordinates module"""

    omega = Angle(111.84644)
    e = 0.96727426
    a = 17.9400782
    t = Epoch(1986, 2, 9.45891)
    time, r = passage_nodes_elliptic(omega, e, a, t)
    year, month, day = time.get_date()

    assert abs(year - 1985) < TOL, \
        "ERROR: 1st passage_nodes_elliptic() test, value doesn't match"

    assert abs(month - 11) < TOL, \
        "ERROR: 2nd passage_nodes_elliptic() test, value doesn't match"

    assert abs(round(day, 2) - 9.16) < TOL, \
        "ERROR: 3rd passage_nodes_elliptic() test, value doesn't match"

    assert abs(round(r, 4) - 1.8045) < TOL, \
        "ERROR: 4th passage_nodes_elliptic() test, value doesn't match"

    time, r = passage_nodes_elliptic(omega, e, a, t, ascending=False)
    year, month, day = time.get_date()

    assert abs(year - 1986) < TOL, \
        "ERROR: 5th passage_nodes_elliptic() test, value doesn't match"

    assert abs(month - 3) < TOL, \
        "ERROR: 6th passage_nodes_elliptic() test, value doesn't match"

    assert abs(round(day, 2) - 10.37) < TOL, \
        "ERROR: 7th passage_nodes_elliptic() test, value doesn't match"

    assert abs(round(r, 4) - 0.8493) < TOL, \
        "ERROR: 8th passage_nodes_elliptic() test, value doesn't match"


def test_coordinates_passage_nodes_parabolic():
    """Tests the passage_nodes_parabolic() function of Coordinates module"""

    omega = Angle(154.9103)
    q = 1.324502
    t = Epoch(1989, 8, 20.291)
    time, r = passage_nodes_parabolic(omega, q, t)
    year, month, day = time.get_date()

    assert abs(year - 1977) < TOL, \
        "ERROR: 1st passage_nodes_parabolic() test, value doesn't match"

    assert abs(month - 9) < TOL, \
        "ERROR: 2nd passage_nodes_parabolic() test, value doesn't match"

    assert abs(round(day, 2) - 17.64) < TOL, \
        "ERROR: 3rd passage_nodes_parabolic() test, value doesn't match"

    assert abs(round(r, 4) - 28.0749) < TOL, \
        "ERROR: 4th passage_nodes_parabolic() test, value doesn't match"

    time, r = passage_nodes_parabolic(omega, q, t, ascending=False)
    year, month, day = time.get_date()

    assert abs(year - 1989) < TOL, \
        "ERROR: 5th passage_nodes_parabolic() test, value doesn't match"

    assert abs(month - 9) < TOL, \
        "ERROR: 6th passage_nodes_parabolic() test, value doesn't match"

    assert abs(round(day, 3) - 17.636) < TOL, \
        "ERROR: 7th passage_nodes_parabolic() test, value doesn't match"

    assert abs(round(r, 4) - 1.3901) < TOL, \
        "ERROR: 8th passage_nodes_parabolic() test, value doesn't match"


def test_coordinates_phase_angle():
    """Tests the phase_angle() function of Coordinates module"""

    sun_dist = 0.724604
    earth_dist = 0.910947
    sun_earth_dist = 0.983824
    angle = phase_angle(sun_dist, earth_dist, sun_earth_dist)

    assert abs(round(angle, 2) - 72.96) < TOL, \
        "ERROR: 1st phase_angle() test, value doesn't match"


def test_coordinates_illuminated_fraction():
    """Tests the illuminated_fraction() function of Coordinates module"""

    sun_dist = 0.724604
    earth_dist = 0.910947
    sun_earth_dist = 0.983824
    k = illuminated_fraction(sun_dist, earth_dist, sun_earth_dist)

    assert abs(round(k, 3) - 0.647) < TOL, \
        "ERROR: 1st illuminated_fraction() test, value doesn't match"
