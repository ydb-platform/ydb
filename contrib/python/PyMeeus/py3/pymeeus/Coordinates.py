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


from math import (
    sqrt, sin, cos, tan, atan, atan2, asin, acos, radians, pi, copysign,
    degrees
)
from pymeeus.base import TOL, iint
from pymeeus.Angle import Angle
from pymeeus.Epoch import Epoch, JDE2000
from pymeeus.Interpolation import Interpolation


"""
.. module:: Coordinates
   :synopsis: Module including different functions to handle coordinates
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


NUTATION_ARG_TABLE = [
    [0, 0, 0, 0, 1],
    [-2, 0, 0, 2, 2],
    [0, 0, 0, 2, 2],
    [0, 0, 0, 0, 2],
    [0, 1, 0, 0, 0],
    [0, 0, 1, 0, 0],
    [-2, 1, 0, 2, 2],
    [0, 0, 0, 2, 1],
    [0, 0, 1, 2, 2],
    [-2, -1, 0, 2, 2],
    [-2, 0, 1, 0, 0],
    [-2, 0, 0, 2, 1],
    [0, 0, -1, 2, 2],
    [2, 0, 0, 0, 0],
    [0, 0, 1, 0, 1],
    [2, 0, -1, 2, 2],
    [0, 0, -1, 0, 1],
    [0, 0, 1, 2, 1],
    [-2, 0, 2, 0, 0],
    [0, 0, -2, 2, 1],
    [2, 0, 0, 2, 2],
    [0, 0, 2, 2, 2],
    [0, 0, 2, 0, 0],
    [-2, 0, 1, 2, 2],
    [0, 0, 0, 2, 0],
    [-2, 0, 0, 2, 0],
    [0, 0, -1, 2, 1],
    [0, 2, 0, 0, 0],
    [2, 0, -1, 0, 1],
    [-2, 2, 0, 2, 2],
    [0, 1, 0, 0, 1],
    [-2, 0, 1, 0, 1],
    [0, -1, 0, 0, 1],
    [0, 0, 2, -2, 0],
    [2, 0, -1, 2, 1],
    [2, 0, 1, 2, 2],
    [0, 1, 0, 2, 2],
    [-2, 1, 1, 0, 0],
    [0, -1, 0, 2, 2],
    [2, 0, 0, 2, 1],
    [2, 0, 1, 0, 0],
    [-2, 0, 2, 2, 2],
    [-2, 0, 1, 2, 1],
    [2, 0, -2, 0, 1],
    [2, 0, 0, 0, 1],
    [0, -1, 1, 0, 0],
    [-2, -1, 0, 2, 1],
    [-2, 0, 0, 0, 1],
    [0, 0, 2, 2, 1],
    [-2, 0, 2, 0, 1],
    [-2, 1, 0, 2, 1],
    [0, 0, 1, -2, 0],
    [-1, 0, 1, 0, 0],
    [-2, 1, 0, 0, 0],
    [1, 0, 0, 0, 0],
    [0, 0, 1, 2, 0],
    [0, 0, -2, 2, 2],
    [-1, -1, 1, 0, 0],
    [0, 1, 1, 0, 0],
    [0, -1, 1, 2, 2],
    [2, -1, -1, 2, 2],
    [0, 0, 3, 2, 2],
    [2, -1, 0, 2, 2],
]
"""This table contains the periodic terms for the argument of the nutation. In
Meeus' book this is Table 22.A and can be found in pages 145-146."""

NUTATION_SINE_COEF_TABLE = [
    [-171996.0, -174.2],
    [-13187.0, -1.6],
    [-2274.0, -0.2],
    [2062.0, 0.2],
    [1426.0, -3.4],
    [712.0, 0.1],
    [-517.0, 1.2],
    [-386.0, -0.4],
    [-301.0, 0.0],
    [217.0, -0.5],
    [-158.0, 0.0],
    [129.0, 0.1],
    [123.0, 0.0],
    [63.0, 0.0],
    [63.0, 0.1],
    [-59.0, 0.0],
    [-58.0, -0.1],
    [-51.0, 0.0],
    [48.0, 0.0],
    [46.0, 0.0],
    [-38.0, 0.0],
    [-31.0, 0.0],
    [29.0, 0.0],
    [29.0, 0.0],
    [26.0, 0.0],
    [-22.0, 0.0],
    [21.0, 0.0],
    [17.0, -0.1],
    [16.0, 0.0],
    [-16.0, 0.1],
    [-15.0, 0.0],
    [-13.0, 0.0],
    [-12.0, 0.0],
    [11.0, 0.0],
    [-10.0, 0.0],
    [-8.0, 0.0],
    [7.0, 0.0],
    [-7.0, 0.0],
    [-7.0, 0.0],
    [-7.0, 0.0],
    [6.0, 0.0],
    [6.0, 0.0],
    [6.0, 0.0],
    [-6.0, 0.0],
    [-6.0, 0.0],
    [5.0, 0.0],
    [-5.0, 0.0],
    [-5.0, 0.0],
    [-5.0, 0.0],
    [4.0, 0.0],
    [4.0, 0.0],
    [4.0, 0.0],
    [-4.0, 0.0],
    [-4.0, 0.0],
    [-4.0, 0.0],
    [3.0, 0.0],
    [-3.0, 0.0],
    [-3.0, 0.0],
    [-3.0, 0.0],
    [-3.0, 0.0],
    [-3.0, 0.0],
    [-3.0, 0.0],
    [-3.0, 0.0],
]
"""This table contains the periodic terms for the coefficients of the sine of
the argument of the nutation, and they are used to compute Delta psi. Units are
in 0.0001''. In Meeus' book this is Table 22.A and can be found in pages
145-146."""

NUTATION_COSINE_COEF_TABLE = [
    [92025.0, 8.9],
    [5736.0, -3.1],
    [977.0, -0.5],
    [-895.0, 0.5],
    [54.0, -0.1],
    [-7.0, 0.0],
    [224.0, -0.6],
    [200.0, 0.0],
    [129.0, -0.1],
    [-95.0, 0.3],
    [0.0, 0.0],
    [-70.0, 0.0],
    [-53.0, 0.0],
    [0.0, 0.0],
    [-33.0, 0.0],
    [26.0, 0.0],
    [32.0, 0.0],
    [27.0, 0.0],
    [0.0, 0.0],
    [-24.0, 0.0],
    [16.0, 0.0],
    [13.0, 0.0],
    [0.0, 0.0],
    [-12.0, 0.0],
    [0.0, 0.0],
    [0.0, 0.0],
    [-10.0, 0.0],
    [0.0, 0.0],
    [-8.0, 0.0],
    [7.0, 0.0],
    [9.0, 0.0],
    [7.0, 0.0],
    [6.0, 0.0],
    [0.0, 0.0],
    [5.0, 0.0],
    [3.0, 0.0],
    [-3.0, 0.0],
    [0.0, 0.0],
    [3.0, 0.0],
    [3.0, 0.0],
    [0.0, 0.0],
    [-3.0, 0.0],
    [-3.0, 0.0],
    [3.0, 0.0],
    [3.0, 0.0],
    [0.0, 0.0],
    [3.0, 0.0],
    [3.0, 0.0],
    [3.0, 0.0],
]
"""This table contains the periodic terms for the coefficients of the cosine of
the argument of the nutation, and they are used to compute Delta epsilon. Units
are in 0.0001''. In Meeus' book this is Table 22.A and can be found in pages
145-146."""


def mean_obliquity(*args, **kwargs):
    """This function computes the mean obliquity (epsilon0) at the provided
    date.

    This function internally uses an :class:`Epoch` object, and the **utc**
    argument then controls the way the UTC->TT conversion is handled for that
    object. If **leap_seconds** argument is set to a value different than zero,
    then that value will be used for the UTC->TAI conversion, and the internal
    leap seconds table will be bypassed.

    :param args: Either :class:`Epoch`, date, datetime or year, month,
        day values, by themselves or inside a tuple or list
    :type args: int, float, :py:class:`Epoch`, datetime, date, tuple,
        list
    :param utc: Whether the provided epoch is a civil time (UTC) or TT
    :type utc: bool
    :param leap_seconds: This is the value to be used in the UTC->TAI
        conversion, instead of taking it from internal leap seconds table.
    :type leap_seconds: int, float

    :returns: The mean obliquity of the ecliptic, as an :class:`Angle`
    :rtype: :class:`Angle`
    :raises: ValueError if input values are in the wrong range.
    :raises: TypeError if input values are of wrong type.

    >>> e0 = mean_obliquity(1987, 4, 10)
    >>> a = e0.dms_tuple()
    >>> a[0]
    23
    >>> a[1]
    26
    >>> round(a[2], 3)
    27.407
    """

    # Get the Epoch object corresponding to input parameters
    t = Epoch.check_input_date(*args, **kwargs)
    # Let's redefine u in units of 100 Julian centuries from Epoch J2000.0
    u = (t.jde() - 2451545.0) / 3652500.0
    epsilon0 = Angle(23, 26, 21.448)
    delta = u * (
        -4680.93
        + u
        * (
            -1.55
            + u
            * (
                1999.25
                + u
                * (
                    -51.38
                    + u
                    * (
                        -249.67
                        + u
                        * (-39.05 + u * (7.12 + u
                                         * (27.87 + u * (5.79 + u * 2.45))))
                    )
                )
            )
        )
    )
    delta = Angle(0, 0, delta)
    epsilon0 += delta
    return epsilon0


def true_obliquity(*args, **kwargs):
    """This function computes the true obliquity (epsilon) at the provided
    date. The true obliquity is the mean obliquity (epsilon0) plus the
    correction provided by the nutation in obliquity (Delta epsilon).

    This function internally uses an :class:`Epoch` object, and the **utc**
    argument then controls the way the UTC->TT conversion is handled for that
    object. If **leap_seconds** argument is set to a value different than zero,
    then that value will be used for the UTC->TAI conversion, and the internal
    leap seconds table will be bypassed.

    :param args: Either :class:`Epoch`, date, datetime or year, month,
        day values, by themselves or inside a tuple or list
    :type args: int, float, :py:class:`Epoch`, datetime, date, tuple,
        list
    :param utc: Whether the provided epoch is a civil time (UTC) or TT
    :type utc: bool
    :param leap_seconds: This is the value to be used in the UTC->TAI
        conversion, instead of taking it from internal leap seconds table.
    :type leap_seconds: int, float

    :returns: The true obliquity of the ecliptic, as an Angle
    :rtype: :class:`Angle`
    :raises: ValueError if input values are in the wrong range.
    :raises: TypeError if input values are of wrong type.

    >>> epsilon = true_obliquity(1987, 4, 10)
    >>> a = epsilon.dms_tuple()
    >>> a[0]
    23
    >>> a[1]
    26
    >>> round(a[2], 3)
    36.849
    """

    epsilon0 = mean_obliquity(*args, **kwargs)
    delta_epsilon = nutation_obliquity(*args, **kwargs)
    return epsilon0 + delta_epsilon


def nutation_longitude(*args, **kwargs):
    """This function computes the nutation in longitude (Delta psi) at the
    provided date.

    This function internally uses an :class:`Epoch` object, and the **utc**
    argument then controls the way the UTC->TT conversion is handled for that
    object. If **leap_seconds** argument is set to a value different than zero,
    then that value will be used for the UTC->TAI conversion, and the internal
    leap seconds table will be bypassed.

    :param args: Either :class:`Epoch`, date, datetime or year, month,
        day values, by themselves or inside a tuple or list
    :type args: int, float, :py:class:`Epoch`, datetime, date, tuple,
        list
    :param utc: Whether the provided epoch is a civil time (UTC) or TT
    :type utc: bool
    :param leap_seconds: This is the value to be used in the UTC->TAI
        conversion, instead of taking it from internal leap seconds table.
    :type leap_seconds: int, float

    :returns: The nutation in longitude (Delta psi), as an Angle
    :rtype: :class:`Angle`
    :raises: ValueError if input values are in the wrong range.
    :raises: TypeError if input values are of wrong type.

    >>> dpsi = nutation_longitude(1987, 4, 10)
    >>> a = dpsi.dms_tuple()
    >>> a[0]
    0
    >>> a[1]
    0
    >>> round(a[2], 3)
    3.788
    >>> a[3]
    -1.0
    """

    # Get the Epoch object corresponding to input parameters
    t = Epoch.check_input_date(*args, **kwargs)
    # Let's redefine t in units of Julian centuries from Epoch J2000.0
    t = (t.jde() - 2451545.0) / 36525.0
    # Let's compute the mean elongation of the Moon from the Sun
    d = 297.85036 + t * (445267.111480 + t * (-0.0019142 + t / 189474.0))
    d = Angle(d)  # Convert into an Angle: It is easier to handle
    # Compute the mean anomaly of the Sun (from Earth)
    m = 357.52772 + t * (35999.050340 + t * (-0.0001603 - t / 300000.0))
    m = Angle(m)
    # Compute the mean anomaly of the Moon
    mprime = 134.96298 + t * (477198.867398 + t * (0.0086972 + t / 56250.0))
    mprime = Angle(mprime)
    # Now, let's compute the Moon's argument of latitude
    f = 93.27191 + t * (483202.017538 + t * (-0.0036825 + t / 327270.0))
    f = Angle(f)
    # And finally, the longitude of the ascending node of the Moon's mean
    # orbit on the ecliptic, measured from the mean equinox of date
    omega = 125.04452 + t * (-1934.136261 + t * (0.0020708 + t / 450000.0))
    omega = Angle(omega)
    # Let's store this results in a list, in preparation for using tables
    arguments = [d, m, mprime, f, omega]
    # Now is time of using the nutation tables
    deltapsi = 0.0
    for i, value in enumerate(NUTATION_SINE_COEF_TABLE):
        argument = Angle()
        coeff = 0.0
        for j in range(5):
            if NUTATION_ARG_TABLE[i][j]:  # Avoid multiplications by zero
                argument += NUTATION_ARG_TABLE[i][j] * arguments[j]
        coeff = value[0]
        if value[1]:
            coeff += value[1] * t
        deltapsi += (coeff * sin(argument.rad())) / 10000.0
    return Angle(0, 0, deltapsi)


def nutation_obliquity(*args, **kwargs):
    """This function computes the nutation in obliquity (Delta epsilon) at
    the provided date.

    This function internally uses an :class:`Epoch` object, and the **utc**
    argument then controls the way the UTC->TT conversion is handled for that
    object. If **leap_seconds** argument is set to a value different than zero,
    then that value will be used for the UTC->TAI conversion, and the internal
    leap seconds table will be bypassed.

    :param args: Either :class:`Epoch`, date, datetime or year, month,
        day values, by themselves or inside a tuple or list
    :type args: int, float, :py:class:`Epoch`, datetime, date, tuple,
        list
    :param utc: Whether the provided epoch is a civil time (UTC) or TT
    :type utc: bool
    :param leap_seconds: This is the value to be used in the UTC->TAI
        conversion, instead of taking it from internal leap seconds table.
    :type leap_seconds: int, float

    :returns: The nutation in obliquity (Delta epsilon), as an
        :class:`Angle`
    :rtype: :class:`Angle`
    :raises: ValueError if input values are in the wrong range.
    :raises: TypeError if input values are of wrong type.

    >>> depsilon = nutation_obliquity(1987, 4, 10)
    >>> a = depsilon.dms_tuple()
    >>> a[0]
    0
    >>> a[1]
    0
    >>> round(a[2], 3)
    9.443
    >>> a[3]
    1.0
    """

    # Get the Epoch object corresponding to input parameters
    t = Epoch.check_input_date(*args, **kwargs)
    # Let's redefine t in units of Julian centuries from Epoch J2000.0
    t = (t.jde() - 2451545.0) / 36525.0
    # Let's compute the mean elongation of the Moon from the Sun
    d = 297.85036 + t * (445267.111480 + t * (-0.0019142 + t / 189474.0))
    d = Angle(d)  # Convert into an Angle: It is easier to handle
    # Compute the mean anomaly of the Sun (from Earth)
    m = 357.52772 + t * (35999.050340 + t * (-0.0001603 - t / 300000.0))
    m = Angle(m)
    # Compute the mean anomaly of the Moon
    mprime = 134.96298 + t * (477198.867398 + t * (0.0086972 + t / 56250.0))
    mprime = Angle(mprime)
    # Now, let's compute the Moon's argument of latitude
    f = 93.27191 + t * (483202.017538 + t * (-0.0036825 + t / 327270.0))
    f = Angle(f)
    # And finally, the longitude of the ascending node of the Moon's mean
    # orbit on the ecliptic, measured from the mean equinox of date
    omega = 125.04452 + t * (-1934.136261 + t * (0.0020708 + t / 450000.0))
    omega = Angle(omega)
    # Let's store this results in a list, in preparation for using tables
    arguments = [d, m, mprime, f, omega]
    # Now is time of using the nutation tables
    deltaepsilon = 0.0
    for i, value in enumerate(NUTATION_COSINE_COEF_TABLE):
        argument = Angle()
        coeff = 0.0
        for j in range(5):
            if NUTATION_ARG_TABLE[i][j]:  # Avoid multiplications by zero
                argument += NUTATION_ARG_TABLE[i][j] * arguments[j]
        coeff = value[0]
        if value[1]:
            coeff += value[1] * t
        deltaepsilon += (coeff * cos(argument.rad())) / 10000.0
    return Angle(0, 0, deltaepsilon)


def precession_equatorial(
    start_epoch, final_epoch, start_ra, start_dec, p_motion_ra=0.0,
    p_motion_dec=0.0
):
    """This function converts the equatorial coordinates (right ascension and
    declination) given for an epoch and a equinox, to the corresponding
    values for another epoch and equinox. Only the **mean** positions, i.e.
    the effects of precession and proper motion, are considered here.

    :param start_epoch: Initial epoch when initial coordinates are given
    :type start_epoch: :py:class:`Epoch`
    :param final_epoch: Final epoch for when coordinates are going to be
        computed
    :type final_epoch: :py:class:`Epoch`
    :param start_ra: Initial right ascension
    :type start_ra: :py:class:`Angle`
    :param start_dec: Initial declination
    :type start_dec: :py:class:`Angle`
    :param p_motion_ra: Proper motion in right ascension, in degrees per
        year. Zero by default.
    :type p_motion_ra: :py:class:`Angle`
    :param p_motion_dec: Proper motion in declination, in degrees per year.
        Zero by default.
    :type p_motion_dec: :py:class:`Angle`

    :returns: Equatorial coordinates (right ascension, declination, in that
        order) corresponding to the final epoch, given as two objects
        :class:`Angle` inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> start_epoch = JDE2000
    >>> final_epoch = Epoch(2028, 11, 13.19)
    >>> alpha0 = Angle(2, 44, 11.986, ra=True)
    >>> delta0 = Angle(49, 13, 42.48)
    >>> pm_ra = Angle(0, 0, 0.03425, ra=True)
    >>> pm_dec = Angle(0, 0, -0.0895)
    >>> alpha, delta = precession_equatorial(start_epoch, final_epoch, alpha0,
    ...                                      delta0, pm_ra, pm_dec)
    >>> print(alpha.ra_str(False, 3))
    2:46:11.331
    >>> print(delta.dms_str(False, 2))
    49:20:54.54
    """

    # First check that input values are of correct types
    if not (
        isinstance(start_epoch, Epoch)
        and isinstance(final_epoch, Epoch)
        and isinstance(start_ra, Angle)
        and isinstance(start_dec, Angle)
    ):
        raise TypeError("Invalid input types")
    if isinstance(p_motion_ra, (int, float)):
        p_motion_ra = Angle(p_motion_ra)
    if isinstance(p_motion_dec, (int, float)):
        p_motion_dec = Angle(p_motion_dec)
    if not (isinstance(p_motion_ra, Angle)
            and isinstance(p_motion_dec, Angle)):
        raise TypeError("Invalid input types")
    tt = (start_epoch - JDE2000) / 36525.0
    t = (final_epoch - start_epoch) / 36525.0
    # Correct starting coordinates by proper motion
    start_ra += p_motion_ra * t * 100.0
    start_dec += p_motion_dec * t * 100.0
    # Compute the conversion parameters
    zeta = t * (
        (2306.2181 + tt * (1.39656 - 0.000139 * tt))
        + t * ((0.30188 - 0.000344 * tt) + 0.017998 * t)
    )
    z = t * (
        (2306.2181 + tt * (1.39656 - 0.000139 * tt))
        + t * ((1.09468 + 0.000066 * tt) + 0.018203 * t)
    )
    theta = t * (
        2004.3109
        + tt * (-0.85330 - 0.000217 * tt)
        + t * (-(0.42665 + 0.000217 * tt) - 0.041833 * t)
    )
    # Redefine the former values as Angles
    zeta = Angle(0, 0, zeta)
    z = Angle(0, 0, z)
    theta = Angle(0, 0, theta)
    a = cos(start_dec.rad()) * sin(start_ra.rad() + zeta.rad())
    b = cos(theta.rad()) * cos(start_dec.rad()) * cos(
        start_ra.rad() + zeta.rad()
    ) - sin(theta.rad()) * sin(start_dec.rad())
    c = sin(theta.rad()) * cos(start_dec.rad()) * cos(
        start_ra.rad() + zeta.rad()
    ) + cos(theta.rad()) * sin(start_dec.rad())
    final_ra = atan2(a, b) + z.rad()
    if start_dec > 85.0:  # Coordinates are close to the pole
        final_dec = sqrt(a * a + b * b)
    else:
        final_dec = asin(c)
    # Convert results to Angles. Please note results are in radians
    final_ra = Angle(final_ra, radians=True)
    final_dec = Angle(final_dec, radians=True)
    return (final_ra, final_dec)


def precession_ecliptical(
    start_epoch, final_epoch, start_lon, start_lat, p_motion_lon=0.0,
    p_motion_lat=0.0
):
    """This function converts the ecliptical coordinates (longitude and
    latitude) given for an epoch and a equinox, to the corresponding
    values for another epoch and equinox. Only the **mean** positions, i.e.
    the effects of precession and proper motion, are considered here.

    :param start_epoch: Initial epoch when initial coordinates are given
    :type start_epoch: :py:class:`Epoch`
    :param final_epoch: Final epoch for when coordinates are going to be
        computed
    :type final_epoch: :py:class:`Epoch`
    :param start_lon: Initial longitude
    :type start_lon: :py:class:`Angle`
    :param start_lat: Initial latitude
    :type start_lat: :py:class:`Angle`
    :param p_motion_lon: Proper motion in longitude, in degrees per year.
        Zero by default.
    :type p_motion_lon: :py:class:`Angle`
    :param p_motion_lat: Proper motion in latitude, in degrees per year.
        Zero by default.
    :type p_motion_lat: :py:class:`Angle`

    :returns: Ecliptical coordinates (longitude, latitude, in that order)
        corresponding to the final epoch, given as two :class:`Angle`
        objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> start_epoch = JDE2000
    >>> final_epoch = Epoch(-214, 6, 30.0)
    >>> lon0 = Angle(149.48194)
    >>> lat0 = Angle(1.76549)
    >>> lon, lat = precession_ecliptical(start_epoch, final_epoch, lon0, lat0)
    >>> print(round(lon(), 3))
    118.704
    >>> print(round(lat(), 3))
    1.615
    """

    # First check that input values are of correct types
    if not (
        isinstance(start_epoch, Epoch)
        and isinstance(final_epoch, Epoch)
        and isinstance(start_lon, Angle)
        and isinstance(start_lat, Angle)
    ):
        raise TypeError("Invalid input types")
    if isinstance(p_motion_lon, (int, float)):
        p_motion_lon = Angle(p_motion_lon)
    if isinstance(p_motion_lat, (int, float)):
        p_motion_lat = Angle(p_motion_lat)
    if not (isinstance(p_motion_lon, Angle)
            and isinstance(p_motion_lat, Angle)):
        raise TypeError("Invalid input types")
    tt = (start_epoch - JDE2000) / 36525.0
    t = (final_epoch - start_epoch) / 36525.0
    # Correct starting coordinates by proper motion
    start_lon += p_motion_lon * t * 100.0
    start_lat += p_motion_lat * t * 100.0
    # Compute the conversion parameters
    eta = t * (
        (47.0029 + tt * (-0.06603 + 0.000598 * tt))
        + t * ((-0.03302 + 0.000598 * tt) + 0.00006 * t)
    )
    pie = tt * (3289.4789 + 0.60622 * tt) + t * (
        -(869.8089 + 0.50491 * tt) + 0.03536 * t
    )
    p = t * (
        5029.0966
        + tt * (2.22226 - 0.000042 * tt)
        + t * (1.11113 - 0.000042 * tt - 0.000006 * t)
    )
    eta = Angle(0, 0, eta)
    pie = Angle(0, 0, pie)
    p = Angle(0, 0, p)
    # But beware!: There is still a missing constant for pie. We didn't add
    # it before because of the mismatch between degrees and seconds
    pie += 174.876384
    a = (cos(eta.rad()) * cos(start_lat.rad())
         * sin(pie.rad() - start_lon.rad()) - sin(eta.rad())
         * sin(start_lat.rad()))
    b = cos(start_lat.rad()) * cos(pie.rad() - start_lon.rad())
    c = cos(eta.rad()) * sin(start_lat.rad()) + sin(eta.rad()) * cos(
        start_lat.rad()
    ) * sin(pie.rad() - start_lon.rad())
    final_lon = p.rad() + pie.rad() - atan2(a, b)
    final_lat = asin(c)
    # Convert results to Angles. Please note results are in radians
    final_lon = Angle(final_lon, radians=True)
    final_lat = Angle(final_lat, radians=True)
    return (final_lon, final_lat)


def p_motion_equa2eclip(p_motion_ra, p_motion_dec, ra, dec, lat, epsilon):
    """It is usual that proper motions are given in equatorial coordinates,
    not in ecliptical ones. Therefore, this function converts the provided
    proper motions in equatorial coordinates to the corresponding ones in
    ecliptical coordinates.

    :param p_motion_ra: Proper motion in right ascension, in degrees per
        year, as an :class:`Angle` object
    :type p_motion_ra: :py:class:`Angle`
    :param p_motion_dec: Proper motion in declination, in degrees per year,
        as an :class:`Angle` object
    :type p_motion_dec: :py:class:`Angle`
    :param ra: Right ascension of the astronomical object, as degrees in an
        :class:`Angle` object
    :type ra: :py:class:`Angle`
    :param dec: Declination of the astronomical object, as degrees in an
        :class:`Angle` object
    :type dec: :py:class:`Angle`
    :param lat: Ecliptical latitude of the astronomical object, as degrees
        in an :class:`Angle` object
    :type lat: :py:class:`Angle`
    :param epsilon: Obliquity of the ecliptic
    :type epsilon: :py:class:`Angle`

    :returns: Proper motions in ecliptical longitude and latitude (in that
        order), given as two :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.
    """

    # First check that input values are of correct types
    if not (
        isinstance(p_motion_ra, Angle)
        and isinstance(p_motion_dec, Angle)
        and isinstance(ra, Angle)
        and isinstance(dec, Angle)
        and isinstance(lat, Angle)
        and isinstance(epsilon, Angle)
    ):
        raise TypeError("Invalid input types")
    pm_ra = p_motion_ra.rad()
    pm_dec = p_motion_dec.rad()
    s_eps = sin(epsilon.rad())
    c_eps = cos(epsilon.rad())
    s_ra = sin(ra.rad())
    c_ra = cos(ra.rad())
    s_dec = sin(dec.rad())
    c_dec = cos(dec.rad())
    c_lat = cos(lat.rad())
    se_ca = s_eps * c_ra
    se_sd_sa = s_eps * s_dec * s_ra
    pa_cd = pm_ra * c_dec
    ce_cd = c_eps * c_dec
    cl2 = c_lat * c_lat
    p_motion_lon = (pm_dec * se_ca + pa_cd * (ce_cd + se_sd_sa)) / cl2
    p_motion_lat = (pm_dec * (ce_cd + se_sd_sa) - pa_cd * se_ca) / c_lat
    return (p_motion_lon, p_motion_lat)


def precession_newcomb(
    start_epoch, final_epoch, start_ra, start_dec, p_motion_ra=0.0,
    p_motion_dec=0.0
):
    """This function implements the Newcomb precessional equations used in
    the old FK4 system. It takes equatorial coordinates (right ascension
    and declination) given for an epoch and a equinox, and converts them to
    the corresponding values for another epoch and equinox. Only the
    **mean** positions, i.e. the effects of precession and proper motion,
    are considered here.

    :param start_epoch: Initial epoch when initial coordinates are given
    :type start_epoch: :py:class:`Epoch`
    :param final_epoch: Final epoch for when coordinates are going to be
        computed
    :type final_epoch: :py:class:`Epoch`
    :param start_ra: Initial right ascension
    :type start_ra: :py:class:`Angle`
    :param start_dec: Initial declination
    :type start_dec: :py:class:`Angle`
    :param p_motion_ra: Proper motion in right ascension, in degrees per
        year. Zero by default.
    :type p_motion_ra: :py:class:`Angle`
    :param p_motion_dec: Proper motion in declination, in degrees per year.
        Zero by default.
    :type p_motion_dec: :py:class:`Angle`

    :returns: Equatorial coordinates (right ascension, declination, in that
        order) corresponding to the final epoch, given as two objects
        :class:`Angle` inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.
    """

    # First check that input values are of correct types
    if not (
        isinstance(start_epoch, Epoch)
        and isinstance(final_epoch, Epoch)
        and isinstance(start_ra, Angle)
        and isinstance(start_dec, Angle)
    ):
        raise TypeError("Invalid input types")
    if isinstance(p_motion_ra, (int, float)):
        p_motion_ra = Angle(p_motion_ra)
    if isinstance(p_motion_dec, (int, float)):
        p_motion_dec = Angle(p_motion_dec)
    if not (isinstance(p_motion_ra, Angle)
            and isinstance(p_motion_dec, Angle)):
        raise TypeError("Invalid input types")
    tt = (start_epoch - 2415020.3135) / 36524.2199
    t = (final_epoch - start_epoch) / 36524.2199
    # Correct starting coordinates by proper motion
    start_ra += p_motion_ra * t * 100.0
    start_dec += p_motion_dec * t * 100.0
    # Compute the conversion parameters
    zeta = t * (2304.25 + 1.396 * tt + t * (0.302 + 0.018 * t))
    z = zeta + t * t * (0.791 + 0.001 * t)
    theta = t * (2004.682 - 0.853 * tt - t * (0.426 + 0.042 * t))
    # Redefine the former values as Angles
    zeta = Angle(0, 0, zeta)
    z = Angle(0, 0, z)
    theta = Angle(0, 0, theta)
    a = cos(start_dec.rad()) * sin(start_ra.rad() + zeta.rad())
    b = cos(theta.rad()) * cos(start_dec.rad()) * cos(
        start_ra.rad() + zeta.rad()
    ) - sin(theta.rad()) * sin(start_dec.rad())
    c = sin(theta.rad()) * cos(start_dec.rad()) * cos(
        start_ra.rad() + zeta.rad()
    ) + cos(theta.rad()) * sin(start_dec.rad())
    final_ra = atan2(a, b) + z.rad()
    if start_dec > 85.0:  # Coordinates are close to the pole
        final_dec = sqrt(a * a + b * b)
    else:
        final_dec = asin(c)
    # Convert results to Angles. Please note results are in radians
    final_ra = Angle(final_ra, radians=True)
    final_dec = Angle(final_dec, radians=True)
    return (final_ra, final_dec)


def motion_in_space(
    start_ra, start_dec, distance, velocity, p_motion_ra, p_motion_dec, time
):
    """This function computes the star's true motion through space relative
    to the Sun, allowing to compute the start proper motion at a given
    time.

    :param start_ra: Initial right ascension
    :type start_ra: :py:class:`Angle`
    :param start_dec: Initial declination
    :type start_dec: :py:class:`Angle`
    :param distance: Star's distance to the Sun, in parsecs. If distance is
        given in light-years, multipy it by 0.3066. If the star's parallax
        **pie** (in arcseconds) is given, use (1.0/pie).
    :type distance: float
    :param velocity: Radial velocity in km/s
    :type velocity: float
    :param p_motion_ra: Proper motion in right ascension, in degrees per
        year.
    :type p_motion_ra: :py:class:`Angle`
    :param p_motion_dec: Proper motion in declination, in degrees per year.
    :type p_motion_dec: :py:class:`Angle`
    :param time: Number of years since starting epoch, positive in the
        future, negative in the past
    :type time: float

    :returns: Equatorial coordinates (right ascension, declination, in that
        order) corresponding to the final epoch, given as two objects
        :class:`Angle` inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> ra = Angle(6, 45, 8.871, ra=True)
    >>> dec = Angle(-16.716108)
    >>> pm_ra = Angle(0, 0, -0.03847, ra=True)
    >>> pm_dec = Angle(0, 0, -1.2053)
    >>> dist = 2.64
    >>> vel = -7.6
    >>> alpha, delta = motion_in_space(ra, dec, dist, vel, pm_ra, pm_dec,
    ...                                -1000.0)
    >>> print(alpha.ra_str(False, 2))
    6:45:47.16
    >>> print(delta.dms_str(False, 1))
    -16:22:56.0
    >>> alpha, delta = motion_in_space(ra, dec, dist, vel, pm_ra, pm_dec,
    ...                                -4000.0)
    >>> print(alpha.ra_str(False, 2))
    6:47:39.91
    >>> print(delta.dms_str(False, 1))
    -15:23:30.6
    """
    # >>> ra = Angle(101.286962)

    # First check that input values are of correct types
    if not (isinstance(start_ra, Angle) and isinstance(start_dec, Angle)):
        raise TypeError("Invalid input types")
    if isinstance(p_motion_ra, (int, float)):
        p_motion_ra = Angle(p_motion_ra)
    if isinstance(p_motion_dec, (int, float)):
        p_motion_dec = Angle(p_motion_dec)
    if not (isinstance(p_motion_ra, Angle)
            and isinstance(p_motion_dec, Angle)):
        raise TypeError("Invalid input types")
    if not (
        isinstance(distance, (int, float))
        and isinstance(velocity, (int, float))
        and isinstance(time, (int, float))
    ):
        raise TypeError("Invalid input types")
    dr = velocity / 977792.0
    x = distance * cos(start_dec.rad()) * cos(start_ra.rad())
    y = distance * cos(start_dec.rad()) * sin(start_ra.rad())
    z = distance * sin(start_dec.rad())
    dx = (
        (x / distance) * dr
        - z * p_motion_dec.rad() * cos(start_ra.rad())
        - y * p_motion_ra.rad()
    )
    dy = (
        (y / distance) * dr
        - z * p_motion_dec.rad() * sin(start_ra.rad())
        + x * p_motion_ra.rad()
    )
    dz = ((z / distance) * dr + distance
          * p_motion_dec.rad() * cos(start_dec.rad()))
    xp = x + time * dx
    yp = y + time * dy
    zp = z + time * dz
    final_ra = atan2(yp, xp)
    final_dec = atan(zp / sqrt(xp * xp + yp * yp))
    # Convert results to Angles. Please note results are in radians
    final_ra = Angle(final_ra, radians=True)
    final_dec = Angle(final_dec, radians=True)
    return (final_ra, final_dec)


def equatorial2ecliptical(right_ascension, declination, obliquity):
    """This function converts from equatorial coordinated (right ascension and
    declination) to ecliptical coordinates (longitude and latitude).

    :param right_ascension: Right ascension, as an Angle object
    :type start_epoch: :py:class:`Angle`
    :param declination: Declination, as an Angle object
    :type start_epoch: :py:class:`Angle`
    :param obliquity: Obliquity of the ecliptic, as an Angle object
    :type obliquity: :py:class:`Angle`

    :returns: Ecliptical coordinates (longitude, latitude, in that order),
        given as two :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> ra = Angle(7, 45, 18.946, ra=True)
    >>> dec = Angle(28, 1, 34.26)
    >>> epsilon = Angle(23.4392911)
    >>> lon, lat = equatorial2ecliptical(ra, dec, epsilon)
    >>> print(round(lon(), 5))
    113.21563
    >>> print(round(lat(), 5))
    6.68417
    """

    # First check that input values are of correct types
    if not (
        isinstance(right_ascension, Angle)
        and isinstance(declination, Angle)
        and isinstance(obliquity, Angle)
    ):
        raise TypeError("Invalid input types")
    ra = right_ascension.rad()
    dec = declination.rad()
    eps = obliquity.rad()
    lon = atan2((sin(ra) * cos(eps) + tan(dec) * sin(eps)), cos(ra))
    lat = asin(sin(dec) * cos(eps) - cos(dec) * sin(eps) * sin(ra))
    lon = Angle(lon, radians=True)
    lon = lon.to_positive()
    lat = Angle(lat, radians=True)
    return (lon, lat)


def ecliptical2equatorial(longitude, latitude, obliquity):
    """This function converts from ecliptical coordinates (longitude and
    latitude) to equatorial coordinated (right ascension and declination).

    :param longitude: Ecliptical longitude, as an Angle object
    :type longitude: :py:class:`Angle`
    :param latitude: Ecliptical latitude, as an Angle object
    :type latitude: :py:class:`Angle`
    :param obliquity: Obliquity of the ecliptic, as an Angle object
    :type obliquity: :py:class:`Angle`

    :returns: Equatorial coordinates (right ascension, declination, in that
        order), given as two :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> lon = Angle(113.21563)
    >>> lat = Angle(6.68417)
    >>> epsilon = Angle(23.4392911)
    >>> ra, dec = ecliptical2equatorial(lon, lat, epsilon)
    >>> print(ra.ra_str(n_dec=3))
    7h 45' 18.946''
    >>> print(dec.dms_str(n_dec=2))
    28d 1' 34.26''
    """

    # First check that input values are of correct types
    if not (
        isinstance(longitude, Angle)
        and isinstance(latitude, Angle)
        and isinstance(obliquity, Angle)
    ):
        raise TypeError("Invalid input types")
    lon = longitude.rad()
    lat = latitude.rad()
    eps = obliquity.rad()
    ra = atan2((sin(lon) * cos(eps) - tan(lat) * sin(eps)), cos(lon))
    dec = asin(sin(lat) * cos(eps) + cos(lat) * sin(eps) * sin(lon))
    ra = Angle(ra, radians=True)
    ra = ra.to_positive()
    dec = Angle(dec, radians=True)
    return (ra, dec)


def equatorial2horizontal(hour_angle, declination, geo_latitude):
    """This function converts from equatorial coordinates (right ascension and
    declination) to local horizontal coordinates (azimuth and elevation).

    Following Meeus' convention, the azimuth is measured westward from the
    SOUTH. If you want the azimuth to be measured from the north (common custom
    between navigators and meteorologits), you should add 180 degrees.

    The hour angle (H) comprises information about the sidereal time, the
    observer's geodetic longitude (positive west from Greenwich) and the right
    ascension. If theta is the local sidereal time, theta0 the sidereal time at
    Greenwich, lon the observer's longitude and ra the right ascension, the
    following expressions hold:

        H = theta - ra
        H = theta0 - lon - ra

    :param hour_angle: Hour angle, as an Angle object
    :type hour_angle: :py:class:`Angle`
    :param declination: Declination, as an Angle object
    :type declination: :py:class:`Angle`
    :param geo_latitude: Geodetic latitude of the observer, as an Angle object
    :type geo_latitude: :py:class:`Angle`

    :returns: Local horizontal coordinates (azimuth, elevation, in that order),
        given as two :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> lon = Angle(77, 3, 56)
    >>> lat = Angle(38, 55, 17)
    >>> ra = Angle(23, 9, 16.641, ra=True)
    >>> dec = Angle(-6, 43, 11.61)
    >>> theta0 = Angle(8, 34, 57.0896, ra=True)
    >>> eps = Angle(23, 26, 36.87)
    >>> delta = Angle(0, 0, ((-3.868*cos(eps.rad()))/15.0), ra=True)
    >>> theta0 += delta
    >>> h = theta0 - lon - ra
    >>> azi, ele = equatorial2horizontal(h, dec, lat)
    >>> print(round(azi, 3))
    68.034
    >>> print(round(ele, 3))
    15.125
    """

    # First check that input values are of correct types
    if not (
        isinstance(hour_angle, Angle)
        and isinstance(declination, Angle)
        and isinstance(geo_latitude, Angle)
    ):
        raise TypeError("Invalid input types")
    h = hour_angle.rad()
    dec = declination.rad()
    lat = geo_latitude.rad()
    azi = atan2(sin(h), (cos(h) * sin(lat) - tan(dec) * cos(lat)))
    ele = asin(sin(lat) * sin(dec) + cos(lat) * cos(dec) * cos(h))
    azi = Angle(azi, radians=True)
    ele = Angle(ele, radians=True)
    return (azi, ele)


def horizontal2equatorial(azimuth, elevation, geo_latitude):
    """This function converts from local horizontal coordinates (azimuth and
    elevation) to equatorial coordinates (right ascension and declination).

    Following Meeus' convention, the azimuth is measured westward from the
    SOUTH.

    This function returns the hour angle and the declination. The hour angle
    (H) comprises information about the sidereal time, the observer's geodetic
    longitude (positive west from Greenwich) and the right ascension. If theta
    is the local sidereal time, theta0 the sidereal time at Greenwich, lon the
    observer's longitude and ra the right ascension, the following expressions
    hold:

        H = theta - ra
        H = theta0 - lon - ra

    :param azimuth: Azimuth, measured westward from south, as an Angle object
    :type azimuth: :py:class:`Angle`
    :param elevation: Elevation from the horizon, as an Angle object
    :type elevation: :py:class:`Angle`
    :param geo_latitude: Geodetic latitude of the observer, as an Angle object
    :type geo_latitude: :py:class:`Angle`

    :returns: Equatorial coordinates (as hour angle and declination, in that
        order), given as two :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> azi = Angle(68.0337)
    >>> ele = Angle(15.1249)
    >>> lat = Angle(38, 55, 17)
    >>> h, dec = horizontal2equatorial(azi, ele, lat)
    >>> print(round(h, 4))
    64.3521
    >>> print(dec.dms_str(n_dec=0))
    -6d 43' 12.0''
    """

    # First check that input values are of correct types
    if not (
        isinstance(azimuth, Angle)
        and isinstance(elevation, Angle)
        and isinstance(geo_latitude, Angle)
    ):
        raise TypeError("Invalid input types")
    azi = azimuth.rad()
    ele = elevation.rad()
    lat = geo_latitude.rad()
    h = atan2(sin(azi), (cos(azi) * sin(lat) + tan(ele) * cos(lat)))
    dec = asin(sin(lat) * sin(ele) - cos(lat) * cos(ele) * cos(azi))
    h = Angle(h, radians=True)
    dec = Angle(dec, radians=True)
    return (h, dec)


def equatorial2galactic(right_ascension, declination):
    """This function converts from equatorial coordinates (right ascension and
    declination) to galactic coordinates (longitude and latitude).

    The current galactic system of coordinates was defined by the International
    Astronomical Union in 1959, using the standard equatorial system of epoch
    B1950.0.

    :param right_ascension: Right ascension, as an Angle object
    :type right_ascension: :py:class:`Angle`
    :param declination: Declination, as an Angle object
    :type declination: :py:class:`Angle`

    :returns: Galactic coordinates (longitude and latitude, in that order),
        given as two :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> ra = Angle(17, 48, 59.74, ra=True)
    >>> dec = Angle(-14, 43, 8.2)
    >>> lon, lat = equatorial2galactic(ra, dec)
    >>> print(round(lon, 4))
    12.9593
    >>> print(round(lat, 4))
    6.0463
    """

    # First check that input values are of correct types
    if not (isinstance(right_ascension, Angle)
            and isinstance(declination, Angle)):
        raise TypeError("Invalid input types")
    ra = right_ascension.rad()
    dec = declination.rad()
    c1 = Angle(192.25)
    c1 = c1.rad()
    c1ra = c1 - ra
    c2 = Angle(27.4)
    c2 = c2.rad()
    x = atan2(sin(c1ra), (cos(c1ra) * sin(c2) - tan(dec) * cos(c2)))
    lon = Angle(-x, radians=True)
    lon = 303.0 + lon
    lon = lon.to_positive()
    lat = asin(sin(dec) * sin(c2) + cos(dec) * cos(c2) * cos(c1ra))
    lat = Angle(lat, radians=True)
    return (lon, lat)


def galactic2equatorial(longitude, latitude):
    """This function converts from galactic coordinates (longitude and
    latitude) to equatorial coordinates (right ascension and declination).

    The current galactic system of coordinates was defined by the International
    Astronomical Union in 1959, using the standard equatorial system of epoch
    B1950.0.

    :param longitude: Longitude, as an Angle object
    :type longitude: :py:class:`Angle`
    :param latitude: Latitude, as an Angle object
    :type latitude: :py:class:`Angle`

    :returns: Equatorial coordinates (right ascension and declination, in that
        order), given as two :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> lon = Angle(12.9593)
    >>> lat = Angle(6.0463)
    >>> ra, dec = galactic2equatorial(lon, lat)
    >>> print(ra.ra_str(n_dec=1))
    17h 48' 59.7''
    >>> print(dec.dms_str(n_dec=0))
    -14d 43' 8.0''
    """

    # First check that input values are of correct types
    if not (isinstance(longitude, Angle) and isinstance(latitude, Angle)):
        raise TypeError("Invalid input types")
    lon = longitude.rad()
    lat = latitude.rad()
    c1 = Angle(123.0)
    c1 = c1.rad()
    c2 = Angle(27.4)
    c2 = c2.rad()
    lc1 = lon - c1
    y = atan2(sin(lc1), (cos(lc1) * sin(c2) - tan(lat) * cos(c2)))
    y = Angle(y, radians=True)
    ra = y + 12.25
    ra.to_positive()
    dec = asin(sin(lat) * sin(c2) + cos(lat) * cos(c2) * cos(lc1))
    dec = Angle(dec, radians=True)
    return (ra, dec)


def parallactic_angle(hour_angle, declination, geo_latitude):
    """This function computes the parallactic angle, an apparent rotation that
    appears because celestial bodies move along parallel circles. By
    convention, the parallactic angle is negative before the passage through
    the southern meridian (in the north hemisphere), and positive afterwards.
    Exactly on the meridian, its value is zero.

    Please note that when the celestial body is exactly at the zenith, the
    parallactic angle is not defined, and this function will return 'None'.

    The hour angle (H) comprises information about the sidereal time, the
    observer's geodetic longitude (positive west from Greenwich) and the right
    ascension. If theta is the local sidereal time, theta0 the sidereal time at
    Greenwich, lon the observer's longitude and ra the right ascension, the
    following expressions hold:

        H = theta - ra
        H = theta0 - lon - ra

    :param hour_angle: Hour angle, as an Angle object
    :type hour_angle: :py:class:`Angle`
    :param declination: Declination, as an Angle object
    :type declination: :py:class:`Angle`
    :param geo_latitude: Geodetic latitude of the observer, as an Angle object
    :type geo_latitude: :py:class:`Angle`

    :returns: Parallactic angle as an py:class:`Angle` object
    :rtype: :py:class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> hour_angle = Angle(0.0)
    >>> declination = Angle(45.0)
    >>> latitude = Angle(50.0)
    >>> q = parallactic_angle(hour_angle, declination, latitude)
    >>> print(q.dms_str(n_dec=1))
    0d 0' 0.0''
    """

    # First check that input values are of correct types
    if not (
        isinstance(hour_angle, Angle)
        and isinstance(declination, Angle)
        and isinstance(geo_latitude, Angle)
    ):
        raise TypeError("Invalid input types")
    h = hour_angle.rad()
    dec = declination.rad()
    lat = geo_latitude.rad()
    den = tan(lat) * cos(dec) - sin(dec) * cos(h)
    if abs(den) < TOL:
        return None
    q = atan2(sin(h), den)
    q = Angle(q, radians=True)
    return q


def ecliptic_horizon(local_sidereal_time, geo_latitude, obliquity):
    """This function returns the longitudes of the two points of the ecliptic
    which are on the horizon, as well as the angle between the ecliptic and the
    horizon.

    :param local_sidereal_time: Local sidereal time, as an Angle object
    :type local_sidereal_time: :py:class:`Angle`
    :param geo_latitude: Geodetic latitude, as an Angle object
    :type geo_latitude: :py:class:`Angle`
    :param obliquity: Obliquity of the ecliptic, as an Angle object
    :type obliquity: :py:class:`Angle`

    :returns: Longitudes of the two points of the ecliptic which are on the
        horizon, and the angle between the ecliptic and the horizon (in that
        order), given as three :class:`Angle` objects inside a tuple
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> sidereal_time = Angle(5.0, ra=True)
    >>> lat = Angle(51.0)
    >>> epsilon = Angle(23.44)
    >>> lon1, lon2, i = ecliptic_horizon(sidereal_time, lat, epsilon)
    >>> print(lon1.dms_str(n_dec=1))
    169d 21' 29.9''
    >>> print(lon2.dms_str(n_dec=1))
    349d 21' 29.9''
    >>> print(round(i, 0))
    62.0
    """

    # First check that input values are of correct types
    if not (
        isinstance(local_sidereal_time, Angle)
        and isinstance(geo_latitude, Angle)
        and isinstance(obliquity, Angle)
    ):
        raise TypeError("Invalid input types")
    theta = local_sidereal_time.rad()
    lat = geo_latitude.rad()
    eps = obliquity.rad()
    # First, let's compute the longitudes of the ecliptic points on the horizon
    lon1 = atan2(-cos(theta), (sin(eps) * tan(lat) + cos(eps) * sin(theta)))
    lon1 = Angle(lon1, radians=True)
    lon1.to_positive()
    # Get the second point, which is 180 degrees apart
    if lon1 < 180.0:
        lon2 = lon1 + 180.0
    else:
        lon2 = lon1
        lon1 = lon2 - 180.0
    # Now, compute the angle between the ecliptic and the horizon
    i = acos(cos(eps) * sin(lat) - sin(eps) * cos(lat) * sin(theta))
    i = Angle(i, radians=True)
    return (lon1, lon2, i)


def ecliptic_equator(longitude, latitude, obliquity):
    """This function returns the angle between the direction of the northern
    celestial pole and the direction of the north pole of the ecliptic, taking
    as reference the point whose ecliptic longitude and latitude are given.

    Please note that if we make latitude=0, the result is the angle between the
    ecliptic (at the given ecliptical longitude) and the east-west direction on
    the celestial sphere.

    :param longitude: Ecliptical longitude, as an Angle object
    :type longitude: :py:class:`Angle`
    :param latitude: Ecliptical latitude, as an Angle object
    :type latitude: :py:class:`Angle`
    :param obliquity: Obliquity of the ecliptic, as an Angle object
    :type obliquity: :py:class:`Angle`

    :returns: Angle between the direction of the northern celestial pole and
        the direction of the north pole of the ecliptic, given as one
        :class:`Angle` object
    :rtype: :class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> lon = Angle(0.0)
    >>> lat = Angle(0.0)
    >>> eps = Angle(23.5)
    >>> ang_ecl_equ = ecliptic_equator(lon, lat, eps)
    >>> print(ang_ecl_equ.dms_str(n_dec=1))
    156d 30' 0.0''
    """

    # First check that input values are of correct types
    if not (
        isinstance(longitude, Angle)
        and isinstance(latitude, Angle)
        and isinstance(obliquity, Angle)
    ):
        raise TypeError("Invalid input types")
    lon = longitude.rad()
    lat = latitude.rad()
    eps = obliquity.rad()
    q = (atan2((cos(lon) * tan(eps)), (sin(lat)
                                       * sin(lon) * tan(eps) - cos(lat))))
    q = Angle(q, radians=True)
    return q


def diurnal_path_horizon(declination, geo_latitude):
    """This function returns the angle of the diurnal path of a celestial body
    relative to the horizon at the time of its rising or setting.

    :param declination: Declination, as an Angle object
    :type declination: :py:class:`Angle`
    :param geo_latitude: Geodetic latitude, as an Angle object
    :type geo_latitude: :py:class:`Angle`

    :returns: Angle of the diurnal path of the celestial body relative to the
        horizon at the time of rising or setting, given as one
        :class:`Angle` object
    :rtype: :class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> declination = Angle(23.44)
    >>> latitude = Angle(40.0)
    >>> path_angle = diurnal_path_horizon(declination, latitude)
    >>> print(path_angle.dms_str(n_dec=1))
    45d 31' 28.4''
    """

    # First check that input values are of correct types
    if not (isinstance(declination, Angle)
            and isinstance(geo_latitude, Angle)):
        raise TypeError("Invalid input types")
    dec = declination.rad()
    lat = geo_latitude.rad()
    b = tan(dec) * tan(lat)
    c = sqrt(1.0 - b * b)
    j = atan2(c * cos(dec), tan(lat))
    j = Angle(j, radians=True)
    return j


def times_rise_transit_set(
    longitude,
    latitude,
    alpha1,
    delta1,
    alpha2,
    delta2,
    alpha3,
    delta3,
    h0,
    delta_t,
    theta0,
):
    """This function computes the times (in Universal Time UT) of rising,
    transit and setting of a given celestial body.

    .. note:: If the body is circumpolar there are no rising, transit nor
        setting times. In such a case a tuple with None's is returned

    .. note:: Care must be taken when interpreting the results. For instance,
        if the setting time is **smaller** than the rising time, it means that
        it belongs to the **following** day. Also, if the rising time is
        **bigger** than the setting time, it belong to the **previous** day.
        The same applies to the transit time.

    :param longitude: Geodetic longitude, as an Angle object. It is measured
        positively west from Greenwich, and negatively to the east.
    :type longitude: :py:class:`Angle`
    :param latitude: Geodetic latitude, as an Angle object
    :type latitude: :py:class:`Angle`
    :param alpha1: Apparent right ascension the previous day at 0h TT, as an
        Angle object
    :type alpha1: :py:class:`Angle`
    :param delta1: Apparent declination the previous day at 0h TT, as an Angle
        object
    :type delta1: :py:class:`Angle`
    :param alpha2: Apparent right ascension the current day at 0h TT, as an
        Angle object
    :type alpha2: :py:class:`Angle`
    :param delta2: Apparent declination the current day at 0h TT, as an Angle
        object
    :type delta2: :py:class:`Angle`
    :param alpha3: Apparent right ascension the following day at 0h TT, as an
        Angle object
    :type alpha3: :py:class:`Angle`
    :param delta3: Apparent declination the following day at 0h TT, as an Angle
        object
    :type delta3: :py:class:`Angle`
    :param h0: 'Standard' altitude: the geometric altitude of the center of the
        body at the time of apparent rising or setting, as degrees in an Angle
        object. It should be -0.5667 deg for stars and planets, -0.8333 deg
        for the Sun, and 0.125 deg for the Moon.
    :type h0: :py:class:`Angle`
    :param delta_t: The difference between Terrestrial Time and Universal Time
        (TT - UT) in seconds of time
    :type delta_t: float
    :param theta0: Apparent sidereal time at 0h TT on the current day for the
        meridian of Greenwich, as degrees in an Angle object
    :type theta0: :py:class:`Angle`

    :returns: A tuple with the times of rising, transit and setting, in that
        order, as hours in UT.
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> longitude = Angle(71, 5, 0.0)
    >>> latitude = Angle(42, 20, 0.0)
    >>> alpha1 = Angle(2, 42, 43.25, ra=True)
    >>> delta1 = Angle(18, 2, 51.4)
    >>> alpha2 = Angle(2, 46, 55.51, ra=True)
    >>> delta2 = Angle(18, 26, 27.3)
    >>> alpha3 = Angle(2, 51, 7.69, ra=True)
    >>> delta3 = Angle(18, 49, 38.7)
    >>> h0 = Angle(-0.5667)
    >>> delta_t = 56.0
    >>> theta0 = Angle(11, 50, 58.1, ra=True)
    >>> rising, transit, setting = times_rise_transit_set(longitude, latitude,\
                                                          alpha1, delta1, \
                                                          alpha2, delta2, \
                                                          alpha3, delta3, h0, \
                                                          delta_t, theta0)
    >>> print(round(rising, 4))
    12.4238
    >>> print(round(transit, 3))
    19.675
    >>> print(round(setting, 3))
    2.911
    """

    def check_value(m):
        while m < 0 or m > 1.0:
            if m < 0.0:
                m += 1
            elif m > 1.0:
                m -= 1
        return m

    def interpol(n, y1, y2, y3):
        a = y2 - y1
        b = y3 - y2
        c = b - a
        return y2 + n * (a + b + n * c) / 2.0

    # First check that input values are of correct types
    if not (
        isinstance(longitude, Angle)
        and isinstance(latitude, Angle)
        and isinstance(alpha1, Angle)
        and isinstance(delta1, Angle)
        and isinstance(alpha2, Angle)
        and isinstance(delta2, Angle)
        and isinstance(alpha3, Angle)
        and isinstance(delta3, Angle)
        and isinstance(h0, Angle)
        and isinstance(theta0, Angle)
        and isinstance(delta_t, (int, float))
    ):
        raise TypeError("Invalid input types")
    # Let's start computing approximate times
    h = h0.rad()
    lat = latitude.rad()
    d2 = delta2.rad()
    hh0 = (sin(h) - sin(lat) * sin(d2)) / (cos(lat) * cos(d2))
    # Check if the body is circumpolar. In such case, there are no rising,
    # transit nor setting times, and a tuple with None's is returned
    if abs(hh0) > 1.0:
        return (None, None, None)
    hh0 = acos(hh0)
    hh0 = Angle(hh0, radians=True)
    hh0.to_positive()
    m0 = (alpha2 + longitude - theta0) / 360.0
    m0 = m0()  # m0 is an Angle. Convert to float
    m1 = m0 - hh0() / 360.0
    m2 = m0 + hh0() / 360.0
    m0 = check_value(m0)
    m1 = check_value(m1)
    m2 = check_value(m2)
    # Carry out this procedure twice
    for _ in range(2):
        # Interpolate alpha and delta values for each (m0, m1, m2)
        n = m0 + delta_t / 86400.0
        transit_alpha = interpol(n, alpha1, alpha2, alpha3)
        n = m1 + delta_t / 86400.0
        rise_alpha = interpol(n, alpha1, alpha2, alpha3)
        rise_delta = interpol(n, delta1, delta2, delta3)
        n = m2 + delta_t / 86400.0
        set_alpha = interpol(n, alpha1, alpha2, alpha3)
        set_delta = interpol(n, delta1, delta2, delta3)
        # Compute the hour angles
        theta = theta0 + 360.985647 * m0
        transit_ha = theta - longitude - transit_alpha
        delta_transit = transit_ha / (-360.0)
        theta = theta0 + 360.985647 * m1
        rise_ha = theta - longitude - rise_alpha
        theta = theta0 + 360.985647 * m2
        set_ha = theta - longitude - set_alpha
        # We need the elevations
        azi, rise_ele = equatorial2horizontal(rise_ha, rise_delta, latitude)
        azi, set_ele = equatorial2horizontal(set_ha, set_delta, latitude)
        delta_rise = (rise_ele - h0) / (
            360.0 * cos(rise_delta.rad()) * cos(lat) * sin(rise_ha.rad())
        )
        delta_set = (set_ele - h0) / (
            360.0 * cos(set_delta.rad()) * cos(lat) * sin(set_ha.rad())
        )
        m0 += delta_transit()
        m1 += delta_rise()
        m2 += delta_set()
    return (m1 * 24.0, m0 * 24.0, m2 * 24.0)


def refraction_apparent2true(apparent_elevation, pressure=1010.0,
                             temperature=10.0):
    """This function computes the atmospheric refraction converting from the
    apparent elevation (i.e., the observed elevation through the air) to the
    true, 'airless', elevation.

    .. note:: This function, by default, assumes that the atmospheric pressure
        is 1010 milibars, and the air temperature is 10 Celsius.

    .. note:: Due to the numerous factors that may affect the atmospheric
        refraction, especially near the horizon, the values given by this
        function are approximate values.

    :param apparent_elevation: The elevation, in degrees and as an Angle
        object, of a given celestial object when observed through the
        normal atmosphere
    :type apparent_elevation: :py:class:`Angle`
    :param pressure: Atmospheric pressure at the observation point, in milibars
    :type pressure: float
    :param temperature: Atmospheric temperature at the observation point, in
        degrees Celsius
    :type temperature: :float

    :returns: An Angle object with the true, 'airless' elevation of the
        celestial object
    :rtype: :py:class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> apparent_elevation = Angle(0, 30, 0.0)
    >>> true_elevation = refraction_apparent2true(apparent_elevation)
    >>> print(true_elevation.dms_str(n_dec=1))
    1' 14.7''
    """

    # First check that input values are of correct types
    if not (
        isinstance(apparent_elevation, Angle)
        and isinstance(pressure, (int, float))
        and isinstance(temperature, (int, float))
    ):
        raise TypeError("Invalid input types")
    x = apparent_elevation + 7.31 / (apparent_elevation + 4.4)
    r = 1.0 / tan(x.rad()) + 0.0013515
    r = Angle(r / 60.0)  # The 'r' value is in minutes of arc
    if pressure != 1010.0 or temperature != 10.0:
        r = r * pressure / 1010.0 * 283.0 / (273.0 + temperature)
    return apparent_elevation - r


def refraction_true2apparent(true_elevation, pressure=1010.0,
                             temperature=10.0):
    """This function computes the atmospheric refraction converting from the
    true, 'airless', elevation (i.e., the one computed from celestial
    coordinates) to the apparent elevation (the observed elevation through the
    air)

    .. note:: This function, by default, assumes that the atmospheric pressure
        is 1010 milibars, and the air temperature is 10 Celsius.

    .. note:: Due to the numerous factors that may affect the atmospheric
        refraction, especially near the horizon, the values given by this
        function are approximate values.

    :param true_elevation: The elevation, in degrees and as an Angle
        object, of a given celestial object when computed from celestial
        coordinates, and assuming there is no atmospheric refraction due to the
        air
    :type true_elevation: :py:class:`Angle`
    :param pressure: Atmospheric pressure at the observation point, in milibars
    :type pressure: float
    :param temperature: Atmospheric temperature at the observation point, in
        degrees Celsius
    :type temperature: :float

    :returns: An Angle object with the aparent, 'with air' elevation of the
        celestial object
    :rtype: :py:class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> true_elevation = Angle(0, 33, 14.76)
    >>> apparent_elevation = refraction_true2apparent(true_elevation)
    >>> print(apparent_elevation.dms_str(n_dec=2))
    57' 51.96''
    """

    # First check that input values are of correct types
    if not (
        isinstance(true_elevation, Angle)
        and isinstance(pressure, (int, float))
        and isinstance(temperature, (int, float))
    ):
        raise TypeError("Invalid input types")
    x = true_elevation + 10.3 / (true_elevation + 5.11)
    r = 1.02 / tan(x.rad()) + 0.0019279
    r = Angle(r / 60.0)  # The 'r' value is in minutes of arc
    if pressure != 1010.0 or temperature != 10.0:
        r = r * pressure / 1010.0 * 283.0 / (273.0 + temperature)
    return true_elevation + r


def angular_separation(alpha1, delta1, alpha2, delta2):
    """This function computes the angular distance between two celestial bodies
    whose right ascensions and declinations are given.

    .. note:: It is possible to use this formula with ecliptial (celestial)
        longitudes and latitudes instead of right ascensions and declinations,
        respectively.

    :param alpha1: Right ascension of celestial body #1, as an Angle object
    :type alpha1: :py:class:`Angle`
    :param delta1: Declination of celestial body #1, as an Angle object
    :type delta1: :py:class:`Angle`
    :param alpha2: Right ascension of celestial body #2, as an Angle object
    :type alpha2: :py:class:`Angle`
    :param delta2: Declination of celestial body #2, as an Angle object
    :type delta2: :py:class:`Angle`

    :returns: An Angle object with the angular separation between the given
        celestial objects
    :rtype: :py:class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> alpha1 = Angle(14, 15, 39.7, ra=True)
    >>> delta1 = Angle(19, 10, 57.0)
    >>> alpha2 = Angle(13, 25, 11.6, ra=True)
    >>> delta2 = Angle(-11, 9, 41.0)
    >>> sep_ang = angular_separation(alpha1, delta1, alpha2, delta2)
    >>> print(round(sep_ang, 3))
    32.793
    """

    # Let's define an auxiliary function
    def hav(theta):
        """Function to compute the haversine (hav)"""
        return (1.0 - cos(theta)) / 2.0

    # First check that input values are of correct types
    if not (
        isinstance(alpha1, Angle)
        and isinstance(delta1, Angle)
        and isinstance(alpha2, Angle)
        and isinstance(delta2, Angle)
    ):
        raise TypeError("Invalid input types")
    dalpha = alpha1 - alpha2
    dalpha = dalpha.rad()
    ddelta = delta1 - delta2
    ddelta = ddelta.rad()
    d1 = delta1.rad()
    d2 = delta2.rad()
    theta = 2.0 * asin(sqrt(hav(ddelta) + cos(d1) * cos(d2) * hav(dalpha)))
    theta = Angle(theta, radians=True)
    return theta


def minimum_angular_separation(
    alpha1_1,
    delta1_1,
    alpha1_2,
    delta1_2,
    alpha1_3,
    delta1_3,
    alpha2_1,
    delta2_1,
    alpha2_2,
    delta2_2,
    alpha2_3,
    delta2_3,
):
    """Given the positions at three different instants of times (equidistant)
    of two celestial objects, this function computes the minimum angular
    distance that will be achieved within that interval of time.

    .. note:: Suffix '1 _' is for the first celestial object, and '2 _' is for
        the second one.

    .. note:: This function provides as output the 'n' fraction of time when
        the minimum angular separation is achieved. For that, the epoch in the
        middle is assigned the value "n = 0". Therefore, n < 0 is for times
        **before** the middle epoch, and n > 0 is for times **after** the
        middle epoch.

    :param alpha1_1: First right ascension of celestial body #1, as an Angle
        object
    :type alpha1_1: :py:class:`Angle`
    :param delta1_1: First declination of celestial body #1, as an Angle object
    :type delta1_1: :py:class:`Angle`
    :param alpha1_2: Second right ascension of celestial body #1, as an Angle
        object
    :type alpha1_2: :py:class:`Angle`
    :param delta1_2: Second declination of celestial body #1, as Angle object
    :type delta1_2: :py:class:`Angle`
    :param alpha1_3: Third right ascension of celestial body #1, as an Angle
        object
    :type alpha1_3: :py:class:`Angle`
    :param delta1_3: Third declination of celestial body #1, as an Angle object
    :type delta1_3: :py:class:`Angle`
    :param alpha2_1: First right ascension of celestial body #2, as an Angle
        object
    :type alpha2_1: :py:class:`Angle`
    :param delta2_1: First declination of celestial body #2, as an Angle object
    :type delta2_1: :py:class:`Angle`
    :param alpha2_2: Second right ascension of celestial body #2, as an Angle
        object
    :type alpha2_2: :py:class:`Angle`
    :param delta2_2: Second declination of celestial body #2, as Angle object
    :type delta2_2: :py:class:`Angle`
    :param alpha2_3: Third right ascension of celestial body #2, as an Angle
        object
    :type alpha2_3: :py:class:`Angle`
    :param delta2_3: Third declination of celestial body #2, as an Angle object
    :type delta2_3: :py:class:`Angle`

    :returns: A tuple with two components: The first component is a float
        containing the 'n' fraction of time when the minimum angular separation
        is achieved. The second component is an Angle object containing the
        minimum angular separation between the given celestial objects
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> alpha1_1 = Angle(10, 29, 44.27, ra=True)
    >>> delta1_1 = Angle(11, 2, 5.9)
    >>> alpha2_1 = Angle(10, 33, 29.64, ra=True)
    >>> delta2_1 = Angle(10, 40, 13.2)
    >>> alpha1_2 = Angle(10, 36, 19.63, ra=True)
    >>> delta1_2 = Angle(10, 29, 51.7)
    >>> alpha2_2 = Angle(10, 33, 57.97, ra=True)
    >>> delta2_2 = Angle(10, 37, 33.4)
    >>> alpha1_3 = Angle(10, 43, 1.75, ra=True)
    >>> delta1_3 = Angle(9, 55, 16.7)
    >>> alpha2_3 = Angle(10, 34, 26.22, ra=True)
    >>> delta2_3 = Angle(10, 34, 53.9)
    >>> a = minimum_angular_separation(alpha1_1, delta1_1, alpha1_2, delta1_2,\
                                       alpha1_3, delta1_3, alpha2_1, delta2_1,\
                                       alpha2_2, delta2_2, alpha2_3, delta2_3)
    >>> print(round(a[0], 6))
    -0.370726
    >>> print(a[1].dms_str(n_dec=0))
    3' 44.0''
    """

    # Let's define some auxiliary functions
    def k_factor(d1, d_a):
        """This auxiliary function returns arcseconds, input is in radians"""
        return (206264.8062 / (1.0 + sin(d1) * sin(d1)
                               * tan(d_a) * tan(d_a / 2.0)))

    def u_factor(k, d1, d_a, d_d):
        """Input is in radians, except for k (arcseconds)"""
        return -k * (1.0 - tan(d1) * sin(d_d)) * cos(d1) * tan(d_a)

    def v_factor(k, d1, d_a, d_d):
        """Input is in radians, except for k (arcseconds)"""
        return k * (sin(d_d) + sin(d1) * cos(d1) * tan(d_a) * tan(d_a / 2.0))

    def u_prime(n, u1, u2, u3):
        return (u3 - u1) / 2.0 + n * (u1 + u3 - 2.0 * u2)

    def delta_n(u, u_p, v, v_p):
        return -(u * u_p + v * v_p) / (u_p * u_p + v_p * v_p)

    def interpol(n, y1, y2, y3):
        """This is formula 3.3 from Meeus book"""
        a = y2 - y1
        b = y3 - y2
        c = b - a
        return y2 + n * (a + b + n * c) / 2.0

    # First check that input values are of correct types
    if not (
        isinstance(alpha1_1, Angle)
        and isinstance(delta1_1, Angle)
        and isinstance(alpha1_2, Angle)
        and isinstance(delta1_2, Angle)
        and isinstance(alpha1_3, Angle)
        and isinstance(delta1_3, Angle)
        and isinstance(alpha2_1, Angle)
        and isinstance(delta2_1, Angle)
        and isinstance(alpha2_2, Angle)
        and isinstance(delta2_2, Angle)
        and isinstance(alpha2_3, Angle)
        and isinstance(delta2_3, Angle)
    ):
        raise TypeError("Invalid input types")
    # Let's define two dictionaries to store the intermediate results
    u = {}
    v = {}
    # Compute intermediate results for first epoch
    d1 = delta1_1.rad()
    d_a = alpha2_1.rad() - alpha1_1.rad()
    d_d = delta2_1.rad() - delta1_1.rad()
    k = k_factor(d1, d_a)
    u[1] = u_factor(k, d1, d_a, d_d)
    v[1] = v_factor(k, d1, d_a, d_d)
    # Compute intermediate results for second epoch
    d1 = delta1_2.rad()
    d_a = alpha2_2.rad() - alpha1_2.rad()
    d_d = delta2_2.rad() - delta1_2.rad()
    k = k_factor(d1, d_a)
    u[2] = u_factor(k, d1, d_a, d_d)
    v[2] = v_factor(k, d1, d_a, d_d)
    # Compute intermediate results for third epoch
    d1 = delta1_3.rad()
    d_a = alpha2_3.rad() - alpha1_3.rad()
    d_d = delta2_3.rad() - delta1_3.rad()
    k = k_factor(d1, d_a)
    u[3] = u_factor(k, d1, d_a, d_d)
    v[3] = v_factor(k, d1, d_a, d_d)
    # Iterate to find the solution
    n = 0.0
    dn = 999999.9
    while abs(dn) > 0.000001:
        uu = interpol(n, u[1], u[2], u[3])
        vv = interpol(n, v[1], v[2], v[3])
        up = u_prime(n, u[1], u[2], u[3])
        vp = u_prime(n, v[1], v[2], v[3])
        dn = delta_n(uu, up, vv, vp)
        n += dn
    # Let's compute the minimum distance, in arcseconds
    arcsec = sqrt(uu * uu + vv * vv)
    d = Angle(0, 0, arcsec)  # Convert to an Angle object
    return n, d


def relative_position_angle(alpha1, delta1, alpha2, delta2):
    """This function computes the position angle P of a body with respect to
    another body.

    :param alpha1: Right ascension of celestial body #1, as an Angle object
    :type alpha1: :py:class:`Angle`
    :param delta1: Declination of celestial body #1, as an Angle object
    :type delta1: :py:class:`Angle`
    :param alpha2: Right ascension of celestial body #2, as an Angle object
    :type alpha2: :py:class:`Angle`
    :param delta2: Declination of celestial body #2, as an Angle object
    :type delta2: :py:class:`Angle`

    :returns: An Angle object with the relative position angle between the
        given celestial objects
    :rtype: :py:class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> alpha1 = Angle(14, 15, 39.7, ra=True)
    >>> delta1 = Angle(19, 10, 57.0)
    >>> alpha2 = Angle(14, 15, 39.7, ra=True)
    >>> delta2 = Angle(-11, 9, 41.0)
    >>> pos_ang = relative_position_angle(alpha1, delta1, alpha2, delta2)
    >>> print(round(pos_ang, 1))
    0.0
    """

    # First check that input values are of correct types
    if not (
        isinstance(alpha1, Angle)
        and isinstance(delta1, Angle)
        and isinstance(alpha2, Angle)
        and isinstance(delta2, Angle)
    ):
        raise TypeError("Invalid input types")
    da = alpha1 - alpha2
    da = da.rad()
    d1 = delta1.rad()
    d2 = delta2.rad()
    p = atan2(sin(da), (cos(d2) * tan(d1) - sin(d2) * cos(da)))
    p = Angle(p, radians=True)
    return p


def planetary_conjunction(alpha1_list, delta1_list, alpha2_list, delta2_list):
    """Given the positions of two planets passing near each other, this
    function computes the time of conjunction in right ascension, and the
    difference in declination of the two bodies at that time.

    .. note:: This function provides as output the 'n' fraction of time when
        the minimum angular separation is achieved. For that, the epoch in the
        middle is assigned the value "n = 0". Therefore, n < 0 is for times
        **before** the middle epoch, and n > 0 is for times **after** the
        middle epoch.

    .. note:: When the entries in the input values are more than three and
        even, the last entry is discarted and an odd number of entries will be
        used.

    :param alpha1_list: List (or tuple) containing the right ascensions (as
        Angle objects) for object #1 (minimum 3 entries)
    :type alpha1_list: list, tuple of :py:class:`Angle`
    :param delta1_list: List (or tuple) containing the declinations (as Angle
        objects) for object #1 (minimum 3 entries)
    :type delta1_list: list, tuple of :py:class:`Angle`
    :param alpha2_list: List (or tuple) containing the right ascensions (as
        Angle objects) for object #2 (minimum 3 entries)
    :type alpha2_list: list, tuple of :py:class:`Angle`
    :param delta2_list: List (or tuple) containing the declinations (as Angle
        objects) for object #2 (minimum 3 entries)
    :type delta2_list: list, tuple of :py:class:`Angle`

    :returns: A tuple with two components: The first component is a float
        containing the 'n' fraction of time when the conjunction occurs. The
        second component is an Angle object containing the declination
        separation between the given objects at conjunction epoch
    :rtype: tuple
    :raises: ValueError if input values have less than three entries or they
        don't have the same number of entries.
    :raises: TypeError if input values are of wrong type.

    >>> alpha1_1 = Angle(10, 24, 30.125, ra=True)
    >>> delta1_1 = Angle( 6, 26, 32.05)
    >>> alpha1_2 = Angle(10, 25,  0.342, ra=True)
    >>> delta1_2 = Angle( 6, 10, 57.72)
    >>> alpha1_3 = Angle(10, 25, 12.515, ra=True)
    >>> delta1_3 = Angle( 5, 57, 33.08)
    >>> alpha1_4 = Angle(10, 25,  6.235, ra=True)
    >>> delta1_4 = Angle( 5, 46, 27.07)
    >>> alpha1_5 = Angle(10, 24, 41.185, ra=True)
    >>> delta1_5 = Angle( 5, 37, 48.45)
    >>> alpha2_1 = Angle(10, 27, 27.175, ra=True)
    >>> delta2_1 = Angle( 4,  4, 41.83)
    >>> alpha2_2 = Angle(10, 26, 32.410, ra=True)
    >>> delta2_2 = Angle( 3, 55, 54.66)
    >>> alpha2_3 = Angle(10, 25, 29.042, ra=True)
    >>> delta2_3 = Angle( 3, 48,  3.51)
    >>> alpha2_4 = Angle(10, 24, 17.191, ra=True)
    >>> delta2_4 = Angle( 3, 41, 10.25)
    >>> alpha2_5 = Angle(10, 22, 57.024, ra=True)
    >>> delta2_5 = Angle( 3, 35, 16.61)
    >>> alpha1_list = [alpha1_1, alpha1_2, alpha1_3, alpha1_4, alpha1_5]
    >>> delta1_list = [delta1_1, delta1_2, delta1_3, delta1_4, delta1_5]
    >>> alpha2_list = [alpha2_1, alpha2_2, alpha2_3, alpha2_4, alpha2_5]
    >>> delta2_list = [delta2_1, delta2_2, delta2_3, delta2_4, delta2_5]
    >>> pc = planetary_conjunction(alpha1_list, delta1_list, \
                                   alpha2_list, delta2_list)
    >>> print(round(pc[0], 5))
    0.23797
    >>> print(pc[1].dms_str(n_dec=1))
    2d 8' 21.8''
    """

    # First check that input values are of correct types
    if not (
        isinstance(alpha1_list, (list, tuple))
        and isinstance(delta1_list, (list, tuple))
        and isinstance(alpha2_list, (list, tuple))
        and isinstance(delta2_list, (list, tuple))
    ):
        raise TypeError("Invalid input types")
    if (
        len(alpha1_list) < 3
        or len(delta1_list) < 3
        or len(alpha2_list) < 3
        or len(delta2_list) < 3
    ):
        raise ValueError("Invalid number of entries")
    if (
        len(alpha1_list) != len(delta1_list)
        or len(alpha1_list) != len(alpha2_list)
        or len(alpha1_list) != len(delta2_list)
    ):
        raise ValueError("Uneven number of entries")
    n_entries = len(alpha1_list)
    if n_entries % 2 != 1:  # Check if number of entries is odd
        alpha1_list = alpha1_list[:-1]  # Drop the last entry
        delta1_list = delta1_list[:-1]
        alpha2_list = alpha2_list[:-1]
        delta2_list = delta2_list[:-1]
        n_entries = len(alpha1_list)
    half_entries = n_entries // 2
    # Compute the list with the time ('n') entries
    n_list = [i - half_entries for i in range(n_entries)]
    # Compute lists with differences between right ascensions and declinations
    # for objects #1 and #2
    dalpha = [alpha1_list[i] - alpha2_list[i] for i in range(n_entries)]
    ddelta = [delta1_list[i] - delta2_list[i] for i in range(n_entries)]
    # Build the interpolation objects
    i_alpha = Interpolation(n_list, dalpha)
    i_delta = Interpolation(n_list, ddelta)
    # Find when the dalphas are 0 (i.e., the 'root')
    n_0 = i_alpha.root()
    # Now, let's find the declination difference with the newly found 'n_0'
    dd = i_delta(n_0)
    # We are done, let's return
    return n_0, dd


def planet_star_conjunction(alpha_list, delta_list, alpha_star, delta_star):
    """Given the positions of one planet passing near a star, this function
    computes the time of conjunction in right ascension, and the difference in
    declination of the two bodies at that time.

    .. note:: This function provides as output the 'n' fraction of time when
        the minimum angular separation is achieved. For that, the epoch in the
        middle is assigned the value "n = 0". Therefore, n < 0 is for times
        **before** the middle epoch, and n > 0 is for times **after** the
        middle epoch.

    .. note:: When the entries in the input values for the planet are more than
        three and pair, the last entry is discarted and an odd number of
        entries will be used.

    :param alpha_list: List (or tuple) containing the right ascensions (as
        Angle objects) for the planet (minimum 3 entries)
    :type alpha_list: list, tuple of :py:class:`Angle`
    :param delta_list: List (or tuple) containing the declinations (as Angle
        objects) for the planet (minimum 3 entries)
    :type delta_list: list, tuple of :py:class:`Angle`
    :param alpha_star: Right ascension, as an Angle object, of the star
    :type alpha_star: :py:class:`Angle`
    :param delta_star: Declination, as an Angle object, of the star
    :type delta_star: :py:class:`Angle`

    :returns: A tuple with two components: The first component is a float
        containing the 'n' fraction of time when the conjunction occurs. The
        second component is an Angle object containing the declination
        separation between the given objects at conjunction epoch
    :rtype: tuple
    :raises: ValueError if input values for planet have less than three entries
        or they don't have the same number of entries.
    :raises: TypeError if input values are of wrong type.

    >>> alpha_1 = Angle(15,  3, 51.937, ra=True)
    >>> delta_1 = Angle(-8, 57, 34.51)
    >>> alpha_2 = Angle(15,  9, 57.327, ra=True)
    >>> delta_2 = Angle(-9,  9,  3.88)
    >>> alpha_3 = Angle(15, 15, 37.898, ra=True)
    >>> delta_3 = Angle(-9, 17, 37.94)
    >>> alpha_4 = Angle(15, 20, 50.632, ra=True)
    >>> delta_4 = Angle(-9, 23, 16.25)
    >>> alpha_5 = Angle(15, 25, 32.695, ra=True)
    >>> delta_5 = Angle(-9, 26,  1.01)
    >>> alpha_star = Angle(15, 17, 0.446, ra=True)
    >>> delta_star = Angle(-9, 22, 58.47)
    >>> alpha_list = [alpha_1, alpha_2, alpha_3, alpha_4, alpha_5]
    >>> delta_list = [delta_1, delta_2, delta_3, delta_4, delta_5]
    >>> pc = planet_star_conjunction(alpha_list, delta_list, \
                                     alpha_star, delta_star)
    >>> print(round(pc[0], 4))
    0.2551
    >>> print(pc[1].dms_str(n_dec=0))
    3' 38.0''
    """

    # Build the corresponding lists for the star
    n_entries = len(alpha_list)
    alpha_star_list = [alpha_star for _ in range(n_entries)]
    delta_star_list = [delta_star for _ in range(n_entries)]
    # Call the 'planetary_conjunction()' function. It handles everything else
    return planetary_conjunction(
        alpha_list, delta_list, alpha_star_list, delta_star_list
    )


def planet_stars_in_line(
    alpha_list, delta_list, alpha_star1, delta_star1, alpha_star2, delta_star2
):
    """Given the positions of one planet, this function computes the time when
    it is in a straight line with two other stars.

    .. note:: This function provides as output the 'n' fraction of time when
        the minimum angular separation is achieved. For that, the epoch in the
        middle is assigned the value "n = 0". Therefore, n < 0 is for times
        **before** the middle epoch, and n > 0 is for times **after** the
        middle epoch.

    .. note:: When the entries in the input values for the planet are more than
        three and pair, the last entry is discarted and an odd number of
        entries will be used.

    :param alpha_list: List (or tuple) containing the right ascensions (as
        Angle objects) for the planet (minimum 3 entries)
    :type alpha_list: list, tuple of :py:class:`Angle`
    :param delta_list: List (or tuple) containing the declinations (as Angle
        objects) for the planet (minimum 3 entries)
    :type delta_list: list, tuple of :py:class:`Angle`
    :param alpha_star1: Right ascension, as an Angle object, of star #1
    :type alpha_star1: :py:class:`Angle`
    :param delta_star1: Declination, as an Angle object, of star #1
    :type delta_star1: :py:class:`Angle`
    :param alpha_star2: Right ascension, as an Angle object, of star #2
    :type alpha_star2: :py:class:`Angle`
    :param delta_star2: Declination, as an Angle object, of star #2
    :type delta_star2: :py:class:`Angle`

    :returns: A float containing the 'n' fraction of time when the alignment
        occurs.
    :rtype: float
    :raises: ValueError if input values for planet have less than three entries
        or they don't have the same number of entries.
    :raises: TypeError if input values are of wrong type.

    >>> alpha_1 = Angle( 7, 55, 55.36, ra=True)
    >>> delta_1 = Angle(21, 41,  3.0)
    >>> alpha_2 = Angle( 7, 58, 22.55, ra=True)
    >>> delta_2 = Angle(21, 35, 23.4)
    >>> alpha_3 = Angle( 8,  0, 48.99, ra=True)
    >>> delta_3 = Angle(21, 29, 38.2)
    >>> alpha_4 = Angle( 8,  3, 14.66, ra=True)
    >>> delta_4 = Angle(21, 23, 47.5)
    >>> alpha_5 = Angle( 8,  5, 39.54, ra=True)
    >>> delta_5 = Angle(21, 17, 51.4)
    >>> alpha_star1 = Angle( 7, 34, 16.40, ra=True)
    >>> delta_star1 = Angle(31, 53, 51.2)
    >>> alpha_star2 = Angle( 7, 45,  0.10, ra=True)
    >>> delta_star2 = Angle(28,  2, 12.5)
    >>> alpha_list = [alpha_1, alpha_2, alpha_3, alpha_4, alpha_5]
    >>> delta_list = [delta_1, delta_2, delta_3, delta_4, delta_5]
    >>> n = planet_stars_in_line(alpha_list, delta_list, alpha_star1, \
                                 delta_star1, alpha_star2, delta_star2)
    >>> print(round(n, 4))
    0.2233
    """

    # Define an auxiliary function
    def straight(alpha1, delta1, alpha2, delta2, alpha3, delta3):
        a1 = alpha1.rad()
        d1 = delta1.rad()
        a2 = alpha2.rad()
        d2 = delta2.rad()
        a3 = alpha3.rad()
        d3 = delta3.rad()
        return (tan(d1) * sin(a2 - a3) + tan(d2) * sin(a3 - a1)
                + tan(d3) * sin(a1 - a2))

    # First check that input values are of correct types
    if not (
        isinstance(alpha_list, (list, tuple))
        and isinstance(delta_list, (list, tuple))
        and isinstance(alpha_star1, Angle)
        and isinstance(delta_star1, Angle)
        and isinstance(alpha_star2, Angle)
        and isinstance(delta_star2, Angle)
    ):
        raise TypeError("Invalid input types")
    if len(alpha_list) < 3 or len(delta_list) < 3:
        raise ValueError("Invalid number of entries")
    if len(alpha_list) != len(delta_list):
        raise ValueError("Uneven number of entries")
    n_entries = len(alpha_list)
    if n_entries % 2 != 1:  # Check if number of entries is odd
        alpha_list = alpha_list[:-1]  # Drop the last entry
        delta_list = delta_list[:-1]
        n_entries = len(alpha_list)
    half_entries = n_entries // 2
    # Compute the list with the time ('n') entries
    n_list = [i - half_entries for i in range(n_entries)]
    # Use auxiliary function 'straight()' to compute the values to interpolate
    dx = [
        straight(
            alpha_list[i],
            delta_list[i],
            alpha_star1,
            delta_star1,
            alpha_star2,
            delta_star2,
        )
        for i in range(n_entries)
    ]
    # Build the interpolation objects
    i = Interpolation(n_list, dx)
    # Find when the dx's are 0 (i.e., the 'root')
    n_0 = i.root()
    return n_0


def straight_line(alpha1, delta1, alpha2, delta2, alpha3, delta3):
    """This function computes if three celestial bodies are in a straight line,
    providing the angle with which the bodies differ from a great circle.

    :param alpha1: Right ascension, as an Angle object, of celestial body #1
    :type alpha1: :py:class:`Angle`
    :param delta1: Declination, as an Angle object, of celestial body #1
    :type delta1: :py:class:`Angle`
    :param alpha2: Right ascension, as an Angle object, of celestial body #2
    :type alpha2: :py:class:`Angle`
    :param delta2: Declination, as an Angle object, of celestial body #2
    :type delta2: :py:class:`Angle`
    :param alpha3: Right ascension, as an Angle object, of celestial body #3
    :type alpha3: :py:class:`Angle`
    :param delta3: Declination, as an Angle object, of celestial body #3
    :type delta3: :py:class:`Angle`

    :returns: A tuple with two components. The first element is an angle (as
        Angle object) with which the bodies differ from a great circle. The
        second element is the Angular distance of central point to the straight
        line (also as Angle object).
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> alpha1 = Angle( 5, 32,  0.40, ra=True)
    >>> delta1 = Angle(0, -17, 56.9)
    >>> alpha2 = Angle( 5, 36, 12.81, ra=True)
    >>> delta2 = Angle(-1, 12,  7.0)
    >>> alpha3 = Angle( 5, 40, 45.52, ra=True)
    >>> delta3 = Angle(-1, 56, 33.3)
    >>> psi, om = straight_line(alpha1, delta1, alpha2, delta2, alpha3, delta3)
    >>> print(psi.dms_str(n_dec=0))
    7d 31' 1.0''
    >>> print(om.dms_str(n_dec=0))
    -5' 24.0''
    """

    # First check that input values are of correct types
    if not (
        isinstance(alpha1, Angle)
        and isinstance(delta1, Angle)
        and isinstance(alpha2, Angle)
        and isinstance(delta2, Angle)
        and isinstance(alpha3, Angle)
        and isinstance(delta3, Angle)
    ):
        raise TypeError("Invalid input types")
    # We need to order the input according to right ascension
    a = [alpha1.rad(), alpha2.rad(), alpha3.rad()]
    d = [delta1.rad(), delta2.rad(), delta3.rad()]
    anew = []
    dnew = []
    amax = max(a) + 1.0
    for _ in range(len(a)):
        # Get the index of the minimum value
        imin = a.index(min(a))
        # Append the current minimum value to the new 'a' list
        anew.append(a[imin])
        # Store the *position* of the current minimum value to new 'd' list
        dnew.append(imin)
        # The current minimum value will no longer be the minimum
        a[imin] = amax

    # In the new 'd' list, substitute the positions by the real values
    for i in range(len(a)):
        dnew[i] = d[dnew[i]]
    # Substitute the new values in the original list
    a = anew
    d = dnew
    # Compute the parameters
    a1 = cos(d[0]) * cos(a[0])
    a2 = cos(d[1]) * cos(a[1])
    a3 = cos(d[2]) * cos(a[2])
    b1 = cos(d[0]) * sin(a[0])
    b2 = cos(d[1]) * sin(a[1])
    b3 = cos(d[2]) * sin(a[2])
    c1 = sin(d[0])
    c2 = sin(d[1])
    c3 = sin(d[2])
    l1 = b1 * c2 - b2 * c1
    l2 = b2 * c3 - b3 * c2
    l3 = b1 * c3 - b3 * c1
    m1 = c1 * a2 - c2 * a1
    m2 = c2 * a3 - c3 * a2
    m3 = c1 * a3 - c3 * a1
    n1 = a1 * b2 - a2 * b1
    n2 = a2 * b3 - a3 * b2
    n3 = a1 * b3 - a3 * b1
    psi = acos(
        (l1 * l2 + m1 * m2 + n1 * n2)
        / (sqrt(l1 * l1 + m1 * m1 + n1 * n1)
           * sqrt(l2 * l2 + m2 * m2 + n2 * n2)))
    omega = asin(
        (a2 * l3 + b2 * m3 + c2 * n3)
        / (sqrt(a2 * a2 + b2 * b2 + c2 * c2)
           * sqrt(l3 * l3 + m3 * m3 + n3 * n3)))
    return Angle(psi, radians=True), Angle(omega, radians=True)


def circle_diameter(alpha1, delta1, alpha2, delta2, alpha3, delta3):
    """This function computes the diameter of the smallest circle that contains
    three celestial bodies.

    :param alpha1: Right ascension, as an Angle object, of celestial body #1
    :type alpha1: :py:class:`Angle`
    :param delta1: Declination, as an Angle object, of celestial body #1
    :type delta1: :py:class:`Angle`
    :param alpha2: Right ascension, as an Angle object, of celestial body #2
    :type alpha2: :py:class:`Angle`
    :param delta2: Declination, as an Angle object, of celestial body #2
    :type delta2: :py:class:`Angle`
    :param alpha3: Right ascension, as an Angle object, of celestial body #3
    :type alpha3: :py:class:`Angle`
    :param delta3: Declination, as an Angle object, of celestial body #3
    :type delta3: :py:class:`Angle`

    :returns: The diameter (as an Angle object) of the smallest circle
        containing the three bodies.
    :rtype: :py:class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> alpha1 = Angle(12, 41,  8.63, ra=True)
    >>> delta1 = Angle(-5, 37, 54.2)
    >>> alpha2 = Angle(12, 52,  5.21, ra=True)
    >>> delta2 = Angle(-4, 22, 26.2)
    >>> alpha3 = Angle(12, 39, 28.11, ra=True)
    >>> delta3 = Angle(-1, 50,  3.7)
    >>> d = circle_diameter(alpha1, delta1, alpha2, delta2, alpha3, delta3)
    >>> print(d.dms_str(n_dec=0))
    4d 15' 49.0''
    >>> alpha1 = Angle(9,  5, 41.44, ra=True)
    >>> delta1 = Angle(18, 30, 30.0)
    >>> alpha2 = Angle(9,  9, 29.0, ra=True)
    >>> delta2 = Angle(17, 43, 56.7)
    >>> alpha3 = Angle(8, 59, 47.14, ra=True)
    >>> delta3 = Angle(17, 49, 36.8)
    >>> d = circle_diameter(alpha1, delta1, alpha2, delta2, alpha3, delta3)
    >>> print(d.dms_str(n_dec=0))
    2d 18' 38.0''
    """

    # First check that input values are of correct types
    if not (
        isinstance(alpha1, Angle)
        and isinstance(delta1, Angle)
        and isinstance(alpha2, Angle)
        and isinstance(delta2, Angle)
        and isinstance(alpha3, Angle)
        and isinstance(delta3, Angle)
    ):
        raise TypeError("Invalid input types")
    d12 = angular_separation(alpha1, delta1, alpha2, delta2)
    d13 = angular_separation(alpha1, delta1, alpha3, delta3)
    d23 = angular_separation(alpha2, delta2, alpha3, delta3)
    if d12 >= d13 and d12 >= d23:
        a = d12()
        b = d13()
        c = d23()
    elif d13 >= d12 and d13 >= d23:
        a = d13()
        b = d12()
        c = d23()
    else:
        a = d23()
        b = d12()
        c = d13()
    if a >= sqrt(b * b + c * c):
        d = a
    else:
        d = (2.0 * a * b * c) / sqrt(
            (a + b + c) * (a + b - c) * (b + c - a) * (a + c - b)
        )
    return Angle(d)


def vsop_pos(epoch, vsop_l, vsop_b, vsop_r):
    """This function computes the position of a celestial body at a given epoch
    when its VSOP87 periodic term tables are provided.

    :param epoch: Epoch to compute the position, given as an :class:`Epoch`
        object
    :type epoch: :py:class:`Epoch`
    :param vsop_l: Table of VSOP87 terms for the heliocentric longitude
    :type vsop_l: list
    :param vsop_b: Table of VSOP87 terms for the heliocentric latitude
    :type vsop_b: list
    :param vsop_r: Table of VSOP87 terms for the radius vector
    :type vsop_r: list

    :returns: A tuple with the heliocentric longitude and latitude (as
        :py:class:`Angle` objects), and the radius vector (as a float,
        in astronomical units), in that order
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.
    """

    # First check that input values are of correct types
    if not (
        isinstance(epoch, Epoch)
        and isinstance(vsop_l, list)
        and isinstance(vsop_b, list)
        and isinstance(vsop_r, list)
    ):
        raise TypeError("Invalid input types")
    # Let's redefine u in units of 100 Julian centuries from Epoch J2000.0
    t = (epoch.jde() - 2451545.0) / 365250.0
    sum_list = []
    for i in range(len(vsop_l)):
        s = 0.0
        for k in range(len(vsop_l[i])):
            s += vsop_l[i][k][0] * cos(vsop_l[i][k][1] + vsop_l[i][k][2] * t)
        sum_list.append(s)
    lon = 0.0
    # Sum the longitude terms, while multiplying by 't' at the same time
    for i in range(len(sum_list) - 1, 0, -1):
        lon = (lon + sum_list[i]) * t
    # Add the L0 term, which is NOT multiplied by 't'
    lon += sum_list[0]
    lon /= 1e8
    lon = Angle(lon, radians=True)
    lon = lon.to_positive()
    sum_list = []
    for i in range(len(vsop_b)):
        s = 0.0
        for k in range(len(vsop_b[i])):
            s += vsop_b[i][k][0] * cos(vsop_b[i][k][1] + vsop_b[i][k][2] * t)
        sum_list.append(s)
    lat = 0.0
    # Sum the latitude terms, while multiplying by 't' at the same time
    for i in range(len(sum_list) - 1, 0, -1):
        lat = (lat + sum_list[i]) * t
    # Add the B0 term, which is NOT multiplied by 't'
    lat += sum_list[0]
    lat /= 1e8
    lat = Angle(lat, radians=True)
    sum_list = []
    for i in range(len(vsop_r)):
        s = 0.0
        for k in range(len(vsop_r[i])):
            s += vsop_r[i][k][0] * cos(vsop_r[i][k][1] + vsop_r[i][k][2] * t)
        sum_list.append(s)
    r = 0.0
    # Sum the radius vector terms, while multiplying by 't' at the same time
    for i in range(len(sum_list) - 1, 0, -1):
        r = (r + sum_list[i]) * t
    # Add the R0 term, which is NOT multiplied by 't'
    r += sum_list[0]
    r /= 1e8
    return (lon, lat, r)


def geometric_vsop_pos(epoch, vsop_l, vsop_b, vsop_r, tofk5=True):
    """This function computes the geometric position of a celestial body at a
    given epoch when its VSOP87 periodic term tables are provided. The small
    correction to convert to the FK5 system may or not be included.

    :param epoch: Epoch to compute the position, given as an :class:`Epoch`
        object
    :type epoch: :py:class:`Epoch`
    :param vsop_l: Table of VSOP87 terms for the heliocentric longitude
    :type vsop_l: list
    :param vsop_b: Table of VSOP87 terms for the heliocentric latitude
    :type vsop_b: list
    :param vsop_r: Table of VSOP87 terms for the radius vector
    :type vsop_r: list
    :param tofk5: Whether or not the small correction to convert to the FK5
        system will be applied
    :type tofk5: bool

    :returns: A tuple with the geometric heliocentric longitude and latitude
        (as :py:class:`Angle` objects), and the radius vector (as a float,
        in astronomical units), in that order
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.
    """

    # First check that input values are of correct types
    if not isinstance(epoch, Epoch):
        raise TypeError("Invalid input types")
    # Second, call the auxiliary function in charge of computations
    lon, lat, r = vsop_pos(epoch, vsop_l, vsop_b, vsop_r)
    if tofk5:
        # Apply the small correction for conversion to the FK5 system
        t = (epoch.jde() - 2451545.0) / 36525.0
        lambda_p = lon - t * (1.397 + 0.00031 * t)
        delta_lon = Angle(0, 0, -0.09033)
        a = 0.03916 * (cos(lambda_p.rad()) + sin(lambda_p.rad()))
        a = a * tan(lat.rad())
        delta_lon += Angle(0, 0, a)
        delta_beta = 0.03916 * (cos(lambda_p.rad()) - sin(lambda_p.rad()))
        delta_beta = Angle(0, 0, delta_beta)
        lon += delta_lon
        lat += delta_beta
    return lon, lat, r


def apparent_vsop_pos(epoch, vsop_l, vsop_b, vsop_r, nutation=True):
    """This function computes the apparent position of a celestial body at a
    given epoch when its VSOP87 periodic term tables are provided. The small
    correction to convert to the FK5 system is always included.

    :param epoch: Epoch to compute the position, given as an :class:`Epoch`
        object
    :type epoch: :py:class:`Epoch`
    :param vsop_l: Table of VSOP87 terms for the heliocentric longitude
    :type vsop_l: list
    :param vsop_b: Table of VSOP87 terms for the heliocentric latitude
    :type vsop_b: list
    :param vsop_r: Table of VSOP87 terms for the radius vector
    :type vsop_r: list
    :param nutation: Whether the nutation correction will be applied
    :type tofk5: bool

    :returns: A tuple with the geometric heliocentric longitude and latitude
        (as :py:class:`Angle` objects), and the radius vector (as a float,
        in astronomical units), in that order
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.
    """

    # First check that input values are of correct types
    if not isinstance(epoch, Epoch):
        raise TypeError("Invalid input types")
    # Second, call auxiliary function in charge of computations
    lon, lat, r = geometric_vsop_pos(epoch, vsop_l, vsop_b, vsop_r)
    if nutation:
        lon += nutation_longitude(epoch)
    delta = -20.4898 / r
    delta = Angle(0, 0, delta)
    lon += delta
    return lon, lat, r


def apparent_position(epoch, alpha, delta, sun_lon):
    """This function computes the apparent position of a star, correcting by
    nutation and aberration effects.

    :param epoch: Epoch to compute the apparent position for
    :type epoch: :py:class:`Epoch`
    :param alpha: Right ascension of the star, as an Angle object
    :type alpha: :py:class:`Angle`
    :param delta: Declination of the star, as an Angle object
    :type delta: :py:class:`Angle`
    :param sun_lon: True (geometric) longitude of the Sun
    :type sun_lon: :py:class:`Angle`

    :returns: A tuple with two Angle objects: Apparent right ascension, and
        aparent declination
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> epoch = Epoch(2028, 11, 13.19)
    >>> alpha = Angle(2, 46, 11.331, ra=True)
    >>> delta = Angle(49, 20, 54.54)
    >>> sun_lon = Angle(231.328)
    >>> app_alpha, app_delta = apparent_position(epoch, alpha, delta, sun_lon)
    >>> print(app_alpha.ra_str(n_dec=2))
    2h 46' 14.39''
    >>> print(app_delta.dms_str(n_dec=2))
    49d 21' 7.45''
    """

    # First check that input values are of correct types
    if not (
        isinstance(epoch, Epoch)
        and isinstance(alpha, Angle)
        and isinstance(delta, Angle)
        and isinstance(sun_lon, Angle)
    ):
        raise TypeError("Invalid input types")
    # Proceed to compute the true obliquity, nutation in longitude and nutation
    # in obliquity
    epsilon = true_obliquity(epoch)
    dpsi = nutation_longitude(epoch)
    depsilon = nutation_obliquity(epoch)
    # Convert the angles to radians
    a = alpha.rad()
    d = delta.rad()
    eps = epsilon.rad()
    # Compute corrections due to nutation
    dalpha1 = ((cos(eps) + sin(eps) * sin(a) * tan(d)) * dpsi
               - (cos(a) * tan(d)) * depsilon)
    ddelta1 = (sin(eps) * cos(a)) * dpsi + sin(a) * depsilon
    dalpha1 = Angle(dalpha1)
    ddelta1 = Angle(ddelta1)
    # Now, let's compute the aberration effect
    t = (epoch - JDE2000) / 36525
    e = 0.016708634 + t * (-0.000042037 - t * 0.0000001267)
    pie = 102.93735 + t * (1.71946 + t * 0.00046)
    pie = radians(pie)
    lon = sun_lon.rad()
    k = 20.49552    # The constant of aberration
    dalpha2 = k * (-(cos(a) * cos(lon) * cos(eps) + sin(a) * sin(lon)) / cos(d)
                   + e * (cos(a) * cos(pie) * cos(eps)
                          + sin(a) * sin(pie)) / cos(d))
    ddelta2 = k * (-(cos(lon) * cos(eps)
                     * (tan(eps) * cos(d) - sin(a) * sin(d))
                     + cos(a) * sin(d) * sin(lon))
                   + e * (cos(pie) * cos(eps) * (tan(eps) * cos(d)
                                                 - sin(a) * sin(d))
                          + cos(a) * sin(d) * sin(pie)))
    dalpha2 = Angle(0, 0, dalpha2)
    ddelta2 = Angle(0, 0, ddelta2)
    # Add the two corrections to the original values
    r_alpha = alpha + dalpha1 + dalpha2
    r_delta = delta + ddelta1 + ddelta2
    return r_alpha, r_delta


def orbital_equinox2equinox(epoch0, epoch, i0, arg0, lon0):
    """This function reduces the orbital elements of a celestial object from
    one equinox to another.

    :param epoch0: Initial epoch
    :type epoch0: :py:class:`Epoch`
    :param epoch: Final epoch
    :type epoch: :py:class:`Epoch`
    :param i0: Initial inclination, as an Angle object
    :type i0: :py:class:`Angle`
    :param arg0: Initial argument of perihelion, as an Angle object
    :type arg0: :py:class:`Angle`
    :param lon0: Initial longitude of ascending node, as an Angle object
    :type lon0: :py:class:`Angle`

    :returns: A tuple with three Angle objects: Final inclination, argument of
        perihelion and longitude of ascending node, in that order
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> epoch0 = Epoch(2358042.5305)
    >>> epoch = Epoch(2433282.4235)
    >>> i0 = Angle(47.122)
    >>> arg0 = Angle(151.4486)
    >>> lon0 = Angle(45.7481)
    >>> i1, arg1, lon1 = orbital_equinox2equinox(epoch0, epoch, i0, arg0, lon0)
    >>> print(round(i1(), 3))
    47.138
    >>> print(round(arg1(), 4))
    151.4782
    >>> print(round(lon1(), 4))
    48.6037
    """

    # First check that input values are of correct types
    if not (
        isinstance(epoch0, Epoch)
        and isinstance(epoch, Epoch)
        and isinstance(i0, Angle)
        and isinstance(arg0, Angle)
        and isinstance(lon0, Angle)
    ):
        raise TypeError("Invalid input types")
    # Compute the auxiliary angles
    tt = (epoch0 - JDE2000) / 36525.0
    t = (epoch - epoch0) / 36525.0
    # Compute the conversion parameters
    eta = t * (
        (47.0029 + tt * (-0.06603 + 0.000598 * tt))
        + t * ((-0.03302 + 0.000598 * tt) + 0.00006 * t)
    )
    pie = tt * (3289.4789 + 0.60622 * tt) + t * (
        -(869.8089 + 0.50491 * tt) + 0.03536 * t
    )
    p = t * (
        5029.0966
        + tt * (2.22226 - 0.000042 * tt)
        + t * (1.11113 - 0.000042 * tt - 0.000006 * t)
    )
    eta = Angle(0, 0, eta)
    pie = Angle(0, 0, pie)
    # But beware!: There is still a missing constant for pie. We didn't add
    # it before because of the mismatch between degrees and seconds
    pie += 174.876384
    p = Angle(0, 0, p)
    i0r = i0.rad()
    etar = eta.rad()
    lon0r = lon0.rad()
    pir = pie.rad()
    # If i0 is very small, the procedure is different
    if i0 < 1.0:
        i1 = eta
        lon1 = pie + p + 180.0
    else:
        a = sin(i0r) * sin(lon0r - pir)
        b = -sin(etar) * cos(i0r) + cos(etar) * sin(i0r) * cos(lon0r - pir)
        i1 = asin(sqrt(a*a + b*b))
        i1 = Angle(i1, radians=True)
        omegapsi = atan2(a, b)
        omegapsi = Angle(omegapsi, radians=True)
        lon1 = omegapsi + pie + p
    domega = atan2(-sin(etar) * sin(lon0r - pir),
                   sin(i0r) * cos(etar)
                   - cos(i0r) * sin(etar) * cos(lon0r - pir))
    domega = Angle(domega, radians=True)
    arg1 = arg0 + domega
    return i1, arg1, lon1


def kepler_equation(eccentricity, mean_anomaly):
    """This function computes the eccentric and true anomalies taking as input
    the mean anomaly and the eccentricity.

    :param eccentricity: Orbit's eccentricity
    :type eccentricity: int, float
    :param mean_anomaly: Mean anomaly, as an Angle object
    :type mean_anomaly: :py:class:`Angle`

    :returns: A tuple with two Angle objects: Eccentric and true anomalies
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> eccentricity = 0.1
    >>> mean_anomaly = Angle(5.0)
    >>> e, v = kepler_equation(eccentricity, mean_anomaly)
    >>> print(round(e(), 6))
    5.554589
    >>> print(round(v(), 6))
    6.139762
    >>> eccentricity = 0.99
    >>> mean_anomaly = Angle(2.0)
    >>> e, v = kepler_equation(eccentricity, mean_anomaly)
    >>> print(round(e(), 6))
    32.361007
    >>> print(round(v(), 6))
    152.542134
    >>> eccentricity = 0.99
    >>> mean_anomaly = Angle(5.0)
    >>> e, v = kepler_equation(eccentricity, mean_anomaly)
    >>> print(round(e(), 6))
    45.361023
    >>> print(round(v(), 6))
    160.745616
    >>> eccentricity = 0.99
    >>> mean_anomaly = Angle(1.0)
    >>> e, v = kepler_equation(eccentricity, mean_anomaly)
    >>> print(round(e(), 6))
    24.725822
    >>> print(round(v(), 6))
    144.155952
    >>> e, v = kepler_equation(0.999, Angle(7.0))
    >>> print(round(e(), 7))
    52.2702615
    >>> print(round(v(), 6))
    174.780018
    >>> e, v = kepler_equation(0.99, Angle(0.2, radians=True))
    >>> print(round(e(), 8))
    61.13444578
    >>> print(round(v(), 6))
    166.311977
    """

    # First check that input values are of correct types
    if not (
        isinstance(eccentricity, (int, float))
        and isinstance(mean_anomaly, Angle)
    ):
        raise TypeError("Invalid input types")
    # Let's implement the third method (from Roger Sinnot), page 206
    # First, compute the eccentric anomaly
    m = mean_anomaly.rad()
    ecc = eccentricity
    f = copysign(1.0, m)
    m = abs(m) / (2.0 * pi)
    m = (m - iint(m)) * 2.0 * pi * f
    if m < 0.0:
        m += 2.0 * pi
    f = 1.0
    if m > pi:
        f = -1
        m = 2.0 * pi - m
    e0 = pi / 2.0
    d = pi / 4.0
    ef = 0.0
    while abs(e0 - ef) > TOL:
        ef = e0
        m1 = e0 - ecc * sin(e0)
        s = copysign(1.0, m - m1)
        e0 += d * s
        d /= 2.0
    e = Angle(e0 * f, radians=True)
    # Now, compute the true anomaly
    er = e.rad()
    v = 2.0 * atan(sqrt((1.0 + ecc) / (1.0 - ecc)) * tan(er / 2.0))
    return e, Angle(v, radians=True)


def orbital_elements(epoch, parameters1, parameters2):
    """This function computes the orbital elements for a given epoch, according
    to the parameters beeing passed as arguments.

    :param epoch: Epoch to compute orbital elements, as an Epoch object
    :type epoch: :py:class:`Epoch`
    :param parameters1: First set of parameters
    :type parameters1: list
    :param parameters2: Second set of parameters
    :type parameters2: list

    :returns: A tuple containing the following six orbital elements:
        - Mean longitude of the planet (Angle)
        - Semimajor axis of the orbit (float, astronomical units)
        - eccentricity of the orbit (float)
        - inclination on the plane of the ecliptic (Angle)
        - longitude of the ascending node (Angle)
        - argument of the perihelion (Angle)
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.
    """

    # First check that input values are of correct types
    if not (isinstance(epoch, Epoch) and isinstance(parameters1, list)
            and isinstance(parameters2, list)):
        raise TypeError("Invalid input types")

    # Define an auxiliary function
    def compute_element(t, param):
        return param[0] + t * (param[1] + t * (param[2] + t * param[3]))

    # Compute the time parameter
    t = (epoch - JDE2000) / 36525.0
    # Compute the orbital elements
    ll = compute_element(t, parameters2[0])
    a = compute_element(t, parameters1[1])
    e = compute_element(t, parameters1[2])
    if len(parameters2) == 4:
        i = compute_element(t, parameters2[1])
        omega = compute_element(t, parameters2[2])
        pie = compute_element(t, parameters2[3])
    else:
        i = compute_element(t, parameters2[3])
        omega = compute_element(t, parameters2[4])
        pie = compute_element(t, parameters2[5])
    arg = pie - omega
    ll = Angle(ll)
    i = Angle(i)
    omega = Angle(omega)
    arg = Angle(arg)
    return ll, a, e, i, omega, arg


def velocity(r, a):
    """This function computes the instantaneous velocity of the moving body, in
    kilometers per second, for an unperturbed elliptic orbit.

    :param r: Distance of the body to the Sun, in Astronomical Units
    :type r: float
    :param a: Semimajor axis of the orbit, in Astronomical Units
    :type a: float

    :returns: Velocity of the body, in kilometers per second
    :rtype: float
    :raises: TypeError if input values are of wrong type.

    >>> r = 1.0
    >>> a = 17.9400782
    >>> v = velocity(r, a)
    >>> print(round(v, 2))
    41.53
    """

    if not (isinstance(r, float) and isinstance(a, float)):
        raise TypeError("Invalid input types")
    return 42.1218 * sqrt((1.0 / r) - (1.0 / (2.0 * a)))


def velocity_perihelion(e, a):
    """This function computes the velocity of the moving body at perihelion, in
    kilometers per second, for an unperturbed elliptic orbit.

    :param e: Orbital eccentricity
    :type e: float
    :param a: Semimajor axis of the orbit, in Astronomical Units
    :type a: float

    :returns: Velocity of the body at perihelion, in kilometers per second
    :rtype: float
    :raises: TypeError if input values are of wrong type.

    >>> a = 17.9400782
    >>> e = 0.96727426
    >>> vp = velocity_perihelion(e, a)
    >>> print(round(vp, 2))
    54.52
    """

    if not (isinstance(e, float) and isinstance(a, float)):
        raise TypeError("Invalid input types")
    temp = sqrt((1.0 + e) / (1.0 - e))
    return 29.7847 * temp / sqrt(a)


def velocity_aphelion(e, a):
    """This function computes the velocity of the moving body at aphelion, in
    kilometers per second, for an unperturbed elliptic orbit.

    :param e: Orbital eccentricity
    :type e: float
    :param a: Semimajor axis of the orbit, in Astronomical Units
    :type a: float

    :returns: Velocity of the body at aphelion, in kilometers per second
    :rtype: float
    :raises: TypeError if input values are of wrong type.

    >>> a = 17.9400782
    >>> e = 0.96727426
    >>> va = velocity_aphelion(e, a)
    >>> print(round(va, 2))
    0.91
    """

    if not (isinstance(e, float) and isinstance(a, float)):
        raise TypeError("Invalid input types")
    temp = sqrt((1.0 - e) / (1.0 + e))
    return 29.7847 * temp / sqrt(a)


def length_orbit(e, a):
    """This function computes the length of an elliptic orbit given its
    eccentricity and semimajor axis.

    :param e: Orbital eccentricity
    :type e: float
    :param a: Semimajor axis of the orbit, in Astronomical Units
    :type a: float

    :returns: Length of the orbit in Astronomical Units
    :rtype: float
    :raises: TypeError if input values are of wrong type.

    >>> a = 17.9400782
    >>> e = 0.96727426
    >>> length = length_orbit(e, a)
    >>> print(round(length, 2))
    77.06
    """

    if not (isinstance(e, float) and isinstance(a, float)):
        raise TypeError("Invalid input types")
    # Let's start computing the semi-minor axis
    b = a * sqrt(1.0 - e * e)
    # Use one formula or another depending on eccentricity
    if e < 0.95:
        aa = (a + b) / 2.0
        gg = sqrt(a * b)
        hh = (2.0 * a * b) / (a + b)
        length = pi * (21.0 * aa - 2.0 * gg - 3.0 * hh) / 8.0
    else:
        length = pi * (3.0 * (a + b) - sqrt((a + 3.0 * b) * (3.0 * a + b)))
    return length


def passage_nodes_elliptic(omega, e, a, t, ascending=True):
    """This function computes the time of passage by the nodes (ascending or
    descending) of a given celestial object with an elliptic orbit.

    :param omega: Argument of the perihelion
    :type omega: :py:class:`Angle`
    :param e: Orbital eccentricity
    :type e: float
    :param a: Semimajor axis of the orbit, in Astronomical Units
    :type a: float
    :param t: Time of perihelion passage
    :type t: :py:class:`Epoch`
    :param ascending: Whether the time of passage by the ascending (True) or
        descending (False) node will be computed
    :type ascending: bool

    :returns: Tuple containing:
        - Time of passage through the node (:py:class:`Epoch`)
        - Radius vector when passing through the node (in AU, float)
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> omega = Angle(111.84644)
    >>> e = 0.96727426
    >>> a = 17.9400782
    >>> t = Epoch(1986, 2, 9.45891)
    >>> time, r = passage_nodes_elliptic(omega, e, a, t)
    >>> year, month, day = time.get_date()
    >>> print(year)
    1985
    >>> print(month)
    11
    >>> print(round(day, 2))
    9.16
    >>> print(round(r, 4))
    1.8045
    >>> time, r = passage_nodes_elliptic(omega, e, a, t, ascending=False)
    >>> year, month, day = time.get_date()
    >>> print(year)
    1986
    >>> print(month)
    3
    >>> print(round(day, 2))
    10.37
    >>> print(round(r, 4))
    0.8493
    """

    if not (isinstance(omega, Angle) and isinstance(e, float)
            and isinstance(a, float) and isinstance(t, Epoch)):
        raise TypeError("Invalid input types")
    # First, get the true anomaly
    if ascending:
        v = 360.0 - omega
    else:
        v = 180.0 - omega
    # Compute the eccentric anomaly
    ee = 2.0 * atan(sqrt((1.0 - e)/(1.0 + e)) * tan(v.rad() / 2.0))
    # Now compute the mean anomaly
    m = ee - e * sin(ee)
    # We need the mean motion, in degrees/day
    n = 0.9856076686/(a * sqrt(a))
    # The time of passage will be
    tt = t + degrees(m) / n
    # And the corresponding radius vector is
    r = a * (1.0 - e * cos(ee))
    return tt, r


def passage_nodes_parabolic(omega, q, t, ascending=True):
    """This function computes the time of passage by the nodes (ascending or
    descending) of a given celestial object with a parabolic orbit.

    :param omega: Argument of the perihelion
    :type omega: :py:class:`Angle`
    :param q: Perihelion distance, in Astronomical Units
    :type q: float
    :param t: Time of perihelion passage
    :type t: :py:class:`Epoch`
    :param ascending: Whether the time of passage by the ascending (True) or
        descending (False) node will be computed
    :type ascending: bool

    :returns: Tuple containing:
        - Time of passage through the node (:py:class:`Epoch`)
        - Radius vector when passing through the node (in AU, float)
    :rtype: tuple
    :raises: TypeError if input values are of wrong type.

    >>> omega = Angle(154.9103)
    >>> q = 1.324502
    >>> t = Epoch(1989, 8, 20.291)
    >>> time, r = passage_nodes_parabolic(omega, q, t)
    >>> year, month, day = time.get_date()
    >>> print(year)
    1977
    >>> print(month)
    9
    >>> print(round(day, 2))
    17.64
    >>> print(round(r, 4))
    28.0749
    >>> time, r = passage_nodes_parabolic(omega, q, t, ascending=False)
    >>> year, month, day = time.get_date()
    >>> print(year)
    1989
    >>> print(month)
    9
    >>> print(round(day, 3))
    17.636
    >>> print(round(r, 4))
    1.3901
    """

    if not (isinstance(omega, Angle) and isinstance(q, float)
            and isinstance(t, Epoch)):
        raise TypeError("Invalid input types")
    # First, get the true anomaly
    if ascending:
        v = 360.0 - omega
    else:
        v = 180.0 - omega
    # Compute an auxiliary value
    s = tan(v.rad() / 2.0)
    s2 = s * s
    # Compute time of passage
    tt = t + 27.403895 * s * (s2 + 3.0) * q * sqrt(q)
    # Compute radius vector
    r = q * (1.0 + s2)
    return tt, r


def phase_angle(sun_dist, earth_dist, sun_earth_dist):
    """This function computes the phase angle, i.e., the angle Sun-planet-Earth
    from the corresponding distances.

    :param sun_dist: Planet's distance to the Sun, in Astronomical Units
    :type sun_dist: float
    :param earth_dist: Distance from planet to Earth, in Astronomical Units
    :type earth_dist: float
    :param sun_earth_dist: Distance Sun-Earth, in Astronomical Units
    :type sun_earth_dist: float

    :returns: The phase angle, as an Angle object
    :rtype: :py:class:`Angle`
    :raises: TypeError if input values are of wrong type.

    >>> sun_dist = 0.724604
    >>> earth_dist = 0.910947
    >>> sun_earth_dist = 0.983824
    >>> angle = phase_angle(sun_dist, earth_dist, sun_earth_dist)
    >>> print(round(angle, 2))
    72.96
    """

    if not (isinstance(sun_dist, float) and isinstance(earth_dist, float)
            and isinstance(sun_earth_dist, float)):
        raise TypeError("Invalid input types")
    angle = acos((sun_dist * sun_dist + earth_dist * earth_dist
                  - sun_earth_dist * sun_earth_dist)
                 / (2.0 * sun_dist * earth_dist))
    angle = Angle(angle, radians=True)
    return angle


def illuminated_fraction(sun_dist, earth_dist, sun_earth_dist):
    """This function computes the illuminated fraction of the disk of a planet,
    as seen from the Earth.

    :param sun_dist: Planet's distance to the Sun, in Astronomical Units
    :type sun_dist: float
    :param earth_dist: Distance from planet to Earth, in Astronomical Units
    :type earth_dist: float
    :param sun_earth_dist: Distance Sun-Earth, in Astronomical Units
    :type sun_earth_dist: float

    :returns: The illuminated fraction of the disc of a planet
    :rtype: float
    :raises: TypeError if input values are of wrong type.

    >>> sun_dist = 0.724604
    >>> earth_dist = 0.910947
    >>> sun_earth_dist = 0.983824
    >>> k = illuminated_fraction(sun_dist, earth_dist, sun_earth_dist)
    >>> print(round(k, 3))
    0.647
    """

    if not (isinstance(sun_dist, float) and isinstance(earth_dist, float)
            and isinstance(sun_earth_dist, float)):
        raise TypeError("Invalid input types")
    k = ((sun_dist + earth_dist) * (sun_dist + earth_dist)
         - sun_earth_dist * sun_earth_dist) / (4.0 * sun_dist * earth_dist)
    return k


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's show some uses of Coordinate functions
    print("\n" + 35 * "*")
    print("*** Use of Coordinate functions")
    print(35 * "*" + "\n")

    # Here follows a series of important parameters related to the angle
    # between Earth's rotation axis and the ecliptic
    e0 = mean_obliquity(1987, 4, 10)
    print(
        "The mean angle between Earth rotation axis and ecliptic axis for "
        + "1987/4/10 is:"
    )
    print_me("Mean obliquity", e0.dms_str(n_dec=3))  # 23d 26' 27.407''
    epsilon = true_obliquity(1987, 4, 10)
    print("'True' (instantaneous) angle between those axes for 1987/4/10 is:")
    print_me("True obliquity", epsilon.dms_str(n_dec=3))  # 23d 26' 36.849''
    epsilon = true_obliquity(2018, 7, 29)
    print("'True' (instantaneous) angle between those axes for 2018/7/29 is:")
    print_me("True obliquity", epsilon.dms_str(True, 4))  # 23d 26' 7.2157''

    # The nutation effect is separated in two components: One parallel to the
    # ecliptic (nutation in longitude) and other perpendicular to the ecliptic
    # (nutation in obliquity)
    print("Nutation correction in longitude for 1987/4/10:")
    dpsi = nutation_longitude(1987, 4, 10)
    print_me("Nutation in longitude", dpsi.dms_str(n_dec=3))  # 0d 0' -3.788''
    print("Nutation correction in obliquity for 1987/4/10:")
    depsilon = nutation_obliquity(1987, 4, 10)  # 0d 0' 9.443''
    print_me("Nutation in obliquity", depsilon.dms_str(n_dec=3))

    print("")

    # We can compute the effects of precession on the equatorial coordinates of
    # a given star, taking also into account its proper motion

    start_epoch = JDE2000
    final_epoch = Epoch(2028, 11, 13.19)
    alpha0 = Angle(2, 44, 11.986, ra=True)
    delta0 = Angle(49, 13, 42.48)  # 2h 44' 11.986''
    print_me("Initial right ascension", alpha0.ra_str(n_dec=3))
    print_me("Initial declination", delta0.dms_str(n_dec=2))  # 49d 13' 42.48''
    pm_ra = Angle(0, 0, 0.03425, ra=True)
    pm_dec = Angle(0, 0, -0.0895)
    alpha, delta = precession_equatorial(
        start_epoch, final_epoch, alpha0, delta0, pm_ra, pm_dec
    )
    print_me("Final right ascension", alpha.ra_str(n_dec=3))  # 2h 46' 11.331''
    print_me("Final declination", delta.dms_str(n_dec=2))  # 49d 20' 54.54''

    print("")

    # Something similar can also be done with the ecliptical coordinates
    start_epoch = JDE2000
    final_epoch = Epoch(-214, 6, 30.0)
    lon0 = Angle(149.48194)
    lat0 = Angle(1.76549)
    print_me("Initial ecliptical longitude", round(lon0(), 5))  # 149.48194
    print_me("Initial ecliptical latitude", round(lat0(), 5))  # 1.76549
    lon, lat = precession_ecliptical(start_epoch, final_epoch, lon0, lat0)
    print_me("Final ecliptical longitude", round(lon(), 3))  # 118.704
    print_me("Final ecliptical latitude", round(lat(), 3))  # 1.615

    print("")

    # It is possible to compute with relative accuracy the proper motion of the
    # stars, taking into account their distance to Sun and relative velocity
    ra = Angle(6, 45, 8.871, ra=True)
    dec = Angle(-16.716108)
    pm_ra = Angle(0, 0, -0.03847, ra=True)
    pm_dec = Angle(0, 0, -1.2053)
    dist = 2.64
    vel = -7.6
    alpha, delta = motion_in_space(ra, dec, dist, vel, pm_ra, pm_dec, -1000.0)

    print_me("Right ascension, year 2000", ra.ra_str(True, 2))
    print_me("Right ascension, year 1000", alpha.ra_str(True, 2))
    # 6h 45' 47.16''
    print_me("Declination, year 2000", dec.dms_str(True, 1))
    print_me("Declination, year 1000", delta.dms_str(True, 1))
    # -16d 22' 56.0''

    print("")

    # This module provides a series of functions to convert between equatorial,
    # ecliptical, horizontal and galactic coordinates

    ra = Angle(7, 45, 18.946, ra=True)
    dec = Angle(28, 1, 34.26)
    epsilon = Angle(23.4392911)
    lon, lat = equatorial2ecliptical(ra, dec, epsilon)
    print_me("Equatorial to ecliptical. Longitude", round(lon(), 5))
    # 113.21563
    print_me("Equatorial to ecliptical. Latitude", round(lat(), 5))
    # 6.68417

    print("")

    lon = Angle(113.21563)
    lat = Angle(6.68417)
    epsilon = Angle(23.4392911)
    ra, dec = ecliptical2equatorial(lon, lat, epsilon)
    print_me("Ecliptical to equatorial. Right ascension", ra.ra_str(n_dec=3))
    # 7h 45' 18.946''
    print_me("Ecliptical to equatorial. Declination", dec.dms_str(n_dec=2))
    # 28d 1' 34.26''

    print("")

    lon = Angle(77, 3, 56)
    lat = Angle(38, 55, 17)
    ra = Angle(23, 9, 16.641, ra=True)
    dec = Angle(-6, 43, 11.61)
    theta0 = Angle(8, 34, 57.0896, ra=True)
    eps = Angle(23, 26, 36.87)
    # Compute correction to convert from mean to apparent sidereal time
    delta = Angle(0, 0, ((-3.868 * cos(eps.rad())) / 15.0), ra=True)
    theta0 += delta
    h = theta0 - lon - ra
    azi, ele = equatorial2horizontal(h, dec, lat)
    print_me("Equatorial to horizontal: Azimuth", round(azi, 3))  # 68.034
    print_me("Equatorial to horizontal: Elevation", round(ele, 3))  # 15.125

    print("")

    azi = Angle(68.0337)
    ele = Angle(15.1249)
    lat = Angle(38, 55, 17)
    h, dec = horizontal2equatorial(azi, ele, lat)
    print_me("Horizontal to equatorial. Hour angle", round(h, 4))  # 64.3521
    print_me("Horizontal to equatorial. Declination", dec.dms_str(n_dec=0))
    # -6d 43' 12.0''

    print("")

    ra = Angle(17, 48, 59.74, ra=True)
    dec = Angle(-14, 43, 8.2)
    lon, lat = equatorial2galactic(ra, dec)
    print_me("Equatorial to galactic. Longitude", round(lon, 4))  # 12.9593
    print_me("Equatorial to galactic. Latitude", round(lat, 4))  # 6.0463

    print("")

    lon = Angle(12.9593)
    lat = Angle(6.0463)
    ra, dec = galactic2equatorial(lon, lat)
    print_me("Galactic to equatorial. Right ascension", ra.ra_str(n_dec=1))
    # 17h 48' 59.7''
    print_me("Galactic to equatorial. Declination", dec.dms_str(n_dec=0))
    # -14d 43' 8.0''

    print("")

    # Get the ecliptic longitudes of the two points of the ecliptic which are
    # on the horizon, as well as the angle between the ecliptic and the horizon
    sidereal_time = Angle(5.0, ra=True)
    lat = Angle(51.0)
    epsilon = Angle(23.44)
    lon1, lon2, i = ecliptic_horizon(sidereal_time, lat, epsilon)
    print_me(
        "Longitude of ecliptic point #1 on the horizon", lon1.dms_str(n_dec=1)
    )  # 169d 21' 29.9''
    print_me(
        "Longitude of ecliptic point #2 on the horizon", lon2.dms_str(n_dec=1)
    )  # 349d 21' 29.9''
    print_me("Angle between the ecliptic and the horizon", round(i, 0))  # 62.0

    print("")

    # Let's compute the angle of the diurnal path of a celestial body relative
    # to the horizon at the time of rising and setting
    dec = Angle(23.44)
    lat = Angle(40.0)
    j = diurnal_path_horizon(dec, lat)
    print_me(
        "Diurnal path vs. horizon angle at time of rising and setting",
        j.dms_str(n_dec=1),
    )  # 45d 31' 28.4''

    print("")

    # There is a function to compute the times (in hours of the day) of rising,
    # transit and setting of a given celestial body
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
    rising, transit, setting = times_rise_transit_set(
        longitude,
        latitude,
        alpha1,
        delta1,
        alpha2,
        delta2,
        alpha3,
        delta3,
        h0,
        delta_t,
        theta0,
    )

    print_me("Time of rising (hours of day)", round(rising, 4))  # 12.4238
    print_me("Time of transit (hours of day)", round(transit, 3))  # 19.675
    print_me("Time of setting (hours of day, next day)", round(setting, 3))
    # 2.911

    print("")

    # The air in the atmosphere introduces an error in the elevation due to the
    # refraction. We can compute the true (airless) elevation from the apparent
    # elevation, and viceversa
    apparent_elevation = Angle(0, 30, 0.0)
    true_elevation = refraction_apparent2true(apparent_elevation)
    print_me(
        "True elevation for an apparent elevation of 30'",
        true_elevation.dms_str(n_dec=1),
    )  # 1' 14.7''

    true_elevation = Angle(0, 33, 14.76)
    apparent_elevation = refraction_true2apparent(true_elevation)
    print_me(
        "Apparent elevation for a true elevation of 33' 14.76''",
        apparent_elevation.dms_str(n_dec=2),
    )  # 57' 51.96''

    print("")

    # The angular separation between two celestial objects can be easily
    # computed with the 'angular_separation()' function

    alpha1 = Angle(14, 15, 39.7, ra=True)
    delta1 = Angle(19, 10, 57.0)
    alpha2 = Angle(13, 25, 11.6, ra=True)
    delta2 = Angle(-11, 9, 41.0)
    sep_ang = angular_separation(alpha1, delta1, alpha2, delta2)
    print_me(
        "Angular separation between two given celestial bodies (degrees)",
        round(sep_ang, 3),
    )  # 32.793

    print("")

    # We can compute the minimum angular separation achieved between two
    # celestial objects. For that, we must provide the positions at three
    # equidistant epochs:

    # EPOCH: Sep 13th, 1978, 0h TT:
    alpha1_1 = Angle(10, 29, 44.27, ra=True)
    delta1_1 = Angle(11, 2, 5.9)
    alpha2_1 = Angle(10, 33, 29.64, ra=True)
    delta2_1 = Angle(10, 40, 13.2)
    # EPOCH: Sep 14th, 1978, 0h TT:
    alpha1_2 = Angle(10, 36, 19.63, ra=True)
    delta1_2 = Angle(10, 29, 51.7)
    alpha2_2 = Angle(10, 33, 57.97, ra=True)
    delta2_2 = Angle(10, 37, 33.4)
    # EPOCH: Sep 15th, 1978, 0h TT:
    alpha1_3 = Angle(10, 43, 1.75, ra=True)
    delta1_3 = Angle(9, 55, 16.7)
    alpha2_3 = Angle(10, 34, 26.22, ra=True)
    delta2_3 = Angle(10, 34, 53.9)
    a = minimum_angular_separation(
        alpha1_1,
        delta1_1,
        alpha1_2,
        delta1_2,
        alpha1_3,
        delta1_3,
        alpha2_1,
        delta2_1,
        alpha2_2,
        delta2_2,
        alpha2_3,
        delta2_3,
    )
    # Epoch fraction:
    print_me("Minimum angular separation, epoch fraction", round(a[0], 6))
    # -0.370726
    # NOTE: Given that 'n' is negative, and Sep 14th is the middle epoch (n=0),
    # then the minimum angular separation is achieved on Sep 13th, specifically
    # at: 1.0 - 0.370726 = 0.629274 => Sep 13.629274 = Sep 13th, 15h 6' 9''

    # Minimum angular separation:
    print_me("Minimum angular separation", a[1].dms_str(n_dec=0))  # 3' 44.0''

    print("")

    # If two objects have the same right ascension, then the relative position
    # angle between them must be 0 (or 180)
    alpha1 = Angle(14, 15, 39.7, ra=True)
    delta1 = Angle(19, 10, 57.0)
    alpha2 = Angle(14, 15, 39.7, ra=True)  # Same as alpha1
    delta2 = Angle(-11, 9, 41.0)
    pos_ang = relative_position_angle(alpha1, delta1, alpha2, delta2)
    print_me("Relative position angle", round(pos_ang, 1))  # 0.0

    print("")

    # Planetary conjunctions may be computed with the appropriate function
    alpha1_1 = Angle(10, 24, 30.125, ra=True)
    delta1_1 = Angle(6, 26, 32.05)
    alpha1_2 = Angle(10, 25, 0.342, ra=True)
    delta1_2 = Angle(6, 10, 57.72)
    alpha1_3 = Angle(10, 25, 12.515, ra=True)
    delta1_3 = Angle(5, 57, 33.08)
    alpha1_4 = Angle(10, 25, 6.235, ra=True)
    delta1_4 = Angle(5, 46, 27.07)
    alpha1_5 = Angle(10, 24, 41.185, ra=True)
    delta1_5 = Angle(5, 37, 48.45)
    alpha2_1 = Angle(10, 27, 27.175, ra=True)
    delta2_1 = Angle(4, 4, 41.83)
    alpha2_2 = Angle(10, 26, 32.410, ra=True)
    delta2_2 = Angle(3, 55, 54.66)
    alpha2_3 = Angle(10, 25, 29.042, ra=True)
    delta2_3 = Angle(3, 48, 3.51)
    alpha2_4 = Angle(10, 24, 17.191, ra=True)
    delta2_4 = Angle(3, 41, 10.25)
    alpha2_5 = Angle(10, 22, 57.024, ra=True)
    delta2_5 = Angle(3, 35, 16.61)
    alpha1_list = [alpha1_1, alpha1_2, alpha1_3, alpha1_4, alpha1_5]
    delta1_list = [delta1_1, delta1_2, delta1_3, delta1_4, delta1_5]
    alpha2_list = [alpha2_1, alpha2_2, alpha2_3, alpha2_4, alpha2_5]
    delta2_list = [delta2_1, delta2_2, delta2_3, delta2_4, delta2_5]
    pc = planetary_conjunction(alpha1_list, delta1_list, alpha2_list,
                               delta2_list)
    print_me("Epoch fraction 'n' for planetary conjunction", round(pc[0], 5))
    # 0.23797
    print_me(
        "Difference in declination at conjunction", pc[1].dms_str(n_dec=1)
    )  # 2d 8' 21.8''

    print("")

    # A planetary conjunction with a star is a little bit simpler
    alpha_1 = Angle(15, 3, 51.937, ra=True)
    delta_1 = Angle(-8, 57, 34.51)
    alpha_2 = Angle(15, 9, 57.327, ra=True)
    delta_2 = Angle(-9, 9, 3.88)
    alpha_3 = Angle(15, 15, 37.898, ra=True)
    delta_3 = Angle(-9, 17, 37.94)
    alpha_4 = Angle(15, 20, 50.632, ra=True)
    delta_4 = Angle(-9, 23, 16.25)
    alpha_5 = Angle(15, 25, 32.695, ra=True)
    delta_5 = Angle(-9, 26, 1.01)
    alpha_star = Angle(15, 17, 0.446, ra=True)
    delta_star = Angle(-9, 22, 58.47)
    alpha_list = [alpha_1, alpha_2, alpha_3, alpha_4, alpha_5]
    delta_list = [delta_1, delta_2, delta_3, delta_4, delta_5]
    pc = planet_star_conjunction(alpha_list, delta_list, alpha_star,
                                 delta_star)
    print_me("Epoch fraction 'n' for planetary conjunction with star",
             round(pc[0], 4))  # 0.2551
    print_me("Difference in declination with star at conjunction",
             pc[1].dms_str(n_dec=0))  # 3' 38.0''

    print("")

    # It is possible to compute when a planet and two other stars will be in a
    # straight line
    alpha_1 = Angle(7, 55, 55.36, ra=True)
    delta_1 = Angle(21, 41, 3.0)
    alpha_2 = Angle(7, 58, 22.55, ra=True)
    delta_2 = Angle(21, 35, 23.4)
    alpha_3 = Angle(8, 0, 48.99, ra=True)
    delta_3 = Angle(21, 29, 38.2)
    alpha_4 = Angle(8, 3, 14.66, ra=True)
    delta_4 = Angle(21, 23, 47.5)
    alpha_5 = Angle(8, 5, 39.54, ra=True)
    delta_5 = Angle(21, 17, 51.4)
    alpha_star1 = Angle(7, 34, 16.40, ra=True)
    delta_star1 = Angle(31, 53, 51.2)
    alpha_star2 = Angle(7, 45, 0.10, ra=True)
    delta_star2 = Angle(28, 2, 12.5)
    alpha_list = [alpha_1, alpha_2, alpha_3, alpha_4, alpha_5]
    delta_list = [delta_1, delta_2, delta_3, delta_4, delta_5]
    n = planet_stars_in_line(alpha_list, delta_list, alpha_star1, delta_star1,
                             alpha_star2, delta_star2)
    print_me("Epoch fraction 'n' when bodies are in a straight line",
             round(n, 4))  # 0.2233

    print("")

    # The function 'straight_line()' computes if three celestial bodies are in
    # line providing the angle with which the bodies differ from a great circle
    alpha1 = Angle(5, 32, 0.40, ra=True)
    delta1 = Angle(0, -17, 56.9)
    alpha2 = Angle(5, 36, 12.81, ra=True)
    delta2 = Angle(-1, 12, 7.0)
    alpha3 = Angle(5, 40, 45.52, ra=True)
    delta3 = Angle(-1, 56, 33.3)
    psi, omega = straight_line(alpha1, delta1, alpha2, delta2, alpha3, delta3)
    print_me("Angle deviation from a straight line", psi.dms_str(n_dec=0))
    # 7d 31' 1.0''
    print_me("Angular distance of central point to the straight line",
             omega.dms_str(n_dec=0))  # -5' 24.0''

    print("")

    # Let's compute the size of the smallest circle that contains three bodies
    alpha1 = Angle(12, 41, 8.63, ra=True)
    delta1 = Angle(-5, 37, 54.2)
    alpha2 = Angle(12, 52, 5.21, ra=True)
    delta2 = Angle(-4, 22, 26.2)
    alpha3 = Angle(12, 39, 28.11, ra=True)
    delta3 = Angle(-1, 50, 3.7)
    d = circle_diameter(alpha1, delta1, alpha2, delta2, alpha3, delta3)
    print_me(
        "Diameter of smallest circle containing three celestial bodies",
        d.dms_str(n_dec=0),
    )  # 4d 15' 49.0''

    print("")

    # Now, let's find the apparent position of a star (Theta Persei) for a
    # given epoch
    epoch = Epoch(2028, 11, 13.19)
    alpha = Angle(2, 46, 11.331, ra=True)
    delta = Angle(49, 20, 54.54)
    sun_lon = Angle(231.328)
    app_alpha, app_delta = apparent_position(epoch, alpha, delta, sun_lon)
    print_me("Apparent right ascension", app_alpha.ra_str(n_dec=2))
    # 2h 46' 14.39''
    print_me("Apparent declination", app_delta.dms_str(n_dec=2))
    # 49d 21' 7.45''

    print("")

    # Convert orbital elements of an object from one equinox to another
    epoch0 = Epoch(2358042.5305)
    epoch = Epoch(2433282.4235)
    i0 = Angle(47.122)
    arg0 = Angle(151.4486)
    lon0 = Angle(45.7481)
    i1, arg1, lon1 = orbital_equinox2equinox(epoch0, epoch, i0, arg0, lon0)
    print_me("New inclination", round(i1(), 3))                     # 47.138
    print_me("New argument of perihelion", round(arg1(), 4))        # 151.4782
    print_me("New longitude of ascending node", round(lon1(), 4))   # 48.6037

    print("")

    # Compute the eccentric and true anomalies using Kepler's equation
    eccentricity = 0.1
    mean_anomaly = Angle(5.0)
    e, v = kepler_equation(eccentricity, mean_anomaly)
    print_me("Eccentric anomaly, Case #1", round(e(), 6))       # 5.554589
    print_me("True anomaly, Case #1", round(v(), 6))            # 6.139762
    e, v = kepler_equation(0.99, Angle(0.2, radians=True))
    print_me("Eccentric anomaly, Case #2", round(e(), 8))       # 61.13444578
    print_me("True anomaly, Case #2", round(v(), 6))            # 166.311977

    print("")

    # Compute the velocity of a body in a given point of its (unperturbated
    # elliptic) orbit
    r = 1.0
    a = 17.9400782
    v = velocity(r, a)
    print_me("Velocity at 1 AU", round(v, 2))           # 41.53

    # Compute the velocity at perihelion
    e = 0.96727426
    vp = velocity_perihelion(e, a)
    print_me("Velocity at perihelion", round(vp, 2))    # 54.52

    # Compute the velocity at aphelion
    va = velocity_aphelion(e, a)
    print_me("Velocity at aphelion", round(va, 2))      # 0.91

    # Calculate the length of the orbit
    length = length_orbit(e, a)
    print_me("Length of the orbit (AU)", round(length, 2))   # 77.06

    print("")

    # Passage through the nodes of an elliptic orbit
    omega = Angle(111.84644)
    e = 0.96727426
    a = 17.9400782
    t = Epoch(1986, 2, 9.45891)
    time, r = passage_nodes_elliptic(omega, e, a, t)
    y, m, d = time.get_date()
    d = round(d, 2)
    print("Time of passage through ascending node: {}/{}/{}".format(y, m, d))
    # 1985/11/9.16
    print("Radius vector at ascending node: {}".format(round(r, 4)))  # 1.8045

    # Passage through the nodes of a parabolic orbit
    omega = Angle(154.9103)
    q = 1.324502
    t = Epoch(1989, 8, 20.291)
    time, r = passage_nodes_parabolic(omega, q, t, ascending=False)
    y, m, d = time.get_date()
    d = round(d, 2)
    print("Time of passage through descending node: {}/{}/{}".format(y, m, d))
    # 1989/9/17.64
    print("Radius vector at descending node: {}".format(round(r, 4)))  # 1.3901

    print("")

    # Compute the phase angle
    sun_dist = 0.724604
    earth_dist = 0.910947
    sun_earth_dist = 0.983824
    angle = phase_angle(sun_dist, earth_dist, sun_earth_dist)
    print_me("Phase angle", round(angle, 2))                        # 72.96
    # Now, let's compute the illuminated fraction of the disk
    k = illuminated_fraction(sun_dist, earth_dist, sun_earth_dist)
    print_me("Illuminated fraction of planet disk", round(k, 3))    # 0.647


if __name__ == "__main__":

    main()
