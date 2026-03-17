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


from math import sin, cos, tan, acos, atan, atan2, sqrt

from pymeeus.base import TOL
from pymeeus.Angle import Angle
from pymeeus.Epoch import Epoch
from pymeeus.Coordinates import kepler_equation
from pymeeus.Sun import Sun


"""
.. module:: Minor
   :synopsis: Class to model celestial bodies like comets and minor planets
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


class Minor(object):
    """
    Class Minor models minor celestial bodies.
    """

    def __init__(self, q, e, i, omega, w, t):
        """Minor constructor.

        The Minor object is initialized with this constructor, setting the
        orbital values and computing some internal parameters. This constructor
        is build upon the 'set()' method.

        :param q: Perihelion distance, in Astronomical Units
        :type q: float
        :param e: Eccentricity of the orbit
        :type e: float
        :param i: Inclination of the orbit, as an Angle object
        :type i: :py:class:`Angle`
        :param omega: Longitude of the ascending node, as an Angle object
        :type omega: :py:class:`Angle`
        :param w: Argument of the perihelion, as an Angle object
        :type w: :py:class:`Angle`
        :param t: Epoch of passage by perihelion, as an Epoch object
        :type t: :py:class:`Epoch`

        :raises: TypeError if input value is of wrong type.
        """

        self._tol = TOL
        self.set(q, e, i, omega, w, t)

    def set(self, q, e, i, omega, w, t):
        """Method used to set the orbital values and set some internal
        parameters.

        :param q: Perihelion distance, in Astronomical Units
        :type q: float
        :param e: Eccentricity of the orbit
        :type e: float
        :param i: Inclination of the orbit, as an Angle object
        :type i: :py:class:`Angle`
        :param omega: Longitude of the ascending node, as an Angle object
        :type omega: :py:class:`Angle`
        :param w: Argument of the perihelion, as an Angle object
        :type w: :py:class:`Angle`
        :param t: Epoch of passage by perihelion, as an Epoch object
        :type t: :py:class:`Epoch`

        :raises: TypeError if input value is of wrong type.
        """

        # First check that input value is of correct types
        if not (isinstance(t, Epoch) and isinstance(q, float)
                and isinstance(e, float) and isinstance(i, Angle)
                and isinstance(omega, Angle) and isinstance(w, Angle)):
            raise TypeError("Invalid input types")
        # Compute auxiliary quantities
        se = 0.397777156
        ce = 0.917482062
        omer = omega.rad()
        ir = i.rad()
        f = cos(omer)
        g = sin(omer) * ce
        h = sin(omer) * se
        p = -sin(omer) * cos(ir)
        qq = cos(omer) * cos(ir) * ce - sin(ir) * se
        r = cos(omer) * cos(ir) * se + sin(ir) * ce
        self._aa = atan2(f, p)
        self._bb = atan2(g, qq)
        self._cc = atan2(h, r)
        self._am = sqrt(f * f + p * p)
        self._bm = sqrt(g * g + qq * qq)
        self._cm = sqrt(h * h + r * r)
        # Store some orbital parameters
        if abs(e - 1.0) > self._tol:
            self._a = abs(q / (1.0 - e))
        else:
            self._a = q
        self._q = q
        self._e = e
        self._i = i
        self._omega = omega
        self._w = w
        self._t = t
        # Compute the mean motion from the semi-major axis (degrees/day)
        self._n = 0.9856076686 / (self._a * sqrt(self._a))
        return

    def _near_parabolic(self, t):
        """This internal function handles the computation of the true anomaly
        and the radius vector when the eccentricity is close to 1.

        :param t: Days since perihelion
        :type t: float

        :returns: A tuple containing the true anomaly (as an Angle object) and
            the radius vector (in Astronomical Units).
        :rtype: tuple
        :raises: TypeError if input value is of wrong type, and ValueError if
            convergence is not possible

        >>> q = 0.5871018
        >>> e = 0.9672746
        >>> t = 20.0
        >>> i = Angle(0.0)
        >>> omega = Angle(0.0)
        >>> w = Angle(0.0)
        >>> ep = Epoch(2000, 1, 1.5)
        >>> minor = Minor(q, e, i, omega, w, ep)
        >>> v, r = minor._near_parabolic(t)
        >>> print(round(v, 5))
        52.85331
        >>> print(round(r, 6))
        0.729116
        >>> q = 3.363943
        >>> e = 1.05731
        >>> t = 1237.1
        >>> minor = Minor(q, e, i, omega, w, ep)
        >>> v, r = minor._near_parabolic(t)
        >>> print(round(v, 5))
        109.40598
        >>> print(round(r, 6))
        10.668551
        """

        # First check that input value is of correct types
        if not isinstance(t, float):
            raise TypeError("Invalid input type")
        # Let's start defining some constants and renaming some parameters
        k = 0.01720209895
        d1 = 10000
        c = 1.0 / 3.0
        d = self._tol
        q = self._q
        e = self._e
        q1 = k * sqrt((1.0 + e) / q) / (2.0 * q)
        g = (1.0 - e) / (1.0 + e)
        # If t == 0, then r = q and v = 0
        if abs(t) > d:
            q2 = q1 * t
            s = 2.0 / (3.0 * abs(q2))
            s = 2.0 / tan(2.0 * atan(tan(atan(s) / 2) ** c))
            if t < 0.0:
                s = -s
            # Parabolic case
            if abs(e - 1.0) < d:
                v = 2.0 * atan(s)
                rr = q * (1.0 + e) / (1.0 + e * cos(v))
                v = Angle(v, radians=True).to_positive()
                return v, rr
            ll = 0.0
            s0 = s + 1.0
            while abs(s - s0) > d:
                s0 = s
                z = 1
                y = s * s
                g1 = -y * s
                q3 = q2 + 2.0 * g * s * y / 3.0
                f = d + 1.0
                while abs(f) > d:
                    z += 1
                    g1 = -g1 * g * y
                    z1 = (z - (z + 1.0) * g) / (2.0 * z + 1.0)
                    f = z1 * g1
                    q3 = q3 + f
                    if z > 50 or abs(f) > d1:
                        raise ValueError("No convergence")
                ll += 1
                if ll > 50:
                    raise ValueError("No convergence")
                s1 = s + 1.0
                while abs(s - s1) > d:
                    s1 = s
                    s = (2.0 * s * s * s / 3.0 + q3) / (s * s + 1.0)
            v = 2.0 * atan(s)
            rr = q * (1.0 + e) / (1.0 + e * cos(v))
            v = Angle(v, radians=True).to_positive()
            return v, rr
        else:
            rr = q
            v = Angle(0.0)
            return v, rr

    def geocentric_position(self, epoch):
        """This method computes the geocentric position of a minor celestial
        body (right ascension and declination) for the given epoch, and
        referred to the standard equinox J2000.0. Additionally, it also
        computes the elongation angle to the Sun.

        :param epoch: Epoch to compute geocentric position, as an Epoch object
        :type epoch: :py:class:`Epoch`

        :returns: A tuple containing the right ascension, the declination and
            the elongation angle to the Sun, as Angle objects
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> a = 2.2091404
        >>> e = 0.8502196
        >>> q = a * (1.0 - e)
        >>> i = Angle(11.94524)
        >>> omega = Angle(334.75006)
        >>> w = Angle(186.23352)
        >>> t = Epoch(1990, 10, 28.54502)
        >>> minor = Minor(q, e, i, omega, w, t)
        >>> epoch = Epoch(1990, 10, 6.0)
        >>> ra, dec, p = minor.geocentric_position(epoch)
        >>> print(ra.ra_str(n_dec=1))
        10h 34' 13.7''
        >>> print(dec.dms_str(n_dec=0))
        19d 9' 32.0''
        >>> print(round(p, 2))
        40.51
        >>> t = Epoch(1998, 4, 14.4358)
        >>> q = 1.487469
        >>> e = 1.0
        >>> i = Angle(0.0)
        >>> omega = Angle(0.0)
        >>> w = Angle(0.0)
        >>> minor = Minor(q, e, i, omega, w, t)
        >>> epoch = Epoch(1998, 8, 5.0)
        >>> ra, dec, p = minor.geocentric_position(epoch)
        >>> print(ra.ra_str(n_dec=1))
        5h 45' 34.5''
        >>> print(dec.dms_str(n_dec=0))
        23d 23' 53.0''
        >>> print(round(p, 2))
        45.73
        """

        # First check that input value is of correct types
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input type")
        # Get internal parameters
        aa, bb, cc = self._aa, self._bb, self._cc
        am, bm, cm = self._am, self._bm, self._cm
        # Get the mean motion and other orbital parameters
        n = self._n
        a = self._a
        e = self._e
        w = self._w
        t = self._t
        # Time since perihelion
        t_peri = epoch - t
        # Now, compute the mean anomaly, in degrees
        m = t_peri * n
        m = Angle(m)
        if e < 0.98:
            # Elliptic case
            # With the mean anomaly, use Kepler's equation to find E and v
            ee, v = kepler_equation(e, m)
            ee = Angle(ee).to_positive()
            # Get r
            er = ee.rad()
            rr = a * (1.0 - e * cos(er))
        elif abs(e - 1.0) < self._tol:
            # Parabolic case
            q = self._q
            ww = (0.03649116245 * (epoch - self._t)) / (q * sqrt(q))
            sp = ww / 3.0
            iterate = True
            while iterate:
                s = (2.0 * sp * sp * sp + ww) / (3.0 * (sp * sp + 1.0))
                iterate = abs(s - sp) > self._tol
                sp = s
            v = 2.0 * atan(s)
            v = Angle(v, radians=True)
            rr = q * (1.0 + s * s)
        else:
            # We are in the near-parabolic case
            v, rr = self._near_parabolic(t_peri)
        # Compute the heliocentric rectangular equatorial coordinates
        wr = w.rad()
        vr = Angle(v).rad()
        x = rr * am * sin(aa + wr + vr)
        y = rr * bm * sin(bb + wr + vr)
        z = rr * cm * sin(cc + wr + vr)
        # Now let's compute Sun's rectangular coordinates
        xs, ys, zs = Sun.rectangular_coordinates_j2000(epoch)
        xi = x + xs
        eta = y + ys
        zeta = z + zs
        delta = sqrt(xi * xi + eta * eta + zeta * zeta)
        # We need to correct for the effect of light-time. Compute delay tau
        tau = 0.0057755183 * delta
        # Recompute some critical parameters
        t_peri = epoch - t - tau
        # Now, compute the mean anomaly, in degrees
        m = t_peri * n
        m = Angle(m)
        if e < 0.98:
            # Elliptic case
            # With the mean anomaly, use Kepler's equation to find E and v
            ee, v = kepler_equation(e, m)
            ee = Angle(ee).to_positive()
            # Get r
            er = ee.rad()
            rr = a * (1.0 - e * cos(er))
        elif abs(e - 1.0) < self._tol:
            # Parabolic case
            q = self._q
            ww = (0.03649116245 * (epoch - self._t)) / (q * sqrt(q))
            sp = ww / 3.0
            iterate = True
            while iterate:
                s = (2.0 * sp * sp * sp + ww) / (3.0 * (sp * sp + 1.0))
                iterate = abs(s - sp) > self._tol
                sp = s
            v = 2.0 * atan(s)
            v = Angle(v, radians=True)
            rr = q * (1.0 + s * s)
        else:
            # We are in the near-parabolic case
            v, rr = self._near_parabolic(t_peri)
        # Compute the heliocentric rectangular equatorial coordinates
        wr = w.rad()
        vr = Angle(v).rad()
        x = rr * am * sin(aa + wr + vr)
        y = rr * bm * sin(bb + wr + vr)
        z = rr * cm * sin(cc + wr + vr)
        xi = x + xs
        eta = y + ys
        zeta = z + zs
        ra = Angle(atan2(eta, xi), radians=True)
        dec = Angle(atan2(zeta, sqrt(xi * xi + eta * eta)), radians=True)
        r_sun = sqrt(xs * xs + ys * ys + zs * zs)
        psi = acos((xi * xs + eta * ys + zeta * zs) / (r_sun * delta))
        psi = Angle(psi, radians=True)
        return ra, dec, psi

    def heliocentric_ecliptical_position(self, epoch):
        """This method computes the heliocentric position of a minor celestial
        body, providing the result in ecliptical coordinates.

        :param epoch: Epoch to compute geocentric position, as an Epoch object
        :type epoch: :py:class:`Epoch`

        :returns: A tuple containing longitude and latitude, as Angle objects
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> a = 2.2091404
        >>> e = 0.8502196
        >>> q = a * (1.0 - e)
        >>> i = Angle(11.94524)
        >>> omega = Angle(334.75006)
        >>> w = Angle(186.23352)
        >>> t = Epoch(1990, 10, 28.54502)
        >>> epoch = Epoch(1990, 10, 6.0)
        >>> minor = Minor(q, e, i, omega, w, t)
        >>> lon, lat = minor.heliocentric_ecliptical_position(epoch)
        >>> print(lon.dms_str(n_dec=1))
        66d 51' 57.8''
        >>> print(lat.dms_str(n_dec=1))
        11d 56' 14.4''
        """

        # First check that input value is of correct types
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input type")
        # Get the mean motion and other orbital parameters
        n = self._n
        a = self._a
        e = self._e
        i = self._i
        omega = self._omega
        w = self._w
        t = self._t
        # Time since perihelion
        t_peri = epoch - t
        # Now, compute the mean anomaly, in degrees
        m = t_peri * n
        m = Angle(m)
        # With the mean anomaly, use Kepler's equation to find E and v
        ee, v = kepler_equation(e, m)
        ee = Angle(ee).to_positive()
        # Get r
        er = ee.rad()
        r = a * (1.0 - e * cos(er))
        # Compute the heliocentric rectangular ecliptical coordinates
        wr = w.rad()
        vr = Angle(v).rad()
        ur = wr + vr
        omer = omega.rad()
        ir = i.rad()
        x = r * (cos(omer) * cos(ur) - sin(omer) * sin(ur) * cos(ir))
        y = r * (sin(omer) * cos(ur) + cos(omer) * sin(ur) * cos(ir))
        z = r * sin(ir) * sin(ur)
        lon = atan2(y, x)
        lat = atan2(z, sqrt(x * x + y * y))
        return Angle(lon, radians=True), Angle(lat, radians=True)


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's show some uses of Minor class
    print("\n" + 35 * "*")
    print("*** Use of Minor class")
    print(35 * "*" + "\n")

    # Let's compute the equatorial coordinates of comet Encke
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
    print_me("Right ascension", ra.ra_str(n_dec=1))     # 10h 34' 13.7''
    print_me("Declination", dec.dms_str(n_dec=0))       # 19d 9' 32.0''
    print_me("Elongation", round(elong, 2))             # 40.51

    print("")

    # Now compute the heliocentric ecliptical coordinates
    lon, lat = minor.heliocentric_ecliptical_position(epoch)
    print_me("Heliocentric ecliptical longitude", lon.dms_str(n_dec=1))
    # 66d 51' 57.8''
    print_me("Heliocentric ecliptical latitude", lat.dms_str(n_dec=1))
    # 11d 56' 14.4''


if __name__ == "__main__":

    main()
