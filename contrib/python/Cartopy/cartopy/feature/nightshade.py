# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import datetime

import numpy as np
import shapely.geometry as sgeom

from .. import crs as ccrs
from . import ShapelyFeature


class Nightshade(ShapelyFeature):
    def __init__(self, date=None, delta=0.1, refraction=-0.83,
                 color="k", alpha=0.5, **kwargs):
        """
        Shade the darkside of the Earth, accounting for refraction.

        Parameters
        ----------
        date : datetime
            A UTC datetime object used to calculate the position of the sun.
            Default: The current UTC time.
        delta : float
            Stepsize in degrees to determine the resolution of the
            night polygon feature (``npts = 180 / delta``).
        refraction : float
            The adjustment in degrees due to refraction,
            thickness of the solar disc, elevation etc...

        Note
        ----
            Matplotlib keyword arguments can be used when drawing the feature.
            This allows standard Matplotlib control over aspects such as
            'color', 'alpha', etc.

        """
        if date is None:
            date = datetime.datetime.now(datetime.UTC)

        # make sure date is UTC, or naive with respect to time zones
        if date.utcoffset():
            raise ValueError(
                f'datetime instance must be UTC, not {date.tzname()}')

        # Returns the Greenwich hour angle,
        # need longitude (opposite direction)
        lon, lat = _solar_position(date)
        pole_lon = lon
        if lat > 0:
            pole_lat = -90 + lat
            central_lon = 180
        else:
            pole_lat = 90 + lat
            central_lon = 0

        rotated_pole = ccrs.RotatedPole(pole_latitude=pole_lat,
                                        pole_longitude=pole_lon,
                                        central_rotated_longitude=central_lon)

        npts = int(180 / delta)
        x = np.empty(npts * 2)
        y = np.empty(npts * 2)

        # Solve the equation for sunrise/sunset:
        # https://en.wikipedia.org/wiki/Sunrise_equation#Generalized_equation
        # NOTE: In the generalized equation on Wikipedia,
        #       delta == 0. in the rotated pole coordinate system.
        #       Therefore, the max/min latitude is +/- (90+refraction)

        # Fill latitudes up and then down
        y[:npts] = np.linspace(-(90 + refraction), 90 + refraction, npts)
        y[npts:] = y[:npts][::-1]

        # Solve the generalized equation for omega0, which is the
        # angle of sunrise/sunset from solar noon
        # We need to clip the input to arccos to [-1, 1] due to floating
        # point precision and arccos creating nans for values outside
        # of the domain
        arccos_tmp = np.clip(np.sin(np.deg2rad(refraction)) /
                             np.cos(np.deg2rad(y)), -1, 1)
        omega0 = np.rad2deg(np.arccos(arccos_tmp))

        # Fill the longitude values from the offset for midnight.
        # This needs to be a closed loop to fill the polygon.
        # Negative longitudes
        x[:npts] = -(180 - omega0[:npts])
        # Positive longitudes
        x[npts:] = 180 - omega0[npts:]

        kwargs.setdefault('facecolor', color)
        kwargs.setdefault('alpha', alpha)

        geom = sgeom.Polygon(np.column_stack((x, y)))
        return super().__init__(
            [geom], rotated_pole, **kwargs)


def _julian_day(date):
    """
    Calculate the Julian day from an input datetime.

    Parameters
    ----------
    date
        A UTC datetime object.

    Note
    ----
    Algorithm implemented following equations from Chapter 3 (Algorithm 14):
    Vallado, David 'Fundamentals of Astrodynamics and Applications', (2007)

    Julian day epoch is: noon on January 1, 4713 BC (proleptic Julian)
                         noon on November 24, 4714 BC (proleptic Gregorian)

    """
    year = date.year
    month = date.month
    day = date.day
    hour = date.hour
    minute = date.minute
    second = date.second

    # January/February correspond to months 13/14 respectively
    # for the constants to work out properly
    if month < 3:
        month += 12
        year -= 1

    B = 2 - year // 100 + (year // 100) // 4
    C = ((second / 60 + minute) / 60 + hour) / 24

    JD = (int(365.25 * (year + 4716)) + int(30.6001 * (month + 1)) +
          day + B - 1524.5 + C)
    return JD


def _solar_position(date):
    """
    Calculate the latitude and longitude point where the sun is
    directly overhead for the given date.

    Parameters
    ----------
    date
        A UTC datetime object.

    Returns
    -------
    (longitude, latitude) in degrees

    Note
    ----
    Algorithm implemented following equations from Chapter 5 (Algorithm 29):
    Vallado, David 'Fundamentals of Astrodynamics and Applications', (2007)

    """
    # NOTE: Constants are in degrees in the textbook,
    #       so we need to convert the values from deg2rad when taking sin/cos

    # Centuries from J2000
    T_UT1 = (_julian_day(date) - 2451545.0) / 36525

    # solar longitude (deg)
    lambda_M_sun = (280.460 + 36000.771 * T_UT1) % 360

    # solar anomaly (deg)
    M_sun = (357.5277233 + 35999.05034 * T_UT1) % 360

    # ecliptic longitude
    lambda_ecliptic = (lambda_M_sun + 1.914666471 * np.sin(np.deg2rad(M_sun)) +
                       0.019994643 * np.sin(np.deg2rad(2 * M_sun)))

    # obliquity of the ecliptic (epsilon in Vallado's notation)
    epsilon = 23.439291 - 0.0130042 * T_UT1

    # declination of the sun
    delta_sun = np.rad2deg(np.arcsin(np.sin(np.deg2rad(epsilon)) *
                                     np.sin(np.deg2rad(lambda_ecliptic))))

    # Greenwich mean sidereal time (seconds)
    theta_GMST = (67310.54841 +
                  (876600 * 3600 + 8640184.812866) * T_UT1 +
                  0.093104 * T_UT1**2 -
                  6.2e-6 * T_UT1**3)
    # Convert to degrees
    theta_GMST = (theta_GMST % 86400) / 240

    # Right ascension calculations
    numerator = (np.cos(np.deg2rad(epsilon)) *
                 np.sin(np.deg2rad(lambda_ecliptic)) /
                 np.cos(np.deg2rad(delta_sun)))
    denominator = (np.cos(np.deg2rad(lambda_ecliptic)) /
                   np.cos(np.deg2rad(delta_sun)))

    alpha_sun = np.rad2deg(np.arctan2(numerator, denominator))

    # longitude is opposite of Greenwich Hour Angle (GHA)
    # GHA == theta_GMST - alpha_sun
    lon = -(theta_GMST - alpha_sun)
    if lon < -180:
        lon += 360

    return (lon, delta_sun)
