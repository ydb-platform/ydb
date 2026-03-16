# The core functionalty of PyEphem lives in the C-language _libastro
# module, which packages the astronomy routines from XEphem as
# convenient Python types.

import ephem._libastro as _libastro
import re
from datetime import datetime as _datetime
from datetime import timedelta as _timedelta
from datetime import tzinfo as _tzinfo
from math import acos, cos, isnan, pi, sin
from time import localtime as _localtime

__version__ = '4.2.1'

# As a favor, compile a regular expression that our C library would
# really rather not compile for itself.

_libastro._scansexa_split = re.compile(r'''
    \s*:\s*         # A colon optionally surrounded by whitespace,
    |               # or,
    (?<!^)\s+(?!$)  # whitespace not at the start or end of the string.
''', re.X).split

# Various constants.

tau = 6.283185307179586476925287
twopi = pi * 2.
halfpi = pi / 2.
quarterpi = pi / 4.
eighthpi = pi / 8.

degree = pi / 180.
arcminute = degree / 60.
arcsecond = arcminute / 60.
half_arcsecond = arcsecond / 2.
tiny = arcsecond / 360.

c = 299792458.  # exact speed of light in meters/second
meters_per_au = _libastro.meters_per_au
earth_radius = _libastro.earth_radius
moon_radius = _libastro.moon_radius
sun_radius = _libastro.sun_radius

B1900 = 2415020.3135 - _libastro.MJD0
B1950 = 2433282.4235 - _libastro.MJD0
J2000 = _libastro.J2000

_slightly_less_than_zero = -1e-15
_slightly_more_than_pi = pi + 1e-15

# We make available several basic types from _libastro.

Angle = _libastro.Angle
degrees = _libastro.degrees
hours = _libastro.hours

Date = _libastro.Date
hour = 1. / 24.
minute = hour / 60.
second = minute / 60.

default_newton_precision = second / 10.
rise_set_iterations = tuple(range(7))

delta_t = _libastro.delta_t
julian_date = _libastro.julian_date

Body = _libastro.Body
Planet = _libastro.Planet
PlanetMoon = _libastro.PlanetMoon
FixedBody = _libastro.FixedBody
EllipticalBody = _libastro.EllipticalBody
ParabolicBody = _libastro.ParabolicBody
HyperbolicBody = _libastro.HyperbolicBody
EarthSatellite = _libastro.EarthSatellite

readdb = _libastro.readdb
readtle = _libastro.readtle
constellation = _libastro.constellation
separation = _libastro.separation
unrefract = _libastro.unrefract
now = _libastro.now

millennium_atlas = _libastro.millennium_atlas
uranometria = _libastro.uranometria
uranometria2000 = _libastro.uranometria2000

# These classes used to be generated dynamically from the list of tuples
# returned by _libastro.builtin_planets() but that made the list of
# classes invisible to both IDEs and real-life readers of the code.

class Mercury(_libastro.Planet): __planet__ = 0
class Venus(_libastro.Planet): __planet__ = 1
class Mars(_libastro.Planet): __planet__ = 2
class Jupiter(_libastro.Planet): __planet__ = 3
class Saturn(_libastro.Planet): __planet__ = 4
class Uranus(_libastro.Planet): __planet__ = 5
class Neptune(_libastro.Planet): __planet__ = 6
class Pluto(_libastro.Planet): __planet__ = 7
class Sun(_libastro.Planet): __planet__ = 8
class Moon(_libastro.Planet): __planet__ = 9
class Phobos(_libastro.PlanetMoon): __planet__ = 10
class Deimos(_libastro.PlanetMoon): __planet__ = 11
class Io(_libastro.PlanetMoon): __planet__ = 12
class Europa(_libastro.PlanetMoon): __planet__ = 13
class Ganymede(_libastro.PlanetMoon): __planet__ = 14
class Callisto(_libastro.PlanetMoon): __planet__ = 15
class Mimas(_libastro.PlanetMoon): __planet__ = 16
class Enceladus(_libastro.PlanetMoon): __planet__ = 17
class Tethys(_libastro.PlanetMoon): __planet__ = 18
class Dione(_libastro.PlanetMoon): __planet__ = 19
class Rhea(_libastro.PlanetMoon): __planet__ = 20
class Titan(_libastro.PlanetMoon): __planet__ = 21
class Hyperion(_libastro.PlanetMoon): __planet__ = 22
class Iapetus(_libastro.PlanetMoon): __planet__ = 23
class Ariel(_libastro.PlanetMoon): __planet__ = 24
class Umbriel(_libastro.PlanetMoon): __planet__ = 25
class Titania(_libastro.PlanetMoon): __planet__ = 26
class Oberon(_libastro.PlanetMoon): __planet__ = 27
class Miranda(_libastro.PlanetMoon): __planet__ = 28

# We now replace two of the classes we have just created, because
# _libastro actually provides separate types for two of the bodies.

Jupiter = _libastro.Jupiter
Saturn = _libastro.Saturn
Moon = _libastro.Moon

# Angles.

def _plusminus_pi(angle):
    return (angle - pi) % tau - pi

# Newton's method.

def newton(f, x0, x1, precision=default_newton_precision):
    """Return an x-value at which the given function reaches zero.

    Stops and declares victory once the x-value is within ``precision``
    of the solution, which defaults to a half-second of clock time.

    """
    f0, f1 = f(x0), f(x1)
    while f1 and abs(x1 - x0) > precision and f1 != f0:
        x0, x1 = x1, x1 + (x1 - x0) / (f0/f1 - 1)
        f0, f1 = f1, f(x1)
    return x1

# Find equinoxes and solstices.

_sun = Sun()                    # used for computing equinoxes

def holiday(d0, motion, offset):
    """Function that assists the finding of equinoxes and solstices."""

    def f(d):
        _sun.compute(d)
        return (_sun.ra + eighthpi) % quarterpi - eighthpi
    d0 = Date(d0)
    _sun.compute(d0)
    angle_to_cover = motion - (_sun.ra + offset) % motion
    if abs(angle_to_cover) < tiny:
        angle_to_cover = motion
    d = d0 + 365.25 * angle_to_cover / twopi
    return date(newton(f, d, d + hour))

def previous_vernal_equinox(date):
    """Return the date of the previous vernal equinox."""
    return holiday(date, -twopi, 0)

def next_vernal_equinox(date):
    """Return the date of the next vernal equinox."""
    return holiday(date, twopi, 0)

def previous_summer_solstice(date):
    """Return the date of the previous summer solstice."""
    return holiday(date, -twopi, pi + halfpi)

def next_summer_solstice(date):
    """Return the date of the next summer solstice."""
    return holiday(date, twopi, pi + halfpi)

def previous_autumnal_equinox(date):
    """Return the date of the previous autumnal equinox."""
    return holiday(date, -twopi, pi)

def next_autumnal_equinox(date):
    """Return the date of the next autumnal equinox."""
    return holiday(date, twopi, pi)

def previous_winter_solstice(date):
    """Return the date of the previous winter solstice."""
    return holiday(date, -twopi, halfpi)

def next_winter_solstice(date):
    """Return the date of the next winter solstice."""
    return holiday(date, twopi, halfpi)

# Common synonyms.

next_spring_equinox = next_vernal_equinox
previous_spring_equinox = previous_vernal_equinox

next_fall_equinox = next_autumn_equinox = next_autumnal_equinox
previous_fall_equinox = previous_autumn_equinox = previous_autumnal_equinox

# More-general functions that find any equinox or solstice.

def previous_equinox(date):
    """Return the date of the previous equinox."""
    return holiday(date, -pi, 0)

def next_equinox(date):
    """Return the date of the next equinox."""
    return holiday(date, pi, 0)

def previous_solstice(date):
    """Return the date of the previous solstice."""
    return holiday(date, -pi, halfpi)

def next_solstice(date):
    """Return the date of the next solstice."""
    return holiday(date, pi, halfpi)

# Find phases of the Moon.

_moon = Moon()                  # used for computing Moon phases

def _find_moon_phase(d0, motion, target):
    """Function that assists the finding of moon phases."""

    def f(d):
        _sun.compute(d)
        _moon.compute(d)
        slon = _libastro.eq_ecl(d, _sun.g_ra, _sun.g_dec)[0]
        mlon = _libastro.eq_ecl(d, _moon.g_ra, _moon.g_dec)[0]
        return (mlon - slon - antitarget) % twopi - pi
    antitarget = target + pi
    d0 = Date(d0)
    f0 = f(d0)
    angle_to_cover = (- f0) % motion
    if abs(angle_to_cover) < tiny:
        angle_to_cover = motion
    d = d0 + 29.53 * angle_to_cover / twopi
    return date(newton(f, d, d + hour))

def previous_new_moon(date):
    """Return the date of the previous New Moon."""
    return _find_moon_phase(date, -twopi, 0)

def next_new_moon(date):
    """Return the date of the next New Moon."""
    return _find_moon_phase(date, twopi, 0)

def previous_first_quarter_moon(date):
    """Return the date of the previous First Quarter Moon."""
    return _find_moon_phase(date, -twopi, halfpi)

def next_first_quarter_moon(date):
    """Return the date of the next First Quarter Moon."""
    return _find_moon_phase(date, twopi, halfpi)

def previous_full_moon(date):
    """Return the date of the previous Full Moon."""
    return _find_moon_phase(date, -twopi, pi)

def next_full_moon(date):
    """Return the date of the next Full Moon."""
    return _find_moon_phase(date, twopi, pi)

def previous_last_quarter_moon(date):
    """Return the date of the previous Last Quarter Moon."""
    return _find_moon_phase(date, -twopi, pi + halfpi)

def next_last_quarter_moon(date):
    """Return the date of the next Last Quarter Moon."""
    return _find_moon_phase(date, twopi, pi + halfpi)

# We provide a Python extension to our _libastro "Observer" class that
# can search for circumstances like transits.

class CircumpolarError(ValueError): pass
class NeverUpError(CircumpolarError): pass
class AlwaysUpError(CircumpolarError): pass

def describe_riset_search(method):
    if method.__doc__ is None:
        return method

    method.__doc__ += """, returning its date.

    The search starts at the `date` of this `Observer` and is limited to
    the single circuit of the sky, from antitransit to antitransit, that
    the `body` was in the middle of describing at that date and time.
    If the body did not, in fact, cross the horizon in the direction you
    are asking about during that particular circuit, then the search
    must raise a `CircumpolarError` exception like `NeverUpError` or
    `AlwaysUpError` instead of returning a date.

    """
    return method

class Observer(_libastro.Observer):
    """A location on earth for which positions are to be computed.

    An `Observer` instance allows you to compute the positions of
    celestial bodies as seen from a particular latitude and longitude on
    the Earth's surface.  The constructor takes no parameters; instead,
    set its attributes once you have created it.  Defaults:

    `date` - the moment the `Observer` is created
    `lat` - zero latitude
    `lon` - zero longitude
    `elevation` - 0 meters above sea level
    `horizon` - 0 degrees
    `epoch` - J2000
    `temp` - 15 degrees Celsius
    `pressure` - 1010 mBar

    """
    __slots__ = [ 'name' ]
    elev = _libastro.Observer.elevation

    def copy(self):
        o = self.__class__()
        o.date = self.date
        o.lat = self.lat
        o.lon = self.lon
        o.elev = self.elev
        o.horizon = self.horizon
        o.epoch = self.epoch
        o.temp = self.temp
        o.pressure = self.pressure
        return o

    __copy__ = copy

    def __repr__(self):
        """Return a useful textual representation of this Observer."""
        return ('<ephem.Observer date=%r epoch=%r'
                " lon='%s' lat='%s' elevation=%sm"
                ' horizon=%s temp=%sC pressure=%smBar>'
                % (str(self.date), str(self.epoch),
                   self.lon, self.lat, self.elevation,
                   self.horizon, self.temp, self.pressure))

    def compute_pressure(self):
        """Set the atmospheric pressure for the current elevation."""
        # Formula from the ISA Standard Atmosphere
        self.pressure = (1013.25 * (1 - 0.0065 * self.elevation / 288.15)
                         ** 5.2558761132785179)

    def _compute_transit(self, body, start, sign, offset):
        """Internal function used to compute transits."""

        if isinstance(body, EarthSatellite):
            raise TypeError(
                'the next and previous transit methods do not'
                ' support earth satellites because of their speed;'
                ' please use the higher-resolution next_pass() method'
                )

        def f(d):
            self.date = d
            body.compute(self)
            return degrees(offset - sidereal_time() + body.g_ra).znorm

        if start is not None:
            self.date = start
        sidereal_time = self.sidereal_time
        body.compute(self)
        ha = sidereal_time() - body.g_ra
        ha_to_move = (offset - ha) % (sign * twopi)
        if abs(ha_to_move) < tiny:
            ha_to_move = sign * twopi
        d = self.date + ha_to_move / twopi
        result = Date(newton(f, d, d + minute))
        return result

    def _previous_transit(self, body, start=None):
        """Find the previous passage of a body across the meridian."""

        return self._compute_transit(body, start, -1., 0.)

    def _next_transit(self, body, start=None):
        """Find the next passage of a body across the meridian."""

        return self._compute_transit(body, start, +1., 0.)

    def _previous_antitransit(self, body, start=None):
        """Find the previous passage of a body across the anti-meridian."""

        return self._compute_transit(body, start, -1., pi)

    def _next_antitransit(self, body, start=None):
        """Find the next passage of a body across the anti-meridian."""

        return self._compute_transit(body, start, +1., pi)

    def previous_transit(self, body, start=None):
        """Find the previous passage of a body across the meridian."""

        original_date = self.date
        d = self._previous_transit(body, start)
        self.date = original_date
        return d

    def next_transit(self, body, start=None):
        """Find the next passage of a body across the meridian."""

        original_date = self.date
        d = self._next_transit(body, start)
        self.date = original_date
        return d

    def previous_antitransit(self, body, start=None):
        """Find the previous passage of a body across the anti-meridian."""

        original_date = self.date
        d = self._previous_antitransit(body, start)
        self.date = original_date
        return d

    def next_antitransit(self, body, start=None):
        """Find the next passage of a body across the anti-meridian."""

        original_date = self.date
        d = self._next_antitransit(body, start)
        self.date = original_date
        return d

    def disallow_circumpolar(self, declination):
        """Raise an exception if the given declination is circumpolar.

        Raises NeverUpError if an object at the given declination is
        always below this Observer's horizon, or AlwaysUpError if such
        an object would always be above the horizon.

        """
        if abs(self.lat - declination) >= halfpi:
            raise NeverUpError('The declination %s never rises'
                               ' above the horizon at latitude %s'
                               % (declination, self.lat))
        if abs(self.lat + declination) >= halfpi:
            raise AlwaysUpError('The declination %s is always'
                                ' above the horizon at latitude %s'
                                % (declination, self.lat))

    @describe_riset_search
    def previous_rising(self, body, start=None, use_center=False):
        """Search for the given body's previous rising"""
        return self._find_rise_or_set(body, start, use_center, -1, True)

    @describe_riset_search
    def previous_setting(self, body, start=None, use_center=False):
        """Search for the given body's previous setting"""
        return self._find_rise_or_set(body, start, use_center, -1, False)

    @describe_riset_search
    def next_rising(self, body, start=None, use_center=False):
        """Search for the given body's next rising"""
        return self._find_rise_or_set(body, start, use_center, +1, True)

    @describe_riset_search
    def next_setting(self, body, start=None, use_center=False):
        """Search for the given body's next setting"""
        return self._find_rise_or_set(body, start, use_center, +1, False)

    def _find_rise_or_set(self, body, start, use_center, direction, do_rising):
        if isinstance(body, EarthSatellite):
            raise TypeError(
                'the rising and settings methods do not'
                ' support earth satellites because of their speed;'
                ' please use the higher-resolution next_pass() method'
                )

        original_pressure = self.pressure
        original_date = self.date
        try:
            self.pressure = 0.0  # otherwise geometry doesn't work
            if start is not None:
                self.date = start
            prev_ha = None
            for _ in rise_set_iterations:
                if isnan(self.date):
                    raise ValueError('cannot find a next rising or setting'
                                     ' if the date is NaN')
                body.compute(self)
                horizon = self.horizon
                if not use_center:
                    horizon -= body.radius
                if original_pressure:
                    horizon = unrefract(original_pressure, self.temp, horizon)
                abs_target_ha = self._target_hour_angle(body, horizon)
                if do_rising:
                    target_ha = - abs_target_ha  # rises in east (az 0-180)
                else:
                    target_ha = abs_target_ha    # sets in west (az 180-360)
                ha = body.ha
                difference = target_ha - ha
                if prev_ha is None:
                    difference %= tau  # force angle to be positive
                    if direction < 0:
                        difference -= tau
                    bump = difference / tau
                    if abs(bump) < default_newton_precision:
                        # Already at target event: move forward to next one.
                        bump += direction
                else:
                    difference = _plusminus_pi(difference)
                    bump = difference / tau
                if abs(bump) < default_newton_precision:
                    break
                self.date += bump
                prev_ha = ha

            if abs_target_ha == _slightly_more_than_pi:
                raise AlwaysUpError('%r is above the horizon at %s'
                                    % (body.name, self.date))

            if abs_target_ha == _slightly_less_than_zero:
                raise NeverUpError('%r is below the horizon at %s'
                                   % (body.name, self.date))

            return self.date
        finally:
            if self.pressure != original_pressure:
                self.pressure = original_pressure
                body.compute(self)
            self.date = original_date

    def _target_hour_angle(self, body, alt):
        lat = self.lat
        dec = body.dec
        arg = (sin(alt) - sin(lat) * sin(dec)) / (cos(lat) * cos(dec))

        if arg < -1.0:
            return _slightly_more_than_pi
        elif arg > 1.0:
            return _slightly_less_than_zero

        return acos(arg)

    def next_pass(self, body, singlepass=True):
        """Return the next rising, culmination, and setting of a satellite.

        If singlepass is True, return next consecutive set of
        ``(rising, culmination, setting)``.

        If singlepass is False, return
        ``(next_rising, next_culmination, next_setting)``.

        """
        if not isinstance(body, EarthSatellite):
            raise TypeError(
                'the next_pass() method is only for use with'
                ' EarthSatellite objects because of their high speed'
                )

        result = _libastro._next_pass(self, body)
        # _libastro behavior is singlepass=False
        if ((not singlepass)
                or (None in result)
                or (result[4] >= result[0])):
            return result
        # retry starting just before next_rising
        obscopy = self.copy()
        # Almost always 1 minute before next_rising except
        # in pathological case where set came immediately before rise
        obscopy.date = result[0] - min(1.0/1440,
                            (result[0] - result[4])/2)
        result = _libastro._next_pass(obscopy, body)
        if result[0] <= result[2] <= result[4]:
            return result
        raise ValueError("this software is having trouble with those satellite parameters")


del describe_riset_search

# Time conversion.

def _convert_to_seconds_and_microseconds(date):
    """Converts a PyEphem date into seconds"""
    microseconds = int(round(24 * 60 * 60 * 1000000 * date))
    seconds, microseconds = divmod(microseconds, 1000000)
    seconds -= 2209032000  # difference between epoch 1900 and epoch 1970
    return seconds, microseconds


def localtime(date):
    """Convert a PyEphem date into naive local time, returning a Python datetime."""
    seconds, microseconds = _convert_to_seconds_and_microseconds(date)
    y, m, d, H, M, S, wday, yday, isdst = _localtime(seconds)
    return _datetime(y, m, d, H, M, S, microseconds)


class _UTC(_tzinfo):
    ZERO = _timedelta(0)
    def utcoffset(self, dt):
        return self.ZERO
    def dst(self, dt):
        return self.ZERO
    def __repr__(self):
        return "<ephem.UTC>"


UTC = _UTC()


def to_timezone(date, tzinfo):
    """"Convert a PyEphem date into a timezone aware Python datetime representation."""
    seconds, microseconds = _convert_to_seconds_and_microseconds(date)
    date = _datetime.fromtimestamp(seconds, tzinfo)
    date = date.replace(microsecond=microseconds)
    return date

# Coordinate transformations.

class Coordinate(object):
    def __init__(self, *args, **kw):

        # Accept an optional "epoch" keyword argument.

        epoch = kw.pop('epoch', None)
        if epoch is not None:
            self.epoch = epoch = Date(epoch)
        if kw:
            raise TypeError('"epoch" is the only keyword argument'
                            ' you can use during %s instantiation'
                            % (type(self).__name__))

        # Interpret a single-argument initialization.

        if len(args) == 1:
            a = args[0]

            if isinstance(a, Body):
                a = Equatorial(a.a_ra, a.a_dec, epoch = a.a_epoch)

            for cls in (Equatorial, Ecliptic, Galactic):
                if isinstance(a, cls):

                    # If the user omitted an "epoch" keyword, then
                    # use the epoch of the other object.

                    if epoch is None:
                        self.epoch = epoch = a.epoch

                    # If we are initialized from another of the same
                    # kind of coordinate and epoch, simply copy the
                    # coordinates and epoch into this new object.

                    if isinstance(self, cls) and epoch == a.epoch:
                        self.set(*a.get())
                        return

                    # Otherwise, convert.

                    ra, dec = a.to_radec()
                    if epoch != a.epoch:
                        ra, dec = _libastro.precess(
                            a.epoch, epoch, ra, dec
                            )
                    self.from_radec(ra, dec)
                    return

            raise TypeError(
                'a single argument used to initialize %s() must be either'
                ' a coordinate or a Body, not an %r' % (type(a).__name__,)
                )

        # Two arguments are interpreted as (ra, dec) or (lon, lat).

        elif len(args) == 2:
            self.set(*args)
            if epoch is None:
                self.epoch = epoch = Date(J2000)

        else:
            raise TypeError(
                'to initialize %s you must pass either a Body,'
                ' another coordinate, or two coordinate values,'
                ' but not: %r' % (type(self).__name__, args,)
                )

class Equatorial(Coordinate):
    """An equatorial sky coordinate in right ascension and declination."""

    def get(self):
        return self.ra, self.dec

    def set(self, ra, dec):
        self.ra, self.dec = hours(ra), degrees(dec)

    to_radec = get
    from_radec = set

class LonLatCoordinate(Coordinate):
    """A coordinate that is measured with a longitude and latitude."""

    def set(self, lon, lat):
        self.lon, self.lat = degrees(lon), degrees(lat)

    def get(self):
        return self.lon, self.lat

    @property
    def long(self):
        return self.lon

    @long.setter
    def long(self, value):
        self.lon = value

class Ecliptic(LonLatCoordinate):
    """An ecliptic latitude and longitude."""

    def to_radec(self):
        return _libastro.ecl_eq(self.epoch, self.lon, self.lat)

    def from_radec(self, ra, dec):
        self.lon, self.lat = _libastro.eq_ecl(self.epoch, ra, dec)

class Galactic(LonLatCoordinate):
    """A galactic latitude and longitude."""

    def to_radec(self):
        return _libastro.gal_eq(self.epoch, self.lon, self.lat)

    def from_radec(self, ra, dec):
        self.lon, self.lat = _libastro.eq_gal(self.epoch, ra, dec)

# For backwards compatibility, provide lower-case names for our Date
# and Angle classes, and also allow "Lon" to be spelled "Long".

date = Date
angle = Angle
LongLatCoordinate = LonLatCoordinate

# Catalog boostraps.  Each of these functions imports a catalog
# module, then replaces itself with the function of the same name that
# lives inside of the catalog.

def star(name, *args, **kwargs):
    """Load the stars database and return a star."""
    global star
    import ephem.stars
    star = ephem.stars.star
    return star(name, *args, **kwargs)

def city(name):
    """Load the cities database and return a city."""
    global city
    import ephem.cities
    city = ephem.cities.city
    return city(name)
