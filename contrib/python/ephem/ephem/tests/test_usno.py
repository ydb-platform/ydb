#!/usr/bin/env python
# -*- coding: utf-8 -*-

import glob, os.path, re, sys, traceback, unittest
from datetime import datetime
from time import strptime
import ephem

# Since users might be in another locale, we have to translate the
# month into an integer on our own.
month_ints = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
    }

# Read an ephemeris from the United States Naval Observatory, and
# confirm that PyEphem returns the same measurements to within one
# arcsecond of accuracy.

class Datum(object):
    pass

def interpret_observer(line):
    """Return the Observer represented by a line from a USNO file.
              Location:  W 84°24'36.0", N33°45'36.0",   322m
    """
    line = line.replace('W ', 'W').replace('E ', 'E')
    line = line.replace('N ', 'N').replace('S ', 'S')
    fields = line.split()
    observer = ephem.Observer()
    observer.lon = ':'.join(re.findall(r'[0-9.]+', fields[1]))
    observer.lat = ':'.join(re.findall(r'[0-9.]+', fields[2]))
    if fields[1].startswith('W'):
        observer.lon *= -1
    if fields[2].startswith('S'):
        observer.lat *= -1
    observer.elevation = float(fields[-1][:-1])
    setup_horizon(observer)
    return observer

def setup_horizon(observer):
    observer.pressure = 0
    observer.horizon = '-0:34'  # per USNO definition

def standard_parse(line):
    """Read the date, RA, and dec from a USNO data file line."""

    fields = re.split(r'   +', line)
    datestrfields = fields[0][:-2].split()
    datestrfields[1] = str(month_ints[datestrfields[1]])
    datestr = ' '.join(datestrfields)
    dt = datetime(*strptime(datestr, "%Y %m %d %H:%M:%S")[0:6])
    date = ephem.Date(dt)
    ra = ephem.hours(':'.join(fields[1].split()))
    sign, mag = fields[2].split(None, 1)
    dec = ephem.degrees(sign + ':'.join(mag.split()))
    return date, ra, dec

def is_near(n, m, error=ephem.arcsecond, move=None):
    """Raise an exception if two values are not within an arcsecond."""

    if abs(n - m) > error:
        at = '' if move is None else '%s at ' % move
        msg = 'the USNO says %s%s but PyEphem says %s' % (at, n, m)
        raise AssertionError(msg)

class Trial(object):
    def select_body(self, name):
        """Return solar system bodies and stars for USNO computations."""
        if hasattr(ephem, name):
            return getattr(ephem, name)()
        else:
            return ephem.star(name)

    def examine_content(self):
        """Return the object named on the first line of the file.

        Since most USNO files name a heavenly body on their first
        line, this generic examine_content looks for it and sets
        "self.body" accordingly.  If when you subclass Trial you are
        dealing with a file that does not follow this convention, then
        both override examine_content with one of your own, and
        neglect to call this one.

        """
        firstline = self.lines[0]
        name = firstline.strip()
        self.body = self.select_body(name)

    def finish(self):
        """Conclude the test.

        By overriding this function, the author of a Trial can do
        processing at the end of his test run.  Commonly, this is used
        when check_data_line() just stores what it finds in each data
        line, and the results need to be checked as a unit.

        """

    def check_line(self, line):
        if line[0].isdigit():
            self.check_data_line(line)

    def run(self, content):
        def report_error():
            exc = RuntimeError(
                'USNO Test file %r line %d raised exception:\n%s'
                % (self.path, self.lineno + 1, traceback.format_exc()))
            exc.__cause__ = None
            return exc

        self.content = content
        self.lines = content.split('\n')
        self.examine_content()
        for self.lineno in range(len(self.lines)):
            line = self.lines[self.lineno]
            if line.strip():
                try:
                    self.check_line(line)
                except:
                    raise report_error()
        try:
            self.finish()
        except Exception as e:
            raise report_error()

# Check an "Astrometric Positions" file.

class Astrometric_Trial(Trial):
    @classmethod
    def matches(cls, content):
        return 'Astrometric Positions' in content

    def check_data_line(self, line):
        date, ra, dec = standard_parse(line)

        self.body.compute(date)
        is_near(ra, self.body.a_ra)
        is_near(dec, self.body.a_dec)

# Check an "Apparent Geocentric Positions" file.

class Apparent_Geocentric_Trial(Trial):
    @classmethod
    def matches(cls, content):
        return 'Apparent Geocentric Positions' in content

    def check_data_line(self, line):
        date, ra, dec = standard_parse(line)

        self.body.compute(date)
        is_near(ra, self.body.g_ra)
        is_near(dec, self.body.g_dec)

        # The values we get should not depend on our epoch.

        self.body.compute(date, epoch=date)
        is_near(ra, self.body.g_ra)
        is_near(dec, self.body.g_dec)

        # Since we only provided the body with a date, and not an
        # Observer, the computation should have been geocentric, and
        # thus the "ra" and "dec" fields should have had the apparent
        # positions copied into them as well.

        assert self.body.g_ra == self.body.ra
        assert self.body.g_dec == self.body.dec

# Check an "Apparent Topocentric Positions" file.

class Apparent_Topocentric_Trial(Trial):
    @classmethod
    def matches(cls, content):
        return 'Apparent Topocentric Positions' in content

    def examine_content(self):
        Trial.examine_content(self) # process object name on first line
        for line in self.lines:
            if 'Location:' in line:
                self.observer = interpret_observer(line.strip())

    def check_data_line(self, line):
        date, ra, dec = standard_parse(line)
        self.observer.date = date
        self.body.compute(self.observer)
        is_near(ra, self.body.ra)
        is_near(dec, self.body.dec)

# Check several days of risings, settings, and transits.

class Rise_Transit_Set_Trial(Trial):
    def __init__(self):
        self.data = []

    @classmethod
    def matches(cls, content):
        return 'Rise  Az.       Transit Alt.       Set  Az.' in content

    def examine_content(self):
        Trial.examine_content(self) # process object name on first line
        for line in self.lines:
            if 'Location:' in line:
                self.observer = interpret_observer(line.strip())
            elif 'Time Zone:' in line:
                if 'west of Greenwich' in line:
                    self.tz = - int(line.split()[2].strip('h')) * ephem.hour

    def check_data_line(self, line):
        """Determine if our rise/transit/set data match the USNO's.

The file looks something like:
      Date               Rise  Az.       Transit Alt.       Set  Az.
     (Zone)
                          h  m   °         h  m  °          h  m   °
2006 Apr 29 (Sat)        10:11 100        15:45 42S        21:19 260
2006 Apr 30 (Sun)        10:07 100        15:41 42S        21:15 260
2006 May 01 (Mon)        10:03 100        15:37 42S        21:11 260

        Here we save the data in self.data for examination by finish().
        """
        datum = Datum()
        fields = line.split()
        fields[1] = str(month_ints[fields[1]])

        dt = datetime(*strptime(' '.join(fields[0:3]), "%Y %m %d")[0:3])
        midnight = ephem.Date(ephem.Date(dt) - self.tz)
        datum.midnight = midnight

        def parse(fields, n):
            if len(fields) < n + 2:
                return None, None
            (timestr, anglestr) = fields[n:n+2]
            if timestr == '*****':
                return None, None
            h, m = [ int(s) for s in timestr.split(':') ]
            date = ephem.Date(midnight + h * ephem.hour + m * ephem.minute)
            angle = ephem.degrees(anglestr.strip('NS'))
            return date, angle

        datum.rising, datum.rising_az = parse(fields, 4)
        datum.transit, datum.transit_alt = parse(fields, 6)
        datum.setting, datum.setting_az = parse(fields, 8)

        self.data.append(datum)

    def finish(self):
        """Go back through and verify the data we have saved."""

        observer = self.observer
        body = self.body
        data = self.data

        # This helper function, given an attribute name of
        # "rising", "transit", or "setting", verifies that the
        # observer and body are located at the time and place that
        # the named transit attributes say they should be.

        def check(attr):
            is_near(getattr(datum, attr), observer.date, ephem.minute, attr)
            if attr == 'transit':
                attr += '_alt'
                our_angle = body.alt
            else:
                attr += '_az'
                our_angle = body.az
            is_near(getattr(datum, attr), our_angle, ephem.degree, attr)

        # Make sure that, from both the midnight starting the day and
        # the midnight ending the day, we can move to the events that
        # happen during each day.

        for i in range(len(data)):
            datum = data[i]

            for attr in ('rising', 'transit', 'setting'):
                datum_date = getattr(datum, attr)

                if datum_date is not None:
                    next_func = getattr(observer, 'next_' + attr)
                    previous_func = getattr(observer, 'previous_' + attr)

                    observer.date = next_func(body, datum.midnight)
                    check(attr)
                    observer.date = previous_func(body, datum.midnight + 1)
                    check(attr)

        # Make sure we can also jump along similar events.

        for attr in ('rising', 'transit', 'setting'):
            prev = None
            for i in range(len(data)):
                datum = data[i]
                datum_date = getattr(datum, attr)
                if datum_date is None:
                    prev = None
                else:
                    if prev is None:
                        observer.date = datum.midnight
                    observer.date = getattr(observer, 'next_' + attr)(body)
                    check(attr)
                    prev = observer.date

        for attr in ('rising', 'transit', 'setting'):
            next = None
            for i in range(len(data) - 1, 0, -1):
                datum = data[i]
                datum_date = getattr(datum, attr)
                if datum_date is None:
                    next = None
                else:
                    if next is None:
                        observer.date = datum.midnight + 1
                    observer.date = getattr(observer, 'previous_' + attr)(body)
                    check(attr)
                    next = observer.date

# Check a whole year of "Rise and Set for the * for *" file.

class Rise_Set_Trial(Trial):
    @classmethod
    def matches(cls, content):
        return 'Rise and Set for' in content

    def examine_content(self):
        name, year = re.search(r'Rise and Set for (.*) for (\d+)',
                               self.content).groups()
        if name.startswith('the '):
            name = name[4:]
        self.body = self.select_body(name)
        self.year = int(year)

        longstr, latstr = re.search(r'Location: (.\d\d\d \d\d), (.\d\d \d\d)',
                                    self.content).groups()
        longstr = longstr.replace(' ', ':').replace('W', '-').strip('E')
        latstr = latstr.replace(' ', ':').replace('S', '-').strip('N')

        self.observer = o = ephem.Observer()
        o.lat = latstr
        o.lon = longstr
        o.elevation = 0
        setup_horizon(o)

        self.error = ephem.minute # error we tolerate in predicted time

    def check_data_line(self, line):
        """Determine whether PyEphem rising times match those of the USNO.
        Lines from this data file look like:

               Jan.       Feb.       Mar.       Apr.       May    ...
        Day Rise  Set  Rise  Set  Rise  Set  Rise  Set  Rise  Set ...
             h m  h m   h m  h m   h m  h m   h m  h m   h m  h m ...
        01  0743 1740  0734 1808  0707 1834  0626 1858  0549 1921 ...
        02  0743 1741  0734 1809  0705 1835  0624 1859  0548 1922 ...

        Note that times in this kind of file are in Eastern Standard
        Time, so we have to variously add and subtract fives hours
        from our UT times to perform a valid comparison.
        """

        def two_digit_compare(event, usno_digits):
            usno_hrmin = usno_digits[:2] + ':' + usno_digits[2:]
            usno_date = ephem.Date('%s %s' % (usno_datestr, usno_hrmin))

            difference = o.date - (usno_date + 5 * ephem.hour)
            if abs(difference) > self.error:
                raise ValueError('On %s, the USNO thinks that %s %s'
                                 ' happens at %r but PyEphem finds %s'
                                 % (usno_datestr, body.name, event,
                                    usno_hrmin, o.date))

        o = self.observer
        year = self.year
        body = self.body
        day = int(line[0:2])
        for month in [ int(m) for m in range(1,13) ]:
            usno_datestr = '%s/%s/%s' % (year, month, day)

            offset = month * 11 - 7 # where month is in line
            usno_rise = line[offset : offset + 4].strip()
            usno_set = line[offset + 5 : offset + 9].strip()

            start_of_day = ephem.Date((year, month, day)) + 5 * ephem.hour

            o.date = start_of_day
            body.compute(o)

            if usno_rise:
                o.date = start_of_day + 1
                o.date = o.previous_rising(body)
                two_digit_compare('rise', usno_rise)

                o.date = start_of_day
                o.date = o.next_rising(body)
                two_digit_compare('rise', usno_rise)

            if usno_set:
                o.date = start_of_day + 1
                o.date = o.previous_setting(body)
                two_digit_compare('set', usno_set)

                o.date = start_of_day
                o.date = o.next_setting(body)
                two_digit_compare('set', usno_set)

# Moon phases.

class Moon_Phases(Trial):
    @classmethod
    def matches(cls, content):
        return 'Phases of the Moon' in content

    def examine_content(self):
        self.year = int(self.lines[0].split()[0])
        self.date = ephem.Date((self.year,)) # Jan 1st
        self.past_heading = False

    def check_line(self, line):
        #         d  h  m         d  h  m         d  h  m         d  h  m"
        #
        #                                    Jan  2  9 02    Jan 10 11 50
        #    Jan 17 21 19    Jan 24 13 58    Feb  1  2 21    Feb  9  7 35

        # Skip the heading of the file.

        if not self.past_heading:
            if 'd  h  m' in line:
                self.past_heading = True
            return

        # Read the phases.

        for phase, datestr in (('new', line[4:16]),
                               ('first_quarter', line[20:32]),
                               ('full', line[36:48]),
                               ('last_quarter', line[52:64]),
                               ):
            if datestr.strip():
                func = getattr(ephem, 'next_%s_moon' % phase)
                self.verify(func, datestr)
                self.date += self.date % 29. # jump ahead arbitrary # days
                func = getattr(ephem, 'previous_%s_moon' % phase)
                self.verify(func, datestr)

    def verify(self, func, datestr):
        datestrfields = datestr.split()
        datestrfields[0] = str(month_ints[datestrfields[0]])
        datestr = ' '.join(datestrfields)
        dt = datetime(*strptime('%d %s' % (self.year, datestr),
                                "%Y %m %d %H %M")[0:5])
        date = ephem.Date(dt)
        self.date = func(self.date) # advance to next phase
        is_near(date, self.date, ephem.minute)

# The actual function that drives the test.

class Mixin(object):
    def test_usno(self):

        f = open(self.path, 'rb')
        try:
            content = f.read()
        finally:
            f.close()

        content = content.decode('utf-8')
        if sys.version_info < (3, 0):
            content = content.encode('ascii', 'replace')

        g = globals()
        for obj in g.values():
            if (isinstance(obj, type) and issubclass(obj, Trial)
                and hasattr(obj, 'matches') and obj.matches(content)):
                trial_class = obj
                trial = trial_class()
                trial.path = self.path
                trial.run(content)
                return
        raise ValueError('Cannot find a test trial that recognizes %r'
                         % self.path)

#
# Auto-detect files
#

#for path in glob.glob(os.path.dirname(__file__) + '/usno/*.txt'):
#    name = os.path.basename(path).replace('.txt', '')
#    exec('class Test_%s(unittest.TestCase, Mixin): path = %r' % (name, path))
#del path, name

from yatest.common import source_path
txts = source_path('contrib/python/ephem/ephem/tests')
class T1(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'appgeo_deneb.txt')
class T2(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'appgeo_jupiter.txt')
class T3(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'appgeo_moon.txt')
class T4(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'appgeo_sun.txt')
class T5(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'apptopo_deneb.txt')
class T6(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'apptopo_moon.txt')
class T7(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'apptopo_sun.txt')
class T8(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'astrom_antares.txt')
class T9(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'astrom_mercury.txt')
class T10(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'astrom_neptune.txt')
class T11(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'moon_phases.txt')
class T12(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'riset_moon.txt')
class T13(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'riset_sun.txt')
class T14(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'risettran_moon.txt')
class T15(unittest.TestCase, Mixin): path = os.path.join(txts, 'usno', 'risettran_rigel.txt')
