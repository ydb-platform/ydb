# rfc3339.py -- Implementation of the majority of RFC 3339 for python.
# Copyright (c) 2008, 2009, 2010 LShift Ltd. <query@lshift.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
Implementation of the majority of http://www.ietf.org/rfc/rfc3339.txt.

Use datetime.datetime.isoformat() as an inverse of the various parsing
routines in this module.

Limitations, with respect to RFC 3339:

 - Section 4.3, "Unknown Local Offset Convention", is not implemented.

 - Section 5.6, "Internet Date/Time Format", is the ONLY supported format
   implemented by the various parsers in this module. (Section 5.6 is
   reproduced in its entirety below.)

 - Section 5.7, "Restrictions", is left to the datetime.datetime constructor
   to implement, with the exception of limits on timezone
   minutes-east-of-UTC magnitude. In particular, leap seconds are not
   addressed by this module. (And it appears that they are not supported
   by datetime, either.)

Potential Improvements:

 - Support for leap seconds. (There's a table of them in RFC 3339 itself,
   and http://tf.nist.gov/pubs/bulletin/leapsecond.htm updates monthly.)

Extensions beyond the RFC:

 - Accepts (but will not generate) dates formatted with a time-offset
   missing a colon. (Implemented because Facebook are generating
   broken RFC 3339 timestamps.)

Here's an excerpt from RFC 3339 itself:

5.6. Internet Date/Time Format

   The following profile of ISO 8601 [ISO8601] dates SHOULD be used in
   new protocols on the Internet.  This is specified using the syntax
   description notation defined in [ABNF].

   date-fullyear   = 4DIGIT
   date-month      = 2DIGIT  ; 01-12
   date-mday       = 2DIGIT  ; 01-28, 01-29, 01-30, 01-31 based on
                             ; month/year
   time-hour       = 2DIGIT  ; 00-23
   time-minute     = 2DIGIT  ; 00-59
   time-second     = 2DIGIT  ; 00-58, 00-59, 00-60 based on leap second
                             ; rules
   time-secfrac    = "." 1*DIGIT
   time-numoffset  = ("+" / "-") time-hour ":" time-minute
   time-offset     = "Z" / time-numoffset

   partial-time    = time-hour ":" time-minute ":" time-second
                     [time-secfrac]
   full-date       = date-fullyear "-" date-month "-" date-mday
   full-time       = partial-time time-offset

   date-time       = full-date "T" full-time

      NOTE: Per [ABNF] and ISO8601, the "T" and "Z" characters in this
      syntax may alternatively be lower case "t" or "z" respectively.

      This date/time format may be used in some environments or contexts
      that distinguish between the upper- and lower-case letters 'A'-'Z'
      and 'a'-'z' (e.g. XML).  Specifications that use this format in
      such environments MAY further limit the date/time syntax so that
      the letters 'T' and 'Z' used in the date/time syntax must always
      be upper case.  Applications that generate this format SHOULD use
      upper case letters.

      NOTE: ISO 8601 defines date and time separated by "T".
      Applications using this syntax may choose, for the sake of
      readability, to specify a full-date and full-time separated by
      (say) a space character.
"""

import datetime, time, calendar
import re

__all__ = ["tzinfo", "UTC_TZ", "parse_date", "parse_datetime", "now", "utcfromtimestamp", "utctotimestamp", "datetimetostr", "timestamptostr", "strtotimestamp"]

ZERO = datetime.timedelta(0)

class tzinfo(datetime.tzinfo):
    """
    Implementation of a fixed-offset tzinfo.
    """
    def __init__(self, minutesEast = 0, name = 'Z'):
        """
        minutesEast -> number of minutes east of UTC that this tzinfo represents.
        name -> symbolic (but uninterpreted) name of this tzinfo.
        """
        self.minutesEast = minutesEast
        self.offset = datetime.timedelta(minutes = minutesEast)
        self.name = name

    def utcoffset(self, dt):
        """Returns minutesEast from the constructor, as a datetime.timedelta."""
        return self.offset

    def dst(self, dt):
        """This is a fixed offset tzinfo, so always returns a zero timedelta."""
        return ZERO

    def tzname(self, dt):
        """Returns the name from the constructor."""
        return self.name

    def __repr__(self):
        """If minutesEast==0, prints specially as rfc3339.UTC_TZ."""
        if self.minutesEast == 0:
            return "rfc3339.UTC_TZ"
        else:
            return "rfc3339.tzinfo(%s,%s)" % (self.minutesEast, repr(self.name))

UTC_TZ = tzinfo(0, 'Z')

date_re_str = r'(\d\d\d\d)-(\d\d)-(\d\d)'
time_re_str = r'(\d\d):(\d\d):(\d\d)(\.(\d+))?([zZ]|(([-+])(\d\d):?(\d\d)))'

def make_re(*parts):
    return re.compile(r'^\s*' + ''.join(parts) + r'\s*$')

date_re = make_re(date_re_str)
datetime_re = make_re(date_re_str, r'[ tT]', time_re_str)

def parse_date(s):
    """
    Given a string matching the 'full-date' production above, returns
    a datetime.date instance. Any deviation from the allowed format
    will produce a raised ValueError.

    >>> parse_date("2008-08-24")
    datetime.date(2008, 8, 24)
    >>> parse_date("   2008-08-24       ")
    datetime.date(2008, 8, 24)
    >>> parse_date("2008-08-00")
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "rfc3339.py", line 134, in parse_date
        return datetime.date(int(y), int(m), int(d))
    ValueError: day is out of range for month
    >>> parse_date("2008-06-31")
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "rfc3339.py", line 134, in parse_date
        return datetime.date(int(y), int(m), int(d))
    ValueError: day is out of range for month
    >>> parse_date("2008-13-01")
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "rfc3339.py", line 134, in parse_date
        return datetime.date(int(y), int(m), int(d))
    ValueError: month must be in 1..12
    >>> parse_date("22008-01-01")
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "rfc3339.py", line 136, in parse_date
        raise ValueError('Invalid RFC 3339 date string', s)
    ValueError: ('Invalid RFC 3339 date string', '22008-01-01')
    >>> parse_date("2008-08-24").isoformat()
    '2008-08-24'
    """
    m = date_re.match(s)
    if m:
        (y, m, d) = m.groups()
        return datetime.date(int(y), int(m), int(d))
    else:
        raise ValueError('Invalid RFC 3339 date string', s)

def _offset_to_tzname(offset):
    """
    Converts an offset in minutes to an RFC 3339 "time-offset" string.

    >>> _offset_to_tzname(0)
    '+00:00'
    >>> _offset_to_tzname(-1)
    '-00:01'
    >>> _offset_to_tzname(-60)
    '-01:00'
    >>> _offset_to_tzname(-779)
    '-12:59'
    >>> _offset_to_tzname(1)
    '+00:01'
    >>> _offset_to_tzname(60)
    '+01:00'
    >>> _offset_to_tzname(779)
    '+12:59'
    """
    offset = int(offset)
    if offset < 0:
        tzsign = '-'
    else:
        tzsign = '+'
    offset = abs(offset)
    tzhour = offset / 60
    tzmin = offset % 60
    return '%s%02d:%02d' % (tzsign, tzhour, tzmin)

def parse_datetime(s):
    """
    Given a string matching the 'date-time' production above, returns
    a datetime.datetime instance. Any deviation from the allowed
    format will produce a raised ValueError.

    >>> parse_datetime("2008-08-24T00:00:00Z")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.UTC_TZ)
    >>> parse_datetime("   2008-08-24T00:00:00Z ")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.UTC_TZ)
    >>> parse_datetime("2008-08-24T00:00:00")
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "rfc3339.py", line 208, in parse_datetime
        raise ValueError('Invalid RFC 3339 datetime string', s)
    ValueError: ('Invalid RFC 3339 datetime string', '2008-08-24T00:00:00')
    >>> parse_datetime("2008-08-24T00:00:00+00:00")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.UTC_TZ)
    >>> parse_datetime("2008-08-24T00:00:00+01:00")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.tzinfo(60,'+01:00'))
    >>> parse_datetime("2008-08-24T00:00:00-01:00")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.tzinfo(-60,'-01:00'))
    >>> parse_datetime("2008-08-24T00:00:00-01:23")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.tzinfo(-83,'-01:23'))
    >>> parse_datetime("2008-08-24T24:00:00Z")
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "rfc3339.py", line 206, in parse_datetime
        tz)
    ValueError: hour must be in 0..23
    >>> midnightUTC = parse_datetime("2008-08-24T00:00:00Z")
    >>> oneamBST = parse_datetime("2008-08-24T01:00:00+01:00")
    >>> midnightUTC == oneamBST
    True
    >>> elevenpmUTC = parse_datetime("2008-08-23T23:00:00Z")
    >>> midnightBST = parse_datetime("2008-08-24T00:00:00+01:00")
    >>> midnightBST == elevenpmUTC
    True
    >>> elevenpmUTC.isoformat()
    '2008-08-23T23:00:00+00:00'
    >>> oneamBST.isoformat()
    '2008-08-24T01:00:00+01:00'
    >>> parse_datetime("2008-08-24T00:00:00.123Z").isoformat()
    '2008-08-24T00:00:00.123000+00:00'

    Facebook generates incorrectly-formatted RFC 3339 timestamps, with
    the time-offset missing the colon:
    >>> parse_datetime("2008-08-24T00:00:00+0000")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.UTC_TZ)
    >>> parse_datetime("2008-08-24T00:00:00+0100")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.tzinfo(60,'+01:00'))
    >>> parse_datetime("2008-08-24T00:00:00-0100")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.tzinfo(-60,'-01:00'))
    >>> parse_datetime("2008-08-24T00:00:00-0123")
    datetime.datetime(2008, 8, 24, 0, 0, tzinfo=rfc3339.tzinfo(-83,'-01:23'))

    While we accept such broken time-offsets, we don't generate them:
    >>> parse_datetime("2008-08-24T00:00:00+0100").isoformat()
    '2008-08-24T00:00:00+01:00'

    Seconds don't have to be integers:
    >>> parse_datetime("2008-08-24T00:00:11.25Z")
    datetime.datetime(2008, 8, 24, 0, 0, 11, 250000, tzinfo=rfc3339.UTC_TZ)
    >>> parse_datetime("2008-08-24T00:00:11.25-0123")
    datetime.datetime(2008, 8, 24, 0, 0, 11, 250000, tzinfo=rfc3339.tzinfo(-83,'-01:23'))
    >>> parse_datetime("2008-08-24T00:00:11.25+0123")
    datetime.datetime(2008, 8, 24, 0, 0, 11, 250000, tzinfo=rfc3339.tzinfo(83,'+01:23'))

    Rendering non-integer seconds produces an acceptable, if
    non-minimal result:
    >>> parse_datetime("2008-08-24T00:00:11.25Z").isoformat()
    '2008-08-24T00:00:11.250000+00:00'
    """
    m = datetime_re.match(s)
    if m:
        (y, m, d, hour, min, sec, ignore1, frac_sec, wholetz, ignore2, tzsign, tzhour, tzmin) = \
            m.groups()

        if frac_sec:
            frac_sec = float("0." + frac_sec)
        else:
            frac_sec = 0
        microsec = int((frac_sec * 1000000) + 0.5)

        if wholetz == 'z' or wholetz == 'Z':
            tz = UTC_TZ
        else:
            tzhour = int(tzhour)
            tzmin = int(tzmin)
            offset = tzhour * 60 + tzmin
            if offset == 0:
                tz = UTC_TZ
            else:
                if tzhour > 24 or tzmin > 60 or offset > 1439: ## see tzinfo docs for the 1439 part
                    raise ValueError('Invalid timezone offset', s, wholetz)

                if tzsign == '-':
                    offset = -offset
                tz = tzinfo(offset, _offset_to_tzname(offset))

        return datetime.datetime(int(y), int(m), int(d),
                                 int(hour), int(min), int(sec), microsec,
                                 tz)
    else:
        raise ValueError('Invalid RFC 3339 datetime string', s)

def now():
    """Return a timezone-aware datetime.datetime object in
    rfc3339.UTC_TZ timezone, representing the current moment
    (time.time()). Useful as a replacement for the (timezone-unaware)
    datetime.datetime.now() method."""
    return utcfromtimestamp(time.time())

def utcfromtimestamp(unix_epoch_timestamp):
    """Interprets its argument as a count of seconds elapsed since the
    Unix epoch, and returns a datetime.datetime in rfc3339.UTC_TZ
    timezone."""
    (y, m, d, hour, min, sec) = time.gmtime(unix_epoch_timestamp)[:6]
    return datetime.datetime(y, m, d, hour, min, sec, 0, UTC_TZ)

def utctotimestamp(dt):
    """Returns a count of the elapsed seconds between the Unix epoch
    and the passed-in datetime.datetime object."""
    return calendar.timegm(dt.utctimetuple())

def datetimetostr(dt):
    """Return a RFC3339 date-time string corresponding to the given
    datetime object."""
    if dt.utcoffset() is not None:
        return dt.isoformat()
    else:
        return "%sZ" % dt.isoformat()

def timestamptostr(ts):
    """Return a RFC3339 date-time string corresponding to the given
    Unix-epoch timestamp."""
    return datetimetostr(utcfromtimestamp(ts))

def strtotimestamp(s):
    """Return the Unix-epoch timestamp corresponding to the given RFC3339
    date-time string."""
    return utctotimestamp(parse_datetime(s))
