from __future__ import absolute_import

"""
M2Crypto wrapper for OpenSSL ASN1 API.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.

Portions created by Open Source Applications Foundation (OSAF) are
Copyright (C) 2005 OSAF. All Rights Reserved.
"""

import datetime
import time

from M2Crypto import BIO, m2, six
from typing import Optional  # noqa

MBSTRING_FLAG = 0x1000
MBSTRING_ASC = MBSTRING_FLAG | 1
MBSTRING_BMP = MBSTRING_FLAG | 2


class ASN1_Integer(object):

    m2_asn1_integer_free = m2.asn1_integer_free

    def __init__(self, asn1int, _pyfree=0):
        # type: (ASN1_Integer, int) -> None
        self.asn1int = asn1int
        self._pyfree = _pyfree

    def __cmp__(self, other):
        # type: (ASN1_Integer) -> int
        if not isinstance(other, ASN1_Integer):
            raise TypeError(
                "Comparisons supported only between ANS1_Integer objects")

        return m2.asn1_integer_cmp(self.asn1int, other.asn1int)

    def __del__(self):
        # type: () -> None
        if self._pyfree:
            self.m2_asn1_integer_free(self.asn1int)

    def __int__(self):
        # type: () -> int
        return m2.asn1_integer_get(self.asn1int)


class ASN1_String(object):

    m2_asn1_string_free = m2.asn1_string_free

    def __init__(self, asn1str, _pyfree=0):
        # type: (ASN1_String, int) -> None
        self.asn1str = asn1str
        self._pyfree = _pyfree

    def __bytes__(self):
        # type: () -> bytes
        buf = BIO.MemoryBuffer()
        m2.asn1_string_print(buf.bio_ptr(), self.asn1str)
        return buf.read_all()

    def __str__(self):
        # type: () -> str
        return six.ensure_text(self.__bytes__())

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_asn1_string_free(self.asn1str)

    def _ptr(self):
        return self.asn1str

    def as_text(self, flags=0):
        # type: (int) -> str
        """Output an ASN1_STRING structure according to the set flags.

        :param flags: determine the format of the output by using
               predetermined constants, see ASN1_STRING_print_ex(3)
               manpage for their meaning.
        :return: output an ASN1_STRING structure.
        """
        buf = BIO.MemoryBuffer()
        m2.asn1_string_print_ex(buf.bio_ptr(), self.asn1str, flags)
        return six.ensure_text(buf.read_all())


class ASN1_Object(object):

    m2_asn1_object_free = m2.asn1_object_free

    def __init__(self, asn1obj, _pyfree=0):
        # type: (ASN1_Object, int) -> None
        self.asn1obj = asn1obj
        self._pyfree = _pyfree

    def __del__(self):
        # type: () -> None
        if self._pyfree:
            self.m2_asn1_object_free(self.asn1obj)

    def _ptr(self):
        return self.asn1obj


class _UTC(datetime.tzinfo):
    def tzname(self, dt):
        # type: (Optional[datetime.datetime]) -> str
        return "UTC"

    def dst(self, dt):
        # type: (Optional[datetime.datetime]) -> datetime.timedelta
        return datetime.timedelta(0)

    def utcoffset(self, dt):
        # type: (Optional[datetime.datetime]) -> datetime.timedelta
        return datetime.timedelta(0)

    def __repr__(self):
        return "<Timezone: %s>" % self.tzname(None)


UTC = _UTC()  # type: _UTC


class LocalTimezone(datetime.tzinfo):
    """Localtimezone from datetime manual."""

    def __init__(self):
        # type: () -> None
        self._stdoffset = datetime.timedelta(seconds=-time.timezone)
        if time.daylight:
            self._dstoffset = datetime.timedelta(seconds=-time.altzone)
        else:
            self._dstoffset = self._stdoffset
        self._dstdiff = self._dstoffset - self._stdoffset

    def utcoffset(self, dt):
        # type: (datetime.datetime) -> datetime.timedelta
        if self._isdst(dt):
            return self._dstoffset
        else:
            return self._stdoffset

    def dst(self, dt):
        # type: (datetime.datetime) -> datetime.timedelta
        if self._isdst(dt):
            return self._dstdiff
        else:
            return datetime.timedelta(0)

    def tzname(self, dt):
        # type: (datetime.datetime) -> str
        return time.tzname[self._isdst(dt).real]

    def _isdst(self, dt):
        # type: (datetime.datetime) -> bool
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, -1)
        stamp = time.mktime(tt)
        tt = time.localtime(stamp)
        return tt.tm_isdst > 0


class ASN1_TIME(object):
    _ssl_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
                   "Sep", "Oct", "Nov", "Dec"]
    m2_asn1_time_free = m2.asn1_time_free

    def __init__(self, asn1_time=None, _pyfree=0, asn1_utctime=None):
        # type: (Optional[ASN1_TIME], Optional[int], Optional[ASN1_TIME]) -> None
        # handle old keyword parameter
        if asn1_time is None:
            asn1_time = asn1_utctime
        if asn1_time is not None:
            assert m2.asn1_time_type_check(asn1_time), \
                "'asn1_time' type error'"
            self.asn1_time = asn1_time
            self._pyfree = _pyfree
        else:
            # that's (ASN1_TIME*)
            self.asn1_time = m2.asn1_time_new()  # type: bytes
            self._pyfree = 1

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_asn1_time_free(self.asn1_time)

    def __str__(self):
        # type: () -> str
        assert m2.asn1_time_type_check(self.asn1_time), \
            "'asn1_time' type error'"
        buf = BIO.MemoryBuffer()
        m2.asn1_time_print(buf.bio_ptr(), self.asn1_time)
        return six.ensure_text(buf.read_all())

    def _ptr(self):
        assert m2.asn1_time_type_check(self.asn1_time), \
            "'asn1_time' type error'"
        return self.asn1_time

    def set_string(self, string):
        # type: (bytes) -> int
        """
        Set time from UTC string.

        :return:  1 if the time value is successfully set and 0
                  otherwise
        """
        assert m2.asn1_time_type_check(self.asn1_time), \
            "'asn1_time' type error'"
        return m2.asn1_time_set_string(self.asn1_time, string)

    def set_time(self, time):
        # type: (int) -> ASN1_TIME
        """
        Set time from seconds since epoch (int).

        :return: pointer to a time structure or NULL if an error
                 occurred
        """
        assert m2.asn1_time_type_check(self.asn1_time), \
            "'asn1_time' type error'"
        if hasattr(__builtins__, "long"):
            time = long(time)
        return m2.asn1_time_set(self.asn1_time, time)

    def get_datetime(self):
        # type: () -> datetime.datetime
        """
        Get time as datetime.datetime object

        :return: always return datetime object
        :raises: ValueError if anything wrong happens
        """
        date = str(self)

        timezone = None
        if ' ' not in date:
            raise ValueError("Invalid date: %s" % date)
        month, rest = date.split(' ', 1)
        if month not in self._ssl_months:
            raise ValueError("Invalid date %s: Invalid month: %s" %
                             (date, month))
        if rest.endswith(' GMT'):
            timezone = UTC
            rest = rest[:-4]
        if '.' in rest:
            dt = datetime.datetime.strptime(rest, "%d %H:%M:%S.%f %Y")
        else:
            dt = datetime.datetime.strptime(rest, "%d %H:%M:%S %Y")
        dt = dt.replace(month=self._ssl_months.index(month) + 1)
        if timezone:
            dt = dt.replace(tzinfo=UTC)
        return dt

    def set_datetime(self, date):
        # type: (datetime.datetime) -> ASN1_TIME
        """
        Set time from datetime.datetime object.

        :return: pointer to a time structure or NULL if an error
                 occurred
        """
        local = LocalTimezone()
        if date.tzinfo is None:
            date = date.replace(tzinfo=local)
        date = date.astimezone(local)
        return self.set_time(int(time.mktime(date.timetuple())))


ASN1_UTCTIME = ASN1_TIME
