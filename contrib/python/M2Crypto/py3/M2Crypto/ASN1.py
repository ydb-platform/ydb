from __future__ import absolute_import

"""
M2Crypto wrapper for OpenSSL ASN1 API.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.

Portions created by Open Source Applications Foundation (OSAF) are
Copyright (C) 2005 OSAF. All Rights Reserved.
"""

import datetime
import time

from M2Crypto import BIO, m2, types as C
from typing import Optional, Union

MBSTRING_FLAG: int = 0x1000
MBSTRING_ASC: int = MBSTRING_FLAG | 1
MBSTRING_BMP: int = MBSTRING_FLAG | 2


class ASN1_Integer:

    m2_asn1_integer_free = m2.asn1_integer_free

    def __init__(
        self, asn1int: Union[C.ASN1_Integer, int], _pyfree: int = 0
    ):
        if isinstance(asn1int, int):
            self.asn1int: C.ASN1_Integer = m2.asn1_integer_new()
            m2.asn1_integer_set(self.asn1int, asn1int)
        else:
            self.asn1int = asn1int
        self._pyfree = _pyfree

    def __cmp__(self, other: "ASN1_Integer") -> int:
        if not isinstance(other, ASN1_Integer):
            raise TypeError(
                "Comparisons supported only between ANS1_Integer objects"
            )

        return m2.asn1_integer_cmp(self.asn1int, other.asn1int)

    def __del__(self) -> None:
        if self._pyfree:
            self.m2_asn1_integer_free(self.asn1int)

    def __int__(self) -> int:
        return m2.asn1_integer_get(self.asn1int)


class ASN1_String:

    m2_asn1_string_free = m2.asn1_string_free

    def __init__(
        self,
        asn1str: Union[C.ASN1_String, str, bytes],
        _pyfree: int = 0,
    ):
        if isinstance(asn1str, str):
            self.asn1str: C.ASN1_String = m2.asn1_string_new()
            m2.asn1_string_set(self.asn1str, asn1str.encode())
        elif isinstance(asn1str, bytes):
            self.asn1str: C.ASN1_String = m2.asn1_string_new()
            m2.asn1_string_set(self.asn1str, asn1str)
        else:
            self.asn1str = asn1str
        self._pyfree = _pyfree

    def __bytes__(self) -> bytes:
        buf = BIO.MemoryBuffer()
        m2.asn1_string_print(buf.bio_ptr(), self.asn1str)
        return buf.read_all()

    def __str__(self) -> str:
        return self.__bytes__().decode()

    def __del__(self) -> None:
        if getattr(self, '_pyfree', 0):
            self.m2_asn1_string_free(self.asn1str)

    def _ptr(self):
        return self.asn1str

    def as_text(self, flags: int = 0) -> str:
        """Output an ASN1_STRING structure according to the set flags.

        :param flags: determine the format of the output by using
               predetermined constants, see ASN1_STRING_print_ex(3)
               manpage for their meaning.
        :return: output an ASN1_STRING structure.
        """
        buf = BIO.MemoryBuffer()
        m2.asn1_string_print_ex(buf.bio_ptr(), self.asn1str, flags)
        return buf.read_all().decode()


class ASN1_Object:

    m2_asn1_object_free = m2.asn1_object_free

    def __init__(
        self, asn1obj: C.ASN1_Object, _pyfree: int = 0
    ) -> None:
        self.asn1obj = asn1obj
        self._pyfree = _pyfree

    def __del__(self) -> None:
        if self._pyfree:
            self.m2_asn1_object_free(self.asn1obj)

    def _ptr(self):
        return self.asn1obj


class _UTC(datetime.tzinfo):
    def tzname(self, dt: Optional[datetime.datetime]) -> str:
        return "UTC"

    def dst(
        self, dt: Optional[datetime.datetime]
    ) -> datetime.timedelta:
        return datetime.timedelta(0)

    def utcoffset(
        self, dt: Optional[datetime.datetime]
    ) -> datetime.timedelta:
        return datetime.timedelta(0)

    def __repr__(self) -> str:
        return "<Timezone: %s>" % self.tzname(None)


UTC: _UTC = _UTC()


class LocalTimezone(datetime.tzinfo):
    """Localtimezone from datetime manual."""

    def __init__(self) -> None:
        self._stdoffset = datetime.timedelta(seconds=-time.timezone)
        if time.daylight:
            self._dstoffset = datetime.timedelta(
                seconds=-time.altzone
            )
        else:
            self._dstoffset = self._stdoffset
        self._dstdiff = self._dstoffset - self._stdoffset

    def utcoffset(self, dt: datetime.datetime) -> datetime.timedelta:
        if self._isdst(dt):
            return self._dstoffset
        else:
            return self._stdoffset

    def dst(self, dt: datetime.datetime) -> datetime.timedelta:
        if self._isdst(dt):
            return self._dstdiff
        else:
            return datetime.timedelta(0)

    def tzname(self, dt: datetime.datetime) -> str:
        return time.tzname[self._isdst(dt).real]

    def _isdst(self, dt: datetime.datetime) -> bool:
        tt = (
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.weekday(),
            0,
            -1,
        )
        stamp = time.mktime(tt)
        tt = time.localtime(stamp)
        return tt.tm_isdst > 0


class ASN1_TIME:
    _ssl_months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    m2_asn1_time_free = m2.asn1_time_free

    def __init__(
        self,
        asn1_time: Optional[C.ASN1_Time] = None,
        _pyfree: int = 0,
        asn1_utctime: Optional[C.ASN1_Time] = None,
    ):
        # handle old keyword parameter
        if asn1_time is None:
            asn1_time = asn1_utctime
        if asn1_time is not None:
            assert m2.asn1_time_type_check(
                asn1_time
            ), "'asn1_time' type error'"
            self.asn1_time = asn1_time
            self._pyfree = _pyfree
        else:
            # that's (ASN1_TIME*)
            self.asn1_time: bytes = m2.asn1_time_new()
            self._pyfree = 1

    def __del__(self) -> None:
        if getattr(self, '_pyfree', 0):
            self.m2_asn1_time_free(self.asn1_time)

    def __str__(self) -> str:
        assert m2.asn1_time_type_check(
            self.asn1_time
        ), "'asn1_time' type error'"
        buf = BIO.MemoryBuffer()
        m2.asn1_time_print(buf.bio_ptr(), self.asn1_time)
        return buf.read_all().decode()

    def _ptr(self):
        assert m2.asn1_time_type_check(
            self.asn1_time
        ), "'asn1_time' type error'"
        return self.asn1_time

    def set_string(self, string: bytes) -> int:
        """
        Set time from UTC string.

        :return:  1 if the time value is successfully set and 0
                  otherwise
        """
        assert m2.asn1_time_type_check(
            self.asn1_time
        ), "'asn1_time' type error'"
        return m2.asn1_time_set_string(self.asn1_time, string)

    def set_time(self, time: int) -> C.ASN1_Time:
        """
        Set time from seconds since epoch (int).

        :return: pointer to a time structure or NULL if an error
                 occurred
        """
        assert m2.asn1_time_type_check(
            self.asn1_time
        ), "'asn1_time' type error'"
        if hasattr(__builtins__, "long"):
            time = long(time)
        return m2.asn1_time_set(self.asn1_time, time)

    def get_datetime(self) -> datetime.datetime:
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
            raise ValueError(
                "Invalid date %s: Invalid month: %s" % (date, month)
            )
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

    def set_datetime(self, date: datetime.datetime) -> C.ASN1_Time:
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
