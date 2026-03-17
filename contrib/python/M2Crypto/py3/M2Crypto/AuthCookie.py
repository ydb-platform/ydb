from __future__ import absolute_import

"""Secure Authenticator Cookies

Copyright (c) 1999-2002 Ng Pheng Siong. All rights reserved."""

import logging
import re
import time

from http.cookies import SimpleCookie

from M2Crypto import Rand, m2, util

from typing import Pattern, Optional, Tuple, Union  # noqa

_MIX_FORMAT = 'exp=%f&data=%s&digest='
_MIX_RE = re.compile(r'exp=(\d+\.\d+)&data=(.+)&digest=(\S*)')

log = logging.getLogger(__name__)


def mix(
    expiry: float, data: Union[str, bytes], format: str = _MIX_FORMAT
) -> Union[str, bytes]:
    return format % (expiry, data)


def unmix(
    dough: Union[str, bytes], regex: Pattern = _MIX_RE
) -> object:
    mo = regex.match(dough)
    if mo:
        return float(mo.group(1)), mo.group(2)
    else:
        return None


def unmix3(
    dough: Union[str, bytes], regex: Pattern = _MIX_RE
) -> Optional[Tuple[float, Union[str, bytes], Union[str, bytes]]]:
    mo = regex.match(dough)
    if mo:
        return float(mo.group(1)), mo.group(2), mo.group(3)
    else:
        return None


_TOKEN: str = '_M2AUTH_'


class AuthCookie:

    def __init__(
        self, expiry: float, data: str, dough: str, mac: str
    ) -> None:
        """
        Create new authentication cookie

        :param expiry: expiration time (in seconds)
        :param data: cookie payload (as a string)
        :param dough: expiry & data concatenated to URL compliant
                      string
        :param mac: SHA1-based HMAC of dough and random key
        """
        self._expiry = expiry
        self._data = data
        self._mac = mac
        self._cookie = SimpleCookie()
        self._cookie[_TOKEN] = '%s%s' % (dough, mac)
        self._name = '%s%s' % (dough, mac)  # WebKit only.

    def expiry(self) -> float:
        """Return the cookie's expiry time."""
        return self._expiry

    def data(self) -> str:
        """Return the data portion of the cookie."""
        return self._data

    def mac(self) -> str:
        """Return the cookie's MAC."""
        return self._mac

    def output(self, header: Optional[str] = "Set-Cookie:") -> str:
        """Return the cookie's output in "Set-Cookie" format."""
        return self._cookie.output(header=header)

    def value(self) -> str:
        """Return the cookie's output minus the "Set-Cookie: " portion."""
        return self._cookie[_TOKEN].value

    def isExpired(self) -> bool:
        """Return 1 if the cookie has expired, 0 otherwise."""
        return isinstance(self._expiry, (float, int)) and (
            time.time() > self._expiry
        )

    # Following two methods are for WebKit only.
    # I may wish to push them to WKAuthCookie, but they are part
    # of the API now. Oh well.
    def name(self) -> str:
        return self._name

    def headerValue(self) -> str:
        return self.value()


class AuthCookieJar:

    _keylen: int = 20

    def __init__(self) -> None:
        self._key = Rand.rand_bytes(self._keylen)

    def _hmac(self, key: bytes, data: str) -> str:
        return util.bin_to_hex(m2.hmac(key, data.encode(), m2.sha1()))

    def makeCookie(self, expiry: float, data: str) -> AuthCookie:
        """
        Make a cookie

        :param expiry: expiration time (float in seconds)
        :param data: cookie content
        :return: AuthCookie object
        """
        if not isinstance(expiry, (int, float)):
            raise ValueError(
                'Expiration time must be number, not "%s' % expiry
            )
        dough = mix(expiry, data)
        return AuthCookie(
            expiry, data, dough, self._hmac(self._key, dough)
        )

    def isGoodCookie(self, cookie: AuthCookie) -> bool:
        if cookie.isExpired():
            return False
        c = self.makeCookie(cookie._expiry, cookie._data)
        return (
            (c._expiry == cookie._expiry)
            and (c._data == cookie._data)
            and (c._mac == cookie._mac)
            and (c.output() == cookie.output())
        )

    def isGoodCookieString(
        self, cookie_str: Union[dict, bytes], _debug: bool = False
    ) -> bool:
        c = SimpleCookie()
        c.load(cookie_str)
        if _TOKEN not in c:
            log.debug('_TOKEN not in c (keys = %s)', dir(c))
            return False
        undough = unmix3(c[_TOKEN].value)
        if undough is None:
            log.debug('undough is None')
            return False
        exp, data, mac = undough
        c2 = self.makeCookie(exp, data)
        if _debug and (c2._mac == mac):
            log.error('cookie_str = %s', cookie_str)
            log.error('c2.isExpired = %s', c2.isExpired())
            log.error('mac = %s', mac)
            log.error('c2._mac = %s', c2._mac)
            log.error('c2._mac == mac: %s', str(c2._mac == mac))
        return (not c2.isExpired()) and (c2._mac == mac)
