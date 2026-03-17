from __future__ import absolute_import

"""Secure Authenticator Cookies

Copyright (c) 1999-2002 Ng Pheng Siong. All rights reserved."""

import logging
import re
import time

from M2Crypto import Rand, m2, six, util
from M2Crypto.six.moves.http_cookies import SimpleCookie

from typing import re as type_re, AnyStr, Optional, Union  # noqa

_MIX_FORMAT = 'exp=%f&data=%s&digest='
_MIX_RE = re.compile(r'exp=(\d+\.\d+)&data=(.+)&digest=(\S*)')

log = logging.getLogger(__name__)


def mix(expiry, data, format=_MIX_FORMAT):
    # type: (float, AnyStr, str) -> AnyStr
    return format % (expiry, data)


def unmix(dough, regex=_MIX_RE):
    # type: (AnyStr, type_re) -> object
    mo = regex.match(dough)
    if mo:
        return float(mo.group(1)), mo.group(2)
    else:
        return None


def unmix3(dough, regex=_MIX_RE):
    # type: (AnyStr, type_re) -> Optional[tuple[float, AnyStr, AnyStr]]
    mo = regex.match(dough)
    if mo:
        return float(mo.group(1)), mo.group(2), mo.group(3)
    else:
        return None


_TOKEN = '_M2AUTH_'  # type: str


class AuthCookieJar(object):

    _keylen = 20  # type: int

    def __init__(self):
        # type: () -> None
        self._key = Rand.rand_bytes(self._keylen)

    def _hmac(self, key, data):
        # type: (bytes, str) -> str
        return util.bin_to_hex(m2.hmac(key, six.ensure_binary(data), m2.sha1()))

    def makeCookie(self, expiry, data):
        # type: (float, str) -> AuthCookie
        """
        Make a cookie

        :param expiry: expiration time (float in seconds)
        :param data: cookie content
        :return: AuthCookie object
        """
        if not isinstance(expiry, (six.integer_types, float)):
            raise ValueError('Expiration time must be number, not "%s' % expiry)
        dough = mix(expiry, data)
        return AuthCookie(expiry, data, dough, self._hmac(self._key, dough))

    def isGoodCookie(self, cookie):
        # type: (AuthCookie) -> Union[bool, int]
        assert isinstance(cookie, AuthCookie)
        if cookie.isExpired():
            return 0
        c = self.makeCookie(cookie._expiry, cookie._data)
        return (c._expiry == cookie._expiry) \
            and (c._data == cookie._data) \
            and (c._mac == cookie._mac) \
            and (c.output() == cookie.output())

    def isGoodCookieString(self, cookie_str, _debug=False):
        # type: (Union[dict, bytes], bool) -> Union[bool, int]
        c = SimpleCookie()
        c.load(cookie_str)
        if _TOKEN not in c:
            log.debug('_TOKEN not in c (keys = %s)', dir(c))
            return 0
        undough = unmix3(c[_TOKEN].value)
        if undough is None:
            log.debug('undough is None')
            return 0
        exp, data, mac = undough
        c2 = self.makeCookie(exp, data)
        if _debug and (c2._mac == mac):
            log.error('cookie_str = %s', cookie_str)
            log.error('c2.isExpired = %s', c2.isExpired())
            log.error('mac = %s', mac)
            log.error('c2._mac = %s', c2._mac)
            log.error('c2._mac == mac: %s', str(c2._mac == mac))
        return (not c2.isExpired()) and (c2._mac == mac)


class AuthCookie(object):

    def __init__(self, expiry, data, dough, mac):
        # type: (float, str, str, str) -> None
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

    def expiry(self):
        # type: () -> float
        """Return the cookie's expiry time."""
        return self._expiry

    def data(self):
        # type: () -> str
        """Return the data portion of the cookie."""
        return self._data

    def mac(self):
        # type: () -> str
        """Return the cookie's MAC."""
        return self._mac

    def output(self, header="Set-Cookie:"):
        # type: (Optional[str]) -> str
        """Return the cookie's output in "Set-Cookie" format."""
        return self._cookie.output(header=header)

    def value(self):
        # type: () -> str
        """Return the cookie's output minus the "Set-Cookie: " portion.
        """
        return self._cookie[_TOKEN].value

    def isExpired(self):
        # type: () -> bool
        """Return 1 if the cookie has expired, 0 otherwise."""
        return isinstance(self._expiry, (float, six.integer_types)) and \
            (time.time() > self._expiry)

    # Following two methods are for WebKit only.
    # I may wish to push them to WKAuthCookie, but they are part
    # of the API now. Oh well.
    def name(self):
        # type: () -> str
        return self._name

    def headerValue(self):
        # type: () -> str
        return self.value()
