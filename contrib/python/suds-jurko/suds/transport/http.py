# This program is free software; you can redistribute it and/or modify it under
# the terms of the (LGPL) GNU Lesser General Public License as published by the
# Free Software Foundation; either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Library Lesser General Public License
# for more details at ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jeff Ortel ( jortel@redhat.com )

"""
Contains classes for basic HTTP transport implementations.

"""

from suds.properties import Unskin
from suds.transport import *

import base64
from http.cookiejar import CookieJar
import http.client
import socket
import sys
import urllib.request, urllib.error, urllib.parse
from urllib.parse import urlparse

from logging import getLogger
log = getLogger(__name__)


class HttpTransport(Transport):
    """
    Basic HTTP transport implemented using using urllib2, that provides for
    cookies & proxies but no authentication.

    """

    def __init__(self, **kwargs):
        """
        @param kwargs: Keyword arguments.
            - B{proxy} - An http proxy to be specified on requests.
                 The proxy is defined as {protocol:proxy,}
                    - type: I{dict}
                    - default: {}
            - B{timeout} - Set the url open timeout (seconds).
                    - type: I{float}
                    - default: 90

        """
        Transport.__init__(self)
        Unskin(self.options).update(kwargs)
        self.cookiejar = CookieJar()
        self.proxy = {}
        self.urlopener = None

    def open(self, request):
        try:
            url = self.__get_request_url(request)
            log.debug('opening (%s)', url)
            u2request = urllib.request.Request(url)
            self.proxy = self.options.proxy
            return self.u2open(u2request)
        except urllib.error.HTTPError as e:
            raise TransportError(str(e), e.code, e.fp)

    def send(self, request):
        result = None
        url = self.__get_request_url(request)
        msg = request.message
        headers = request.headers
        try:
            u2request = urllib.request.Request(url, msg, headers)
            self.addcookies(u2request)
            self.proxy = self.options.proxy
            request.headers.update(u2request.headers)
            log.debug('sending:\n%s', request)
            fp = self.u2open(u2request)
            self.getcookies(fp, u2request)
            if sys.version_info < (3, 0):
                headers = fp.headers.dict
            else:
                headers = fp.headers
            result = Reply(http.client.OK, headers, fp.read())
            log.debug('received:\n%s', result)
        except urllib.error.HTTPError as e:
            if e.code in (http.client.ACCEPTED, http.client.NO_CONTENT):
                result = None
            else:
                raise TransportError(e.msg, e.code, e.fp)
        return result

    def addcookies(self, u2request):
        """
        Add cookies in the cookiejar to the request.

        @param u2request: A urllib2 request.
        @rtype: u2request: urllib2.Request.

        """
        self.cookiejar.add_cookie_header(u2request)

    def getcookies(self, fp, u2request):
        """
        Add cookies in the request to the cookiejar.

        @param u2request: A urllib2 request.
        @rtype: u2request: urllib2.Request.

        """
        self.cookiejar.extract_cookies(fp, u2request)

    def u2open(self, u2request):
        """
        Open a connection.

        @param u2request: A urllib2 request.
        @type u2request: urllib2.Request.
        @return: The opened file-like urllib2 object.
        @rtype: fp

        """
        tm = self.options.timeout
        url = self.u2opener()
        if (sys.version_info < (3, 0)) and (self.u2ver() < 2.6):
            socket.setdefaulttimeout(tm)
            return url.open(u2request)
        return url.open(u2request, timeout=tm)

    def u2opener(self):
        """
        Create a urllib opener.

        @return: An opener.
        @rtype: I{OpenerDirector}

        """
        if self.urlopener is None:
            return urllib.request.build_opener(*self.u2handlers())
        return self.urlopener

    def u2handlers(self):
        """
        Get a collection of urllib handlers.

        @return: A list of handlers to be installed in the opener.
        @rtype: [Handler,...]

        """
        handlers = []
        handlers.append(urllib.request.ProxyHandler(self.proxy))
        return handlers

    def u2ver(self):
        """
        Get the major/minor version of the urllib2 lib.

        @return: The urllib2 version.
        @rtype: float
        """
        try:
            part = urllib.request.__version__.split('.', 1)
            return float('.'.join(part))
        except Exception as e:
            log.exception(e)
            return 0

    def __deepcopy__(self, memo={}):
        clone = self.__class__()
        p = Unskin(self.options)
        cp = Unskin(clone.options)
        cp.update(p)
        return clone

    @staticmethod
    def __get_request_url(request):
        """
        Returns the given request's URL, properly encoded for use with urllib.

        URLs are allowed to be:
            under Python 2.x: unicode strings, single-byte strings;
            under Python 3.x: unicode strings.
        In any case, they are allowed to contain ASCII characters only. We
        raise a UnicodeError derived exception if they contain any non-ASCII
        characters (UnicodeEncodeError or UnicodeDecodeError depending on
        whether the URL was specified as a unicode or a single-byte string).

        Python 3.x httplib.client implementation must be given a unicode string
        and not a bytes object and the given string is internally converted to
        a bytes object using an explicitly specified ASCII encoding.

        Python 2.7 httplib implementation expects the URL passed to it to not
        be a unicode string. If it is, then passing it to the underlying
        httplib Request object will cause that object to forcefully convert all
        of its data to unicode, assuming that data contains ASCII data only and
        raising a UnicodeDecodeError exception if it does not (caused by simple
        unicode + string concatenation).

        Python 2.4 httplib implementation does not really care about this as it
        does not use the internal optimization present in the Python 2.7
        implementation causing all the requested data to be converted to
        unicode.

        """
        url = request.url
        py2 = sys.version_info < (3, 0)
        if py2 and isinstance(url, str):
            encodedURL = url
            decodedURL = url.decode("ascii")
        else:
            # On Python3, calling encode() on a bytes or a bytearray object
            # raises an AttributeError exception.
            assert py2 or not isinstance(url, bytes)
            assert py2 or not isinstance(url, bytearray)
            decodedURL = url
            encodedURL = url.encode("ascii")
        if py2:
            return encodedURL  # Python 2 urllib - single-byte URL string.
        return decodedURL  # Python 3 urllib - unicode URL string.


class HttpAuthenticated(HttpTransport):
    """
    Provides basic HTTP authentication for servers that do not follow the
    specified challenge/response model. Appends the I{Authorization} HTTP
    header with base64 encoded credentials on every HTTP request.
    """

    def open(self, request):
        self.addcredentials(request)
        return HttpTransport.open(self, request)

    def send(self, request):
        self.addcredentials(request)
        return HttpTransport.send(self, request)

    def addcredentials(self, request):
        credentials = self.credentials()
        if not (None in credentials):
            credentials = ':'.join(credentials)
            if sys.version_info < (3,0):
                basic = 'Basic %s' % base64.b64encode(credentials)
            else:
                encodedBytes = base64.urlsafe_b64encode(credentials.encode())
                encodedString = encodedBytes.decode()
                basic = 'Basic %s' % encodedString
            request.headers['Authorization'] = basic

    def credentials(self):
        return self.options.username, self.options.password
