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
Contains transport interface (classes).

"""

from suds import UnicodeMixin


class TransportError(Exception):
    def __init__(self, reason, httpcode, fp=None):
        Exception.__init__(self, reason)
        self.httpcode = httpcode
        self.fp = fp


class Request(UnicodeMixin):
    """
    A transport request.

    @ivar url: The URL for the request.
    @type url: str
    @ivar message: The message to be sent in a POST request.
    @type message: str
    @ivar headers: The HTTP headers to be used for the request.
    @type headers: dict

    """

    def __init__(self, url, message=None):
        """
        @param url: The URL for the request.
        @type url: str
        @param message: The (optional) message to be sent in the request.
        @type message: str

        """
        self.url = url
        self.headers = {}
        self.message = message

    def __unicode__(self):
        return """\
URL: %s
HEADERS: %s
MESSAGE:
%s""" % (self.url, self.headers, self.message)


class Reply(UnicodeMixin):
    """
    A transport reply.

    @ivar code: The HTTP code returned.
    @type code: int
    @ivar message: The message to be sent in a POST request.
    @type message: str
    @ivar headers: The HTTP headers to be used for the request.
    @type headers: dict

    """

    def __init__(self, code, headers, message):
        """
        @param code: The HTTP code returned.
        @type code: int
        @param headers: The HTTP returned headers.
        @type headers: dict
        @param message: The (optional) reply message received.
        @type message: str

        """
        self.code = code
        self.headers = headers
        self.message = message

    def __unicode__(self):
        return """\
CODE: %s
HEADERS: %s
MESSAGE:
%s""" % (self.code, self.headers, self.message)


class Transport:
    """The transport I{interface}."""

    def __init__(self):
        from suds.transport.options import Options
        self.options = Options()

    def open(self, request):
        """
        Open the URL in the specified request.

        @param request: A transport request.
        @type request: L{Request}
        @return: An input stream.
        @rtype: stream
        @raise TransportError: On all transport errors.

        """
        raise Exception('not-implemented')

    def send(self, request):
        """
        Send soap message. Implementations are expected to handle:
            - proxies
            - I{HTTP} headers
            - cookies
            - sending message
            - brokering exceptions into L{TransportError}

        @param request: A transport request.
        @type request: L{Request}
        @return: The reply
        @rtype: L{Reply}
        @raise TransportError: On all transport errors.

        """
        raise Exception('not-implemented')
