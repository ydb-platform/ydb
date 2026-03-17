"""ndg_httpsclient HTTPS module containing PyOpenSSL implementation of
httplib.HTTPSConnection

PyOpenSSL utility to make a httplib-like interface suitable for use with 
urllib2
"""
__author__ = "P J Kershaw (STFC)"
__date__ = "09/12/11"
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__contact__ = "Philip.Kershaw@stfc.ac.uk"
__revision__ = '$Id$'
import logging
import socket
import sys

if sys.version_info[0] > 2:
    from http.client import HTTPS_PORT
    from http.client import HTTPConnection

    from urllib.request import AbstractHTTPHandler
else:
    from httplib import HTTPS_PORT
    from httplib import HTTPConnection

    from urllib2 import AbstractHTTPHandler


from OpenSSL import SSL

from ndg.httpsclient.ssl_socket import SSLSocket

log = logging.getLogger(__name__)


class HTTPSConnection(HTTPConnection):
    """This class allows communication via SSL using PyOpenSSL.
    It is based on httplib.HTTPSConnection, modified to use PyOpenSSL.

    Note: This uses the constructor inherited from HTTPConnection to allow it to
    be used with httplib and HTTPSContextHandler. To use the class directly with
    an SSL context set ssl_context after construction.
    
    @cvar default_port: default port for this class (443)
    @type default_port: int
    @cvar default_ssl_method: default SSL method used if no SSL context is
    explicitly set - defaults to version 2/3.
    @type default_ssl_method: int
    """
    default_port = HTTPS_PORT
    default_ssl_method = SSL.TLSv1_2_METHOD
    
    def __init__(self, host, port=None, strict=None,
                 timeout=socket._GLOBAL_DEFAULT_TIMEOUT, ssl_context=None):
        HTTPConnection.__init__(self, host, port, strict, timeout)
        if not hasattr(self, 'ssl_context'):
            self.ssl_context = None

        if ssl_context is not None:
            if not isinstance(ssl_context, SSL.Context):
                raise TypeError('Expecting OpenSSL.SSL.Context type for "'
                                'ssl_context" keyword; got %r instead' %
                                ssl_context)
                
            self.ssl_context = ssl_context
            
    def connect(self):
        """Create SSL socket and connect to peer
        """
        if getattr(self, 'ssl_context', None):
            if not isinstance(self.ssl_context, SSL.Context):
                raise TypeError('Expecting OpenSSL.SSL.Context type for "'
                                'ssl_context" attribute; got %r instead' %
                                self.ssl_context)
            ssl_context = self.ssl_context
        else:
            ssl_context = SSL.Context(self.__class__.default_ssl_method)

        sock = socket.create_connection((self.host, self.port), self.timeout)
        
        # Tunnel if using a proxy - ONLY available for Python 2.6.2 and above
        if getattr(self, '_tunnel_host', None):
            self.sock = sock
            self._tunnel()
            
        self.sock = SSLSocket(ssl_context, sock)
        
        # Go to client mode.
        self.sock.set_connect_state()

    def close(self):
        """Close socket and shut down SSL connection"""
        if hasattr(self.sock, "close"):
            self.sock.close()
        
        
class HTTPSContextHandler(AbstractHTTPHandler):
    '''HTTPS handler that allows a SSL context to be set for the SSL
    connections.
    '''
    https_request = AbstractHTTPHandler.do_request_

    SSL_METHOD = SSL.TLSv1_2_METHOD
    
    def __init__(self, ssl_context, debuglevel=0):
        """
        @param ssl_context:SSL context
        @type ssl_context: OpenSSL.SSL.Context
        @param debuglevel: debug level for HTTPSHandler
        @type debuglevel: int
        """
        AbstractHTTPHandler.__init__(self, debuglevel)

        if ssl_context is not None:
            if not isinstance(ssl_context, SSL.Context):
                raise TypeError('Expecting OpenSSL.SSL.Context type for "'
                                'ssl_context" keyword; got %r instead' %
                                ssl_context)
            self.ssl_context = ssl_context
        else:
            self.ssl_context = SSL.Context(self.__class__.SSL_METHOD)

    def https_open(self, req):
        """Opens HTTPS request
        @param req: HTTP request
        @return: HTTP Response object
        """
        # Make a custom class extending HTTPSConnection, with the SSL context
        # set as a class variable so that it is available to the connect method.
        customHTTPSContextConnection = type('CustomHTTPSContextConnection',
                                            (HTTPSConnection, object),
                                            {'ssl_context': self.ssl_context})
        return self.do_open(customHTTPSContextConnection, req)
