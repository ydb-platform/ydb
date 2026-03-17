"""PyOpenSSL utilities including HTTPSSocket class which wraps PyOpenSSL
SSL connection into a httplib-like interface suitable for use with urllib2

"""
__author__ = "P J Kershaw"
__date__ = "21/12/10"
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__contact__ = "Philip.Kershaw@stfc.ac.uk"
__revision__ = '$Id$'

from datetime import datetime
import logging
import socket
from io import BytesIO

from OpenSSL import SSL

log = logging.getLogger(__name__)


class SSLSocket(object):
    """SSL Socket class wraps pyOpenSSL's SSL.Connection class implementing
    the makefile method so that it is compatible with the standard socket
    interface and usable with httplib.

    @cvar default_buf_size: default buffer size for recv operations in the
    makefile method
    @type default_buf_size: int
    """
    default_buf_size = 8192

    def __init__(self, ctx, sock=None):
        """Create SSL socket object

        @param ctx: SSL context
        @type ctx: OpenSSL.SSL.Context
        @param sock: underlying socket object
        @type sock: socket.socket
        """
        if sock is not None:
            self.socket = sock
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.__ssl_conn = SSL.Connection(ctx, self.socket)
        self.buf_size = self.__class__.default_buf_size
        self._makefile_refs = 0

    def __del__(self):
        """Close underlying socket when this object goes out of scope
        """
        self.close()

    @property
    def buf_size(self):
        """Buffer size for makefile method recv() operations"""
        return self.__buf_size

    @buf_size.setter
    def buf_size(self, value):
        """Buffer size for makefile method recv() operations"""
        if not isinstance(value, int):
            raise TypeError('Expecting int type for "buf_size"; '
                            'got %r instead' % type(value))
        self.__buf_size = value

    def close(self):
        """Shutdown the SSL connection and call the close method of the
        underlying socket"""
        if self._makefile_refs < 1:        
            try:
                self.__ssl_conn.shutdown()
            except (SSL.Error, SSL.SysCallError):
                # Make errors on shutdown non-fatal
                pass
        else:
            self._makefile_refs -= 1

    def set_shutdown(self, mode):
        """Set the shutdown state of the Connection.
        @param mode: bit vector of either or both of SENT_SHUTDOWN and
        RECEIVED_SHUTDOWN
        """
        self.__ssl_conn.set_shutdown(mode)

    def get_shutdown(self):
        """Get the shutdown state of the Connection.
        @return: bit vector of either or both of SENT_SHUTDOWN and
        RECEIVED_SHUTDOWN
        """
        return self.__ssl_conn.get_shutdown()

    def bind(self, addr):
        """bind to the given address - calls method of the underlying socket
        @param addr: address/port number tuple
        @type addr: tuple"""
        self.__ssl_conn.bind(addr)

    def listen(self, backlog):
        """Listen for connections made to the socket.

        @param backlog: specifies the maximum number of queued connections and
        should be at least 1; the maximum value is system-dependent (usually 5).
        @param backlog: int
        """
        self.__ssl_conn.listen(backlog)

    def set_accept_state(self):
        """Set the connection to work in server mode. The handshake will be
        handled automatically by read/write"""
        self.__ssl_conn.set_accept_state()

    def accept(self):
        """Accept an SSL connection.

        @return: pair (ssl, addr) where ssl is a new SSL connection object and
        addr is the address bound to the other end of the SSL connection.
        @rtype: tuple
        """
        return self.__ssl_conn.accept()

    def set_connect_state(self):
        """Set the connection to work in client mode. The handshake will be
        handled automatically by read/write"""
        self.__ssl_conn.set_connect_state()

    def connect(self, addr):
        """Call the connect method of the underlying socket and set up SSL on
        the socket, using the Context object supplied to this Connection object
        at creation.

        @param addr: address/port number pair
        @type addr: tuple
        """
        self.__ssl_conn.connect(addr)

    def shutdown(self, how):
        """Send the shutdown message to the Connection.

        @param how: for socket.socket this flag determines whether read, write
        or both type operations are supported.  OpenSSL.SSL.Connection doesn't
        support this so this parameter is IGNORED
        @return: true if the shutdown message exchange is completed and false
        otherwise (in which case you call recv() or send() when the connection
        becomes readable/writeable.
        @rtype: bool
        """
        return self.__ssl_conn.shutdown()

    def renegotiate(self):
        """Renegotiate this connection's SSL parameters."""
        return self.__ssl_conn.renegotiate()

    def pending(self):
        """@return: numbers of bytes that can be safely read from the SSL
        buffer.
        @rtype: int
        """
        return self.__ssl_conn.pending()

    def send(self, data, *flags_arg):
        """Send data to the socket. Nb. The optional flags argument is ignored.
        - retained for compatibility with socket.socket interface

        @param data: data to send down the socket
        @type data: string
        """
        return self.__ssl_conn.send(data)

    def sendall(self, data):
        self.__ssl_conn.sendall(data)

    def recv(self, size=default_buf_size):
        """Receive data from the Connection.

        @param size: The maximum amount of data to be received at once
        @type size: int
        @return: data received.
        @rtype: string
        """
        return self.__ssl_conn.recv(size)

    def setblocking(self, mode):
        """Set this connection's underlying socket blocking _mode_.

        @param mode: blocking mode
        @type mode: int
        """
        self.__ssl_conn.setblocking(mode)

    def fileno(self):
        """
        @return: file descriptor number for the underlying socket
        @rtype: int
        """
        return self.__ssl_conn.fileno()

    def getsockopt(self, *args):
        """See socket.socket.getsockopt
        """
        return self.__ssl_conn.getsockopt(*args)

    def setsockopt(self, *args):
        """See socket.socket.setsockopt

        @return: value of the given socket option
        @rtype: int/string
        """
        return self.__ssl_conn.setsockopt(*args)

    def state_string(self):
        """Return the SSL state of this connection."""
        return self.__ssl_conn.state_string()

    def makefile(self, *args):
        """Specific to Python socket API and required by httplib: convert
        response into a file-like object.  This implementation reads using recv
        and copies the output into a StringIO buffer to simulate a file object
        for consumption by httplib

        Nb. Ignoring optional file open mode (StringIO is generic and will
        open for read and write unless a string is passed to the constructor)
        and buffer size - httplib set a zero buffer size which results in recv
        reading nothing

        @return: file object for data returned from socket
        @rtype: cStringIO.StringO
        """
        self._makefile_refs += 1
        
        # Optimisation
        _buf_size = self.buf_size

        i=0
        stream = BytesIO()
        startTime = datetime.utcnow()
        try:
            dat = self.__ssl_conn.recv(_buf_size)
            while dat:
                i+=1
                stream.write(dat)
                dat = self.__ssl_conn.recv(_buf_size)

        except (SSL.ZeroReturnError, SSL.SysCallError):
            # Connection is closed - assuming here that all is well and full
            # response has been received.  httplib will catch an error in
            # incomplete content since it checks the content-length header
            # against the actual length of data received
            pass

        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug("Socket.makefile %d recv calls completed in %s", i,
                      datetime.utcnow() - startTime)

        # Make sure to rewind the buffer otherwise consumers of the content will
        # read from the end of the buffer
        stream.seek(0)

        return stream
    
    def getsockname(self):
        """
        @return: the socket's own address
        @rtype:
        """
        return self.__ssl_conn.getsockname()

    def getpeername(self):
        """
        @return: remote address to which the socket is connected
        """
        return self.__ssl_conn.getpeername()

    def get_context(self):
        '''Retrieve the Context object associated with this Connection. '''
        return self.__ssl_conn.get_context()

    def get_peer_certificate(self):
        '''Retrieve the other side's certificate (if any)  '''
        return self.__ssl_conn.get_peer_certificate()
