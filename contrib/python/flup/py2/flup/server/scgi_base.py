# Copyright (c) 2005, 2006 Allan Saddi <allan@saddi.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#
# $Id$

__author__ = 'Allan Saddi <allan@saddi.com>'
__version__ = '$Revision$'

import sys
import logging
import socket
import select
import errno
import io as StringIO
import signal
import datetime
import os
import warnings
import traceback

# Threads are required. If you want a non-threaded (forking) version, look at
# SWAP <http://www.idyll.org/~t/www-tools/wsgi/>.
try:
    import thread as _thread
except ImportError:
    import _thread
import threading

__all__ = ['BaseSCGIServer']

from flup.server import NoDefault

# The main classes use this name for logging.
LoggerName = 'scgi-wsgi'

# Set up module-level logger.
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(logging.Formatter('%(asctime)s : %(message)s',
                                       '%Y-%m-%d %H:%M:%S'))
logging.getLogger(LoggerName).addHandler(console)
del console

class ProtocolError(Exception):
    """
    Exception raised when the server does something unexpected or
    sends garbled data. Usually leads to a Connection closing.
    """
    pass

def recvall(sock, length):
    """
    Attempts to receive length bytes from a socket, blocking if necessary.
    (Socket may be blocking or non-blocking.)
    """
    dataList = []
    recvLen = 0
    while length:
        try:
            data = sock.recv(length)
        except socket.error as e:
            if e.args[0] == errno.EAGAIN:
                select.select([sock], [], [])
                continue
            else:
                raise
        if not data: # EOF
            break
        dataList.append(data)
        dataLen = len(data)
        recvLen += dataLen
        length -= dataLen
    return b''.join(dataList), recvLen

def readNetstring(sock):
    """
    Attempt to read a netstring from a socket.
    """
    # First attempt to read the length.
    size = b''
    while True:
        try:
            c = sock.recv(1)
        except socket.error as e:
            if e.args[0] == errno.EAGAIN:
                select.select([sock], [], [])
                continue
            else:
                raise
        if c == b':':
            break
        if not c:
            raise EOFError
        size += c

    # Try to decode the length.
    try:
        size = int(size)
        if size < 0:
            raise ValueError
    except ValueError:
        raise ProtocolError('invalid netstring length')

    # Now read the string.
    s, length = recvall(sock, size)

    if length < size:
        raise EOFError

    # Lastly, the trailer.
    trailer, length = recvall(sock, 1)

    if length < 1:
        raise EOFError

    if trailer != b',':
        raise ProtocolError('invalid netstring trailer')

    return s

class StdoutWrapper(object):
    """
    Wrapper for sys.stdout so we know if data has actually been written.
    """
    def __init__(self, stdout):
        self._file = stdout
        self.dataWritten = False

    def write(self, data):
        if data:
            self.dataWritten = True
        self._file.write(data)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def __getattr__(self, name):
        return getattr(self._file, name)

class Request(object):
    """
    Encapsulates data related to a single request.

    Public attributes:
      environ - Environment variables from web server.
      stdin - File-like object representing the request body.
      stdout - File-like object for writing the response.
    """
    def __init__(self, conn, environ, input, output):
        self._conn = conn
        self.environ = environ
        self.stdin = input
        self.stdout = StdoutWrapper(output)

        self.logger = logging.getLogger(LoggerName)

    def run(self):
        self.logger.info('%s %s%s',
                         self.environ['REQUEST_METHOD'],
                         self.environ.get('SCRIPT_NAME', ''),
                         self.environ.get('PATH_INFO', ''))

        start = datetime.datetime.now()

        try:
            self._conn.server.handler(self)
        except:
            self.logger.exception('Exception caught from handler')
            if not self.stdout.dataWritten:
                self._conn.server.error(self)

        end = datetime.datetime.now()

        handlerTime = end - start
        self.logger.debug('%s %s%s done (%.3f secs)',
                          self.environ['REQUEST_METHOD'],
                          self.environ.get('SCRIPT_NAME', ''),
                          self.environ.get('PATH_INFO', ''),
                          handlerTime.seconds +
                          handlerTime.microseconds / 1000000.0)

class TimeoutException(Exception):
    pass

class Connection(object):
    """
    Represents a single client (web server) connection. A single request
    is handled, after which the socket is closed.
    """
    def __init__(self, sock, addr, server, timeout):
        self._sock = sock
        self._addr = addr
        self.server = server
        self._timeout = timeout

        self.logger = logging.getLogger(LoggerName)

    def timeout_handler(self, signum, frame):
        self.logger.error('Timeout Exceeded')
        self.logger.error("\n".join(traceback.format_stack(frame)))

        raise TimeoutException

    def run(self):
        if len(self._addr) == 2:
            self.logger.debug('Connection starting up (%s:%d)',
                              self._addr[0], self._addr[1])

        try:
            self.processInput()
        except (EOFError, KeyboardInterrupt):
            pass
        except ProtocolError as e:
            self.logger.error("Protocol error '%s'", str(e))
        except:
            self.logger.exception('Exception caught in Connection')

        if len(self._addr) == 2:
            self.logger.debug('Connection shutting down (%s:%d)',
                              self._addr[0], self._addr[1])

        # All done!
        self._sock.close()

    def processInput(self):
        # Read headers
        headers = readNetstring(self._sock)
        headers = headers.split(b'\x00')[:-1]
        if len(headers) % 2 != 0:
            raise ProtocolError('invalid headers')
        environ = {}
        for i in range(len(headers) // 2):
            environ[headers[2*i].decode('latin-1')] = headers[2*i+1].decode('latin-1')

        clen = environ.get('CONTENT_LENGTH')
        if clen is None:
            raise ProtocolError('missing CONTENT_LENGTH')
        try:
            clen = int(clen)
            if clen < 0:
                raise ValueError
        except ValueError:
            raise ProtocolError('invalid CONTENT_LENGTH')

        self._sock.setblocking(1)
        if clen:
            input = self._sock.makefile('rb')
        else:
            # Empty input.
            input = StringIO.StringIO()

        # stdout
        output = self._sock.makefile('wb')

        # Allocate Request
        req = Request(self, environ, input, output)

        # If there is a timeout
        if self._timeout:
            old_alarm = signal.signal(signal.SIGALRM, self.timeout_handler)
            signal.alarm(self._timeout)
            
        # Run it.
        req.run()

        output.close()
        input.close()

        # Restore old handler if timeout was given
        if self._timeout:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_alarm)


class BaseSCGIServer(object):
    # What Request class to use.
    requestClass = Request

    def __init__(self, application, scriptName=NoDefault, environ=None,
                 multithreaded=True, multiprocess=False,
                 bindAddress=('localhost', 4000), umask=None,
                 allowedServers=NoDefault,
                 loggingLevel=logging.INFO, debug=False):
        """
        scriptName is the initial portion of the URL path that "belongs"
        to your application. It is used to determine PATH_INFO (which doesn't
        seem to be passed in). An empty scriptName means your application
        is mounted at the root of your virtual host.

        environ, which must be a dictionary, can contain any additional
        environment variables you want to pass to your application.

        Set multithreaded to False if your application is not thread-safe.

        Set multiprocess to True to explicitly set wsgi.multiprocess to
        True. (Only makes sense with threaded servers.)

        bindAddress is the address to bind to, which must be a string or
        a tuple of length 2. If a tuple, the first element must be a string,
        which is the host name or IPv4 address of a local interface. The
        2nd element of the tuple is the port number. If a string, it will
        be interpreted as a filename and a UNIX socket will be opened.

        If binding to a UNIX socket, umask may be set to specify what
        the umask is to be changed to before the socket is created in the
        filesystem. After the socket is created, the previous umask is
        restored.
        
        allowedServers must be None or a list of strings representing the
        IPv4 addresses of servers allowed to connect. None means accept
        connections from anywhere. By default, it is a list containing
        the single item '127.0.0.1'.

        loggingLevel sets the logging level of the module-level logger.
        """
        if environ is None:
            environ = {}

        self.application = application
        self.scriptName = scriptName
        self.environ = environ
        self.multithreaded = multithreaded
        self.multiprocess = multiprocess
        self.debug = debug
        self._bindAddress = bindAddress
        self._umask = umask
        if allowedServers is NoDefault:
            allowedServers = ['127.0.0.1']
        self._allowedServers = allowedServers

        # Used to force single-threadedness.
        self._appLock = _thread.allocate_lock()

        self.logger = logging.getLogger(LoggerName)
        self.logger.setLevel(loggingLevel)

    def _setupSocket(self):
        """Creates and binds the socket for communication with the server."""
        oldUmask = None
        if type(self._bindAddress) is str:
            # Unix socket
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                os.unlink(self._bindAddress)
            except OSError:
                pass
            if self._umask is not None:
                oldUmask = os.umask(self._umask)
        else:
            # INET socket
            assert type(self._bindAddress) is tuple
            family = socket.AF_INET
            if len(self._bindAddress) > 2:
                family = socket.AF_INET6
            sock = socket.socket(family, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock.bind(self._bindAddress)
        sock.listen(socket.SOMAXCONN)

        if oldUmask is not None:
            os.umask(oldUmask)

        return sock

    def _cleanupSocket(self, sock):
        """Closes the main socket."""
        sock.close()

    def _isClientAllowed(self, addr):
        ret = self._allowedServers is None or \
              len(addr) != 2 or \
              (len(addr) == 2 and addr[0] in self._allowedServers)
        if not ret:
            self.logger.warning('Server connection from %s disallowed',
                                addr[0])
        return ret

    def handler(self, request):
        """
        WSGI handler. Sets up WSGI environment, calls the application,
        and sends the application's response.
        """
        environ = request.environ
        environ.update(self.environ)

        environ['wsgi.version'] = (1,0)
        environ['wsgi.input'] = request.stdin
        environ['wsgi.errors'] = sys.stderr
        environ['wsgi.multithread'] = self.multithreaded
        environ['wsgi.multiprocess'] = self.multiprocess
        environ['wsgi.run_once'] = False

        if environ.get('HTTPS', 'off') in ('on', '1'):
            environ['wsgi.url_scheme'] = 'https'
        else:
            environ['wsgi.url_scheme'] = 'http'

        self._sanitizeEnv(environ)

        headers_set = []
        headers_sent = []
        result = None

        def write(data):
            if type(data) is str:
                data = data.encode('latin-1')

            assert type(data) is bytes, 'write() argument must be bytes'
            assert headers_set, 'write() before start_response()'

            if not headers_sent:
                status, responseHeaders = headers_sent[:] = headers_set
                found = False
                for header,value in responseHeaders:
                    if header.lower() == b'content-length':
                        found = True
                        break
                if not found and result is not None:
                    try:
                        if len(result) == 1:
                            responseHeaders.append((b'Content-Length',
                                                    str(len(data)).encode('latin-1')))
                    except:
                        pass
                s = b'Status: ' + status + b'\r\n'
                for header,value in responseHeaders:
                    s += header + b': ' + value + b'\r\n'
                s += b'\r\n'
                request.stdout.write(s)

            request.stdout.write(data)
            request.stdout.flush()

        def start_response(status, response_headers, exc_info=None):
            if exc_info:
                try:
                    if headers_sent:
                        # Re-raise if too late
                        raise exc_info[0](exc_info[1]).with_traceback(exc_info[2])
                finally:
                    exc_info = None # avoid dangling circular ref
            else:
                assert not headers_set, 'Headers already set!'

            if type(status) is str:
                status = status.encode('latin-1')

            assert type(status) is bytes, 'Status must be a bytes'
            assert len(status) >= 4, 'Status must be at least 4 characters'
            assert int(status[:3]), 'Status must begin with 3-digit code'
            assert status[3] == 0x20, 'Status must have a space after code'
            assert type(response_headers) is list, 'Headers must be a list'
            new_response_headers = []
            for name,val in response_headers:
                if type(name) is str:
                    name = name.encode('latin-1')
                if type(val) is str:
                    val = val.encode('latin-1')

                assert type(name) is bytes, 'Header name "%s" must be bytes' % name
                assert type(val) is bytes, 'Value of header "%s" must be bytes' % name

                new_response_headers.append((name, val))

            headers_set[:] = [status, new_response_headers]
            return write

        if not self.multithreaded:
            self._appLock.acquire()
        try:
            try:
                result = self.application(environ, start_response)
                try:
                    for data in result:
                        if data:
                            write(data)
                    if not headers_sent:
                        write(b'') # in case body was empty
                finally:
                    if hasattr(result, 'close'):
                        result.close()
            except socket.error as e:
                if e.args[0] != errno.EPIPE:
                    raise # Don't let EPIPE propagate beyond server
        finally:
            if not self.multithreaded:
                self._appLock.release()

    def _sanitizeEnv(self, environ):
        """Fill-in/deduce missing values in environ."""
        reqUri = None
        if 'REQUEST_URI' in environ:
            reqUri = environ['REQUEST_URI'].split('?', 1)

        # Ensure QUERY_STRING exists
        if 'QUERY_STRING' not in environ or not environ['QUERY_STRING']:
            if reqUri is not None and len(reqUri) > 1:
                environ['QUERY_STRING'] = reqUri[1]
            else:
                environ['QUERY_STRING'] = ''

        # Check WSGI_SCRIPT_NAME
        scriptName = environ.get('WSGI_SCRIPT_NAME')
        if scriptName is None:
            scriptName = self.scriptName
        else:
            warnings.warn('WSGI_SCRIPT_NAME environment variable for scgi '
                          'servers is deprecated',
                          DeprecationWarning)
            if scriptName.lower() == 'none':
                scriptName = None

        if scriptName is None:
            # Do nothing (most likely coming from cgi2scgi)
            return

        if scriptName is NoDefault:
            # Pull SCRIPT_NAME/PATH_INFO from environment, with empty defaults
            if 'SCRIPT_NAME' not in environ:
                environ['SCRIPT_NAME'] = ''
            if 'PATH_INFO' not in environ or not environ['PATH_INFO']:
                if reqUri is not None:
                    scriptName = environ['SCRIPT_NAME']
                    if not reqUri[0].startswith(scriptName):
                        self.logger.warning('SCRIPT_NAME does not match request URI')
                    environ['PATH_INFO'] = reqUri[0][len(scriptName):]
                else:
                    environ['PATH_INFO'] = ''
        else:
            # Configured scriptName
            warnings.warn('Configured SCRIPT_NAME is deprecated\n'
                          'Do not use WSGI_SCRIPT_NAME or the scriptName\n'
                          'keyword parameter -- they will be going away',
                          DeprecationWarning)

            value = environ['SCRIPT_NAME']
            value += environ.get('PATH_INFO', '')
            if not value.startswith(scriptName):
                self.logger.warning('scriptName does not match request URI')

            environ['PATH_INFO'] = value[len(scriptName):]
            environ['SCRIPT_NAME'] = scriptName

    def error(self, request):
        """
        Override to provide custom error handling. Ideally, however,
        all errors should be caught at the application level.
        """
        if self.debug:
            import cgitb
            request.stdout.write(b'Status: 500 Internal Server Error\r\n' +
                                 b'Content-Type: text/html\r\n\r\n' +
                                 cgitb.html(sys.exc_info()).encode('latin-1'))
        else:
            errorpage = b"""<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">
<html><head>
<title>Unhandled Exception</title>
</head><body>
<h1>Unhandled Exception</h1>
<p>An unhandled exception was thrown by the application.</p>
</body></html>
"""
            request.stdout.write(b'Status: 500 Internal Server Error\r\n' +
                                 b'Content-Type: text/html\r\n\r\n' +
                                 errorpage)
