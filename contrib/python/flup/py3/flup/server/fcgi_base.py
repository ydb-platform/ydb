# Copyright (c) 2002, 2003, 2005, 2006 Allan Saddi <allan@saddi.com>
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
import os
import signal
import struct
import io as StringIO
import select
import socket
import errno
import traceback

try:
    try:
        import thread as _thread
    except ImportError:
        import _thread
    import threading
    thread_available = True
except ImportError:
    import _dummy_thread as thread
    import dummy_threading as threading
    thread_available = False

# Apparently 2.3 doesn't define SHUT_WR? Assume it is 1 in this case.
if not hasattr(socket, 'SHUT_WR'):
    socket.SHUT_WR = 1

__all__ = ['BaseFCGIServer']

# Constants from the spec.
FCGI_LISTENSOCK_FILENO = 0

FCGI_HEADER_LEN = 8

FCGI_VERSION_1 = 1

FCGI_BEGIN_REQUEST = 1
FCGI_ABORT_REQUEST = 2
FCGI_END_REQUEST = 3
FCGI_PARAMS = 4
FCGI_STDIN = 5
FCGI_STDOUT = 6
FCGI_STDERR = 7
FCGI_DATA = 8
FCGI_GET_VALUES = 9
FCGI_GET_VALUES_RESULT = 10
FCGI_UNKNOWN_TYPE = 11
FCGI_MAXTYPE = FCGI_UNKNOWN_TYPE

FCGI_NULL_REQUEST_ID = 0

FCGI_KEEP_CONN = 1

FCGI_RESPONDER = 1
FCGI_AUTHORIZER = 2
FCGI_FILTER = 3

FCGI_REQUEST_COMPLETE = 0
FCGI_CANT_MPX_CONN = 1
FCGI_OVERLOADED = 2
FCGI_UNKNOWN_ROLE = 3

FCGI_MAX_CONNS = 'FCGI_MAX_CONNS'
FCGI_MAX_REQS = 'FCGI_MAX_REQS'
FCGI_MPXS_CONNS = 'FCGI_MPXS_CONNS'

FCGI_Header = '!BBHHBx'
FCGI_BeginRequestBody = '!HB5x'
FCGI_EndRequestBody = '!LB3x'
FCGI_UnknownTypeBody = '!B7x'

FCGI_EndRequestBody_LEN = struct.calcsize(FCGI_EndRequestBody)
FCGI_UnknownTypeBody_LEN = struct.calcsize(FCGI_UnknownTypeBody)

if __debug__:
    import time

    # Set non-zero to write debug output to a file.
    DEBUG = 0
    DEBUGLOG = '/tmp/fcgi.log'

    def _debug(level, msg):
        if DEBUG < level:
            return

        try:
            f = open(DEBUGLOG, 'a')
            f.write('%sfcgi: %s\n' % (time.ctime()[4:-4], msg))
            f.close()
        except:
            pass

class InputStream(object):
    """
    File-like object representing FastCGI input streams (FCGI_STDIN and
    FCGI_DATA). Supports the minimum methods required by WSGI spec.
    """
    def __init__(self, conn):
        self._conn = conn

        # See Server.
        self._shrinkThreshold = conn.server.inputStreamShrinkThreshold

        self._buf = b''
        self._bufList = []
        self._pos = 0 # Current read position.
        self._avail = 0 # Number of bytes currently available.

        self._eof = False # True when server has sent EOF notification.

    def _shrinkBuffer(self):
        """Gets rid of already read data (since we can't rewind)."""
        if self._pos >= self._shrinkThreshold:
            self._buf = self._buf[self._pos:]
            self._avail -= self._pos
            self._pos = 0

            assert self._avail >= 0

    def _waitForData(self):
        """Waits for more data to become available."""
        self._conn.process_input()

    def read(self, n=-1):
        if self._pos == self._avail and self._eof:
            return b''
        while True:
            if n < 0 or (self._avail - self._pos) < n:
                # Not enough data available.
                if self._eof:
                    # And there's no more coming.
                    newPos = self._avail
                    break
                else:
                    # Wait for more data.
                    self._waitForData()
                    continue
            else:
                newPos = self._pos + n
                break
        # Merge buffer list, if necessary.
        if self._bufList:
            self._buf += b''.join(self._bufList)
            self._bufList = []
        r = self._buf[self._pos:newPos]
        self._pos = newPos
        self._shrinkBuffer()
        return r

    def readline(self, length=None):
        if self._pos == self._avail and self._eof:
            return b''
        while True:
            # Unfortunately, we need to merge the buffer list early.
            if self._bufList:
                self._buf += b''.join(self._bufList)
                self._bufList = []
            # Find newline.
            i = self._buf.find(b'\n', self._pos)
            if i < 0:
                # Not found?
                if self._eof:
                    # No more data coming.
                    newPos = self._avail
                    break
                else:
                    if length is not None and len(self._buf) >= length + self._pos:
                        newPos = self._pos + length
                        break
                    # Wait for more to come.
                    self._waitForData()
                    continue
            else:
                newPos = i + 1
                break
        r = self._buf[self._pos:newPos]
        self._pos = newPos
        self._shrinkBuffer()
        return r

    def readlines(self, sizehint=0):
        total = 0
        lines = []
        line = self.readline()
        while line:
            lines.append(line)
            total += len(line)
            if 0 < sizehint <= total:
                break
            line = self.readline()
        return lines

    def __iter__(self):
        return self

    def __next__(self):
        r = self.readline()
        if not r:
            raise StopIteration
        return r

    next = __next__

    def add_data(self, data):
        if not data:
            self._eof = True
        else:
            self._bufList.append(data)
            self._avail += len(data)

class MultiplexedInputStream(InputStream):
    """
    A version of InputStream meant to be used with MultiplexedConnections.
    Assumes the MultiplexedConnection (the producer) and the Request
    (the consumer) are running in different threads.
    """
    def __init__(self, conn):
        super(MultiplexedInputStream, self).__init__(conn)

        # Arbitrates access to this InputStream (it's used simultaneously
        # by a Request and its owning Connection object).
        lock = threading.RLock()

        # Notifies Request thread that there is new data available.
        self._lock = threading.Condition(lock)

    def _waitForData(self):
        # Wait for notification from add_data().
        self._lock.wait()

    def read(self, n=-1):
        self._lock.acquire()
        try:
            return super(MultiplexedInputStream, self).read(n)
        finally:
            self._lock.release()

    def readline(self, length=None):
        self._lock.acquire()
        try:
            return super(MultiplexedInputStream, self).readline(length)
        finally:
            self._lock.release()

    def add_data(self, data):
        self._lock.acquire()
        try:
            super(MultiplexedInputStream, self).add_data(data)
            self._lock.notify()
        finally:
            self._lock.release()

class OutputStream(object):
    """
    FastCGI output stream (FCGI_STDOUT/FCGI_STDERR). By default, calls to
    write() or writelines() immediately result in Records being sent back
    to the server. Buffering should be done in a higher level!
    """
    def __init__(self, conn, req, type, buffered=False):
        self._conn = conn
        self._req = req
        self._type = type
        self._buffered = buffered
        self._bufList = [] # Used if buffered is True
        self.dataWritten = False
        self.closed = False

    def _write(self, data):
        length = len(data)
        while length:
            toWrite = min(length, self._req.server.maxwrite - FCGI_HEADER_LEN)

            rec = Record(self._type, self._req.requestId)
            rec.contentLength = toWrite
            rec.contentData = data[:toWrite]
            self._conn.writeRecord(rec)

            data = data[toWrite:]
            length -= toWrite

    def write(self, data):
        assert not self.closed

        if not data:
            return
        if sys.version_info[0] != 2 and type(data) == str:
            data = data.encode()

        self.dataWritten = True

        if self._buffered:
            self._bufList.append(data)
        else:
            self._write(data)

    def writelines(self, lines):
        assert not self.closed

        for line in lines:
            self.write(line)

    def flush(self):
        # Only need to flush if this OutputStream is actually buffered.
        if self._buffered:
            data = b''.join(self._bufList)
            self._bufList = []
            self._write(data)

    # Though available, the following should NOT be called by WSGI apps.
    def close(self):
        """Sends end-of-stream notification, if necessary."""
        if not self.closed and self.dataWritten:
            self.flush()
            rec = Record(self._type, self._req.requestId)
            self._conn.writeRecord(rec)
            self.closed = True

class TeeOutputStream(object):
    """
    Simple wrapper around two or more output file-like objects that copies
    written data to all streams.
    """
    def __init__(self, streamList):
        self._streamList = streamList

    def write(self, data):
        for f in self._streamList:
            f.write(data)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        for f in self._streamList:
            f.flush()

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
        if hasattr(self._file, 'buffer'):
            self._file.buffer.write(data)
        else:
            self._file.write(data)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def __getattr__(self, name):
        return getattr(self._file, name)

def decode_pair(s, pos=0):
    """
    Decodes a name/value pair.

    The number of bytes decoded as well as the name/value pair
    are returned.
    """
    if sys.version_info[0] == 2:
        nameLength = ord(s[pos])
    else:
        nameLength = s[pos]
    if nameLength & 128:
        nameLength = struct.unpack('!L', s[pos:pos+4])[0] & 0x7fffffff
        pos += 4
    else:
        pos += 1

    if sys.version_info[0] == 2:
        valueLength = ord(s[pos])
    else:
        valueLength = s[pos]
    if valueLength & 128:
        valueLength = struct.unpack('!L', s[pos:pos+4])[0] & 0x7fffffff
        pos += 4
    else:
        pos += 1

    name = s[pos:pos+nameLength]
    pos += nameLength
    value = s[pos:pos+valueLength]
    pos += valueLength

    return (pos, (name, value))

def encode_pair(name, value):
    """
    Encodes a name/value pair.

    The encoded string is returned.
    """
    nameLength = len(name)
    if nameLength < 128:
        if sys.version_info[0] == 2:
            s = chr(nameLength)
        else:
            s = bytes([nameLength])
    else:
        s = struct.pack('!L', nameLength | 0x80000000)

    valueLength = len(value)
    if valueLength < 128:
        if sys.version_info[0] == 2:
            s += chr(valueLength)
        else:
            s += bytes([valueLength])
    else:
        s += struct.pack('!L', valueLength | 0x80000000)

    return s + name + value
    
class Record(object):
    """
    A FastCGI Record.

    Used for encoding/decoding records.
    """
    def __init__(self, type=FCGI_UNKNOWN_TYPE, requestId=FCGI_NULL_REQUEST_ID):
        self.version = FCGI_VERSION_1
        self.type = type
        self.requestId = requestId
        self.contentLength = 0
        self.paddingLength = 0
        self.contentData = b''

    def _recvall(sock, length):
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
    _recvall = staticmethod(_recvall)

    def read(self, sock):
        """Read and decode a Record from a socket."""
        try:
            header, length = self._recvall(sock, FCGI_HEADER_LEN)
        except:
            raise EOFError

        if length < FCGI_HEADER_LEN:
            raise EOFError
        
        self.version, self.type, self.requestId, self.contentLength, \
                      self.paddingLength = struct.unpack(FCGI_Header, header)

        if __debug__: _debug(9, 'read: fd = %d, type = %d, requestId = %d, '
                             'contentLength = %d' %
                             (sock.fileno(), self.type, self.requestId,
                              self.contentLength))
        
        if self.contentLength:
            try:
                self.contentData, length = self._recvall(sock,
                                                         self.contentLength)
            except:
                raise EOFError

            if length < self.contentLength:
                raise EOFError

        if self.paddingLength:
            try:
                self._recvall(sock, self.paddingLength)
            except:
                raise EOFError

    def _sendall(sock, data):
        """
        Writes data to a socket and does not return until all the data is sent.
        """
        length = len(data)
        while length:
            try:
                sent = sock.send(data)
            except socket.error as e:
                if e.args[0] == errno.EAGAIN:
                    select.select([], [sock], [])
                    continue
                else:
                    raise
            data = data[sent:]
            length -= sent
    _sendall = staticmethod(_sendall)

    def write(self, sock):
        """Encode and write a Record to a socket."""
        self.paddingLength = -self.contentLength & 7

        if __debug__: _debug(9, 'write: fd = %d, type = %d, requestId = %d, '
                             'contentLength = %d' %
                             (sock.fileno(), self.type, self.requestId,
                              self.contentLength))

        header = struct.pack(FCGI_Header, self.version, self.type,
                             self.requestId, self.contentLength,
                             self.paddingLength)
        self._sendall(sock, header)
        if self.contentLength:
            self._sendall(sock, self.contentData)
        if self.paddingLength:
            self._sendall(sock, b'\x00'*self.paddingLength)
            
class TimeoutException(Exception):
    pass

class Request(object):
    """
    Represents a single FastCGI request.

    These objects are passed to your handler and is the main interface
    between your handler and the fcgi module. The methods should not
    be called by your handler. However, server, params, stdin, stdout,
    stderr, and data are free for your handler's use.
    """
    def __init__(self, conn, inputStreamClass, timeout):
        self._conn = conn
        self._timeout = timeout

        self.server = conn.server
        self.params = {}
        self.stdin = inputStreamClass(conn)
        self.stdout = OutputStream(conn, self, FCGI_STDOUT)
        self.stderr = OutputStream(conn, self, FCGI_STDERR, buffered=True)
        self.data = inputStreamClass(conn)

    def timeout_handler(self, signum, frame):
        self.stderr.write('Timeout Exceeded\n')
        self.stderr.write("\n".join(traceback.format_stack(frame)))
        self.stderr.flush()

        raise TimeoutException

    def run(self):
        """Runs the handler, flushes the streams, and ends the request."""
        # If there is a timeout
        if self._timeout:
            old_alarm = signal.signal(signal.SIGALRM, self.timeout_handler)
            signal.alarm(self._timeout)
            
        try:
            protocolStatus, appStatus = self.server.handler(self)
        except:
            traceback.print_exc(file=self.stderr)
            self.stderr.flush()
            if not self.stdout.dataWritten:
                self.server.error(self)

            protocolStatus, appStatus = FCGI_REQUEST_COMPLETE, 0

        if __debug__: _debug(1, 'protocolStatus = %d, appStatus = %d' %
                             (protocolStatus, appStatus))

        # Restore old handler if timeout was given
        if self._timeout:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_alarm)

        try:
            self._flush()
            self._end(appStatus, protocolStatus)
        except socket.error as e:
            if e.args[0] != errno.EPIPE:
                raise

    def _end(self, appStatus=0, protocolStatus=FCGI_REQUEST_COMPLETE):
        self._conn.end_request(self, appStatus, protocolStatus)
        
    def _flush(self):
        self.stdout.close()
        self.stderr.close()

class CGIRequest(Request):
    """A normal CGI request disguised as a FastCGI request."""
    def __init__(self, server):
        # These are normally filled in by Connection.
        self.requestId = 1
        self.role = FCGI_RESPONDER
        self.flags = 0
        self.aborted = False
        
        self.server = server
        self.params = dict(os.environ)
        self.stdin = sys.stdin
        self.stdout = StdoutWrapper(sys.stdout) # Oh, the humanity!
        self.stderr = sys.stderr
        self.data = StringIO.StringIO()
        self._timeout = 0
        
    def _end(self, appStatus=0, protocolStatus=FCGI_REQUEST_COMPLETE):
        sys.exit(appStatus)

    def _flush(self):
        # Not buffered, do nothing.
        pass

class Connection(object):
    """
    A Connection with the web server.

    Each Connection is associated with a single socket (which is
    connected to the web server) and is responsible for handling all
    the FastCGI message processing for that socket.
    """
    _multiplexed = False
    _inputStreamClass = InputStream

    def __init__(self, sock, addr, server, timeout):
        self._sock = sock
        self._addr = addr
        self.server = server
        self._timeout = timeout

        # Active Requests for this Connection, mapped by request ID.
        self._requests = {}

    def _cleanupSocket(self):
        """Close the Connection's socket."""
        try:
            self._sock.shutdown(socket.SHUT_WR)
        except:
            return
        try:
            while True:
                r, w, e = select.select([self._sock], [], [])
                if not r or not self._sock.recv(1024):
                    break
        except:
            pass
        self._sock.close()
        
    def run(self):
        """Begin processing data from the socket."""
        self._keepGoing = True
        while self._keepGoing:
            try:
                self.process_input()
            except (EOFError, KeyboardInterrupt):
                break
            except (select.error, socket.error) as e:
                if e.args[0] == errno.EBADF: # Socket was closed by Request.
                    break
                raise

        self._cleanupSocket()

    def process_input(self):
        """Attempt to read a single Record from the socket and process it."""
        # Currently, any children Request threads notify this Connection
        # that it is no longer needed by closing the Connection's socket.
        # We need to put a timeout on select, otherwise we might get
        # stuck in it indefinitely... (I don't like this solution.)
        while self._keepGoing:
            try:
                r, w, e = select.select([self._sock], [], [], 1.0)
            except ValueError:
                # Sigh. ValueError gets thrown sometimes when passing select
                # a closed socket.
                raise EOFError
            if r: break
        if not self._keepGoing:
            return
        rec = Record()
        rec.read(self._sock)

        if rec.type == FCGI_GET_VALUES:
            self._do_get_values(rec)
        elif rec.type == FCGI_BEGIN_REQUEST:
            self._do_begin_request(rec)
        elif rec.type == FCGI_ABORT_REQUEST:
            self._do_abort_request(rec)
        elif rec.type == FCGI_PARAMS:
            self._do_params(rec)
        elif rec.type == FCGI_STDIN:
            self._do_stdin(rec)
        elif rec.type == FCGI_DATA:
            self._do_data(rec)
        elif rec.requestId == FCGI_NULL_REQUEST_ID:
            self._do_unknown_type(rec)
        else:
            # Need to complain about this.
            pass

    def writeRecord(self, rec):
        """
        Write a Record to the socket.
        """
        rec.write(self._sock)

    def end_request(self, req, appStatus=0,
                    protocolStatus=FCGI_REQUEST_COMPLETE, remove=True):
        """
        End a Request.

        Called by Request objects. An FCGI_END_REQUEST Record is
        sent to the web server. If the web server no longer requires
        the connection, the socket is closed, thereby ending this
        Connection (run() returns).
        """
        rec = Record(FCGI_END_REQUEST, req.requestId)
        rec.contentData = struct.pack(FCGI_EndRequestBody, appStatus,
                                      protocolStatus)
        rec.contentLength = FCGI_EndRequestBody_LEN
        self.writeRecord(rec)

        if remove:
            del self._requests[req.requestId]

        if __debug__: _debug(2, 'end_request: flags = %d' % req.flags)

        if not (req.flags & FCGI_KEEP_CONN) and not self._requests:
            self._cleanupSocket()
            self._keepGoing = False

    def _do_get_values(self, inrec):
        """Handle an FCGI_GET_VALUES request from the web server."""
        outrec = Record(FCGI_GET_VALUES_RESULT)

        pos = 0
        while pos < inrec.contentLength:
            pos, (name, value) = decode_pair(inrec.contentData, pos)
            cap = self.server.capability.get(name)
            if cap is not None:
                if sys.version_info[0] == 2:
                    outrec.contentData += encode_pair(name, str(cap))
                else:
                    outrec.contentData += encode_pair(name, str(cap).encode('latin-1'))

        outrec.contentLength = len(outrec.contentData)
        self.writeRecord(outrec)

    def _do_begin_request(self, inrec):
        """Handle an FCGI_BEGIN_REQUEST from the web server."""
        role, flags = struct.unpack(FCGI_BeginRequestBody, inrec.contentData)

        req = self.server.request_class(self, self._inputStreamClass,
                                        self._timeout)
        req.requestId, req.role, req.flags = inrec.requestId, role, flags
        req.aborted = False

        if not self._multiplexed and self._requests:
            # Can't multiplex requests.
            self.end_request(req, 0, FCGI_CANT_MPX_CONN, remove=False)
        else:
            self._requests[inrec.requestId] = req

    def _do_abort_request(self, inrec):
        """
        Handle an FCGI_ABORT_REQUEST from the web server.

        We just mark a flag in the associated Request.
        """
        req = self._requests.get(inrec.requestId)
        if req is not None:
            req.aborted = True

    def _start_request(self, req):
        """Run the request."""
        # Not multiplexed, so run it inline.
        req.run()

    def _do_params(self, inrec):
        """
        Handle an FCGI_PARAMS Record.

        If the last FCGI_PARAMS Record is received, start the request.
        """
        req = self._requests.get(inrec.requestId)
        if req is not None:
            if inrec.contentLength:
                pos = 0
                while pos < inrec.contentLength:
                    pos, (name, value) = decode_pair(inrec.contentData, pos)
                    if sys.version_info[0] == 2:
                        req.params[name] = value
                    else:
                        req.params[name.decode('latin-1')] = value.decode('latin-1')
            else:
                self._start_request(req)

    def _do_stdin(self, inrec):
        """Handle the FCGI_STDIN stream."""
        req = self._requests.get(inrec.requestId)
        if req is not None:
            req.stdin.add_data(inrec.contentData)

    def _do_data(self, inrec):
        """Handle the FCGI_DATA stream."""
        req = self._requests.get(inrec.requestId)
        if req is not None:
            req.data.add_data(inrec.contentData)

    def _do_unknown_type(self, inrec):
        """Handle an unknown request type. Respond accordingly."""
        outrec = Record(FCGI_UNKNOWN_TYPE)
        outrec.contentData = struct.pack(FCGI_UnknownTypeBody, inrec.type)
        outrec.contentLength = FCGI_UnknownTypeBody_LEN
        self.writeRecord(outrec)
        
class MultiplexedConnection(Connection):
    """
    A version of Connection capable of handling multiple requests
    simultaneously.
    """
    _multiplexed = True
    _inputStreamClass = MultiplexedInputStream

    def __init__(self, sock, addr, server, timeout):
        super(MultiplexedConnection, self).__init__(sock, addr, server,
                                                    timeout)

        # Used to arbitrate access to self._requests.
        lock = threading.RLock()

        # Notification is posted everytime a request completes, allowing us
        # to quit cleanly.
        self._lock = threading.Condition(lock)

    def _cleanupSocket(self):
        # Wait for any outstanding requests before closing the socket.
        self._lock.acquire()
        while self._requests:
            self._lock.wait()
        self._lock.release()

        super(MultiplexedConnection, self)._cleanupSocket()
        
    def writeRecord(self, rec):
        # Must use locking to prevent intermingling of Records from different
        # threads.
        self._lock.acquire()
        try:
            # Probably faster than calling super. ;)
            rec.write(self._sock)
        finally:
            self._lock.release()

    def end_request(self, req, appStatus=0,
                    protocolStatus=FCGI_REQUEST_COMPLETE, remove=True):
        self._lock.acquire()
        try:
            super(MultiplexedConnection, self).end_request(req, appStatus,
                                                           protocolStatus,
                                                           remove)
            self._lock.notify()
        finally:
            self._lock.release()

    def _do_begin_request(self, inrec):
        self._lock.acquire()
        try:
            super(MultiplexedConnection, self)._do_begin_request(inrec)
        finally:
            self._lock.release()

    def _do_abort_request(self, inrec):
        self._lock.acquire()
        try:
            super(MultiplexedConnection, self)._do_abort_request(inrec)
        finally:
            self._lock.release()

    def _start_request(self, req):
        try:
            _thread.start_new_thread(req.run, ())
        except thread.error as e:
            self.end_request(req, 0, FCGI_OVERLOADED, remove=True)

    def _do_params(self, inrec):
        self._lock.acquire()
        try:
            super(MultiplexedConnection, self)._do_params(inrec)
        finally:
            self._lock.release()

    def _do_stdin(self, inrec):
        self._lock.acquire()
        try:
            super(MultiplexedConnection, self)._do_stdin(inrec)
        finally:
            self._lock.release()

    def _do_data(self, inrec):
        self._lock.acquire()
        try:
            super(MultiplexedConnection, self)._do_data(inrec)
        finally:
            self._lock.release()
        
class BaseFCGIServer(object):
    request_class = Request
    cgirequest_class = CGIRequest

    # The maximum number of bytes (per Record) to write to the server.
    # I've noticed mod_fastcgi has a relatively small receive buffer (8K or
    # so).
    maxwrite = 8192

    # Limits the size of the InputStream's string buffer to this size + the
    # server's maximum Record size. Since the InputStream is not seekable,
    # we throw away already-read data once this certain amount has been read.
    inputStreamShrinkThreshold = 102400 - 8192

    def __init__(self, application, environ=None,
                 multithreaded=True, multiprocess=False,
                 bindAddress=None, umask=None, multiplexed=False,
                 debug=False, roles=(FCGI_RESPONDER,),
                 forceCGI=False):
        """
        bindAddress, if present, must either be a string or a 2-tuple. If
        present, run() will open its own listening socket. You would use
        this if you wanted to run your application as an 'external' FastCGI
        app. (i.e. the webserver would no longer be responsible for starting
        your app) If a string, it will be interpreted as a filename and a UNIX
        socket will be opened. If a tuple, the first element, a string,
        is the interface name/IP to bind to, and the second element (an int)
        is the port number.

        If binding to a UNIX socket, umask may be set to specify what
        the umask is to be changed to before the socket is created in the
        filesystem. After the socket is created, the previous umask is
        restored.
        
        Set multiplexed to True if you want to handle multiple requests
        per connection. Some FastCGI backends (namely mod_fastcgi) don't
        multiplex requests at all, so by default this is off (which saves
        on thread creation/locking overhead). If threads aren't available,
        this keyword is ignored; it's not possible to multiplex requests
        at all.
        """
        if environ is None:
            environ = {}

        self.application = application
        self.environ = environ
        self.multithreaded = multithreaded
        self.multiprocess = multiprocess
        self.debug = debug
        self.roles = roles
        self.forceCGI = forceCGI

        self._bindAddress = bindAddress
        self._umask = umask
        
        # Used to force single-threadedness
        self._appLock = _thread.allocate_lock()

        if thread_available:
            try:
                import resource
                # Attempt to glean the maximum number of connections
                # from the OS.
                maxConns = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
            except ImportError:
                maxConns = 100 # Just some made up number.
            maxReqs = maxConns
            if multiplexed:
                self._connectionClass = MultiplexedConnection
                maxReqs *= 5 # Another made up number.
            else:
                self._connectionClass = Connection
            self.capability = {
                FCGI_MAX_CONNS: maxConns,
                FCGI_MAX_REQS: maxReqs,
                FCGI_MPXS_CONNS: multiplexed and 1 or 0
                }
        else:
            self._connectionClass = Connection
            self.capability = {
                # If threads aren't available, these are pretty much correct.
                FCGI_MAX_CONNS: 1,
                FCGI_MAX_REQS: 1,
                FCGI_MPXS_CONNS: 0
                }

    def _setupSocket(self):
        if self._bindAddress is None:
            # Run as a normal FastCGI?
            # FastCGI/CGI discrimination is broken on Mac OS X.
            # Set the environment variable FCGI_FORCE_CGI to "Y" or "y"
            # if you want to run your app as a simple CGI. (You can do
            # this with Apache's mod_env [not loaded by default in OS X
            # client, ha ha] and the SetEnv directive.)
            forceCGI = self.forceCGI or \
               os.environ.get('FCGI_FORCE_CGI', 'N').upper().startswith('Y')

            if forceCGI:
                isFCGI = False
            else:
                if not hasattr(socket, 'fromfd'):
                    # can happen on win32, no socket.fromfd there!
                    raise ValueError(
                        'If you want FCGI, please create an external FCGI server '
                        'by providing a valid bindAddress. '
                        'If you want CGI, please force CGI operation. Use '
                        'FCGI_FORCE_CGI=Y environment or forceCGI parameter.')
                sock = socket.fromfd(FCGI_LISTENSOCK_FILENO, socket.AF_INET,
                                     socket.SOCK_STREAM)
                isFCGI = True
                try:
                    sock.getpeername()
                except socket.error as e:
                    if e.args[0] == errno.ENOTSOCK:
                        # Not a socket, assume CGI context.
                        isFCGI = False
                    elif e.args[0] != errno.ENOTCONN:
                        raise

            if not isFCGI:
                req = self.cgirequest_class(self)
                req.run()
                sys.exit(0)
        else:
            # Run as a server
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

    def handler(self, req):
        """Special handler for WSGI."""
        if req.role not in self.roles:
            return FCGI_UNKNOWN_ROLE, 0

        # Mostly taken from example CGI gateway.
        environ = req.params
        environ.update(self.environ)

        environ['wsgi.version'] = (1,0)
        environ['wsgi.input'] = req.stdin
        if self._bindAddress is None:
            stderr = req.stderr
        else:
            stderr = TeeOutputStream((sys.stderr, req.stderr))
        environ['wsgi.errors'] = stderr
        environ['wsgi.multithread'] = not isinstance(req, CGIRequest) and \
                                      thread_available and self.multithreaded
        environ['wsgi.multiprocess'] = isinstance(req, CGIRequest) or \
                                       self.multiprocess
        environ['wsgi.run_once'] = isinstance(req, CGIRequest)

        if environ.get('HTTPS', 'off') in ('on', '1'):
            environ['wsgi.url_scheme'] = 'https'
        else:
            environ['wsgi.url_scheme'] = 'http'

        self._sanitizeEnv(environ)

        headers_set = []
        headers_sent = []
        result = None

        def write(data):
            if sys.version_info[0] != 2 and type(data) is str:
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
                            if sys.version_info[0] ==2:
                                responseHeaders.append((b'Content-Length',
                                                        str(len(data))))
                            else:
                                responseHeaders.append((b'Content-Length',
                                                        str(len(data)).encode('latin-1')))
                    except:
                        pass
                s = b'Status: ' + status + b'\r\n'
                for header,value in responseHeaders:
                    s += header + b': ' + value + b'\r\n'
                s += b'\r\n'
                req.stdout.write(s)

            req.stdout.write(data)
            req.stdout.flush()

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

            if sys.version_info[0] != 2 and type(status) is str:
                status = status.encode('latin-1')

            assert type(status) is bytes, 'Status must be a string'
            assert len(status) >= 4, 'Status must be at least 4 characters'
            assert int(status[:3]), 'Status must begin with 3-digit code'
            if sys.version_info[0] == 2:
                assert status[3] == ' ', 'Status must have a space after code'
            else:
                assert status[3] == 0x20, 'Status must have a space after code'
            assert type(response_headers) is list, 'Headers must be a list'
            new_response_headers = []
            for name,val in response_headers:
                if sys.version_info[0] != 2 and type(name) is str:
                    name = name.encode('latin-1')
                if sys.version_info[0] != 2 and type(val) is str:
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

        return FCGI_REQUEST_COMPLETE, 0

    def _sanitizeEnv(self, environ):
        """Ensure certain values are present, if required by WSGI."""
        if 'SCRIPT_NAME' not in environ:
            environ['SCRIPT_NAME'] = ''

        reqUri = None
        if 'REQUEST_URI' in environ:
            reqUri = environ['REQUEST_URI'].split('?', 1)

        if 'PATH_INFO' not in environ or not environ['PATH_INFO']:
            if reqUri is not None:
                scriptName = environ['SCRIPT_NAME']
                if not reqUri[0].startswith(scriptName):
                    environ['wsgi.errors'].write('WARNING: SCRIPT_NAME does not match REQUEST_URI')
                environ['PATH_INFO'] = reqUri[0][len(scriptName):]
            else:
                environ['PATH_INFO'] = ''
        if 'QUERY_STRING' not in environ or not environ['QUERY_STRING']:
            if reqUri is not None and len(reqUri) > 1:
                environ['QUERY_STRING'] = reqUri[1]
            else:
                environ['QUERY_STRING'] = ''

        # If any of these are missing, it probably signifies a broken
        # server...
        for name,default in [('REQUEST_METHOD', 'GET'),
                             ('SERVER_NAME', 'localhost'),
                             ('SERVER_PORT', '80'),
                             ('SERVER_PROTOCOL', 'HTTP/1.0')]:
            if name not in environ:
                environ['wsgi.errors'].write('%s: missing FastCGI param %s '
                                             'required by WSGI!\n' %
                                             (self.__class__.__name__, name))
                environ[name] = default
            
    def error(self, req):
        """
        Called by Request if an exception occurs within the handler. May and
        should be overridden.
        """
        if self.debug:
            import cgitb
            req.stdout.write(b'Status: 500 Internal Server Error\r\n' +
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
            req.stdout.write(b'Status: 500 Internal Server Error\r\n' +
                             b'Content-Type: text/html\r\n\r\n' +
                             errorpage)
