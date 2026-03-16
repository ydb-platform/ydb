# Copyright (c) 2006 Allan Saddi <allan@saddi.com>
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

import select
import struct
import socket
import errno
import types

__all__ = ['SCGIApp']

def encodeNetstring(s):
    return b''.join([str(len(s)).encode('latin-1'), b':', s, b','])

class SCGIApp(object):
    def __init__(self, connect=None, host=None, port=None,
                 filterEnviron=True):
        if host is not None:
            assert port is not None
            connect=(host, port)

        assert connect is not None
        self._connect = connect

        self._filterEnviron = filterEnviron
        
    def __call__(self, environ, start_response):
        sock = self._getConnection()

        outfile = sock.makefile('wb')
        infile = sock.makefile('rb')

        sock.close()

        # Filter WSGI environ and send as request headers
        if self._filterEnviron:
            headers = self._defaultFilterEnviron(environ)
        else:
            headers = self._lightFilterEnviron(environ)
        # TODO: Anything not from environ that needs to be sent also?

        content_length = int(environ.get('CONTENT_LENGTH') or 0)
        if 'CONTENT_LENGTH' in headers:
            del headers['CONTENT_LENGTH']
            
        headers_out = [b'CONTENT_LENGTH', str(content_length).encode('latin-1'), b'SCGI', b'1']
        for k,v in list(headers.items()):
            headers_out.append(k.encode('latin-1'))
            headers_out.append(v.encode('latin-1'))
        headers_out.append(b'') # For trailing NUL
        outfile.write(encodeNetstring(b'\x00'.join(headers_out)))

        # Transfer wsgi.input to outfile
        while True:
            chunk_size = min(content_length, 4096)
            s = environ['wsgi.input'].read(chunk_size)
            content_length -= len(s)
            outfile.write(s)

            if not s: break

        outfile.close()
        
        # Read result from SCGI server
        result = []
        while True:
            buf = infile.read(4096)
            if not buf: break

            result.append(buf)

        infile.close()
        
        result = b''.join(result)

        # Parse response headers
        status = b'200 OK'
        headers = []
        pos = 0
        while True:
            eolpos = result.find(b'\n', pos)
            if eolpos < 0: break
            line = result[pos:eolpos-1]
            pos = eolpos + 1

            # strip in case of CR. NB: This will also strip other
            # whitespace...
            line = line.strip()
            
            # Empty line signifies end of headers
            if not line: break

            # TODO: Better error handling
            header, value = line.split(b':', 1)
            header = header.strip().lower()
            value = value.strip()

            if header == b'status':
                # Special handling of Status header
                status = value
                if status.find(b' ') < 0:
                    # Append a dummy reason phrase if one was not provided
                    status += b' SCGIApp'
            else:
                headers.append((header, value))

        result = result[pos:]

        # Set WSGI status, headers, and return result.
        start_response(status, headers)
        return [result]

    def _getConnection(self):
        try:
            is_string = isinstance(self._connect, types.StringTypes)
        except AttributeError:
            is_string = isinstance(self._connect, str)
        if is_string:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self._connect)
        elif hasattr(socket, 'create_connection'):
            sock = socket.create_connection(self._connect)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(self._connect)
        return sock
    
    _environPrefixes = ['SERVER_', 'HTTP_', 'REQUEST_', 'REMOTE_', 'PATH_',
                        'CONTENT_']
    _environCopies = ['SCRIPT_NAME', 'QUERY_STRING', 'AUTH_TYPE']
    _environRenames = {}

    def _defaultFilterEnviron(self, environ):
        result = {}
        for n in list(environ.keys()):
            for p in self._environPrefixes:
                if n.startswith(p):
                    result[n] = environ[n]
            if n in self._environCopies:
                result[n] = environ[n]
            if n in self._environRenames:
                result[self._environRenames[n]] = environ[n]
                
        return result

    def _lightFilterEnviron(self, environ):
        result = {}
        for n in list(environ.keys()):
            if n.upper() == n:
                result[n] = environ[n]
        return result

if __name__ == '__main__':
    from flup.server.ajp import WSGIServer
    app = SCGIApp(connect=('localhost', 4000))
    #import paste.lint
    #app = paste.lint.middleware(app)
    WSGIServer(app).run()
