"""
.. highlight:: python
   :linenothreshold: 5

.. highlight:: bash
   :linenothreshold: 5

scgi - an SCGI/WSGI gateway.

:copyright: Copyright (c) 2005, 2006 Allan Saddi <allan@saddi.com>
  All rights reserved.
:license:

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:

 1. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.

 THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS **AS IS** AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 SUCH DAMAGE.

For more information about SCGI and mod_scgi for Apache1/Apache2, see
http://www.mems-exchange.org/software/scgi/.

For more information about the Web Server Gateway Interface, see
http://www.python.org/peps/pep-0333.html.

Example usage::

  #!/usr/bin/env python
  import sys
  from myapplication import app # Assume app is your WSGI application object
  from scgi import WSGIServer
  ret = WSGIServer(app).run()
  sys.exit(ret and 42 or 0)

See the documentation for WSGIServer for more information.

About the bit of logic at the end:
Upon receiving SIGHUP, the python script will exit with status code 42. This
can be used by a wrapper script to determine if the python script should be
re-run. When a SIGINT or SIGTERM is received, the script exits with status
code 0, possibly indicating a normal exit.

Example wrapper script::

  #!/bin/sh
  STATUS=42
  while test $STATUS -eq 42; do
    python "$@" that_script_above.py
    STATUS=$?
  done
"""

__author__ = 'Allan Saddi <allan@saddi.com>'
__version__ = '$Revision$'

import logging
import socket

from flup.server import NoDefault
from flup.server.scgi_base import BaseSCGIServer, Connection
from flup.server.preforkserver import PreforkServer

__all__ = ['WSGIServer']

class WSGIServer(BaseSCGIServer, PreforkServer):
    """
    SCGI/WSGI server. For information about SCGI (Simple Common Gateway
    Interface), see http://www.mems-exchange.org/software/scgi/.

    This server is similar to SWAP http://www.idyll.org/~t/www-tools/wsgi/,
    another SCGI/WSGI server.

    It differs from SWAP in that it isn't based on scgi.scgi_server and
    therefore, it allows me to implement concurrency using threads. (Also,
    this server was written from scratch and really has no other depedencies.)
    Which server to use really boils down to whether you want multithreading
    or forking. (But as an aside, I've found scgi.scgi_server's implementation
    of preforking to be quite superior. So if your application really doesn't
    mind running in multiple processes, go use SWAP. ;)
    """
    def __init__(self, application, scriptName=NoDefault, environ=None,
                 bindAddress=('localhost', 4000), umask=None,
                 allowedServers=None,
                 loggingLevel=logging.INFO, debug=False, timeout=None, **kw):
        """
        scriptName is the initial portion of the URL path that "belongs"
        to your application. It is used to determine PATH_INFO (which doesn't
        seem to be passed in). An empty scriptName means your application
        is mounted at the root of your virtual host.

        environ, which must be a dictionary, can contain any additional
        environment variables you want to pass to your application.

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
        connections from anywhere.

        loggingLevel sets the logging level of the module-level logger.
        """
        BaseSCGIServer.__init__(self, application,
                                scriptName=scriptName,
                                environ=environ,
                                multithreaded=False,
                                multiprocess=True,
                                bindAddress=bindAddress,
                                umask=umask,
                                allowedServers=allowedServers,
                                loggingLevel=loggingLevel,
                                debug=debug)
        for key in ('multithreaded', 'multiprocess', 'jobClass', 'jobArgs'):
            if key in kw:
                del kw[key]
        
        PreforkServer.__init__(self, jobClass=Connection,
                               jobArgs=(self, timeout), **kw)

    def run(self):
        """
        Main loop. Call this after instantiating WSGIServer. SIGHUP, SIGINT,
        SIGQUIT, SIGTERM cause it to cleanup and return. (If a SIGHUP
        is caught, this method returns True. Returns False otherwise.)
        """
        self.logger.info('%s starting up', self.__class__.__name__)

        try:
            sock = self._setupSocket()
        except socket.error as e:
            self.logger.error('Failed to bind socket (%s), exiting', e[1])
            return False

        ret = PreforkServer.run(self, sock)

        self._cleanupSocket(sock)

        self.logger.info('%s shutting down%s', self.__class__.__name__,
                         self._hupReceived and ' (reload requested)' or '')

        return ret

if __name__ == '__main__':
    def test_app(environ, start_response):
        """Probably not the most efficient example."""
        from . import cgi
        start_response('200 OK', [('Content-Type', 'text/html')])
        yield '<html><head><title>Hello World!</title></head>\n' \
              '<body>\n' \
              '<p>Hello World!</p>\n' \
              '<table border="1">'
        names = list(environ.keys())
        names.sort()
        for name in names:
            yield '<tr><td>%s</td><td>%s</td></tr>\n' % (
                name, cgi.escape(repr(environ[name])))

        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)
        if form.list:
            yield '<tr><th colspan="2">Form data</th></tr>'

        for field in form.list:
            yield '<tr><td>%s</td><td>%s</td></tr>\n' % (
                field.name, field.value)

        yield '</table>\n' \
              '</body></html>\n'

    from wsgiref import validate
    test_app = validate.validator(test_app)
    WSGIServer(test_app,
               loggingLevel=logging.DEBUG).run()
