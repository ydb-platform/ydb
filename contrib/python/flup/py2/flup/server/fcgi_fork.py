"""
.. highlight:: python
   :linenothreshold: 5

.. highlight:: bash
   :linenothreshold: 5

fcgi - a FastCGI/WSGI gateway.

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

For more information about FastCGI, see http://www.fastcgi.com/.

For more information about the Web Server Gateway Interface, see
http://www.python.org/peps/pep-0333.html.

Example usage::

  #!/usr/bin/env python
  from myapplication import app # Assume app is your WSGI application object
  from fcgi import WSGIServer
  WSGIServer(app).run()

See the documentation for WSGIServer for more information.

On most platforms, fcgi will fallback to regular CGI behavior if run in a
non-FastCGI context. If you want to force CGI behavior, set the environment
variable FCGI_FORCE_CGI to "Y" or "y".
"""

__author__ = 'Allan Saddi <allan@saddi.com>'
__version__ = '$Revision$'

import os

from .fcgi_base import BaseFCGIServer, FCGI_RESPONDER, \
     FCGI_MAX_CONNS, FCGI_MAX_REQS, FCGI_MPXS_CONNS
from .preforkserver import PreforkServer

__all__ = ['WSGIServer']

class WSGIServer(BaseFCGIServer, PreforkServer):
    """
    FastCGI server that supports the Web Server Gateway Interface. See
    http://www.python.org/peps/pep-0333.html.
    """
    def __init__(self, application, environ=None,
                 bindAddress=None, umask=None, multiplexed=False,
                 debug=False, roles=(FCGI_RESPONDER,), forceCGI=False,
                 timeout=None, **kw):
        """
        environ, if present, must be a dictionary-like object. Its
        contents will be copied into application's environ. Useful
        for passing application-specific variables.

        bindAddress, if present, must either be a string or a 2-tuple. If
        present, run() will open its own listening socket. You would use
        this if you wanted to run your application as an 'external' FastCGI
        app. (i.e. the webserver would no longer be responsible for starting
        your app) If a string, it will be interpreted as a filename and a UNIX
        socket will be opened. If a tuple, the first element, a string,
        is the interface name/IP to bind to, and the second element (an int)
        is the port number.
        """
        BaseFCGIServer.__init__(self, application,
                                environ=environ,
                                multithreaded=False,
                                multiprocess=True,
                                bindAddress=bindAddress,
                                umask=umask,
                                multiplexed=multiplexed,
                                debug=debug,
                                roles=roles,
                                forceCGI=forceCGI)
        for key in ('multithreaded', 'multiprocess', 'jobClass', 'jobArgs'):
            if key in kw:
                del kw[key]
        PreforkServer.__init__(self, jobClass=self._connectionClass,
                               jobArgs=(self, timeout), **kw)

        try:
            import resource
            # Attempt to glean the maximum number of connections
            # from the OS.
            try:
                maxProcs = resource.getrlimit(resource.RLIMIT_NPROC)[0]
                maxConns = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
                maxConns = min(maxConns, maxProcs)
            except AttributeError:
                maxConns = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        except ImportError:
            maxConns = 100 # Just some made up number.
        maxReqs = maxConns
        self.capability = {
            FCGI_MAX_CONNS: maxConns,
            FCGI_MAX_REQS: maxReqs,
            FCGI_MPXS_CONNS: 0
            }

    def _isClientAllowed(self, addr):
        return self._web_server_addrs is None or \
               (len(addr) == 2 and addr[0] in self._web_server_addrs)

    def run(self):
        """
        The main loop. Exits on SIGHUP, SIGINT, SIGTERM. Returns True if
        SIGHUP was received, False otherwise.
        """
        self._web_server_addrs = os.environ.get('FCGI_WEB_SERVER_ADDRS')
        if self._web_server_addrs is not None:
            self._web_server_addrs = [x.strip() for x in self._web_server_addrs.split(',')]

        sock = self._setupSocket()

        ret = PreforkServer.run(self, sock)

        self._cleanupSocket(sock)

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
    WSGIServer(test_app).run()
