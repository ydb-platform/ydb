# Copyright (C) 2010-2013 Sebastian Rahlf and others (see AUTHORS).
#
# This program is release under the MIT license. You can find the full text of
# the license in the LICENSE file.

import json
import sys
import threading

from werkzeug.serving import make_server
from werkzeug.wrappers import Response, Request


class WSGIServer(threading.Thread):

    """
    HTTP server running a WSGI application in its own thread.
    """

    def __init__(self, host='127.0.0.1', port=0, application=None, **kwargs):
        self.app = application
        self._server = make_server(host, port, self.app, **kwargs)
        self.server_address = self._server.server_address

        super(WSGIServer, self).__init__(
            name=self.__class__,
            target=self._server.serve_forever)

    def __del__(self):
        self.stop()

    def stop(self):
        self._server.shutdown()

    @property
    def url(self):
        host, port = self.server_address
        proto = 'http' if self._server.ssl_context is None else 'https'
        return '%s://%s:%i' % (proto, host, port)


class ContentServer(WSGIServer):

    """
    Small test server which can be taught which content (i.e. string) to serve
    with which response code. Try the following snippet for testing API calls::

        server = ContentServer(port=8080)
        server.start()
        print 'Test server running at http://%s:%i' % server.server_address

        # any request to http://localhost:8080 will get a 503 response.
        server.content = 'Hello World!'
        server.code = 503

        # ...

        # we're done
        server.stop()

    """

    def __init__(self, host='127.0.0.1', port=0, ssl_context=None):
        super(ContentServer, self).__init__(host, port, self, ssl_context=ssl_context)
        self.content, self.code = ('', 204)  # HTTP 204: No Content
        self.headers = {}
        self.show_post_vars = False
        self.compress = None
        self.requests = []

    def __call__(self, environ, start_response):
        """
        This is the WSGI application.
        """
        request = Request(environ)
        self.requests.append(request)
        if (request.content_type == 'application/x-www-form-urlencoded'
        and request.method == 'POST' and self.show_post_vars):
            content = json.dumps(request.form)
        else:
            content = self.content

        response = Response(status=self.code)
        response.headers.clear()
        response.headers.extend(self.headers)

        # FIXME get compression working!
        # if self.compress == 'gzip':
        #     content = gzip.compress(content.encode('utf-8'))
        #     response.content_encoding = 'gzip'

        response.data = content
        return response(environ, start_response)

    def serve_content(self, content, code=200, headers=None):
        """
        Serves string content (with specified HTTP error code) as response to
        all subsequent request.

        :param content: content to be displayed
        :param code: HTTP status code
        :param headers: HTTP headers to be returned
        """
        self.content, self.code = (content, code)
        if headers:
            self.headers = headers


if __name__ == '__main__':  # pragma: no cover
    import os.path
    import time

    app = ContentServer()
    server = WSGIServer(application=app)
    server.start()

    print('HTTP server is running at %s' % server.url)
    print('Type <Ctrl-C> to stop')

    try:
        path = sys.argv[1]
    except IndexError:
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '..', 'README.rst')

    app.serve_content(open(path).read(), 302)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\rstopping...')
    server.stop()
