from __future__ import absolute_import

try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
from . import sqltap
try:
    import queue
except ImportError:
    import Queue as queue

from werkzeug.wrappers import Response


class SQLTapMiddleware(object):
    """ SQLTap dashboard middleware for WSGI applications.

    For example, if you are using Flask::

        app.wsgi_app = SQLTapMiddleware(app.wsgi_app)

    And then you can use SQLTap dashboard from ``/__sqltap__`` page (this
    path prefix can be set by ``path`` parameter).

    :param app: A WSGI application object to be wrap.
    :param path: A path prefix for access. Default is `'/__sqltap__'`
    """

    def __init__(self, app, path='/__sqltap__'):
        self.app = app
        self.path = path.rstrip('/')
        self.on = False
        self.collector = queue.Queue(0)
        self.stats = []
        self.profiler = sqltap.ProfilingSession(collect_fn=self.collector.put)

    def __call__(self, environ, start_response):
        path = environ.get('PATH_INFO', '')
        if path == self.path or path == self.path + '/':
            return self.render(environ, start_response)
        return self.app(environ, start_response)

    def start(self):
        if not self.on:
            self.on = True
            self.profiler.start()

    def stop(self):
        if self.on:
            self.on = False
            self.profiler.stop()

    def render(self, environ, start_response):
        verb = environ.get('REQUEST_METHOD', 'GET').strip().upper()
        if verb not in ('GET', 'POST'):
            response = Response('405 Method Not Allowed', status=405,
                                mimetype='text/plain')
            response.headers['Allow'] = 'GET, POST'
            return response(environ, start_response)

        # handle on/off switch
        if verb == 'POST':
            try:
                clen = int(environ.get('CONTENT_LENGTH', '0'))
            except ValueError:
                clen = 0
            body = environ['wsgi.input'].read(clen).decode('utf-8')
            body = urlparse.parse_qs(body)
            clear = body.get('clear', None)
            if clear:
                del self.stats[:]
                return self.render_response(environ, start_response)

            turn = body.get('turn', ' ')[0].strip().lower()
            if turn not in ('on', 'off'):
                response = Response('400 Bad Request: parameter '
                                    '"turn=(on|off)" required',
                                    status='400', mimetype='text/plain')
                return response(environ, start_response)
            if turn == 'on':
                self.start()
            else:
                self.stop()

        try:
            while True:
                self.stats.append(self.collector.get(block=False))
        except queue.Empty:
            pass

        return self.render_response(environ, start_response)

    def render_response(self, environ, start_response):
        html = sqltap.report(self.stats, middleware=self, report_format="wsgi")
        response = Response(html.encode('utf-8'), mimetype="text/html")
        return response(environ, start_response)
