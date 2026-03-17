# Copyright 2011 The scales Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Defines a Flask request handler for status reporting."""


from greplin import scales
from greplin.scales import formats, util

from flask import request, abort

from six import StringIO

import functools


def statsHandler(serverName, path=''):
  """Renders a GET request, by showing this nodes stats and children."""
  path = path.lstrip('/')
  parts = path.split('/')
  if not parts[0]:
    parts = parts[1:]
  statDict = util.lookup(scales.getStats(), parts)

  if statDict is None:
    abort(404, 'No stats found with path /%s' % '/'.join(parts))

  output = StringIO()
  outputFormat = request.args.get('format', 'html')
  query = request.args.get('query', None)
  if outputFormat == 'json':
    formats.jsonFormat(output, statDict, query)
  elif outputFormat == 'prettyjson':
    formats.jsonFormat(output, statDict, query, pretty=True)
  else:
    formats.htmlHeader(output, '/' + path, serverName, query)
    formats.htmlFormat(output, tuple(parts), statDict, query)

  return output.getvalue()


def registerStatsHandler(app, serverName, prefix='/status/'):
  """Register the stats handler with a Flask app, serving routes
  with a given prefix. The prefix defaults to '/status/', which is
  generally what you want."""
  if prefix[-1] != '/':
    prefix += '/'
  handler = functools.partial(statsHandler, serverName)
  app.add_url_rule(prefix, 'statsHandler', handler, methods=['GET'])
  app.add_url_rule(prefix + '<path:path>', 'statsHandler', handler, methods=['GET'])


def serveInBackground(port, serverName, prefix='/status/'):
  """Convenience function: spawn a background server thread that will
  serve HTTP requests to get the status. Returns the thread."""
  import flask, threading
  from wsgiref.simple_server import make_server
  app = flask.Flask(__name__)
  registerStatsHandler(app, serverName, prefix)
  server = threading.Thread(target=make_server('', port, app).serve_forever)
  server.daemon = True
  server.start()
  return server
