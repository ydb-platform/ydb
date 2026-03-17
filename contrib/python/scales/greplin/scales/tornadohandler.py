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

"""Defines a Tornado request handler for status reporting."""


from greplin import scales
from greplin.scales import formats, util

import tornado.web



class StatsHandler(tornado.web.RequestHandler):
  """Tornado request handler for a status page."""

  serverName = None


  def initialize(self, serverName): # pylint: disable=W0221
    """Initializes the handler."""
    self.serverName = serverName


  def get(self, path): # pylint: disable=W0221
    """Renders a GET request, by showing this nodes stats and children."""
    path = path or ''
    path = path.lstrip('/')
    parts = path.split('/')
    if not parts[0]:
      parts = parts[1:]
    statDict = util.lookup(scales.getStats(), parts)

    if statDict is None:
      self.set_status(404)
      self.finish('Path not found.')
      return

    outputFormat = self.get_argument('format', default='html')
    query = self.get_argument('query', default=None)
    if outputFormat == 'json':
      formats.jsonFormat(self, statDict, query)
    elif outputFormat == 'prettyjson':
      formats.jsonFormat(self, statDict, query, pretty=True)
    else:
      formats.htmlHeader(self, '/' + path, self.serverName, query)
      formats.htmlFormat(self, tuple(parts), statDict, query)

    return None
