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

"""Defines Twisted Web resources for status reporting."""


from greplin import scales
from greplin.scales import formats, util

from twisted.web import resource



class StatsResource(resource.Resource):
  """Twisted web resource for a status page."""

  isLeaf = True


  def __init__(self, serverName):
    resource.Resource.__init__(self)
    self.serverName = serverName


  def render_GET(self, request):
    """Renders a GET request, by showing this nodes stats and children."""
    fullPath = request.path.split('/')
    if not fullPath[-1]:
      fullPath = fullPath[:-1]
    parts = fullPath[2:]
    statDict = util.lookup(scales.getStats(), parts)

    if statDict is None:
      request.setResponseCode(404)
      return "Path not found."

    if 'query' in request.args:
      query = request.args['query'][0]
    else:
      query = None

    if 'format' in request.args and request.args['format'][0] == 'json':
      request.headers['content-type'] = 'text/javascript; charset=UTF-8'
      formats.jsonFormat(request, statDict, query)
    elif 'format' in request.args and request.args['format'][0] == 'prettyjson':
      request.headers['content-type'] = 'text/javascript; charset=UTF-8'
      formats.jsonFormat(request, statDict, query, pretty=True)
    else:
      formats.htmlHeader(request, '/' + '/'.join(parts), self.serverName, query)
      formats.htmlFormat(request, tuple(parts), statDict, query)

    return ''
