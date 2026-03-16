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

"""Formatting methods for stats."""

from greplin import scales

import html
import six
import json
import operator
import re

OPERATORS = {
  '>=': operator.ge,
  '>': operator.gt,
  '<': operator.lt,
  '<=': operator.le,
  '=': operator.eq,
  '==': operator.eq,
  '!=': operator.ne
}

OPERATOR = re.compile('(%s)' % '|'.join(list(OPERATORS.keys())))


def runQuery(statDict, query):
  """Filters for the given query."""
  parts = [x.strip() for x in OPERATOR.split(query)]
  assert len(parts) in (1, 3)
  queryKey = parts[0]

  result = {}
  for key, value in six.iteritems(statDict):
    if key == queryKey:
      if len(parts) == 3:
        op = OPERATORS[parts[1]]
        try:
          queryValue = type(value)(parts[2]) if value else parts[2]
        except (TypeError, ValueError):
          continue
        if not op(value, queryValue):
          continue
      result[key] = value
    elif isinstance(value, scales.StatContainer) or isinstance(value, dict):
      child = runQuery(value, query)
      if child:
        result[key] = child
  return result


def htmlHeader(output, path, serverName, query = None):
  """Writes an HTML header."""
  if path and path != '/':
    output.write('<title>%s - Status: %s</title>' % (serverName, path))
  else:
    output.write('<title>%s - Status</title>' % serverName)
  output.write('''
<style>
body,td { font-family: monospace }
.level div {
  padding-bottom: 4px;
}
.level .level {
  margin-left: 2em;
  padding: 1px 0;
}
span { color: #090; vertical-align: top }
.key { color: black; font-weight: bold }
.int, .float { color: #00c }
</style>
  ''')
  output.write('<h1 style="margin: 0">Stats</h1>')
  output.write('<h3 style="margin: 3px 0 18px">%s</h3>' % serverName)
  output.write(
      '<p><form action="#" method="GET">Filter: <input type="text" name="query" size="20" value="%s"></form></p>' %
      (query or ''))


def htmlFormat(output, pathParts = (), statDict = None, query = None):
  """Formats as HTML, writing to the given object."""
  statDict = statDict or scales.getStats()
  if query:
    statDict = runQuery(statDict, query)
  _htmlRenderDict(pathParts, statDict, output)


def _htmlRenderDict(pathParts, statDict, output):
  """Render a dictionary as a table - recursing as necessary."""
  keys = list(statDict.keys())
  keys.sort()

  links = []

  output.write('<div class="level">')
  for key in keys:
    keyStr = html.escape(_utf8str(key))
    value = statDict[key]
    if hasattr(value, '__call__'):
      value = value()
    if hasattr(value, 'keys'):
      valuePath = pathParts + (keyStr,)
      if isinstance(value, scales.StatContainer) and value.isCollapsed():
        link = '/status/' + '/'.join(valuePath)
        links.append('<div class="key"><a href="%s">%s</a></div>' % (link, keyStr))
      else:
        output.write('<div class="key">%s</div>' % keyStr)
        _htmlRenderDict(valuePath, value, output)
    else:
      output.write('<div><span class="key">%s</span> <span class="%s">%s</span></div>' %
                   (keyStr, type(value).__name__, html.escape(_utf8str(value)).replace('\n', '<br/>')))

  if links:
    for link in links:
      output.write(link)

  output.write('</div>')


def _utf8str(x):
  """Like str(x), but returns UTF8."""
  if six.PY3:
    return str(x)
  if isinstance(x, six.binary_type):
    return x
  elif isinstance(x, six.text_type):
    return x.encode('utf-8')
  else:
    return six.binary_type(x)


def jsonFormat(output, statDict = None, query = None, pretty = False):
  """Formats as JSON, writing to the given object."""
  statDict = statDict or scales.getStats()
  if query:
    statDict = runQuery(statDict, query)
  indent = 2 if pretty else None
  # At first, assume that strings are in UTF-8. If this fails -- if, for example, we have
  # crazy binary data -- then in order to get *something* out, we assume ISO-8859-1,
  # which maps each byte to a unicode code point.
  try:
    serialized = json.dumps(statDict, cls=scales.StatContainerEncoder, indent=indent)
  except UnicodeDecodeError:
    serialized = json.dumps(statDict, cls=scales.StatContainerEncoder, indent=indent, encoding='iso-8859-1')

  output.write(serialized)
  output.write('\n')
