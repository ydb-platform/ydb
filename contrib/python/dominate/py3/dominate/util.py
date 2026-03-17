'''
Utility classes for creating dynamic html documents
'''

__license__ = '''
This file is part of Dominate.

Dominate is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

Dominate is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General
Public License along with Dominate.  If not, see
<http://www.gnu.org/licenses/>.
'''

import re

from .dom_tag import dom_tag

try:
  basestring = basestring
except NameError:
  basestring = str
  unichr = chr


def include(f):
  '''
  includes the contents of a file on disk.
  takes a filename
  '''
  fl = open(f, 'r')
  data = fl.read()
  fl.close()
  return raw(data)


def system(cmd, data=None):
  '''
  pipes the output of a program
  '''
  import subprocess
  s = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
  out, err = s.communicate(data)
  return out.decode('utf8')


def escape(data, quote=True):  # stolen from std lib cgi
  '''
  Escapes special characters into their html entities
  Replace special characters "&", "<" and ">" to HTML-safe sequences.
  If the optional flag quote is true, the quotation mark character (")
  is also translated.

  This is used to escape content that appears in the body of an HTML document
  '''
  data = data.replace("&", "&amp;")  # Must be done first!
  data = data.replace("<", "&lt;")
  data = data.replace(">", "&gt;")
  if quote:
    data = data.replace('"', "&quot;")
  return data


_unescape = {
  'quot': 34,
  'amp':  38,
  'lt':   60,
  'gt':   62,
  'nbsp': 32,
  # more here
  # http://www.w3.org/TR/html4/sgml/entities.html
  'yuml': 255,
}
str_escape = escape


def unescape(data):
  '''
  unescapes html entities. the opposite of escape.
  '''
  cc = re.compile(r'&(?:(?:#(\d+))|([^;]+));')

  result = []
  m = cc.search(data)
  while m:
    result.append(data[0:m.start()])
    d = m.group(1)
    if d:
      d = int(d)
      result.append(unichr(d))
    else:
      d = _unescape.get(m.group(2), ord('?'))
      result.append(unichr(d))

    data = data[m.end():]
    m = cc.search(data)

  result.append(data)
  return ''.join(result)


_reserved = ";/?:@&=+$, "
_replace_map = dict((c, '%%%2X' % ord(c)) for c in _reserved)


def url_escape(data):
  return ''.join(_replace_map.get(c, c) for c in data)


def url_unescape(data):
  return re.sub('%([0-9a-fA-F]{2})',
    lambda m: unichr(int(m.group(1), 16)), data)


class container(dom_tag):
  '''
  Contains multiple elements, but does not add a level
  '''
  is_inline = True
  def _render(self, sb, indent_level, indent_str, pretty, xhtml):
    inline = self._render_children(sb, indent_level, indent_str, pretty, xhtml)
    if pretty and not inline:
      sb.append('\n')
      sb.append(indent_str * (indent_level - 1))
    return sb


class lazy(dom_tag):
  '''
  delays function execution until rendered
  '''
  def __new__(_cls, *args, **kwargs):
    '''
    Need to reset this special method or else
    dom_tag will think it's being used as a dectorator.

    This means lazy() can't be used as a dectorator, but
    thinking about when you might want that just confuses me.
    '''
    return object.__new__(_cls)

  def __init__(self, func, *args, **kwargs):
    super(lazy, self).__init__()
    self.func   = func
    self.args   = args
    self.kwargs = kwargs


  def _render(self, sb, *a, **kw):
    r = self.func(*self.args, **self.kwargs)
    sb.append(str(r))


class text(dom_tag):
  '''
  Just a string. Useful for inside context managers
  '''
  is_pretty = False
  is_inline = True

  def __init__(self, _text, escape=True):
    super(text, self).__init__()
    self.escape = escape
    if escape:
      self.text = str_escape(_text)
    else:
      self.text = _text

  def _render(self, sb, *a, **kw):
    sb.append(self.text)
    return sb


def raw(s):
  '''
  Inserts a raw string into the DOM. Unsafe. Alias for text(x, escape=False)
  '''
  return text(s, escape=False)
