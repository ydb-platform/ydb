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

try:
  basestring = basestring
except NameError: # py3
  basestring = str
  unicode = str


class dom1core(object):
  '''
  Implements the Document Object Model (Core) Level 1

  http://www.w3.org/TR/1998/REC-DOM-Level-1-19981001/
  http://www.w3.org/TR/1998/REC-DOM-Level-1-19981001/level-one-core.html
  '''
  @property
  def parentNode(self):
    '''
    DOM API: Returns the parent tag of the current element.
    '''
    return self.parent

  def getElementById(self, id):
    '''
    DOM API: Returns single element with matching id value.
    '''
    results = self.get(id=id)
    if len(results) > 1:
      raise ValueError('Multiple tags with id "%s".' % id)
    elif results:
      return results[0]
    return None

  def getElementsByTagName(self, name):
    '''
    DOM API: Returns all tags that match name.
    '''
    if isinstance(name, basestring):
      return self.get(name.lower())
    return None

  def appendChild(self, obj):
    '''
    DOM API: Add an item to the end of the children list.
    '''
    self.add(obj)
    return self
