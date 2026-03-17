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

# pylint: disable=bad-indentation, bad-whitespace, missing-docstring

import copy
import numbers
from collections import defaultdict, namedtuple
from functools import wraps
import threading

try:
  # Python 3
  from collections.abc import Callable
except ImportError: # pragma: no cover
  # Python 2.7
  from collections import Callable

try:
  basestring = basestring
except NameError: # py3 # pragma: no cover
  basestring = str
  unicode = str


try:
  import greenlet
except ImportError:
  greenlet = None


def _get_thread_context():
  context = [threading.current_thread()]
  if greenlet:
    context.append(greenlet.getcurrent())
  return hash(tuple(context))


class dom_tag(object):
  is_single = False  # Tag does not require matching end tag (ex. <hr/>)
  is_pretty = True   # Text inside the tag should be left as-is (ex. <pre>)
                     # otherwise, text will be escaped() and whitespace may be
                     # modified
  is_inline = False


  def __new__(_cls, *args, **kwargs):
    '''
    Check if bare tag is being used a a decorator
    (called with a single function arg).
    decorate the function and return
    '''
    if len(args) == 1 and isinstance(args[0], Callable) \
        and not isinstance(args[0], dom_tag) and not kwargs:
      wrapped = args[0]

      @wraps(wrapped)
      def f(*args, **kwargs):
        with _cls() as _tag:
          return wrapped(*args, **kwargs) or _tag
      return f
    return object.__new__(_cls)


  def __init__(self, *args, **kwargs):
    '''
    Creates a new tag. Child tags should be passed as arguments and attributes
    should be passed as keyword arguments.

    There is a non-rendering attribute which controls how the tag renders:

    * `__inline` - Boolean value. If True renders all children tags on the same
                   line.
    '''

    self.attributes = {}
    self.children   = []
    self.parent     = None

    # Does not insert newlines on all children if True (recursive attribute)
    self.is_inline = kwargs.pop('__inline', self.is_inline)
    self.is_pretty = kwargs.pop('__pretty', self.is_pretty)

    #Add child elements
    if args:
      self.add(*args)

    for attr, value in kwargs.items():
      self.set_attribute(*type(self).clean_pair(attr, value))

    self._ctx = None
    self._add_to_ctx()


  # context manager
  frame = namedtuple('frame', ['tag', 'items', 'used'])
  # stack of frames
  _with_contexts = defaultdict(list)

  def _add_to_ctx(self):
    stack = dom_tag._with_contexts.get(_get_thread_context())
    if stack:
      self._ctx = stack[-1]
      stack[-1].items.append(self)


  def __enter__(self):
    stack = dom_tag._with_contexts[_get_thread_context()]
    stack.append(dom_tag.frame(self, [], set()))
    return self


  def __exit__(self, type, value, traceback):
    thread_id = _get_thread_context()
    stack = dom_tag._with_contexts[thread_id]
    frame = stack.pop()
    for item in frame.items:
      if item in frame.used: continue
      self.add(item)
    if not stack:
      del dom_tag._with_contexts[thread_id]


  def __call__(self, func):
    '''
    tag instance is being used as a decorator.
    wrap func to make a copy of this tag
    '''
    # remove decorator from its context so it doesn't
    # get added in where it was defined
    if self._ctx:
      self._ctx.used.add(self)

    @wraps(func)
    def f(*args, **kwargs):
      tag = copy.deepcopy(self)
      tag._add_to_ctx()
      with tag:
        return func(*args, **kwargs) or tag
    return f


  def set_attribute(self, key, value):
    '''
    Add or update the value of an attribute.
    '''
    if isinstance(key, int):
      self.children[key] = value
    elif isinstance(key, basestring):
      self.attributes[key] = value
    else:
      raise TypeError('Only integer and string types are valid for assigning '
          'child tags and attributes, respectively.')
  __setitem__ = set_attribute


  def delete_attribute(self, key):
    if isinstance(key, int):
      del self.children[key:key+1]
    else:
      del self.attributes[key]
  __delitem__ = delete_attribute


  def add(self, *args):
    '''
    Add new child tags.
    '''
    for obj in args:
      if isinstance(obj, numbers.Number):
        # Convert to string so we fall into next if block
        obj = str(obj)

      if isinstance(obj, basestring):
        obj = util.escape(obj)
        self.children.append(obj)

      elif isinstance(obj, dom_tag):
        stack = dom_tag._with_contexts.get(_get_thread_context(), [])
        for s in stack:
          s.used.add(obj)
        self.children.append(obj)
        obj.parent = self

      elif isinstance(obj, dict):
        for attr, value in obj.items():
          self.set_attribute(*dom_tag.clean_pair(attr, value))

      elif hasattr(obj, '__iter__'):
        for subobj in obj:
          self.add(subobj)

      else:  # wtf is it?
        raise ValueError('%r not a tag or string.' % obj)

    if len(args) == 1:
      return args[0]

    return args


  def add_raw_string(self, s):
    self.children.append(s)


  def remove(self, obj):
    self.children.remove(obj)


  def clear(self):
    for i in self.children:
      if isinstance(i, dom_tag) and i.parent is self:
        i.parent = None
    self.children = []


  def get(self, tag=None, **kwargs):
    '''
    Recursively searches children for tags of a certain
    type with matching attributes.
    '''
    # Stupid workaround since we can not use dom_tag in the method declaration
    if tag is None: tag = dom_tag

    attrs = [(dom_tag.clean_attribute(attr), value)
        for attr, value in kwargs.items()]

    results = []
    for child in self.children:
      if (isinstance(tag, basestring) and type(child).__name__ == tag) or \
        (not isinstance(tag, basestring) and isinstance(child, tag)):

        if all(child.attributes.get(attribute) == value
            for attribute, value in attrs):
          # If the child is of correct type and has all attributes and values
          # in kwargs add as a result
          results.append(child)
      if isinstance(child, dom_tag):
        # If the child is a dom_tag extend the search down through its children
        results.extend(child.get(tag, **kwargs))
    return results


  def __getitem__(self, key):
    '''
    Returns the stored value of the specified attribute or child
    (if it exists).
    '''
    if isinstance(key, int):
      # Children are accessed using integers
      try:
        return object.__getattribute__(self, 'children')[key]
      except IndexError:
        raise IndexError('Child with index "%s" does not exist.' % key)
    elif isinstance(key, basestring):
      # Attributes are accessed using strings
      try:
        return object.__getattribute__(self, 'attributes')[key]
      except KeyError:
        raise AttributeError('Attribute "%s" does not exist.' % key)
    else:
      raise TypeError('Only integer and string types are valid for accessing '
          'child tags and attributes, respectively.')
  __getattr__ = __getitem__


  def __len__(self):
    '''
    Number of child elements.
    '''
    return len(self.children)


  def __bool__(self):
    '''
    Hack for "if x" and __len__
    '''
    return True
  __nonzero__ = __bool__


  def __iter__(self):
    '''
    Iterates over child elements.
    '''
    return self.children.__iter__()


  def __contains__(self, item):
    '''
    Checks recursively if item is in children tree.
    Accepts both a string and a class.
    '''
    return bool(self.get(item))


  def __iadd__(self, obj):
    '''
    Reflexive binary addition simply adds tag as a child.
    '''
    self.add(obj)
    return self

  # String and unicode representations are the same as render()
  def __unicode__(self):
    return self.render()
  __str__ = __unicode__


  def render(self, indent='  ', pretty=True, xhtml=False):
    data = self._render([], 0, indent, pretty, xhtml)
    return u''.join(data)


  def _render(self, sb, indent_level, indent_str, pretty, xhtml):
    pretty = pretty and self.is_pretty

    name = getattr(self, 'tagname', type(self).__name__)

    # Workaround for python keywords and standard classes/methods
    # (del, object, input)
    if name[-1] == '_':
      name = name[:-1]

    # open tag
    sb.append('<')
    sb.append(name)

    for attribute, value in sorted(self.attributes.items()):
      if value in (False, None):
        continue
      val = unicode(value) if isinstance(value, util.text) and not value.escape else util.escape(unicode(value), True)
      sb.append(' %s="%s"' % (attribute, val))

    sb.append(' />' if self.is_single and xhtml else '>')

    if self.is_single:
      return sb

    inline = self._render_children(sb, indent_level + 1, indent_str, pretty, xhtml)
    if pretty and not inline:
      sb.append('\n')
      sb.append(indent_str * indent_level)

    # close tag
    sb.append('</')
    sb.append(name)
    sb.append('>')

    return sb

  def _render_children(self, sb, indent_level, indent_str, pretty, xhtml):
    inline = True
    for child in self.children:
      if isinstance(child, dom_tag):
        if pretty and not child.is_inline:
          inline = False
          sb.append('\n')
          sb.append(indent_str * indent_level)
        child._render(sb, indent_level, indent_str, pretty, xhtml)
      else:
        sb.append(unicode(child))

    return inline


  def __repr__(self):
    name = '%s.%s' % (self.__module__, type(self).__name__)

    attributes_len = len(self.attributes)
    attributes = '%s attribute' % attributes_len
    if attributes_len != 1: attributes += 's'

    children_len = len(self.children)
    children = '%s child' % children_len
    if children_len != 1: children += 'ren'

    return '<%s at %x: %s, %s>' % (name, id(self), attributes, children)


  @staticmethod
  def clean_attribute(attribute):
    '''
    Normalize attribute names for shorthand and work arounds for limitations
    in Python's syntax
    '''

    # Shorthand
    attribute = {
      'cls': 'class',
      'className': 'class',
      'class_name': 'class',
      'klass': 'class',
      'fr': 'for',
      'html_for': 'for',
      'htmlFor': 'for',
      'phor': 'for',
    }.get(attribute, attribute)

    # Workaround for Python's reserved words
    if attribute[0] == '_':
      attribute = attribute[1:]

    # Workaround for dash
    special_prefix = any([attribute.startswith(x) for x in ('data_', 'aria_')])
    if attribute in set(['http_equiv']) or special_prefix:
      attribute = attribute.replace('_', '-').lower()

    # Workaround for colon
    if attribute.split('_')[0] in ('xlink', 'xml', 'xmlns'):
      attribute = attribute.replace('_', ':', 1).lower()

    return attribute


  @classmethod
  def clean_pair(cls, attribute, value):
    '''
    This will call `clean_attribute` on the attribute and also allows for the
    creation of boolean attributes.

    Ex. input(selected=True) is equivalent to input(selected="selected")
    '''
    attribute = cls.clean_attribute(attribute)

    # Check for boolean attributes
    # (i.e. selected=True becomes selected="selected")
    if value is True:
      value = attribute

    # Ignore `if value is False`: this is filtered out in render()

    return (attribute, value)


_get_current_none = object()
def get_current(default=_get_current_none):
  '''
  get the current tag being used as a with context or decorated function.
  if no context is active, raises ValueError, or returns the default, if provided
  '''
  h = _get_thread_context()
  ctx = dom_tag._with_contexts.get(h, None)
  if ctx:
    return ctx[-1].tag
  if default is _get_current_none:
    raise ValueError('no current context')
  return default


def attr(*args, **kwargs):
  '''
  Set attributes on the current active tag context
  '''
  c = get_current()
  dicts = args + (kwargs,)
  for d in dicts:
    for attr, value in d.items():
      c.set_attribute(*dom_tag.clean_pair(attr, value))


from . import util
