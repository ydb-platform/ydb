#######################################################################
# namedlist is similar to collections.namedtuple, but supports default
#  values and is writable.  Also contains an implementation of
#  namedtuple, which is the same as collections.namedtuple supporting
#  defaults.
#
# Copyright 2011-2020 True Blade Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notes:
#  See http://code.activestate.com/recipes/576555/ for a similar
# concept.
#
########################################################################

__all__ = ['namedlist', 'namedtuple', 'NO_DEFAULT', 'FACTORY']

# All of this hassle with ast is solely to provide a decent __init__
#  function, that takes all of the right arguments and defaults. But
#  it's worth it to get all of the normal python error messages.
# For other functions, like __repr__, we don't bother. __init__ is
#  the only function where we really need the argument processing,
#  because __init__ is the only function whose signature will vary
#  per class.

import ast as _ast
import sys as _sys
import copy as _copy
import operator as _operator
import itertools as _itertools
from keyword import iskeyword as _iskeyword
import collections as _collections
import abc as _abc

try:
    import collections.abc as _collections_abc
except ImportError:
    import collections as _collections_abc

_PY2 = _sys.version_info[0] == 2
_PY3 = _sys.version_info[0] == 3
_PY38_or_higher = _PY3 and _sys.version_info.minor >= 8

try:
    _OrderedDict = _collections.OrderedDict
except AttributeError:
    _OrderedDict = None


if _PY2:
    _basestring = basestring
    _iteritems = lambda d, **kw: iter(d.iteritems(**kw))
else:
    _basestring = str
    _iteritems = lambda d, **kw: iter(d.items(**kw))


NO_DEFAULT = object()

# Wrapper around a callable. Used to specify a factory function instead
#  of a plain default value.
class FACTORY(object):
    def __init__(self, callable):
        self._callable = callable

    def __repr__(self):
        return 'FACTORY({0!r})'.format(self._callable)


########################################################################
# Keep track of fields, both with and without defaults.
class _Fields(object):
    default_not_specified = object()

    def __init__(self, default):
        self.default = default
        self.with_defaults = []        # List of (field_name, default).
        self.without_defaults = []     # List of field_name.

    def add(self, field_name, default):
        if default is self.default_not_specified:
            if self.default is NO_DEFAULT:
                # No default. There can't be any defaults already specified.
                if len(self.with_defaults) != 0:
                    raise ValueError('field {0} without a default follows fields '
                                     'with defaults'.format(field_name))
                self.without_defaults.append(field_name)
            else:
                self.add(field_name, self.default)
        else:
            if default is NO_DEFAULT:
                self.add(field_name, self.default_not_specified)
            else:
                self.with_defaults.append((field_name, default))


########################################################################
# Validate and possibly sanitize the field and type names.
class _NameChecker(object):
    def __init__(self, typename):
        self.seen_fields = set()
        self._check_common(typename, 'Type')

    def check_field_name(self, fieldname, rename, idx):
        try:
            self._check_common(fieldname, 'Field')
            self._check_specific_to_fields(fieldname)
        except ValueError as ex:
            if rename:
                return '_' + str(idx)
            else:
                raise

        self.seen_fields.add(fieldname)
        return fieldname

    def _check_common(self, name, type_of_name):
        # tests that are common to both field names and the type name
        if len(name) == 0:
            raise ValueError('{0} names cannot be zero '
                             'length: {1!r}'.format(type_of_name, name))
        if _PY2:
            if not all(c.isalnum() or c=='_' for c in name):
                raise ValueError('{0} names can only contain '
                                 'alphanumeric characters and underscores: '
                                 '{1!r}'.format(type_of_name, name))
            if name[0].isdigit():
                raise ValueError('{0} names cannot start with a '
                                 'number: {1!r}'.format(type_of_name, name))
        else:
            if not name.isidentifier():
                raise ValueError('{0} names names must be valid '
                                 'identifiers: {1!r}'.format(type_of_name, name))
        if _iskeyword(name):
            raise ValueError('{0} names cannot be a keyword: '
                             '{1!r}'.format(type_of_name, name))

    def _check_specific_to_fields(self, name):
        # these tests don't apply for the typename, just the fieldnames
        if name in self.seen_fields:
            raise ValueError('Encountered duplicate field name: '
                             '{0!r}'.format(name))

        if name.startswith('_'):
            raise ValueError('Field names cannot start with an underscore: '
                             '{0!r}'.format(name))


########################################################################
# Returns a function with name 'name', that calls another function 'chain_fn'
# This is used to create the __init__ function with the right argument names and defaults, that
#  calls into _init to do the real work.
# The new function takes args as arguments, with defaults as given.
def _make_fn(name, chain_fn, args, defaults):
    args_with_self = ['_self'] + list(args)
    arguments = [_ast.Name(id=arg, ctx=_ast.Load()) for arg in args_with_self]
    defs = [_ast.Name(id='_def{0}'.format(idx), ctx=_ast.Load()) for idx, _ in enumerate(defaults)]
    if _PY2:
        parameters = _ast.arguments(args=[_ast.Name(id=arg, ctx=_ast.Param()) for arg in args_with_self],
                                    defaults=defs)
    else:
        if _PY38_or_higher:
            parameters = _ast.arguments(args=[_ast.arg(arg=arg) for arg in args_with_self],
                                        posonlyargs=[],
                                        kwonlyargs=[],
                                        defaults=defs,
                                        kw_defaults=[])
        else:
            parameters = _ast.arguments(args=[_ast.arg(arg=arg) for arg in args_with_self],
                                        kwonlyargs=[],
                                        defaults=defs,
                                        kw_defaults=[])

    if _PY38_or_higher:
        module_node = _ast.Module(body=[_ast.FunctionDef(name=name,
                                                         args=parameters,
                                                         body=[_ast.Return(value=_ast.Call(func=_ast.Name(id='_chain', ctx=_ast.Load()),
                                                                                           args=arguments,
                                                                                           keywords=[]))],
                                                         decorator_list=[])],
                                  type_ignores=[])
    else:
        module_node = _ast.Module(body=[_ast.FunctionDef(name=name,
                                                         args=parameters,
                                                         body=[_ast.Return(value=_ast.Call(func=_ast.Name(id='_chain', ctx=_ast.Load()),
                                                                                           args=arguments,
                                                                                           keywords=[]))],
                                                         decorator_list=[])])

    module_node = _ast.fix_missing_locations(module_node)

    # compile the ast
    code = compile(module_node, '<string>', 'exec')

    # and eval it in the right context
    globals_ = {'_chain': chain_fn}
    locals_ = dict(('_def{0}'.format(idx), value) for idx, value in enumerate(defaults))
    eval(code, globals_, locals_)

    # extract our function from the newly created module
    return locals_[name]


########################################################################
# Produce a docstring for the class.

def _field_name_with_default(name, default):
    if default is NO_DEFAULT:
        return name
    return '{0}={1!r}'.format(name, default)

def _build_docstring(typename, fields, defaults):
    # We can use NO_DEFAULT as a sentinel here, becuase it will never be
    #  present in defaults. By this point, it has been removed and replaced
    #  with actual default values.

    # The defaults make this a little tricky. Append a sentinel in
    #  front of defaults until it's the same length as fields. The
    #  sentinel value is used in _name_with_default
    defaults = [NO_DEFAULT] * (len(fields) - len(defaults)) + defaults
    return '{0}({1})'.format(typename, ', '.join(_field_name_with_default(name, default)
                                                 for name, default in zip(fields, defaults)))


########################################################################
# Given the typename, fields_names, default, and the rename flag,
#  return a tuple of fields and a list of defaults.
def _fields_and_defaults(typename, field_names, default, rename):
    # field_names must be a string or an iterable, consisting of fieldname
    #  strings or 2-tuples. Each 2-tuple is of the form (fieldname,
    #  default).

    # Keeps track of the fields we're adding, with their defaults.
    fields = _Fields(default)

    # Validates field and type names.
    name_checker = _NameChecker(typename)

    if isinstance(field_names, _basestring):
        # No per-field defaults. So it's like a collections.namedtuple,
        #  but with a possible default value.
        field_names = field_names.replace(',', ' ').split()

    # If field_names is a Mapping, change it to return the
    #  (field_name, default) pairs, as if it were a list.
    if isinstance(field_names, _collections_abc.Mapping):
        field_names = field_names.items()

    # Parse and validate the field names.

    # field_names is now an iterable. Walk through it,
    # sanitizing as needed, and add to fields.

    for idx, field_name in enumerate(field_names):
        if isinstance(field_name, _basestring):
            default = fields.default_not_specified
        else:
            try:
                if len(field_name) != 2:
                    raise ValueError('field_name must be a 2-tuple: '
                                     '{0!r}'.format(field_name))
            except TypeError:
                # field_name doesn't have a __len__.
                raise ValueError('field_name must be a 2-tuple: '
                                 '{0!r}'.format(field_name))
            default = field_name[1]
            field_name = field_name[0]

        # Okay: now we have the field_name and the default value (if any).
        # Validate the name, and add the field.
        # Convert field_name to str for python 2.x
        fields.add(name_checker.check_field_name(str(field_name), rename, idx), default)

    return (tuple(fields.without_defaults + [name for name, default in
                                             fields.with_defaults]),
            [default for _, default in fields.with_defaults])

########################################################################
# Common member functions for the generated classes.

def _repr(self):
    return '{0}({1})'.format(self.__class__.__name__, ', '.join('{0}={1!r}'.format(name, getattr(self, name)) for name in self._fields))

def _asdict(self):
    # In 2.6, return a dict.
    # Otherwise, return an OrderedDict
    t = _OrderedDict if _OrderedDict is not None else dict
    return t(zip(self._fields, self))

# Set up methods and fields shared by namedlist and namedtuple
def _common_fields(fields, docstr):
    type_dict = {'__repr__': _repr,
                 '__dict__': property(_asdict),
                 '__doc__': docstr,
                 '_asdict': _asdict,
                 '_fields': fields}

    # See collections.namedtuple for a description of
    #  what's happening here.
    # _getframe(2) instead of 1, because we're now inside
    #  another function.
    try:
        type_dict['__module__'] = _sys._getframe(2).f_globals.get('__name__', '__main__')
    except (AttributeError, ValueError):
        pass
    return type_dict


########################################################################
# namedlist methods

# The function that __init__ calls to do the actual work.
def _nl_init(self, *args):
    # sets all of the fields to their passed in values
    for fieldname, value in _get_values(self._fields, args):
        setattr(self, fieldname, value)

def _nl_eq(self, other):
    return isinstance(other, self.__class__) and all(getattr(self, name) == getattr(other, name) for name in self._fields)

def _nl_ne(self, other):
    return not _nl_eq(self, other)

def _nl_len(self):
    return len(self._fields)

def _nl_getstate(self):
    return tuple(getattr(self, fieldname) for fieldname in self._fields)

def _nl_setstate(self, state):
    for fieldname, value in zip(self._fields, state):
        setattr(self, fieldname, value)

def _nl_getitem(self, idx):
    if isinstance(idx, slice):
        # this isn't super-efficient, but works
        return [getattr(self, self._fields[i]) for i in range(*idx.indices(len(self._fields)))]

    return getattr(self, self._fields[idx])

def _nl_setitem(self, idx, value):
    return setattr(self, self._fields[idx], value)

def _nl_iter(self):
    return (getattr(self, fieldname) for fieldname in self._fields)

def _nl_count(self, value):
    return sum(1 for v in iter(self) if v == value)

def _nl_index(self, value, start=NO_DEFAULT, stop=NO_DEFAULT):
    # not the most efficient way to implement this, but it will work
    l = list(self)
    if start is NO_DEFAULT and stop is NO_DEFAULT:
        return l.index(value)
    if stop is NO_DEFAULT:
        return l.index(value, start)
    return l.index(value, start, stop)

def _nl_update(_self, _other=None, **kwds):
    if isinstance(_other, type(_self)):
        _other = zip(_self._fields, _other)
    elif isinstance(_other, _collections_abc.Mapping):
        tmp = []
        for field_name in _self._fields:
            try:
                other_value = _other[field_name]
            except KeyError:
                pass
            else:
                tmp.append((field_name, other_value))
        _other = tmp
    elif _other is None:
        _other = []

    chained = _itertools.chain(_other, (x for x in _iteritems(kwds)
                                        if x[0] in _self._fields))
    for key, value in chained:
        setattr(_self, key, value)

def _nl_replace(_self, **kwds):
    # make a shallow copy, then mutate it
    other = _copy.copy(_self)
    for key, value in _iteritems(kwds):
        setattr(other, key, value)
    return other

########################################################################
# The actual namedlist factory function.
def namedlist(typename, field_names, default=NO_DEFAULT, rename=False,
              use_slots=True):
    typename = str(typename) # for python 2.x
    fields, defaults = _fields_and_defaults(typename, field_names, default, rename)

    type_dict = {'__init__': _make_fn('__init__', _nl_init, fields, defaults),
                 '__eq__': _nl_eq,
                 '__ne__': _nl_ne,
                 '__len__': _nl_len,
                 '__getstate__': _nl_getstate,
                 '__setstate__': _nl_setstate,
                 '__getitem__': _nl_getitem,
                 '__setitem__': _nl_setitem,
                 '__iter__': _nl_iter,
                 '__hash__': None,
                 'count': _nl_count,
                 'index': _nl_index,
                 '_update': _nl_update,
                 '_replace': _nl_replace}
    type_dict.update(_common_fields(fields, _build_docstring(typename, fields, defaults)))

    if use_slots:
        type_dict['__slots__'] = fields

    # Create the new type object.
    t = type(typename, (object,), type_dict)

    # Register its ABC's
    _collections_abc.Sequence.register(t)

    # And return it.
    return t


########################################################################
# namedtuple methods
def _nt_new(cls, *args):
    # sets all of the fields to their passed in values
    assert len(args) == len(cls._fields)
    values = [value for _, value in _get_values(cls._fields, args)]
    return tuple.__new__(cls, values)

def _nt_replace(_self, **kwds):
    result = _self._make(map(kwds.pop, _self._fields, _self))
    if kwds:
        raise ValueError('Got unexpected field names: %r' % list(kwds))
    return result

def _nt_make(cls, iterable, new=tuple.__new__):
    result = new(cls, iterable)
    if len(result) != len(cls._fields):
        raise TypeError('Expected {0} arguments, got {1}'.format(len(cls._fields), len(result)))
    return result

def _nt_getnewargs(self):
    'Return self as a plain tuple.  Used by copy and pickle.'
    return tuple(self)

def _nt_getstate(self):
    'Exclude the OrderedDict from pickling'
    return None

def _get_values(fields, args):
    # Returns [(fieldname, value)]. If the value is a FACTORY, call it.
    assert len(fields) == len(args)
    return [(fieldname, (value._callable() if isinstance(value, FACTORY) else value))
            for fieldname, value in zip(fields, args)]

########################################################################
# The actual namedtuple factory function.
def namedtuple(typename, field_names, default=NO_DEFAULT, rename=False):
    typename = str(typename) # for python 2.x
    fields, defaults = _fields_and_defaults(typename, field_names, default, rename)

    type_dict = {'__new__': _make_fn('__new__', _nt_new, fields, defaults),
                 '__getnewargs__': _nt_getnewargs,
                 '__getstate__': _nt_getstate,
                 '_replace': _nt_replace,
                 '_make': classmethod(_nt_make),
                 '__slots__': ()}
    type_dict.update(_common_fields(fields, _build_docstring(typename, fields, defaults)))

    # Create each field property.
    for idx, field in enumerate(fields):
        type_dict[field] = property(_operator.itemgetter(idx),
                                    doc='Alias for field number {0}'.format(idx))

    # Create the new type object.
    return type(typename, (tuple,), type_dict)
