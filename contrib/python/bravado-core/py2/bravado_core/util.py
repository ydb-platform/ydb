# -*- coding: utf-8 -*-
import copy
import inspect
import re

import typing
from enum import Enum
from six import iteritems
from six import iterkeys

from bravado_core._compat import get_function_spec
from bravado_core._compat import wraps
from bravado_core.schema import is_dict_like
from bravado_core.schema import is_list_like

if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core._compat_typing import FuncType
    from bravado_core._compat_typing import JSONDict

    CacheKey = typing.Tuple[typing.Tuple[typing.Text, int], ...]
    T = typing.TypeVar('T')


SANITIZE_RULES = [
    (re.compile(regex), replacement)
    for regex, replacement in [
        ('[^A-Za-z0-9_]', '_'),  # valid chars for method names
        ('__+', '_'),  # collapse consecutive _
        ('^[0-9_]+|_+$', ''),  # trim leading/trailing _ and leading digits
    ]
]


FALLBACK_SANITIZE_RULES = [
    (re.compile(regex), replacement)
    for regex, replacement in [
        ('[^A-Za-z0-9_]', '_'),  # valid chars for method names
        ('^[0-9]+', '_'),  # replace leading digits
        ('__+', '_'),  # collapse consecutive _
    ]
]


class cached_property(object):
    """
    A property that is only computed once per instance and then replaces
    itself with an ordinary attribute. Deleting the attribute resets the
    property.

    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """

    def __init__(self, func):
        # type: (FuncType) -> None
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        # type: (typing.Any, typing.Any) -> typing.Any
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


class lazy_class_attribute(object):
    """
    An attribute that is computed once per class.

    WARNING: once the attribute has been evaluated is not possible to modify it
    """

    def __init__(self, func):
        # type: (FuncType) -> None
        self.__doc__ = getattr(func, '__doc__')
        self.func = func
        super(lazy_class_attribute, self).__init__()

    def __get__(self, obj, cls):
        # type: (T, typing.Type[T]) -> typing.Any
        value = self.func(cls)
        setattr(cls, self.func.__name__, value)
        return value


class RecursiveCallException(Exception):
    pass


def memoize_by_id(func):
    # type: (FuncType) -> FuncType
    cache = func.cache = {}  # type: ignore  # It's not worth to modify the signature to include handling of cache attribute  # noqa: E501
    key_in_progress_set = set()  # type: typing.Set[CacheKey]
    _CACHE_MISS = object()

    spec = get_function_spec(func)
    default_mapping = dict(zip(reversed(spec.args), reversed(spec.defaults or [])))

    def make_key(*args, **kwargs):
        # type: (typing.Any, typing.Any) -> CacheKey
        """
        Create a cache key starting from *args and **kwargs.
        The cache key is an ordered tuple containing parameter name and related id as in (name, id(value)).
        """
        if args:
            param_name_to_value_mapping = sorted(iteritems(inspect.getcallargs(func, *args, **kwargs)))
        else:
            # Use the default values while determining the parameter name to value to be used for the cache key
            param_name_to_value_mapping = sorted(iteritems(dict(default_mapping, **kwargs)))

        return tuple(
            (param_name, id(param_value))
            for param_name, param_value in param_name_to_value_mapping
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        # type: (typing.Any, typing.Any) -> typing.Any
        cache_key = make_key(*args, **kwargs)
        cached_value = cache.get(cache_key, _CACHE_MISS)
        if cached_value is _CACHE_MISS:
            if cache_key in key_in_progress_set:
                raise RecursiveCallException()
            key_in_progress_set.add(cache_key)
            cached_value = func(*args, **kwargs)
            key_in_progress_set.remove(cache_key)
            cache[cache_key] = cached_value
        return cached_value
    return wrapper  # type: ignore  # ignoring type to avoiding typing.cast call


def sanitize_name(name):
    # type: (typing.Text) -> typing.Text
    """Convert a given name so that it is a valid python identifier."""
    if name == '':
        return name

    sanitized_name = name
    for regex, replacement in SANITIZE_RULES:
        sanitized_name = regex.sub(replacement, sanitized_name)

    if sanitized_name == '':  # use fallback rules with more underscores
        sanitized_name = '_' + name  # prepend _ so digits are not stripped
        for regex, replacement in FALLBACK_SANITIZE_RULES:
            sanitized_name = regex.sub(replacement, sanitized_name)

    return sanitized_name


class AliasKeyDict(dict):
    """Dictionary class that allows you to set additional key names for existing keys. Retrieving
    values using these aliased keys works, but when iterating over the dictionary, only the main
    keys are returned."""

    def __init__(self, *args, **kwargs):
        # type: (typing.Any, typing.Any) -> None
        super(AliasKeyDict, self).__init__(*args, **kwargs)
        self.alias_to_key = {}  # type: typing.Dict[typing.Text, typing.Any]

    def add_alias(self, alias, key):
        # type: (typing.Text, typing.Text) -> None
        if alias != key:
            self.alias_to_key[alias] = key

    def determine_key(self, key):
        # type: (typing.Any) -> typing.Any
        if key in self.alias_to_key:  # this will normally be False, optimize for it
            key = self.alias_to_key[key]
        return key

    def get(self, key, default=None):
        # type: (typing.Text, typing.Any) -> typing.Any
        return super(AliasKeyDict, self).get(self.determine_key(key), default)

    def pop(self, key, default=None):
        # type: (typing.Text, typing.Any) -> typing.Any
        return super(AliasKeyDict, self).pop(self.determine_key(key), default)

    def __getitem__(self, key):
        # type: (typing.Text) -> typing.Any
        return super(AliasKeyDict, self).__getitem__(self.determine_key(key))

    def __delitem__(self, key):
        # type: (typing.Text) -> None
        final_key = self.alias_to_key.get(key, key)
        if final_key != key:
            del self.alias_to_key[key]
        return super(AliasKeyDict, self).__delitem__(final_key)

    def __contains__(self, key):
        # type: (typing.Any) -> bool
        return super(AliasKeyDict, self).__contains__(self.determine_key(key))

    def copy(self):
        # type: () -> 'AliasKeyDict'
        copied_dict = type(self)(self)
        copied_dict.alias_to_key = self.alias_to_key.copy()
        return copied_dict


class ObjectType(Enum):
    """
    Container of object types.
    NOTE: the value of the enum is not supposed to be used outside of this library
    """
    UNKNOWN = None
    SCHEMA = 'definitions'
    PARAMETER = 'parameters'
    RESPONSE = 'responses'
    PATH_ITEM = None

    def get_root_holder(self):
        # type: () -> typing.Optional[typing.Text]
        return self.value


def determine_object_type(object_dict, default_type_to_object=None):
    # type: (typing.Any, typing.Optional[bool]) -> ObjectType
    """
    Use best guess to determine the object type based on the object keys.

    NOTE: it assumes that the base swagger specs are validated and perform type detection for
    the four types of object that could be references in the specs: parameter, path item, response and schema.

    :type object_dict: dict
    :default_type_to_object: Default object type attribute to object if missing (as from bravado_core.spec.Spec config)
    :type default_type_to_object: bool

    :return: determined type of ``object_dict``. The return values is an ObjectType
    :rtype: ObjectType
    """

    if not is_dict_like(object_dict):
        return ObjectType.UNKNOWN

    if 'in' in object_dict and 'name' in object_dict:
        # A parameter object is the only object type that could contain 'in' and 'name' at the same time
        return ObjectType.PARAMETER
    else:
        http_operations = {'get', 'put', 'post', 'delete', 'options', 'head', 'patch'}
        # A path item object MUST have defined at least one http operation and could optionally have 'parameter'
        # attribute. NOTE: patterned fields (``^x-``) are acceptable in path item objects
        object_keys = {key for key in iterkeys(object_dict) if not key.startswith('x-')}
        if object_keys.intersection(http_operations):
            remaining_keys = object_keys.difference(http_operations)
            if not remaining_keys or remaining_keys == {'parameters'}:
                return ObjectType.PATH_ITEM
        else:
            # A response object has:
            #  - mandatory description field
            #  - optional schema, headers and examples field
            #  - no other fields are allowed
            response_allowed_keys = {'description', 'schema', 'headers', 'examples'}

            # If description field is specified and there are no other fields other the allowed response fields
            if 'description' in object_keys and not bool(object_keys - response_allowed_keys):
                return ObjectType.RESPONSE
            else:
                # A schema object has:
                #  - no mandatory parameters
                #  - long list of optional parameters (ie. description, type, items, properties, discriminator, etc.)
                #  - no other fields are allowed
                # NOTE: In case the method is mis-determining the type of a schema object, confusing it with a
                #       response type it will be enough to add, to the object, one key that is not defined
                #       in ``response_allowed_keys``.  (ie. ``additionalProperties: {}``, implicitly defined be specs)
                if default_type_to_object or 'type' in object_dict:
                    return ObjectType.SCHEMA
    return ObjectType.UNKNOWN


def strip_xscope(spec_dict):
    # type: (JSONDict) -> typing.Mapping[typing.Text, typing.Any]
    """
    :param spec_dict: Swagger spec in dict form. This is treated as read-only.
    :return: deep copy of spec_dict with the x-scope metadata stripped out.
    """
    result = copy.deepcopy(spec_dict)

    def descend(fragment):
        # type: (typing.Any) -> None
        if is_dict_like(fragment):
            fragment.pop('x-scope', None)  # Removes 'x-scope' key if present
            for key in iterkeys(fragment):
                descend(fragment[key])
        elif is_list_like(fragment):
            for element in fragment:
                descend(element)

    descend(result)
    return result
