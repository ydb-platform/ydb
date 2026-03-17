# This file is part of Dictdiffer.
#
# Copyright (C) 2015 CERN.
# Copyright (C) 2017, 2019 ETH Zurich, Swiss Data Science Center, Jiri Kuncar.
#
# Dictdiffer is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more
# details.

"""Utils gathers helper functions, classes for the dictdiffer module."""

import math
import sys
from itertools import zip_longest

num_types = int, float
EPSILON = sys.float_info.epsilon


class WildcardDict(dict):
    """Provide possibility to use special wildcard keys to access values.

    Those wildcards are:
        *:  wildcard for everything that follows
        +:  wildcard for anything on the same path level
    The intended use case of this are dictionaries, that utilize tuples as
    keys.

        >>> from dictdiffer.utils import WildcardDict
        >>> w = WildcardDict({('foo', '*'): '* card',
        ...                   ('banana', '+'): '+ card'})
        >>> w[ ('foo', 'bar', 'baz') ]
        '* card'
        >>> w[ ('banana', 'apple') ]
        '+ card'
    """

    def __init__(self, values=None):
        """Set lookup key indices.

        :param values: a dictionary
        """
        super(WildcardDict, self).__init__()
        self.star_keys = set()
        self.plus_keys = set()

        if values is not None:
            for key, value in values.items():
                self.__setitem__(key, value)

    def __getitem__(self, key):
        """Return the value corresponding to the key, regarding wildcards.

        If the key doesn't exit it tries the '+' wildcard and then the
        '*' wildcard.

            >>> w = WildcardDict({('foo', '*'): '* card',
            ...                   ('banana', '+'): '+ card'})
            >>> w[ ('foo', 'bar') ]
            '* card'
            >>> w[ ('foo', 'bar', 'baz') ]
            '* card'
            >>> w[ ('banana', 'apple') ]
            '+ card'
            >>> w[ ('banana', 'apple', 'mango') ]
            Traceback (most recent call last):
                ...
            KeyError
        """
        try:
            return super(WildcardDict, self).__getitem__(key)
        except KeyError:
            if key[:-1] in self.plus_keys:
                return super(WildcardDict, self).__getitem__(key[:-1]+('+',))
            for _key in [key[:-i] for i in range(1, len(key)+1)]:
                if _key in self.star_keys:
                    return super(WildcardDict, self).__getitem__(_key+('*',))
            raise KeyError

    def __setitem__(self, key, value):
        """Set the item for a given key (path)."""
        super(WildcardDict, self).__setitem__(key, value)

        if key[-1] == '+':
            self.plus_keys.add(key[:-1])
        if key[-1] == '*':
            self.star_keys.add(key[:-1])

    def query_path(self, key):
        """Return the key (path) that matches the queried key.

        >>> w = WildcardDict({('foo', '*'): 'banana'})
        >>> w.query_path(('foo', 'bar', 'baz'))
        ('foo', '*')
        """
        if key in self:
            return key
        if key[:-1] in self.plus_keys:
            return key[:-1]+('+',)
        for _key in [key[:-i] for i in range(1, len(key)+1)]:
            if _key in self.star_keys:
                return _key+('*',)

        raise KeyError


class PathLimit(object):
    """Class to limit recursion depth during the dictdiffer.diff execution."""

    def __init__(self, path_limits=[], final_key=None):
        """Initialize a dictionary structure to determine a path limit.

        :param path_limits: list of keys (tuples) determining the path limits
        :param final_key: the key used in the dictionary to determin if the
                          path is final

            >>> pl = PathLimit( [('foo', 'bar')] , final_key='!@#$%FINAL')
            >>> pl.dict
            {'foo': {'bar': {'!@#$%FINAL': True}}}
        """
        self.final_key = final_key if final_key else '!@#$FINAL'
        self.dict = {}
        for key_path in path_limits:
            containing = self.dict
            for key in key_path:
                try:
                    containing = containing[key]
                except KeyError:
                    containing[key] = {}
                    containing = containing[key]

            containing[self.final_key] = True

    def path_is_limit(self, key_path):
        """Query the PathLimit object if the given key_path is a limit.

        >>> pl = PathLimit( [('foo', 'bar')] , final_key='!@#$%FINAL')
        >>> pl.path_is_limit( ('foo', 'bar') )
        True
        """
        containing = self.dict
        for key in key_path:
            try:
                containing = containing[key]
            except KeyError:
                try:
                    containing = containing['*']
                except KeyError:
                    return False

        return containing.get(self.final_key, False)


def create_dotted_node(node):
    """Create the *dotted node* notation for the dictdiffer.diff patches.

    >>> create_dotted_node( ['foo', 'bar', 'baz'] )
    'foo.bar.baz'
    """
    if all(map(lambda x: isinstance(x, str), node)):
        return '.'.join(node)
    else:
        return list(node)


def get_path(patch):
    """Return the path for a given dictdiffer.diff patch."""
    if patch[1] != '':
        keys = (patch[1].split('.') if isinstance(patch[1], str)
                else patch[1])
    else:
        keys = []
    keys = keys + [patch[2][0][0]] if patch[0] != 'change' else keys
    return tuple(keys)


def is_super_path(path1, path2):
    """Check if one path is the super path of the other.

    Super path means, that the n values in tuple are equal to the first n of m
    vales in tuple b.

        >>> is_super_path( ('foo', 'bar'), ('foo', 'bar') )
        True

        >>> is_super_path( ('foo', 'bar'), ('foo', 'bar', 'baz') )
        True

        >>> is_super_path( ('foo', 'bar'), ('foo', 'apple', 'banana') )
        False
    """
    return all(map(lambda x: x[0] == x[1] or x[0] is None,
                   zip_longest(path1, path2)))


def nested_hash(obj):
    """Create a hash of nested, mutable data structures.

    It shall be noted, that the uniqeness of those hashes in general cases is
    not assured but it should be enough for the cases occurring during the
    merging process.
    """
    try:
        return hash(obj)
    except TypeError:
        if isinstance(obj, (list, tuple)):
            return hash(tuple(map(nested_hash, obj)))
        elif isinstance(obj, set):
            return hash(tuple(map(nested_hash, sorted(obj))))
        elif isinstance(obj, dict):
            return hash(tuple(map(nested_hash, sorted(obj.items()))))


def dot_lookup(source, lookup, parent=False):
    """Allow you to reach dictionary items with string or list lookup.

    Recursively find value by lookup key split by '.'.

        >>> from dictdiffer.utils import dot_lookup
        >>> dot_lookup({'a': {'b': 'hello'}}, 'a.b')
        'hello'

    If parent argument is True, returns the parent node of matched
    object.

        >>> dot_lookup({'a': {'b': 'hello'}}, 'a.b', parent=True)
        {'b': 'hello'}

    If node is empty value, returns the whole dictionary object.

        >>> dot_lookup({'a': {'b': 'hello'}}, '')
        {'a': {'b': 'hello'}}

    """
    if lookup is None or lookup == '' or lookup == []:
        return source

    value = source
    if isinstance(lookup, str):
        keys = lookup.split('.')
    elif isinstance(lookup, list):
        keys = lookup
    else:
        raise TypeError('lookup must be string or list')

    if parent:
        keys = keys[:-1]

    for key in keys:
        if isinstance(value, list):
            key = int(key)
        value = value[key]
    return value


def are_different(first, second, tolerance, absolute_tolerance=None):
    """Check if 2 values are different.

    In case of numerical values, the tolerance is used to check if the values
    are different.
    In all other cases, the difference is straight forward.
    """
    if first == second:
        # values are same - simple case
        return False

    first_is_nan, second_is_nan = bool(first != first), bool(second != second)

    if first_is_nan or second_is_nan:
        # two 'NaN' values are not different (see issue #114)
        return not (first_is_nan and second_is_nan)
    elif isinstance(first, num_types) and isinstance(second, num_types):
        # two numerical values are compared with tolerance
        return not math.isclose(
            first,
            second,
            rel_tol=tolerance or 0,
            abs_tol=absolute_tolerance or 0,
        )
    # we got different values
    return True
