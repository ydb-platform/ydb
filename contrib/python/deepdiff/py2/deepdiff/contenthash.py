#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
import sys
from collections import Iterable
from collections import MutableMapping
from collections import defaultdict
from decimal import Decimal
from hashlib import sha1
import logging

from deepdiff.helper import py3, int, strings, numbers, items

logger = logging.getLogger(__name__)


class Skipped(object):
    def __repr__(self):
        return "Skipped"  # pragma: no cover

    def __str__(self):
        return "Skipped"  # pragma: no cover


class Unprocessed(object):
    def __repr__(self):
        return "Error: Unprocessed"  # pragma: no cover

    def __str__(self):
        return "Error: Unprocessed"  # pragma: no cover


class NotHashed(object):
    def __repr__(self):
        return "Error: NotHashed"  # pragma: no cover

    def __str__(self):
        return "Error: NotHashed"  # pragma: no cover


class DeepHash(dict):
    r"""
    **DeepHash**
    """

    def __init__(self,
                 obj,
                 hashes=None,
                 exclude_types=set(),
                 hasher=hash,
                 ignore_repetition=True,
                 significant_digits=None,
                 **kwargs):
        if kwargs:
            raise ValueError(
                ("The following parameter(s) are not valid: %s\n"
                 "The valid parameters are obj, hashes, exclude_types."
                 "hasher and ignore_repetition.") % ', '.join(kwargs.keys()))
        self.obj = obj
        self.exclude_types = set(exclude_types)
        self.exclude_types_tuple = tuple(
            exclude_types)  # we need tuple for checking isinstance
        self.ignore_repetition = ignore_repetition

        self.hasher = hasher
        hashes = hashes if hashes else {}
        self.update(hashes)
        self['unprocessed'] = []
        self.unprocessed = Unprocessed()
        self.skipped = Skipped()
        self.not_hashed = NotHashed()
        self.significant_digits = significant_digits

        self.__hash(obj, parents_ids=frozenset({id(obj)}))

        if self['unprocessed']:
            logger.warning("Can not hash the following items: {}.".format(self['unprocessed']))
        else:
            del self['unprocessed']

    @staticmethod
    def sha1hex(obj):
        """Use Sha1 for more accuracy."""
        if py3:  # pragma: no cover
            if isinstance(obj, str):
                obj = "{}:{}".format(type(obj).__name__, obj)
                obj = obj.encode('utf-8')
            elif isinstance(obj, bytes):
                obj = type(obj).__name__.encode('utf-8') + b":" + obj
        else:  # pragma: no cover
            if isinstance(obj, unicode):
                obj = u"{}:{}".format(type(obj).__name__, obj)
                obj = obj.encode('utf-8')
            elif isinstance(obj, str):
                obj = type(obj).__name__ + ":" + obj
        return sha1(obj).hexdigest()

    @staticmethod
    def __add_to_frozen_set(parents_ids, item_id):
        parents_ids = set(parents_ids)
        parents_ids.add(item_id)
        return frozenset(parents_ids)

    def __get_and_set_str_hash(self, obj):
        obj_id = id(obj)
        result = self.hasher(obj)
        result = "str:{}".format(result)
        self[obj_id] = result
        return result

    def __hash_obj(self, obj, parents_ids=frozenset({}), is_namedtuple=False):
        """Difference of 2 objects"""
        try:
            if is_namedtuple:
                obj = obj._asdict()
            else:
                obj = obj.__dict__
        except AttributeError:
            try:
                obj = {i: getattr(obj, i) for i in obj.__slots__}
            except AttributeError:
                self['unprocessed'].append(obj)
                return self.unprocessed

        result = self.__hash_dict(obj, parents_ids)
        result = "nt{}".format(result) if is_namedtuple else "obj{}".format(
            result)
        return result

    def __skip_this(self, obj):
        skip = False
        if isinstance(obj, self.exclude_types_tuple):
            skip = True

        return skip

    def __hash_dict(self, obj, parents_ids=frozenset({})):

        result = []
        obj_keys = set(obj.keys())

        for key in obj_keys:
            key_hash = self.__hash(key)
            item = obj[key]
            item_id = id(item)
            if parents_ids and item_id in parents_ids:
                continue
            parents_ids_added = self.__add_to_frozen_set(parents_ids, item_id)
            hashed = self.__hash(item, parents_ids_added)
            hashed = "{}:{}".format(key_hash, hashed)
            result.append(hashed)

        result.sort()
        result = ';'.join(result)
        result = "dict:{%s}" % result

        return result

    def __hash_set(self, obj):
        return "set:{}".format(self.__hash_iterable(obj))

    def __hash_iterable(self, obj, parents_ids=frozenset({})):

        result = defaultdict(int)

        for i, x in enumerate(obj):
            if self.__skip_this(x):
                continue

            item_id = id(x)
            if parents_ids and item_id in parents_ids:
                continue

            parents_ids_added = self.__add_to_frozen_set(parents_ids, item_id)
            hashed = self.__hash(x, parents_ids_added)
            result[hashed] += 1

        if self.ignore_repetition:
            result = list(result.keys())
        else:
            result = [
                '{}|{}'.format(i[0], i[1]) for i in getattr(result, items)()
            ]

        result.sort()
        result = ','.join(result)
        result = "{}:{}".format(type(obj).__name__, result)

        return result

    def __hash_str(self, obj):
        return self.__get_and_set_str_hash(obj)

    def __hash_number(self, obj):
        # Based on diff.DeepDiff.__diff_numbers
        if self.significant_digits is not None and isinstance(obj, (
                float, complex, Decimal)):
            obj_s = ("{:.%sf}" % self.significant_digits).format(obj)

            # Special case for 0: "-0.00" should compare equal to "0.00"
            if set(obj_s) <= set("-0."):
                obj_s = "0.00"
            result = "number:{}".format(obj_s)
            obj_id = id(obj)
            self[obj_id] = result
        else:
            result = "{}:{}".format(type(obj).__name__, obj)
        return result

    def __hash_tuple(self, obj, parents_ids):
        # Checking to see if it has _fields. Which probably means it is a named
        # tuple.
        try:
            obj._asdict
        # It must be a normal tuple
        except AttributeError:
            result = self.__hash_iterable(obj, parents_ids)
        # We assume it is a namedtuple then
        else:
            result = self.__hash_obj(obj, parents_ids, is_namedtuple=True)
        return result

    def __hash(self, obj, parent="root", parents_ids=frozenset({})):
        """The main diff method"""

        obj_id = id(obj)
        if obj_id in self:
            return self[obj_id]

        result = self.not_hashed

        if self.__skip_this(obj):
            result = self.skipped

        elif obj is None:
            result = 'NONE'

        elif isinstance(obj, strings):
            result = self.__hash_str(obj)

        elif isinstance(obj, numbers):
            result = self.__hash_number(obj)

        elif isinstance(obj, MutableMapping):
            result = self.__hash_dict(obj, parents_ids)

        elif isinstance(obj, tuple):
            result = self.__hash_tuple(obj, parents_ids)

        elif isinstance(obj, (set, frozenset)):
            result = self.__hash_set(obj)

        elif isinstance(obj, Iterable):
            result = self.__hash_iterable(obj, parents_ids)

        else:
            result = self.__hash_obj(obj, parents_ids)

        if result != self.not_hashed and obj_id not in self and not isinstance(
                obj, numbers):
            self[obj_id] = result

        if result is self.not_hashed:  # pragma: no cover
            self[obj_id] = self.not_hashed
            self['unprocessed'].append(obj)

        return result


if __name__ == "__main__":  # pragma: no cover
    if not py3:
        sys.exit("Please run with Python 3 to verify the doc strings.")
    import doctest
    doctest.testmod()
