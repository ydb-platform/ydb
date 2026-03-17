# -*- coding: utf-8 -*-
import sys
import datetime
from decimal import Decimal
from collections import namedtuple
import logging

logger = logging.getLogger(__name__)

py_major_version = sys.version[0]
py_minor_version = sys.version[2]

py3 = py_major_version == '3'

if (py_major_version, py_minor_version) == (2.6):  # pragma: no cover
    sys.exit('Python 2.6 is not supported.')

pypy3 = py3 and hasattr(sys, "pypy_translation_info")

if py3:  # pragma: no cover
    from builtins import int
    strings = (str, bytes)  # which are both basestring
    unicode_type = str
    bytes_type = bytes
    numbers = (int, float, complex, datetime.datetime, datetime.date, Decimal)
    items = 'items'
else:  # pragma: no cover
    int = int
    strings = (str, unicode)
    unicode_type = unicode
    bytes_type = str
    numbers = (int, float, long, complex, datetime.datetime, datetime.date,
               Decimal)

    items = 'iteritems'

IndexedHash = namedtuple('IndexedHash', 'indexes item')

EXPANDED_KEY_MAP = {  # pragma: no cover
    'dic_item_added': 'dictionary_item_added',
    'dic_item_removed': 'dictionary_item_removed',
    'newindexes': 'new_indexes',
    'newrepeat': 'new_repeat',
    'newtype': 'new_type',
    'newvalue': 'new_value',
    'oldindexes': 'old_indexes',
    'oldrepeat': 'old_repeat',
    'oldtype': 'old_type',
    'oldvalue': 'old_value'}


def short_repr(item, max_length=15):
    """Short representation of item if it is too long"""
    item = repr(item)
    if len(item) > max_length:
        item = '{}...{}'.format(item[:max_length - 3], item[-1])
    return item


class ListItemRemovedOrAdded(object):  # pragma: no cover
    """Class of conditions to be checked"""
    pass


class NotPresent(object):  # pragma: no cover
    """
    In a change tree, this indicated that a previously existing object has been removed -- or will only be added
    in the future.
    We previously used None for this but this caused problem when users actually added and removed None. Srsly guys? :D
    """
    def __repr__(self):
        return "Not Present"

    def __str__(self):
        return self.__repr__()


notpresent = NotPresent()

WARNING_NUM = 0


def warn(*args, **kwargs):
    global WARNING_NUM

    if WARNING_NUM < 10:
        WARNING_NUM += 1
        logger.warning(*args, **kwargs)


class RemapDict(dict):
    """
    Remap Dictionary.

    For keys that have a new, longer name, remap the old key to the new key.
    Other keys that don't have a new name are handled as before.
    """

    def __getitem__(self, old_key):
        new_key = EXPANDED_KEY_MAP.get(old_key, old_key)
        if new_key != old_key:
            warn(
                "DeepDiff Deprecation: %s is renamed to %s. Please start using "
                "the new unified naming convention.", old_key, new_key)
        if new_key in self:
            return self.get(new_key)
        else:  # pragma: no cover
            raise KeyError(new_key)


class Verbose(object):
    """
    Global verbose level
    """
    level = 1
