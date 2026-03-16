# coding: utf-8

from __future__ import unicode_literals

# pylint:disable=invalid-name
try:
    unicode_type = unicode
except NameError:
    unicode_type = str


def ensure_unicode_string(obj):
    """
    Return a unicode string representation of the given obj.

    :param obj:
        The obj we want to represent in unicode
    :type obj:
        varies
    :rtype:
        `unicode`
    """
    try:
        return unicode_type(obj)
    except UnicodeDecodeError:
        if hasattr(obj, 'decode'):
            return obj.decode('utf-8', 'replace')
        return str(obj).decode('utf-8', 'replace')
