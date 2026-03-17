# -*- coding: utf-8 -*-

""" py3 compatibility class


"""

from __future__ import absolute_import, print_function, with_statement

try:
    basestring
except NameError:
    basestring = str

try:
    unicode
except NameError:
    unicode = str


if isinstance(b'', type('')):  # py 2.x
    text_types = (basestring,)  # noqa
    bytes_type = bytes
    str_type = basestring  # noqa

    def utf8(data):
        """  return utf8-encoded string """
        if isinstance(data, basestring):
            return data.decode("utf8")
        return unicode(data)

    def to_string(data):
        """ return string """
        if isinstance(data, unicode):
            return data.encode("utf8")
        return str(data)

    def to_bytes(data):
        """ return bytes """
        if isinstance(data, unicode):
            return data.encode("utf8")
        return str(data)

else:  # py 3.x
    text_types = (bytes, str)
    bytes_type = bytes
    str_type = str

    def utf8(data):
        """ return utf8-encoded string """
        if isinstance(data, bytes):
            return data.decode("utf8")
        return str(data)

    def to_string(data):
        """convert to string"""
        if isinstance(data, bytes):
            return data.decode("utf8")
        return str(data)

    def to_bytes(data):
        """return bytes"""
        if isinstance(data, str):
            return data.encode("utf8")
        return bytes(data)
