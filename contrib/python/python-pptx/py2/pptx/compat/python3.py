# encoding: utf-8

"""Provides Python 3 compatibility objects."""

from io import BytesIO  # noqa


def is_integer(obj):
    """
    Return True if *obj* is an int, False otherwise.
    """
    return isinstance(obj, int)


def is_string(obj):
    """
    Return True if *obj* is a string, False otherwise.
    """
    return isinstance(obj, str)


def is_unicode(obj):
    """
    Return True if *obj* is a unicode string, False otherwise.
    """
    return isinstance(obj, str)


def to_unicode(text):
    """Return *text* as a (unicode) str.

    *text* can be str or bytes. A bytes object is assumed to be encoded as UTF-8.
    If *text* is a str object it is returned unchanged.
    """
    if isinstance(text, str):
        return text
    try:
        return text.decode("utf-8")
    except AttributeError:
        raise TypeError("expected unicode string, got %s value %s" % (type(text), text))


Unicode = str
