"""
This module contains helpers for working with JSON data.

Tries to use the `simplejson` module if it exists, otherwise falls back to the
`json` module.

If the `bson` module exists, it allows `bson.ObjectId` objects to be decoded
into JSON automatically.

"""

from datetime import datetime

import simplejson as json

# Conditionally handle bson import so we don't have to depend on pymongo
try:
    import bson
except ImportError:
    # Make a mock bson module (as a class object)
    bson = type("bson", (object,), {"ObjectId": type("ObjectId", (object,), {})})


__all__ = [
    "as_json",
    "from_json",
]


def _default(obj):
    """Handle encoding of an object which isn't JSON serializable by the
    regular encoder."""
    # Datetime objects get encoded according to the ISO standard
    if isinstance(obj, datetime):
        return obj.strftime("%a %b %d %Y %H:%M:%S %z").strip()
    # BSON ObjectId types get encoded as their hex string
    if isinstance(obj, bson.ObjectId):
        return str(obj)
    # This will raise a TypeError, which is what we want at this point
    raise TypeError(repr(obj) + " is not JSON serializable")


def as_json(obj, **kwargs):
    """
    Returns an object JSON encoded properly.

    This method allows you to implement a hook method ``for_json()`` on your
    objects if you want to allow arbitrary objects to be encoded to JSON. A
    ``for_json()`` hook must return a basic JSON type (dict, list, int, float,
    string, unicode, float or None), or a basic JSON type which contains other
    objects which implement the ``for_json()`` hook.

    If an object implements both ``_asdict()`` and ``for_json()`` the latter is
    given preference.

    Also adds additional encoders for :class:`~datetime.datetime` and
    :class:`bson.ObjectId`.

    :param object obj: An object to encode.
    :param kwargs: Any optional keyword arguments to pass to the \
                   JSONEncoder
    :returns: JSON encoded version of `obj`.

    .. versionadded:: 2.4
       Objects which have an ``_asdict()`` method will have that method
       called as part of encoding to JSON, even when not using simplejson.

    .. versionadded:: 2.4
       Objects which have a ``for_json()`` method will have that method called
       and the return value used for encoding instead.

    .. versionchanged:: 3.0
       simplejson (``>= 3.2.0``) is now required, and relied upon for the
       ``_asdict()`` and ``for_json()`` hooks. This change may break backwards
       compatibility in any code that uses these hooks.

    """
    return json.dumps(obj, default=_default, for_json=True)


def from_json(value):
    """Decodes a JSON string into an object.

    :param str value: String to decode
    :returns: Decoded JSON object

    """
    return json.loads(value)
