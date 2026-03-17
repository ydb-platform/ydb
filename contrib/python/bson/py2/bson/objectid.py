# Copyright 2009-2015 MongoDB, Inc.
# Modifications copyright (C) 2018 Gabriel Leopoldino
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

"""Tools for working with MongoDB `ObjectIds
<http://dochub.mongodb.org/core/objectids>`_.
"""

import binascii
import calendar
import datetime
import os
import random
import socket
import struct
import threading
import time

from bson.py3compat import PY3, bytes_from_hex, string_type, text_type
from bson.tz_util import utc


# fnv_1a_24 adaptation taken from MongoDB Python Driver at https://github.com/mongodb/mongo-python-driver/commit/61850357a0e0eeec1a30e1adc0bbf7ebee807358
if PY3:
    _ord = lambda x: x
else:
    _ord = ord
# http://isthe.com/chongo/tech/comp/fnv/index.html#FNV-1a
def _fnv_1a_24(data, _ord=_ord):
    """FNV-1a 24 bit hash"""
    # http://www.isthe.com/chongo/tech/comp/fnv/index.html#xor-fold
    # Start with FNV-1a 32 bit.
    hash_size = 2 ** 32
    fnv_32_prime = 16777619
    fnv_1a_hash = 2166136261  # 32-bit FNV-1 offset basis
    for elt in data:
        fnv_1a_hash = fnv_1a_hash ^ _ord(elt)
        fnv_1a_hash = (fnv_1a_hash * fnv_32_prime) % hash_size
    # xor-fold the result to 24 bit.
    return (fnv_1a_hash >> 24) ^ (fnv_1a_hash & 0xffffff)

def _machine_bytes():
    """Get the machine portion of an ObjectId.
    """
    # gethostname() returns a unicode string in python 3.x
    # We only need 3 bytes, and _fnv_1a_24 returns a 24 bit integer.
    # Remove the padding byte.
    return struct.pack("<I", _fnv_1a_24(socket.gethostname().encode()))[:3]


class InvalidId(ValueError):
    """Raised when trying to create an ObjectId from invalid data.
    """

def _raise_invalid_id(oid):
    raise InvalidId(
        "%r is not a valid ObjectId, it must be a 12-byte input"
        " or a 24-character hex string" % oid)


class ObjectId(object):
    """A MongoDB ObjectId.
    """

    _inc = random.randint(0, 0xFFFFFF)
    _inc_lock = threading.Lock()

    _machine_bytes = _machine_bytes()

    __slots__ = ('__id')

    _type_marker = 7

    def __init__(self, oid=None):
        """Initialize a new ObjectId.

        An ObjectId is a 12-byte unique identifier consisting of:

          - a 4-byte value representing the seconds since the Unix epoch,
          - a 3-byte machine identifier,
          - a 2-byte process id, and
          - a 3-byte counter, starting with a random value.

        By default, ``ObjectId()`` creates a new unique identifier. The
        optional parameter `oid` can be an :class:`ObjectId`, or any 12
        :class:`bytes` or, in Python 2, any 12-character :class:`str`.

        For example, the 12 bytes b'foo-bar-quux' do not follow the ObjectId
        specification but they are acceptable input::

          >>> ObjectId(b'foo-bar-quux')
          ObjectId('666f6f2d6261722d71757578')

        `oid` can also be a :class:`unicode` or :class:`str` of 24 hex digits::

          >>> ObjectId('0123456789ab0123456789ab')
          ObjectId('0123456789ab0123456789ab')
          >>>
          >>> # A u-prefixed unicode literal:
          >>> ObjectId(u'0123456789ab0123456789ab')
          ObjectId('0123456789ab0123456789ab')

        Raises :class:`~bson.errors.InvalidId` if `oid` is not 12 bytes nor
        24 hex digits, or :class:`TypeError` if `oid` is not an accepted type.

        :Parameters:
          - `oid` (optional): a valid ObjectId.

        .. mongodoc:: objectids
        """
        if oid is None:
            self.__generate()
        elif isinstance(oid, bytes) and len(oid) == 12:
            self.__id = oid
        else:
            self.__validate(oid)

    @classmethod
    def from_datetime(cls, generation_time):
        """Create a dummy ObjectId instance with a specific generation time.

        This method is useful for doing range queries on a field
        containing :class:`ObjectId` instances.

        .. warning::
           It is not safe to insert a document containing an ObjectId
           generated using this method. This method deliberately
           eliminates the uniqueness guarantee that ObjectIds
           generally provide. ObjectIds generated with this method
           should be used exclusively in queries.

        `generation_time` will be converted to UTC. Naive datetime
        instances will be treated as though they already contain UTC.

        An example using this helper to get documents where ``"_id"``
        was generated before January 1, 2010 would be:

        >>> gen_time = datetime.datetime(2010, 1, 1)
        >>> dummy_id = ObjectId.from_datetime(gen_time)
        >>> result = collection.find({"_id": {"$lt": dummy_id}})

        :Parameters:
          - `generation_time`: :class:`~datetime.datetime` to be used
            as the generation time for the resulting ObjectId.
        """
        if generation_time.utcoffset() is not None:
            generation_time = generation_time - generation_time.utcoffset()
        timestamp = calendar.timegm(generation_time.timetuple())
        oid = struct.pack(
            ">i", int(timestamp)) + b"\x00\x00\x00\x00\x00\x00\x00\x00"
        return cls(oid)

    @classmethod
    def is_valid(cls, oid):
        """Checks if a `oid` string is valid or not.

        :Parameters:
          - `oid`: the object id to validate

        .. versionadded:: 2.3
        """
        if not oid:
            return False

        try:
            ObjectId(oid)
            return True
        except (InvalidId, TypeError):
            return False

    def __generate(self):
        """Generate a new value for this ObjectId.
        """

        # 4 bytes current time
        oid = struct.pack(">i", int(time.time()))

        # 3 bytes machine
        oid += ObjectId._machine_bytes

        # 2 bytes pid
        oid += struct.pack(">H", os.getpid() % 0xFFFF)

        # 3 bytes inc
        with ObjectId._inc_lock:
            oid += struct.pack(">i", ObjectId._inc)[1:4]
            ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF

        self.__id = oid

    def __validate(self, oid):
        """Validate and use the given id for this ObjectId.

        Raises TypeError if id is not an instance of
        (:class:`basestring` (:class:`str` or :class:`bytes`
        in python 3), ObjectId) and InvalidId if it is not a
        valid ObjectId.

        :Parameters:
          - `oid`: a valid ObjectId
        """
        if isinstance(oid, ObjectId):
            self.__id = oid.binary
        # bytes or unicode in python 2, str in python 3
        elif isinstance(oid, string_type):
            if len(oid) == 24:
                try:
                    self.__id = bytes_from_hex(oid)
                except (TypeError, ValueError):
                    _raise_invalid_id(oid)
            else:
                _raise_invalid_id(oid)
        else:
            raise TypeError("id must be an instance of (bytes, %s, ObjectId), "
                            "not %s" % (text_type.__name__, type(oid)))

    @property
    def binary(self):
        """12-byte binary representation of this ObjectId.
        """
        return self.__id

    @property
    def generation_time(self):
        """A :class:`datetime.datetime` instance representing the time of
        generation for this :class:`ObjectId`.

        The :class:`datetime.datetime` is timezone aware, and
        represents the generation time in UTC. It is precise to the
        second.
        """
        timestamp = struct.unpack(">i", self.__id[0:4])[0]
        return datetime.datetime.fromtimestamp(timestamp, utc)

    def __getstate__(self):
        """return value of object for pickling.
        needed explicitly because __slots__() defined.
        """
        return self.__id

    def __setstate__(self, value):
        """explicit state set from pickling
        """
        # Provide backwards compatability with OIDs
        # pickled with pymongo-1.9 or older.
        if isinstance(value, dict):
            oid = value["_ObjectId__id"]
        else:
            oid = value
        # ObjectIds pickled in python 2.x used `str` for __id.
        # In python 3.x this has to be converted to `bytes`
        # by encoding latin-1.
        if PY3 and isinstance(oid, text_type):
            self.__id = oid.encode('latin-1')
        else:
            self.__id = oid

    def __str__(self):
        if PY3:
            return binascii.hexlify(self.__id).decode()
        return binascii.hexlify(self.__id)

    def __repr__(self):
        return "ObjectId('%s')" % (str(self),)

    def __eq__(self, other):
        if isinstance(other, ObjectId):
            return self.__id == other.binary
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, ObjectId):
            return self.__id != other.binary
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, ObjectId):
            return self.__id < other.binary
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, ObjectId):
            return self.__id <= other.binary
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, ObjectId):
            return self.__id > other.binary
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, ObjectId):
            return self.__id >= other.binary
        return NotImplemented

    def __hash__(self):
        """Get a hash value for this :class:`ObjectId`."""
        return hash(self.__id)
