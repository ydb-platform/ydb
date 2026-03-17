# coding: utf-8

import functools
import catalogue

# These need to be imported before packer and unpacker
from ._epoch import utc, epoch  # noqa

from ._version import version
from .exceptions import *

# In msgpack-python these are put under a _cmsgpack module that textually includes
# them. I dislike this so I refactored it.
from ._packer import Packer as _Packer
from ._unpacker import unpackb as _unpackb
from ._unpacker import Unpacker as _Unpacker
from .ext import ExtType
from ._msgpack_numpy import encode_numpy as _encode_numpy
from ._msgpack_numpy import decode_numpy as _decode_numpy


msgpack_encoders = catalogue.create("srsly", "msgpack_encoders", entry_points=True)
msgpack_decoders = catalogue.create("srsly", "msgpack_decoders", entry_points=True)

msgpack_encoders.register("numpy", func=_encode_numpy)
msgpack_decoders.register("numpy", func=_decode_numpy)


# msgpack_numpy extensions
class Packer(_Packer):
    def __init__(self, *args, **kwargs):
        default = kwargs.get("default")
        for encoder in msgpack_encoders.get_all().values():
            default = functools.partial(encoder, chain=default)
        kwargs["default"] = default
        super(Packer, self).__init__(*args, **kwargs)


class Unpacker(_Unpacker):
    def __init__(self, *args, **kwargs):
        object_hook = kwargs.get("object_hook")
        for decoder in msgpack_decoders.get_all().values():
            object_hook = functools.partial(decoder, chain=object_hook)
        kwargs["object_hook"] = object_hook
        super(Unpacker, self).__init__(*args, **kwargs)


def pack(o, stream, **kwargs):
    """
    Pack an object and write it to a stream.
    """
    packer = Packer(**kwargs)
    stream.write(packer.pack(o))


def packb(o, **kwargs):
    """
    Pack an object and return the packed bytes.
    """
    return Packer(**kwargs).pack(o)


def unpack(stream, **kwargs):
    """
    Unpack a packed object from a stream.
    """
    if "object_pairs_hook" not in kwargs:
        object_hook = kwargs.get("object_hook")
        for decoder in msgpack_decoders.get_all().values():
            object_hook = functools.partial(decoder, chain=object_hook)
        kwargs["object_hook"] = object_hook
    data = stream.read()
    return _unpackb(data, **kwargs)


def unpackb(packed, **kwargs):
    """
    Unpack a packed object.
    """
    if "object_pairs_hook" not in kwargs:
        object_hook = kwargs.get("object_hook")
        for decoder in msgpack_decoders.get_all().values():
            object_hook = functools.partial(decoder, chain=object_hook)
        kwargs["object_hook"] = object_hook
    return _unpackb(packed, **kwargs)


# alias for compatibility to simplejson/marshal/pickle.
load = unpack
loads = unpackb

dump = pack
dumps = packb
