#!/usr/bin/env python

"""
Support for serialization of numpy data types with msgpack.
"""

# Copyright (c) 2013-2018, Lev E. Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license
try:
    import numpy as np

    has_numpy = True
except ImportError:
    has_numpy = False

try:
    import cupy

    has_cupy = True
except ImportError:
    has_cupy = False


def encode_numpy(obj, chain=None):
    """
    Data encoder for serializing numpy data types.
    """
    if not has_numpy:
        return obj if chain is None else chain(obj)
    if has_cupy and isinstance(obj, cupy.ndarray):
        obj = obj.get()
    if isinstance(obj, np.ndarray):
        # If the dtype is structured, store the interface description;
        # otherwise, store the corresponding array protocol type string:
        if obj.dtype.kind == "V":
            kind = b"V"
            descr = obj.dtype.descr
        else:
            kind = b""
            descr = obj.dtype.str
        return {
            b"nd": True,
            b"type": descr,
            b"kind": kind,
            b"shape": obj.shape,
            b"data": obj.data if obj.flags["C_CONTIGUOUS"] else obj.tobytes(),
        }
    elif isinstance(obj, (np.bool_, np.number)):
        return {b"nd": False, b"type": obj.dtype.str, b"data": obj.data}
    elif isinstance(obj, complex):
        return {b"complex": True, b"data": obj.__repr__()}
    else:
        return obj if chain is None else chain(obj)


def tostr(x):
    if isinstance(x, bytes):
        return x.decode()
    else:
        return str(x)


def decode_numpy(obj, chain=None):
    """
    Decoder for deserializing numpy data types.
    """

    try:
        if b"nd" in obj:
            if obj[b"nd"] is True:

                # Check if b'kind' is in obj to enable decoding of data
                # serialized with older versions (#20):
                if b"kind" in obj and obj[b"kind"] == b"V":
                    descr = [
                        tuple(tostr(t) if type(t) is bytes else t for t in d)
                        for d in obj[b"type"]
                    ]
                else:
                    descr = obj[b"type"]
                return np.frombuffer(obj[b"data"], dtype=np.dtype(descr)).reshape(
                    obj[b"shape"]
                )
            else:
                descr = obj[b"type"]
                return np.frombuffer(obj[b"data"], dtype=np.dtype(descr))[0]
        elif b"complex" in obj:
            return complex(tostr(obj[b"data"]))
        else:
            return obj if chain is None else chain(obj)
    except KeyError:
        return obj if chain is None else chain(obj)
