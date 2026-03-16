from collections import namedtuple
from srsly.msgpack import packb, unpackb, ExtType


def test_namedtuple():
    T = namedtuple("T", "foo bar")

    def default(o):
        if isinstance(o, T):
            return dict(o._asdict())
        raise TypeError("Unsupported type %s" % (type(o),))

    packed = packb(T(1, 42), strict_types=True, use_bin_type=True, default=default)
    unpacked = unpackb(packed, raw=False)
    assert unpacked == {"foo": 1, "bar": 42}


def test_tuple():
    t = ("one", 2, b"three", (4,))

    def default(o):
        if isinstance(o, tuple):
            return {"__type__": "tuple", "value": list(o)}
        raise TypeError("Unsupported type %s" % (type(o),))

    def convert(o):
        if o.get("__type__") == "tuple":
            return tuple(o["value"])
        return o

    data = packb(t, strict_types=True, use_bin_type=True, default=default)
    expected = unpackb(data, raw=False, object_hook=convert)

    assert expected == t


def test_tuple_ext():
    t = ("one", 2, b"three", (4,))

    MSGPACK_EXT_TYPE_TUPLE = 0

    def default(o):
        if isinstance(o, tuple):
            # Convert to list and pack
            payload = packb(
                list(o), strict_types=True, use_bin_type=True, default=default
            )
            return ExtType(MSGPACK_EXT_TYPE_TUPLE, payload)
        raise TypeError(repr(o))

    def convert(code, payload):
        if code == MSGPACK_EXT_TYPE_TUPLE:
            # Unpack and convert to tuple
            return tuple(unpackb(payload, raw=False, ext_hook=convert))
        raise ValueError("Unknown Ext code {}".format(code))

    data = packb(t, strict_types=True, use_bin_type=True, default=default)
    expected = unpackb(data, raw=False, ext_hook=convert)

    assert expected == t
