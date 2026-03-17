import random
import struct
import time


# https://github.com/Yelp/py_zipkin/blob/
# 61f8aa3412f6c1b4e1218ed34cb117e97cc9a6cc/py_zipkin/util.py#L22-L75
def generate_random_64bit_string() -> str:
    """Returns a 64 bit UTF-8 encoded string. In the interests of simplicity,
    this is always cast to a `str` instead of (in py2 land) a unicode string.
    Certain clients (I'm looking at you, Twisted) don't enjoy unicode headers.

    :returns: random 16-character string
    """
    return f"{random.getrandbits(64):016x}"


# https://github.com/Yelp/py_zipkin/blob/
# 61f8aa3412f6c1b4e1218ed34cb117e97cc9a6cc/py_zipkin/util.py#L32
def generate_random_128bit_string() -> str:
    """Returns a 128 bit UTF-8 encoded string. Follows the same conventions
    as generate_random_64bit_string().

    The upper 32 bits are the current time in epoch seconds, and the
    lower 96 bits are random. This allows for AWS X-Ray `interop
    <https://github.com/openzipkin/zipkin/issues/1754>`_

    :returns: 32-character hex string
    """
    t = int(time.time())
    lower_96 = random.getrandbits(96)
    return f"{(t << 96) | lower_96:032x}"


def unsigned_hex_to_signed_int(hex_string: str) -> int:
    """Converts a 64-bit hex string to a signed int value.

    This is due to the fact that Apache Thrift only has signed values.

    Examples:
        '17133d482ba4f605' => 1662740067609015813
        'b6dbb1c2b362bf51' => -5270423489115668655

    :param hex_string: the string representation of a zipkin ID
    :returns: signed int representation
    """
    v: int = struct.unpack("q", struct.pack("Q", int(hex_string, 16)))[0]
    return v


def signed_int_to_unsigned_hex(signed_int: int) -> str:
    """Converts a signed int value to a 64-bit hex string.

    Examples:
        1662740067609015813  => '17133d482ba4f605'
        -5270423489115668655 => 'b6dbb1c2b362bf51'

    :param signed_int: an int to convert
    :returns: unsigned hex string
    """
    hex_string = hex(struct.unpack("Q", struct.pack("q", signed_int))[0])[2:]
    if hex_string.endswith("L"):
        return hex_string[:-1]
    return hex_string
