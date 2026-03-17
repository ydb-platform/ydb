"""
A serializer that extends msgpack to specify recommended parameters and adds a
4 byte length prefix to store multiple objects per file.
"""
import msgpack
import struct
from typing import Any, BinaryIO, Dict


def dump(value: Any, fp: BinaryIO, sort_keys: bool = False) -> None:
    """
    Serialize value as msgpack to a byte-mode file object with a length prefix.

    Args:
        value: The Python object to serialize.
        fp: A file-like object supporting binary write operations.
        sort_keys: If True, the output of dictionaries will be sorted by key.

    Returns:
        None
    """
    if sort_keys and isinstance(value, Dict):
        value = {key: value[key] for key in sorted(value)}
    packed = msgpack.packb(value, use_bin_type=True)
    length = struct.pack("<L", len(packed))
    fp.write(length)
    fp.write(packed)


def dumps(value: Any, sort_keys: bool = False) -> bytes:
    """
    Serialize value as msgpack to bytes.

    Args:
        value: The Python object to serialize.
        sort_keys: If True, the output of dictionaries will be sorted by key.

    Returns:
        A bytes object containing the serialized representation of value.
    """
    if sort_keys and isinstance(value, Dict):
        value = {key: value[key] for key in sorted(value)}
    return msgpack.packb(value, use_bin_type=True)


def load(fp: BinaryIO) -> Any:
    """
    Deserialize one msgpack value from a byte-mode file object using length
    prefix.

    Args:
        fp: A file-like object supporting binary read operations.

    Returns:
        The deserialized Python object.
    """
    length = struct.unpack("<L", fp.read(4))[0]
    return msgpack.unpackb(fp.read(length), use_list=False, raw=False)


def loads(bytes_value: bytes) -> Any:
    """
    Deserialize one msgpack value from bytes.

    Args:
        bytes_value: A bytes object containing the serialized msgpack data.

    Returns:
        The deserialized Python object.
    """
    return msgpack.unpackb(bytes_value, use_list=False, raw=False)
