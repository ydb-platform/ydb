"""
A serializer that extends cbor2 to specify recommended parameters and adds a
4 byte length prefix to store multiple objects per file.
"""
import cbor2
from struct import Struct
from typing import Any, BinaryIO

# Define the Struct for prefixing serialized objects with their byte length
length_struct = Struct("<L")


def dump(value: Any, fp: BinaryIO, sort_keys: bool = False) -> None:
    """
    Serialize value as cbor2 to a byte-mode file object with a length prefix.

    Args:
        value: The Python object to serialize.
        fp: A file-like object supporting binary write operations.
        sort_keys: If True, the output of dictionaries will be sorted by key.

    Returns:
        None
    """
    # If sorting is required and the value is a dictionary, sort it by keys
    if sort_keys and isinstance(value, dict):
        value = {key: value[key] for key in sorted(value)}
    packed = cbor2.dumps(value)
    length = length_struct.pack(len(packed))
    fp.write(length)
    fp.write(packed)


def dumps(value: Any, sort_keys: bool = False) -> bytes:
    """
    Serialize value as cbor2 to bytes without length prefix.

    Args:
        value: The Python object to serialize.
        sort_keys: If True, the output of dictionaries will be sorted by key.

    Returns:
        A bytes object containing the serialized representation of the value.
    """
    # If sorting is required and the value is a dictionary, sort it by keys
    if sort_keys and isinstance(value, dict):
        value = {key: value[key] for key in sorted(value)}
    return cbor2.dumps(value)


def load(fp: BinaryIO) -> Any:
    """
    Deserialize one cbor2 value from a byte-mode file object
    using length prefix.

    Args:
        fp: A file-like object supporting binary read operations.

    Returns:
        The deserialized Python object.
    """
    # Read the 4-byte length prefix and determine the length of the
    # serialized object
    length = length_struct.unpack(fp.read(4))[0]
    # Read the serialized object using the determined length and
    # deserialize it
    return cbor2.loads(fp.read(length))


def loads(bytes_value: bytes) -> Any:
    """
    Deserialize one cbor2 value from bytes.

    Args:
        bytes_value: The bytes object containing the serialized representation.

    Returns:
        The deserialized Python object.
    """
    return cbor2.loads(bytes_value)
