"""
A serializer that extends json to use bytes and uses newlines to store
multiple objects per file.
"""
import json
from typing import Any, BinaryIO


def dump(value: Any, fp: BinaryIO, sort_keys: bool = False) -> None:
    """Serialize value as json line to a byte-mode file object.

    Args:
        value: The Python object to serialize.
        fp: A file-like object supporting .write() in binary mode.
        sort_keys: If True, the output of dictionaries will be sorted by key.

    Returns:
        None
    """
    fp.write(json.dumps(value, sort_keys=sort_keys).encode('utf-8'))
    fp.write(b"\n")


def dumps(value: Any, sort_keys: bool = False) -> bytes:
    """Serialize value as json to bytes.

    Args:
        value: The Python object to serialize.
        sort_keys: If True, the output of dictionaries will be sorted by key.

    Returns:
        A json-encoded string converted to bytes.
    """
    return json.dumps(value, sort_keys=sort_keys).encode('utf-8')


def load(fp: BinaryIO) -> Any:
    """Deserialize one json line from a byte-mode file object.

    Args:
        fp: A file-like object supporting .readline() in binary mode.

    Returns:
        The deserialized Python object.
    """
    return json.loads(fp.readline().decode('utf-8'))


def loads(bytes_value: bytes) -> Any:
    """Deserialize one json value from bytes.

    Args:
        bytes_value: The json-encoded bytes to deserialize.

    Returns:
        The deserialized Python object.
    """
    return json.loads(bytes_value.decode('utf-8'))
