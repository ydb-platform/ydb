"""A serializer that extends pickle to change the default protocol."""
from typing import Any, BinaryIO, Dict
import pickle
import logging


log = logging.getLogger(__name__)

# Retrieve the selected pickle protocol from a common utility module
protocol = 4  # Python 3 uses protocol version 4 or higher
log.info("Selected pickle protocol: '{}'".format(protocol))


def dump(value: Any, fp: BinaryIO, sort_keys: bool = False) -> None:
    """
    Serialize value as pickle to a byte-mode file object.

    Args:
        value: The Python object to serialize.
        fp: A file-like object supporting binary write operations.
        sort_keys: If True and if the value is a dictionary, the keys will
                   be sorted before serialization.

    Returns:
        None
    """
    if sort_keys and isinstance(value, Dict):
        # Sort the dictionary by keys if sort_keys is True
        value = {key: value[key] for key in sorted(value)}
    pickle.dump(value, fp, protocol=protocol)


def dumps(value: Any, sort_keys: bool = False) -> bytes:
    """
    Serialize value as pickle to bytes.

    Args:
        value: The Python object to serialize.
        sort_keys: If True and if the value is a dictionary, the keys will
                   be sorted before serialization.

    Returns:
        A bytes object containing the serialized representation of value.
    """
    if sort_keys and isinstance(value, Dict):
        # Sort the dictionary by keys if sort_keys is True
        value = {key: value[key] for key in sorted(value)}
    return pickle.dumps(value, protocol=protocol)


def load(fp: BinaryIO) -> Any:
    """
    Deserialize one pickle value from a byte-mode file object.

    Args:
        fp: A file-like object supporting binary read operations.

    Returns:
        The deserialized Python object.
    """
    return pickle.load(fp)


def loads(bytes_value: bytes) -> Any:
    """
    Deserialize one pickle value from bytes.

    Args:
        bytes_value: A bytes object containing the serialized pickle data.

    Returns:
        The deserialized Python object.
    """
    return pickle.loads(bytes_value)
