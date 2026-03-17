import gc

from . import msgpack
from .msgpack import msgpack_encoders, msgpack_decoders  # noqa: F401
from .util import force_path, FilePath, JSONInputBin, JSONOutputBin


def msgpack_dumps(data: JSONInputBin) -> bytes:
    """Serialize an object to a msgpack byte string.

    data: The data to serialize.
    RETURNS (bytes): The serialized bytes.
    """
    return msgpack.dumps(data, use_bin_type=True)


def msgpack_loads(data: bytes, use_list: bool = True) -> JSONOutputBin:
    """Deserialize msgpack bytes to a Python object.

    data (bytes): The data to deserialize.
    use_list (bool): Don't use tuples instead of lists. Can make
        deserialization slower.
    RETURNS: The deserialized Python object.
    """
    # msgpack-python docs suggest disabling gc before unpacking large messages
    gc.disable()
    msg = msgpack.loads(data, raw=False, use_list=use_list)
    gc.enable()
    return msg


def write_msgpack(path: FilePath, data: JSONInputBin) -> None:
    """Create a msgpack file and dump contents.

    location (FilePath): The file path.
    data (JSONInputBin): The data to serialize.
    """
    file_path = force_path(path, require_exists=False)
    with file_path.open("wb") as f:
        msgpack.dump(data, f, use_bin_type=True)


def read_msgpack(path: FilePath, use_list: bool = True) -> JSONOutputBin:
    """Load a msgpack file.

    location (FilePath): The file path.
    use_list (bool): Don't use tuples instead of lists. Can make
        deserialization slower.
    RETURNS (JSONOutputBin): The loaded and deserialized content.
    """
    file_path = force_path(path)
    with file_path.open("rb") as f:
        # msgpack-python docs suggest disabling gc before unpacking large messages
        gc.disable()
        msg = msgpack.load(f, raw=False, use_list=use_list)
        gc.enable()
        return msg
