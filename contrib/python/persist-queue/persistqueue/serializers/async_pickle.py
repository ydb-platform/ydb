"""An async serializer that extends pickle to work with async file objects."""
from typing import Any, Dict
import pickle
import logging
from aiofiles.threadpool import AsyncBufferedIOBase

log = logging.getLogger(__name__)

protocol = 4  # Python 3 uses protocol version 4 or higher
log.info("Selected pickle protocol: '%s'", protocol)


async def dump(value: Any, fp: AsyncBufferedIOBase,
               sort_keys: bool = False) -> None:
    if sort_keys and isinstance(value, Dict):
        value = {key: value[key] for key in sorted(value)}
    data = pickle.dumps(value, protocol=protocol)
    await fp.write(data)


async def dumps(value: Any, sort_keys: bool = False) -> bytes:
    if sort_keys and isinstance(value, Dict):
        value = {key: value[key] for key in sorted(value)}
    return pickle.dumps(value, protocol=protocol)


async def load(fp: AsyncBufferedIOBase) -> Any:
    data = await fp.read()
    return pickle.loads(data)


def loads(bytes_value: bytes) -> Any:
    return pickle.loads(bytes_value)
