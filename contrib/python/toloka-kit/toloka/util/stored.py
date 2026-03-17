__all__ = [
    'PICKLE_DEFAULT_PROTOCOL',
    'get_base64_digest',
    'get_stored_meta',
    'pickle_dumps_base64',
    'pickle_loads_base64',
]

import base64
import datetime
import hashlib
import os
import pickle
import socket
import sys
import time

from ..__version__ import __version__


STORAGE_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
PICKLE_DEFAULT_PROTOCOL = 4


def get_base64_digest(key: str) -> str:
    # Equal sign ('=') at the end is just a padding. Skip it since we don't need to decode hash value.
    return base64.urlsafe_b64encode(hashlib.sha3_512(key.encode()).digest()).rstrip(b'=').decode()


def get_stored_meta():
    return {
        'os_name': os.name,
        'hostname': socket.gethostname(),
        'pid': os.getpid(),
        'py_version': '.'.join(map(str, sys.version_info[:3])),
        'pickle_version': PICKLE_DEFAULT_PROTOCOL,
        'toloka_kit_version': __version__,
        'datetime': datetime.datetime.now().strftime(STORAGE_DATETIME_FORMAT),
        'ts': time.time(),
    }


def pickle_dumps_base64(obj) -> bytes:
    return base64.b64encode(pickle.dumps(obj, protocol=PICKLE_DEFAULT_PROTOCOL))


def pickle_loads_base64(dumped) -> object:
    return pickle.loads(base64.b64decode(dumped if isinstance(dumped, bytes) else dumped.encode()))
