from threading import Lock

from . import Backend
from .. import signals, config


class MemoryBackend(Backend):
    """
    Simple in-memory backend that should be mostly used for testing purposes
    """
    _storage = {}
    _lock = Lock()

    def __init__(self):
        super().__init__()

    def get(self, key):
        with self._lock:
            return self._storage.get(key)

    def mget(self, keys):
        if not keys:
            return
        result = []
        with self._lock:
            for key in keys:
                value = self._storage.get(key)
                if value is not None:
                    result.append((key, value))
        return result

    def set(self, key, value):
        with self._lock:
            old_value = self._storage.get(key)
            self._storage[key] = value
            signals.config_updated.send(
                sender=config, key=key, old_value=old_value, new_value=value
            )
