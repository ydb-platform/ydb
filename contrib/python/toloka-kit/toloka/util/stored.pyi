__all__ = [
    'PICKLE_DEFAULT_PROTOCOL',
    'get_base64_digest',
    'get_stored_meta',
    'pickle_dumps_base64',
    'pickle_loads_base64',
]
PICKLE_DEFAULT_PROTOCOL = 4

def get_base64_digest(key: str) -> str: ...


def get_stored_meta(): ...


def pickle_dumps_base64(obj) -> bytes: ...


def pickle_loads_base64(dumped) -> object: ...
