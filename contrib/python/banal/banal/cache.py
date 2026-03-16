import types
from hashlib import sha1
from itertools import chain
from typing import Any, Union, Iterable
from datetime import date, datetime

from banal.dicts import is_mapping
from banal.lists import is_sequence


def _bytes_str(obj: Union[str, bytes]) -> bytes:
    if not isinstance(obj, str):
        return obj
    return obj.encode("utf-8")


def bytes_iter(obj: Any) -> Iterable[bytes]:
    """Turn a complex object into an iterator of byte strings.
    The resulting iterator can be used for caching.
    """
    if obj is None:
        return
    elif isinstance(obj, (bytes, str)):
        yield _bytes_str(obj)
    elif isinstance(obj, (date, datetime)):
        yield _bytes_str(obj.isoformat())
    elif is_mapping(obj):
        if None in obj:
            yield from bytes_iter(obj.pop(None))
        for key in sorted(obj.keys()):
            for out in chain(bytes_iter(key), bytes_iter(obj[key])):
                yield out
    elif is_sequence(obj):
        if isinstance(obj, (list, set)):
            try:
                obj = sorted(obj)
            except Exception:
                pass
        for item in obj:
            for out in bytes_iter(item):
                yield out
    elif isinstance(
        obj,
        (
            types.FunctionType,
            types.BuiltinFunctionType,
            types.MethodType,
            types.BuiltinMethodType,
        ),
    ):
        yield _bytes_str(getattr(obj, "func_name", ""))
    else:
        yield _bytes_str(str(obj))


def hash_data(obj: Any) -> str:
    """Generate a SHA1 from a complex object."""
    collect = sha1()
    for data in bytes_iter(obj):
        collect.update(data)
    return collect.hexdigest()
