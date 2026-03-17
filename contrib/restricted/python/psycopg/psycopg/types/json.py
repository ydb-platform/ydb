"""
Adapters for JSON types.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import json
import logging
from types import CodeType
from typing import Any, TypeAlias
from threading import Lock
from collections.abc import Callable

from .. import _oids, abc
from .. import errors as e
from ..pq import Format
from ..adapt import AdaptersMap, Buffer, Dumper, Loader, PyFormat
from ..errors import DataError

JsonDumpsFunction: TypeAlias = Callable[[Any], str | bytes]
JsonLoadsFunction: TypeAlias = Callable[[str | bytes], Any]
_AdapterKey: TypeAlias = tuple[type, Callable[..., Any] | CodeType]

logger = logging.getLogger("psycopg")


def set_json_dumps(
    dumps: JsonDumpsFunction, context: abc.AdaptContext | None = None
) -> None:
    """
    Set the JSON serialisation function to store JSON objects in the database.

    :param dumps: The dump function to use.
    :type dumps: `!Callable[[Any], str]`
    :param context: Where to use the `!dumps` function. If not specified, use it
        globally.
    :type context: `~psycopg.Connection` or `~psycopg.Cursor`

    By default dumping JSON uses the builtin `json.dumps`. You can override
    it to use a different JSON library or to use customised arguments.

    If the `Json` wrapper specified a `!dumps` function, use it in precedence
    of the one set by this function.
    """
    if context is None:
        # If changing load function globally, just change the default on the
        # global class
        _JsonDumper._dumps = dumps
    else:
        adapters = context.adapters

        # If the scope is smaller than global, create subclassess and register
        # them in the appropriate scope.
        grid = [
            (Json, PyFormat.BINARY),
            (Json, PyFormat.TEXT),
            (Jsonb, PyFormat.BINARY),
            (Jsonb, PyFormat.TEXT),
        ]
        for wrapper, format in grid:
            base = _get_current_dumper(adapters, wrapper, format)
            dumper = _make_dumper(base, dumps)
            adapters.register_dumper(wrapper, dumper)


def set_json_loads(
    loads: JsonLoadsFunction, context: abc.AdaptContext | None = None
) -> None:
    """
    Set the JSON parsing function to fetch JSON objects from the database.

    :param loads: The load function to use.
    :type loads: `!Callable[[bytes], Any]`
    :param context: Where to use the `!loads` function. If not specified, use
        it globally.
    :type context: `~psycopg.Connection` or `~psycopg.Cursor`

    By default loading JSON uses the builtin `json.loads`. You can override
    it to use a different JSON library or to use customised arguments.
    """
    if context is None:
        # If changing load function globally, just change the default on the
        # global class
        _JsonLoader._loads = loads
    else:
        # If the scope is smaller than global, create subclassess and register
        # them in the appropriate scope.
        grid = [
            ("json", JsonLoader),
            ("json", JsonBinaryLoader),
            ("jsonb", JsonbLoader),
            ("jsonb", JsonbBinaryLoader),
        ]
        for tname, base in grid:
            loader = _make_loader(base, loads)
            context.adapters.register_loader(tname, loader)


# Cache all dynamically-generated types to avoid leaks in case the types
# cannot be GC'd.

_dumpers_cache: dict[_AdapterKey, type[abc.Dumper]] = {}
_loaders_cache: dict[_AdapterKey, type[abc.Loader]] = {}


def _make_dumper(
    base: type[abc.Dumper], dumps: JsonDumpsFunction, __lock: Lock = Lock()
) -> type[abc.Dumper]:
    with __lock:
        if key := _get_adapter_key(base, dumps):
            try:
                return _dumpers_cache[key]
            except KeyError:
                pass

        if not (name := base.__name__).startswith("Custom"):
            name = f"Custom{name}"
        rv = type(name, (base,), {"_dumps": dumps})

        if key:
            _dumpers_cache[key] = rv

        return rv


def _make_loader(
    base: type[Loader], loads: JsonLoadsFunction, __lock: Lock = Lock()
) -> type[abc.Loader]:
    with __lock:
        if key := _get_adapter_key(base, loads):
            try:
                return _loaders_cache[key]
            except KeyError:
                pass

        if not (name := base.__name__).startswith("Custom"):
            name = f"Custom{name}"
        rv = type(name, (base,), {"_loads": loads})

        if key:
            _loaders_cache[key] = rv

        return rv


def _get_adapter_key(t: type, f: Callable[..., Any]) -> _AdapterKey | None:
    """
    Return an adequate caching key for a dumps/loads function and a base type.

    We can't use just the function, even if it is hashable, because different
    lambda expression will have a different hash. The code, instead, will be
    the same if a lambda if defined in a function, so we can use it as a more
    stable hash key.
    """
    # If this function has no code or closure, optimistically assume that it's
    # an ok object and stable enough that will not cause a leak. for example it
    # might be a C function (such as `orjson.loads()`).
    try:
        f.__code__
        f.__closure__
    except AttributeError:
        return (t, f)

    # If there is a closure, the same code might have different effects
    # according to the closure arguments. We could do something funny like
    # using the closure values to build a cache key, but I am not 100% sure
    # about whether the closure objects are always `cell` (the type says it's
    # `cell | Any`) and the solution would be partial anyway because of
    # non-hashable closure objects, therefore let's just give a warning (which
    # can be detected via logging) and avoid to create a leak.
    if f.__closure__:
        logger.warning(
            "using a closure in a dumps/loads function may cause a resource leak"
        )
        return None

    return (t, f.__code__)


class _JsonWrapper:
    __slots__ = ("obj", "dumps")

    def __init__(self, obj: Any, dumps: JsonDumpsFunction | None = None):
        self.obj = obj
        self.dumps = dumps

    def __repr__(self) -> str:
        if len(sobj := repr(self.obj)) > 40:
            sobj = f"{sobj[:35]} ... ({len(sobj)} chars)"
        return f"{self.__class__.__name__}({sobj})"


class Json(_JsonWrapper):
    __slots__ = ()


class Jsonb(_JsonWrapper):
    __slots__ = ()


class _JsonDumper(Dumper):
    # The globally used JSON dumps() function. It can be changed globally (by
    # set_json_dumps) or by a subclass.
    _dumps: JsonDumpsFunction = json.dumps

    def __init__(self, cls: type, context: abc.AdaptContext | None = None):
        super().__init__(cls, context)
        self.dumps = self.__class__._dumps

    def dump(self, obj: Any) -> Buffer | None:
        if isinstance(obj, _JsonWrapper):
            dumps = obj.dumps or self.dumps
            obj = obj.obj
        else:
            dumps = self.dumps
        if isinstance((data := dumps(obj)), str):
            return data.encode()
        return data


class JsonDumper(_JsonDumper):
    oid = _oids.JSON_OID


class JsonBinaryDumper(_JsonDumper):
    format = Format.BINARY
    oid = _oids.JSON_OID


class JsonbDumper(_JsonDumper):
    oid = _oids.JSONB_OID


class JsonbBinaryDumper(_JsonDumper):
    format = Format.BINARY
    oid = _oids.JSONB_OID

    def dump(self, obj: Any) -> Buffer | None:
        if (obj_bytes := super().dump(obj)) is not None:
            return b"\x01" + obj_bytes
        else:
            return None


class _JsonLoader(Loader):
    # The globally used JSON loads() function. It can be changed globally (by
    # set_json_loads) or by a subclass.
    _loads: JsonLoadsFunction = json.loads

    def __init__(self, oid: int, context: abc.AdaptContext | None = None):
        super().__init__(oid, context)
        self.loads = self.__class__._loads

    def load(self, data: Buffer) -> Any:
        # json.loads() cannot work on memoryview.
        if not isinstance(data, bytes):
            data = bytes(data)
        return self.loads(data)


class JsonLoader(_JsonLoader):
    pass


class JsonbLoader(_JsonLoader):
    pass


class JsonBinaryLoader(_JsonLoader):
    format = Format.BINARY


class JsonbBinaryLoader(_JsonLoader):
    format = Format.BINARY

    def load(self, data: Buffer) -> Any:
        if data and data[0] != 1:
            raise DataError("unknown jsonb binary format: {data[0]}")
        if not isinstance((data := data[1:]), bytes):
            data = bytes(data)
        return self.loads(data)


def _get_current_dumper(
    adapters: AdaptersMap, cls: type, format: PyFormat
) -> type[abc.Dumper]:
    try:
        return adapters.get_dumper(cls, format)
    except e.ProgrammingError:
        return _default_dumpers[cls, format]


_default_dumpers: dict[tuple[type[_JsonWrapper], PyFormat], type[Dumper]] = {
    (Json, PyFormat.BINARY): JsonBinaryDumper,
    (Json, PyFormat.TEXT): JsonDumper,
    (Jsonb, PyFormat.BINARY): JsonbBinaryDumper,
    (Jsonb, PyFormat.TEXT): JsonDumper,
}


def register_default_adapters(context: abc.AdaptContext) -> None:
    adapters = context.adapters

    # Currently json binary format is nothing different than text, maybe with
    # an extra memcopy we can avoid.
    adapters.register_dumper(Json, JsonBinaryDumper)
    adapters.register_dumper(Json, JsonDumper)
    adapters.register_dumper(Jsonb, JsonbBinaryDumper)
    adapters.register_dumper(Jsonb, JsonbDumper)
    adapters.register_loader("json", JsonLoader)
    adapters.register_loader("jsonb", JsonbLoader)
    adapters.register_loader("json", JsonBinaryLoader)
    adapters.register_loader("jsonb", JsonbBinaryLoader)
