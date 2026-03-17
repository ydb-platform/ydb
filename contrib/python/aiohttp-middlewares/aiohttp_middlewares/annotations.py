"""
===============================
aiohttp_middlewares.annotations
===============================

Type annotation shortcuts for ``aiohttp_middlewares`` library.

"""

import importlib
from typing import (
    Any,
    Awaitable,
    Callable,
    Collection,
    Dict,
    Pattern,
    Type,
    Union,
)

from aiohttp import web
from yarl import URL


try:
    # (<3.9.0) Try to import Middleware from aiohttp.web_middlewares
    Middleware = importlib.import_module("aiohttp.web_middlewares")._Middleware
except AttributeError:
    # (>=3.9.0) If that fails, import Middleware from aiohttp.typedefs
    Middleware = importlib.import_module("aiohttp.typedefs").Middleware

# Make flake8 happy
(Middleware,)  # noqa: B018

DictStrAny = Dict[str, Any]
DictStrStr = Dict[str, str]

ExceptionType = Type[Exception]
# FIXME: Drop Handler type definition after `aiohttp-middlewares` will require
# only `aiohttp>=3.8.0`
Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]

IntCollection = Collection[int]
StrCollection = Collection[str]

Url = Union[str, Pattern[str], URL]
UrlCollection = Collection[Url]
UrlDict = Dict[Url, StrCollection]
Urls = Union[UrlCollection, UrlDict]
