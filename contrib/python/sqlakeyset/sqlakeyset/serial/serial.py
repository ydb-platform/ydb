"""Bookmark (de)serialization logic."""

from __future__ import unicode_literals

import base64
import csv
import datetime
import decimal
import uuid
from contextlib import suppress
from io import StringIO
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, TypeVar

import dateutil.parser


class InvalidPage(ValueError):
    """An invalid page marker (in either tuple or bookmark string form) was
    provided to a paging method."""


class BadBookmark(InvalidPage):
    """A bookmark string failed to parse"""


class PageSerializationError(ValueError):
    """Generic serialization error."""


class UnregisteredType(NotImplementedError):
    """An unregistered type was encountered when serializing a bookmark."""


class ConfigurationError(Exception):
    """An error to do with configuring custom bookmark types."""


NONE = "x"
TRUE = "true"
FALSE = "false"
STRING = "s"
BINARY = "b"
INTEGER = "i"
FLOAT = "f"
DECIMAL = "n"
DATE = "d"
DATETIME = "dt"
TIME = "t"
UUID = "uuid"


def parsedate(x):
    return dateutil.parser.parse(x).date()


def binencode(x):
    return base64.b64encode(x).decode("utf-8")


def bindecode(x):
    return base64.b64decode(x.encode("utf-8"))


def escape(x):
    """Python's CSV writer/reader leave newlines unchanged, so records with
    strings containing newlines are split over multiple lines. This is
    undesirable, so we manually escape newlines."""
    return r"\n".join(x.replace(r"\n", r"\\n") for x in x.split("\n"))


def unescape(x):
    """Inverse of escape."""
    return r"\n".join(x.replace(r"\n", "\n") for x in x.split(r"\\n"))


TYPES = [
    (str, "s", unescape, escape),
    (int, "i"),
    (float, "f"),
    (bytes, "b", bindecode, binencode),
    (decimal.Decimal, "n"),
    (uuid.UUID, "uuid"),
    (datetime.datetime, "dt", dateutil.parser.parse),
    (datetime.date, "d", parsedate),
    (datetime.time, "t"),
]


# These special values are serialized without prefix codes.
BUILTINS = {
    "x": None,
    "true": True,
    "false": False,
}


class NotABuiltin(Exception):
    pass


def invert_builtin(x) -> str:
    for k, v in BUILTINS.items():
        if x is v:
            return k

    raise NotABuiltin()


T = TypeVar("T")


def deserialize_int(s: str) -> int:
    return int(s)


class Serial(object):
    serializers: Dict[type, Callable[[Any], Tuple[str, str]]]
    deserializers: Dict[str, Callable[[str], Any]]

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.serializers = {}
        self.deserializers = {}
        for definition in TYPES:
            self.register_type(*definition)

    def register_type(
        self,
        type: Type[T],  # TODO: rename this in a major release
        code: str,
        deserializer: Optional[Callable[[str], T]] = None,
        serializer: Optional[Callable[[T], str]] = None,
    ):
        if serializer is None:
            serializer = str
        if deserializer is None:
            deserializer = type
        if type in self.serializers:
            raise ConfigurationError(
                f"Type {type} already has a serializer registered."
            )
        if code in self.deserializers:
            raise ConfigurationError(f"Type code {code} is already in use.")
        self.serializers[type] = lambda x: (code, serializer(x))
        self.deserializers[code] = deserializer

    def split(self, joined: str) -> List[str]:
        s = StringIO(joined)
        r = csv.reader(s, **self.kwargs)
        row = next(r)
        return row

    def join(self, string_list: Iterable[str]) -> str:
        s = StringIO()
        w = csv.writer(s, **self.kwargs)
        w.writerow(string_list)
        return s.getvalue()

    def serialize_values(self, values: Optional[Iterable]) -> str:
        if values is None:
            return ""
        return self.join(self.serialize_value(_) for _ in values)

    def unserialize_values(self, s: str) -> Optional[Tuple]:
        if s == "":
            return None
        return tuple(self.unserialize_value(_) for _ in self.split(s))

    def get_serializer(self, x):
        for cls in type(x).__mro__:
            with suppress(KeyError):
                return self.serializers[cls]

        return None

    def serialize_value(self, x) -> str:
        with suppress(NotABuiltin):
            return invert_builtin(x)

        serializer = self.get_serializer(x)

        if serializer is None:
            raise UnregisteredType(
                "Don't know how to serialize type of {} ({}). "
                "Use custom_bookmark_type to register it.".format(x, type(x))
            )

        try:
            c, x = serializer(x)
        except Exception as e:
            raise PageSerializationError(
                "Custom bookmark serializer encountered error"
            ) from e

        return "{}:{}".format(c, x)

    def unserialize_value(self, x: str):
        try:
            c, v = x.split(":", 1)
        except ValueError:
            # Must be a builtin
            try:
                return BUILTINS[x]
            except KeyError:
                raise BadBookmark("unrecognized value {}".format(x))

        try:
            deserializer = self.deserializers[c]
        except KeyError:
            # try it as a builtin?
            # This behaviour doesn't make much sense, but keeping it for now for backwards-compatiblity.
            try:
                return BUILTINS[c]
            except KeyError:
                raise BadBookmark("unrecognized value {}".format(x))
        else:
            try:
                return deserializer(v)
            except Exception as e:
                raise BadBookmark(
                    "Custom bookmark deserializer encountered error"
                ) from e
