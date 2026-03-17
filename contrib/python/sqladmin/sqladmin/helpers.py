from __future__ import annotations

import csv
import enum
import os
import re
import unicodedata
from abc import ABC, abstractmethod
from datetime import date, datetime, time, timedelta
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Generator,
    TypeVar,
)

from sqlalchemy import Column, inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import RelationshipProperty, sessionmaker

from sqladmin._types import MODEL_PROPERTY

T = TypeVar("T")


_filename_ascii_strip_re = re.compile(r"[^A-Za-z0-9_.-]")
_windows_device_files = (
    "CON",
    "AUX",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "LPT1",
    "LPT2",
    "LPT3",
    "PRN",
    "NUL",
)

standard_duration_re = re.compile(
    r"^"
    r"(?:(?P<days>-?\d+) (days?, )?)?"
    r"(?P<sign>-?)"
    r"((?:(?P<hours>\d+):)(?=\d+:\d+))?"
    r"(?:(?P<minutes>\d+):)?"
    r"(?P<seconds>\d+)"
    r"(?:[\.,](?P<microseconds>\d{1,6})\d{0,6})?"
    r"$"
)

# Support the sections of ISO 8601 date representation that are accepted by timedelta
iso8601_duration_re = re.compile(
    r"^(?P<sign>[-+]?)"
    r"P"
    r"(?:(?P<days>\d+([\.,]\d+)?)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+([\.,]\d+)?)H)?"
    r"(?:(?P<minutes>\d+([\.,]\d+)?)M)?"
    r"(?:(?P<seconds>\d+([\.,]\d+)?)S)?"
    r")?"
    r"$"
)

# Support PostgreSQL's day-time interval format, e.g. "3 days 04:05:06". The
# year-month and mixed intervals cannot be converted to a timedelta and thus
# aren't accepted.
postgres_interval_re = re.compile(
    r"^"
    r"(?:(?P<days>-?\d+) (days? ?))?"
    r"(?:(?P<sign>[-+])?"
    r"(?P<hours>\d+):"
    r"(?P<minutes>\d\d):"
    r"(?P<seconds>\d\d)"
    r"(?:\.(?P<microseconds>\d{1,6}))?"
    r")?$"
)


def prettify_class_name(name: str) -> str:
    return re.sub(r"(?<=.)([A-Z])", r" \1", name)


def slugify_class_name(name: str) -> str:
    dashed = re.sub("(.)([A-Z][a-z]+)", r"\1-\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1-\2", dashed).lower()


def slugify_action_name(name: str) -> str:
    if not re.search(r"^[A-Za-z0-9 \-_]+$", name):
        raise ValueError(
            "name must be non-empty and contain only allowed characters"
            " - use `label` for more expressive names"
        )

    return re.sub(r"[_ ]", "-", name).lower()


def secure_filename(filename: str) -> str:
    """Ported from Werkzeug.

    Pass it a filename and it will return a secure version of it. This
    filename can then safely be stored on a regular file system and passed
    to :func:`os.path.join`. The filename returned is an ASCII only string
    for maximum portability.
    On windows systems the function also makes sure that the file is not
    named after one of the special device files.
    """
    filename = unicodedata.normalize("NFKD", filename)
    filename = filename.encode("ascii", "ignore").decode("ascii")

    for sep in os.path.sep, os.path.altsep:
        if sep:
            filename = filename.replace(sep, " ")
    filename = str(_filename_ascii_strip_re.sub("", "_".join(filename.split()))).strip(
        "._"
    )

    # on nt a couple of special files are present in each folder.  We
    # have to ensure that the target file is not such a filename.  In
    # this case we prepend an underline
    if (
        os.name == "nt"
        and filename
        and filename.split(
            ".",
            maxsplit=1,
        )[0].upper()
        in _windows_device_files
    ):
        filename = f"_{filename}"  # pragma: no cover

    return filename


class Writer(ABC):
    """https://docs.python.org/3/library/csv.html#writer-objects"""

    @abstractmethod
    def writerow(self, row: list[str]) -> None:
        pass  # pragma: no cover

    @abstractmethod
    def writerows(self, rows: list[list[str]]) -> None:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def dialect(self) -> csv.Dialect:
        pass  # pragma: no cover


class _PseudoBuffer:
    """An object that implements just the write method of the file-like
    interface.
    """

    encoding = "utf-8"

    def write(self, value: T) -> bytes:
        return str(value).encode(self.encoding)


def stream_to_csv(
    callback: Callable[[Writer], AsyncGenerator[T, None]],
) -> Generator[T, None, None]:
    """Function that takes a callable (that yields from a CSV Writer), and
    provides it a writer that streams the output directly instead of
    storing it in a buffer. The direct output stream is intended to go
    inside a `starlette.responses.StreamingResponse`.

    Loosely adapted from here:

    https://docs.djangoproject.com/en/1.8/howto/outputting-csv/
    """
    writer = csv.writer(_PseudoBuffer())
    return callback(writer)  # type: ignore


def get_primary_keys(model: Any) -> tuple[Column, ...]:
    return tuple(inspect(model).mapper.primary_key)


def get_object_identifier(obj: Any) -> Any:
    """Returns a value that uniquely identifies this object."""
    primary_keys = get_primary_keys(obj)
    values = [getattr(obj, pk.name) for pk in primary_keys]

    # Unaltered value for tables with a single primary key
    if len(values) == 1:
        return values[0]

    # Combine into single string for multiple primary key support
    return ";".join(str(v).replace("\\", "\\\\").replace(";", r"\;") for v in values)


def _object_identifier_parts(id_string: str, model: type) -> tuple[str, ...]:
    pks = get_primary_keys(model)
    if len(pks) == 1:
        # Only one primary key so no special processing
        return (id_string,)

    values = []
    escape_next = False
    value_start = 0
    for idx, char in enumerate(id_string):
        if escape_next:
            escape_next = False
            continue

        if char == ";":
            values.append(id_string[value_start:idx])
            value_start = idx + 1

        escape_next = char == "\\"

    # Add the last part that's not followed by semicolon
    values.append(id_string[value_start:])

    if len(values) != len(pks):
        raise ValueError(f"Malformed identifier string for model {model.__name__}.")

    # Undo escaping for ; and \
    return tuple(v.replace(r"\;", ";").replace(r"\\", "\\") for v in values)


def object_identifier_values(id_string: str, model: Any) -> tuple:
    values = []
    pks = get_primary_keys(model)
    for pk, part in zip(pks, _object_identifier_parts(id_string, model)):
        type_ = get_column_python_type(pk)
        value: Any
        if issubclass(type_, (date, datetime, time)):
            value = type_.fromisoformat(part)
        elif issubclass(type_, bool):
            value = False if part == "False" else type_(part)
        else:
            value = type_(part)  # type: ignore [call-arg]
        values.append(value)
    return tuple(values)


def get_direction(prop: MODEL_PROPERTY) -> str:
    if not isinstance(prop, RelationshipProperty):
        raise TypeError("Expected RelationshipProperty, got %s" % type(prop))

    name = prop.direction.name
    if name == "ONETOMANY" and not prop.uselist:
        return "ONETOONE"
    return name


def get_column_python_type(column: Column) -> type:
    try:
        return column.type.python_type
    except NotImplementedError:
        if hasattr(column.type, "impl"):
            try:
                return column.type.impl.python_type
            except NotImplementedError:
                ...
        return str


def is_relationship(prop: MODEL_PROPERTY) -> bool:
    return isinstance(prop, RelationshipProperty)


def parse_interval(value: str) -> timedelta | None:
    match = (
        standard_duration_re.match(value)
        or iso8601_duration_re.match(value)
        or postgres_interval_re.match(value)
    )

    if not match:
        return None

    kw: dict[str, Any] = match.groupdict()
    sign = -1 if kw.pop("sign", "+") == "-" else 1
    if kw.get("microseconds"):
        kw["microseconds"] = kw["microseconds"].ljust(6, "0")
    kw = {k: float(v.replace(",", ".")) for k, v in kw.items() if v is not None}
    days = timedelta(kw.pop("days", 0.0) or 0.0)
    if match.re == iso8601_duration_re:
        days *= sign
    return days + sign * timedelta(**kw)


def is_falsy_value(value: Any) -> bool:
    if value is None:
        return True

    if not value and isinstance(value, str):
        return True

    return False


def choice_type_coerce_factory(type_: Any) -> Callable[[Any], Any]:
    from sqlalchemy_utils import Choice

    choices = type_.choices
    if isinstance(choices, type) and issubclass(choices, enum.Enum):
        key, choice_cls = "value", choices
    else:
        key, choice_cls = "code", Choice

    def choice_coerce(value: Any) -> Any:
        if value is None:
            return None

        return (
            getattr(value, key)
            if isinstance(value, choice_cls)
            else type_.python_type(value)
        )

    return choice_coerce


def is_async_session_maker(session_maker: sessionmaker) -> bool:
    return AsyncSession in session_maker.class_.__mro__
