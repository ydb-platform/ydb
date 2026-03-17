"""
flask_marshmallow.validate
~~~~~~~~~~~~~~~~~~~~~~~~~~

Custom validation classes for various types of data.
"""

from __future__ import annotations

import io
import os
import re
import typing
from tempfile import SpooledTemporaryFile

from marshmallow.exceptions import ValidationError
from marshmallow.validate import Validator as Validator
from werkzeug.datastructures import FileStorage


def _get_filestorage_size(file: FileStorage) -> int:
    """Return the size of the FileStorage object in bytes."""
    stream = file.stream
    if isinstance(stream, io.BytesIO):
        return stream.getbuffer().nbytes

    if isinstance(stream, SpooledTemporaryFile):
        return os.stat(stream.fileno()).st_size

    size = len(file.read())
    file.stream.seek(0)
    return size


# This function is copied from loguru with few modifications.
# https://github.com/Delgan/loguru/blob/master/loguru/_string_parsers.py#L35
def _parse_size(size: str) -> float:
    """Return the value which the ``size`` represents in bytes."""
    size = size.strip()
    reg = re.compile(r"([e\+\-\.\d]+)\s*([kmgtpezy])?(i)?(b)", flags=re.I)

    match = reg.fullmatch(size)

    if not match:
        raise ValueError(f"Invalid size value: '{size!r}'")

    s, u, i, b = match.groups()

    try:
        s = float(s)
    except ValueError as e:
        raise ValueError(f"Invalid float value while parsing size: '{s!r}'") from e

    u = "kmgtpezy".index(u.lower()) + 1 if u else 0
    i = 1024 if i else 1000
    b = {"b": 8, "B": 1}[b] if b else 1
    return s * i**u / b


class FileSize(Validator):
    """Validator which succeeds if the file passed to it is within the specified
    size range. If ``min`` is not specified, or is specified as `None`,
    no lower bound exists. If ``max`` is not specified, or is specified as `None`,
    no upper bound exists. The inclusivity of the bounds (if they exist)
    is configurable.
    If ``min_inclusive`` is not specified, or is specified as `True`, then
    the ``min`` bound is included in the range. If ``max_inclusive`` is not specified,
    or is specified as `True`, then the ``max`` bound is included in the range.

    Example: ::

        class ImageSchema(Schema):
            image = File(required=True, validate=FileSize(min="1 MiB", max="2 MiB"))

    :param min: The minimum size (lower bound). If not provided, minimum
        size will not be checked.
    :param max: The maximum size (upper bound). If not provided, maximum
        size will not be checked.
    :param min_inclusive: Whether the ``min`` bound is included in the range.
    :param max_inclusive: Whether the ``max`` bound is included in the range.
    :param error: Error message to raise in case of a validation error.
        Can be interpolated with `{input}`, `{min}` and `{max}`.
    """

    message_min = "Must be {min_op} {{min}}."
    message_max = "Must be {max_op} {{max}}."
    message_all = "Must be {min_op} {{min}} and {max_op} {{max}}."

    message_gte = "greater than or equal to"
    message_gt = "greater than"
    message_lte = "less than or equal to"
    message_lt = "less than"

    def __init__(
        self,
        min: str | None = None,
        max: str | None = None,
        min_inclusive: bool = True,
        max_inclusive: bool = True,
        error: str | None = None,
    ):
        self.min = min
        self.max = max
        self.min_size = _parse_size(self.min) if self.min else None
        self.max_size = _parse_size(self.max) if self.max else None
        self.min_inclusive = min_inclusive
        self.max_inclusive = max_inclusive
        self.error = error

        self.message_min = self.message_min.format(
            min_op=self.message_gte if self.min_inclusive else self.message_gt
        )
        self.message_max = self.message_max.format(
            max_op=self.message_lte if self.max_inclusive else self.message_lt
        )
        self.message_all = self.message_all.format(
            min_op=self.message_gte if self.min_inclusive else self.message_gt,
            max_op=self.message_lte if self.max_inclusive else self.message_lt,
        )

    def _repr_args(self):
        return (
            f"min={self.min!r}, max={self.max!r}, "
            f"min_inclusive={self.min_inclusive!r}, "
            f"max_inclusive={self.max_inclusive!r}"
        )

    def _format_error(self, value, message):
        return (self.error or message).format(input=value, min=self.min, max=self.max)

    def __call__(self, value):
        if not isinstance(value, FileStorage):
            raise TypeError(
                f"A FileStorage object is required, not {type(value).__name__!r}"
            )

        file_size = _get_filestorage_size(value)
        if self.min_size is not None and (
            file_size < self.min_size
            if self.min_inclusive
            else file_size <= self.min_size
        ):
            message = self.message_min if self.max is None else self.message_all
            raise ValidationError(self._format_error(value, message))

        if self.max_size is not None and (
            file_size > self.max_size
            if self.max_inclusive
            else file_size >= self.max_size
        ):
            message = self.message_max if self.min is None else self.message_all
            raise ValidationError(self._format_error(value, message))

        return value


class FileType(Validator):
    """Validator which succeeds if the uploaded file is allowed by a given list
    of extensions.

    Example: ::

        class ImageSchema(Schema):
            image = File(required=True, validate=FileType([".png"]))

    :param accept: A sequence of allowed extensions.
    :param error: Error message to raise in case of a validation error.
        Can be interpolated with ``{input}`` and ``{extensions}``.
    """

    default_message = "Not an allowed file type. Allowed file types: [{extensions}]"

    def __init__(
        self,
        accept: typing.Iterable[str],
        error: str | None = None,
    ):
        self.allowed_types = {ext.lower() for ext in accept}
        self.error = error or self.default_message

    def _format_error(self, value):
        return (self.error or self.default_message).format(
            input=value, extensions=",".join(self.allowed_types)
        )

    def __call__(self, value):
        if not isinstance(value, FileStorage):
            raise TypeError(
                f"A FileStorage object is required, not {type(value).__name__!r}"
            )

        _, extension = (
            os.path.splitext(value.filename) if value.filename else (None, None)
        )
        if extension is None or extension.lower() not in self.allowed_types:
            raise ValidationError(self._format_error(value))

        return value
