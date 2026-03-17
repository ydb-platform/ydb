#  Copyright (c) Kuba Szczodrzyński 2023-9-11.

from datastruct.fields import adapter, field
from datastruct.types import Value


def uuid_le_field(*, default=...):
    from uuid import UUID

    return adapter(
        encode=lambda value, ctx: value.bytes_le,
        decode=lambda value, ctx: UUID(bytes_le=value),
    )(field(16, default=default))


def utf16le_field(length: Value[int], *, default: str = ...):
    return adapter(
        encode=lambda value, ctx: (value + "\x00").encode("utf-16le"),
        decode=lambda value, ctx: value.decode("utf-16le").rstrip("\x00"),
    )(field(length, default=default))
