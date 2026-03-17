#  Copyright (c) Kuba Szczodrzyński 2023-9-11.

from datastruct.fields import adapter, field
from datastruct.types import Value


def filetime_field(*, default=...):
    from datetime import datetime
    from struct import pack, unpack

    return adapter(
        encode=lambda value, ctx: pack(
            "<Q", int((value.timestamp() + 11644473600) * 10000000)
        ),
        decode=lambda value, ctx: datetime.fromtimestamp(
            (unpack("<Q", value)[0] / 10000000) - 11644473600
        ),
    )(field(8, default=default))


def timedelta_field(fmt: Value[str] = "I", *, default=...):
    from datetime import timedelta

    return adapter(
        encode=lambda value, ctx: int(value.total_seconds()),
        decode=lambda value, ctx: timedelta(seconds=value),
    )(field(fmt, default=default))
