#  Copyright (c) Kuba Szczodrzyński 2023-9-11.

from datastruct.fields import adapter, field


def ipv4_field(*, default=...):
    from ipaddress import ip_address

    return adapter(
        encode=lambda value, ctx: value.packed,
        decode=lambda value, ctx: ip_address(value),
    )(field(4, default=default))


def ipv6_field(*, default=...):
    from ipaddress import ip_address

    return adapter(
        encode=lambda value, ctx: value.packed,
        decode=lambda value, ctx: ip_address(value),
    )(field(16, default=default))


def mac_field(*, default=...):
    from macaddress import MAC

    return adapter(
        encode=lambda value, ctx: bytes(value),
        decode=lambda value, ctx: MAC(value),
    )(field(6, default=default))
