import socket

from typing import Tuple
from typing import Optional
from typing import Any

from . import raise_validator
from . import not_none_string
from . import check_any
from . import check_re_match
from . import add_validator_magic


# =====
@add_validator_magic
def valid_ip_or_host(
    arg: Any,
    strip: bool=False,
) -> Tuple[str, Optional[socket.AddressFamily]]:  # pylint: disable=no-member

    name = "IPv4/IPv6 address or RFC-1123 hostname"
    return check_any(
        arg=not_none_string(arg, name, strip),
        name=name,
        validators=[
            valid_ip,
            lambda arg: (valid_rfc_host(arg), None),
        ],
    )


@add_validator_magic
def valid_ip(
    arg: Any,
    strip: bool=False,
) -> Tuple[str, socket.AddressFamily]:  # pylint: disable=no-member

    name = "IPv4/6 address"
    return check_any(
        arg=not_none_string(arg, name, strip),
        name=name,
        validators=[
            lambda arg: ((arg, socket.inet_pton(socket.AF_INET, arg))[0], socket.AF_INET),
            lambda arg: ((arg, socket.inet_pton(socket.AF_INET6, arg))[0], socket.AF_INET6),
        ],
    )


@add_validator_magic
def valid_rfc_host(
    arg: Any,
    strip: bool=False,
) -> str:

    # http://stackoverflow.com/questions/106179/regular-expression-to-match-hostname-or-ip-address
    pattern = r"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*" \
              r"([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$"
    return check_re_match(arg, "RFC-1123 hostname", pattern, strip)


@add_validator_magic
def valid_port(
    arg: Any,
    strip: bool=False,
) -> int:

    name = "TCP/UDP portnumber"
    arg = not_none_string(arg, name, strip)
    try:
        if not (0 <= int(arg) < 65536):
            raise ValueError()
        return int(arg)
    except Exception:
        raise_validator(arg, name)
