from __future__ import annotations

import os
import re
from pathlib import PurePath
from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from upath.core import UPath

__all__ = [
    "get_upath_protocol",
    "normalize_empty_netloc",
    "compatible_protocol",
]

# Regular expression to match fsspec style protocols.
# Matches single slash usage too for compatibility.
_PROTOCOL_RE = re.compile(
    r"^(?P<protocol>[A-Za-z][A-Za-z0-9+]+):(?P<slashes>//?)(?P<path>.*)"
)

# Matches data URIs
_DATA_URI_RE = re.compile(r"^data:[^,]*,")


def _match_protocol(pth: str) -> str:
    if m := _PROTOCOL_RE.match(pth):
        return m.group("protocol")
    elif _DATA_URI_RE.match(pth):
        return "data"
    return ""


def get_upath_protocol(
    pth: str | PurePath | os.PathLike,
    *,
    protocol: str | None = None,
    storage_options: dict[str, Any] | None = None,
) -> str:
    """return the filesystem spec protocol"""
    if isinstance(pth, str):
        pth_protocol = _match_protocol(pth)
    elif isinstance(pth, PurePath):
        pth_protocol = getattr(pth, "protocol", "")
    elif hasattr(pth, "__fspath__"):
        pth_protocol = _match_protocol(pth.__fspath__())
    else:
        pth_protocol = _match_protocol(str(pth))
    # if storage_options and not protocol and not pth_protocol:
    #     protocol = "file"
    if protocol and pth_protocol and not pth_protocol.startswith(protocol):
        raise ValueError(
            f"requested protocol {protocol!r} incompatible with {pth_protocol!r}"
        )
    return protocol or pth_protocol or ""


def normalize_empty_netloc(pth: str) -> str:
    if m := _PROTOCOL_RE.match(pth):
        if len(m.group("slashes")) == 1:
            protocol = m.group("protocol")
            path = m.group("path")
            pth = f"{protocol}:///{path}"
    return pth


def compatible_protocol(protocol: str, *args: str | os.PathLike[str] | UPath) -> bool:
    """check if UPath protocols are compatible"""
    for arg in args:
        other_protocol = get_upath_protocol(arg)
        # consider protocols equivalent if they match up to the first "+"
        other_protocol = other_protocol.partition("+")[0]
        # protocols: only identical (or empty "") protocols can combine
        if other_protocol and other_protocol != protocol:
            return False
    return True
