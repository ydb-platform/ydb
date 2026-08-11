"""Shared endpoint parsing used by both tracing and metrics attributes."""

from typing import Optional, Tuple


def split_endpoint(endpoint: Optional[str]) -> Tuple[str, int]:
    ep = endpoint or ""
    if ep.startswith("grpcs://"):
        ep = ep[len("grpcs://") :]
    elif ep.startswith("grpc://"):
        ep = ep[len("grpc://") :]

    if ep.startswith("["):
        close = ep.find("]")
        if close != -1 and len(ep) > close + 1 and ep[close + 1] == ":":
            host = ep[: close + 1]
            port_s = ep[close + 2 :]
            return host, int(port_s) if port_s.isdigit() else 0

    host, sep, port_s = ep.rpartition(":")
    if not sep:
        return ep, 0
    return host, int(port_s) if port_s.isdigit() else 0
