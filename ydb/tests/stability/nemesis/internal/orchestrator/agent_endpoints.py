"""Resolve agent FQDNs to IPs once so orchestrator→agent HTTP survives DNS chaos.

Logical host names (cluster.yaml) stay the identity for chaos targets / UI; only the
HTTP transport uses the cached address.
"""

from __future__ import annotations

import logging
import socket

logger = logging.getLogger(__name__)


def format_http_host(addr: str) -> str:
    """Bracket IPv6 literals for use in ``http://…`` URLs."""
    if ":" in addr and not addr.startswith("["):
        return f"[{addr}]"
    return addr


def resolve_agent_endpoints(hosts: list[str]) -> dict[str, str]:
    """Map each logical host name to an HTTP host (prefer IPv6, then IPv4).

    Unresolvable names keep the original hostname so callers still have a fallback.
    """
    out: dict[str, str] = {}
    for host in hosts:
        if not host:
            continue
        try:
            infos = socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
        except socket.gaierror as e:
            logger.warning("agent endpoint: could not resolve %s (%s); keeping hostname", host, e)
            out[host] = host
            continue
        if not infos:
            out[host] = host
            continue
        # Prefer IPv6: YDB interconnect / testing hosts are often v6-first.
        infos = sorted(infos, key=lambda info: 0 if info[0] == socket.AF_INET6 else 1)
        addr = infos[0][4][0]
        out[host] = format_http_host(addr)
        logger.info("agent endpoint: %s -> %s", host, out[host])
    return out
