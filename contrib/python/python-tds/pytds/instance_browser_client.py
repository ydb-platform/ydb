"""
This module implements client interface for MSSQL server browser which provides
information about MSSQL server instances running on the host via UDP socket at port 1434.
"""
from __future__ import annotations
import socket
import typing
from . import tds_base
from .tds_base import logger


def parse_instances_response(msg: bytes) -> dict[str, dict[str, str]] | None:
    """
    Parses instances response as received from MSSQL server browser endpoint
    """
    name: str | None = None
    if len(msg) > 3 and tds_base.my_ord(msg[0]) == 5:
        tokens = msg[3:].decode("ascii").split(";")
        results: dict[str, dict[str, str]] = {}
        instdict: dict[str, str] = {}
        got_name = False
        for token in tokens:
            if got_name and name:
                instdict[name] = token
                got_name = False
            else:
                name = token
                if not name:
                    if not instdict:
                        break
                    results[instdict["InstanceName"].upper()] = instdict
                    instdict = {}
                    continue
                got_name = True
        return results
    return None


def tds7_get_instances(
    ip_addr: typing.Any, timeout: float = 5
) -> dict[str, dict[str, str]] | None:
    """
    Get MSSQL instances information from instance browser service endpoint.
    Returns a dictionary keyed by instance name of dictionaries of instances information.
    """
    with socket.socket(type=socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        # send the request
        s.sendto(b"\x03", (ip_addr, 1434))
        msg = s.recv(16 * 1024 - 1)
        # got data, read and parse
        return parse_instances_response(msg)


def resolve_instance_port(
    server: typing.Any, port: int | None, instance: str, timeout: float = 5
) -> int:
    """
    Resolve MSSQL server instance's port, if instance name is provided and port not provided
    """
    if instance and not port:
        logger.info("querying %s for list of instances", server)
        instances = tds7_get_instances(server, timeout=timeout)
        if not instances:
            raise RuntimeError(
                "Querying list of instances failed, returned value has invalid format"
            )
        if instance not in instances:
            raise tds_base.LoginError(
                f"Instance {instance} not found on server {server}"
            )
        instdict = instances[instance]
        if "tcp" not in instdict:
            raise tds_base.LoginError(
                f"Instance {instance} doen't have tcp connections enabled"
            )
        port = int(instdict["tcp"])
    return port or 1433
