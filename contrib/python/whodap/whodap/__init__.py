import ipaddress
from typing import Optional, Union

from httpx import Client, AsyncClient

from .client import DNSClient, IPv4Client, IPv6Client, ASNClient
from .response import DomainResponse, IPv4Response, IPv6Response, ASNResponse

__all__ = [
    "lookup_domain",
    "lookup_ipv4",
    "lookup_ipv6",
    "lookup_asn",
    "aio_lookup_domain",
    "aio_lookup_ipv4",
    "aio_lookup_ipv6",
    "aio_lookup_asn",
    "DNSClient",
    "IPv4Client",
    "IPv6Client",
    "ASNClient",
]
__version__ = "0.1.14"


def lookup_domain(
    domain: str, tld: str, httpx_client: Optional[Client] = None
) -> DomainResponse:
    """
    Convenience function that instantiates a DNSClient,
    submits an RDAP query for the given domain, and returns
    the result as a DomainResponse.

    :param domain: the domain name to query
    :param tld: the top level domain (e.g. "com", "net", "buzz")
    :param httpx_client: Optional preconfigured instance of `httpx.Client`
    :return: an instance of DomainResponse
    """
    dns_client = DNSClient.new_client(httpx_client)
    try:
        resp = dns_client.lookup(domain, tld)
    finally:
        # close the default client created by DNSClient if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            dns_client.close()
    return resp


async def aio_lookup_domain(
    domain: str, tld: str, httpx_client: Optional[AsyncClient] = None
) -> DomainResponse:
    """
    Async-compatible convenience function that instantiates
    a DNSClient, submits an RDAP query for the given domain,
    and returns the result as a DomainResponse.

    :param domain: the domain name to query
    :param tld: the top level domain (e.g. "com", "net", "buzz")
    :param httpx_client: Optional preconfigured instance of `httpx.AsyncClient`
    :return: an instance of DomainResponse
    """
    dns_client = await DNSClient.new_aio_client(httpx_client)
    try:
        resp = await dns_client.aio_lookup(domain, tld)
    finally:
        # close the default client created by DNSClient if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            await dns_client.aio_close()
    return resp


def lookup_ipv4(
    ipv4: Union[str, ipaddress.IPv4Address], httpx_client: Optional[Client] = None
) -> IPv4Response:
    """
    Convenience function that instantiates an IPv4Client,
    submits an RDAP query for the given IP address, and
    returns the result as an IPv4Response.

    :param ipv4: The ipv4 string or ipaddress.IPv4address object
    :param httpx_client: Optional preconfigured instance of `httpx.Client`
    :return: an instance of IPv4Response
    """
    ipv4_client = IPv4Client.new_client(httpx_client)
    try:
        resp = ipv4_client.lookup(ipv4)
    finally:
        # close the default client created by IPv4Client if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            ipv4_client.close()
    return resp


async def aio_lookup_ipv4(
    ipv4: Union[str, ipaddress.IPv4Address], httpx_client: Optional[AsyncClient] = None
) -> IPv4Response:
    """
    Convenience function that instantiates an IPv4Client,
    submits an RDAP query for the given IP address, and
    returns the result as an IPv4Response.

    :param ipv4: The ipv4 string or ipaddress.IPv4address object
    :param httpx_client: Optional preconfigured instance of `httpx.AsyncClient`
    :return: an instance of IPv4Response
    """
    ipv4_client = await IPv4Client.new_aio_client(httpx_client)
    try:
        resp = await ipv4_client.aio_lookup(ipv4)
    finally:
        # close the default client created by IPv4Client if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            await ipv4_client.aio_close()
    return resp


def lookup_ipv6(
    ipv6: Union[str, ipaddress.IPv6Address], httpx_client: Optional[Client] = None
) -> IPv6Response:
    """
    Convenience function that instantiates an IPv6Client,
    submits an RDAP query for the given IP address, and
    returns the result as a DomainResponse.

    :param ipv6: The ipv6 string or ipaddress.IPv6address object
    :param httpx_client: Optional preconfigured instance of `httpx.Client`
    :return: an instance of IPv6Response
    """
    ipv6_client = IPv6Client.new_client(httpx_client)
    try:
        resp = ipv6_client.lookup(ipv6)
    finally:
        # close the default client created by IPv6Client if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            ipv6_client.close()
    return resp


async def aio_lookup_ipv6(
    ipv6: Union[str, ipaddress.IPv6Address], httpx_client: Optional[AsyncClient] = None
) -> IPv6Response:
    """
    Convenience function that instantiates an IPv6Client,
    submits an RDAP query for the given IP address, and
    returns the result as a DomainResponse.

    :param ipv6: The ipv6 string or ipaddress.IPv6address object
    :param httpx_client: Optional preconfigured instance of `httpx.AsyncClient`
    :return: an instance of IPv6Response
    """
    ipv6_client = await IPv6Client.new_aio_client(httpx_client)
    try:
        resp = await ipv6_client.aio_lookup(ipv6)
    finally:
        # close the default client created by IPv6Client if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            await ipv6_client.aio_close()
    return resp


def lookup_asn(asn: int, httpx_client: Optional[Client] = None) -> ASNResponse:
    """
    Convenience function that instantiates an ASNClient,
    submits an RDAP query for the given ASN, and returns
    the result as an ASNResponse.

    :param asn: The asn number to lookup
    :param httpx_client: Optional preconfigured instance of `httpx.Client`
    :return: an instance of ASNResponse
    """
    asn_client = ASNClient.new_client(httpx_client)
    try:
        resp = asn_client.lookup(asn)
    finally:
        # close the default client created by ASNClient if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            asn_client.close()
    return resp


async def aio_lookup_asn(
    asn: int, httpx_client: Optional[AsyncClient] = None
) -> ASNResponse:
    """
    Convenience function that instantiates an ASNClient,
    submits an RDAP query for the given ASN, and returns
    the result as an ASNResponse.

    :param asn: The asn number to lookup
    :param httpx_client: Optional preconfigured instance of `httpx.AsyncClient`
    :return: an instance of ASNResponse
    """
    asn_client = await ASNClient.new_aio_client(httpx_client)
    try:
        resp = await asn_client.aio_lookup(asn)
    finally:
        # close the default client created by ASNClient if it exists;
        # otherwise it's up to the user to close their `httpx_client`
        if not httpx_client:
            await asn_client.aio_close()
    return resp
