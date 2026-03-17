import ipaddress
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Optional, Union
from dataclasses import dataclass
from warnings import warn

import whodap
from tldextract.tldextract import TLDExtract

from .client import (
    ASNClient,
    DomainClient,
    NumberClient,
    convert_to_ip,
)
from .errors import NotFoundError, GeneralError, QueryError, WhoIsError

__all__ = [
    "aio_rdap",
    "aio_whois",
    "whois",
    "rdap",
    "aio_whois_domain",
    "aio_whois_ipv4",
    "aio_whois_ipv6",
    "aio_rdap_domain",
    "aio_rdap_ipv4",
    "aio_rdap_ipv6",
    "aio_rdap_asn",
    "rdap_domain",
    "rdap_ipv4",
    "rdap_ipv6",
    "rdap_asn",
    "whois_domain",
    "whois_ipv4",
    "whois_ipv6",
    "ASNClient",
    "DomainClient",
    "NumberClient",
    "NotFoundError",
    "WhoIsError",
    "GeneralError",
    "QueryError",
]
__version__ = "1.1.12"


def whois(
    search_term: Union[str, ipaddress.IPv4Address, ipaddress.IPv6Address],
    authoritative_only: bool = False,  # todo: deprecate and remove this argument
    find_authoritative_server: bool = True,
    ignore_not_found: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
    tldextract_obj: TLDExtract = None,
) -> tuple[str, dict]:
    """
    Performs a WHOIS query for the given `search_term`. If `search_term` is or can be cast to an
    instance of `ipaddress.IPv4Address` or `ipaddress.IPv6Address` then an IP search is performed
    otherwise a DNS search is performed.

    :param search_term: Any domain, URL, IPv4, or IPv6
    :param authoritative_only: DEPRECATED - If False (default), asyncwhois returns the entire WHOIS query chain,
        otherwise if True, only the authoritative response is returned.
    :param find_authoritative_server: This parameter only applies to domain queries. If True (default), asyncwhois
        will attempt to find the authoritative response, otherwise if False, asyncwhois will only query the whois server
        associated with the given TLD as specified in the IANA root db (`asyncwhois/servers/domains.py`).
    :param ignore_not_found:  If False (default), the `NotFoundError` exception is raised if the query output
        contains "no such domain" language. If True, asyncwhois will not raise `NotFoundError` exceptions.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url (e.g. 'socks5://host:port')
    :param timeout: Connection timeout. Default is 10 seconds.
    :param tldextract_obj: An optional preconfigured instance of `tldextract.TLDExtract` (used for parsing URLs)
    :returns: a tuple containing the WHOIS query text and a dictionary of key values parsed from the text
    """
    if isinstance(search_term, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return NumberClient(
            authoritative_only=authoritative_only,
            proxy_url=proxy_url,
            timeout=timeout,
        ).whois(search_term)
    elif isinstance(search_term, str):
        try:
            search_term = convert_to_ip(search_term)
            return NumberClient(
                authoritative_only=authoritative_only,
                proxy_url=proxy_url,
                timeout=timeout,
            ).whois(search_term)
        except (ipaddress.AddressValueError, ValueError):
            return DomainClient(
                authoritative_only=authoritative_only,
                find_authoritative_server=find_authoritative_server,
                ignore_not_found=ignore_not_found,
                proxy_url=proxy_url,
                timeout=timeout,
                tldextract_obj=tldextract_obj,
            ).whois(search_term)
    else:
        return "", {}


def rdap(
    search_term: Union[int, str, ipaddress.IPv4Address, ipaddress.IPv6Address],
    authoritative_only: bool = False,
    whodap_client: Union[
        whodap.DNSClient, whodap.IPv4Client, whodap.IPv6Client, whodap.ASNClient
    ] = None,
    tldextract_obj: Optional[TLDExtract] = None,
) -> tuple[str, dict]:
    """
    Performs an RDAP query for the given `search_term`. If `search_term` is or can be cast to an
    instance of `ipaddress.IPv4Address` or `ipaddress.IPv6Address` then an IP search is performed
    otherwise a DNS search is performed.

    :param search_term: Any domain, URL, IPv4, or IPv6
    :param authoritative_only: If False (default), asyncwhois returns the entire WHOIS query chain,
        otherwise if True, only the authoritative response is returned.
    :param whodap_client: An optional preconfigured instance of either an ASN, DNS, IPv4, or IPv6 `whodap` client
    :param tldextract_obj: An optional preconfigured instance of `tldextract.TLDExtract` (used for parsing URLs)
    :returns: a tuple containing the WHOIS query text and a dictionary of key values parsed from the text
    """
    if isinstance(search_term, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return NumberClient(
            authoritative_only=authoritative_only,
            whodap_client=whodap_client,
        ).rdap(search_term)
    elif isinstance(search_term, str):
        try:
            search_term = convert_to_ip(search_term)
            return NumberClient(
                authoritative_only=authoritative_only,
                whodap_client=whodap_client,
            ).rdap(search_term)
        except (ipaddress.AddressValueError, ValueError):
            return DomainClient(
                authoritative_only=authoritative_only,
                whodap_client=whodap_client,
                tldextract_obj=tldextract_obj,
            ).rdap(search_term)
    elif isinstance(search_term, int):
        return ASNClient(whodap_client=whodap_client).rdap(search_term)
    else:
        return "", {}


async def aio_whois(
    search_term: str,
    authoritative_only: bool = False,  # todo: deprecate and remove this argument
    find_authoritative_server: bool = True,
    ignore_not_found: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
    tldextract_obj: TLDExtract = None,
) -> tuple[str, dict]:
    """
    Performs a WHOIS query for the given `search_term`. If `search_term` is or can be cast to an
    instance of `ipaddress.IPv4Address` or `ipaddress.IPv6Address` then an IP search is performed
    otherwise a DNS search is performed.

    :param search_term: Any domain, URL, IPv4, or IPv6
    :param authoritative_only: DEPRECATED - If False (default), asyncwhois returns the entire WHOIS query chain,
        otherwise if True, only the authoritative response is returned.
    :param find_authoritative_server: This parameter only applies to domain queries. If True (default), asyncwhois
        will attempt to find the authoritative response, otherwise if False, asyncwhois will only query the whois server
        associated with the given TLD as specified in the IANA root db (`asyncwhois/servers/domains.py`).
    :param ignore_not_found:  If False (default), the `NotFoundError` exception is raised if the query output
        contains "no such domain" language. If True, asyncwhois will not raise `NotFoundError` exceptions.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url (e.g. 'socks5://host:port')
    :param timeout: Connection timeout. Default is 10 seconds.
    :param tldextract_obj: An optional preconfigured instance of `tldextract.TLDExtract` (used for parsing URLs)
    :returns: a tuple containing the WHOIS query text and a dictionary of key values parsed from the text
    """
    if isinstance(search_term, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return await NumberClient(
            authoritative_only=authoritative_only,
            proxy_url=proxy_url,
            timeout=timeout,
        ).aio_whois(search_term)
    elif isinstance(search_term, str):
        try:
            search_term = convert_to_ip(search_term)
            return await NumberClient(
                authoritative_only=authoritative_only,
                proxy_url=proxy_url,
                timeout=timeout,
            ).aio_whois(search_term)
        except (ipaddress.AddressValueError, ValueError):
            return await DomainClient(
                authoritative_only=authoritative_only,
                ignore_not_found=ignore_not_found,
                proxy_url=proxy_url,
                timeout=timeout,
                tldextract_obj=tldextract_obj,
                find_authoritative_server=find_authoritative_server,
            ).aio_whois(search_term)
    else:
        return "", {}


async def aio_rdap(
    search_term: Union[int, str, ipaddress.IPv4Address, ipaddress.IPv6Address],
    authoritative_only: bool = False,
    whodap_client: Union[
        whodap.DNSClient, whodap.IPv4Client, whodap.IPv6Client, whodap.ASNClient
    ] = None,
    tldextract_obj: Optional[TLDExtract] = None,
) -> tuple[str, dict]:
    """
    Performs an RDAP query for the given `search_term`. If `search_term` is or can be cast to an
    instance of `ipaddress.IPv4Address` or `ipaddress.IPv6Address` then an IP search is performed
    otherwise a DNS search is performed.

    :param search_term: Any domain, URL, IPv4, or IPv6
    :param authoritative_only: If False (default), asyncwhois returns the entire WHOIS query chain,
        otherwise if True, only the authoritative response is returned.
    :param whodap_client: An optional preconfigured instance of either an ASN, DNS, IPv4, or IPv6 `whodap` client
    :param tldextract_obj: An optional preconfigured instance of `tldextract.TLDExtract` (used for parsing URLs)
    :returns: a tuple containing the WHOIS query text and a dictionary of key values parsed from the text
    """
    if isinstance(search_term, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return await NumberClient(
            authoritative_only=authoritative_only,
            whodap_client=whodap_client,
        ).aio_rdap(search_term)
    elif isinstance(search_term, str):
        try:
            search_term = convert_to_ip(search_term)
            return await NumberClient(
                authoritative_only=authoritative_only,
                whodap_client=whodap_client,
            ).aio_rdap(search_term)
        except (ipaddress.AddressValueError, ValueError):
            return await DomainClient(
                authoritative_only=authoritative_only,
                whodap_client=whodap_client,
                tldextract_obj=tldextract_obj,
            ).aio_rdap(search_term)
    elif isinstance(search_term, int):
        return await ASNClient(whodap_client=whodap_client).aio_rdap(search_term)
    else:
        return "", {}


# ====================
# TODO: ALL the code below will be removed in a future release; it is here for backwards compatibility only


@dataclass
class DomainLookup:
    query_output: str
    parser_output: dict


@dataclass
class NumberLookup:
    query_output: str
    parser_output: dict


@dataclass
class ASNLookup:
    query_output: str
    parser_output: dict


def whois_domain(
    domain: str,
    authoritative_only: bool = False,
    ignore_not_found: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
    tldextract_obj: TLDExtract = None,
) -> DomainLookup:
    """
    Performs domain lookups with WHOIS.
    Finds the authoritative WHOIS server and parses the response from the server.

    :param domain: Any domain or URL (e.g. 'wikipedia.org' or 'https://en.wikipedia.org/wiki/WHOIS')
    :param authoritative_only: If False (default), asyncwhois returns the entire WHOIS query chain
        in `query_output`; If True only the authoritative response is included.
    :param ignore_not_found:  If False (default), the `NotFoundError` exception is raised if the `query_output`
        contains "no such domain" language. If True, asyncwhois will not raise `NotFoundError` exceptions.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url (e.g. 'socks5://host:port')
    :param timeout: Connection timeout. Default is 10 seconds.
    :param tldextract_obj: An optional preconfigured instance of `tldextract.tldextract.TLDExtract`
    :return: instance of DomainLookup
    """
    warn(
        "`asyncwhois.whois_domain` is deprecated. Please use `asyncwhois.whois` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    query_output, parser_output = DomainClient(
        authoritative_only=authoritative_only,
        ignore_not_found=ignore_not_found,
        proxy_url=proxy_url,
        timeout=timeout,
        tldextract_obj=tldextract_obj,
    ).whois(domain)
    return DomainLookup(query_output, parser_output)


async def aio_whois_domain(
    domain: str,
    authoritative_only: bool = False,
    ignore_not_found: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
    tldextract_obj: TLDExtract = None,
) -> DomainLookup:
    """
    Performs asynchronous domain lookups with WHOIS.
    Finds the authoritative WHOIS server and parses the response from the server.

    :param domain: Any domain or URL (e.g. 'wikipedia.org' or 'https://en.wikipedia.org/wiki/WHOIS')
    :param authoritative_only: If False (default), asyncwhois returns the entire WHOIS query chain
        in `query_output`; If True only the authoritative response is included.
    :param ignore_not_found:  If False (default), the `NotFoundError` exception is raised if the `query_output`
        contains "no such domain" language. If True, asyncwhois will not raise `NotFoundError` exceptions.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url (e.g. 'socks5://host:port')
    :param timeout: Connection timeout. Default is 10 seconds.
    :param tldextract_obj: An optional preconfigured instance of `tldextract.tldextract.TLDExtract`
    :return: instance of DomainLookup
    """
    warn(
        "`asyncwhois.aio_whois_domain` is deprecated. Please use `asyncwhois.aio_whois` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    query_output, parser_output = await DomainClient(
        authoritative_only=authoritative_only,
        ignore_not_found=ignore_not_found,
        proxy_url=proxy_url,
        timeout=timeout,
        tldextract_obj=tldextract_obj,
    ).aio_whois(domain)
    return DomainLookup(query_output, parser_output)


def rdap_domain(
    domain: str,
    httpx_client: Optional[Any] = None,
    tldextract_obj: Optional[TLDExtract] = None,
) -> DomainLookup:
    """
    Performs an RDAP query for the given domain.
    Finds the authoritative RDAP server and parses the response from that server.

    :param domain: Any domain name or URL
        (e.g. 'wikipedia.org' or 'https://en.wikipedia.org/wiki/WHOIS')
    :param httpx_client: Optional preconfigured instance of `httpx.AsyncClient`
    :param tldextract_obj: Optional preconfigured instance of `tldextract.tldextract.TLDExtract`
    :return: instance of DomainLookup
    """
    warn(
        "`asyncwhois.rdap_domain` is deprecated. Please use `asyncwhois.aio_rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.DNSClient.new_client(httpx_client=httpx_client)
    query_output, parser_output = DomainClient(
        whodap_client=whodap_client,
        tldextract_obj=tldextract_obj,
    ).rdap(domain)
    return DomainLookup(query_output, parser_output)


async def aio_rdap_domain(
    domain: str,
    httpx_client: Optional[Any] = None,
    tldextract_obj: Optional[TLDExtract] = None,
) -> DomainLookup:
    """
    Performs an async RDAP query for the given domain name.

    :param domain: Any domain or URL (e.g. 'wikipedia.org' or 'https://en.wikipedia.org/wiki/WHOIS')
    :param httpx_client: Optional preconfigured instance of `httpx.AsyncClient`
    :param tldextract_obj: Optional preconfigured instance of `tldextract.tldextract.TLDExtract`
    :return: instance of DomainLookup
    """
    warn(
        "`asyncwhois.aio_rdap_domain` is deprecated. Please use `asyncwhois.aio_rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.DNSClient.new_aio_client(httpx_client=httpx_client)
    query_output, parser_output = await DomainClient(
        whodap_client=whodap_client,
        tldextract_obj=tldextract_obj,
    ).aio_rdap(domain)
    return DomainLookup(query_output, parser_output)


def whois_ipv4(
    ipv4: Union[IPv4Address, str],
    authoritative_only: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
) -> NumberLookup:
    """
    Performs a WHOIS query for the given IPv4 address.
    Finds the authoritative WHOIS server and parses the response from the server.

    :param ipv4: ip address as a str or `ipaddress.IPv4Address` object
    :param authoritative_only: If False, returns the entire WHOIS query chain
        in `query_output`; If True only the authoritative response is included.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url
    :param timeout: Connection timeout. Default is 10 seconds.
    :return: instance of DomainLookup
    """
    warn(
        "`asyncwhois.whois_ipv4` is deprecated. Please use `asyncwhois.whois` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    query_output, parser_output = NumberClient(
        authoritative_only=authoritative_only,
        proxy_url=proxy_url,
        timeout=timeout,
    ).whois(ipv4)
    return NumberLookup(query_output, parser_output)


async def aio_whois_ipv4(
    ipv4: Union[IPv4Address, str],
    authoritative_only: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
) -> NumberLookup:
    """
    Performs an async WHOIS query for the given IPv4 address.
    Finds the authoritative WHOIS server and parses the response from the server.

    :param ipv4: ip address as a str or `ipaddress.IPv4Address` object
    :param authoritative_only: If False, returns the entire WHOIS query chain
        in `query_output`; If True only the authoritative response is included.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url
    :param timeout: Connection timeout. Default is 10 seconds.
    :return: instance of NumberLookup
    """
    warn(
        "`asyncwhois.aio_whois_ipv4` is deprecated. Please use `asyncwhois.aio_whois` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    query_output, parser_output = await NumberClient(
        authoritative_only=authoritative_only,
        proxy_url=proxy_url,
        timeout=timeout,
    ).aio_whois(ipv4)
    return NumberLookup(query_output, parser_output)


def rdap_ipv4(
    ipv4: Union[IPv4Address, str], httpx_client: Optional[Any] = None
) -> NumberLookup:
    """
    Performs an RDAP query for the given IPv4 address.

    :param ipv4: IP address as a string or `ipaddress.IPv4Address` object
    :param httpx_client: Optional preconfigured `httpx.Client`
    :return: instance of NumberLookup
    """
    warn(
        "`asyncwhois.rdap_ipv4` is deprecated. Please use `asyncwhois.rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.IPv4Client.new_client(httpx_client=httpx_client)
    query_output, parser_output = NumberClient(
        whodap_client=whodap_client,
    ).rdap(ipv4)
    return NumberLookup(query_output, parser_output)


async def aio_rdap_ipv4(
    ipv4: Union[IPv4Address, str], httpx_client: Optional[Any] = None
) -> NumberLookup:
    """
    Performs an async RDAP query for the given IPv6 address.

    :param ipv4: IP address as a string or `ipaddress.IPv4Address` object
    :param httpx_client: Optional preconfigured `httpx.AsyncClient`
    :return: instance of NumberLookup
    """
    warn(
        "`asyncwhois.aio_rdap_ipv4` is deprecated. Please use `asyncwhois.aio_rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.IPv4Client.new_client(httpx_client=httpx_client)
    query_output, parser_output = NumberClient(
        whodap_client=whodap_client,
    ).aio_rdap(ipv4)
    return NumberLookup(query_output, parser_output)


def whois_ipv6(
    ipv6: Union[IPv6Address, str],
    authoritative_only: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
) -> NumberLookup:
    """
    Performs a WHOIS query for the given IPv6 address.
    Looks up the WHOIS server, submits the query, and then parses the response from the server.

    :param ipv6: ip address as a str or `ipaddress.IPv6address` object
    :param authoritative_only: If False, returns the entire WHOIS query chain
        in `query_output`; If True only the authoritative response is included.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url
    :param timeout: Connection timeout. Default is 10 seconds.
    :return: instance of NumberLookup
    """
    warn(
        "`asyncwhois.whois_ipv6` is deprecated. Please use `asyncwhois.whois` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    query_output, parser_output = NumberClient(
        authoritative_only=authoritative_only,
        proxy_url=proxy_url,
        timeout=timeout,
    ).whois(ipv6)
    return NumberLookup(query_output, parser_output)


async def aio_whois_ipv6(
    ipv6: Union[IPv6Address, str],
    authoritative_only: bool = False,
    proxy_url: Optional[str] = None,
    timeout: int = 10,
) -> NumberLookup:
    """
    Performs an async WHOIS query for the given IPv6 address.
    Looks up the WHOIS server, submits the query, and then parses the response from the server.

    :param ipv6: ip address as a str or `ipaddress.IPv6Address` object
    :param authoritative_only: If False, returns the entire WHOIS query chain
        in `query_output`; If True only the authoritative response is included.
    :param proxy_url: Optional SOCKS4 or SOCKS5 proxy url
    :param timeout: Connection timeout. Default is 10 seconds.
    :return: instance of NumberLookup
    """
    warn(
        "`asyncwhois.aio_whois_ipv6` is deprecated. Please use `asyncwhois.aio_whois` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    query_output, parser_output = await NumberClient(
        authoritative_only=authoritative_only,
        proxy_url=proxy_url,
        timeout=timeout,
    ).aio_whois(ipv6)
    return NumberLookup(query_output, parser_output)


def rdap_ipv6(
    ipv6: Union[IPv6Address, str], httpx_client: Optional[Any] = None
) -> NumberLookup:
    """
    Performs an RDAP query for the given IPv6 address.

    :param ipv6: IP address as a string or `ipaddress.IPv6Address` object
    :param httpx_client: Optional preconfigured `httpx.Client`
    :return: instance of NumberLookup
    """
    warn(
        "`asyncwhois.rdap_ipv6` is deprecated. Please use `asyncwhois.rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.IPv6Client.new_client(httpx_client=httpx_client)
    query_output, parser_output = NumberClient(
        whodap_client=whodap_client,
    ).rdap(ipv6)
    return NumberLookup(query_output, parser_output)


async def aio_rdap_ipv6(
    ipv6: Union[IPv6Address, str], httpx_client: Optional[Any] = None
) -> NumberLookup:
    """
    Performs an async RDAP query for the given IPv6 address.

    :param ipv6: IP address as a string or `ipaddress.IPv6Address` object
    :param httpx_client: Optional preconfigured `httpx.AsyncClient`
    :return: instance of NumberLookup
    """
    warn(
        "`asyncwhois.aio_rdap_ipv6` is deprecated. Please use `asyncwhois.aio_rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.IPv6Client.new_aio_client(httpx_client=httpx_client)
    query_output, parser_output = NumberClient(
        whodap_client=whodap_client,
    ).aio_rdap(ipv6)
    return NumberLookup(query_output, parser_output)


def rdap_asn(asn: int, httpx_client: Optional[Any] = None) -> ASNLookup:
    """
    Performs an RDAP query for the given Autonomous System Number.

    :param asn: The ASN number as an integer
    :param httpx_client: Optional preconfigured `httpx.Client`
    :return: instance of ASNLookup
    """
    warn(
        "`asyncwhois.rdap_asn` is deprecated. Please use `asyncwhois.rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.IPv6Client.new_client(httpx_client=httpx_client)
    query_output, parser_output = ASNClient(
        whodap_client=whodap_client,
    ).rdap(asn)
    return ASNLookup(query_output, parser_output)


async def aio_rdap_asn(asn: int, httpx_client: Optional[Any] = None) -> ASNLookup:
    """
    Performs an async RDAP query for the given Autonomous System Number.

    :param asn: The ASN number as an integer
    :param httpx_client: Optional preconfigured `httpx.AsyncClient`
    :return: instance of ASNLookup
    """
    warn(
        "`asyncwhois.aio_rdap_asn` is deprecated. Please use `asyncwhois.aio_rdap` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    whodap_client = None
    if httpx_client is not None:
        whodap_client = whodap.IPv6Client.new_aio_client(httpx_client=httpx_client)
    query_output, parser_output = ASNClient(
        whodap_client=whodap_client,
    ).aio_rdap(asn)
    return ASNLookup(query_output, parser_output)
