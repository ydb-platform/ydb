import sys
import posixpath
import ipaddress
from typing import Dict, Any, Union, Optional, List
from contextlib import contextmanager
from json import JSONDecodeError

# different installs for async contextmanager based on python version
if sys.version_info < (3, 7):
    from async_generator import asynccontextmanager
else:
    from contextlib import asynccontextmanager

import httpx

from .codes import RDAPStatusCodes
from .errors import (
    RateLimitError,
    NotFoundError,
    MalformedQueryError,
    BadStatusCode,
    WhodapError,
)
from .response import DomainResponse, IPv4Response, IPv6Response, ASNResponse


class RDAPClient:
    """
    Base class for RDAP Clients.
    Abstracts HTTP helper functions and context managers.
    """

    _iana_publication_key: str = "publication"
    _iana_version_key: str = "version"
    _iana_services_key: str = "services"
    _iana_uri: str = None

    def __init__(self, httpx_client: Union[httpx.Client, httpx.AsyncClient]):
        self.httpx_client = httpx_client
        self.version: str = ""
        self.publication: str = ""
        self._target: Union[str, int, ipaddress.IPv4Address, ipaddress.IPv6Address] = ""

    def lookup(self, *args, **kwargs):
        """
        Subclasses implement methods for queries.
        """
        ...

    async def aio_lookup(self, *args, **kwargs):
        """
        Subclasses implement methods for queries.
        """
        ...

    @staticmethod
    def _build_query_href(rdap_href: str, target: str) -> str:
        """
        Subclasses implement logic for building HREFs.
        """
        ...

    def _set_iana_info(self, iana_resp: Dict[str, Any]) -> None:
        """
        Subclasses implement logic for parsing and storing
        the JSON response from IANA.
        """
        ...

    def close(self):
        """
        Closes the underlying `httpx.Client`
        """
        if not self.httpx_client.is_closed:
            self.httpx_client.close()

    async def aio_close(self):
        """
        Closes the underlying `httpx.AsyncClient`
        """
        if not self.httpx_client.is_closed:
            await self.httpx_client.aclose()

    @classmethod
    @contextmanager
    def new_client_context(cls, httpx_client: Optional[httpx.Client] = None):
        """
        Contextmanager for instantiating a Synchronous DNSClient

        :httpx_client: pre-configured instance of `httpx.Client`
        :return: yields the initialized DNSClient
        """
        client = cls(httpx_client or httpx.Client(follow_redirects=True, timeout=10))
        try:
            iana_dns_info = client._get_iana_info()
            client._set_iana_info(iana_dns_info)
            yield client
        finally:
            if not client.httpx_client.is_closed:
                client.httpx_client.close()

    @classmethod
    def new_client(cls, httpx_client: Optional[httpx.Client] = None):
        """
        Classmethod for instantiating a synchronous instance of Client

        :httpx_client: pre-configured instance of `httpx.Client`
        :return: DNSClient with a sync httpx_client
        """
        # init the client with a default httpx.Client if one is not provided
        client = cls(httpx_client or httpx.Client(follow_redirects=True, timeout=10))
        # load the dns server information from IANA
        iana_info = client._get_iana_info()
        # parse and save the server information
        client._set_iana_info(iana_info)
        # return the loaded client
        return client

    @classmethod
    @asynccontextmanager
    async def new_aio_client_context(
        cls, httpx_client: Optional[httpx.AsyncClient] = None
    ):
        """
        Contextmanager for instantiating an Asynchronous DNSClient

        :httpx_client: Optional pre-configured instance of `httpx.AsyncClient`
        :return: yields the initialized DNSClient
        """
        client = cls(
            httpx_client or httpx.AsyncClient(follow_redirects=True, timeout=10)
        )
        try:
            iana_info = await client._aio_get_iana_info()
            client._set_iana_info(iana_info)
            yield client
        finally:
            if not client.httpx_client.is_closed:
                await client.httpx_client.aclose()

    @classmethod
    async def new_aio_client(cls, httpx_client: Optional[httpx.AsyncClient] = None):
        """
        Classmethod for instantiating an asynchronous instance of DNSClient

        :httpx_client: pre-configured instance of `httpx.AsyncClient`
        :return: DNSClient with an async httpx_client
        """
        client = cls(
            httpx_client or httpx.AsyncClient(follow_redirects=True, timeout=10)
        )
        iana_info = await client._aio_get_iana_info()
        client._set_iana_info(iana_info)
        return client

    def _get_iana_info(self):
        """
        Retrieves the JSON payload from IANA.
        Each Subclass of RDAPClient implements its own `iana_url`

        :return: JSON data dictionary
        """
        response = self._get_request(self._iana_uri)
        return response.json()

    async def _aio_get_iana_info(self):
        """
        Retrieves the JSON payload from IANA.
        Each Subclass of RDAPClient implements its own `iana_url`

        :return: JSON data dictionary
        """
        response = await self._aio_get_request(self._iana_uri)
        return response.json()

    def _get_request(self, uri: str) -> httpx.Response:
        return self.httpx_client.get(uri)

    async def _aio_get_request(self, uri: str) -> httpx.Response:
        return await self.httpx_client.get(uri)

    def _get_authoritative_response(
        self, href: str, seen: List[str], depth: int = 0
    ) -> Optional[httpx.Response]:
        """
        Makes HTTP calls to RDAP servers until it finds
        the authoritative source.

        :param href: href containing the location of an RDAP
        :param depth: recursion counter
        :return: `httpx` response object
        """
        resp = self._get_request(href)
        try:
            self._check_status_code(resp.status_code)
        except NotFoundError:
            # Occasionally, gTLD RDAP servers are wrong or not fully implemented.
            # If we've succeeded previously, but now the href returns a 404,
            # return None, so the last OK response is returned.
            if depth != 0:
                return None
            else:
                raise
        # check for more authoritative source
        try:
            # If for some reason the response is invalid json, then just return None.
            # This may happen if we request an authoritative href that is not actually
            # rdap+json, but instead a webpage/html.
            rdap_json = resp.json()
        except JSONDecodeError:
            return None
        links = rdap_json.get("links")
        if links:
            next_href = self._check_next_href(href, links)
            if next_href and next_href not in seen:
                seen.append(next_href)
                resp = (
                    self._get_authoritative_response(next_href, seen, depth + 1) or resp
                )
        # return authoritative response
        return resp

    async def _aio_get_authoritative_response(
        self, href: str, seen: List[str], depth: int = 0
    ) -> Optional[httpx.Response]:
        """
        Makes HTTP calls to RDAP servers until it finds
        the authoritative source.

        :param href: href containing the location of an RDAP
        :param depth: recursion counter
        :return: `httpx` response object
        """
        resp = await self._aio_get_request(href)
        try:
            self._check_status_code(resp.status_code)
        except NotFoundError:
            # Occasionally, gTLD RDAP servers are wrong or not implemented.
            # If we've succeeded previously, but now the href returns a 404,
            # return None, so the last OK response is returned.
            if depth != 0:
                return None
            else:
                raise
        try:
            # If for some reason the response is invalid json, then just return None.
            # This may happen if we request an authoritative href that is not actually
            # rdap+json, but instead a webpage/html.
            rdap_json = resp.json()
        except JSONDecodeError:
            return None
        links = rdap_json.get("links")
        if links:
            next_href = self._check_next_href(href, links)
            if next_href and next_href not in seen:
                seen.append(next_href)
                resp = (
                    await self._aio_get_authoritative_response(
                        next_href, seen, depth + 1
                    )
                    or resp
                )
        return resp

    def _check_next_href(
        self, current_href: str, links: List[Dict[str, str]]
    ) -> Optional[str]:
        # RFC: https://datatracker.ietf.org/doc/html/rfc9083#section-4.2
        # find next href or return None
        for link in links:
            href = link.get("href").lower()  # RFC required
            rel = link.get("rel").lower()  # RFC required
            # some gTLD servers have confusing/inconsistent edge-cases;
            # checks if this href is "authoritative"
            title = link.get("title", "").lower()
            if "authoritative" in title and rel == "self":
                return None
            # skip this link if the "type" is specified and not json
            _type = link.get("type")
            if _type and _type != "application/rdap+json":
                continue
            # otherwise compare the hrefs and check "rel"
            if href != current_href and rel == "related":
                # special case for ipv4 and ipv6 checks
                if isinstance(
                    self._target, (ipaddress.IPv4Address, ipaddress.IPv6Address)
                ):
                    # ensure the href contains the IP, otherwise the link may be "related",
                    # but a URI for something not useful for this information.
                    if str(self._target) not in href:
                        continue
                # ensure href is properly formatted;
                # sometimes it's just the server name e.g. "rdap.server.com"
                if not href.endswith(str(self._target)):
                    return self._build_query_href(href, str(self._target))
                else:
                    return href

    @staticmethod
    def _check_status_code(status_code: int) -> None:
        if status_code == RDAPStatusCodes.POSITIVE_ANSWER_200:
            return None
        elif status_code == RDAPStatusCodes.MALFORMED_QUERY_400:
            raise MalformedQueryError(
                f"Malformed query: {RDAPStatusCodes.MALFORMED_QUERY_400}"
            )
        elif status_code == RDAPStatusCodes.NEGATIVE_ANSWER_404:
            raise NotFoundError(
                f"Domain not found: {RDAPStatusCodes.NEGATIVE_ANSWER_404}"
            )
        elif status_code == RDAPStatusCodes.RATE_LIMIT_429:
            raise RateLimitError(f"Too many requests: {RDAPStatusCodes.RATE_LIMIT_429}")
        else:
            raise BadStatusCode(f"Status code <{status_code}>")


class DNSClient(RDAPClient):
    # IANA DNS
    _iana_uri: str = "https://data.iana.org/rdap/dns.json"

    def __init__(self, httpx_client: Union[httpx.Client, httpx.AsyncClient]):
        super(DNSClient, self).__init__(httpx_client)
        self.iana_dns_server_map: Dict[str, str] = {}
        self._target = None

    def lookup(self, domain: str, tld: str, auth_href: str = None) -> DomainResponse:
        """
        Performs an RDAP domain lookup.
        Finds the authoritative server for the domain,
        submits an HTTP request, and encapsulates the
        response into a DomainResponse object.

        :param domain: The domain name
        :param tld: The top level domain
        :param auth_href: Optional authoritative href for the given TLD
        :return: instance of DomainResponse
        """
        self._target = domain + "." + tld
        # set starting href
        if auth_href:
            href = auth_href
        else:
            base_href = self.iana_dns_server_map.get(tld)
            if not base_href:
                raise NotImplementedError(
                    f"No RDAP server found for .{tld.upper()} domains"
                )
            # build query href
            href = self._build_query_href(base_href, self._target)
        # get response
        rdap_resp = self._get_authoritative_response(href, [href])
        # construct and return domain response
        domain_response = DomainResponse.from_json(rdap_resp.read())
        return domain_response

    async def aio_lookup(
        self, domain: str, tld: str, auth_href: str = None
    ) -> DomainResponse:
        """
        Performs an RDAP domain lookup.
        Finds the authoritative server for the domain,
        submits an HTTP request, and encapsulates the
        response into a DomainResponse object.

        :param domain: The domain name
        :param tld: The top level domain
        :param auth_href: Optional authoritative href for the given TLD
        :return: instance of DomainResponse
        """
        self._target = domain + "." + tld
        # set starting href
        if auth_href:
            href = auth_href
        else:
            base_href = self.iana_dns_server_map.get(tld)
            if not base_href:
                raise NotImplementedError(
                    f"No RDAP server found for .{tld.upper()} domains"
                )
            # build query href
            href = self._build_query_href(base_href, self._target)
        # get response
        rdap_resp = await self._aio_get_authoritative_response(href, [href])
        # construct and return domain response
        domain_response = DomainResponse.from_json(rdap_resp.read())
        return domain_response

    @staticmethod
    def _build_query_href(rdap_href: str, target: str) -> str:
        href = posixpath.join(rdap_href, "domain", target.lstrip("/"))
        if not href.startswith("http"):
            href = "https://" + href
        return href

    def _set_iana_info(self, iana_resp: Dict[str, Any]) -> None:
        """
        Populates `iana_dns_server_map` attribute with
        server information found in the given `iana_resp`.

        :param iana_resp: Server information retrieved from `self._iana_url`
        :return: None
        """
        self.publication = iana_resp.get(self._iana_publication_key)
        self.version = iana_resp.get(self._iana_version_key)
        tld_server_map = {}
        for tlds, server in iana_resp.get(self._iana_services_key):
            for tld in tlds:
                tld_server_map[tld] = server[0]
        self.iana_dns_server_map = tld_server_map


class IPv4Client(RDAPClient):
    # IANA IPv4
    _iana_uri: str = "https://data.iana.org/rdap/ipv4.json"

    def __init__(self, httpx_client: Union[httpx.Client, httpx.AsyncClient]):
        super().__init__(httpx_client)
        self.iana_ipv4_server_map: Dict[ipaddress.IPv4Network, str] = {}
        self._target = None

    def lookup(
        self, ipv4: Union[str, ipaddress.IPv4Address], auth_href: str = None
    ) -> IPv4Response:
        """
        Performs an RDAP ipv4 lookup.
        Finds the authoritative server for the ip address,
        submits an HTTP request, and encapsulates the
        response into a IPv4Response object.

        :param ipv4: ipv4 address as string or IPv4Address object
        :param auth_href: Optional authoritative rdap href for the IPv4
        :return: instance of IPv4Response
        """
        if not isinstance(ipv4, ipaddress.IPv4Address):
            self._target = ipaddress.IPv4Address(ipv4)
        else:
            self._target = ipv4
        server = self._get_rdap_server(self._target)
        href = self._build_query_href(server, str(self._target))
        rdap_resp = self._get_authoritative_response(href, [href])
        ipv4_response = IPv4Response.from_json(rdap_resp.read())
        return ipv4_response

    async def aio_lookup(
        self, ipv4: Union[str, ipaddress.IPv4Address], auth_href: str = None
    ) -> IPv4Response:
        """
        Performs an RDAP IPv4 lookup.
        Finds the authoritative server for the ip address,
        submits an HTTP request, and encapsulates the
        response into a IPv4Response object.

        :param ipv4: IPv4 address as string or IPv4Address object
        :param auth_href: Optional authoritative rdap href for the IPv4
        :return: instance of IPv4Response
        """
        if not isinstance(ipv4, ipaddress.IPv4Address):
            self._target = ipaddress.IPv4Address(ipv4)
        else:
            self._target = ipv4
        server = self._get_rdap_server(self._target)
        href = self._build_query_href(server, str(self._target))
        rdap_resp = await self._aio_get_authoritative_response(href, [href])
        ipv4_response = IPv4Response.from_json(rdap_resp.read())
        return ipv4_response

    @staticmethod
    def _build_query_href(rdap_href: str, target: str) -> str:
        href = posixpath.join(rdap_href, "ip", target.lstrip("/"))
        if not href.startswith("http"):
            href = "https://" + href
        return href

    def _set_iana_info(self, iana_ipv4_map: Dict[str, Any]) -> None:
        self.publication = iana_ipv4_map.get(self._iana_publication_key)
        self.version = iana_ipv4_map.get(self._iana_version_key)
        for service in iana_ipv4_map.get("services"):
            ips, servers = service[0], service[1]
            for ip in ips:
                ipv4 = ipaddress.IPv4Network(ip)
                self.iana_ipv4_server_map[ipv4] = servers[0]  # https

    def _get_rdap_server(self, ipv4: ipaddress.IPv4Address) -> Optional[str]:
        for network, server in self.iana_ipv4_server_map.items():
            if ipv4 in network:
                return server
        return None


class IPv6Client(RDAPClient):
    # IANA IPv6
    _iana_uri: str = "https://data.iana.org/rdap/ipv6.json"

    def __init__(self, httpx_client: Union[httpx.Client, httpx.AsyncClient]):
        super().__init__(httpx_client)
        self.iana_ipv6_server_map: Dict[ipaddress.IPv6Network, str] = {}
        self._target = None

    def lookup(
        self, ipv6: Union[str, ipaddress.IPv6Address], auth_href: str = None
    ) -> IPv6Response:
        """
        Performs an RDAP IPv6 lookup.
        Finds the authoritative server for the ip address,
        submits an HTTP request, and encapsulates the
        response into a IPv6Response object.

        :param ipv6: IPv6 address as string or IPv6Address object
        :param auth_href: Optional authoritative rdap href for the IPv6
        :return: instance of IPv6Response
        """
        if not isinstance(ipv6, ipaddress.IPv6Address):
            self._target = ipaddress.IPv6Address(ipv6)
        else:
            self._target = ipv6
        server = self._get_rdap_server(self._target)
        if server is None:
            raise WhodapError(f"No RDAP server found for IPv6={ipv6}")
        href = self._build_query_href(server, str(self._target))
        rdap_resp = self._get_authoritative_response(href, [href])
        ipv6_response = IPv6Response.from_json(rdap_resp.read())
        return ipv6_response

    async def aio_lookup(
        self, ipv6: Union[str, ipaddress.IPv4Address], auth_href: str = None
    ) -> IPv6Response:
        """
        Performs an RDAP IPv6 lookup.
        Finds the authoritative server for the ip address,
        submits an HTTP request, and encapsulates the
        response into a IPv6Response object.

        :param ipv6: IPv6 address as string or IPv6Address object
        :param auth_href: Optional authoritative rdap href for the IPv6
        :return: instance of IPv6Response
        """
        if not isinstance(ipv6, ipaddress.IPv6Address):
            self._target = ipaddress.IPv6Address(ipv6)
        else:
            self._target = ipv6
        server = self._get_rdap_server(self._target)
        if server is None:
            raise WhodapError(f"No RDAP server found for IPv6={ipv6}")
        href = self._build_query_href(server, str(self._target))
        rdap_resp = await self._aio_get_authoritative_response(href, [href])
        ipv6_response = IPv6Response.from_json(rdap_resp.read())
        return ipv6_response

    @staticmethod
    def _build_query_href(rdap_href: str, target: str) -> str:
        return posixpath.join(rdap_href, "ip", target.lstrip("/"))

    def _set_iana_info(self, iana_ipv6_map: Dict[str, Any]) -> None:
        self.publication = iana_ipv6_map.get(self._iana_publication_key)
        self.version = iana_ipv6_map.get(self._iana_version_key)
        for service in iana_ipv6_map.get("services"):
            ips, servers = service[0], service[1]
            for ip in ips:
                ipv6 = ipaddress.IPv6Network(ip)
                self.iana_ipv6_server_map[ipv6] = servers[0]  # https

    def _get_rdap_server(self, ipv6: ipaddress.IPv6Address) -> Optional[str]:
        for network, server in self.iana_ipv6_server_map.items():
            if ipv6 in network:
                return server
        return None


class ASNClient(RDAPClient):
    # IANA ASN
    _iana_uri: str = "https://data.iana.org/rdap/asn.json"

    def __init__(self, httpx_client: Union[httpx.Client, httpx.AsyncClient]):
        super().__init__(httpx_client)
        self.iana_asn_server_map: Dict[str, str] = {}
        self._target = None

    def lookup(self, asn: int, auth_href: str = None) -> ASNResponse:
        """
        Performs an RDAP ASN lookup.

        :param asn: the Autonomous System Number
        :param auth_href: Optional authoritative rdap href
        :return: ASNResponse
        """
        self._target = asn
        server = self._get_rdap_server(asn)
        href = self._build_query_href(server, str(asn))
        rdap_resp = self._get_authoritative_response(href, [href])
        asn_response = ASNResponse.from_json(rdap_resp.read())
        return asn_response

    async def aio_lookup(self, asn: int, auth_href: str = None) -> ASNResponse:
        """
        Performs an RDAP ASN lookup.

        :param asn: the Autonomous System Number
        :param auth_href: Optional authoritative rdap href
        :return: ASNResponse
        """
        self._target = asn
        server = self._get_rdap_server(asn)
        href = self._build_query_href(server, str(asn))
        rdap_resp = await self._aio_get_authoritative_response(href, [href])
        asn_response = ASNResponse.from_json(rdap_resp.read())
        return asn_response

    @staticmethod
    def _build_query_href(rdap_href: str, target: str) -> str:
        return posixpath.join(rdap_href, "autnum", target.lstrip("/"))

    def _set_iana_info(self, iana_asn_map: Dict[str, Any]) -> None:
        self.publication = iana_asn_map.get(self._iana_publication_key)
        self.version = iana_asn_map.get(self._iana_version_key)
        for service in iana_asn_map.get("services"):
            asn_ranges, servers = service[0], service[1]
            for asn_range in asn_ranges:
                self.iana_asn_server_map[asn_range] = servers[0]  # https

    def _get_rdap_server(self, asn_number: int) -> Optional[str]:
        for asn_range, server in self.iana_asn_server_map.items():
            if "-" in asn_range:
                lower, upper = [int(n) for n in asn_range.split("-")]
            else:
                lower = upper = int(asn_range)
            if lower <= asn_number <= upper:
                return server
        return None
