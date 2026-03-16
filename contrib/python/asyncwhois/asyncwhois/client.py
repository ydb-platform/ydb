import ipaddress
from typing import Union, Any, Optional

from tldextract.tldextract import extract, TLDExtract
import whodap

from .parse import convert_whodap_keys, IPBaseKeys, TLDBaseKeys
from .parse_rir import NumberParser
from .parse_tld import DomainParser
from .query import DomainQuery, NumberQuery


def convert_to_ip(ip: str):
    try:
        ip_obj = ipaddress.ip_address(ip)
        return ip_obj
    except ipaddress.AddressValueError as e:
        raise e


class Client:
    def __init__(self, whodap_client):
        self.whodap_client = whodap_client

    def init_whodap_client(self, ipv4: bool = True):
        if isinstance(self, DomainClient):
            self.whodap_client = whodap.DNSClient.new_client()
        elif isinstance(self, NumberClient):
            if ipv4:
                self.whodap_client = whodap.IPv4Client.new_client()
            else:
                self.whodap_client = whodap.IPv6Client.new_client()
        elif isinstance(self, ASNClient):
            self.whodap_client = whodap.ASNClient.new_client()

    async def init_async_whodap_client(self, ipv4: bool = True):
        if isinstance(self, DomainClient):
            self.whodap_client = await whodap.DNSClient.new_aio_client()
        elif isinstance(self, NumberClient):
            if ipv4:
                self.whodap_client = await whodap.IPv4Client.new_aio_client()
            else:
                self.whodap_client = await whodap.IPv6Client.new_aio_client()
        elif isinstance(self, ASNClient):
            self.whodap_client = await whodap.ASNClient.new_aio_client()


class DomainClient(Client):
    def __init__(
        self,
        authoritative_only: bool = False,
        find_authoritative_server: bool = True,
        ignore_not_found: bool = False,
        proxy_url: Optional[str] = None,
        whodap_client: whodap.DNSClient = None,
        timeout: int = 10,
        tldextract_obj: TLDExtract = None,
    ):
        super().__init__(whodap_client)
        self.authoritative_only = authoritative_only
        self.ignore_not_found = ignore_not_found
        self.proxy_url = proxy_url
        self.timeout = timeout
        self.tldextract_obj = tldextract_obj
        self.query_obj = DomainQuery(
            proxy_url=proxy_url,
            timeout=timeout,
            find_authoritative_server=find_authoritative_server,
        )
        self.parse_obj = DomainParser(ignore_not_found=ignore_not_found)

    def _get_domain_components(self, domain: str) -> tuple[str, str, str]:
        ext = (
            extract(domain)
            if self.tldextract_obj is None
            else self.tldextract_obj(domain)
        )
        suffix = ext.suffix.split(".")[-1]
        domain_core = ext.registered_domain.removesuffix(f".{suffix}")
        return ext.registered_domain, domain_core, suffix

    def rdap(self, domain: str) -> tuple[str, dict]:
        if self.whodap_client is None:
            self.init_whodap_client()
        _, domain_core, tld = self._get_domain_components(domain)
        rdap_output = self.whodap_client.lookup(domain_core, tld)
        query_string = rdap_output.to_json()
        parsed_dict = convert_whodap_keys(rdap_output.to_whois_dict())
        return query_string, parsed_dict

    def whois(self, domain: str) -> tuple[str, dict[TLDBaseKeys, Any]]:
        registered_domain, _, tld = self._get_domain_components(domain)
        query_chain: list[str] = self.query_obj.run(registered_domain)
        authoritative_answer = query_chain[-1]
        parsed_dict: dict[TLDBaseKeys, Any] = self.parse_obj.parse(
            authoritative_answer, tld
        )
        query_string = (
            authoritative_answer if self.authoritative_only else "\n".join(query_chain)
        )
        return query_string, parsed_dict

    async def aio_rdap(self, domain: str) -> tuple[str, dict]:
        if self.whodap_client is None:
            await self.init_async_whodap_client()
        _, domain_core, tld = self._get_domain_components(domain)
        rdap_output = await self.whodap_client.aio_lookup(domain_core, tld)
        query_string = rdap_output.to_json()
        parsed_dict = convert_whodap_keys(rdap_output.to_whois_dict())
        return query_string, parsed_dict

    async def aio_whois(self, domain: str) -> tuple[str, dict[TLDBaseKeys, Any]]:
        registered_domain, _, tld = self._get_domain_components(domain)
        query_chain: list[str] = await self.query_obj.aio_run(registered_domain)
        authoritative_answer = query_chain[-1]
        parsed_dict: dict[TLDBaseKeys, Any] = self.parse_obj.parse(
            authoritative_answer, tld
        )
        query_string = (
            authoritative_answer if self.authoritative_only else "\n".join(query_chain)
        )
        return query_string, parsed_dict


class NumberClient(Client):
    def __init__(
        self,
        authoritative_only: bool = False,
        proxy_url: Optional[str] = None,
        whodap_client: Union[whodap.IPv4Client, whodap.IPv6Client] = None,
        timeout: int = 10,
    ):
        super().__init__(whodap_client)
        self.authoritative_only = authoritative_only
        self.proxy_url = proxy_url
        self.timeout = timeout
        self.whodap_client = whodap_client
        self.query_obj = NumberQuery(proxy_url=proxy_url, timeout=timeout)
        self.parse_obj = NumberParser()

    def rdap(
        self, ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address, str]
    ) -> tuple[str, dict]:
        if not isinstance(ip, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            ip = convert_to_ip(ip)
        if self.whodap_client is None:
            self.init_whodap_client(ipv4=(ip.version == 4))
        query_string = self.whodap_client.lookup(ip).to_json()
        return query_string, {}  # no parsed output available

    def whois(
        self, ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address, str]
    ) -> tuple[str, dict[IPBaseKeys, Any]]:
        if not isinstance(ip, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            ip = convert_to_ip(ip)
        query_chain: list[str] = self.query_obj.run(ip)
        authoritative_answer = query_chain[-1]
        parsed_dict: dict[IPBaseKeys, Any] = {}
        if isinstance(ip, ipaddress.IPv4Address):
            parsed_dict = self.parse_obj.parse(authoritative_answer, ip)
        query_string = (
            authoritative_answer if self.authoritative_only else "\n".join(query_chain)
        )
        return query_string, parsed_dict

    async def aio_rdap(
        self, ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address, str]
    ) -> tuple[str, dict]:
        if not isinstance(ip, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            ip = convert_to_ip(ip)
        if self.whodap_client is None:
            await self.init_async_whodap_client(ipv4=(ip.version == 4))
        query_resp = await self.whodap_client.aio_lookup(ip)
        query_string = query_resp.to_json()
        return query_string, {}  # no parsed output available

    async def aio_whois(
        self, ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address, str]
    ) -> tuple[str, dict[IPBaseKeys, Any]]:
        if not isinstance(ip, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            ip = convert_to_ip(ip)
        query_chain: list[str] = await self.query_obj.aio_run(ip)
        authoritative_answer = query_chain[-1]
        parsed_dict: dict[IPBaseKeys, Any] = {}
        if isinstance(ip, ipaddress.IPv4Address):
            parsed_dict = self.parse_obj.parse(authoritative_answer, ip)
        query_string = (
            authoritative_answer if self.authoritative_only else "\n".join(query_chain)
        )
        return query_string, parsed_dict


class ASNClient(Client):
    def __init__(
        self,
        whodap_client: whodap.ASNClient = None,
        timeout: int = 10,
    ):
        super().__init__(whodap_client)
        self.timeout = timeout

    def rdap(self, asn: int) -> tuple[str, dict]:
        if self.whodap_client is None:
            self.init_whodap_client()
        query_resp = self.whodap_client.lookup(asn)
        query_string = query_resp.to_json()
        return query_string, {}

    async def aio_rdap(self, asn: int) -> tuple[str, dict]:
        if self.whodap_client is None:
            await self.init_async_whodap_client()
        query_resp = await self.whodap_client.aio_lookup(asn)
        query_string = query_resp.to_json()
        return query_string, {}
