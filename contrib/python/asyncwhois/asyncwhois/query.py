import asyncio
import ipaddress
import re
import socket
from typing import Tuple, Generator, Union, Optional
from contextlib import contextmanager, asynccontextmanager

from python_socks.sync import Proxy
from python_socks.async_.asyncio import Proxy as AsyncProxy

from .servers import IPv4Allocations, CountryCodeTLD, GenericTLD, SponsoredTLD

BLOCKSIZE = 1500


class Query:
    iana_server = "whois.iana.org"
    whois_port = 43
    refer_regex = r"refer: *(.+)"
    whois_server_regex = r".+ whois server: *(.+)"

    def __init__(
        self,
        proxy_url: Optional[str] = None,
        timeout: int = 10,
        find_authoritative_server: bool = True,
    ):
        self.proxy_url = proxy_url
        self.timeout = timeout
        self.find_authoritative_server = find_authoritative_server

    @staticmethod
    def _find_match(regex: str, blob: str) -> str:
        match = ""
        found = re.search(regex, blob, flags=re.IGNORECASE)
        if found:
            match = found.group(1).rstrip("\r").replace(" ", "").rstrip(":").rstrip("/")
        return match

    @contextmanager
    def _create_connection(
        self, address: Tuple[str, int], proxy_url: Optional[str] = None
    ) -> Generator[socket.socket, None, None]:
        s = None
        try:
            # Use proxy if specified
            if proxy_url:
                proxy = Proxy.from_url(proxy_url)
                # proxy is a standard python socket in blocking mode
                s = proxy.connect(*address, timeout=self.timeout)
            else:
                # otherwise use socket
                s = socket.create_connection(address, self.timeout)
            yield s
        finally:
            if s and hasattr(s, "close"):
                s.close()

    @asynccontextmanager
    async def _aio_create_connection(
        self, address: Tuple[str, int], proxy_url: Optional[str] = None
    ) -> Generator[Tuple[asyncio.StreamReader, asyncio.StreamWriter], None, None]:
        # init
        reader, writer = None, None
        # Use proxy if specified
        if proxy_url:
            proxy = AsyncProxy.from_url(proxy_url)
            # sock is a standard python socket in blocking mode
            sock = await proxy.connect(*address, timeout=self.timeout)
            # pass it to asyncio
            s = asyncio.open_connection(host=None, port=None, sock=sock)
        else:
            # otherwise use asyncio to open the connection
            s = asyncio.open_connection(*address)
        try:
            reader, writer = await asyncio.wait_for(s, self.timeout)
            yield reader, writer
        finally:
            if writer:
                writer.close()
                if hasattr(writer, "wait_closed"):
                    await writer.wait_closed()

    @staticmethod
    def _send_and_recv(conn: socket.socket, data: str) -> str:
        conn.sendall(data.encode())
        result = ""
        while True:
            received = conn.recv(BLOCKSIZE)
            if received == b"":
                break
            else:
                result += received.decode("utf-8", errors="ignore")
        return result

    @staticmethod
    async def _aio_send_and_recv(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter, data: str
    ) -> str:
        writer.write(data.encode())
        result = ""
        while True:
            received = await reader.read(BLOCKSIZE)
            if received == b"":
                break
            else:
                result += received.decode("utf-8", errors="ignore")
        return result

    def run(self, search_term: str, server: Optional[str] = None) -> list[str]:
        """
        Submits the `search_term` to the WHOIS server and returns a list of query responses.
        """
        data = search_term + "\r\n"
        if not server:
            # TODO: think about moving this to subclass
            if ":" in data:  # ipv6
                server_regex = r"whois: *(.+)"
            else:
                server_regex = self.refer_regex
            server = self.iana_server
        else:
            server_regex = self.whois_server_regex
        return self._do_query(server, data, server_regex, [])

    @staticmethod
    def _continue_querying(current_server: str, next_server: str) -> bool:
        next_server = next_server.lower()
        return (
            next_server
            and next_server != current_server
            and not next_server.startswith("http")
            and not next_server.startswith("www.")
        )

    async def aio_run(
        self, search_term: str, server: Optional[str] = None
    ) -> list[str]:
        data = search_term + "\r\n"
        if not server:
            if ":" in data:  # ipv6
                server_regex = r"whois: *(.+)"
            else:
                server_regex = self.refer_regex
            server = self.iana_server
        else:
            server_regex = self.whois_server_regex
        return await self._aio_do_query(server, data, server_regex, [])

    def _do_query(
        self, server: str, data: str, regex: str, chain: list[str]
    ) -> list[str]:
        """
        Recursively submits WHOIS queries until it reaches the Authoritative Server.
        """
        # connect to whois://<server>:43
        with self._create_connection((server, self.whois_port), self.proxy_url) as conn:
            # submit domain and receive raw query output
            query_output = self._send_and_recv(conn, data)
            # save query chain
            chain.append(query_output)
            # if we should find the authoritative response,
            # then parse the response for the next server
            if self.find_authoritative_server:
                # parse response for the referred WHOIS server name
                whois_server = self._find_match(regex, query_output)
                if self._continue_querying(server, whois_server):
                    # recursive call to find more authoritative server
                    chain = self._do_query(
                        whois_server, data, self.whois_server_regex, chain
                    )
        # return the WHOIS query chain
        return chain

    async def _aio_do_query(
        self, server: str, data: str, regex: str, chain: list[str]
    ) -> list[str]:
        # connect to whois://<server>:43
        async with self._aio_create_connection(
            (server, self.whois_port), self.proxy_url
        ) as r_and_w:
            # socket reader and writer
            reader, writer = r_and_w
            # submit domain and receive raw query output
            query_output = await asyncio.wait_for(
                self._aio_send_and_recv(reader, writer, data), self.timeout
            )
            chain.append(query_output)
            # if we should find the authoritative response,
            # then parse the response for the next server
            if self.find_authoritative_server:
                # parse response for the referred WHOIS server name
                whois_server = self._find_match(regex, query_output)
                if self._continue_querying(server, whois_server):
                    # recursive call to find more authoritative server
                    chain = await self._aio_do_query(
                        whois_server, data, self.whois_server_regex, chain
                    )
        # return the WHOIS query chain
        return chain


class DomainQuery(Query):
    def __init__(
        self,
        server: Optional[str] = None,
        proxy_url: Optional[str] = None,
        timeout: int = 10,
        find_authoritative_server: bool = True,
    ):
        super().__init__(proxy_url, timeout, find_authoritative_server)
        self.server = server

    @staticmethod
    def _get_server_name(domain_name: str) -> Union[str, None]:
        tld = domain_name.split(".")[-1]
        tld_converted = tld.upper().replace("-", "_")
        for servers in [CountryCodeTLD, GenericTLD, SponsoredTLD]:
            if hasattr(servers, tld_converted):
                server = getattr(servers, tld_converted)
                return server
        return None

    def run(self, search_term: str, server: Optional[str] = None) -> list[str]:
        if not server:
            server = self._get_server_name(search_term)
        return super().run(str(search_term), server)

    async def aio_run(
        self, search_term: str, server: Optional[str] = None
    ) -> list[str]:
        if not server:
            server = self._get_server_name(search_term)
        return await super().aio_run(str(search_term), server)


class NumberQuery(Query):
    def __init__(
        self,
        server: Optional[str] = None,
        proxy_url: Optional[str] = None,
        timeout: int = 10,
    ):
        super().__init__(proxy_url, timeout)
        self.server = server
        self.whois_server_regex = r"ReferralServer: *whois://(.+)"

    @staticmethod
    def _get_server_name(ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address]):
        if isinstance(ip, ipaddress.IPv4Address):
            _, server = IPv4Allocations().get_servers(ip)
            return server
        elif isinstance(ip, ipaddress.IPv6Address):
            return None

    def run(
        self,
        search_term: Union[ipaddress.IPv4Address, ipaddress.IPv6Address],
        server: Optional[str] = None,
    ) -> list[str]:
        if not server:
            server = self._get_server_name(search_term)
        return super().run(str(search_term), server)

    async def aio_run(
        self,
        search_term: Union[ipaddress.IPv4Address, ipaddress.IPv6Address],
        server: Optional[str] = None,
    ) -> list[str]:
        if not server:
            server = self._get_server_name(search_term)
        return await super().aio_run(str(search_term), server)
