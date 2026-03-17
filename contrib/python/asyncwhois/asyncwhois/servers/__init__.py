from typing import Tuple
from ipaddress import IPv4Address

from ..errors import GeneralError
from .ipv4 import IPV4_ALLOCATIONS, AllocationsT
from .domains import CountryCodeTLD, GenericTLD, SponsoredTLD


class IPv4Allocations:
    """Regional Internet Registry IPv4 Allocations:
    https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.xhtml
    and https://data.iana.org/rdap/ipv4.json
    """

    _allocations: AllocationsT = IPV4_ALLOCATIONS

    def get_servers(self, ipv4: IPv4Address) -> Tuple[str, str]:
        """
        Retrieves the WHOIS and RDAP servers for the given IPv4 address.
        """
        for network, servers in self._allocations.items():
            if ipv4 in network:
                return servers["rdap"], servers["whois"]
        # no match
        raise GeneralError(f"No WHOIS or RDAP server for: {ipv4}")
