import builtins
from ipaddress import (
    IPv4Address,
    IPv4Network,
    IPv6Address,
    IPv6Network,
    ip_address,
    ip_network,
)
from typing import Iterable, List, Sequence, Union

from .exceptions import (
    IncorrectIPCount,
    IPAddress,
    IPNetwork,
    IPRule,
    Trusted,
    UntrustedIP,
)

Elem = Iterable[Union[str, IPAddress, IPNetwork]]
ElemEllpisis = Union["builtins.ellipsis", Elem]
TrustedOrig = Iterable[ElemEllpisis]

MSG = "Trusted list should be a sequence of sets " "with either addresses or networks."

IP_CLASSES = (IPv4Address, IPv6Address, IPv4Network, IPv6Network)


def parse_trusted_element(elem: Elem) -> List[Union[IPAddress, IPNetwork]]:
    new_elem = []
    for item in elem:
        if isinstance(item, IP_CLASSES):
            new_elem.append(item)
            continue
        try:
            new_elem.append(ip_address(item))
        except ValueError:
            try:
                new_elem.append(ip_network(item))
            except ValueError:
                raise ValueError(f"{item!r} is not IPv4 or IPv6 address or network")
    return new_elem


def parse_trusted_list(lst: TrustedOrig) -> Trusted:
    if isinstance(lst, str) or not isinstance(lst, Sequence):
        raise TypeError(MSG)
    out = []
    has_ellipsis = False
    for elem in lst:
        new_elem: ElemEllpisis
        if elem is ...:
            has_ellipsis = True
            new_elem = ...
        else:
            if has_ellipsis:
                raise ValueError("Ellipsis is allowed only at the end of list")
            if isinstance(elem, str) or not isinstance(elem, Iterable):
                raise TypeError(MSG)
            new_elem = parse_trusted_element(elem)
        out.append(new_elem)
    return out


def remote_ip(trusted: Trusted, ips: Sequence[IPAddress]) -> IPAddress:
    if len(trusted) + 1 != len(ips):
        raise IncorrectIPCount(len(trusted) + 1, ips)
    for i in range(len(trusted)):
        ip = ips[i]
        tr = trusted[i]
        if tr is ...:
            return ip
        # cast drops previously handled ... type
        check_ip(tr, ip)
    return ips[-1]


def check_ip(trusted: Sequence[IPRule], ip: IPAddress) -> None:
    for elem in trusted:
        if isinstance(elem, (IPv4Address, IPv6Address)):
            if elem == ip:
                break
        else:
            if ip in elem:
                break
    else:
        raise UntrustedIP(ip, trusted)
