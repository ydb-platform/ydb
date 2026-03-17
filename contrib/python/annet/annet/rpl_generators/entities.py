from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from ipaddress import IPv4Network, IPv6Network, ip_network
from typing import List, Literal, Optional

from annet.rpl import OrLonger, PrefixMatchValue, RoutingPolicy


class CommunityLogic(Enum):
    AND = "AND"
    OR = "OR"


class CommunityType(Enum):
    BASIC = "BASIC"
    RT = "RT"
    SOO = "SOO"
    COST = "COST"
    LARGE = "LARGE"


@dataclass(frozen=True)
class CommunityList:
    name: str
    members: Sequence[str]
    type: CommunityType = CommunityType.BASIC
    logic: CommunityLogic = CommunityLogic.OR
    use_regex: bool = False


@dataclass(frozen=True)
class RDFilter:
    name: str
    number: int
    members: Sequence[str]


@dataclass(frozen=True)
class AsPathFilter:
    name: str
    filters: Sequence[str]


@dataclass
class IpPrefixListMember:
    prefix: IPv4Network | IPv6Network
    or_longer: OrLonger = (None, None)

    def __post_init__(self):
        self.prefix = ip_network(self.prefix)


@dataclass
class IpPrefixList:
    name: str
    members: list[IpPrefixListMember]

    def __post_init__(self):
        for i, m in enumerate(self.members):
            if isinstance(m, str):
                self.members[i] = IpPrefixListMember(m)


def ip_prefix_list(
    name: str,
    members_or_str: Sequence[IpPrefixListMember | str],
    or_longer: OrLonger = (None, None),
) -> IpPrefixList:
    members: List[IpPrefixListMember] = []
    for m in members_or_str:
        if isinstance(m, str):
            m = IpPrefixListMember(
                prefix=ip_network(m),
                or_longer=or_longer,
            )
        elif m.or_longer == (None, None):
            m = IpPrefixListMember(
                prefix=m.prefix,
                or_longer=or_longer,
            )
        members.append(m)
    return IpPrefixList(
        name=name,
        members=members,
    )


def arista_well_known_community(community: str) -> str:
    if community == "65535:0":
        return "GSHUT"
    return community


def mangle_united_community_list_name(values: Sequence[str]) -> str:
    """Name for a list used as HAS_ANY between multiple lists"""
    return "_OR_".join(values)


class PrefixListNameGenerator:
    def __init__(self, prefix_lists: Sequence[IpPrefixList], policies: Sequence[RoutingPolicy]):
        self._prefix_lists = {x.name: x for x in prefix_lists}
        self._policies = {x.name: x for x in policies}  # this is here for a later use ~azryve@

    def get_prefix(self, name: str, match: PrefixMatchValue | None) -> IpPrefixList:
        orig_prefix = self._prefix_lists[name]
        override_name: Optional[str] = None
        override_orlonger: Optional[OrLonger] = None

        if match and match.or_longer != (None, None):
            ge, le = match.or_longer
            ge_str = "unset" if ge is None else str(ge)
            le_str = "unset" if le is None else str(le)
            override_name = f"{orig_prefix.name}_{ge_str}_{le_str}"
            override_orlonger = match.or_longer

        return IpPrefixList(
            name=override_name or name,
            members=[
                IpPrefixListMember(
                    x.prefix,
                    or_longer=override_orlonger or x.or_longer,
                )
                for x in orig_prefix.members
            ],
        )


def group_community_members(
    all_communities: dict[str, CommunityList],
    communities: list[str],
) -> dict[CommunityType, list[str]]:
    members: dict[CommunityType, list[str]] = defaultdict(list)
    for community_name in communities:
        community = all_communities[community_name]
        members[community.type].extend(community.members)
    return members


class JuniperPrefixListNameGenerator(PrefixListNameGenerator):
    def get_prefix(self, name: str, match: PrefixMatchValue | None) -> IpPrefixList:
        plist = super().get_prefix(name, match)
        flavour = self.get_plist_flavour(plist)
        # keep the orginal name for an orlonger match
        if flavour != "custom":
            plist.name = name
        return plist

    def get_type(self, name: str, match: PrefixMatchValue | None) -> Literal["prefix-list", "route-filter"]:
        orig_plist = self.get_prefix(name, None)
        plist = self.get_prefix(name, match)
        orig_flavour = self.get_plist_flavour(orig_plist)
        flavour = self.get_plist_flavour(plist)
        if orig_flavour == "custom" or flavour == "custom":
            return "route-filter"
        else:
            return "prefix-list"

    def get_plist_flavour(self, plist: IpPrefixList) -> Literal["simple", "orlonger", "custom"]:
        is_orlonger: bool = False
        is_custom: bool = False
        for m in plist.members:
            ge, le = m.or_longer

            # or_longer != (None, None)
            if ge is not None or le is not None:
                is_orlonger = True

            # orlonger != (n, None), where n is .../n
            if ge is not None and ge != m.prefix.prefixlen:
                is_custom = True
                break

            # orlonger != (None, n), where n is 32 or 128 (ipv4/ipv6)
            if le is not None and le != m.prefix.max_prefixlen:
                is_custom = True
                break

        if is_custom:
            return "custom"
        elif is_orlonger:
            return "orlonger"
        else:
            return "simple"
