from ipaddress import IPv6Network

from annet.rpl_generators import (
    AsPathFilter,
    CommunityList,
    CommunityLogic,
    CommunityType,
    IpPrefixListMember,
    RDFilter,
    ip_prefix_list,
)


AS_PATH_FILTERS = [
    AsPathFilter("ASP_EXAMPLE", [".*123456.*", ".*22.*"]),
]
COMMUNITIES = [
    CommunityList("COMMUNITY_EXAMPLE_ADD", ["1234:1000", "1234:1001"], logic=CommunityLogic.AND),
    CommunityList("COMMUNITY_EXAMPLE_REMOVE", ["1234:999", "1234:998"], logic=CommunityLogic.OR),
    CommunityList("EXTCOMMUNITY_EXAMPLE_ADD", ["12345:1000"], CommunityType.RT),
    CommunityList("EXTCOMMUNITY_EXAMPLE_REMOVE", ["12345:999"], CommunityType.RT),
]

RD_FILTERS = [
    RDFilter("RD_EXAMPLE1", 1, ["100:1", "200:1"]),
    RDFilter("RD_EXAMPLE2", 2, ["10.2.2.2:1", "10.3.3.3:1"]),
]

PREFIX_LISTS = [
    ip_prefix_list(
        "IPV6_LIST_EXAMPLE",
        [
            "2a13:5941::/32",
            IpPrefixListMember(IPv6Network("2a13:5942::/32"), or_longer=(32, 48)),
            IpPrefixListMember(IPv6Network("2a13:5943::/32"), or_longer=(32, None)),
            IpPrefixListMember(IPv6Network("2a13:5944::/32"), or_longer=(32, 32)),
        ],
    ),
    ip_prefix_list("IPV4_LIST_EXAMPLE", ["0.0.0.0/8", "10.0.0.0/8"]),
]
