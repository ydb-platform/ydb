from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Annotated, Literal, Optional, Union


class VidRange:
    def __init__(self, start: int, stop: int) -> None:
        self.start = start
        self.stop = stop

    def is_single(self):
        return self.start == self.stop

    def __iter__(self):
        return iter(range(self.start, self.stop + 1))

    def __str__(self):
        if self.is_single():
            return str(self.start)
        return f"{self.start}-{self.stop}"

    def __repr__(self):
        return f"VlanRange({self.start}, {self.stop})"

    def __eq__(self, other: object) -> bool:
        if type(other) is VidRange:
            return self.start == other.start and self.stop == other.stop
        return NotImplemented


def _parse_vlan_ranges(ranges: str) -> Iterable[VidRange]:
    for range in ranges.split(","):
        start, sep, stop = range.strip().partition("-")
        try:
            if not sep:
                int_start = int(start)
                yield VidRange(int_start, int_start)
            elif not stop or not start:
                raise ValueError(f"Cannot parse range {range!r}. Expected `start-stop`")
            else:
                yield VidRange(int(start), int(stop))
        except ValueError:
            raise ValueError(f"Cannot parse range {range!r}. Expected `vid1-vid2` or `vid`")


class VidCollection:
    @staticmethod
    def parse(ranges: int | str) -> "VidCollection":
        if isinstance(ranges, int):
            return VidCollection([VidRange(ranges, ranges)])
        elif isinstance(ranges, str):
            return VidCollection(list(_parse_vlan_ranges(ranges)))
        elif isinstance(ranges, VidCollection):
            return VidCollection(ranges.ranges)
        else:
            raise TypeError(f"Expected str or int, got {type(ranges)}")

    def __init__(self, ranges: list[VidRange]) -> None:
        self.ranges = ranges

    def __str__(self):
        return ",".join(map(str, self.ranges))

    def __repr__(self):
        return f"VlanCollection({str(self)!r})"

    def __iter__(self):
        for range in self.ranges:
            yield from range

    def __eq__(self, other: object) -> bool:
        if type(other) is VidCollection:
            return self.ranges == other.ranges
        return False


class ASN(int):
    """
    Stores ASN number and formats it as в AS1.AS2
    None is treated like 0. Supports integer operations
    Supported formats: https://tools.ietf.org/html/rfc5396#section-1
    """

    PLAIN_MAX = 0x10000

    def __new__(cls, asn: Union[int, str, None, "ASN"]):
        if isinstance(asn, ASN):
            return asn
        elif asn is None:
            asn = 0
        elif not isinstance(asn, int):
            if isinstance(asn, str) and "." in asn:
                high, low = [int(token) for token in asn.split(".")]
                if not (0 <= high < ASN.PLAIN_MAX and 0 <= low < ASN.PLAIN_MAX):
                    raise ValueError("Invalid ASN asn %r" % asn)
                asn = (high << 16) + low
            asn = int(asn)
        if not 0 <= asn <= 0xFFFFFFFF:
            raise ValueError("Invalid ASN asn %r" % asn)
        return int.__new__(cls, asn)

    def __add__(self, other: int):
        return ASN(super().__add__(other))

    def __sub__(self, other: int):
        return ASN(super().__sub__(other))

    def __mul__(self, other: int):
        return ASN(super().__mul__(other))

    def is_plain(self) -> bool:
        return self < ASN.PLAIN_MAX

    def asdot(self) -> str:
        if not self.is_plain():
            return "%d.%d" % (self // ASN.PLAIN_MAX, self % ASN.PLAIN_MAX)
        return "%d" % self

    def asplain(self) -> str:
        return "%d" % self

    def asdot_high(self) -> int:
        return self // ASN.PLAIN_MAX

    def asdot_low(self) -> int:
        return self % ASN.PLAIN_MAX

    __str__ = asdot

    def __repr__(self) -> str:
        srepr = str(self)
        if "." in srepr:
            srepr = repr(srepr)
        return f"{self.__class__.__name__}({srepr})"


@dataclass(frozen=True)
class BFDTimers:
    minimum_interval: int = 500
    multiplier: int = 4


Family = Literal[
    "ipv4_unicast",
    "ipv6_unicast",
    "ipv4_vpn_unicast",
    "ipv6_vpn_unicast",
    "ipv4_labeled_unicast",
    "ipv6_labeled_unicast",
    "l2vpn_evpn",
]


@dataclass(frozen=True)
class PeerOptions:
    """The same options as for group but any field is optional"""

    local_as: Optional[ASN] = None
    unnumbered: Optional[bool] = None
    rr_client: Optional[bool] = None
    next_hop_self: Optional[bool] = None
    extended_next_hop: Optional[bool] = None
    send_community: Optional[bool] = None
    send_lcommunity: Optional[bool] = None
    send_extcommunity: Optional[bool] = None
    send_labeled: Optional[bool] = None
    import_limit: Optional[int] = None
    import_limit_action: Optional[str] = None
    teardown_timeout: Optional[bool] = None
    redistribute: Optional[bool] = None
    passive: Optional[bool] = None
    mtu_discovery: Optional[bool] = None
    advertise_inactive: Optional[bool] = None
    advertise_bgp_static: Optional[bool] = None
    allowas_in: Optional[bool] = None
    auth_key: Optional[bool] = None
    add_path: Optional[bool] = None
    multipath: Optional[bool] = None
    multihop: Optional[bool] = None
    multihop_no_nexthop_change: Optional[bool] = None
    af_no_install: Optional[bool] = None
    bfd: Optional[bool] = None
    rib: Optional[bool] = None
    bfd_timers: Optional[BFDTimers] = None
    resolve_vpn: Optional[bool] = None
    af_rib_group: Optional[str] = None
    af_loops: Optional[int] = None
    hold_time: Optional[int] = None
    listen_network: Optional[list[str]] = None
    remove_private: Optional[bool] = None
    as_override: Optional[bool] = None
    aigp: Optional[bool] = None
    bmp_monitor: Optional[bool] = None
    no_prepend: Optional[bool] = None
    no_explicit_null: Optional[bool] = None
    uniq_iface: Optional[bool] = None
    advertise_peer_as: Optional[bool] = None
    connect_retry: Optional[bool] = None
    advertise_external: Optional[bool] = None
    advertise_irb: Optional[bool] = None
    listen_only: Optional[bool] = None
    soft_reconfiguration_inbound: Optional[bool] = None
    not_active: Optional[bool] = None
    mtu: Optional[int] = None
    password: Optional[str] = None
    cluster_id: Optional[str] = None


@dataclass
class PeerFamilyOption:
    af_loops: Optional[int] = None
    import_limit: Optional[int] = None


@dataclass
class PeerFamilyOptions:
    ipv4_unicast: PeerFamilyOption = field(default_factory=PeerFamilyOption)
    ipv6_unicast: PeerFamilyOption = field(default_factory=PeerFamilyOption)
    ipv4_vpn_unicast: PeerFamilyOption = field(default_factory=PeerFamilyOption)
    ipv6_vpn_unicast: PeerFamilyOption = field(default_factory=PeerFamilyOption)
    ipv4_labeled_unicast: PeerFamilyOption = field(default_factory=PeerFamilyOption)
    ipv6_labeled_unicast: PeerFamilyOption = field(default_factory=PeerFamilyOption)
    l2vpn_evpn: PeerFamilyOption = field(default_factory=PeerFamilyOption)


@dataclass
class Peer:
    addr: str
    interface: Optional[str]
    remote_as: ASN
    families: set[Family] = field(default_factory=set)
    family_options: PeerFamilyOptions = field(default_factory=PeerFamilyOptions)
    description: str = ""
    vrf_name: str = ""
    group_name: str = ""
    import_policy: str = ""
    export_policy: str = ""
    update_source: Optional[str] = None
    options: Optional[PeerOptions] = None
    hostname: str = ""


@dataclass
class Aggregate:
    policy: str = ""
    routes: tuple[str, ...] = ()  # "182.168.1.0/24",
    as_path: str = ""
    reference: str = ""
    suppress: bool = False
    discard: bool = True
    as_set: bool = False


@dataclass
class Redistribute:
    protocol: str
    policy: str = ""


@dataclass
class FamilyOptions:
    family: Family
    vrf_name: str = ""
    multipath: int = 0
    global_multipath: int = 0
    aggregate: Aggregate = field(default_factory=Aggregate)  # use `aggregates` instead
    aggregates: tuple[Aggregate, ...] = ()
    af_loops: Optional[int] = None
    redistributes: tuple[Redistribute, ...] = ()
    allow_default: bool = False
    aspath_relax: bool = False
    igp_ignore: bool = False
    next_hop_policy: bool = False
    rib_import_policy: bool = False
    advertise_l2vpn_evpn: bool = False
    rib_group: bool = False
    loops: int = 0
    advertise_bgp_static: bool = False
    import_policy: Optional[str] = None
    export_policy: Optional[str] = None


@dataclass(frozen=True)
class PeerGroup:
    name: str
    remote_as: ASN = ASN(None)
    families: set[Family] = field(default_factory=set)
    family_options: PeerFamilyOptions = field(default_factory=PeerFamilyOptions)
    internal_name: str = ""
    description: str = ""
    update_source: str = ""
    peer_filter: str = ""
    import_policy: str = ""
    export_policy: str = ""

    # more strict version of PeerOptions
    local_as: ASN = ASN(None)
    unnumbered: bool = False
    rr_client: bool = False
    next_hop_self: bool = False
    extended_next_hop: bool = False
    send_community: bool = False
    send_lcommunity: bool = False
    send_extcommunity: bool = False
    send_labeled: bool = False
    import_limit: int = False
    import_limit_action: Optional[str] = None
    teardown_timeout: bool = False
    redistribute: bool = False
    passive: bool = False
    mtu_discovery: bool = False
    advertise_inactive: bool = False
    advertise_bgp_static: bool = False
    allowas_in: bool = False
    auth_key: bool = False
    add_path: bool = False
    multipath: bool = False
    multihop: Optional[int] = None
    multihop_no_nexthop_change: bool = False
    af_no_install: bool = False
    bfd: bool = False
    rib: bool = False
    bfd_timers: Optional[BFDTimers] = None
    resolve_vpn: bool = False
    af_rib_group: Optional[str] = None
    af_loops: int = 0
    hold_time: int = 0
    listen_network: list[str] = field(default_factory=list)
    remove_private: bool = False
    as_override: bool = False
    aigp: bool = False
    bmp_monitor: bool = False
    no_prepend: bool = False
    no_explicit_null: bool = False
    uniq_iface: bool = False
    advertise_peer_as: bool = False
    connect_retry: bool = False
    advertise_external: bool = False
    advertise_irb: bool = False
    listen_only: bool = False
    soft_reconfiguration_inbound: bool = False
    not_active: bool = False
    mtu: int = 0
    password: Optional[str] = None
    cluster_id: Optional[str] = None


@dataclass
class L2VpnOptions:
    name: str
    vid: VidCollection
    l2vni: int  # VNI, possible values are 1 to 2**24-1
    route_distinguisher: str = ""  # like in VrfOptions
    rt_import: list[str] = field(default_factory=list)  # like in VrfOptions
    rt_export: list[str] = field(default_factory=list)  # like in VrfOptions
    advertise_host_routes: bool = True  # advertise IP+MAC routes into L3VNI


@dataclass
class VrfOptions:
    vrf_name: str

    ipv4_unicast: FamilyOptions
    ipv6_unicast: FamilyOptions
    ipv4_labeled_unicast: FamilyOptions
    ipv6_labeled_unicast: FamilyOptions
    l2vpn_evpn: FamilyOptions

    vrf_name_global: Optional[str] = None
    import_policy: str = ""
    export_policy: str = ""
    as_path_relax: bool = False
    l3vni: Optional[int] = None
    rt_import: list[str] = field(default_factory=list)
    rt_export: list[str] = field(default_factory=list)
    rt_import_v4: list[str] = field(default_factory=list)
    rt_export_v4: list[str] = field(default_factory=list)
    route_distinguisher: Optional[str] = None
    static_label: Optional[int] = None  # FIXME: str?

    groups: list[PeerGroup] = field(default_factory=list)


@dataclass
class GlobalOptions:
    ipv4_unicast: FamilyOptions
    ipv6_unicast: FamilyOptions
    ipv4_labeled_unicast: FamilyOptions
    ipv6_labeled_unicast: FamilyOptions
    l2vpn_evpn: FamilyOptions

    as_path_relax: bool = False
    local_as: ASN = ASN(None)
    loops: int = 0
    multipath: int = 0
    router_id: str = ""
    cluster_id: Optional[str] = None

    vrf: dict[str, VrfOptions] = field(default_factory=dict)
    groups: list[PeerGroup] = field(default_factory=list)
    l2vpn: dict[str, L2VpnOptions] = field(default_factory=dict)


@dataclass
class BgpConfig:
    global_options: GlobalOptions
    peers: list[Peer]


def _used_policies(peer: Union[Peer, PeerGroup, VrfOptions]) -> Iterable[str]:
    if peer.import_policy:
        yield peer.import_policy
    if peer.export_policy:
        yield peer.export_policy


def _used_families_policies(opts: Union[GlobalOptions, VrfOptions]) -> Iterable[str]:
    for family_opts in (
        opts.ipv4_unicast,
        opts.ipv6_unicast,
        opts.ipv4_labeled_unicast,
        opts.ipv6_labeled_unicast,
        opts.l2vpn_evpn,
    ):
        for red in family_opts.redistributes:
            if red.policy:
                yield red.policy
        if family_opts.aggregate and family_opts.aggregate.policy:
            yield family_opts.aggregate.policy
        for aggregate in family_opts.aggregates:
            if aggregate.policy:
                yield aggregate.policy
        if family_opts.export_policy:
            yield family_opts.export_policy
        if family_opts.import_policy:
            yield family_opts.import_policy


def extract_policies(config: BgpConfig) -> Sequence[str]:
    result: list[str] = []
    for vrf in config.global_options.vrf.values():
        for group in vrf.groups:
            result.extend(_used_policies(group))
        result.extend(_used_families_policies(vrf))
        result.extend(_used_policies(vrf))
    for group in config.global_options.groups:
        result.extend(_used_policies(group))
    for peer in config.peers:
        result.extend(_used_policies(peer))
    result.extend(_used_families_policies(config.global_options))
    return result
