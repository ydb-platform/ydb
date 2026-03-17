from typing import Annotated, Literal, Optional, Union

from ..bgp_models import BFDTimers, PeerFamilyOptions
from .basemodel import BaseMeshModel, Concat, Unite


FamilyName = Literal["ipv4_unicast", "ipv6_unicast", "ipv4_labeled_unicast", "ipv6_labeled_unicast", "l2vpn_evpn"]


class _SharedOptionsDTO(BaseMeshModel):
    """
    Options which can be set on connected pair or group of peers
    """

    add_path: bool
    multipath: bool
    advertise_irb: bool
    send_labeled: bool
    send_community: bool
    bfd: bool
    bfd_timers: BFDTimers
    password: str


class MeshSession(_SharedOptionsDTO):
    """
    Options which are set on connected pair
    """

    asnum: Union[int, str]
    vrf: str
    families: Annotated[set[FamilyName], Unite()]
    group_name: str

    bmp_monitor: bool

    import_policy: str
    export_policy: str


class _OptionsDTO(_SharedOptionsDTO):
    """
    Options which can be set on group of peers or peer itself
    """

    def __init__(self, **kwargs):
        kwargs.setdefault("family_options", PeerFamilyOptions())
        super().__init__(**kwargs)

    unnumbered: bool
    rr_client: bool
    next_hop_self: bool
    extended_next_hop: bool
    send_lcommunity: bool
    send_extcommunity: bool
    import_limit: Optional[int]
    import_limit_action: Optional[str]
    teardown_timeout: bool
    redistribute: bool
    passive: bool
    mtu_discovery: bool
    advertise_inactive: bool
    advertise_bgp_static: bool
    allowas_in: bool
    auth_key: bool
    multihop: Optional[int]
    multihop_no_nexthop_change: bool
    af_no_install: bool
    rib: bool
    resolve_vpn: bool
    af_rib_group: Optional[str]
    af_loops: int
    hold_time: int
    listen_network: list[str]
    remove_private: bool
    as_override: bool
    aigp: bool
    no_prepend: bool
    no_explicit_null: bool
    uniq_iface: bool
    advertise_peer_as: bool
    connect_retry: bool
    advertise_external: bool
    listen_only: bool
    soft_reconfiguration_inbound: bool
    not_active: bool
    mtu: int
    family_options: PeerFamilyOptions
    cluster_id: Optional[str]


class DirectPeerDTO(MeshSession, _OptionsDTO):
    pod: int
    addr: str
    description: str
    update_source: str

    subif: int
    lag: Optional[int]
    lag_links_min: Optional[int]
    svi: Optional[int]


class IndirectPeerDTO(MeshSession, _OptionsDTO):
    pod: int
    addr: str
    description: str
    update_source: str

    ifname: Optional[str]
    subif: int
    svi: Optional[int]


class VirtualLocalDTO(MeshSession, _OptionsDTO):
    pod: int
    addr: str
    description: str
    update_source: str

    svi: int


class VirtualPeerDTO(MeshSession):
    addr: str
    description: str


class MeshPeerGroup(_OptionsDTO):
    name: str
    families: Annotated[set[FamilyName], Unite()]
    local_as: Union[int, str]
    remote_as: Union[int, str]
    internal_name: str
    update_source: str
    description: str
    peer_filter: str

    import_policy: str
    export_policy: str
