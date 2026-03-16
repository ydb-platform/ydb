from typing import Annotated, Optional, Union

from annet.bgp_models import Aggregate, Family, Redistribute

from .basemodel import BaseMeshModel, Concat, DictMerge, KeyDefaultDict, Merge
from .peer_models import MeshPeerGroup


class _Aggregate(BaseMeshModel):
    policy: str
    routes: Annotated[tuple[str, ...], Concat()]
    as_path: str
    reference: str
    suppress: bool
    discard: bool
    as_set: bool


class FamilyOptions(BaseMeshModel):
    def __init__(self, **kwargs):
        kwargs.setdefault("aggregate", _Aggregate())
        super().__init__(**kwargs)

    family: Family
    vrf_name: str
    multipath: int
    global_multipath: int
    aggregate: Annotated[_Aggregate, Merge()]  # use `aggregates` instead
    aggregates: Annotated[tuple[Aggregate, ...], Concat()]
    af_loops: int
    redistributes: Annotated[tuple[Redistribute, ...], Concat()]
    allow_default: bool
    aspath_relax: bool
    igp_ignore: bool
    next_hop_policy: bool
    rib_import_policy: bool
    advertise_l2vpn_evpn: bool
    rib_group: bool
    loops: int
    advertise_bgp_static: bool
    import_policy: Optional[str]
    export_policy: Optional[str]


class _FamiliesMixin:
    def __init__(self, **kwargs):
        kwargs.setdefault("ipv4_unicast", FamilyOptions(family="ipv4_unicast"))
        kwargs.setdefault("ipv6_unicast", FamilyOptions(family="ipv6_unicast"))
        kwargs.setdefault("ipv4_vpn_unicast", FamilyOptions(family="ipv4_vpn_unicast"))
        kwargs.setdefault("ipv6_vpn_unicast", FamilyOptions(family="ipv6_vpn_unicast"))
        kwargs.setdefault("ipv4_labeled_unicast", FamilyOptions(family="ipv4_labeled_unicast"))
        kwargs.setdefault("ipv6_labeled_unicast", FamilyOptions(family="ipv6_labeled_unicast"))
        kwargs.setdefault("l2vpn_evpn", FamilyOptions(family="l2vpn_evpn"))
        super().__init__(**kwargs)

    ipv4_unicast: Annotated[FamilyOptions, Merge()]
    ipv6_unicast: Annotated[FamilyOptions, Merge()]
    ipv4_vpn_unicast: Annotated[FamilyOptions, Merge()]
    ipv6_vpn_unicast: Annotated[FamilyOptions, Merge()]
    ipv4_labeled_unicast: Annotated[FamilyOptions, Merge()]
    ipv6_labeled_unicast: Annotated[FamilyOptions, Merge()]
    l2vpn_evpn: Annotated[FamilyOptions, Merge()]


class VrfOptions(_FamiliesMixin, BaseMeshModel):
    def __init__(self, vrf_name: str, **kwargs):
        kwargs.setdefault("ipv4_unicast", FamilyOptions(family="ipv4_unicast", vrf_name=vrf_name))
        kwargs.setdefault("ipv6_unicast", FamilyOptions(family="ipv6_unicast", vrf_name=vrf_name))
        kwargs.setdefault("ipv4_vpn_unicast", FamilyOptions(family="ipv4_unicast", vrf_name=vrf_name))
        kwargs.setdefault("ipv6_vpn_unicast", FamilyOptions(family="ipv6_unicast", vrf_name=vrf_name))
        kwargs.setdefault("ipv4_labeled_unicast", FamilyOptions(family="ipv4_labeled_unicast", vrf_name=vrf_name))
        kwargs.setdefault("ipv6_labeled_unicast", FamilyOptions(family="ipv6_labeled_unicast", vrf_name=vrf_name))
        kwargs.setdefault("l2vpn_evpn", FamilyOptions(family="l2vpn_evpn", vrf_name=vrf_name))
        kwargs.setdefault("groups", KeyDefaultDict(lambda x: MeshPeerGroup(name=x)))
        super().__init__(vrf_name=vrf_name, **kwargs)

    vrf_name: str
    vrf_name_global: Optional[str]
    as_path_relax: bool
    import_policy: Optional[str]
    export_policy: Optional[str]
    l3vni: Optional[int]
    rt_import: Annotated[tuple[str, ...], Concat()]
    rt_export: Annotated[tuple[str, ...], Concat()]
    rt_import_v4: Annotated[tuple[str, ...], Concat()]
    rt_export_v4: Annotated[tuple[str, ...], Concat()]
    route_distinguisher: Optional[str]
    static_label: Optional[int]  # FIXME: str?
    groups: Annotated[dict[str, MeshPeerGroup], DictMerge(Merge())]


class L2VpnOptions(BaseMeshModel):
    name: str
    vid: str | int  # VLAN ID, possible values are 1 to 4094, ranges can be set as strings
    l2vni: int  # VNI, possible values are 1 to 2**24-1
    route_distinguisher: str  # like in VrfOptions
    rt_import: Annotated[tuple[str, ...], Concat()]  # like in VrfOptions
    rt_export: Annotated[tuple[str, ...], Concat()]  # like in VrfOptions
    advertise_host_routes: bool  # advertise IP+MAC routes into L3VNI


class GlobalOptionsDTO(_FamiliesMixin, BaseMeshModel):
    def __init__(self, **kwargs):
        kwargs.setdefault("groups", KeyDefaultDict(lambda x: MeshPeerGroup(name=x)))
        kwargs.setdefault("vrf", KeyDefaultDict(lambda x: VrfOptions(vrf_name=x)))
        kwargs.setdefault("l2vpn", KeyDefaultDict(lambda x: L2VpnOptions(name=x)))
        super().__init__(**kwargs)

    as_path_relax: bool
    local_as: Union[int, str]
    loops: int
    multipath: int
    router_id: str
    cluster_id: Optional[str]
    vrf: Annotated[dict[str, VrfOptions], DictMerge(Merge())]
    groups: Annotated[dict[str, MeshPeerGroup], DictMerge(Merge())]
    l2vpn: Annotated[dict[str, L2VpnOptions], DictMerge(Merge())]
