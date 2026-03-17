from dataclasses import dataclass
from ipaddress import ip_interface
from typing import Optional, Union

from adaptix import Chain, Retort, as_is_loader, loader, name_mapping

from ..bgp_models import (
    ASN,
    Aggregate,
    BFDTimers,
    FamilyOptions,
    GlobalOptions,
    L2VpnOptions,
    Peer,
    PeerFamilyOption,
    PeerFamilyOptions,
    PeerGroup,
    PeerOptions,
    Redistribute,
    VidCollection,
    VrfOptions,
)
from .peer_models import DirectPeerDTO, IndirectPeerDTO, VirtualLocalDTO, VirtualPeerDTO


PeerDTO = Union[DirectPeerDTO, IndirectPeerDTO, VirtualPeerDTO]
LocalDTO = Union[DirectPeerDTO, IndirectPeerDTO, VirtualLocalDTO]


@dataclass
class InterfaceChanges:
    addr: Optional[str] = None
    lag: Optional[int] = None
    lag_links_min: Optional[int] = None
    svi: Optional[int] = None
    subif: Optional[int] = None
    vrf: Optional[str] = None

    def __post_init__(self):
        if self.lag is not None and self.svi is not None:
            raise ValueError("Cannot use LAG and SVI together")
        if self.svi is not None and self.subif is not None:
            raise ValueError("Cannot use Subif and SVI together")


class ObjMapping:
    def __init__(self, obj):
        self.obj = obj

    def __contains__(self, item):
        return hasattr(self.obj, item)

    def __getitem__(self, name):
        return getattr(self.obj, name)

    def get(self, name, default=None):
        return getattr(self.obj, name, default)


retort = Retort(
    recipe=[
        loader(InterfaceChanges, ObjMapping, Chain.FIRST),
        loader(ASN, ASN),
        loader(VidCollection, VidCollection.parse),
        loader(GlobalOptions, ObjMapping, Chain.FIRST),
        loader(VrfOptions, ObjMapping, Chain.FIRST),
        loader(L2VpnOptions, ObjMapping, Chain.FIRST),
        loader(FamilyOptions, ObjMapping, Chain.FIRST),
        loader(Aggregate, ObjMapping, Chain.FIRST),
        loader(PeerOptions, ObjMapping, Chain.FIRST),
        loader(PeerFamilyOptions, ObjMapping, Chain.FIRST),
        loader(PeerFamilyOption, ObjMapping, Chain.FIRST),
        as_is_loader(Redistribute),
        as_is_loader(BFDTimers),
        name_mapping(
            PeerOptions,
            map={
                "local_as": "asnum",
            },
        ),
        loader(list[PeerGroup], lambda x: list(x.values()), Chain.FIRST),
        loader(PeerGroup, ObjMapping, Chain.FIRST),
    ]
)

to_bgp_global_options = retort.get_loader(GlobalOptions)


def to_interface_changes(local: LocalDTO, peer: PeerDTO) -> InterfaceChanges:
    return InterfaceChanges(
        addr=getattr(local, "addr", None),
        lag_links_min=getattr(local, "lag_links_min", None),
        svi=getattr(local, "svi", None),
        subif=getattr(local, "subif", None),
        vrf=getattr(peer, "vrf", None),
        lag=getattr(local, "lag", None),
    )


def to_bgp_peer(local: LocalDTO, connected: PeerDTO, connected_hostname: str, interface: Optional[str]) -> Peer:
    options = retort.load(local, PeerOptions)
    result = Peer(
        addr=str(ip_interface(connected.addr).ip),
        interface=interface,
        remote_as=ASN(connected.asnum),
        hostname=connected_hostname,
        options=options,
    )
    # connected
    result.vrf_name = getattr(connected, "vrf", result.vrf_name)
    result.group_name = getattr(connected, "group_name", result.group_name)
    result.description = getattr(connected, "description", result.description)
    result.families = getattr(connected, "families", result.families)
    # local
    result.import_policy = getattr(local, "import_policy", result.import_policy)
    result.export_policy = getattr(local, "export_policy", result.export_policy)
    result.update_source = getattr(local, "update_source", result.update_source)
    result.family_options = getattr(local, "family_options", result.family_options)
    return result
