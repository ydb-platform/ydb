from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from ipaddress import IPv6Interface, ip_interface
from typing import Any, Dict, Generic, List, Optional, Sequence, TypeVar

from annet.annlib.netdev.views.dump import DumpableView
from annet.annlib.netdev.views.hardware import HardwareView, lag_name
from annet.storage import Storage
from annet.vendors import registry_connector


@dataclass
class Entity(DumpableView):
    id: int
    name: str

    @property
    def _dump__list_key(self):
        return self.name


@dataclass
class EntityWithSlug(Entity):
    slug: str


@dataclass
class Label:
    value: str
    label: str


@dataclass
class IpFamily:
    value: int
    label: str


@dataclass
class DeviceType:
    id: int
    manufacturer: Entity
    model: str


@dataclass
class DeviceIp(DumpableView):
    id: int
    display: str
    address: str

    @property
    def _dump__list_key(self):
        return self.address


@dataclass
class Prefix(DumpableView):
    id: int
    prefix: str
    # `site` deprecated since v4.2, replace in derived classes.
    vrf: Optional[Entity]
    tenant: Optional[Entity]
    vlan: Optional[Entity]
    role: Optional[Entity]
    status: Label
    is_pool: bool
    custom_fields: dict[str, Any]
    created: datetime
    last_updated: datetime
    description: Optional[str] = ""

    @property
    def _dump__list_key(self):
        return self.prefix


_PrefixT = TypeVar("_PrefixT", bound=Prefix)


@dataclass
class IpAddress(DumpableView, Generic[_PrefixT]):
    id: int
    assigned_object_id: int | None
    display: str
    family: IpFamily
    address: str
    status: Label
    tags: List[EntityWithSlug]
    created: datetime
    last_updated: datetime
    prefix: Optional[_PrefixT] = None
    vrf: Optional[Entity] = None

    @property
    def _dump__list_key(self):
        return self.address


@dataclass
class InterfaceConnectedEndpoint(Entity):
    device: Entity | None


@dataclass
class InterfaceType:
    value: str
    label: str


@dataclass
class InterfaceMode:
    value: str
    label: str


@dataclass
class InterfaceVlan(Entity):
    vid: int


def vrf_object(vrf: str | None) -> Entity | None:
    if vrf is None:
        return None
    else:
        return Entity(id=0, name=vrf)


_IpAddressT = TypeVar("_IpAddressT", bound=IpAddress)


_DeviceIPT = TypeVar("_DeviceIPT", bound=DeviceIp)


@dataclass
class FHRPGroup(Generic[_DeviceIPT]):
    id: int
    group_id: int
    display: str
    protocol: str
    description: str | None

    name: str
    auth_type: str | None
    auth_key: str
    tags: list[EntityWithSlug]
    custom_fields: dict[str, Any]
    ip_addresses: list[_DeviceIPT]
    comments: str | None = None


_FHRPGroupT = TypeVar("_FHRPGroupT", bound=FHRPGroup)


@dataclass
class FHRPGroupAssignment(Generic[_FHRPGroupT]):
    id: int
    display: str
    priority: int
    group: _FHRPGroupT
    fhrp_group_id: int

    interface_type: str | None
    interface_id: int | None


_FHRPGroupAssignmentT = TypeVar(
    "_FHRPGroupAssignmentT",
    bound=FHRPGroupAssignment,
)


@dataclass
class Interface(Entity, Generic[_IpAddressT, _FHRPGroupAssignmentT]):
    device: Entity
    enabled: bool
    description: str
    type: InterfaceType
    connected_endpoints: Optional[list[InterfaceConnectedEndpoint]]
    mode: Optional[InterfaceMode]
    untagged_vlan: Optional[InterfaceVlan]
    tagged_vlans: Optional[List[InterfaceVlan]]
    tags: List[EntityWithSlug] = field(default_factory=list)
    display: str = ""
    vrf: Optional[Entity] = None
    mtu: int | None = None
    lag: Entity | None = None
    lag_min_links: int | None = None
    speed: int | None = None

    custom_fields: Dict[str, Any] = field(default_factory=dict)

    ip_addresses: List[_IpAddressT] = field(default_factory=list)
    count_ipaddresses: int = 0
    fhrp_groups: List[_FHRPGroupAssignmentT] = field(default_factory=list)
    count_fhrp_groups: int = 0

    def add_addr(self, address_mask: str, vrf: str | None) -> None:
        addr = ip_interface(address_mask)

        # when comparing ip addressess
        # we dont care about the vrf of an addr
        # because the actual vrf will be
        # determined by an interface's vrf
        # and there can be only one
        #
        # also we dont care about a mask
        # because no device will allow
        # the same addr with multiple masks
        for existing_addr in self.ip_addresses:
            existing = ip_interface(existing_addr.address)
            if existing.ip == addr.ip:
                return

        vrf_obj = vrf_object(vrf)
        if isinstance(addr, IPv6Interface):
            family = IpFamily(value=6, label="IPv6")
        else:
            family = IpFamily(value=4, label="IPv4")
        self._add_new_addr(address_mask, vrf_obj, family)

    @abstractmethod
    def _add_new_addr(self, address_mask: str, vrf: Entity | None, family: IpFamily):
        raise NotImplementedError


_InterfaceT = TypeVar("_InterfaceT", bound=Interface)


@dataclass
class NetboxDevice(Entity, Generic[_InterfaceT, _DeviceIPT]):
    url: str
    storage: Storage

    display: str
    device_type: DeviceType
    # `device_role` deprecated since v4.0, replace in derived classes.
    tenant: Optional[Entity]
    platform: Optional[EntityWithSlug]
    serial: str
    asset_tag: Optional[str]
    site: Entity
    rack: Optional[Entity]
    position: Optional[float]
    face: Optional[Label]
    status: Label
    primary_ip: Optional[_DeviceIPT]
    primary_ip4: Optional[_DeviceIPT]
    primary_ip6: Optional[_DeviceIPT]
    tags: List[EntityWithSlug]
    custom_fields: Dict[str, Any]
    created: datetime
    last_updated: datetime
    cluster: Optional[Entity]
    config_context: Optional[dict[str, Any]]
    config_template: Optional[dict[str, Any]]

    fqdn: str
    hostname: str
    hw: Optional[HardwareView]
    breed: str

    interfaces: List[_InterfaceT]

    @property
    def neighbors(self) -> List["Entity"]:
        return [
            endpoint.device
            for iface in self.interfaces
            if iface.connected_endpoints
            for endpoint in iface.connected_endpoints
            if endpoint.device
        ]

    # compat
    @property
    def neighbours_fqdns(self) -> list[str]:
        return [dev.name for dev in self.neighbors]

    @property
    def neighbours_ids(self):
        return [dev.id for dev in self.neighbors]

    def __hash__(self):
        return hash((self.id, type(self)))

    def __eq__(self, other):
        return type(self) is type(other) and self.url == other.url

    def is_pc(self) -> bool:
        custom_breed_pc = ("Mellanox", "NVIDIA", "Moxa", "Nebius")
        return self.device_type.manufacturer.name in custom_breed_pc or self.breed == "pc"

    @abstractmethod
    def _make_interface(self, name: str, type: InterfaceType) -> _InterfaceT:
        raise NotImplementedError

    def _lag_name(self, lag: int) -> str:
        return lag_name(self.hw, lag)

    def make_lag(self, lag: int, ports: Sequence[str], lag_min_links: int | None) -> _InterfaceT:
        new_name = self._lag_name(lag)
        for target_interface in self.interfaces:
            if target_interface.name == new_name:
                return target_interface
        lag_interface = self._make_interface(
            name=new_name,
            type=InterfaceType(value="lag", label="Link Aggregation Group (LAG)"),
        )
        lag_interface.lag_min_links = lag_min_links
        for interface in self.interfaces:
            if interface.name in ports:
                interface.lag = lag_interface
        self.interfaces.append(lag_interface)
        return lag_interface

    def _svi_name(self, svi: int) -> str:
        return registry_connector.get().match(self.hw).svi_name(svi)

    def add_svi(self, svi: int) -> _InterfaceT:
        name = self._svi_name(svi)
        for interface in self.interfaces:
            if interface.name == name:
                return interface
        interface = self._make_interface(name=name, type=InterfaceType("virtual", "Virtual"))
        self.interfaces.append(interface)
        return interface

    def _subif_name(self, interface: str, subif: int) -> str:
        return f"{interface}.{subif}"

    def add_subif(self, interface: str, subif: int) -> _InterfaceT:
        name = self._subif_name(interface, subif)
        for target_port in self.interfaces:
            if target_port.name == name:
                return target_port
        target_port = self._make_interface(name=name, type=InterfaceType("virtual", "Virtual"))
        self.interfaces.append(target_port)
        return target_port

    def find_interface(self, name: str) -> Optional[_InterfaceT]:
        for iface in self.interfaces:
            if iface.name == name:
                return iface
        return None
