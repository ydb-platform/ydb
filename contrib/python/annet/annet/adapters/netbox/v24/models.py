from dataclasses import dataclass, field
from datetime import datetime, timezone
from ipaddress import IPv6Interface, ip_interface
from typing import Any, Dict, List, Optional, Sequence

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
    family: int

    @property
    def _dump__list_key(self):
        return self.address


@dataclass
class Prefix(DumpableView):
    id: int
    prefix: str
    site: Optional[Entity]
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


@dataclass
class IpAddress(DumpableView):
    id: int
    assigned_object_id: int
    display: str
    family: IpFamily
    address: str
    status: Label
    tags: List[Entity]
    created: datetime
    last_updated: datetime
    prefix: Optional[Prefix] = None
    vrf: Optional[Entity] = None

    @property
    def _dump__list_key(self):
        return self.address


@dataclass
class InterfaceConnectedEndpoint(Entity):
    device: Entity


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


@dataclass
class Interface(Entity):
    device: Entity
    enabled: bool
    description: str
    type: InterfaceType
    connected_endpoints: Optional[list[InterfaceConnectedEndpoint]]
    mode: Optional[InterfaceMode]
    untagged_vlan: Optional[InterfaceVlan]
    tagged_vlans: Optional[List[InterfaceVlan]]
    display: str = ""
    ip_addresses: List[IpAddress] = field(default_factory=list)
    vrf: Optional[Entity] = None
    mtu: int | None = None
    lag: Entity | None = None
    lag_min_links: int | None = None

    def add_addr(self, address_mask: str, vrf: str | None) -> None:
        addr = ip_interface(address_mask)
        if vrf is None:
            vrf_obj = None
        else:
            vrf_obj = Entity(id=0, name=vrf)

        if isinstance(addr, IPv6Interface):
            family = IpFamily(value=6, label="IPv6")
        else:
            family = IpFamily(value=4, label="IPv4")

        for existing_addr in self.ip_addresses:
            if existing_addr.address == address_mask and (
                (existing_addr.vrf is None and vrf is None)
                or (existing_addr.vrf is not None and existing_addr.vrf.name == vrf)
            ):
                return
        self.ip_addresses.append(
            IpAddress(
                id=0,
                display=address_mask,
                address=address_mask,
                vrf=vrf_obj,
                prefix=None,
                family=family,
                created=datetime.now(timezone.utc),
                last_updated=datetime.now(timezone.utc),
                tags=[],
                status=Label(value="active", label="Active"),
                assigned_object_id=self.id,
            )
        )


@dataclass
class NetboxDevice(Entity):
    url: str
    storage: Storage

    display: str
    device_type: DeviceType
    device_role: Entity
    tenant: Optional[Entity]
    platform: Optional[Entity]
    serial: str
    asset_tag: Optional[str]
    site: Entity
    rack: Optional[Entity]
    position: Optional[float]
    face: Optional[Label]
    status: Label
    primary_ip: Optional[DeviceIp]
    primary_ip4: Optional[DeviceIp]
    primary_ip6: Optional[DeviceIp]
    tags: List[Entity]
    custom_fields: Dict[str, Any]
    created: datetime
    last_updated: datetime

    fqdn: str
    hostname: str
    hw: HardwareView
    breed: str

    interfaces: List[Interface]

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

    def _make_interface(self, name: str, type: InterfaceType) -> Interface:
        return Interface(
            name=name,
            device=self,
            enabled=True,
            description="",
            type=type,
            id=0,
            vrf=None,
            display=name,
            untagged_vlan=None,
            tagged_vlans=[],
            ip_addresses=[],
            connected_endpoints=[],
            mode=None,
        )

    def _lag_name(self, lag: int) -> str:
        return lag_name(self.hw, lag)

    def make_lag(self, lag: int, ports: Sequence[str], lag_min_links: int | None) -> Interface:
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

    def add_svi(self, svi: int) -> Interface:
        name = self._svi_name(svi)
        for interface in self.interfaces:
            if interface.name == name:
                return interface
        interface = self._make_interface(name=name, type=InterfaceType("virtual", "Virtual"))
        self.interfaces.append(interface)
        return interface

    def _subif_name(self, interface: str, subif: int) -> str:
        return f"{interface}.{subif}"

    def add_subif(self, interface: str, subif: int) -> Interface:
        name = self._subif_name(interface, subif)
        for target_port in self.interfaces:
            if target_port.name == name:
                return target_port
        target_port = self._make_interface(name=name, type=InterfaceType("virtual", "Virtual"))
        self.interfaces.append(target_port)
        return target_port

    def find_interface(self, name: str) -> Optional[Interface]:
        for iface in self.interfaces:
            if iface.name == name:
                return iface
        return None
