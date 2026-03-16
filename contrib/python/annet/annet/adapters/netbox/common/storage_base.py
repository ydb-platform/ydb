import ssl
from ipaddress import ip_interface
from logging import getLogger
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union, cast

from annetbox.v37 import models as api_models
from requests import Session
from requests_cache import CachedSession, SQLiteCache

from annet.adapters.netbox.common.query import FIELD_VALUE_SEPARATOR, NetboxQuery
from annet.adapters.netbox.common.storage_opts import NetboxStorageOpts
from annet.storage import Storage

from .adapter import NetboxAdapter
from .models import (
    FHRPGroup,
    FHRPGroupAssignment,
    Interface,
    IpAddress,
    NetboxDevice,
    Prefix,
)


logger = getLogger(__name__)
NetboxDeviceT = TypeVar("NetboxDeviceT", bound=NetboxDevice)
InterfaceT = TypeVar("InterfaceT", bound=Interface)
IpAddressT = TypeVar("IpAddressT", bound=IpAddress)
PrefixT = TypeVar("PrefixT", bound=Prefix)
FHRPGroupT = TypeVar("FHRPGroupT", bound=FHRPGroup)
FHRPGroupAssignmentT = TypeVar(
    "FHRPGroupAssignmentT",
    bound=FHRPGroupAssignment,
)


class BaseNetboxStorage(
    Storage,
    Generic[
        NetboxDeviceT,
        InterfaceT,
        IpAddressT,
        PrefixT,
        FHRPGroupT,
        FHRPGroupAssignmentT,
    ],
):
    """
    Base class for Netbox storage
    """

    def __init__(self, opts: Optional[NetboxStorageOpts] = None):
        ctx: Optional[ssl.SSLContext] = None
        url = ""
        token = ""
        self.exact_host_filter = False
        threads = 1
        session_factory = None
        if opts:
            if opts.insecure:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            url = opts.url
            token = opts.token
            threads = opts.threads
            self.exact_host_filter = opts.exact_host_filter
            self.all_hosts_filter = opts.all_hosts_filter

            if opts.cache_path:
                session_factory = cached_requests_session(opts)

        self.netbox = self._init_adapter(
            url=url,
            token=token,
            ssl_context=ctx,
            threads=threads,
            session_factory=session_factory,
        )
        self._all_fqdns: Optional[list[str]] = None
        self._id_devices: dict[int, NetboxDeviceT] = {}
        self._name_devices: dict[str, NetboxDeviceT] = {}
        self._short_name_devices: dict[str, NetboxDeviceT] = {}

    def _init_adapter(
        self,
        url: str,
        token: str,
        ssl_context: Optional[ssl.SSLContext],
        threads: int,
        session_factory: Callable[[Session], Session] | None = None,
    ) -> NetboxAdapter[NetboxDeviceT, InterfaceT, IpAddressT, PrefixT, FHRPGroupT, FHRPGroupAssignmentT]:
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, _, __, ___):
        pass

    def resolve_object_ids_by_query(self, query: NetboxQuery):
        return [d.id for d in self._load_devices(query)]

    def resolve_fdnds_by_query(self, query: NetboxQuery):
        return [d.name for d in self._load_devices(query)]

    def resolve_all_fdnds(self) -> list[str]:
        if self._all_fqdns is None:
            self._all_fqdns = self.netbox.list_fqdns(self.all_hosts_filter)
        return self._all_fqdns

    def make_devices(
        self,
        query: Union[NetboxQuery, list],
        preload_neighbors=False,
        use_mesh=None,
        preload_extra_fields=False,
        **kwargs,
    ) -> List[NetboxDeviceT]:
        if isinstance(query, list):
            query = NetboxQuery.new(query)

        devices = []
        if query.is_host_query():
            globs = []
            for glob in query.globs:
                if glob in self._name_devices:
                    devices.append(self._name_devices[glob])
                if glob in self._short_name_devices:
                    devices.append(self._short_name_devices[glob])
                else:
                    globs.append(glob)
            if not globs:
                return devices
            query = NetboxQuery.new(globs)

        new_devices = self._load_devices(query)
        self._fill_device_interfaces(new_devices)
        for device in new_devices:
            self._record_device(device)
        return devices + new_devices

    def _load_devices(self, query: NetboxQuery) -> List[NetboxDeviceT]:
        if not query.globs:
            return []
        query_groups = parse_glob(self.exact_host_filter, query)
        devices = [
            device
            for device in self.netbox.list_devices(query_groups)
            if _match_query(self.exact_host_filter, query, device)
        ]
        return devices

    def _fill_device_interfaces(self, devices: list[NetboxDeviceT]) -> None:
        device_mapping = {d.id: d for d in devices}
        interfaces = self.netbox.list_interfaces_by_devices(list(device_mapping))
        for interface in interfaces:
            device_mapping[interface.device.id].interfaces.append(interface)
        self._fill_interface_fhrp_groups(interfaces)
        self._fill_interface_ipaddress(interfaces)

    def _fill_interface_fhrp_groups(self, interfaces: list[InterfaceT]) -> None:
        interface_mapping = {i.id: i for i in interfaces if i.count_fhrp_groups}
        assignments = self.netbox.list_fhrp_group_assignments(list(interface_mapping))
        group_ids = {r.fhrp_group_id for r in assignments}
        groups = {g.id: g for g in self.netbox.list_fhrp_groups(list(group_ids))}
        for assignment in assignments:
            assignment.group = groups[assignment.fhrp_group_id]
            interface_mapping[assignment.interface_id].fhrp_groups.append(assignment)

    def _fill_interface_ipaddress(self, interfaces: list[InterfaceT]) -> None:
        interface_mapping = {i.id: i for i in interfaces if i.count_ipaddresses}
        ips = self.netbox.list_ipaddr_by_ifaces(list(interface_mapping))
        for ip in ips:
            interface_mapping[ip.assigned_object_id].ip_addresses.append(ip)
        self._fill_ipaddr_prefixes(ips)

    def _fill_ipaddr_prefixes(self, ips: list[IpAddressT]) -> None:
        ip_to_cidrs: Dict[str, str] = {ip.address: str(ip_interface(ip.address).network) for ip in ips}
        prefixes = self.netbox.list_ipprefixes(list(ip_to_cidrs.values()))
        cidr_to_prefix: Dict[str, PrefixT] = {x.prefix: x for x in prefixes}
        for ip in ips:
            cidr = ip_to_cidrs[ip.address]
            ip.prefix = cidr_to_prefix.get(cidr)

    def _record_device(self, device: NetboxDeviceT):
        self._id_devices[device.id] = device
        self._short_name_devices[device.name] = device
        if not self.exact_host_filter:
            short_name = device.name.split(".")[0]
            self._short_name_devices[short_name] = device

    def get_device(
        self,
        obj_id,
        preload_neighbors=False,
        use_mesh=None,
        **kwargs,
    ) -> NetboxDeviceT:
        if obj_id in self._id_devices:
            return self._id_devices[obj_id]

        device = self.netbox.get_device(obj_id)
        self._fill_device_interfaces([device])
        self._record_device(device)
        return device

    def flush_perf(self):
        pass

    def search_connections(
        self,
        device: NetboxDeviceT,
        neighbor: NetboxDeviceT,
    ) -> list[tuple[InterfaceT, InterfaceT]]:
        if device.storage is not self:
            raise ValueError("device does not belong to this storage")
        if neighbor.storage is not self:
            raise ValueError("neighbor does not belong to this storage")
        # both devices are NetboxDevice if they are loaded from this storage
        res = []
        for local_port in device.interfaces:
            if not local_port.connected_endpoints:
                continue
            for endpoint in local_port.connected_endpoints:
                if endpoint.device.id == neighbor.id:
                    for remote_port in neighbor.interfaces:
                        if remote_port.name == endpoint.name:
                            res.append((local_port, remote_port))
                            break
        return res


def _match_query(exact_host_filter: bool, query: NetboxQuery, device_data: api_models.Device) -> bool:
    """
    Additional filtering after netbox due to limited backend logic.
    """
    if exact_host_filter:
        return True  # nothing to check, all filtering is done by netbox
    hostnames = [subquery.strip() for subquery in query.globs if FIELD_VALUE_SEPARATOR not in subquery]
    if not hostnames:
        return True  # no hostnames to check

    short_name = device_data.name.split(".")[0]
    for hostname in hostnames:
        hostname = hostname.strip().rstrip(".")
        if short_name == hostname or device_data.name == hostname:
            return True
    return False


def _hostname_dot_hack(raw_query: str) -> str:
    # there is no proper way to lookup host by its hostname
    # ie find "host" with fqdn "host.example.com"
    # besides using name__ic (ie startswith)
    # since there is no direct analogue for this field in netbox
    # so we need to add a dot to hostnames (top-level fqdn part)
    # so we would not receive devices with a common name prefix
    def add_dot(raw_query: Any) -> Any:
        if isinstance(raw_query, str) and "." not in raw_query:
            raw_query = raw_query + "."
        return raw_query

    if isinstance(raw_query, list):
        for i, name in enumerate(raw_query):
            raw_query[i] = add_dot(name)
    elif isinstance(raw_query, str):
        raw_query = add_dot(raw_query)

    return raw_query


def parse_glob(exact_host_filter: bool, query: NetboxQuery) -> dict[str, list[str]]:
    query_groups = cast(dict[str, list[str]], query.parse_query())
    if names := query_groups.pop("name", None):
        if exact_host_filter:
            query_groups["name__ie"] = names
        else:
            query_groups["name__ic"] = [_hostname_dot_hack(name) for name in names]
    return query_groups


def cached_requests_session(opts: NetboxStorageOpts) -> Callable[[Session], Session]:
    def session_factory(session: Session) -> Session:
        backend = SQLiteCache(db_path=opts.cache_path)
        if opts.recache:
            backend.clear()
        return CachedSession.wrap(
            session,
            backend=backend,
            expire_after=opts.cache_ttl,
        )

    return session_factory
