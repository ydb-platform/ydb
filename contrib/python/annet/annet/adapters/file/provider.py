import dataclasses
from dataclasses import dataclass, fields
from typing import Any, Iterable, List, Optional, Sequence

import yaml

from annet.adapters.netbox.common.manufacturer import get_breed
from annet.annlib.netdev.views.dump import DumpableView
from annet.annlib.netdev.views.hardware import HardwareView
from annet.connectors import AdapterWithName
from annet.hardware import hardware_connector
from annet.storage import Device as DeviceProtocol
from annet.storage import Query, Storage, StorageProvider


@dataclass
class Interface(DumpableView):
    name: str
    description: str
    enabled: bool = True

    @property
    def _dump__list_key(self):
        return self.name


@dataclass
class DeviceStorage:
    fqdn: str

    vendor: Optional[str] = None
    hw_model: Optional[str] = None
    sw_version: Optional[str] = None
    breed: Optional[str] = None

    hostname: Optional[str] = None
    serial: Optional[str] = None
    id: Optional[str] = None
    interfaces: Optional[list[Interface]] = None
    storage: Optional[Storage] = None

    hw: HardwareView = dataclasses.field(init=False)

    def __post_init__(self):
        if not self.id:
            self.id = self.fqdn
        if not self.hostname:
            self.hostname = self.fqdn.split(".")[0]

        if self.hw_model:
            hw = HardwareView(self.hw_model, self.sw_version)
            if self.vendor and self.vendor != hw.vendor:
                raise Exception(f"Vendor {self.vendor} is not vendor from hw model ({hw.vendor})")
            else:
                self.vendor = hw.vendor
        else:
            hw_provider = hardware_connector.get()
            hw: HardwareView = hw_provider.vendor_to_hw(self.vendor)
            if not hw:
                raise Exception("unknown vendor")
            self.hw_model = hw.model
        self.hw = hw

        if not self.breed:
            if self.hw.model:
                parts = self.hw.model.split(maxsplit=1)
                if len(parts) >= 2:
                    self.breed = get_breed(parts[0], parts[1])
            if not self.breed:
                self.breed = self.hw.vendor

        if isinstance(self.interfaces, list):
            interfaces = []
            for iface in self.interfaces:
                try:
                    interfaces.append(Interface(**iface))
                except Exception as e:
                    raise Exception("unable to parse %s as Interface: %s" % (iface, e))
            self.interfaces = interfaces

    def set_storage(self, storage: Storage):
        self.storage = storage


@dataclass
class Device(DeviceProtocol, DumpableView):
    dev: DeviceStorage

    def __hash__(self):
        return hash((self.id, type(self)))

    def __eq__(self, other):
        return type(self) is type(other) and self.fqdn == other.fqdn and self.vendor == other.vendor

    def is_pc(self) -> bool:
        return False

    @property
    def hostname(self) -> str:
        return self.dev.hostname

    @property
    def fqdn(self) -> str:
        return self.dev.fqdn

    @property
    def id(self):
        return self.dev.id

    @property
    def storage(self) -> Storage:
        return self.dev.storage

    @property
    def hw(self) -> HardwareView:
        return self.dev.hw

    @property
    def breed(self) -> str:
        return self.dev.breed

    @property
    def neighbours_ids(self):
        return []

    @property
    def neighbours_fqdns(self) -> list[str]:
        return []

    def make_lag(self, lag: int, ports: Sequence[str], lag_min_links: Optional[int]) -> Interface:
        raise NotImplementedError

    def add_svi(self, svi: int) -> Interface:
        raise NotImplementedError

    def add_subif(self, interface: str, subif: int) -> Interface:
        raise NotImplementedError

    def find_interface(self, name: str) -> Optional[Interface]:
        raise NotImplementedError

    def flush_perf(self):
        pass


@dataclass
class Devices:
    devices: list[Device]

    def __post_init__(self):
        if isinstance(self.devices, list):
            devices = []
            for dev in self.devices:
                try:
                    devices.append(Device(dev=DeviceStorage(**dev)))
                except Exception as e:
                    raise Exception("unable to parse %s as Device: %s" % (dev, e))
            self.devices = devices


class Provider(StorageProvider, AdapterWithName):
    def storage(self):
        return storage_factory

    def opts(self):
        return StorageOpts

    def query(self):
        return Query

    @classmethod
    def name(cls) -> str:
        return "file"


@dataclass
class Query(Query):
    query: List[str]

    @classmethod
    def new(cls, query: str | Iterable[str], hosts_range: Optional[slice] = None) -> "Query":
        if hosts_range is not None:
            raise ValueError("host_range is not supported")
        return cls(query=list(query))

    @property
    def globs(self):
        return self.query

    def is_empty(self) -> bool:
        return len(self.query) == 0


class StorageOpts:
    def __init__(self, path: str):
        self.path = path

    @classmethod
    def parse_params(cls, conf_params: Optional[dict[str, str]], cli_opts: Any):
        path = conf_params.get("path")
        if not path:
            raise Exception("empty path")
        return cls(path=path)


def storage_factory(opts: StorageOpts) -> Storage:
    return FS(opts)


class FS(Storage):
    def __init__(self, opts: StorageOpts):
        self.opts = opts
        self.inventory: Devices = read_inventory(opts.path, self)

    def __enter__(self):
        return self

    def __exit__(self, _, __, ___):
        pass

    def resolve_object_ids_by_query(self, query: Query) -> list[str]:
        result = filter_query(self.inventory.devices, query)
        return [dev.fqdn for dev in result]

    def resolve_fdnds_by_query(self, query: Query) -> list[str]:
        result = filter_query(self.inventory.devices, query)
        return [dev.fqdn for dev in result]

    def make_devices(
        self,
        query: Query | list,
        preload_neighbors=False,
        use_mesh=None,
        preload_extra_fields=False,
        **kwargs,
    ) -> list[Device]:
        if isinstance(query, list):
            query = Query.new(query)
        result = filter_query(self.inventory.devices, query)
        return result

    def get_device(self, obj_id: str, preload_neighbors=False, use_mesh=None, **kwargs) -> Device:
        result = filter_query(self.inventory.devices, Query.new(obj_id))
        if not result:
            raise Exception("not found")
        return result[0]

    def flush_perf(self):
        pass

    def resolve_all_fdnds(self) -> list[str]:
        return [d.fqdn for d in self.inventory.devices]

    def search_connections(self, device: "Device", neighbor: "Device") -> list[tuple["Interface", "Interface"]]:
        return []


def filter_query(devices: list[Device], query: Query) -> list[Device]:
    result: list[Device] = []
    for dev in devices:
        if dev.fqdn in query.query:
            result.append(dev)
    return result


def read_inventory(path: str, storage: Storage) -> Devices:
    with open(path, "r") as f:
        file_data = yaml.load(f, Loader=yaml.SafeLoader)
    res = dataclass_from_dict(Devices, file_data)
    for dev in res.devices:
        dev.dev.set_storage(storage)
    return res


def dataclass_from_dict(klass: type, d: dict[str, Any]):
    try:
        fieldtypes = {f.name: f.type for f in fields(klass)}
    except TypeError:
        return d
    return klass(**{f: dataclass_from_dict(fieldtypes[f], d[f]) for f in d})
