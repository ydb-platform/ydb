from logging import getLogger
from typing import List, Optional, Union

from annetbox.v24 import models as api_models
from annetbox.v24.client_sync import NetboxV24

from annet.adapters.netbox.common.manufacturer import (
    get_breed,
    get_hw,
)
from annet.adapters.netbox.common.models import InterfaceType
from annet.adapters.netbox.common.query import NetboxQuery
from annet.adapters.netbox.common.storage_opts import NetboxStorageOpts
from annet.adapters.netbox.v24 import models
from annet.annlib.netdev.views.hardware import HardwareView
from annet.storage import Storage


logger = getLogger(__name__)


def extend_device_ip(
    ip: Optional[api_models.DeviceIp],
) -> Optional[models.DeviceIp]:
    if not ip:
        return None
    return models.DeviceIp(
        address=ip.address,
        id=ip.id,
        display=ip.address,
        family=ip.family,
    )


def extend_label(
    label: Optional[api_models.Label],
) -> Optional[models.Label]:
    if not label:
        return None
    return models.Label(
        label=label.label,
        value=str(label.value),
    )


def extend_device(
    device: api_models.Device,
    storage: Storage,
) -> models.NetboxDevice:
    manufacturer = device.device_type.manufacturer.name
    model = device.device_type.model
    platform_name = ""
    if device.platform:
        platform_name = device.platform.name

    return models.NetboxDevice(
        url=device.url,
        id=device.id,
        name=device.name,
        display=device.display_name,
        device_type=device.device_type,
        device_role=device.device_role,
        tenant=device.tenant,
        platform=device.platform,
        serial=device.serial,
        asset_tag=device.asset_tag,
        site=device.site,
        rack=device.rack,
        position=device.position,
        face=extend_label(device.face),
        status=device.status,
        primary_ip=extend_device_ip(device.primary_ip),
        primary_ip4=extend_device_ip(device.primary_ip4),
        primary_ip6=extend_device_ip(device.primary_ip6),
        tags=[models.Entity(0, tag) for tag in device.tags],
        custom_fields=device.custom_fields,  # ???
        created=device.created,
        last_updated=device.last_updated,
        fqdn=device.name,
        hostname=device.name,
        hw=get_hw(manufacturer, model, platform_name),
        breed=get_breed(manufacturer, model),
        interfaces=[],
        storage=storage,
    )


def extend_interface(interface: api_models.Interface) -> models.Interface:
    return models.Interface(
        id=interface.id,
        name=interface.name,
        description="",
        device=interface.device,
        enabled=interface.enabled,
        display=interface.name,
        type=InterfaceType("Unknown", "unknown"),
        connected_endpoints=None,
        mode=None,
        untagged_vlan=None,
        tagged_vlans=None,
        ip_addresses=[],
    )


def extend_ip(ip: api_models.IpAddress) -> models.IpAddress:
    return models.IpAddress(
        id=ip.id,
        assigned_object_id=ip.interface.id,
        display=ip.address,
        family=models.IpFamily(
            value=ip.family,
            label=str(ip.family),
        ),
        address=ip.address,
        status=extend_label(ip.status),
        tags=[models.Entity(0, tag) for tag in ip.tags],
        created=ip.created,
        last_updated=ip.last_updated,
    )


class NetboxStorageV24(Storage):
    def __init__(self, opts: Optional[NetboxStorageOpts] = None):
        self.netbox = NetboxV24(
            url=opts.url,
            token=opts.token,
        )

    def __enter__(self):
        return self

    def __exit__(self, _, __, ___):
        pass

    def resolve_object_ids_by_query(self, query: NetboxQuery):
        return [d.id for d in self._load_devices(query)]

    def resolve_fdnds_by_query(self, query: NetboxQuery):
        return [d.name for d in self._load_devices(query)]

    def make_devices(
        self,
        query: Union[NetboxQuery, list],
        preload_neighbors=False,
        use_mesh=None,
        preload_extra_fields=False,
        **kwargs,
    ) -> List[models.NetboxDevice]:
        if isinstance(query, list):
            query = NetboxQuery.new(query)
        device_ids = {device.id: extend_device(device=device, storage=self) for device in self._load_devices(query)}
        if not device_ids:
            return []

        interfaces = self._load_interfaces(list(device_ids))
        for interface in interfaces:
            device_ids[interface.device.id].interfaces.append(interface)
        return list(device_ids.values())

    def _load_devices(self, query: NetboxQuery) -> List[api_models.Device]:
        if not query.globs:
            return []
        return [device for device in self.netbox.dcim_all_devices().results if _match_query(query, device)]

    def _load_interfaces(self, device_ids: List[int]) -> List[models.Interface]:
        interfaces = self.netbox.dcim_all_interfaces(device_id=device_ids)
        extended_ifaces = {interface.id: extend_interface(interface) for interface in interfaces.results}

        ips = self.netbox.ipam_all_ip_addresses(interface_id=list(extended_ifaces))
        for ip in ips.results:
            extended_ip = extend_ip(ip)
            interface = extended_ifaces[extended_ip.assigned_object_id]
            interface.ip_addresses.append(extended_ip)
        return list(extended_ifaces.values())

    def get_device(
        self,
        obj_id,
        preload_neighbors=False,
        use_mesh=None,
        **kwargs,
    ) -> models.NetboxDevice:
        device = self.netbox.dcim_device(obj_id)
        res = extend_device(device=device, storage=self)
        res.interfaces = self._load_interfaces([device.id])
        return res

    def flush_perf(self):
        pass


def _match_query(query: NetboxQuery, device_data: api_models.Device) -> bool:
    for subquery in query.globs:
        if subquery.strip() in device_data.name:
            return True
    return False
