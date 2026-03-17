import ssl
from typing import Callable

from adaptix import P
from adaptix.conversion import get_converter, link, link_constant, link_function
from annetbox.v37 import client_sync
from annetbox.v37 import models as api_models
from requests import Session

from annet.adapters.netbox.common.adapter import NetboxAdapter, get_device_breed, get_device_hw
from annet.adapters.netbox.common.storage_base import BaseNetboxStorage
from annet.storage import Storage

from .models import (
    FHRPGroupAssignmentV37,
    FHRPGroupV37,
    InterfaceV37,
    IpAddressV37,
    NetboxDeviceV37,
    PrefixV37,
)


class NetboxV37Adapter(
    NetboxAdapter[
        NetboxDeviceV37,
        InterfaceV37,
        IpAddressV37,
        PrefixV37,
        FHRPGroupV37,
        FHRPGroupAssignmentV37,
    ]
):
    def __init__(
        self,
        storage: Storage,
        url: str,
        token: str,
        ssl_context: ssl.SSLContext | None,
        threads: int,
        session_factory: Callable[[Session], Session] | None,
    ):
        self.netbox = client_sync.NetboxV37(
            url=url, token=token, ssl_context=ssl_context, threads=threads, session_factory=session_factory
        )
        self.convert_device = get_converter(
            api_models.Device,
            NetboxDeviceV37,
            recipe=[
                link_function(get_device_breed, P[NetboxDeviceV37].breed),
                link_function(get_device_hw, P[NetboxDeviceV37].hw),
                link_constant(P[NetboxDeviceV37].interfaces, factory=list),
                link_constant(P[NetboxDeviceV37].storage, value=storage),
                link(P[api_models.Device].name, P[NetboxDeviceV37].hostname),
                link(P[api_models.Device].name, P[NetboxDeviceV37].fqdn),
            ],
        )
        self.convert_interfaces = get_converter(
            list[api_models.Interface],
            list[InterfaceV37],
            recipe=[
                link_constant(P[InterfaceV37].ip_addresses, factory=list),
                link_constant(P[InterfaceV37].fhrp_groups, factory=list),
                link_constant(P[InterfaceV37].lag_min_links, value=None),
            ],
        )
        self.convert_ip_addresses = get_converter(
            list[api_models.IpAddress],
            list[IpAddressV37],
            recipe=[
                link_constant(P[IpAddressV37].prefix, value=None),
            ],
        )
        self.convert_ip_prefixes = get_converter(
            list[api_models.Prefix],
            list[PrefixV37],
        )
        self.convert_fhrp_group_assignments = get_converter(
            list[api_models.FHRPGroupAssignmentBrief],
            list[FHRPGroupAssignmentV37],
            recipe=[
                link_constant(P[FHRPGroupAssignmentV37].group, value=None),
                link_function(lambda model: model.group.id, P[FHRPGroupAssignmentV37].fhrp_group_id),
            ],
        )
        self.convert_fhrp_groups = get_converter(
            list[api_models.FHRPGroup],
            list[FHRPGroupV37],
        )

    def list_fqdns(self, query: dict[str, list[str]] | None = None) -> list[str]:
        query = query or {}
        return [d.name for d in self.netbox.dcim_all_devices_brief(**query).results]

    def list_devices(self, query: dict[str, list[str]]) -> list[NetboxDeviceV37]:
        return [self.convert_device(dev) for dev in self.netbox.dcim_all_devices(**query).results]

    def get_device(self, device_id: int) -> NetboxDeviceV37:
        return self.convert_device(self.netbox.dcim_device(device_id))

    def list_interfaces_by_devices(self, device_ids: list[int]) -> list[InterfaceV37]:
        return self.convert_interfaces(self.netbox.dcim_all_interfaces(device_id=device_ids).results)

    def list_interfaces(self, ids: list[int]) -> list[InterfaceV37]:
        return self.convert_interfaces(self.netbox.dcim_all_interfaces(id=ids).results)

    def list_ipaddr_by_ifaces(self, iface_ids: list[int]) -> list[IpAddressV37]:
        return self.convert_ip_addresses(self.netbox.ipam_all_ip_addresses(interface_id=iface_ids).results)

    def list_ipprefixes(self, prefixes: list[str]) -> list[PrefixV37]:
        return self.convert_ip_prefixes(self.netbox.ipam_all_prefixes(prefix=prefixes).results)

    def list_fhrp_group_assignments(
        self,
        iface_ids: list[int],
    ) -> list[FHRPGroupAssignmentV37]:
        raw_assignments = self.netbox.ipam_all_fhrp_group_assignments_by_interface(
            interface_id=iface_ids,
        )
        return self.convert_fhrp_group_assignments(raw_assignments.results)

    def list_fhrp_groups(self, ids: list[int]) -> list[FHRPGroupV37]:
        raw_groups = self.netbox.ipam_all_fhrp_groups_by_id(id=list(ids))
        return self.convert_fhrp_groups(raw_groups.results)


class NetboxStorageV37(
    BaseNetboxStorage[
        NetboxDeviceV37,
        InterfaceV37,
        IpAddressV37,
        PrefixV37,
        FHRPGroupV37,
        FHRPGroupAssignmentV37,
    ]
):
    def _init_adapter(
        self,
        url: str,
        token: str,
        ssl_context: ssl.SSLContext | None,
        threads: int,
        session_factory: Callable[[Session], Session] | None = None,
    ) -> NetboxAdapter[NetboxDeviceV37, InterfaceV37, IpAddressV37, PrefixV37, FHRPGroupV37, FHRPGroupAssignmentV37]:
        return NetboxV37Adapter(self, url, token, ssl_context, threads, session_factory)
