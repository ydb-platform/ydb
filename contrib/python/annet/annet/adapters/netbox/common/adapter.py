from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from annet.annlib.netdev.views.hardware import HardwareView

from .manufacturer import get_breed, get_hw
from .models import FHRPGroup, FHRPGroupAssignment, Interface, IpAddress, NetboxDevice, Prefix


NetboxDeviceT = TypeVar("NetboxDeviceT", bound=NetboxDevice)
InterfaceT = TypeVar("InterfaceT", bound=Interface)
IpAddressT = TypeVar("IpAddressT", bound=IpAddress)
PrefixT = TypeVar("PrefixT", bound=Prefix)
FHRPGroupT = TypeVar("FHRPGroupT", bound=FHRPGroup)
FHRPGroupAssignmentT = TypeVar(
    "FHRPGroupAssignmentT",
    bound=FHRPGroupAssignment,
)


def get_device_breed(device: NetboxDeviceT) -> str:
    if device.device_type and device.device_type.manufacturer:
        return get_breed(
            device.device_type.manufacturer.name,
            device.device_type.model,
        )
    return ""


def get_device_hw(device: NetboxDeviceT) -> HardwareView:
    if device.device_type and device.device_type.manufacturer:
        return get_hw(
            device.device_type.manufacturer.name,
            device.device_type.model,
            device.platform.name if device.platform else "",
        )
    return HardwareView("", "")


class NetboxAdapter(
    ABC,
    Generic[
        NetboxDeviceT,
        InterfaceT,
        IpAddressT,
        PrefixT,
        FHRPGroupT,
        FHRPGroupAssignmentT,
    ],
):
    @abstractmethod
    def list_fqdns(self, query: dict[str, list[str]] | None = None) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def list_devices(self, query: dict[str, list[str]]) -> list[NetboxDeviceT]:
        raise NotImplementedError()

    @abstractmethod
    def get_device(self, device_id: int) -> NetboxDeviceT:
        raise NotImplementedError()

    @abstractmethod
    def list_interfaces_by_devices(self, device_ids: list[int]) -> list[InterfaceT]:
        raise NotImplementedError()

    @abstractmethod
    def list_interfaces(self, ids: list[int]) -> list[InterfaceT]:
        raise NotImplementedError()

    @abstractmethod
    def list_ipaddr_by_ifaces(self, iface_ids: list[int]) -> list[IpAddressT]:
        raise NotImplementedError()

    @abstractmethod
    def list_ipprefixes(self, prefixes: list[str]) -> list[PrefixT]:
        raise NotImplementedError()

    @abstractmethod
    def list_fhrp_group_assignments(
        self,
        iface_ids: list[int],
    ) -> list[FHRPGroupAssignmentT]:
        raise NotImplementedError()

    @abstractmethod
    def list_fhrp_groups(
        self,
        ids: list[int],
    ) -> list[FHRPGroupT]:
        raise NotImplementedError()
