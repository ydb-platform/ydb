import abc
from typing import Any

from annet.annlib.netdev.views.hardware import HardwareView
from annet.connectors import Connector
from annet.vendors import registry_connector


class _HardwareConnector(Connector["HardwareProvider"]):
    name = "Hardware"
    ep_name = "hardware"
    ep_by_group_only = "annet.connectors.hardware"


hardware_connector = _HardwareConnector()


class HardwareProvider(abc.ABC):
    @abc.abstractmethod
    def make_hw(self, hw_model: str, sw_version: str) -> Any:
        pass

    @abc.abstractmethod
    def vendor_to_hw(self, vendor: str) -> Any:
        pass

    @abc.abstractmethod
    def hw_to_vendor(self, hw: Any) -> str | None:
        pass


class AnnetHardwareProvider(HardwareProvider):
    def make_hw(self, hw_model: str, sw_version: str) -> HardwareView:
        return HardwareView(hw_model, sw_version)

    def vendor_to_hw(self, vendor: str) -> HardwareView:
        return registry_connector.get().get(vendor.lower()).hardware

    def hw_to_vendor(self, hw: HardwareView) -> str | None:
        if vendor := registry_connector.get().match(hw, None):
            return vendor.NAME
        return None
