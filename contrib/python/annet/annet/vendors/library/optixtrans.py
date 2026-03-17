from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.registry import registry
from annet.vendors.tabparser import OptixtransFormatter

from .huawei import HuaweiVendor


@registry.register
class OptixTransVendor(HuaweiVendor):
    NAME = "optixtrans"

    def match(self) -> list[str]:
        return ["Huawei.OptiXtrans"]

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Huawei OptiXtrans")

    def make_formatter(self, **kwargs) -> OptixtransFormatter:
        return OptixtransFormatter(**kwargs)

    def svi_name(self, num: int) -> str:
        return f"vlan{num}"
