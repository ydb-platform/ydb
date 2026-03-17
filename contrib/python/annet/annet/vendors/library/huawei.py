from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import HuaweiFormatter


@registry.register
class HuaweiVendor(AbstractVendor):
    NAME = "huawei"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("system-view"))
        if do_commit and (hw.Huawei.CE or hw.Huawei.NE):
            after.add_cmd(Command("commit"))
        after.add_cmd(Command("q"))
        if do_finalize:
            after.add_cmd(Command("save", timeout=20))

        return before, after

    def match(self) -> list[str]:
        return ["Huawei"]

    @property
    def reverse(self) -> str:
        return "undo"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Huawei")

    def svi_name(self, num: int) -> str:
        return f"Vlanif{num}"

    def make_formatter(self, **kwargs) -> HuaweiFormatter:
        return HuaweiFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "quit"
