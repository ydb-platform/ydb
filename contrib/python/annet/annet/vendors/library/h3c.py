from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import HuaweiFormatter


@registry.register
class H3CVendor(AbstractVendor):
    NAME = "h3c"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("system-view"))
        if do_finalize:
            after.add_cmd(Command("save force", timeout=90, read_timeout=90))

        return before, after

    def match(self) -> list[str]:
        return ["H3C"]

    @property
    def reverse(self) -> str:
        return "undo"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("H3C")

    def svi_name(self, num: int) -> str:
        return f"Vlan-interface{num}"

    def make_formatter(self, **kwargs) -> HuaweiFormatter:
        return HuaweiFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "quit"
