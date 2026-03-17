from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import B4comFormatter


@registry.register
class B4ComVendor(AbstractVendor):
    NAME = "b4com"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        if hw.B4com.CS2148P:
            before.add_cmd(Command("conf t"))
            after.add_cmd(Command("end"))
            if do_finalize:
                after.add_cmd(Command("write", timeout=40))
        else:
            before.add_cmd(Command("conf t"))
            if do_commit:
                after.add_cmd(Command("commit"))
                after.add_cmd(Command("end"))
            if do_finalize:
                after.add_cmd(Command("write", timeout=40))

        return before, after

    def match(self) -> list[str]:
        return ["B4com"]

    @property
    def reverse(self) -> str:
        return "no"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("B4com")

    def make_formatter(self, **kwargs) -> B4comFormatter:
        return B4comFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "exit"

    def svi_name(self, num: int) -> str:
        return f"vlan1.{num}"
