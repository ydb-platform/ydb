from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import AsrFormatter


@registry.register
class IosXrVendor(AbstractVendor):
    NAME = "iosxr"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("configure exclusive"))
        if do_commit:
            after.add_cmd(Command("commit show-error"))
        after.add_cmd(Command("exit"))

        return before, after

    def match(self) -> list[str]:
        return ["Cisco.ASR", "Cisco.XR", "Cisco.XRV", "Cisco.NCS"]

    @property
    def reverse(self) -> str:
        return "no"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Cisco ASR")

    def svi_name(self, num: int) -> str:
        return f"Vlan{num}"

    def make_formatter(self, **kwargs) -> AsrFormatter:
        return AsrFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "exit"
