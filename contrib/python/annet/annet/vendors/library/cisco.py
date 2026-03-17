from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import CiscoFormatter


@registry.register
class CiscoVendor(AbstractVendor):
    NAME = "cisco"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("conf t"))
        after.add_cmd(Command("exit"))
        if do_finalize:
            after.add_cmd(Command("copy running-config startup-config", timeout=40))

        return before, after

    def match(self) -> list[str]:
        # ASR1k runs IOS-XE, so Cisco.ASR.ASR1000 must be explicetely returned here
        return ["Cisco", "Cisco.ASR.ASR1000"]

    @property
    def reverse(self) -> str:
        return "no"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Cisco")

    def svi_name(self, num: int) -> str:
        return f"Vlan{num}"

    def make_formatter(self, **kwargs) -> CiscoFormatter:
        return CiscoFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "exit"
