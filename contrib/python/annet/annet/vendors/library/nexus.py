from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import NexusFormatter


@registry.register
class NexusVendor(AbstractVendor):
    NAME = "nexus"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("conf t"))
        after.add_cmd(Command("exit"))
        if do_finalize:
            after.add_cmd(Command("copy running-config startup-config", timeout=40))

        return before, after

    def match(self) -> list[str]:
        return ["Cisco.Nexus"]

    @property
    def reverse(self) -> str:
        return "no"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Cisco Nexus")

    def make_formatter(self, **kwargs) -> NexusFormatter:
        return NexusFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "exit"
