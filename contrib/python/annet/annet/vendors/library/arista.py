from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import AristaFormatter


@registry.register
class AristaVendor(AbstractVendor):
    NAME = "arista"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("conf s"))
        if do_commit:
            after.add_cmd(Command("commit"))
        else:
            after.add_cmd(Command("abort"))  # просто exit оставит висеть configure session
        if do_finalize:
            after.add_cmd(Command("write memory"))

        return before, after

    def match(self) -> list[str]:
        return ["Arista"]

    @property
    def reverse(self) -> str:
        return "no"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Arista")

    def svi_name(self, num: int) -> str:
        return f"Vlan{num}"

    def make_formatter(self, **kwargs) -> AristaFormatter:
        return AristaFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "exit"
