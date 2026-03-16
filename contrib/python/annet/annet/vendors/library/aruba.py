from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import ArubaFormatter


@registry.register
class ArubaVendor(AbstractVendor):
    NAME = "aruba"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("conf t"))
        after.add_cmd(Command("end"))
        if do_commit:
            after.add_cmd(Command("commit apply"))
        if do_finalize:
            after.add_cmd(Command("write memory"))

        return before, after

    def match(self) -> list[str]:
        return ["Aruba"]

    @property
    def reverse(self) -> str:
        return "no"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Aruba")

    def make_formatter(self, **kwargs) -> ArubaFormatter:
        return ArubaFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "exit"

    def diff(self, order: bool) -> str:
        return "common.ordered_diff" if order else "aruba.default_diff"
