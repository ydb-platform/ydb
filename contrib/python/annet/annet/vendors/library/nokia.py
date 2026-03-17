from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import NokiaFormatter


@registry.register
class NokiaVendor(AbstractVendor):
    NAME = "nokia"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("configure private"))
        if do_commit:
            after.add_cmd(Command("commit"))

        return before, after

    def match(self) -> list[str]:
        return ["Nokia"]

    @property
    def reverse(self) -> str:
        return "delete"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Nokia")

    def make_formatter(self, **kwargs) -> NokiaFormatter:
        return NokiaFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return ""

    def diff(self, order: bool) -> str:
        return "juniper.ordered_diff" if order else "juniper.default_diff"
