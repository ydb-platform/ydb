from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import RibbonFormatter


@registry.register
class RibbonVendor(AbstractVendor):
    NAME = "ribbon"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("configure exclusive"))
        if do_commit:
            after.add_cmd(Command("commit", timeout=30))
        after.add_cmd(Command("exit"))

        return before, after

    def match(self) -> list[str]:
        return ["Ribbon"]

    @property
    def reverse(self) -> str:
        return "delete"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Ribbon")

    def make_formatter(self, **kwargs) -> RibbonFormatter:
        return RibbonFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return "exit"

    def diff(self, order: bool) -> str:
        return "juniper.ordered_diff" if order else "juniper.default_diff"
