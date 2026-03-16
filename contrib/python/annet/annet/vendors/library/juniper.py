from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import JuniperFormatter


@registry.register
class JuniperVendor(AbstractVendor):
    NAME = "juniper"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        before.add_cmd(Command("configure exclusive"))
        if do_commit:
            after.add_cmd(Command("commit", timeout=30, read_timeout=30))
        after.add_cmd(Command("exit"))

        return before, after

    def match(self) -> list[str]:
        return ["Juniper"]

    @property
    def reverse(self) -> str:
        return "delete"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("Juniper")

    def svi_name(self, num: int) -> str:
        return f"irb.{num}"

    def make_formatter(self, **kwargs) -> JuniperFormatter:
        return JuniperFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return ""

    def diff(self, order: bool) -> str:
        return "juniper.ordered_diff" if order else "juniper.default_diff"
