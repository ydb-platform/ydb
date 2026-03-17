import os

from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import CommonFormatter


@registry.register
class PCVendor(AbstractVendor):
    NAME = "pc"

    def match(self) -> list[str]:
        return ["PC"]

    @property
    def reverse(self) -> str:
        return "-"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        if hw.soft.startswith(("Cumulus", "SwitchDev")):
            if os.environ.get("ETCKEEPER_CHECK", False):
                before.add_cmd(Command("etckeeper check"))

        return before, after

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("PC")

    def make_formatter(self, **kwargs) -> CommonFormatter:
        return CommonFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return ""
