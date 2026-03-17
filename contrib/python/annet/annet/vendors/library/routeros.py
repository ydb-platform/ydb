from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.base import AbstractVendor
from annet.vendors.registry import registry
from annet.vendors.tabparser import RosFormatter


@registry.register
class RouterOSVendor(AbstractVendor):
    NAME = "routeros"

    def apply(self, hw: HardwareView, do_commit: bool, do_finalize: bool, path: str) -> tuple[CommandList, CommandList]:
        before, after = CommandList(), CommandList()

        # FIXME: пока не удалось победить \x1b[c после включения safe mode
        # if len(cmds) > 99:
        #     raise Exception("RouterOS does not support more 100 actions in safe mode")
        # before.add_cmd(RosDevice.SAFE_MODE)
        pass
        # after.add_cmd(RosDevice.SAFE_MODE)

        return before, after

    def match(self) -> list[str]:
        return ["RouterOS"]

    @property
    def reverse(self) -> str:
        return "remove"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("RouterOS")

    def make_formatter(self, **kwargs) -> RosFormatter:
        return RosFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return ""
