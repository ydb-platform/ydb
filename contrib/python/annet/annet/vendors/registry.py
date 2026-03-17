import enum
from operator import itemgetter

from annet.annlib.netdev.views.hardware import HardwareView
from annet.vendors.tabparser import CommonFormatter

from .base import AbstractVendor


_SENTINEL = enum.Enum("_SENTINEL", "sentinel")
sentinel = _SENTINEL.sentinel


class GenericVendor(AbstractVendor):
    def match(self) -> list[str]:
        return []

    @property
    def reverse(self) -> str:
        return "-"

    @property
    def hardware(self) -> HardwareView:
        return HardwareView("")

    def make_formatter(self, **kwargs) -> CommonFormatter:
        return CommonFormatter(**kwargs)

    @property
    def exit(self) -> str:
        return ""


GENERIC_VENDOR = GenericVendor()


class Registry:
    def __init__(self):
        self.vendors: dict[str, AbstractVendor] = {}
        self._matchers = {}

    def register(self, cls: type[AbstractVendor]) -> type[AbstractVendor]:
        if not cls.NAME:
            raise RuntimeError(f"{cls.__name__} has empty NAME field")
        if cls.NAME in self.vendors:
            raise RuntimeError(f"{cls.__name__} with name {cls.NAME} already registered")
        self.vendors[cls.NAME] = cls()

        return cls

    def __add__(self, other: "Registry"):
        self.vendors = dict(**other.vendors, **self.vendors)

    def __getitem__(self, item) -> AbstractVendor:
        if item in self.vendors:
            return self.vendors[item]
        raise RuntimeError(f"Unknown vendor {item}")

    def match(
        self, hw: HardwareView | str, default: _SENTINEL | AbstractVendor | None = sentinel
    ) -> AbstractVendor | None:
        if isinstance(hw, str):
            hw = HardwareView(hw, "")

        matched: list[tuple[AbstractVendor, int]] = []
        for name, vendor in self.vendors.items():
            for item in vendor.match():
                if hw.match(item):
                    matched.append((vendor, item.count(".")))

        if matched:
            return next(iter(sorted(matched, key=itemgetter(1), reverse=True)))[0]
        if default is sentinel:
            return GENERIC_VENDOR
        return default

    def get(self, item: str, default: _SENTINEL | AbstractVendor | None = sentinel) -> AbstractVendor | None:
        if item in self:
            return self[item]
        if default is sentinel:
            return GENERIC_VENDOR
        return default

    def __contains__(self, item):
        return item in self.vendors

    def __iter__(self):
        return iter(self.vendors)


registry = Registry()
