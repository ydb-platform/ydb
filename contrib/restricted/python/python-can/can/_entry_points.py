import importlib
import sys
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any


@dataclass
class _EntryPoint:
    key: str
    module_name: str
    class_name: str

    def load(self) -> Any:
        module = importlib.import_module(self.module_name)
        return getattr(module, self.class_name)


# See https://docs.python.org/3/library/importlib.metadata.html#entry-points,
# "Compatibility Note".
if sys.version_info >= (3, 10):

    def read_entry_points(group: str) -> list[_EntryPoint]:
        return [
            _EntryPoint(ep.name, ep.module, ep.attr) for ep in entry_points(group=group)
        ]

else:

    def read_entry_points(group: str) -> list[_EntryPoint]:
        return [
            _EntryPoint(ep.name, *ep.value.split(":", maxsplit=1))
            for ep in entry_points().get(group, [])  # pylint: disable=no-member
        ]
