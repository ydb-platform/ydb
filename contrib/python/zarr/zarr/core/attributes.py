from __future__ import annotations

from collections.abc import MutableMapping
from typing import TYPE_CHECKING

from zarr.core.common import JSON

if TYPE_CHECKING:
    from collections.abc import Iterator

    from zarr.core.group import Group
    from zarr.types import AnyArray


class Attributes(MutableMapping[str, JSON]):
    def __init__(self, obj: AnyArray | Group) -> None:
        # key=".zattrs", read_only=False, cache=True, synchronizer=None
        self._obj = obj

    def __getitem__(self, key: str) -> JSON:
        return self._obj.metadata.attributes[key]

    def __setitem__(self, key: str, value: JSON) -> None:
        new_attrs = dict(self._obj.metadata.attributes)
        new_attrs[key] = value
        self._obj = self._obj.update_attributes(new_attrs)

    def __delitem__(self, key: str) -> None:
        new_attrs = dict(self._obj.metadata.attributes)
        del new_attrs[key]
        self.put(new_attrs)

    def __iter__(self) -> Iterator[str]:
        return iter(self._obj.metadata.attributes)

    def __len__(self) -> int:
        return len(self._obj.metadata.attributes)

    def put(self, d: dict[str, JSON]) -> None:
        """
        Overwrite all attributes with the values from `d`.

        Equivalent to the following pseudo-code, but performed atomically.

        ```python
        attrs = {"a": 1, "b": 2}
        attrs.clear()
        attrs.update({"a": "3", "c": 4})
        print(attrs)
        #> {'a': '3', 'c': 4}
        ```
        """
        self._obj.metadata.attributes.clear()
        self._obj = self._obj.update_attributes(d)

    def asdict(self) -> dict[str, JSON]:
        return dict(self._obj.metadata.attributes)
