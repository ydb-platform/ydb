import typing
from typing import Any


class BaseFile(typing.Dict[str, Any]):
    """Base class for file object.

    It keeps information on a content related to a specific storage.
    It is a specialized dictionary that provides also attribute style access,
    the dictionary parent permits easy encoding/decoding to JSON.

    """

    def __getitem__(self, key: str) -> Any:
        return dict.__getitem__(self, key)

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setitem__(self, key: str, value: Any) -> None:
        if getattr(self, "_frozen", False):
            raise TypeError("Already saved files are immutable")
        return dict.__setitem__(self, key, value)

    __setattr__ = __setitem__

    def __delattr__(self, name: str) -> None:
        if getattr(self, "_frozen", False):
            raise TypeError("Already saved files are immutable")

        try:
            del self[name]
        except KeyError:
            raise AttributeError(name)

    def __delitem__(self, key: str) -> None:
        if object.__getattribute__(self, "_frozen"):
            raise TypeError("Already saved files are immutable")
        dict.__delitem__(self, key)

    def _freeze(self) -> None:
        object.__setattr__(self, "_frozen", True)

    def _thaw(self) -> None:
        object.__setattr__(self, "_frozen", False)
