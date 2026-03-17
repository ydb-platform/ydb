from collections.abc import MutableMapping
from typing import Any

from typing_extensions import override

class AttributeDictMixin:
    def __getattr__(self, k: str) -> Any: ...
    @override
    def __setattr__(self, key: str, value: Any) -> None: ...

class ChainMap(  # type: ignore[misc]  # pyright: ignore[reportImplicitAbstractClass]
    MutableMapping[str, Any]
): ...
class ConfigurationView(  # type: ignore[misc]  # pyright: ignore[reportImplicitAbstractClass]
    ChainMap, AttributeDictMixin
): ...
