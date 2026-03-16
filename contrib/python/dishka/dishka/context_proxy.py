from collections.abc import Iterator, MutableMapping
from typing import Any, NoReturn

from .entities.key import DependencyKey


class ContextProxy(MutableMapping[DependencyKey, Any]):
    def __init__(
            self,
            context: dict[DependencyKey, Any],
            cache: dict[DependencyKey, Any],
    ) -> None:
        self._cache = cache
        self._context = context

    def __setitem__(self, key: DependencyKey, value: Any) -> None:
        self._cache[key] = value
        self._context[key] = value

    def __delitem__(self, key: DependencyKey) -> NoReturn:
        raise RuntimeError(  # noqa: TRY003
            "Cannot delete anything from context",
        )

    def __getitem__(self, key: DependencyKey) -> Any:
        return self._cache[key]

    def __len__(self) -> int:
        return len(self._cache)

    def __iter__(self) -> Iterator[DependencyKey]:
        return iter(self._cache)
