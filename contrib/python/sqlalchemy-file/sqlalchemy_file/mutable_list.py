import typing
from typing import Any, List, Tuple, TypeVar, no_type_check

from sqlalchemy.ext.mutable import Mutable

T = TypeVar("T", bound=Any)


class MutableList(Mutable, typing.List[T]):
    """A list type that implements :class:`Mutable`.

    The :class:`MutableList` object implements a list that will
    emit change events to the underlying mapping when the contents of
    the list are altered, including when values are added or removed.

    This is a replication of default Mutablelist provide by SQLAlchemy.
    The difference here is the properties _removed which keep every element
    removed from the list in order to be able to delete them after commit
    and keep them when session rolled back.

    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self._removed: List[T] = []

    @classmethod
    def coerce(cls, key: Any, value: Any) -> Any:
        if not isinstance(value, MutableList):
            if isinstance(value, list):
                return MutableList(value)
            # this call will raise ValueError
            return Mutable.coerce(key, value)  # pragma: no cover
        return value  # pragma: no cover

    @no_type_check
    def __reduce_ex__(self, proto):  # pragma: no cover
        return self.__class__, (list(self),)

    # needed for backwards compatibility with
    # older pickles
    def __getstate__(self) -> Tuple[List[T], List[T]]:  # pragma: no cover
        return list(self), self._removed

    def __setstate__(self, state: Any) -> None:  # pragma: no cover
        self[:] = state[0]
        self._removed = state[1]

    def __setitem__(self, index: Any, value: Any) -> None:
        """Detect list set events and emit change events."""
        old_value = self[index] if isinstance(index, slice) else [self[index]]
        list.__setitem__(self, index, value)
        self.changed()
        self._removed.extend(old_value)

    def __delitem__(self, index: Any) -> None:
        """Detect list del events and emit change events."""
        old_value = self[index] if isinstance(index, slice) else [self[index]]
        list.__delitem__(self, index)
        self.changed()
        self._removed.extend(old_value)

    def pop(self, *arg) -> "T":  # type: ignore
        result = list.pop(self, *arg)
        self.changed()
        self._removed.append(result)
        return result

    def append(self, x: Any) -> None:
        list.append(self, x)
        self.changed()

    def extend(self, x: Any) -> None:
        list.extend(self, x)
        self.changed()

    @no_type_check
    def __iadd__(self, x):
        self.extend(x)
        return self

    def insert(self, i: Any, x: Any) -> None:
        list.insert(self, i, x)
        self.changed()

    def remove(self, i: "T") -> None:
        list.remove(self, i)
        self._removed.append(i)
        self.changed()

    def clear(self) -> None:
        self._removed.extend(self)
        list.clear(self)  # type: ignore
        self.changed()

    def sort(self, **kw: Any) -> None:
        list.sort(self, **kw)
        self.changed()

    def reverse(self) -> None:
        list.reverse(self)  # type: ignore
        self.changed()
