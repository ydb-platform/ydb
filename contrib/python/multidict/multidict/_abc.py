import abc
from collections.abc import Iterable, Mapping, MutableMapping
from typing import TYPE_CHECKING, Protocol, TypeVar, Union, overload

if TYPE_CHECKING:
    from ._multidict_py import istr
else:
    istr = str

_V = TypeVar("_V")
_V_co = TypeVar("_V_co", covariant=True)
_T = TypeVar("_T")


class SupportsKeys(Protocol[_V_co]):
    def keys(self) -> Iterable[str]: ...
    def __getitem__(self, key: str, /) -> _V_co: ...


class SupportsIKeys(Protocol[_V_co]):
    def keys(self) -> Iterable[istr]: ...
    def __getitem__(self, key: istr, /) -> _V_co: ...


MDArg = SupportsKeys[_V] | SupportsIKeys[_V] | Iterable[tuple[str, _V]] | None


class MultiMapping(Mapping[str, _V_co]):
    @overload
    def getall(self, key: str) -> list[_V_co]: ...
    @overload
    def getall(self, key: str, default: _T) -> list[_V_co] | _T: ...
    @abc.abstractmethod
    def getall(self, key: str, default: _T = ...) -> list[_V_co] | _T:
        """Return all values for key."""

    @overload
    def getone(self, key: str) -> _V_co: ...
    @overload
    def getone(self, key: str, default: _T) -> _V_co | _T: ...
    @abc.abstractmethod
    def getone(self, key: str, default: _T = ...) -> _V_co | _T:
        """Return first value for key."""

    def to_dict(self) -> dict[str, list[_V_co]]:
        """Return a dict with lists of all values for each key.

        Deliberately concrete, not abstract: ``MultiMapping`` is public, so
        requiring a new method would stop every existing subclass outside
        this project from being instantiated. Subclasses whose keys compare
        equal under a normalisation the iteration does not apply, such as
        case-insensitive mappings, must override this; the default would
        emit one entry per spelling.
        """
        result: dict[str, list[_V_co]] = {}
        for key in self:
            # Iteration yields a duplicated key once per occurrence, so the
            # membership test is what keeps getall() from being applied twice.
            if key not in result:
                # getall() already returns a fresh list.
                result[key] = self.getall(key)
        return result


class MutableMultiMapping(MultiMapping[_V], MutableMapping[str, _V]):
    @abc.abstractmethod
    def add(self, key: str, value: _V) -> None:
        """Add value to list."""

    @abc.abstractmethod
    def extend(self, arg: MDArg[_V] = None, /, **kwargs: _V) -> None:
        """Add everything from arg and kwargs to the mapping."""

    @abc.abstractmethod
    def merge(self, arg: MDArg[_V] = None, /, **kwargs: _V) -> None:
        """Merge into the mapping, adding non-existing keys."""

    @overload
    def popone(self, key: str) -> _V: ...
    @overload
    def popone(self, key: str, default: _T) -> _V | _T: ...
    @abc.abstractmethod
    def popone(self, key: str, default: _T = ...) -> _V | _T:
        """Remove specified key and return the corresponding value."""

    @overload
    def popall(self, key: str) -> list[_V]: ...
    @overload
    def popall(self, key: str, default: _T) -> list[_V] | _T: ...
    @abc.abstractmethod
    def popall(self, key: str, default: _T = ...) -> list[_V] | _T:
        """Remove all occurrences of key and return the list of corresponding values."""
