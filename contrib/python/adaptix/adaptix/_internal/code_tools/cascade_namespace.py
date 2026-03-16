from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from typing import Optional

from .utils import NAME_TO_BUILTIN


class CascadeNamespace(ABC):
    @abstractmethod
    def add_constant(self, name: str, value: object) -> None:
        ...

    @abstractmethod
    def try_add_constant(self, name: str, value: object) -> bool:
        ...

    @abstractmethod
    def register_var(self, name: str) -> None:
        ...

    @abstractmethod
    def try_register_var(self, name: str) -> bool:
        ...


class BuiltinCascadeNamespace(CascadeNamespace):
    __slots__ = ("_allow_builtins", "_constants", "_occupied", "_outer_constants", "_variables")

    def __init__(
        self,
        constants: Optional[Mapping[str, object]] = None,
        outer_constants: Optional[Mapping[str, object]] = None,
        occupied: Optional[Set[str]] = None,
        *,
        allow_builtins: bool = False,
    ):
        self._constants = {} if constants is None else dict(constants)
        self._outer_constants = {} if outer_constants is None else dict(outer_constants)
        self._occupied = set() if occupied is None else occupied
        self._variables: set[str] = set()
        self._allow_builtins = allow_builtins

    def try_add_constant(self, name: str, value: object) -> bool:
        if (
            name in self._occupied
            or name in self._variables
            or name in self._outer_constants
            or (name in NAME_TO_BUILTIN and not self._allow_builtins)
        ):
            return False
        if name in self._constants:
            return value is self._constants[name]
        self._constants[name] = value
        return True

    def try_add_outer_constant(self, name: str, value: object) -> bool:
        if (
            name in self._constants
            or (name in NAME_TO_BUILTIN and not self._allow_builtins)
        ):
            return False
        if name in self._outer_constants:
            return value is self._outer_constants[name]
        self._outer_constants[name] = value
        return True

    def try_register_var(self, name: str) -> bool:
        if (
            name in self._occupied
            or name in self._constants
            or name in self._variables
            or (name in NAME_TO_BUILTIN and not self._allow_builtins)
        ):
            return False
        self._variables.add(name)
        return True

    def add_constant(self, name: str, value: object) -> None:
        if not self.try_add_constant(name, value):
            raise KeyError(f"Key {name} is duplicated")

    def add_outer_constant(self, name: str, value: object) -> None:
        if not self.try_add_outer_constant(name, value):
            raise KeyError(f"Key {name} is duplicated")

    def register_var(self, name: str) -> None:
        if not self.try_register_var(name):
            raise KeyError(f"Key {name} is duplicated")

    @property
    def all_constants(self) -> Mapping[str, object]:
        return {**self._constants, **self._outer_constants}
