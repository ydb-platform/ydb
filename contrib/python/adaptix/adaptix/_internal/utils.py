import itertools
import sys
import warnings
from abc import ABC, abstractmethod
from collections.abc import Collection, Generator, Iterable, Iterator, Mapping
from contextlib import contextmanager
from copy import copy
from typing import Any, Callable, Generic, Protocol, TypeVar, Union, final, overload

from .feature_requirement import HAS_NATIVE_EXC_GROUP, HAS_PY_310, HAS_PY_311

C = TypeVar("C", bound="Cloneable")


class Cloneable(ABC):
    @abstractmethod
    def _calculate_derived(self) -> None:
        return

    @contextmanager
    @final
    def _clone(self: C) -> Generator[C, Any, Any]:
        self_copy = copy(self)
        try:
            yield self_copy
        finally:
            self_copy._calculate_derived()


class ForbiddingDescriptor:
    def __init__(self):
        self._name = None

    def __set_name__(self, owner, name):
        self._name = name

    def __get__(self, instance, owner):
        raise AttributeError(f"Cannot read {self._name!r} attribute")

    def __set__(self, instance, value):
        raise AttributeError(f"Cannot set {self._name!r} attribute")

    def __delete__(self, instance):
        raise AttributeError(f"Cannot delete {self._name!r} attribute")


def _singleton_repr(self):
    return f"{type(self).__name__}()"


def _singleton_hash(self) -> int:
    return hash(type(self))


def _singleton_copy(self):
    return self


def _singleton_deepcopy(self, memo):
    return self


def _singleton_new(cls):
    return cls._instance


class SingletonMeta(type):
    def __new__(mcs, name, bases, namespace, **kwargs):
        namespace.setdefault("__repr__", _singleton_repr)
        namespace.setdefault("__str__", _singleton_repr)
        namespace.setdefault("__hash__", _singleton_hash)
        namespace.setdefault("__copy__", _singleton_copy)
        namespace.setdefault("__deepcopy__", _singleton_deepcopy)
        namespace.setdefault("__slots__", ())
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        instance = super().__call__(cls)
        cls._instance = instance
        if "__new__" not in cls.__dict__:
            cls.__new__ = _singleton_new
        return cls

    def __call__(cls):
        return cls._instance


T = TypeVar("T")

if HAS_PY_310:
    pairs = itertools.pairwise
else:
    def pairs(iterable: Iterable[T]) -> Iterable[tuple[T, T]]:  # type: ignore[no-redef]
        it = iter(iterable)
        try:
            prev = next(it)
        except StopIteration:
            return

        for current in it:
            yield prev, current
            prev = current


class Omitted(metaclass=SingletonMeta):
    def __bool__(self):
        raise TypeError("Omitted() cannot be used in boolean context")


Omittable = Union[T, Omitted]


ComparableSeqT = TypeVar("ComparableSeqT", bound="ComparableSequence")


class ComparableSequence(Protocol[T]):
    def __lt__(self, __other: T, /) -> bool:
        ...

    @overload
    def __getitem__(self, index: int) -> T:
        ...

    @overload
    def __getitem__(self: ComparableSeqT, index: slice) -> ComparableSeqT:
        ...

    def __iter__(self) -> Iterator[T]:
        ...

    def __len__(self) -> int:
        ...

    def __contains__(self, value: object) -> bool:
        ...

    def __reversed__(self) -> Iterator[T]:
        ...


def get_prefix_groups(
    values: Collection[ComparableSeqT],
) -> Collection[tuple[ComparableSeqT, Iterable[ComparableSeqT]]]:
    groups: list[tuple[ComparableSeqT, list[ComparableSeqT]]] = []
    sorted_values = iter(sorted(values))
    current_group: list[ComparableSeqT] = []
    try:
        prefix = next(sorted_values)
    except StopIteration:
        return []

    for value in sorted_values:
        if value[:len(prefix)] == prefix:
            current_group.append(value)
        else:
            if current_group:
                groups.append((prefix, current_group))
                current_group = []
            prefix = value

    if current_group:
        groups.append((prefix, current_group))
    return groups


def copy_exception_dunders(source: BaseException, target: BaseException) -> None:
    if hasattr(source, "__notes__"):
        target.__notes__ = source.__notes__
    elif hasattr(target, "__notes__"):
        delattr(target, "__notes__")
    target.__context__ = source.__context__
    target.__cause__ = source.__cause__
    target.__traceback__ = source.__traceback__
    target.__suppress_context__ = source.__suppress_context__


if HAS_NATIVE_EXC_GROUP:
    def add_note(exc: BaseException, note: str) -> None:
        exc.add_note(note)
else:
    def add_note(exc: BaseException, note: str) -> None:
        if hasattr(exc, "__notes__"):
            exc.__notes__.append(note)
        else:
            exc.__notes__ = [note]


def get_notes(exc: BaseException) -> list[str]:
    return getattr(exc, "__notes__", [])


ClassT = TypeVar("ClassT", bound=type)


def with_module(module: str) -> Callable[[ClassT], ClassT]:
    def decorator(cls):
        cls.__module__ = module
        return cls

    return decorator


def fix_dataclass_from_builtin(cls: ClassT) -> ClassT:
    """Dataclass decorator fails on python 3.8-3.10
    when you run python with -OO flags
    trying to get signature from Exception class.
    """
    if not HAS_PY_311 and cls.__doc__ is None:
        cls.__doc__ = "This is stub documentation generated by fix_dataclass_from_builtin function"
    return cls


def create_deprecated_alias_getter(module_name, old_name_to_new_name):
    def deprecated_alias_getter(name):
        if name not in old_name_to_new_name:
            raise AttributeError(f"module {module_name!r} has no attribute {name!r}")

        new_name = old_name_to_new_name[name]
        warnings.warn(
            f"Name {name!r} is deprecated, use {new_name!r} instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(sys.modules[module_name], new_name)

    return deprecated_alias_getter


MappingT = TypeVar("MappingT", bound=Mapping)


class OrderedMappingHashWrapper(Generic[MappingT]):
    __slots__ = ("_hash", "mapping")

    def __init__(self, mapping: MappingT):
        self.mapping = mapping
        self._hash = hash(tuple(self.mapping.items()))

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        if isinstance(other, OrderedMappingHashWrapper):
            return self.mapping == other.mapping
        return NotImplemented

    def __repr__(self):
        return f"OrderedMappingHashWrapper({self.mapping})"


class MappingHashWrapper(Generic[MappingT]):
    __slots__ = ("_hash", "mapping")

    def __init__(self, mapping: MappingT):
        self.mapping = mapping
        self._hash = hash(frozenset(self.mapping.items()))

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        if isinstance(other, OrderedMappingHashWrapper):
            return self.mapping == other.mapping
        return NotImplemented

    def __repr__(self):
        return f"MappingHashWrapper({self.mapping})"


class AlwaysEqualHashWrapper(Generic[T]):
    __slots__ = ("value", )

    def __init__(self, value: T):
        self.value = value

    def __hash__(self):
        return 0

    def __eq__(self, other):
        if isinstance(other, AlwaysEqualHashWrapper):
            return True
        return NotImplemented

    def __repr__(self):
        return f"AlwaysEqualHashWrapper({self.value})"
