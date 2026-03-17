from abc import ABC
from copy import copy
from dataclasses import MISSING, fields, is_dataclass, replace
from enum import Enum
from typing import (
    Annotated,
    Any,
    Callable,
    ClassVar,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)


class MergeForbiddenError(Exception):
    pass


class Special(Enum):
    NOT_SET = "<NOT SET>"


T = TypeVar("T")


class Merger(ABC):
    def __call__(self, name: str, x: Union[T, Special], y: Union[T, Special]) -> Union[T, Special]:
        if x is Special.NOT_SET:
            return y
        if y is Special.NOT_SET:
            return x
        return self._merge(name, x, y)

    def _merge(self, name: str, x: T, y: T) -> T:
        raise NotImplementedError


class UseFirst(Merger):
    def _merge(self, name: str, x: T, y: T) -> T:
        return x


class UseLast(Merger):
    def _merge(self, name: str, x: T, y: T) -> T:
        return y


class Forbid(Merger):
    def _merge(self, name: str, x: T, y: T) -> T:
        raise MergeForbiddenError(f"Override is forbidden for field {name}")


class ForbidChange(Merger):
    def _merge(self, name: str, x: T, y: T) -> T:
        if x == y:
            return x
        raise MergeForbiddenError(f"Override with different value is forbidden for field {name}:\nOld: {x}\nNew: {y}")


class Concat(Merger):
    def _merge(self, name: str, x: T, y: T) -> T:
        return x + y  # type: ignore[operator]


class Unite(Merger):
    def _merge(self, name: str, x: T, y: T) -> T:
        return x | y  # type: ignore[operator]


class Merge(Merger):
    def _merge(self, name: str, x: "ModelT", y: "ModelT") -> "ModelT":  # type: ignore[override]
        return merge(x, y)


class DictMerge(Merger):
    def __init__(self, value_merger: Merger = Forbid()):
        self.value_merger = value_merger

    def _merge(self, name: str, x: dict, y: dict) -> dict:  # type: ignore[override]
        result = copy(x)
        for key, value in y.items():
            if key in result:
                result[key] = self.value_merger(key, result[key], value)
            else:
                result[key] = value
        return result


class ApplyFunc(Merger):
    def __init__(self, func: Callable):
        self.func = func

    def __call__(self, name: str, x: Union[T, Special], y: Union[T, Special]) -> Union[T, Special]:
        if x is Special.NOT_SET:
            return y
        if y is Special.NOT_SET:
            return x
        return self.func(x, y)


def _get_merger(hint: Any):
    if get_origin(hint) is not Annotated:
        return ForbidChange()
    for arg in get_args(hint):
        if isinstance(arg, Merger):
            return arg
    return ForbidChange()


class BaseMeshModel:
    _field_mergers: ClassVar[dict[str, Merger]]

    def __init__(self, **kwargs):
        if extra_keys := (kwargs.keys() - self._field_mergers.keys()):
            raise ValueError(f"Extra arguments: {extra_keys}")
        self.__dict__.update(kwargs)

    def unset_attrs(self):
        return type(self)._field_mergers.keys() - vars(self).keys()

    def __repr__(self):
        return f"{self.__class__.__name__}(" + ", ".join(f"{key}={value}" for key, value in vars(self).items()) + ")"

    def __init_subclass__(cls, **kwargs):
        cls._field_mergers = {
            field: _get_merger(hint)
            for field, hint in get_type_hints(cls, include_extras=True).items()
            if get_origin(hint) is not ClassVar
        }

    def __setattr__(self, key, value):
        if key not in self._field_mergers:
            raise AttributeError(f"{self.__class__.__name__} has no field {key}")
        super().__setattr__(key, value)

    def is_empty(self):
        return not self.__dict__


ModelT = TypeVar("ModelT", bound=BaseMeshModel)


def _merge(a: ModelT, b: BaseMeshModel) -> ModelT:
    result = copy(a)
    for attr_name, merger in a._field_mergers.items():
        aval = getattr(a, attr_name, Special.NOT_SET)
        bval = getattr(b, attr_name, Special.NOT_SET)
        if is_dataclass(aval) and is_dataclass(bval):
            new_dto = merge_dataclass(aval, bval)
            setattr(result, attr_name, new_dto)
        else:
            new_value = merger(attr_name, aval, bval)
            if new_value is not Special.NOT_SET:
                setattr(result, attr_name, new_value)
    return result


@overload
def merge(first: Special, second: ModelT, /, *others: BaseMeshModel) -> ModelT:
    pass


@overload
def merge(first: ModelT, /, *others: BaseMeshModel) -> ModelT:
    pass


@overload
def merge(first: Special, /) -> Special:
    pass


def merge(first: Any, /, *others: Any) -> Any:
    if first is Special.NOT_SET:
        if not others:
            return Special.NOT_SET
        return merge(*others)
    for second in others:
        first = _merge(first, second)
    return first


def merge_dataclass(a: Any, b: Any) -> Any:
    if a.__class__ != b.__class__:
        raise MergeForbiddenError(f"Dataclasses belonging to different instaces can't be merged:\nOld: {a}\nNew: {b}")

    if a == b:
        return replace(a)

    empty = dataclass_default(a.__class__)
    merged = {}
    for f in fields(empty):
        default = getattr(empty, f.name)
        aval = getattr(a, f.name)
        bval = getattr(b, f.name)
        if aval == default:
            merged[f.name] = bval
        elif bval == default:
            merged[f.name] = aval
        elif aval == bval:
            merged[f.name] = aval
        elif is_dataclass(aval) and is_dataclass(bval):
            merged[f.name] = merge_dataclass(aval, bval)
        else:
            raise MergeForbiddenError(
                f"Dataclasses with non-equal field {f.name} can't be merged:\nOld: {aval}\nNew: {bval}"
            )

    return replace(empty, **merged)


def is_dataclass_empty(a: Any) -> bool:
    try:
        dataclass_default(a.__class__)
    except ValueError:
        return False
    return True


def dataclass_default(dataclass_cls: type[T]) -> T:
    if not is_dataclass(dataclass_cls):
        raise TypeError(f"Class {dataclass_cls} is not a dataclass")

    non_default_fields = []
    for f in fields(dataclass_cls):
        if f.default == MISSING and f.default_factory == MISSING:
            non_default_fields.append(f.name)

    if non_default_fields:
        raise ValueError(f"Class {dataclass_cls} has non-default fields: {' '.join(non_default_fields)}")

    return cast(T, dataclass_cls())


class KeyDefaultDict(dict):
    def __init__(self, factory: Callable[[str], Any]):
        super().__init__()
        self.factory = factory

    def __missing__(self, key):
        x = self[key] = self.factory(key)
        return x
