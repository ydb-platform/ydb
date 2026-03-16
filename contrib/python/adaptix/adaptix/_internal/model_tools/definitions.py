from abc import ABC, abstractmethod
from collections.abc import Hashable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, Optional, TypeVar, Union

from ..common import Catchable, TypeHint, VarTuple
from ..feature_requirement import DistributionRequirement, DistributionVersionRequirement
from ..struct_trail import Attr, TrailElement
from ..utils import SingletonMeta, pairs

S = TypeVar("S")
T = TypeVar("T")


class NoDefault(metaclass=SingletonMeta):
    pass


@dataclass(frozen=True)
class DefaultValue(Generic[T]):
    value: T

    def __hash__(self):
        try:
            return hash(self.value)
        except TypeError:
            return 236  # some random number that fits in byte


@dataclass(frozen=True)
class DefaultFactory(Generic[T]):
    factory: Callable[[], T]


@dataclass(frozen=True)
class DefaultFactoryWithSelf(Generic[S, T]):
    factory: Callable[[S], T]


Default = Union[NoDefault, DefaultValue[Any], DefaultFactory[Any], DefaultFactoryWithSelf[Any, Any]]


class Accessor(Hashable, ABC):
    @property
    @abstractmethod
    def getter(self) -> Callable[[Any], Any]:
        ...

    @property
    @abstractmethod
    def access_error(self) -> Optional[Catchable]:
        ...

    @property
    @abstractmethod
    def trail_element(self) -> TrailElement:
        ...


class DescriptorAccessor(Accessor, ABC):
    def __init__(self, attr_name: str, access_error: Optional[Catchable]):
        self._attr_name = attr_name
        self._access_error = access_error

    # noinspection PyMethodOverriding
    def getter(self, obj):
        return getattr(obj, self._attr_name)

    @property
    def access_error(self) -> Optional[Catchable]:
        return self._access_error

    @property
    def trail_element(self) -> TrailElement:
        return Attr(self.attr_name)

    @property
    def attr_name(self) -> str:
        return self._attr_name

    def __eq__(self, other):
        if isinstance(other, DescriptorAccessor):
            return self._attr_name == other._attr_name and self._access_error == other._access_error
        return NotImplemented

    def __hash__(self):
        return hash((self._attr_name, self._access_error))

    def __repr__(self):
        return f"{type(self).__qualname__}(attr_name={self.attr_name!r}, access_error={self.access_error})"


class ItemAccessor(Accessor):
    def __init__(self, key: Union[int, str], access_error: Optional[Catchable], path_element: TrailElement):
        self.key = key
        self._access_error = access_error
        self._path_element = path_element

    # noinspection PyMethodOverriding
    def getter(self, obj):
        return obj[self.key]

    @property
    def access_error(self) -> Optional[Catchable]:
        return self._access_error

    @property
    def trail_element(self) -> TrailElement:
        return self._path_element

    def __eq__(self, other):
        if isinstance(other, ItemAccessor):
            return (
                self.key == other.key
                and self._access_error == other._access_error
                and self._path_element == other._path_element
            )
        return NotImplemented

    def __hash__(self):
        try:
            return hash((self.key, self._access_error))
        except TypeError:
            return hash(self._access_error)

    def __repr__(self):
        return (
            f"{type(self).__qualname__}"
            f"(key={self.key!r}, access_error={self.access_error}, path_element={self.trail_element!r})"
        )


def create_attr_accessor(attr_name: str, *, is_required: bool) -> DescriptorAccessor:
    return DescriptorAccessor(
        attr_name=attr_name,
        access_error=None if is_required else AttributeError,
    )


def create_key_accessor(key: Union[str, int], access_error: Optional[Catchable]) -> ItemAccessor:
    return ItemAccessor(
        key=key,
        access_error=access_error,
        path_element=key,
    )


def is_valid_field_id(value: str) -> bool:
    return value.isidentifier()


@dataclass(frozen=True)
class BaseField:
    id: str
    type: TypeHint
    default: Default
    metadata: Mapping[Any, Any] = field(hash=False)
    # Mapping almost never defines __hash__,
    # so it will be more convenient to exclude this field
    # from hash computation
    original: Any = field(hash=False)

    def __post_init__(self):
        if not is_valid_field_id(self.id):
            raise ValueError(f"Field id must be python identifier, now it is a {self.id!r}")


@dataclass(frozen=True)
class InputField(BaseField):
    is_required: bool

    @property
    def is_optional(self):
        return not self.is_required


@dataclass(frozen=True)
class OutputField(BaseField):
    accessor: Accessor

    @property
    def is_optional(self) -> bool:
        return not self.is_required

    @property
    def is_required(self) -> bool:
        return self.accessor.access_error is None


@dataclass(frozen=True)
class BaseShape:
    """Signature of class, it is divided into two parts: input and output.
    See doc :class InputShape: and :class OutputShape: for more details
    """
    fields: VarTuple[BaseField]
    overriden_types: frozenset[str]
    fields_dict: Mapping[str, BaseField] = field(init=False, hash=False, repr=False, compare=False)

    def _validate(self):
        field_ids = {fld.id for fld in self.fields}
        if len(field_ids) != len(self.fields):
            duplicates = {
                fld.id for fld in self.fields
                if fld.id in field_ids
            }
            raise ValueError(f"Field ids {duplicates} are duplicated")

        wild_overriden_types = self.overriden_types - field_ids
        if wild_overriden_types:
            raise ValueError(f"overriden_types contains non existing fields {wild_overriden_types}")

    def __post_init__(self):
        super().__setattr__("fields_dict", {fld.id: fld for fld in self.fields})
        self._validate()


class ParamKind(Enum):
    POS_ONLY = 0
    POS_OR_KW = 1
    KW_ONLY = 3  # 2 is for VAR_POS

    def __repr__(self):
        return f"{type(self).__name__}.{self.name}"


@dataclass(frozen=True)
class Param:
    field_id: str
    name: str
    kind: ParamKind

    def _validate(self) -> None:
        if not self.name.isidentifier():
            raise ValueError(f"Parameter name must be python identifier, now it is a {self.name!r}")
        if not is_valid_field_id(self.field_id):
            raise ValueError(f"Field id must be python identifier, now it is a {self.field_id!r}")

    def __post_init__(self):
        self._validate()


@dataclass(frozen=True)
class ParamKwargs:
    type: TypeHint


@dataclass(frozen=True)
class InputShape(BaseShape, Generic[T]):
    """Description of desired object creation

    :param constructor: Callable that produces an instance of the class.
    :param fields: Parameters of the constructor
    """
    fields: VarTuple[InputField]
    params: VarTuple[Param]
    kwargs: Optional[ParamKwargs]
    constructor: Callable[..., T]
    fields_dict: Mapping[str, InputField] = field(init=False, hash=False, repr=False, compare=False)

    @property
    def allow_kwargs(self) -> bool:
        return self.kwargs is not None

    def _validate(self):
        super()._validate()

        param_names = {param.name for param in self.params}
        if len(param_names) != len(self.params):
            duplicates = {
                param.name for param in self.params
                if param.name in param_names
            }
            raise ValueError(f"Parameter names {duplicates} are duplicated")

        wild_params = {param.name: param.field_id for param in self.params if param.field_id not in self.fields_dict}
        if wild_params:
            raise ValueError(f"Parameters {wild_params} bind to non-existing fields")

        wild_fields = self.fields_dict.keys() - {param.field_id for param in self.params}
        if wild_fields:
            raise ValueError(f"Fields {wild_fields} do not bound to any parameter")

        for past, current in pairs(self.params):
            if past.kind.value > current.kind.value:
                raise ValueError(
                    f"Inconsistent order of fields, {current.kind} must be after {past.kind}",
                )

            if (
                self.fields_dict[past.field_id].is_optional
                and self.fields_dict[current.field_id].is_required
                and current.kind != ParamKind.KW_ONLY
            ):
                raise ValueError(
                    f"All not required fields must be after required ones"
                    f" except {ParamKind.KW_ONLY} fields",
                )

        for param in self.params:
            if param.kind == ParamKind.POS_ONLY and self.fields_dict[param.field_id].is_optional:
                raise ValueError(f"Field {param.field_id!r} cannot be positional only and optional")


@dataclass(frozen=True)
class OutputShape(BaseShape):
    """Description of extraction data from an object

    :param fields: Fields (can be not only attributes) of an object
    """
    fields: VarTuple[OutputField]
    fields_dict: Mapping[str, OutputField] = field(init=False, hash=False, repr=False, compare=False)


Inp = TypeVar("Inp", bound=Optional[InputShape])
Out = TypeVar("Out", bound=Optional[OutputShape])


@dataclass(frozen=True)
class Shape(Generic[Inp, Out]):
    input: Inp
    output: Out


FullShape = Shape[InputShape, OutputShape]
ShapeIntrospector = Callable[[Any], Shape]


class IntrospectionError(Exception):
    pass


class NoTargetPackageError(IntrospectionError):
    def __init__(self, requirement: DistributionRequirement):
        self.requirement = requirement


class TooOldPackageError(IntrospectionError):
    def __init__(self, requirement: DistributionVersionRequirement):
        self.requirement = requirement


class ClarifiedIntrospectionError(IntrospectionError):
    def __init__(self, description: str):
        self.description = description
