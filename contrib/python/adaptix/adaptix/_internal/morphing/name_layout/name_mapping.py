from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Callable, Optional, Union

from ...common import EllipsisType
from ...model_tools.definitions import BaseField, BaseShape, OutputField, is_valid_field_id
from ...provider.essential import CannotProvide, Mediator, Provider
from ...provider.loc_stack_filtering import Pred
from ...provider.located_request import LocatedRequest
from ...provider.methods_provider import MethodsProvider, method_handler
from .base import Key, KeyPath

RawKey = Union[Key, EllipsisType]
RawPath = Iterable[RawKey]
MapResult = Union[RawKey, RawPath, None]
NameMap = Union[
    Mapping[str, MapResult],
    Iterable[
        Union[
            Mapping[str, MapResult],
            tuple[Pred, MapResult],
            tuple[Pred, Callable[[BaseShape, BaseField], MapResult]],
            Provider,
        ]
    ],
]


@dataclass(frozen=True)
class NameMappingRequest(LocatedRequest[Optional[KeyPath]]):
    shape: BaseShape
    field: BaseField
    generated_key: Key


def resolve_map_result(generated_key: Key, map_result: MapResult) -> Optional[KeyPath]:
    if map_result is None:
        return None
    if isinstance(map_result, (str, int)):
        return (map_result, )
    if isinstance(map_result, EllipsisType):
        return (generated_key,)
    return tuple(generated_key if isinstance(key, EllipsisType) else key for key in map_result)


class NameMappingProvider(MethodsProvider, ABC):
    @abstractmethod
    @method_handler
    def provide_name_mapping(self, mediator: Mediator, request: NameMappingRequest) -> Optional[KeyPath]:
        ...


class DictNameMappingProvider(NameMappingProvider):
    def __init__(self, name_map: Mapping[str, MapResult]):
        self._name_map = name_map
        self._validate()

    def _validate(self) -> None:
        invalid_keys = [key for key in self._name_map if not is_valid_field_id(key)]
        if invalid_keys:
            raise ValueError(
                "Keys of dict name mapping must be valid field_id (valid python identifier)."
                f" Keys {invalid_keys!r} does not meet this condition.",
            )

    def provide_name_mapping(self, mediator: Mediator, request: NameMappingRequest) -> Optional[KeyPath]:
        try:
            map_result = self._name_map[request.field.id]
        except KeyError:
            raise CannotProvide
        return resolve_map_result(request.generated_key, map_result)


class ConstNameMappingProvider(NameMappingProvider):
    def __init__(self, result: MapResult):
        self._result = result

    def provide_name_mapping(self, mediator: Mediator, request: NameMappingRequest) -> Optional[KeyPath]:
        return resolve_map_result(request.generated_key, self._result)


class FuncNameMappingProvider(NameMappingProvider):
    def __init__(self, func: Callable[[BaseShape, BaseField], MapResult]):
        self._func = func

    def provide_name_mapping(self, mediator: Mediator, request: NameMappingRequest) -> Optional[KeyPath]:
        result = self._func(request.shape, request.field)
        return resolve_map_result(request.generated_key, result)


class SkipPrivateFieldsNameMappingProvider(NameMappingProvider):
    def provide_name_mapping(self, mediator: Mediator, request: NameMappingRequest) -> Optional[KeyPath]:
        if not isinstance(request.field, OutputField):
            raise CannotProvide
        if request.field.id.startswith("_"):
            return None
        raise CannotProvide
