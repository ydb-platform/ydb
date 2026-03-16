import inspect
from collections.abc import Container, Iterable
from dataclasses import dataclass, replace
from typing import Any, Generic, Optional, TypeVar, Union, cast

from ..common import TypeHint
from ..model_tools.definitions import (
    ClarifiedIntrospectionError,
    DescriptorAccessor,
    InputShape,
    IntrospectionError,
    OutputField,
    OutputShape,
    Shape,
    ShapeIntrospector,
    TooOldPackageError,
)
from ..model_tools.introspection.attrs import get_attrs_shape
from ..model_tools.introspection.class_init import get_class_init_shape
from ..model_tools.introspection.dataclass import get_dataclass_shape
from ..model_tools.introspection.msgspec import get_msgspec_shape
from ..model_tools.introspection.named_tuple import get_named_tuple_shape
from ..model_tools.introspection.pydantic import get_pydantic_shape
from ..model_tools.introspection.sqlalchemy import get_sqlalchemy_shape
from ..model_tools.introspection.typed_dict import get_typed_dict_shape
from ..provider.essential import CannotProvide, Mediator
from ..provider.loc_stack_filtering import create_loc_stack_checker
from ..type_tools.generic_resolver import GenericResolver, MembersStorage
from .essential import RequestChecker
from .located_request import LocatedRequest, LocatedRequestChecker
from .methods_provider import MethodsProvider, method_handler
from .provider_wrapper import ConcatProvider


@dataclass(frozen=True)
class InputShapeRequest(LocatedRequest[InputShape]):
    pass


@dataclass(frozen=True)
class OutputShapeRequest(LocatedRequest[OutputShape]):
    pass


class ShapeProvider(MethodsProvider):
    def __init__(self, introspector: ShapeIntrospector):
        self._introspector = introspector

    def __repr__(self):
        return f"{type(self)}({self._introspector})"

    def _get_shape(self, tp) -> Shape:
        try:
            return self._introspector(tp)
        except TooOldPackageError as e:
            raise CannotProvide(message=e.requirement.fail_reason, is_demonstrative=True) from e
        except ClarifiedIntrospectionError as e:
            raise CannotProvide(message=e.description, is_demonstrative=True) from e
        except IntrospectionError as e:
            raise CannotProvide from e

    @method_handler
    def _provide_input_shape(self, mediator: Mediator, request: InputShapeRequest) -> InputShape:
        shape = mediator.cached_call(self._get_shape, request.last_loc.type)
        if shape.input is None:
            raise CannotProvide
        return shape.input

    @method_handler
    def _provide_output_shape(self, mediator: Mediator, request: OutputShapeRequest) -> OutputShape:
        shape = mediator.cached_call(self._get_shape, request.last_loc.type)
        if shape.output is None:
            raise CannotProvide
        return shape.output


BUILTIN_SHAPE_PROVIDER = ConcatProvider(
    ShapeProvider(get_named_tuple_shape),
    ShapeProvider(get_typed_dict_shape),
    ShapeProvider(get_dataclass_shape),
    ShapeProvider(get_msgspec_shape),
    ShapeProvider(get_attrs_shape),
    ShapeProvider(get_sqlalchemy_shape),
    ShapeProvider(get_pydantic_shape),
    # class init introspection must be the last
    ShapeProvider(get_class_init_shape),
)


class PropertyExtender(MethodsProvider):
    def __init__(
        self,
        output_fields: Iterable[OutputField],
        infer_types_for: Container[str],
    ):
        self._output_fields = output_fields
        self._infer_types_for = infer_types_for

        bad_fields_accessors = [
            field for field in self._output_fields if not isinstance(field.accessor, DescriptorAccessor)
        ]
        if bad_fields_accessors:
            raise ValueError(
                f"Fields {bad_fields_accessors} has bad accessors,"
                f" all fields must use DescriptorAccessor",
            )

    @method_handler
    def _provide_output_shape(self, mediator: Mediator[OutputShape], request: OutputShapeRequest) -> OutputShape:
        tp = request.last_loc.type
        shape = mediator.provide_from_next()

        additional_fields = tuple(
            replace(field, type=self._infer_property_type(tp, self._get_attr_name(field)))
            if field.id in self._infer_types_for else
            field
            for field in self._output_fields
        )
        return replace(shape, fields=shape.fields + additional_fields)

    def _get_attr_name(self, field: OutputField) -> str:
        return cast(DescriptorAccessor, field.accessor).attr_name

    def _infer_property_type(self, tp: TypeHint, attr_name: str) -> TypeHint:
        prop = getattr(tp, attr_name)

        if not isinstance(prop, property):
            raise CannotProvide

        if prop.fget is None:
            raise CannotProvide

        signature = inspect.signature(prop.fget)

        if signature.return_annotation is inspect.Signature.empty:
            return Any
        return signature.return_annotation


ShapeT = TypeVar("ShapeT", bound=Union[InputShape, OutputShape])


class ShapeGenericResolver(Generic[ShapeT]):
    def __init__(self, mediator: Mediator, initial_request: LocatedRequest[ShapeT]):
        self._mediator = mediator
        self._initial_request = initial_request

    def provide(self) -> ShapeT:
        resolver = GenericResolver(self._get_members)
        members_storage = resolver.get_resolved_members(
            self._initial_request.last_loc.type,
        )
        if members_storage.meta is None:
            raise CannotProvide
        return replace(
            members_storage.meta,
            fields=tuple(  # type: ignore[arg-type]
                replace(fld, type=members_storage.members[fld.id])
                for fld in members_storage.meta.fields
            ),
        )

    def _get_members(self, tp) -> MembersStorage[str, Optional[ShapeT]]:
        try:
            shape = self._mediator.delegating_provide(
                replace(
                    self._initial_request,
                    loc_stack=self._initial_request.loc_stack.replace_last_type(tp),
                ),
            )
        except CannotProvide:
            return MembersStorage(
                meta=None,
                members={},
                overriden=frozenset(),
            )
        return MembersStorage(
            meta=shape,
            members={field.id: field.type for field in shape.fields},
            overriden=shape.overriden_types,
        )


def provide_generic_resolved_shape(mediator: Mediator, request: LocatedRequest[ShapeT]) -> ShapeT:
    return ShapeGenericResolver(mediator, request).provide()


T = TypeVar("T")


class SimilarShapeProvider(MethodsProvider):
    def __init__(self, target: TypeHint, prototype: TypeHint, *, for_input: bool = True, for_output: bool = True):
        self._target = target
        self._prototype = prototype
        self._loc_stack_checker = create_loc_stack_checker(self._target)
        self._for_input = for_input
        self._for_output = for_output

    def _get_request_checker(self) -> RequestChecker:
        return LocatedRequestChecker(self._loc_stack_checker)

    @method_handler
    def _provide_input_shape(self, mediator: Mediator, request: InputShapeRequest) -> InputShape:
        if not self._for_input:
            raise CannotProvide

        shape = mediator.delegating_provide(
            replace(
                request,
                loc_stack=request.loc_stack.replace_last_type(self._prototype),
            ),
        )
        return replace(shape, constructor=self._target)

    @method_handler
    def _provide_output_shape(self, mediator: Mediator, request: OutputShapeRequest) -> OutputShape:
        if not self._for_output:
            raise CannotProvide

        return mediator.delegating_provide(
            replace(
                request,
                loc_stack=request.loc_stack.replace_last_type(self._prototype),
            ),
        )
