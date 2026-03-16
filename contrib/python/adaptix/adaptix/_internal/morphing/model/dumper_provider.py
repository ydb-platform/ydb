from collections.abc import Mapping, Sequence
from functools import partial
from typing import Any

from ...code_tools.compiler import BasicClosureCompiler, ClosureCompiler
from ...code_tools.name_sanitizer import BuiltinNameSanitizer, NameSanitizer
from ...common import Dumper
from ...definitions import DebugTrail, Direction
from ...model_tools.definitions import DefaultFactory, DefaultValue, OutputField, OutputShape
from ...provider.essential import CannotProvide, Mediator
from ...provider.fields import output_field_to_loc
from ...provider.located_request import LocatedRequest
from ...provider.shape_provider import OutputShapeRequest, provide_generic_resolved_shape
from ...utils import AlwaysEqualHashWrapper, Omittable, Omitted, OrderedMappingHashWrapper
from ..json_schema.definitions import JSONSchema
from ..json_schema.request_cls import JSONSchemaRequest
from ..json_schema.schema_model import JSONValue
from ..provider_template import DumperProvider, JSONSchemaProvider
from ..request_cls import DebugTrailRequest, DumperRequest
from .basic_gen import (
    CodeGenHook,
    ModelDumperGen,
    compile_closure_with_globals_capturing,
    fetch_code_gen_hook,
    get_extra_targets_at_crown,
    get_optional_fields_at_list_crown,
    get_wild_extra_targets,
)
from .crown_definitions import OutExtraMove, OutputNameLayout, OutputNameLayoutRequest
from .dumper_gen import BuiltinModelDumperGen, ModelOutputJSONSchemaGen


class ModelDumperProvider(DumperProvider, JSONSchemaProvider):
    def __init__(self, *, name_sanitizer: NameSanitizer = BuiltinNameSanitizer()):
        self._name_sanitizer = name_sanitizer

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        shape = self._fetch_shape(mediator, request)
        name_layout = self._fetch_name_layout(mediator, request, shape)
        fields_dumpers = self._fetch_field_dumpers(mediator, request, shape)
        return mediator.cached_call(
            self._make_dumper,
            shape=shape,
            name_layout=name_layout,
            fields_dumpers=OrderedMappingHashWrapper(fields_dumpers),
            debug_trail=mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack)),
            code_gen_hook=AlwaysEqualHashWrapper(fetch_code_gen_hook(mediator, request.loc_stack)),
            model_identity=self._fetch_model_identity(mediator, request, shape, name_layout),
            closure_name=self._get_closure_name(request),
            file_name=self._get_file_name(request),
        )

    def _make_dumper(
        self,
        *,
        shape: OutputShape,
        name_layout: OutputNameLayout,
        fields_dumpers: OrderedMappingHashWrapper[Mapping[str, Dumper]],
        debug_trail: DebugTrail,
        code_gen_hook: AlwaysEqualHashWrapper[CodeGenHook],
        model_identity: str,
        closure_name: str,
        file_name: str,
    ) -> Dumper:
        self._validate_params(shape, name_layout)
        dumper_gen = self._create_model_dumper_gen(
            debug_trail=debug_trail,
            shape=shape,
            name_layout=name_layout,
            fields_dumpers=fields_dumpers.mapping,
            model_identity=model_identity,
        )
        dumper_code, dumper_namespace = dumper_gen.produce_code(closure_name=closure_name)
        return compile_closure_with_globals_capturing(
            compiler=self._get_compiler(),
            code_gen_hook=code_gen_hook.value,
            namespace=dumper_namespace,
            closure_code=dumper_code,
            closure_name=closure_name,
            file_name=file_name,
        )

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        if request.ctx.direction != Direction.OUTPUT:
            raise CannotProvide

        shape = self._fetch_shape(mediator, request)
        name_layout = self._fetch_name_layout(mediator, request, shape)
        self._validate_params(shape, name_layout)

        schema_gen = self._get_schema_gen(mediator, request, shape, name_layout.extra_move)
        return schema_gen.convert_crown(name_layout.crown)

    def _get_schema_gen(
        self,
        mediator: Mediator,
        request: JSONSchemaRequest,
        shape: OutputShape,
        extra_move: OutExtraMove,
    ) -> ModelOutputJSONSchemaGen:
        return ModelOutputJSONSchemaGen(
            shape=shape,
            field_default_dumper=partial(self._dump_field_default, mediator, request),
            field_json_schema_getter=partial(self._get_field_json_schema, mediator, request),
            extra_move=extra_move,
            placeholder_dumper=self._dump_placeholder,
        )

    def _dump_field_default(
        self,
        mediator: Mediator,
        request: JSONSchemaRequest,
        field: OutputField,
    ) -> Omittable[JSONValue]:
        if isinstance(field.default, DefaultValue):
            default_value = field.default.value
        elif isinstance(field.default, DefaultFactory):
            default_value = field.default.factory()
        else:
            return Omitted()

        dumper = mediator.mandatory_provide(
            DumperRequest(loc_stack=request.loc_stack.append_with(output_field_to_loc(field))),
        )
        return dumper(default_value)

    def _dump_placeholder(self, data: Any) -> JSONValue:
        if isinstance(data, Mapping):
            return {str(self._dump_placeholder(key)): self._dump_placeholder(value) for key, value in data.items()}
        if isinstance(data, Sequence):
            return [self._dump_placeholder(element) for element in data]
        if isinstance(data, (str, int, float, bool)) or data is None:
            return data
        raise TypeError(f"Cannot dump placeholder {data}")

    def _get_field_json_schema(
        self,
        mediator: Mediator,
        request: JSONSchemaRequest,
        field: OutputField,
    ) -> JSONSchema:
        return mediator.mandatory_provide(request.append_loc(output_field_to_loc(field)))

    def _fetch_model_dumper_gen(self, mediator: Mediator, request: LocatedRequest) -> ModelDumperGen:
        shape = self._fetch_shape(mediator, request)
        name_layout = self._fetch_name_layout(mediator, request, shape)
        self._validate_params(shape, name_layout)

        fields_dumpers = self._fetch_field_dumpers(mediator, request, shape)
        debug_trail = mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack))
        return self._create_model_dumper_gen(
            debug_trail=debug_trail,
            shape=shape,
            name_layout=name_layout,
            fields_dumpers=fields_dumpers,
            model_identity=self._fetch_model_identity(mediator, request, shape, name_layout),
        )

    def _fetch_model_identity(
        self,
        mediator: Mediator,
        request: LocatedRequest,
        shape: OutputShape,
        name_layout: OutputNameLayout,
    ) -> str:
        return repr(request.last_loc.type)

    def _create_model_dumper_gen(
        self,
        debug_trail: DebugTrail,
        shape: OutputShape,
        name_layout: OutputNameLayout,
        fields_dumpers: Mapping[str, Dumper],
        model_identity: str,
    ) -> ModelDumperGen:
        return BuiltinModelDumperGen(
            shape=shape,
            name_layout=name_layout,
            debug_trail=debug_trail,
            fields_dumpers=fields_dumpers,
            model_identity=model_identity,
        )

    def _request_to_view_string(self, request: DumperRequest) -> str:
        tp = request.last_loc.type
        if isinstance(tp, type):
            return tp.__name__
        return str(tp)

    def _merge_view_string(self, *fragments: str) -> str:
        return "_".join(filter(None, fragments))

    def _get_file_name(self, request: DumperRequest) -> str:
        return self._merge_view_string(
            "model_dumper", self._request_to_view_string(request),
        )

    def _get_closure_name(self, request: DumperRequest) -> str:
        return self._merge_view_string(
            "model_dumper", self._name_sanitizer.sanitize(self._request_to_view_string(request)),
        )

    def _get_compiler(self) -> ClosureCompiler:
        return BasicClosureCompiler()

    def _fetch_shape(self, mediator: Mediator, request: LocatedRequest) -> OutputShape:
        return provide_generic_resolved_shape(mediator, OutputShapeRequest(loc_stack=request.loc_stack))

    def _fetch_name_layout(self, mediator: Mediator, request: LocatedRequest, shape: OutputShape) -> OutputNameLayout:
        return mediator.mandatory_provide(
            OutputNameLayoutRequest(
                loc_stack=request.loc_stack,
                shape=shape,
            ),
        )

    def _fetch_field_dumpers(
        self,
        mediator: Mediator,
        request: LocatedRequest,
        shape: OutputShape,
    ) -> Mapping[str, Dumper]:
        dumpers = mediator.mandatory_provide_by_iterable(
            [
                request.append_loc(output_field_to_loc(field))
                for field in shape.fields
            ],
            lambda: "Cannot create dumper for model. Dumpers for some fields cannot be created",
        )
        return {field.id: dumper for field, dumper in zip(shape.fields, dumpers)}

    def _validate_params(self, shape: OutputShape, name_layout: OutputNameLayout) -> None:
        optional_fields_at_list_crown = get_optional_fields_at_list_crown(
            {field.id: field for field in shape.fields},
            name_layout.crown,
        )
        if optional_fields_at_list_crown:
            raise ValueError(
                f"Optional fields {optional_fields_at_list_crown} are found at list crown",
            )

        wild_extra_targets = get_wild_extra_targets(shape, name_layout.extra_move)
        if wild_extra_targets:
            raise ValueError(
                f"ExtraTargets {wild_extra_targets} are attached to non-existing fields",
            )

        extra_targets_at_crown = get_extra_targets_at_crown(name_layout)
        if extra_targets_at_crown:
            raise ValueError(
                f"Extra targets {extra_targets_at_crown} are found at crown",
            )
