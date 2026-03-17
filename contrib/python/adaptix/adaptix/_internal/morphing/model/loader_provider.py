from collections.abc import Mapping, Set
from functools import partial

from ...code_tools.compiler import BasicClosureCompiler, ClosureCompiler
from ...code_tools.name_sanitizer import BuiltinNameSanitizer, NameSanitizer
from ...common import Loader
from ...definitions import DebugTrail, Direction
from ...model_tools.definitions import DefaultFactory, DefaultValue, InputField, InputShape
from ...provider.essential import CannotProvide, Mediator
from ...provider.fields import input_field_to_loc
from ...provider.located_request import LocatedRequest
from ...provider.shape_provider import InputShapeRequest, provide_generic_resolved_shape
from ...utils import AlwaysEqualHashWrapper, Omittable, Omitted, OrderedMappingHashWrapper
from ..json_schema.definitions import JSONSchema
from ..json_schema.request_cls import JSONSchemaRequest
from ..json_schema.schema_model import JSONValue
from ..model.loader_gen import BuiltinModelLoaderGen, ModelInputJSONSchemaGen, ModelLoaderProps
from ..provider_template import JSONSchemaProvider, LoaderProvider
from ..request_cls import DebugTrailRequest, DumperRequest, LoaderRequest, StrictCoercionRequest
from .basic_gen import (
    CodeGenHook,
    ModelLoaderGen,
    compile_closure_with_globals_capturing,
    fetch_code_gen_hook,
    get_extra_targets_at_crown,
    get_optional_fields_at_list_crown,
    get_skipped_fields,
    get_wild_extra_targets,
    has_collect_policy,
)
from .crown_definitions import InputNameLayout, InputNameLayoutRequest


class ModelLoaderProvider(LoaderProvider, JSONSchemaProvider):
    def __init__(
        self,
        *,
        name_sanitizer: NameSanitizer = BuiltinNameSanitizer(),
        props: ModelLoaderProps = ModelLoaderProps(),
    ):
        self._name_sanitizer = name_sanitizer
        self._props = props

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        shape = self._fetch_shape(mediator, request)
        name_layout = self._fetch_name_layout(mediator, request, shape)
        field_loaders = self._fetch_field_loaders(mediator, request, shape)
        return mediator.cached_call(
            self._make_loader,
            shape=shape,
            name_layout=name_layout,
            field_loaders=OrderedMappingHashWrapper(field_loaders),
            strict_coercion=mediator.mandatory_provide(StrictCoercionRequest(loc_stack=request.loc_stack)),
            debug_trail=mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack)),
            code_gen_hook=AlwaysEqualHashWrapper(fetch_code_gen_hook(mediator, request.loc_stack)),
            model_identity=self._fetch_model_identity(mediator, request, shape, name_layout),
            closure_name=self._get_closure_name(request),
            file_name=self._get_file_name(request),
        )

    def _make_loader(
        self,
        *,
        shape: InputShape,
        name_layout: InputNameLayout,
        field_loaders: OrderedMappingHashWrapper[Mapping[str, Loader]],
        strict_coercion: bool,
        debug_trail: DebugTrail,
        code_gen_hook: AlwaysEqualHashWrapper[CodeGenHook],
        model_identity: str,
        closure_name: str,
        file_name: str,
    ) -> Loader:
        skipped_fields = get_skipped_fields(shape, name_layout)
        self._validate_params(shape, name_layout, skipped_fields)
        loader_gen = self._create_model_loader_gen(
            debug_trail=debug_trail,
            strict_coercion=strict_coercion,
            shape=shape,
            name_layout=name_layout,
            field_loaders=field_loaders.mapping,
            skipped_fields=skipped_fields,
            model_identity=model_identity,
        )
        loader_code, loader_namespace = loader_gen.produce_code(closure_name=closure_name)
        return compile_closure_with_globals_capturing(
            compiler=self._get_compiler(),
            code_gen_hook=code_gen_hook.value,
            namespace=loader_namespace,
            closure_code=loader_code,
            closure_name=closure_name,
            file_name=file_name,
        )

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        if request.ctx.direction != Direction.INPUT:
            raise CannotProvide

        shape = self._fetch_shape(mediator, request)
        name_layout = self._fetch_name_layout(mediator, request, shape)
        skipped_fields = get_skipped_fields(shape, name_layout)
        self._validate_params(shape, name_layout, skipped_fields)
        schema_gen = self._get_schema_gen(mediator, request, shape)
        return schema_gen.convert_crown(name_layout.crown)

    def _get_schema_gen(
        self,
        mediator: Mediator,
        request: JSONSchemaRequest,
        shape: InputShape,
    ) -> ModelInputJSONSchemaGen:
        return ModelInputJSONSchemaGen(
            shape=shape,
            field_default_dumper=partial(self._dump_field_default, mediator, request),
            field_json_schema_getter=partial(self._get_field_json_schema, mediator, request),
        )

    def _dump_field_default(
        self,
        mediator: Mediator,
        request: JSONSchemaRequest,
        field: InputField,
    ) -> Omittable[JSONValue]:
        if isinstance(field.default, DefaultValue):
            default_value = field.default.value
        elif isinstance(field.default, DefaultFactory):
            default_value = field.default.factory()
        else:
            return Omitted()

        dumper = mediator.mandatory_provide(
            DumperRequest(loc_stack=request.loc_stack.append_with(input_field_to_loc(field))),
        )
        return dumper(default_value)

    def _get_field_json_schema(
        self,
        mediator: Mediator,
        request: JSONSchemaRequest,
        field: InputField,
    ) -> JSONSchema:
        return mediator.mandatory_provide(request.append_loc(input_field_to_loc(field)))

    def _fetch_model_identity(
        self,
        mediator: Mediator,
        request: LoaderRequest,
        shape: InputShape,
        name_layout: InputNameLayout,
    ) -> str:
        return repr(request.last_loc.type)

    def _create_model_loader_gen(
        self,
        *,
        debug_trail: DebugTrail,
        strict_coercion: bool,
        shape: InputShape,
        name_layout: InputNameLayout,
        field_loaders: Mapping[str, Loader],
        skipped_fields: Set[str],
        model_identity: str,
    ) -> ModelLoaderGen:
        return BuiltinModelLoaderGen(
            shape=shape,
            name_layout=name_layout,
            debug_trail=debug_trail,
            strict_coercion=strict_coercion,
            field_loaders=field_loaders,
            skipped_fields=skipped_fields,
            model_identity=model_identity,
            props=self._props,
        )

    def _request_to_view_string(self, request: LoaderRequest) -> str:
        tp = request.last_loc.type
        if isinstance(tp, type):
            return tp.__name__
        return str(tp)

    def _merge_view_string(self, *fragments: str) -> str:
        return "_".join(filter(None, fragments))

    def _get_file_name(self, request: LoaderRequest) -> str:
        return self._merge_view_string(
            "model_loader", self._request_to_view_string(request),
        )

    def _get_closure_name(self, request: LoaderRequest) -> str:
        return self._merge_view_string(
            "model_loader", self._name_sanitizer.sanitize(self._request_to_view_string(request)),
        )

    def _get_compiler(self) -> ClosureCompiler:
        return BasicClosureCompiler()

    def _fetch_shape(self, mediator: Mediator, request: LocatedRequest) -> InputShape:
        return provide_generic_resolved_shape(mediator, InputShapeRequest(loc_stack=request.loc_stack))

    def _fetch_name_layout(self, mediator: Mediator, request: LocatedRequest, shape: InputShape) -> InputNameLayout:
        return mediator.mandatory_provide(
            InputNameLayoutRequest(
                loc_stack=request.loc_stack,
                shape=shape,
            ),
            lambda x: "Cannot create loader for model. Cannot fetch `InputNameLayout`",
        )

    def _fetch_field_loaders(
        self,
        mediator: Mediator,
        request: LoaderRequest,
        shape: InputShape,
    ) -> Mapping[str, Loader]:
        loaders = mediator.mandatory_provide_by_iterable(
            [
                request.append_loc(input_field_to_loc(field))
                for field in shape.fields
            ],
            lambda: "Cannot create loader for model. Loaders for some fields cannot be created",
        )
        return {field.id: loader for field, loader in zip(shape.fields, loaders)}

    def _validate_params(
        self,
        shape: InputShape,
        name_layout: InputNameLayout,
        skipped_fields: Set[str],
    ) -> None:
        skipped_required_fields = [
            field.id
            for field in shape.fields
            if field.is_required and field.id in skipped_fields
        ]
        if skipped_required_fields:
            raise ValueError(
                f"Required fields {skipped_required_fields} are skipped",
            )

        if name_layout.extra_move is None and has_collect_policy(name_layout.crown):
            raise ValueError(
                "Cannot create loader that collect extra data"
                " if InputShape does not take extra data",
            )

        extra_targets_at_crown = get_extra_targets_at_crown(name_layout)
        if extra_targets_at_crown:
            raise ValueError(
                f"Extra targets {extra_targets_at_crown} are found at crown",
            )

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


class InlinedShapeModelLoaderProvider(ModelLoaderProvider):
    def __init__(
        self,
        *,
        name_sanitizer: NameSanitizer = BuiltinNameSanitizer(),
        props: ModelLoaderProps = ModelLoaderProps(),
        shape: InputShape,
    ):
        super().__init__(name_sanitizer=name_sanitizer, props=props)
        self._shape = shape

    def _fetch_shape(self, mediator: Mediator, request: LocatedRequest) -> InputShape:
        return self._shape
