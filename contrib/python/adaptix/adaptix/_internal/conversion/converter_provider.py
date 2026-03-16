import itertools
from collections.abc import Mapping, Sequence
from functools import update_wrapper
from inspect import Parameter, Signature
from typing import Any, Callable, Optional

from ..code_tools.cascade_namespace import BuiltinCascadeNamespace, CascadeNamespace
from ..code_tools.code_builder import CodeBuilder
from ..code_tools.compiler import BasicClosureCompiler, ClosureCompiler
from ..code_tools.name_sanitizer import BuiltinNameSanitizer, NameSanitizer
from ..common import Coercer, Converter, TypeHint
from ..conversion.request_cls import CoercerRequest, ConversionContext, ConverterRequest
from ..model_tools.definitions import DefaultValue, NoDefault
from ..morphing.model.basic_gen import compile_closure_with_globals_capturing, fetch_code_gen_hook
from ..provider.essential import CannotProvide, Mediator
from ..provider.loc_stack_filtering import LocStack
from ..provider.location import FieldLoc, TypeHintLoc
from .provider_template import ConverterProvider


class BuiltinConverterProvider(ConverterProvider):
    def __init__(self, *, name_sanitizer: NameSanitizer = BuiltinNameSanitizer()):
        self._name_sanitizer = name_sanitizer

    def _provide_converter(self, mediator: Mediator, request: ConverterRequest) -> Converter:
        signature = request.signature
        if len(signature.parameters.values()) == 0:
            raise CannotProvide(
                message="At least one parameter is required",
                is_demonstrative=True,
            )
        if any(
            param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD)
            for param in signature.parameters.values()
        ):
            raise CannotProvide(
                message="Parameters specified by *args and **kwargs are not supported",
                is_demonstrative=True,
            )

        return self._make_converter(mediator, request)

    def _make_converter(self, mediator: Mediator, request: ConverterRequest):
        src_loc, *extra_params_locs = map(self._param_to_loc, request.signature.parameters.values())
        dst_loc = self._get_dst_field(request.signature.return_annotation)

        coercer = mediator.mandatory_provide(
            CoercerRequest(
                src=LocStack(src_loc),
                ctx=ConversionContext(tuple(extra_params_locs)),
                dst=LocStack(dst_loc),
            ),
            lambda x: "Cannot create top-level coercer",
        )
        closure_name = self._get_closure_name(request)
        dumper_code, dumper_namespace = self._produce_code(
            signature=request.signature,
            closure_name=closure_name,
            stub_function=request.stub_function,
            coercer=coercer,
        )
        return compile_closure_with_globals_capturing(
            compiler=self._get_compiler(),
            code_gen_hook=fetch_code_gen_hook(mediator, LocStack(dst_loc)),
            namespace=dumper_namespace,
            closure_code=dumper_code,
            closure_name=closure_name,
            file_name=self._get_file_name(request),
        )

    def _register_mangled(self, namespace: CascadeNamespace, base: str, obj: object) -> str:
        base = self._name_sanitizer.sanitize(base)
        if namespace.try_add_constant(base, obj):
            return base

        for i in itertools.count(1):
            name = f"{base}_{i}"
            if namespace.try_add_constant(name, obj):
                return name
        raise RuntimeError

    def _produce_code(
        self,
        signature: Signature,
        stub_function: Optional[Callable],
        closure_name: str,
        coercer: Coercer,
    ) -> tuple[str, Mapping[str, object]]:
        builder = CodeBuilder()
        namespace = BuiltinCascadeNamespace(occupied=signature.parameters.keys())
        namespace.add_outer_constant("_closure_signature", signature)
        namespace.add_outer_constant("_stub_function", stub_function)
        namespace.add_outer_constant("_update_wrapper", update_wrapper)
        coercer_var = self._register_mangled(namespace, "coercer", coercer)

        no_types_signature = signature.replace(
            parameters=[param.replace(annotation=Signature.empty) for param in signature.parameters.values()],
            return_annotation=Signature.empty,
        )
        parameters = tuple(signature.parameters.values())
        ctx_passing = self._get_ctx_passing(parameters[1:])
        builder(
            f"""
            def {closure_name}{no_types_signature}:
                return {coercer_var}({parameters[0].name}, {ctx_passing})
            """,
        )
        if stub_function is not None:
            builder += f"_update_wrapper({closure_name}, _stub_function)"
        builder += f"{closure_name}.__signature__ = _closure_signature"
        builder += f"{closure_name}.__name__ = {closure_name!r}"
        return builder.string(), namespace.all_constants

    def _get_ctx_passing(self, ctx_parameters: Sequence[Parameter]) -> str:
        if len(ctx_parameters) == 0:
            return "None"
        if len(ctx_parameters) == 1:
            return ctx_parameters[0].name
        return "(" + ", ".join(param.name for param in ctx_parameters) + ")"

    def _get_compiler(self) -> ClosureCompiler:
        return BasicClosureCompiler()

    def _get_closure_name(self, request: ConverterRequest) -> str:
        if request.function_name is not None:
            return request.function_name
        stub_function_name = getattr(request.stub_function, "__name__", None)
        if stub_function_name is not None:
            return stub_function_name
        src = next(iter(request.signature.parameters.values()))
        dst = self._get_type_from_annotation(request.signature.return_annotation)
        return self._name_sanitizer.sanitize(f"convert_{src}_to_{dst}")

    def _get_file_name(self, request: ConverterRequest) -> str:
        if request.function_name is not None:
            return request.function_name
        stub_function_name = getattr(request.stub_function, "__name__", None)
        if stub_function_name is not None:
            return stub_function_name
        src = next(iter(request.signature.parameters.values()))
        dst = self._get_type_from_annotation(request.signature.return_annotation)
        return self._name_sanitizer.sanitize(f"convert_{src}_to_{dst}")

    def _get_type_from_annotation(self, annotation: Any) -> TypeHint:
        return Any if annotation == Signature.empty else annotation

    def _param_to_loc(self, parameter: Parameter) -> FieldLoc:
        return FieldLoc(
            field_id=parameter.name,
            type=self._get_type_from_annotation(parameter.annotation),
            default=NoDefault() if parameter.default == Signature.empty else DefaultValue(parameter.default),
            metadata={},
        )

    def _get_dst_field(self, return_annotation: Any) -> TypeHintLoc:
        return TypeHintLoc(
            self._get_type_from_annotation(return_annotation),
        )
