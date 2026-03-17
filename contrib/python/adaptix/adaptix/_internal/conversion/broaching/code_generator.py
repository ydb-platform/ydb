import ast
import itertools
from abc import ABC, abstractmethod
from ast import AST
from collections import defaultdict
from collections.abc import Mapping
from inspect import Signature
from typing import Union

from ...code_tools.ast_templater import ast_substitute
from ...code_tools.cascade_namespace import BuiltinCascadeNamespace, CascadeNamespace
from ...code_tools.code_builder import CodeBuilder
from ...code_tools.name_sanitizer import NameSanitizer
from ...code_tools.utils import get_literal_expr, get_literal_from_factory
from ...model_tools.definitions import DescriptorAccessor, ItemAccessor
from ...special_cases_optimization import as_is_stub, as_is_stub_with_ctx
from .definitions import (
    AccessorElement,
    ConstantElement,
    FunctionElement,
    KeywordArg,
    ParameterElement,
    PositionalArg,
    UnpackIterable,
    UnpackMapping,
)

BroachingPlan = Union[
    ParameterElement,
    ConstantElement,
    FunctionElement["BroachingPlan"],
    AccessorElement["BroachingPlan"],
]


class GenState:
    def __init__(self, namespace: CascadeNamespace, name_sanitizer: NameSanitizer):
        self._namespace = namespace
        self._name_sanitizer = name_sanitizer
        self._prefix_counter: defaultdict[str, int] = defaultdict(lambda: 0)

    def register_next_id(self, prefix: str, obj: object) -> str:
        number = self._prefix_counter[prefix]
        self._prefix_counter[prefix] += 1
        name = f"{prefix}_{number}"
        return self.register_mangled(name, obj)

    def register_mangled(self, base: str, obj: object) -> str:
        base = self._name_sanitizer.sanitize(base)
        if self._namespace.try_add_constant(base, obj):
            return base

        for i in itertools.count(1):
            name = f"{base}_{i}"
            if self._namespace.try_add_constant(name, obj):
                return name
        raise RuntimeError


class BroachingCodeGenerator(ABC):
    @abstractmethod
    def produce_code(self, signature: Signature, closure_name: str) -> tuple[str, Mapping[str, object]]:
        ...


class BuiltinBroachingCodeGenerator(BroachingCodeGenerator):
    def __init__(self, plan: BroachingPlan, name_sanitizer: NameSanitizer):
        self._plan = plan
        self._name_sanitizer = name_sanitizer

    def _create_state(self, namespace: CascadeNamespace) -> GenState:
        return GenState(
            namespace=namespace,
            name_sanitizer=self._name_sanitizer,
        )

    def produce_code(self, signature: Signature, closure_name: str) -> tuple[str, Mapping[str, object]]:
        builder = CodeBuilder()
        namespace = BuiltinCascadeNamespace(occupied=signature.parameters.keys())
        state = self._create_state(namespace=namespace)

        namespace.add_outer_constant("_closure_signature", signature)
        no_types_signature = signature.replace(
            parameters=[param.replace(annotation=Signature.empty) for param in signature.parameters.values()],
            return_annotation=Signature.empty,
        )
        with builder(f"def {closure_name}{no_types_signature}:"):
            body = self._gen_plan_element_dispatch(state, self._plan)
            builder += "return " + ast.unparse(body)

        builder += f"{closure_name}.__signature__ = _closure_signature"
        builder += f"{closure_name}.__name__ = {closure_name!r}"
        return builder.string(), namespace.all_constants

    def _gen_plan_element_dispatch(self, state: GenState, element: BroachingPlan) -> AST:
        if isinstance(element, ParameterElement):
            return self._gen_parameter_element(state, element)
        if isinstance(element, ConstantElement):
            return self._gen_constant_element(state, element)
        if isinstance(element, FunctionElement):
            return self._gen_function_element(state, element)
        if isinstance(element, AccessorElement):
            return self._gen_accessor_element(state, element)
        raise TypeError

    def _gen_parameter_element(self, state: GenState, element: ParameterElement) -> AST:
        return ast.Name(id=element.name, ctx=ast.Load())

    def _gen_constant_element(self, state: GenState, element: ConstantElement) -> AST:
        expr = get_literal_expr(element.value)
        if expr is not None:
            return ast.parse(expr)

        name = state.register_next_id("constant", element.value)
        return ast.Name(id=name, ctx=ast.Load())

    def _gen_function_element(self, state: GenState, element: FunctionElement[BroachingPlan]) -> AST:
        if (
            element.func == as_is_stub
            and len(element.args) == 1
            and isinstance(element.args[0], PositionalArg)
        ):
            return self._gen_plan_element_dispatch(state, element.args[0].element)

        if (
            element.func == as_is_stub_with_ctx
            and len(element.args) == 2  # noqa: PLR2004
            and isinstance(element.args[0], PositionalArg)
            and isinstance(element.args[1], PositionalArg)
        ):
            return self._gen_plan_element_dispatch(state, element.args[0].element)

        if not element.args:
            literal = get_literal_from_factory(element.func)
            if literal is not None:
                return ast.parse(literal)

        if getattr(element.func, "__name__", None) is not None:
            name = state.register_mangled(element.func.__name__, element.func)
        else:
            name = state.register_next_id("func", element.func)

        return self._gen_function_call(state, element, name)

    def _gen_function_call(self, state: GenState, element: FunctionElement[BroachingPlan], name: str) -> AST:
        args = []
        keywords = []
        for arg in element.args:
            if isinstance(arg, PositionalArg):
                sub_ast = self._gen_plan_element_dispatch(state, arg.element)
                args.append(sub_ast)
            elif isinstance(arg, KeywordArg):
                sub_ast = self._gen_plan_element_dispatch(state, arg.element)
                keywords.append(ast.keyword(arg=arg.key, value=sub_ast))  # type: ignore[call-overload]
            elif isinstance(arg, UnpackMapping):
                sub_ast = self._gen_plan_element_dispatch(state, arg.element)
                keywords.append(ast.keyword(value=sub_ast))  # type: ignore[call-overload]
            elif isinstance(arg, UnpackIterable):
                sub_ast = self._gen_plan_element_dispatch(state, arg.element)
                args.append(ast.Starred(value=sub_ast, ctx=ast.Load()))  # type: ignore[arg-type]
            else:
                raise TypeError

        return ast.Call(
            func=ast.Name(name, ast.Load()),
            args=args,  # type: ignore[arg-type]
            keywords=keywords,
        )

    def _gen_accessor_element(self, state: GenState, element: AccessorElement[BroachingPlan]) -> AST:
        target_expr = self._gen_plan_element_dispatch(state, element.target)
        if isinstance(element.accessor, DescriptorAccessor):
            if element.accessor.attr_name.isidentifier():
                return ast_substitute(
                    f"__target_expr__.{element.accessor.attr_name}",
                    target_expr=target_expr,
                )
            return ast_substitute(
                f"getattr(__target_expr__, {element.accessor.attr_name!r})",
                target_expr=target_expr,
            )

        if isinstance(element.accessor, ItemAccessor):
            return ast_substitute(
                f"__target_expr__[{element.accessor.key!r}]",
                target_expr=target_expr,
            )

        name = state.register_next_id("accessor", element.accessor.getter)
        return ast_substitute(
            f"{name}(__target_expr__)",
            target_expr=target_expr,
        )
