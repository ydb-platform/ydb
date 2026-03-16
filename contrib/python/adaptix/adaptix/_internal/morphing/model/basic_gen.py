import itertools
from abc import ABC, abstractmethod
from collections.abc import Collection, Container, Iterable, Mapping, Set
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, Union

from ...code_tools.code_builder import CodeBuilder
from ...code_tools.compiler import ClosureCompiler
from ...code_tools.utils import get_literal_expr
from ...model_tools.definitions import InputField, OutputField
from ...provider.essential import CannotProvide, Mediator
from ...provider.loc_stack_filtering import LocStack
from ...provider.located_request import LocatedRequest
from ...provider.methods_provider import MethodsProvider, method_handler
from .crown_definitions import (
    BaseCrown,
    BaseDictCrown,
    BaseFieldCrown,
    BaseListCrown,
    BaseNameLayout,
    BaseNoneCrown,
    BaseShape,
    ExtraCollect,
    ExtraTargets,
    InpCrown,
    InpDictCrown,
    InpExtraMove,
    InpFieldCrown,
    InpListCrown,
    InpNoneCrown,
    OutExtraMove,
)


@dataclass
class CodeGenHookData:
    namespace: dict[str, Any]
    source: str


CodeGenHook = Callable[[CodeGenHookData], None]


def stub_code_gen_hook(data: CodeGenHookData):
    pass


@dataclass(frozen=True)
class CodeGenHookRequest(LocatedRequest[CodeGenHook]):
    pass


def fetch_code_gen_hook(mediator: Mediator, loc_stack: LocStack) -> CodeGenHook:
    try:
        return mediator.delegating_provide(CodeGenHookRequest(loc_stack=loc_stack))
    except CannotProvide:
        return stub_code_gen_hook


class CodeGenAccumulator(MethodsProvider):
    """Accumulates all generated code. It may be useful for debugging"""

    def __init__(self) -> None:
        self.list: list[tuple[CodeGenHookRequest, CodeGenHookData]] = []

    @method_handler
    def _provide_code_gen_hook(self, mediator: Mediator, request: CodeGenHookRequest) -> CodeGenHook:
        def hook(data: CodeGenHookData):
            self.list.append((request, data))

        return hook

    @property
    def code_pairs(self):
        return [
            (request.last_loc.type, hook_data.source)
            for request, hook_data in self.list
        ]

    @property
    def code_dict(self):
        return dict(self.code_pairs)


T = TypeVar("T")


def _concatenate_iters(args: Iterable[Iterable[T]]) -> Collection[T]:
    return list(itertools.chain.from_iterable(args))


def _inner_collect_used_direct_fields(crown: BaseCrown) -> Iterable[str]:
    if isinstance(crown, BaseDictCrown):
        return _concatenate_iters(
            _inner_collect_used_direct_fields(sub_crown)
            for sub_crown in crown.map.values()
        )
    if isinstance(crown, BaseListCrown):
        return _concatenate_iters(
            _inner_collect_used_direct_fields(sub_crown)
            for sub_crown in crown.map
        )
    if isinstance(crown, BaseFieldCrown):
        return [crown.id]
    if isinstance(crown, BaseNoneCrown):
        return []
    raise TypeError


def _collect_used_direct_fields(crown: BaseCrown) -> set[str]:
    lst = _inner_collect_used_direct_fields(crown)

    used_set = set()
    for f_name in lst:
        if f_name in used_set:
            raise ValueError(f"Field {f_name!r} is duplicated at crown")
        used_set.add(f_name)

    return used_set


def get_skipped_fields(shape: BaseShape, name_layout: BaseNameLayout) -> Set[str]:
    used_direct_fields = _collect_used_direct_fields(name_layout.crown)
    extra_targets = name_layout.extra_move.fields if isinstance(name_layout.extra_move, ExtraTargets) else ()
    return {
        field.id for field in shape.fields
        if field.id not in used_direct_fields and field.id not in extra_targets
    }


def _inner_get_extra_targets_at_crown(extra_targets: Container[str], crown: BaseCrown) -> Collection[str]:
    if isinstance(crown, BaseDictCrown):
        return _concatenate_iters(
            _inner_get_extra_targets_at_crown(extra_targets, sub_crown)
            for sub_crown in crown.map.values()
        )
    if isinstance(crown, BaseListCrown):
        return _concatenate_iters(
            _inner_get_extra_targets_at_crown(extra_targets, sub_crown)
            for sub_crown in crown.map
        )
    if isinstance(crown, BaseFieldCrown):
        return [crown.id] if crown.id in extra_targets else []
    if isinstance(crown, BaseNoneCrown):
        return []
    raise TypeError


def get_extra_targets_at_crown(name_layout: BaseNameLayout) -> Collection[str]:
    if not isinstance(name_layout.extra_move, ExtraTargets):
        return []

    return _inner_get_extra_targets_at_crown(name_layout.extra_move.fields, name_layout.crown)


def get_optional_fields_at_list_crown(
    fields_map: Mapping[str, Union[InputField, OutputField]],
    crown: BaseCrown,
) -> Collection[str]:
    if isinstance(crown, BaseDictCrown):
        return _concatenate_iters(
            get_optional_fields_at_list_crown(fields_map, sub_crown)
            for sub_crown in crown.map.values()
        )
    if isinstance(crown, BaseListCrown):
        return _concatenate_iters(
            (
                [sub_crown.id]
                if fields_map[sub_crown.id].is_optional else
                []
            )
            if isinstance(sub_crown, BaseFieldCrown) else
            get_optional_fields_at_list_crown(fields_map, sub_crown)
            for sub_crown in crown.map
        )
    if isinstance(crown, (BaseFieldCrown, BaseNoneCrown)):
        return []
    raise TypeError


def get_wild_extra_targets(shape: BaseShape, extra_move: Union[InpExtraMove, OutExtraMove]) -> Collection[str]:
    if not isinstance(extra_move, ExtraTargets):
        return []

    return [
        target for target in extra_move.fields
        if target not in shape.fields_dict
    ]


def compile_closure_with_globals_capturing(
    compiler: ClosureCompiler,
    code_gen_hook: CodeGenHook,
    namespace: Mapping[str, object],
    *,
    closure_name: str,
    closure_code: str,
    file_name: str,
):
    builder = CodeBuilder()

    global_namespace_dict = {}
    for name, value in namespace.items():
        value_literal = get_literal_expr(value)
        if value_literal is None:
            global_name = f"g_{name}"
            global_namespace_dict[global_name] = value
            builder += f"{name} = {global_name}"
        else:
            builder += f"{name} = {value_literal}"

    builder.empty_line()
    builder += closure_code
    builder += f"return {closure_name}"

    code_gen_hook(
        CodeGenHookData(
            namespace=global_namespace_dict,
            source=builder.string(),
        ),
    )

    return compiler.compile(
        file_name,
        lambda uid: f"<adaptix generated {uid}>",
        builder,
        global_namespace_dict,
    )


def has_collect_policy(crown: InpCrown) -> bool:
    if isinstance(crown, InpDictCrown):
        return crown.extra_policy == ExtraCollect() or any(
            has_collect_policy(sub_crown)
            for sub_crown in crown.map.values()
        )
    if isinstance(crown, InpListCrown):
        return any(
            has_collect_policy(sub_crown)
            for sub_crown in crown.map
        )
    if isinstance(crown, (InpFieldCrown, InpNoneCrown)):
        return False
    raise TypeError


class ModelLoaderGen(ABC):
    @abstractmethod
    def produce_code(self, closure_name: str) -> tuple[str, Mapping[str, object]]:
        ...


class ModelDumperGen(ABC):
    @abstractmethod
    def produce_code(self, closure_name: str) -> tuple[str, Mapping[str, object]]:
        ...
