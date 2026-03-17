from dataclasses import dataclass, field, replace
from inspect import Signature
from typing import Callable, Optional, TypeVar, Union

from ..common import Coercer, VarTuple
from ..model_tools.definitions import DefaultFactory, DefaultValue, InputField, ParamKind
from ..provider.essential import Request
from ..provider.loc_stack_filtering import LocStack
from ..provider.located_request import LocatedRequest
from ..provider.location import FieldLoc, GenericParamLoc, InputFieldLoc, InputFuncFieldLoc, OutputFieldLoc, TypeHintLoc


@dataclass(frozen=True)
class ConverterRequest(Request):
    signature: Signature
    function_name: Optional[str]
    stub_function: Optional[Callable]


ConversionSourceItem = Union[FieldLoc, OutputFieldLoc, GenericParamLoc]
ConversionDestItem = Union[TypeHintLoc, InputFieldLoc, InputFuncFieldLoc, GenericParamLoc]


@dataclass(frozen=True)
class ConversionContext:
    params: VarTuple[FieldLoc]
    loc_stacks: VarTuple[LocStack[FieldLoc]] = field(init=False, hash=False, repr=False, compare=False)

    def __post_init__(self):
        super().__setattr__("loc_stacks", tuple(LocStack(param) for param in self.params))


LinkingSource = LocStack[ConversionSourceItem]


@dataclass(frozen=True)
class FieldLinking:
    source: LinkingSource
    coercer: Optional[Coercer]


@dataclass(frozen=True)
class ConstantLinking:
    constant: Union[DefaultValue, DefaultFactory]


@dataclass(frozen=True)
class ModelLinking:
    pass


@dataclass(frozen=True)
class FunctionLinking:
    @dataclass(frozen=True)
    class ParamSpec:
        field: InputField
        param_kind: ParamKind
        linking: "LinkingResult"

    func: Callable
    param_specs: VarTuple[ParamSpec]


@dataclass(frozen=True)
class LinkingResult:
    linking: Union[FieldLinking, ConstantLinking, ModelLinking, FunctionLinking]


@dataclass(frozen=True)
class LinkingRequest(Request[LinkingResult]):
    sources: VarTuple[LinkingSource]
    context: ConversionContext
    destination: LocStack[ConversionDestItem]


CR = TypeVar("CR", bound="CoercerRequest")


@dataclass(frozen=True)
class CoercerRequest(Request[Coercer]):
    src: LocStack[ConversionSourceItem]
    ctx: ConversionContext
    dst: LocStack[ConversionDestItem]

    def append_loc(self: CR, *, src_loc: ConversionSourceItem, dst_loc: ConversionDestItem) -> CR:
        return replace(self, src=self.src.append_with(src_loc), dst=self.dst.append_with(dst_loc))

    def append_dst_loc(self: CR, dst_loc: ConversionDestItem) -> CR:
        return replace(self, dst=self.dst.append_with(dst_loc))


@dataclass(frozen=True)
class UnlinkedOptionalPolicy:
    is_allowed: bool


@dataclass(frozen=True)
class UnlinkedOptionalPolicyRequest(LocatedRequest[UnlinkedOptionalPolicy]):
    pass
