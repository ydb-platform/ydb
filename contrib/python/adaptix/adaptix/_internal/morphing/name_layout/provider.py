from typing import TypeVar

from ...model_tools.definitions import InputShape, OutputShape
from ...provider.essential import Mediator
from ...provider.methods_provider import MethodsProvider, method_handler
from ..model.crown_definitions import (
    BranchInpCrown,
    BranchOutCrown,
    DictExtraPolicy,
    InputNameLayout,
    InputNameLayoutRequest,
    LeafInpCrown,
    LeafOutCrown,
    OutputNameLayout,
    OutputNameLayoutRequest,
    Sieve,
)
from .base import ExtraMoveMaker, ExtraPoliciesMaker, PathsTo, SievesMaker, StructureMaker
from .crown_builder import InpCrownBuilder, OutCrownBuilder

T = TypeVar("T")


class BuiltinNameLayoutProvider(MethodsProvider):
    def __init__(
        self,
        structure_maker: StructureMaker,
        sieves_maker: SievesMaker,
        extra_policies_maker: ExtraPoliciesMaker,
        extra_move_maker: ExtraMoveMaker,
    ):
        self._structure_maker = structure_maker
        self._sieves_maker = sieves_maker
        self._extra_policies_maker = extra_policies_maker
        self._extra_move_maker = extra_move_maker

    @method_handler
    def _provide_input_name_layout(self, mediator: Mediator, request: InputNameLayoutRequest) -> InputNameLayout:
        extra_move = self._extra_move_maker.make_inp_extra_move(mediator, request)
        paths_to_leaves = self._structure_maker.make_inp_structure(mediator, request, extra_move)
        extra_policies = self._extra_policies_maker.make_extra_policies(mediator, request, paths_to_leaves)
        if paths_to_leaves:
            crown = self._create_input_crown(
                mediator,
                request.shape,
                paths_to_leaves,
                extra_policies,
            )
        else:
            crown = self._create_empty_input_crown(
                mediator=mediator,
                shape=request.shape,
                extra_policies=extra_policies,
                as_list=self._structure_maker.empty_as_list_inp(mediator, request),
            )
        return InputNameLayout(crown=crown, extra_move=extra_move)

    def _create_input_crown(
        self,
        mediator: Mediator,
        shape: InputShape,
        paths_to_leaves: PathsTo[LeafInpCrown],
        extra_policies: PathsTo[DictExtraPolicy],
    ) -> BranchInpCrown:
        return InpCrownBuilder(extra_policies, paths_to_leaves).build_crown()

    def _create_empty_input_crown(
        self,
        mediator: Mediator,
        shape: InputShape,
        extra_policies: PathsTo[DictExtraPolicy],
        *,
        as_list: bool,
    ) -> BranchInpCrown:
        return InpCrownBuilder(extra_policies, {}).build_empty_crown(as_list=as_list)

    @method_handler
    def _provide_output_name_layout(self, mediator: Mediator, request: OutputNameLayoutRequest) -> OutputNameLayout:
        extra_move = self._extra_move_maker.make_out_extra_move(mediator, request)
        paths_to_leaves = self._structure_maker.make_out_structure(mediator, request, extra_move)
        path_to_sieve = self._sieves_maker.make_sieves(mediator, request, paths_to_leaves)
        if paths_to_leaves:
            crown = self._create_output_crown(
                mediator,
                request.shape,
                paths_to_leaves,
                path_to_sieve,
            )
        else:
            crown = self._create_empty_output_crown(
                mediator=mediator,
                shape=request.shape,
                path_to_sieve=path_to_sieve,
                as_list=self._structure_maker.empty_as_list_out(mediator, request),
            )
        return OutputNameLayout(crown=crown, extra_move=extra_move)

    def _create_output_crown(
        self,
        mediator: Mediator,
        shape: OutputShape,
        paths_to_leaves: PathsTo[LeafOutCrown],
        path_to_sieve: PathsTo[Sieve],
    ) -> BranchOutCrown:
        return OutCrownBuilder(path_to_sieve, paths_to_leaves).build_crown()

    def _create_empty_output_crown(
        self,
        mediator: Mediator,
        shape: OutputShape,
        path_to_sieve: PathsTo[Sieve],
        *,
        as_list: bool,
    ):
        return OutCrownBuilder(path_to_sieve, {}).build_empty_crown(as_list=as_list)
