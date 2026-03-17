from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import TypeVar, Union

from ...common import VarTuple
from ...provider.essential import Mediator
from ..model.crown_definitions import (
    DictExtraPolicy,
    Extractor,
    ExtraForbid,
    ExtraKwargs,
    ExtraSkip,
    InpExtraMove,
    InputNameLayoutRequest,
    LeafInpCrown,
    LeafOutCrown,
    OutExtraMove,
    OutputNameLayoutRequest,
    Saturator,
    Sieve,
)

T = TypeVar("T")


ExtraIn = Union[ExtraSkip, str, Iterable[str], ExtraForbid, ExtraKwargs, Saturator]
ExtraOut = Union[ExtraSkip, str, Iterable[str], Extractor]

Key = Union[str, int]
KeyPath = VarTuple[Key]
PathsTo = Mapping[KeyPath, T]


class ExtraMoveMaker(ABC):
    @abstractmethod
    def make_inp_extra_move(
        self,
        mediator: Mediator,
        request: InputNameLayoutRequest,
    ) -> InpExtraMove:
        ...

    @abstractmethod
    def make_out_extra_move(
        self,
        mediator: Mediator,
        request: OutputNameLayoutRequest,
    ) -> OutExtraMove:
        ...


class StructureMaker(ABC):
    @abstractmethod
    def make_inp_structure(
        self,
        mediator: Mediator,
        request: InputNameLayoutRequest,
        extra_move: InpExtraMove,
    ) -> PathsTo[LeafInpCrown]:
        ...

    @abstractmethod
    def make_out_structure(
        self,
        mediator: Mediator,
        request: OutputNameLayoutRequest,
        extra_move: OutExtraMove,
    ) -> PathsTo[LeafOutCrown]:
        ...

    @abstractmethod
    def empty_as_list_inp(
        self,
        mediator: Mediator,
        request: InputNameLayoutRequest,
    ) -> bool:
        ...

    @abstractmethod
    def empty_as_list_out(
        self,
        mediator: Mediator,
        request: OutputNameLayoutRequest,
    ) -> bool:
        ...


class SievesMaker(ABC):
    @abstractmethod
    def make_sieves(
        self,
        mediator: Mediator,
        request: OutputNameLayoutRequest,
        paths_to_leaves: PathsTo[LeafOutCrown],
    ) -> PathsTo[Sieve]:
        ...


class ExtraPoliciesMaker(ABC):
    @abstractmethod
    def make_extra_policies(
        self,
        mediator: Mediator,
        request: InputNameLayoutRequest,
        paths_to_leaves: PathsTo[LeafInpCrown],
    ) -> PathsTo[DictExtraPolicy]:
        ...
