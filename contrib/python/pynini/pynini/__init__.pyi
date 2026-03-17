# Copyright 2016-2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# For general information on the Pynini grammar compilation library, see
# pynini.opengrm.org.

from _pywrapfst import Fst as _Fst
from _pywrapfst import VectorFst as _VectorFst
from _pywrapfst import Weight
from _pywrapfst import SymbolTable
from _pywrapfst import SymbolTableView

from _pywrapfst import FstArgError
from _pywrapfst import FstIOError
from _pywrapfst import FstOpError

## Typing imports.
from _pywrapfst import _ArcTypeFlag
from _pywrapfst import _Filename
from _pywrapfst import _Label
from _pywrapfst import _StateId
from _pywrapfst import ArcMapType
from _pywrapfst import ComposeFilter
from _pywrapfst import DeterminizeType
from _pywrapfst import EpsNormalizeType
from _pywrapfst import FarType
from _pywrapfst import ProjectType
from _pywrapfst import QueueType
from _pywrapfst import RandArcSelection
from _pywrapfst import ReplaceLabelType
from _pywrapfst import ReweightType
from _pywrapfst import SortType
from _pywrapfst import StateMapType
from _pywrapfst import WeightLike

from typing import Type, TypeVar, Union, Tuple, Any, Optional, List, Iterable, Iterator, ContextManager, Callable

# Custom exceptions.
class FstStringCompilationError(FstArgError, ValueError): ...

# Custom types

# TODO(wolfsonkin): Drop version check once Python 3.8 is our minimum version.
import sys
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

_TokenTypeFlag = Literal["byte", "utf8"]
CDRewriteDirection = Literal["ltr", "rtl", "sim"]
CDRewriteMode = Literal["obl", "opt"]
FarFileMode = Literal["r", "w"]

FstLike = Union[Fst, str]
TokenType = Union[SymbolTableView, _TokenTypeFlag]

# Helper functions.

_GenericCallable = TypeVar('_GenericCallable', bound=Callable[..., Any])
class _ContextDecoratorNone(ContextManager[None]):
  def __call__(self, func: _GenericCallable) -> _GenericCallable: ...

def default_token_type(token_type: TokenType) -> _ContextDecoratorNone: ...

T = TypeVar("T", bound="Fst")
class Fst(_VectorFst):
  def __init__(self, arc_type: _ArcTypeFlag = ...): ...
  @classmethod
  def from_pywrapfst(cls: Type[T], fst: _Fst) -> T: ...
  @classmethod
  def read(cls: Type[T], filename: _Filename) -> T: ...
  @classmethod
  def read_from_string(cls: Type[T], state: bytes) -> T: ...
  def __reduce__(self) -> Union[str, Tuple[Any, ...]]: ...
  def paths(self,
            input_token_type: Optional[TokenType] = ...,
            output_token_type: Optional[TokenType] = ...
  ) -> _StringPathIterator: ...
  def string(self, token_type: Optional[TokenType] = ...) -> str: ...
  # The following all override their definition in MutableFst.
  def copy(self: T) -> T: ...
  def closure(self: T, lower: int = ..., upper: int = ...) -> T: ...
  @property
  def plus(self) -> Fst: ...
  @property
  def ques(self) -> Fst: ...
  @property
  def star(self) -> Fst: ...
  def concat(self: T, fst2: FstLike) -> T: ...
  def optimize(self: T, compute_props: bool = ...) -> T: ...
  def union(self: T, *fsts2: FstLike) -> T: ...
  # Operator overloads.
  def __eq__(self, other: FstLike) -> bool: ...
  def __ne__(self, other: FstLike) -> bool: ...
  def __add__(self, other: FstLike) -> Fst: ...
  def __sub__(self, other: FstLike) -> Fst: ...
  def __pow__(self,
              other: Union[int, Tuple[int, Union[int, ellipsis]]]) -> Fst: ...
  def __matmul__(self, other: FstLike) -> Fst: ...
  def __or__(self, other: FstLike) -> Fst: ...
  # NOTE: Cython automatically generates the reversed overloads.
  def __req__(self, other: FstLike) -> bool: ...
  def __rne__(self, other: FstLike) -> bool: ...
  def __radd__(self, other: FstLike) -> Fst: ...
  def __rsub__(self, other: FstLike) -> Fst: ...
  def __rmatmul__(self, other: FstLike) -> Fst: ...
  def __ror__(self, other: FstLike) -> Fst: ...

# Utility functions
def escape(string: str) -> str: ...

# Core functions for FST creation.

def accep(astring: str,
          weight: Optional[WeightLike] = ...,
          arc_type: _ArcTypeFlag = ...,
          token_type: Optional[TokenType] = ...) -> Fst: ...
def cross(fst1: FstLike,
        fst2: FstLike) -> Fst: ...
def cdrewrite(
    tau: FstLike,
    l: FstLike,
    r: FstLike,
    sigma_star: FstLike,
    direction: CDRewriteDirection = ...,
    mode: CDRewriteMode = ...
) -> Fst: ...
def leniently_compose(fst1: FstLike,
                      fst2: FstLike,
                      sigma: FstLike,
                      compose_filter: ComposeFilter = ...,
                      connect: bool = ...) -> Fst: ...
def string_file(filename: _Filename,
                arc_type: _ArcTypeFlag = ...,
                input_token_type: Optional[TokenType] = ...,
                output_token_type: Optional[TokenType] = ...) -> Fst: ...
def string_map(lines: Iterable[Union[str, Iterable[str]]],
               arc_type: _ArcTypeFlag = ...,
               input_token_type: Optional[TokenType] = ...,
               output_token_type: Optional[TokenType] = ...) -> Fst: ...
def generated_symbols() -> SymbolTableView: ...


# # Decorator for one-argument constructive FST operations.

# NOTE: These are copy-pasta from _pywrapfst.pyx but with
# `s/ifst: Fst/ifst: FstLike/` and `s/-> MutableFst/-> Fst/`.


def arcmap(
    ifst: FstLike,
    delta: float = ...,
    map_type: ArcMapType = ...,
    power: float = ...,
    weight: Optional[WeightLike] = ...) -> Fst: ...
def determinize(
    ifst: FstLike,
    delta: float = ...,
    det_type: DeterminizeType = ...,
    nstate: _StateId = ...,
    subsequential_label: _Label = ...,
    weight: Optional[WeightLike] = ...,
    increment_subsequential_label: bool = ...) -> Fst: ...
def disambiguate(ifst: FstLike,
                 delta: float = ...,
                 nstate: _StateId = ...,
                 subsequential_label: _Label = ...,
                 weight: Optional[WeightLike] = ...) -> Fst: ...
def epsnormalize(ifst: FstLike,
                 eps_norm_type: EpsNormalizeType = ...) -> Fst: ...
def prune(ifst: FstLike,
          delta: float = ...,
          nstate: _StateId = ...,
          weight: Optional[WeightLike] = ...) -> Fst: ...
def push(ifst: FstLike,
         delta: float = ...,
         push_weights: bool = ...,
         push_labels: bool = ...,
         remove_common_affix: bool = ...,
         remove_total_weight: bool = ...,
         reweight_type: ReweightType = ...) -> Fst: ...
def randgen(
    ifst: FstLike,
    npath: int = ...,
    select: RandArcSelection = ...,
    max_length: int = ...,
    weighted: bool = ...,
    remove_total_weight: bool = ...,
    seed: int = ...) -> Fst: ...
def reverse(ifst: FstLike, require_superinitial: bool = ...) -> Fst: ...
def shortestpath(
    ifst: FstLike,
    delta: float = ...,
    nshortest: int = ...,
    nstate: _StateId = ...,
    queue_type: QueueType = ...,
    unique: bool = ...,
    weight: Optional[WeightLike] = ...) -> Fst: ...
def statemap(ifst: FstLike, map_type: StateMapType) -> Fst: ...
def synchronize(ifst: FstLike) -> Fst: ...

# NOTE: This are copy-pasta from _pywrapfst.pyx but with
# `s/ifst: Fst/fst: FstLike/`.

def shortestdistance(
    ifst: FstLike,
    delta: float = ...,
    nstate: _StateId = ...,
    queue_type: QueueType = ...,
    reverse: bool = ...) -> List[Weight]: ...

# # Two-argument constructive FST operations. If just one of the two FST
# # arguments has been compiled, the arc type of the compiled argument is used to
# # determine the arc type of the not-yet-compiled argument.

# NOTE: These are copy-pasta from _pywrapfst.pyx but with
# `s/ifst(\d): Fst/fst\1: FstLike/` and `s/-> MutableFst/-> Fst/`.


def compose(
    fst1: FstLike,
    fst2: FstLike,
    compose_filter: ComposeFilter = ...,
    connect: bool = ...) -> Fst: ...
def intersect(fst1: FstLike,
              fst2: FstLike,
              compose_filter: ComposeFilter = ...,
              connect: bool = ...) -> Fst: ...
# NOTE: This is copy-pasta from _pywrapfst.pyx but with
# `s/ifst(\d): Fst/fst\1: FstLike/` and `s/-> MutableFst/-> Fst/`.


def difference(
    fst1: FstLike,
    fst2: FstLike,
    compose_filter: ComposeFilter = ...,
    connect: bool = ...) -> Fst: ...
# # Simple comparison operations.

# NOTE: This is copy-pasta from _pywrapfst.pyx but with
# `s/ifst(\d): Fst/fst\1: FstLike/` and `s/-> MutableFst/-> Fst/`.

def equal(fst1: FstLike, fst2: FstLike, delta: float = ...) -> bool: ...
def equivalent(fst1: FstLike, fst2: FstLike, delta: float = ...) -> bool: ...
def isomorphic(fst1: FstLike, fst2: FstLike, delta: float = ...) -> bool: ...
def randequivalent(
    fst1: FstLike,
    fst2: FstLike,
    npath: int = ...,
    delta: float = ...,
    select: RandArcSelection = ...,
    max_length: int = ...,
    seed: int = ...) -> bool: ...
############################################################

def concat(fst1: FstLike, fst2: FstLike) -> Fst: ...
def replace(
    pairs: Iterable[Tuple[int, Fst]],
    call_arc_labeling: ReplaceLabelType = ...,
    return_arc_labeling: ReplaceLabelType = ...,
    epsilon_on_replace: bool = ...,
    return_label: _Label = ...) -> Fst: ...
def union(*fsts: FstLike) -> Fst: ...
# Pushdown transducer classes and operations.

class PdtParentheses:
  def __repr__(self) -> str: ...
  def __len__(self) -> int: ...
  def __iter__(self) -> Iterator[Tuple[int, int]]: ...
  def copy(self) -> PdtParentheses: ...
  def add_pair(self, push: int, pop: int) -> None: ...
  @classmethod
  def read(cls, filename: _Filename) -> PdtParentheses: ...
  def write(self, filename: _Filename) -> None: ...

def pdt_compose(fst1: FstLike,
                fst2: FstLike,
                parens: PdtParentheses,
                compose_filter: ComposeFilter = ...,
                left_pdt: bool = ...) -> Fst: ...
def pdt_expand(fst: FstLike,
               parens: PdtParentheses,
               connect: bool = ...,
               keep_parentheses: bool = ...,
               weight: Optional[WeightLike] = ...) -> Fst: ...
def pdt_replace(
    pairs: Iterable[Tuple[int, FstLike]],
    pdt_parser_type: str = ...,
    start_paren_labels: _Label = ...,
    left_paren_prefix: str = ...,
    right_paren_prefix: str = ...) -> Tuple[Fst, PdtParentheses]: ...
def pdt_reverse(fst: FstLike, parens: PdtParentheses) -> Fst: ...
def pdt_shortestpath(fst: FstLike,
                     parens: PdtParentheses,
                     queue_type: QueueType = ...,
                     keep_parentheses: bool = ...,
                     path_gc: bool = ...) -> Fst: ...

# Multi-pushdown transducer classes and operations.
class MPdtParentheses:
  def __repr__(self) -> str: ...
  def __len__(self) -> int: ...
  def __iter__(self) -> Iterator[Tuple[_Label, _Label, _Label]]: ...
  def copy(self) -> MPdtParentheses: ...
  def add_triple(self, push: _Label, pop: _Label, assignment: _Label) -> None: ...
  @classmethod
  def read(cls, filename: _Filename) -> MPdtParentheses: ...
  def write(self, filename: _Filename) -> None: ...

def mpdt_compose(fst1: FstLike,
                 fst2: FstLike,
                 parens: MPdtParentheses,
                 compose_filter: ComposeFilter = ...,
                 left_mpdt: bool = ...) -> Fst: ...
def mpdt_expand(fst: FstLike,
                parens: MPdtParentheses,
                connect: bool = ...,
                keep_parentheses: bool = ...) -> Fst: ...
def mpdt_reverse(fst: FstLike,
                 parens: MPdtParentheses) -> Tuple[Fst, MPdtParentheses]: ...

class _StringPathIterator:
  def __repr__(self) -> str: ...
  def __init__(self,
               fst: FstLike,
               input_token_type: Optional[TokenType] = ...,
               output_token_type: Optional[TokenType] = ...) -> None: ...
  def done(self) -> bool: ...
  def error(self) -> bool: ...
  def ilabels(self) -> List[_Label]: ...
  def olabels(self) -> List[_Label]: ...
  def istring(self) -> str: ...
  def istrings(self) -> Iterator[str]: ...
  def items(self) -> Iterator[Tuple[str, str, Weight]]: ...
  def next(self) -> None: ...
  def reset(self) -> None: ...
  def ostring(self) -> str: ...
  def ostrings(self) -> Iterator[str]: ...
  def weight(self) -> Weight: ...
  def weights(self) -> Iterator[Weight]: ...

class Far:
  def __init__(self,
               filename: _Filename,
               mode: FarFileMode = ...,
               arc_type: _ArcTypeFlag = ...,
               far_type: FarType = ...) -> None: ...
  def error(self) -> bool: ...
  # TODO(wolfsonkin): Maybe just return string.
  def arc_type(self) -> _ArcTypeFlag: ...
  def closed(self) -> bool: ...
  def far_type(self) -> Union[FarType, Literal["closed"]]: ...
  def mode(self) -> FarFileMode: ...
  def name(self) -> str: ...
  def done(self) -> bool: ...
  def find(self, key: str) -> bool: ...
  def get_fst(self) -> Fst: ...
  def get_key(self) -> str: ...
  def next(self) -> None: ...
  def reset(self) -> None: ...
  def __getitem__(self, key: str) -> Fst: ...
  def __next__(self) -> Tuple[str, Fst]: ...
  def __iter__(self) -> Far: ...
  def add(self, key: str, fst: Fst) -> None: ...
  # TODO(wolfsonkin): Make this support FstLike.
  def __setitem__(self, key: str, fst: Fst) -> None: ...
  def close(self) -> None: ...
  # Adds support for use as a PEP-343 context manager.
  def __enter__(self) -> Far: ...
  # TODO(wolfsonkin): Add typing to this.
  # See https://github.com/python/typeshed/blob/master/stdlib/2and3/builtins.pyi#L1664
  # for more detail.
  def __exit__(self, exc, value, tb): ...
## PYTHON IMPORTS.

# Classes from _pywrapfst.

from _pywrapfst import Arc
from _pywrapfst import EncodeMapper
from _pywrapfst import SymbolTable
from _pywrapfst import Weight
from _pywrapfst import _ArcIterator
from _pywrapfst import _MutableArcIterator
from _pywrapfst import _StateIterator

# Exceptions not yet imported.
from _pywrapfst import FstBadWeightError
from _pywrapfst import FstIndexError

# FST constants.
from _pywrapfst import NO_LABEL
from _pywrapfst import NO_STATE_ID
from _pywrapfst import NO_SYMBOL

# FST properties.
from _pywrapfst import ACCEPTOR
from _pywrapfst import ACCESSIBLE
from _pywrapfst import ACYCLIC
from _pywrapfst import ADD_ARC_PROPERTIES
from _pywrapfst import ADD_STATE_PROPERTIES
from _pywrapfst import ADD_SUPERFINAL_PROPERTIES
from _pywrapfst import ARC_SORT_PROPERTIES
from _pywrapfst import BINARY_PROPERTIES
from _pywrapfst import COACCESSIBLE
from _pywrapfst import COPY_PROPERTIES
from _pywrapfst import CYCLIC
from _pywrapfst import DELETE_ARC_PROPERTIES
from _pywrapfst import DELETE_STATE_PROPERTIES
from _pywrapfst import EPSILONS
from _pywrapfst import ERROR
from _pywrapfst import EXPANDED
from _pywrapfst import EXTRINSIC_PROPERTIES
from _pywrapfst import FST_PROPERTIES
from _pywrapfst import FstProperties
from _pywrapfst import INITIAL_ACYCLIC
from _pywrapfst import INITIAL_CYCLIC
from _pywrapfst import INTRINSIC_PROPERTIES
from _pywrapfst import I_DETERMINISTIC
from _pywrapfst import I_EPSILONS
from _pywrapfst import I_LABEL_INVARIANT_PROPERTIES
from _pywrapfst import I_LABEL_SORTED
from _pywrapfst import MUTABLE
from _pywrapfst import NEG_TRINARY_PROPERTIES
from _pywrapfst import NON_I_DETERMINISTIC
from _pywrapfst import NON_O_DETERMINISTIC
from _pywrapfst import NOT_ACCEPTOR
from _pywrapfst import NOT_ACCESSIBLE
from _pywrapfst import NOT_COACCESSIBLE
from _pywrapfst import NOT_I_LABEL_SORTED
from _pywrapfst import NOT_O_LABEL_SORTED
from _pywrapfst import NOT_STRING
from _pywrapfst import NOT_TOP_SORTED
from _pywrapfst import NO_EPSILONS
from _pywrapfst import NO_I_EPSILONS
from _pywrapfst import NO_O_EPSILONS
from _pywrapfst import NULL_PROPERTIES
from _pywrapfst import O_DETERMINISTIC
from _pywrapfst import O_EPSILONS
from _pywrapfst import O_LABEL_INVARIANT_PROPERTIES
from _pywrapfst import O_LABEL_SORTED
from _pywrapfst import POS_TRINARY_PROPERTIES
from _pywrapfst import RM_SUPERFINAL_PROPERTIES
from _pywrapfst import SET_ARC_PROPERTIES
from _pywrapfst import SET_FINAL_PROPERTIES
from _pywrapfst import SET_START_PROPERTIES
from _pywrapfst import STATE_SORT_PROPERTIES
from _pywrapfst import STRING
from _pywrapfst import TOP_SORTED
from _pywrapfst import TRINARY_PROPERTIES
from _pywrapfst import UNWEIGHTED
from _pywrapfst import UNWEIGHTED_CYCLES
from _pywrapfst import WEIGHTED
from _pywrapfst import WEIGHTED_CYCLES
from _pywrapfst import WEIGHT_INVARIANT_PROPERTIES

# Arc iterator properties.
from _pywrapfst import ARC_FLAGS
from _pywrapfst import ARC_I_LABEL_VALUE
from _pywrapfst import ARC_NEXT_STATE_VALUE
from _pywrapfst import ARC_NO_CACHE
from _pywrapfst import ARC_O_LABEL_VALUE
from _pywrapfst import ARC_VALUE_FLAGS
from _pywrapfst import ARC_WEIGHT_VALUE

# Encode mapper properties.
from _pywrapfst import ENCODE_FLAGS
from _pywrapfst import ENCODE_LABELS
from _pywrapfst import ENCODE_WEIGHTS

# NOTE: The following are copy-pasta from _pywrapfst.pyx but with
# `s/self: T/fst: FstLike/` and `s/-> T/-> Fst/`.

def arcsort(fst: FstLike, sort_type: SortType = ...) -> Fst: ...
def closure(fst: FstLike, lower: int = ..., upper: int = ...) -> Fst: ...
def connect(fst: FstLike) -> Fst: ...
def decode(fst: FstLike, mapper: EncodeMapper) -> Fst: ...
def encode(fst: FstLike, mapper: EncodeMapper) -> Fst: ...
def invert(fst: FstLike) -> Fst: ...
def minimize(fst: FstLike,
             delta: float = ...,
             allow_nondet: bool = ...) -> Fst: ...
def optimize(fst: FstLike, compute_props: bool = ...) -> Fst: ...
def project(fst: FstLike, project_type: ProjectType) -> Fst: ...
def relabel_pairs(
    fst: FstLike,
    ipairs: Optional[Iterable[Tuple[_Label, _Label]]] = ...,
    opairs: Optional[Iterable[Tuple[_Label, _Label]]] = ...) -> Fst: ...
def relabel_tables(fst: FstLike,
                   old_isymbols: Optional[SymbolTableView] = ...,
                   new_isymbols: Optional[SymbolTableView] = ...,
                   unknown_isymbol: str = ...,
                   attach_new_isymbols: bool = ...,
                   old_osymbols: Optional[SymbolTableView] = ...,
                   new_osymbols: Optional[SymbolTableView] = ...,
                   unknown_osymbol: str = ...,
                   attach_new_osymbols: bool = ...) -> Fst: ...
def reweight(fst: FstLike,
             potentials: Iterable[WeightLike],
             reweight_type: ReweightType = ...) -> Fst: ...
def rmepsilon(fst: FstLike,
              queue_type: QueueType = ...,
              connect: bool = ...,
              weight: Optional[WeightLike] = ...,
              nstate: _StateId = ...,
              delta: float = ...) -> Fst: ...
def topsort(fst: FstLike) -> Fst: ...
#############################################

from _pywrapfst import compact_symbol_table
from _pywrapfst import merge_symbol_table

from _pywrapfst import divide
from _pywrapfst import power
from _pywrapfst import plus
from _pywrapfst import times

