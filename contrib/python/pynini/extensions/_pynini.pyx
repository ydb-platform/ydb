#cython: c_string_encoding=utf8, c_string_type=unicode, language_level=3, c_api_binop_methods=True, nonecheck=True
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
"""Pynini: finite-state grammar compilation for Python."""


## IMPLEMENTATION.

## Cython imports. Sorry. There are a lot.

from cython.operator cimport address as addr       # &foo
from cython.operator cimport dereference as deref  # *foo
from cython.operator cimport preincrement as inc   # ++foo

from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint64_t

from libcpp cimport bool
from libcpp.memory cimport static_pointer_cast
from libcpp.memory cimport unique_ptr
from libcpp.utility cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

from cmemory cimport WrapUnique

from cpywrapfst cimport ArcSort
from cpywrapfst cimport ArcSortType
from cpywrapfst cimport Closure
from cpywrapfst cimport CLOSURE_PLUS
from cpywrapfst cimport CLOSURE_STAR
from cpywrapfst cimport Compose
from cpywrapfst cimport ComposeOptions
from cpywrapfst cimport FstClass
from cpywrapfst cimport GetTokenType
from cpywrapfst cimport kDelta
from cpywrapfst cimport kError
from cpywrapfst cimport kNoLabel
from cpywrapfst cimport kNoStateId
from cpywrapfst cimport kNoSymbol
from cpywrapfst cimport kILabelSorted
from cpywrapfst cimport kOLabelSorted
from cpywrapfst cimport LabelFstClassPair
from cpywrapfst cimport MutableFstClass
from cpywrapfst cimport VectorFstClass
from cpywrapfst cimport WeightClass

from _pywrapfst cimport FarReader
from _pywrapfst cimport FarWriter
from _pywrapfst cimport Fst as _Fst
from _pywrapfst cimport MutableFst as _MutableFst
from _pywrapfst cimport SymbolTable_ptr
from _pywrapfst cimport const_SymbolTable_ptr
from _pywrapfst cimport VectorFst as _VectorFst
from _pywrapfst cimport Weight as _Weight
from _pywrapfst cimport SymbolTable as _SymbolTable
from _pywrapfst cimport SymbolTableView as _SymbolTableView
from _pywrapfst cimport _get_WeightClass_or_one
from _pywrapfst cimport _get_WeightClass_or_zero
from _pywrapfst cimport _get_compose_filter
from _pywrapfst cimport _get_queue_type
from _pywrapfst cimport _get_replace_label_type
from _pywrapfst cimport equal
from _pywrapfst cimport replace as _replace
from _pywrapfst cimport tostring
from _pywrapfst cimport path_tostring

# C++ code for Pynini not from fst_util.

from cpynini cimport CDRewriteCompile
from cpynini cimport CDRewriteDirection as _CDRewriteDirection
from cpynini cimport CDRewriteMode as _CDRewriteMode
from cpynini cimport Compose
from cpynini cimport ConcatRange
from cpynini cimport Cross
from cpynini cimport Escape
from cpynini cimport Expand
from cpynini cimport GeneratedSymbols
from cpynini cimport GetCDRewriteDirection
from cpynini cimport GetDefaultSymbols
from cpynini cimport GetDefaultTokenType
from cpynini cimport GetCDRewriteMode
from cpynini cimport GetPdtComposeFilter
from cpynini cimport GetPdtParserType
from cpynini cimport LenientlyCompose
from cpynini cimport MPdtComposeOptions
from cpynini cimport MPdtExpandOptions
from cpynini cimport Optimize
from cpynini cimport OptimizeDifferenceRhs
from cpynini cimport PdtComposeFilter
from cpynini cimport PdtComposeOptions
from cpynini cimport PdtExpandOptions
from cpynini cimport PdtParserType
from cpynini cimport PdtShortestPathOptions
from cpynini cimport Replace
from cpynini cimport Reverse
from cpynini cimport ReadLabelPairs
from cpynini cimport ReadLabelTriples
from cpynini cimport ShortestPath
from cpynini cimport StringCompile
from cpynini cimport PopDefaults
from cpynini cimport PushDefaults
from cpynini cimport StringFileCompile
from cpynini cimport StringMapCompile
from cpynini cimport StringPathIteratorClass
from cpynini cimport StringPrint
from cpynini cimport TokenType as _TokenType
from cpynini cimport WriteLabelPairs
from cpynini cimport WriteLabelTriples
from cpynini cimport kBosIndex
from cpynini cimport kEosIndex


# Python imports needed for implementation.


import contextlib
import functools
import io
import os

from _pywrapfst import FstArgError
from _pywrapfst import FstIOError
from _pywrapfst import FstOpError

import _pywrapfst


# Custom exceptions.


class FstStringCompilationError(FstArgError, ValueError):

  pass


# Helper functions.


cdef _TokenType _get_token_type(const string &token_type) except *:
  """Matches string with the appropriate TokenType enum value.

  This function takes a string argument and returns the matching TokenType
  enum value used by many string/FST conversion functions.

  Args:
    token_type: A string matching a known token type.

  Returns:
    A TokenType enum value.

  Raises:
    FstArgError: Unknown token type.

  This function is not visible to Python users.
  """
  cdef _TokenType _token_type
  if not GetTokenType(token_type, addr(_token_type)):
    raise FstArgError(f"Unknown token type: {token_type}")
  return _token_type


cdef _CDRewriteDirection _get_cdrewrite_direction(
    const string &direction) except *:
  """Matches string with the appropriate CDRewriteDirection enum value.

  This function takes a string argument and returns the matching
  CDRewriteDirection enum value used by PyniniCDRewrite.

  Args:
    direction: A string matching a known rewrite direction type.

  Returns:
    A CDRewriteDirection enum value.

  Raises:
    FstArgError: Unknown context-dependent rewrite direction.

  This function is not visible to Python users.
  """
  cdef _CDRewriteDirection _direction
  if not GetCDRewriteDirection(direction, addr(_direction)):
    raise FstArgError(
        f"Unknown context-dependent rewrite direction: {direction}")
  return _direction


cdef _CDRewriteMode _get_cdrewrite_mode(const string &mode) except *:
  """Matches string with the appropriate CDRewriteMode enum value.

  This function takes a string argument and returns the matching
  CDRewriteMode enum value used by PyniniCDRewrite.

  Args:
    mode: A string matching a known rewrite mode type.

  Returns:
    A CDRewriteMode enum value.

  Raises:
    FstArgError: Unknown context-dependent rewrite mode.

  This function is not visible to Python users.
  """
  cdef _CDRewriteMode _mode
  if not GetCDRewriteMode(mode, addr(_mode)):
    raise FstArgError(
        f"Unknown context-dependent rewrite mode: {mode}")
  return _mode


cdef PdtComposeFilter _get_pdt_compose_filter(
    const string &compose_filter) except *:
  """Matches string with the appropriate PdtComposeFilter enum value.

  Args:
    compose_filter: A string matching a known PdtComposeFilter type.

  Returns:
    A PdtComposeFilter enum value.

  Raises:
    FstArgError: Unknown PDT compose filter type.

  This function is not visible to Python users.
  """
  cdef PdtComposeFilter _compose_filter
  if not GetPdtComposeFilter(compose_filter, addr(_compose_filter)):
    raise FstArgError(f"Unknown PDT compose filter type: {compose_filter}")
  return _compose_filter


cdef PdtParserType _get_pdt_parser_type(const string &pdt_parser_type) except *:
  """Matches string with the appropriate PdtParserType enum value.

  This function takes a string argument and returns the matching PdtParserType
  enum value used by PDT Replace.

  Args:
    pdt_parser_type: A string matching a known parser type.

  Returns:
    A PdtParserType enum value.

  Raises:
    FstArgError: Unknown PDT parser type.

  This function is not visible to Python users.
  """
  cdef PdtParserType _pdt_parser_type
  if not GetPdtParserType(pdt_parser_type, addr(_pdt_parser_type)):
    raise FstArgError(f"Unknown PDT parser type: {pdt_parser_type}")
  return _pdt_parser_type


cdef void _maybe_arcsort(MutableFstClass *fst1, MutableFstClass *fst2):
  """Arc-sorts two FST arguments for composition, if necessary.

  Args:
    fst1: A pointer to the left-hand MutableFstClass.
    fst2: A pointer to the right-hand MutableFstClass.

  This function is not visible to Python users.
  """
  # It is probably much quicker to force recomputation of the property (if
  # necessary) to call the underlying sort on a vector of arcs.
  if fst1.Properties(kOLabelSorted, True) != kOLabelSorted:
    ArcSort(fst1, ArcSortType.OLABEL_SORT)
  if fst2.Properties(kILabelSorted, True) != kILabelSorted:
    ArcSort(fst2, ArcSortType.ILABEL_SORT)


class default_token_type(contextlib.ContextDecorator):
  """Override the default token_type used by Pynini functions and classes.

  A context manager and context decorator to temporarily override the default
  token_type used by Pynini functions and classes.

  Args:
    token_type: A string indicating how the string is to be constructed from
        arc labels---one of: "byte" (interprets arc labels as raw bytes),
        "utf8" (interprets arc labels as Unicode code points), or a
        SymbolTable.

  Returns:
    A context decorator that temporarily overrides the default token_type used
    by Pynini.

  Raises:
    FstArgError: Unknown token type.
  """

  def __init__(self, token_type):
    self._token_type = token_type

  def __enter__(self):
    cdef _TokenType _token_type
    cdef const_SymbolTable_ptr _symbols = NULL
    if isinstance(self._token_type, _pywrapfst.SymbolTableView):
      _token_type = _TokenType.SYMBOL
      _symbols = (<_SymbolTableView> self._token_type)._raw_ptr_or_raise()
    else:
      _token_type = _get_token_type(tostring(self._token_type))
    PushDefaults(_token_type, _symbols)

  def __exit__(self, exc_type, exc_value, traceback):
    PopDefaults()


# Class for FSTs created from within Pynini. It overrides instance methods of
# the superclass which take an FST argument so that it can string-compile said
# argument if it is not yet an FST. It also overloads binary == (equals),
# * and @ (composition), # + (concatenation), and | (union).


cdef class Fst(_VectorFst):

  """
  Fst(arc_type="standard")

  This class wraps a mutable FST and exposes all destructive methods.

  Args:
    arc_type: An optional string indicating the arc type for the FST.
  """

  cdef void _from_MutableFstClass(self, MutableFstClass *tfst):
    """
    _from_MutableFstClass(tfst)

    Constructs a Pynini Fst by taking ownership of a MutableFstClass pointer.

    This method is not visible to Python users.
    """
    self._fst.reset(tfst)
    self._mfst = static_pointer_cast[MutableFstClass, FstClass](self._fst)

  def __init__(self, arc_type="standard"):
    cdef unique_ptr[VectorFstClass] _tfst
    _tfst.reset(new VectorFstClass(tostring(arc_type)))
    if _tfst.get().Properties(kError, True) == kError:
       raise FstArgError(f"Unknown arc type: {arc_type}")
    self._from_MutableFstClass(_tfst.release())

  @classmethod
  def from_pywrapfst(cls, _Fst fst):
    """
    Fst.from_pywrapfst(fst)

    Constructs a Pynini FST from a _pywrapfst.Fst.

    This class method converts an FST from the pywrapfst module (_pywrapfst.Fst
    or its derivatives) to a Pynini.Fst. This is essentially a downcasting
    operation which grants the object additional instance methods, including an
    enhanced `closure`, `paths`, and `string`. The input FST is not invalidated,
    but mutation of the input or output object while the other is still in scope
    will trigger a deep copy.

    Args:
      fst: Input FST of type _pywrapfst.Fst.

    Returns:
      An FST of type Fst.
    """
    return _from_pywrapfst(fst)

  @classmethod
  def read(cls, filename):
    """
    Fst.read(filename)

    Reads an FST from a file.

    Args:
      filename: The string location of the input file.

    Returns:
      An FST.

    Raises:
      FstIOError: Read failed.
    """
    return _read(filename)

  @classmethod
  def read_from_string(cls, state):
    """
    Fst.read(string)

    Reads an FST from a serialized string.

    Args:
      state: A string containing the serialized FST.

    Returns:
      An FST.

    Raises:
      FstIOError: Read failed.
    """
    return _read_from_string(state)

  # Registers the class for pickling.

  def __reduce__(self):
    return (_read_from_string, (self.write_to_string(),))

  cpdef _StringPathIterator paths(self, input_token_type=None,
                                  output_token_type=None):
    """
    paths(self, input_token_type=None, output_token_type=None)

    Creates iterator over all string paths in an acyclic FST.

    This method returns an iterator over all paths (represented as pairs of
    strings and an associated path weight) through an acyclic FST. This
    operation is only feasible when the FST is acyclic. Depending on the
    requested token type, the arc labels along the input and output sides of a
    path are interpreted as UTF-8-encoded Unicode strings, raw bytes, or a
    concatenation of string labels from a symbol table.

    Args:
      input_token_type: An optional string indicating how the input strings are
          to be constructed from arc labels---one of: "byte" (interprets arc
          labels as raw bytes), "utf8" (interprets arc labels as Unicode code
          points), "symbol" (interprets arc labels using the input symbol
          table), or a SymbolTable. If not set, or set to None, the value is set
          to the default token_type, which begins as "byte", but can be
          overridden for regions of code using the default_token_type context
          manager.
      output_token_type: An optional string indicating how the output strings
          are to be constructed from arc labels---one of: "byte" (interprets arc
          labels as raw bytes), "utf8" (interprets arc labels as Unicode code
          points), "symbol" (interprets arc labels using the input symbol
          table), or a SymbolTable. If not set, or set to None, the value is set
          to the default token_type, which begins as "byte", but can be
          overridden for regions of code using the default_token_type context
          manager.

    Raises:
      FstArgError: Unknown token type.
      FstOpError: Operation failed.
    """
    return _StringPathIterator(self, input_token_type, output_token_type)

  cpdef string string(self, token_type=None) except *:
    """
    string(self, token_type=None)

    Creates a string from a string FST.

    This method returns the string recognized by the FST as a Python byte or
    Unicode string. This is only well-defined when the FST is an acceptor and a
    "string" FST (meaning that the start state is numbered 0, and there is
    exactly one transition from each state i to each state i + 1, there are no
    other transitions, and the last state is final). Depending on the requested
    token type, the arc labels are interpreted as a UTF-8-encoded Unicode
    string, raw bytes, or as a concatenation of string labels from the output
    symbol table.

    The underlying routine reads only the output labels, so if the FST is
    not an acceptor, it will be treated as the output projection of the FST.

    Args:
      token_type: An optional string indicating how the string is to be
          constructed from arc labels---one of: "byte" (interprets arc labels as
          raw bytes), "utf8" (interprets arc labels as Unicode code points), or
          a SymbolTable. If not set, or set to None, the value is set to the
          default token_type, which begins as "byte", but can be overridden for
          regions of code using the default_token_type context manager.

    Returns:
      The string corresponding to (an output projection) of the FST.

    Raises:
      FstArgError: Unknown token type.
      FstOpError: Operation failed.
    """
    cdef _TokenType _token_type
    cdef const_SymbolTable_ptr _symbols = NULL
    if token_type is None:
      _token_type = GetDefaultTokenType()
      _symbols = GetDefaultSymbols()
    elif isinstance(token_type, _pywrapfst.SymbolTableView):
      _token_type = _TokenType.SYMBOL
      _symbols = (<_SymbolTableView> token_type)._raw_ptr_or_raise()
    else:
      _token_type = _get_token_type(tostring(token_type))
    cdef string result
    if not StringPrint(deref(self._fst), addr(result), _token_type, _symbols):
      raise FstOpError("Operation failed")
    return result

  # The following all override their definition in MutableFst.

  cpdef Fst copy(self):
    """
    copy(self)

    Makes a copy of the FST.
    """
    return _init_Fst_from_MutableFst(super(_MutableFst, self).copy())

  def closure(self, int32_t lower=0, int32_t upper=0):
    """
    closure(self, lower)
    closure(self, lower, upper)

    Computes concatenative closure.

    This operation destructively converts the FST to its concatenative closure.
    If A transduces string x to y with weight w, then the zero-argument form
    `A.closure()` mutates A so that it transduces between empty strings with
    weight 1, transduces string x to y with weight w, transduces xx to yy with
    weight w \otimes w, string xxx to yyy with weight w \otimes w \otimes w
    (and so on).

    When called with two optional positive integer arguments, these act as
    lower and upper bounds, respectively, for the number of cycles through the
    original FST that the mutated FST permits. Therefore, `A.closure(0, 1)`
    mutates A so that it permits 0 or 1 cycles; i.e., the mutated A transduces
    between empty strings or transduces x to y.

    When called with one optional positive integer argument, this argument
    acts as the lower bound, with the upper bound implicitly set to infinity.
    Therefore, `A.closure(1)` performs a mutation roughly equivalent to
    `A.closure()` except that the former does not transduce between empty
    strings.

    The following are the equivalents for the closure-style syntax used in
    Perl-style regular expressions:

    Regexp:\t\tThis method:\t\tCopy shortcuts:

    /x?/\t\tx.closure(0, 1)\t\tx.ques
    /x*/\t\tx.closure()\t\tx.star
    /x+/\t\tx.closure(1)\t\tx.plus
    /x{N}/\t\tx.closure(N, N)\t\tx ** N
    /x{M,N}/\t\tx.closure(M, N)
    /x{N,}/\t\tx.closure(N)
    /x{,N}/\t\tx.closure(0, N)

    Args:
      lower: lower bound.
      upper: upper bound.

    Returns:
      self.
    """
    ConcatRange(self._mfst.get(), lower, upper)
    self._check_mutating_imethod()
    return self

  @property
  def plus(self):
    """
    plus(self)

    Constructively computes +-closure.

    Returns:
      An FST copy.
    """
    cdef Fst result = self.copy()
    Closure(result._mfst.get(), CLOSURE_PLUS)
    result._check_mutating_imethod()
    return result

  @property
  def ques(self):
    """
    ques(self)

    Constructively computes ?-closure.

    Returns:
      An FST copy.
    """
    cdef Fst result = self.copy()
    ConcatRange(result._mfst.get(), 0, 1)
    result._check_mutating_imethod()
    return result

  @property
  def star(self):
    """
    star(self)

    Constructively computes *-closure.

    Returns:
      An FST copy.
    """
    cdef Fst result = self.copy()
    Closure(result._mfst.get(), CLOSURE_STAR)
    result._check_mutating_imethod()
    return result

  def concat(self, fst2):
    """
    concat(self, fst2)

    Computes the concatenation (product) of two FSTs.

    This operation destructively concatenates the FST with a second FST. If A
    transduces string x to y with weight a and B transduces string w to v with
    weight b, then their concatenation transduces string xw to yv with weight
    a \otimes b.

    Args:
      fst2: The second input FST.

    Returns:
      self.
    """
    cdef Fst _fst2 = _compile_or_copy_Fst(fst2, self.arc_type())
    return super().concat(_fst2)

  cdef void _optimize(self, bool compute_props=False) except *:
    Optimize(self._mfst.get(), compute_props)
    self._check_mutating_imethod()

  def optimize(self, bool compute_props=False):
    """
    optimize(self, compute_props=False)

    Performs a generic optimization of the FST.

    This operation destructively optimizes the FST, producing an equivalent
    FST while heuristically reducing the number of states. The algorithm is
    as follows:

    * If the FST is not (known to be) epsilon-free, perform epsilon-removal.
    * Combine identically labeled multi-arcs and sum their weights.
    * If the FST does not have idempotent weights, halt.
    * If the FST is not (known to be) deterministic:
      - If the FST is a (known) acceptor:
        * If the FST is not (known to be) unweighted and/or acyclic, encode
          weights.
      - Otherwise, encode labels and, if the FST is not (known to be)
        unweighted, encode weights.
      - Determinize the FST.
    * Minimize the FST.
    * If the FST was previously encoded, decode it, and combine
      identically-labeled multi-arcs and sum their weights.

    This optimization generally reduces the number of states and arcs, and may
    also result in faster composition. However, determinization, a prerequisite
    for minimization, may in the worst case trigger an exponential blowup in
    the number of states. Judicious use of optimization is something of a black
    art, but one is generally encouraged to optimize final forms of rules
    or cascades thereof.

    Args:
      compute_props: Should unknown FST properties be computed to help choose
          appropriate optimizations?

    Returns:
      self.
    """
    self._optimize(compute_props)
    return self

  def union(self, *fsts2):
    return super().union(*(_compile_or_copy_Fst(fst2, self.arc_type())
                           for fst2 in fsts2))

  # Operator overloads.

  def __eq__(self, other):
    cdef Fst _fst1
    cdef Fst _fst2
    (_fst1, _fst2) = _compile_or_copy_two_Fsts(self, other)
    return equal(_fst1, _fst2)

  def __ne__(self, other):
    return not self == other

  def __add__(self, other):
    return concat(self, other)

  def __iadd__(self, other):
    return self.concat(other)

  def __sub__(self, other):
    return difference(self, other)

  # __isub__ is not implemented separately because difference is not an
  # in-place operation.

  def __pow__(self, other, modulo):
    """Constructively generates the range-concatenation of the FST.

    For all natural numbers n, `f ** n` is the same as `f ** (n, n).
    Note that `f ** (0, ...)` is the same as `f.star`,
    `f ** (1, ...)` is `f.plus`,
    `f ** (0, 1)` is the same as `f.ques`.
    and `f ** (5, ...)` is the obvious generalization.
    """
    if not isinstance(self, Fst) or modulo is not None:
      return NotImplemented
    if isinstance(other, int):
      return closure(self, other, other)
    if isinstance(other, tuple):
      try:
        (lower, upper) = other
      except ValueError:
          raise ValueError("Expected tuple of length two")
      if lower is Ellipsis:
        raise TypeError("The lower bound must be an integer")
      elif upper is Ellipsis:
        return closure(self, lower)
      else:
        return closure(self, lower, upper)
    return NotImplemented

  # TODO(kbg): Cython only has support for two-argument __ipow__; see:
  #
  #   http://github.com/cython/cython/commit/829d6
  #
  # If this ever changes, implement __ipow__ in the obvious fashion.

  def __matmul__(self, other):
    return compose(self, other)

  # __imatmul__ is not implemented separately because composition is not an
  # in-place operation.

  def __or__(self, other):
    return union(self, other)

  def __ior__(self, other):
    return self.union(other)


# Makes a reference-counted copy, if it's already an FST; otherwise, compiles
# it into an acceptor.


cdef Fst _compile_or_copy_Fst(arg, arc_type="standard"):
  if not isinstance(arg, Fst):
    return accep(arg, arc_type=arc_type)
  else:
    return arg.copy()


# Makes copies or compiles, using the arc type of the first if specified,
# then the arc type of the second, then using the default.


cdef object _compile_or_copy_two_Fsts(fst1, fst2):
  cdef Fst _fst1
  cdef Fst _fst2
  if isinstance(fst1, Fst):
    _fst1 = fst1.copy()
    _fst2 = _compile_or_copy_Fst(fst2, _fst1.arc_type())
  elif isinstance(fst2, Fst):
    _fst2 = fst2.copy()
    _fst1 = accep(fst1, arc_type=_fst2.arc_type())
  else:
    _fst1 = accep(fst1)
    _fst2 = accep(fst2)
  return (_fst1, _fst2)


# Down-casts a MutableFst to a Pynini Fst by taking ownership of the
# underlying pointers of the former.


cdef Fst _init_Fst_from_MutableFst(_MutableFst fst):
  cdef Fst result = Fst.__new__(Fst)
  result._fst = fst._fst
  result._mfst = fst._mfst
  return result


# Calls pywrapfst class methods and then down-cast to a Pynini Fst.


cpdef Fst _from_pywrapfst(_Fst fst):
  cdef Fst result = Fst.__new__(Fst)
  result._from_MutableFstClass(new VectorFstClass(deref(fst._fst)))
  return result


cpdef Fst _read(filename):
  return _from_pywrapfst(_pywrapfst.Fst.read(filename))


cpdef Fst _read_from_string(state):
  return _from_pywrapfst(_pywrapfst.Fst.read_from_string(state))


# Utility functions.


cpdef string escape(data):
  """
  escape(data)

  Escape all characters in a string that can be used to generate symbols.

  This function returns a new string which backslash-escapes the opening and
  closing square bracket characters as well as backslashes to allow the passing
  of arbitrary strings into Pynini functions without worrying about string
  compilation errors.

  Args:
    data: The input string.

  Returns:
    An escaped string.
  """
  return Escape(tostring(data))


# Functions for FST compilation.


cpdef Fst accep(astring, weight=None, arc_type="standard", token_type=None):
  """
  accep(astring, weight=None, arc_type=None, token_type=None)

  Creates an acceptor from a string.

  This function creates an FST which accepts its input with a fixed weight
  (defaulting to semiring One).

  Args:
    astring: The input string.
    weight: A Weight or weight string indicating the desired path weight. If
        omitted or null, the path weight is set to semiring One.
    arc_type: An optional string indicating the arc type for the compiled FST.
        This argument is silently ignored if istring and/or ostring is already
        compiled.
    token_type: An optional string indicating how the input string is to be
        encoded as arc labels---one of: "utf8" (encodes the strings as UTF-8
        encoded Unicode string), "byte" (encodes the string as raw bytes)---or
        a SymbolTable to be used to encode the string. If not set, or set to
        None, the value is set to the default token_type, which begins as
        "byte", but can be overridden for regions of code using the
        default_token_type context manager.

    Returns:
      An FST.

    Raises:
      FstArgError: Unknown arc type.
      FstArgError: Unknown token type.
      FstOpError: Operation failed.
      FstStringCompilationError: String compilation failed.
  """
  cdef Fst result = Fst(arc_type)
  cdef WeightClass _weight = _get_WeightClass_or_one(result.weight_type(),
                                                     weight)
  cdef _TokenType _token_type
  cdef const_SymbolTable_ptr _symbols = NULL
  if token_type is None:
    _token_type = GetDefaultTokenType()
    _symbols = GetDefaultSymbols()
  elif isinstance(token_type, _pywrapfst.SymbolTableView):
    _token_type = _TokenType.SYMBOL
    _symbols = (<_SymbolTableView> token_type)._raw_ptr_or_raise()
  else:
    _token_type = _get_token_type(tostring(token_type))
  cdef bool success = StringCompile(
      tostring(astring),
      result._mfst.get(),
      _token_type,
      _symbols,
      _weight)
  # First we check whether there were problems with arc or weight type, then
  # for string compilation issues.
  result._check_mutating_imethod()
  if not success:
    raise FstStringCompilationError("String compilation failed")
  return result


cpdef Fst cross(fst1, fst2):
  """
  cross(fst1, fst2)

  Creates a cross-product transducer.

  This function creates an FST which transduces from the upper language
  to the lower language.

  Args:
    fst1: The input string, or an acceptor FST representing the upper
        language.
    fst2: The output string, or an acceptor FST representing the upper
        language.

  Returns:
    An FST.

  Raises:
    FstOpError: Operation failed.
  """
  cdef Fst _fst1
  cdef Fst _fst2
  (_fst1, _fst2) = _compile_or_copy_two_Fsts(fst1, fst2)
  cdef Fst result = Fst(_fst1.arc_type())
  Cross(deref(_fst1._fst), deref(_fst2._fst), result._mfst.get())
  result._check_mutating_imethod()
  return result


cpdef Fst cdrewrite(tau, l, r, sigma_star, direction="ltr", mode="obl"):
  """
  cdrewrite(tau, l, r, sigma_star, direction="ltr", mode="obl")

  Compiles a transducer expressing a context-dependent rewrite rule.

  This operation compiles a transducer representing a context-dependent
  rewrite rule of the form

      tau / L __ R

  over a finite vocabulary.

  There are two reserved symbols: "[BOS]" denotes the left edge of a string
  within L, and "[EOS]" (end of string) denotes the right edge of a string
  within R. Note that these reserved symbols do not have any special
  interpretation anywhere else within this library.
  
  Args:
    tau: A transducer representing the desired transduction tau.
    l: An unweighted acceptor representing the left context L.
    r: An unweighted acceptor representing the right context R.
    sigma_star: A cyclic, unweighted acceptor representing the closure over the
        alphabet.
    direction: A string specifying the direction of rule application; one of:
        "ltr" (left-to-right application), "rtl" (right-to-left application),
        or "sim" (simultaneous application).
    mode: A string specifying the mode of rule application; one of: "obl"
        (obligatory application), "opt" (optional application).

  Returns:
    An FST.

  Raises:
    FstArgError: Unknown cdrewrite direction type.
    FstArgError: Unknown cdrewrite mode type.
    FstOpError: Operation failed.
  """
  cdef Fst _sigma_star = _compile_or_copy_Fst(sigma_star)
  cdef string arc_type = _sigma_star.arc_type()
  cdef Fst _tau = _compile_or_copy_Fst(tau, arc_type)
  cdef Fst _l = _compile_or_copy_Fst(l, arc_type)
  cdef Fst _r = _compile_or_copy_Fst(r, arc_type)
  cdef Fst result = Fst(arc_type)
  cdef _CDRewriteDirection _direction = _get_cdrewrite_direction(tostring(
      direction))
  cdef _CDRewriteMode _mode = _get_cdrewrite_mode(tostring(mode))
  CDRewriteCompile(deref(_tau._fst),
                   deref(_l._fst),
                   deref(_r._fst),
                   deref(_sigma_star._fst),
                   result._mfst.get(),
                   _direction,
                   _mode,
                   kBosIndex,
                   kEosIndex)
  result._check_mutating_imethod()
  return result


cpdef Fst leniently_compose(mu, nu, sigma_star, compose_filter="auto",
                            bool connect=True):
  """
  leniently_compose(mu, nu, sigma_star, compose_filter="auto", connect=True)

  Constructively leniently-composes two FSTs.

  This operation computes the lenient composition of two FSTs. The lenient
  composition of two FSTs is the priority union of their composition and the
  left-hand side argument, where priority union is simply union in which the
  left-hand side argument's relations have "priority" over the right-hand side
  argument's relations.

  Args:
    mu: The first input FST, taking higher priority.
    nu: The second input FST, taking lower priority.
    sigma_star: A cyclic, unweighted acceptor representing the closure over the
        alphabet.
    compose_filter: A string matching a known composition filter; one of:
        "alt_sequence", "auto", "match", "no_match", "null", "sequence",
        "trivial".
    connect: Should output be trimmed?

  Returns:
    An FST.

  Raises:
    FstOpError: Operation failed.
  """
  cdef Fst _mu
  cdef Fst _nu
  (_mu, _nu) = _compile_or_copy_two_Fsts(mu, nu)
  cdef Fst _sigma_star = _compile_or_copy_Fst(sigma_star, _mu.arc_type())
  cdef unique_ptr[ComposeOptions] _opts
  _opts.reset(
      new ComposeOptions(connect,
                         _get_compose_filter(tostring(compose_filter))))
  cdef Fst result = Fst(_mu.arc_type())
  LenientlyCompose(deref(_mu._fst),
                   deref(_nu._fst),
                   deref(_sigma_star._fst),
                   result._mfst.get(),
                   deref(_opts))
  result._check_mutating_imethod()
  return result


cpdef Fst string_file(filename,
                      arc_type="standard",
                      input_token_type=None,
                      output_token_type=None):
  """
  string_file(filename, arc_type="standard",
              input_token_type=None, output_token_type=None)

  Creates a transducer that maps between elements of mappings read from
  a tab-delimited file.

  The first column is interpreted as the input string to a transduction.

  The second column, separated from the first by a single tab character, is
  interpreted as the output string for the transduction; an acceptor can be
  modeled by using identical first and second columns.

  An optional third column, separated from the second by a single tab character,
  is interpreted as a weight for the transduction; if not specified the weight
  defaults to semiring One. Note that weights are never permitted in the second
  column.

  The comment character is #, and has scope until the end of the line. Any
  preceding whitespace before a comment is ignored. To use the '#' literal
  (i.e., to ensure it is not interpreted as the start of a comment) escape it
  with \; the escaping \ in the string "\#" also ignored.

  Args:
    filename: The path to a TSV file formatted as described above.
    arc_type: A string indicating the arc type.
    input_token_type: An optional string indicating how the input strings are
        to be encoded as arc labels---one of: "utf8" (encodes strings as a UTF-8
        encoded Unicode strings), "byte" (encodes strings as raw bytes)---or a
        SymbolTable. If not set, or set to None, the value is set to the
        default token_type, which begins as "byte", but can be overridden for
        regions of code using the default_token_type context manager.
    output_token_type: An optional string indicating how the output strings are
        to be encoded as arc labels---one of: "utf8" (encodes strings as a UTF-8
        encoded Unicode strings), "byte" (encodes strings as raw bytes)---or a
        SymbolTable. If not set, or set to None, the value is set to the
        default token_type, which begins as "byte", but can be overridden for
        regions of code using the default_token_type context manager.

  Returns:
    An FST.

  Raises:
    FstIOError: Read failed.
  """
  cdef _TokenType _input_token_type
  cdef const_SymbolTable_ptr _isymbols = NULL
  if input_token_type is None:
    _input_token_type = GetDefaultTokenType()
    _isymbols = GetDefaultSymbols()
  elif isinstance(input_token_type, _pywrapfst.SymbolTableView):
    _input_token_type = _TokenType.SYMBOL
    _isymbols = (<_SymbolTableView> input_token_type)._raw_ptr_or_raise()
  else:
    _input_token_type = _get_token_type(tostring(input_token_type))
  cdef _TokenType _output_token_type
  cdef const_SymbolTable_ptr _osymbols = NULL
  if output_token_type is None:
    _output_token_type = GetDefaultTokenType()
    _osymbols = GetDefaultSymbols()
  elif isinstance(output_token_type, _pywrapfst.SymbolTableView):
    _output_token_type = _TokenType.SYMBOL
    _osymbols = (<_SymbolTableView> output_token_type)._raw_ptr_or_raise()
  else:
    _output_token_type = _get_token_type(tostring(output_token_type))
  cdef Fst result = Fst(arc_type=arc_type)
  if not StringFileCompile(path_tostring(filename),
                           result._mfst.get(),
                           _input_token_type,
                           _output_token_type,
                           _isymbols,
                           _osymbols):
    raise FstIOError("Read failed")
  return result


cpdef Fst string_map(lines,
                     arc_type="standard",
                     input_token_type=None,
                     output_token_type=None):
  """
  string_map(lines, arc_type="standard",
             input_token_type=None, output_token_type=None)

  Creates an acceptor or cross-product transducer that maps between
  elements of mappings read from an iterable.

  Args:
    lines: An iterable of iterables of size one, two, or three, or an iterable
        of strings. The first element in each indexable (or each string, if the
        input is an iterable of strings) is interpreted as the input string,
        the second (optional) as the output string, defaulting to the input
        string, and the third (optional) as a string to be parsed as a weight,
        defaulting to semiring One.
    arc_type: A string indicating the arc type.
    input_token_type: An optional string indicating how the input strings are to
        be encoded as arc labels---one of: "utf8" (encodes strings as a UTF-8
        encoded Unicode strings), "byte" (encodes strings as raw bytes)---or a
        SymbolTable. If not set, or set to None, the value is set to the
        default token_type, which begins as "byte", but can be overridden for
        regions of code using the default_token_type context manager.
    output_token_type: An optional string indicating how the output strings are
        to be encoded as arc labels---one of: "utf8" (encodes strings as a UTF-8
        encoded Unicode strings), "byte" (encodes strings as raw bytes)---or a
        SymbolTable. If not set, or set to None, the value is set to the
        default token_type, which begins as "byte", but can be overridden for
        regions of code using the default_token_type context manager.

  Returns:
    An FST.

  Raises:
    FstArgError: String map compilation failed.
  """
  cdef _TokenType _input_token_type
  cdef const_SymbolTable_ptr _isymbols = NULL
  if input_token_type is None:
    _input_token_type = GetDefaultTokenType()
    _isymbols = GetDefaultSymbols()
  elif isinstance(input_token_type, _pywrapfst.SymbolTableView):
    _input_token_type = _TokenType.SYMBOL
    _isymbols = (<_SymbolTableView> input_token_type)._raw_ptr_or_raise()
  else:
    _input_token_type = _get_token_type(tostring(input_token_type))
  cdef _TokenType _output_token_type
  cdef const_SymbolTable_ptr _osymbols = NULL
  if output_token_type is None:
    _output_token_type = GetDefaultTokenType()
    _osymbols = GetDefaultSymbols()
  elif isinstance(output_token_type, _pywrapfst.SymbolTableView):
    _output_token_type = _TokenType.SYMBOL
    _osymbols = (<_SymbolTableView> output_token_type)._raw_ptr_or_raise()
  else:
    _output_token_type = _get_token_type(tostring(output_token_type))
  cdef vector[vector[string]] _lines
  for line in lines:
    if isinstance(line, str):
      _lines.push_back([tostring(line)])
    else:
      _lines.push_back([tostring(elem) for elem in line])
  cdef Fst result = Fst(arc_type)
  if not StringMapCompile(_lines,
                          result._mfst.get(),
                          _input_token_type,
                          _output_token_type,
                          _isymbols,
                          _osymbols):
    raise FstArgError("String map compilation failed")
  return result



cdef class _PointerSymbolTableView(_SymbolTableView):

  """
  (No constructor.)

  Immutable SymbolTable class for unowned tables.

  This class wraps a library const SymbolTable pointer, and is used to wrap
  the generated symbols table singleton.
  """

  cdef const_SymbolTable_ptr _symbols

  # NB: Do not expose any non-const methods of the wrapped SymbolTable here.
  # Doing so will allow undefined behavior.

  def __repr__(self):
    return f"<const pointer SymbolTableView {self.name()!r} at 0x{id(self):x}>"

  cdef const_SymbolTable_ptr _raw(self):
    return self._symbols


cpdef _PointerSymbolTableView generated_symbols():
  """Returns a view of a symbol table containing generated symbols."""
  cdef _PointerSymbolTableView _symbols = (
      _PointerSymbolTableView.__new__(_PointerSymbolTableView))
  _symbols._symbols = addr(GeneratedSymbols())
  return _symbols


# Decorator for one-argument constructive FST operations.


def _1arg_patch(fnc):
  @functools.wraps(fnc)
  def patch(fst, *args, **kwargs):
    cdef Fst _fst = _compile_or_copy_Fst(fst)
    return _init_Fst_from_MutableFst(fnc(_fst, *args, **kwargs))
  return patch


arcmap = _1arg_patch(_pywrapfst.arcmap)
determinize = _1arg_patch(_pywrapfst.determinize)
disambiguate = _1arg_patch(_pywrapfst.disambiguate)
epsnormalize = _1arg_patch(_pywrapfst.epsnormalize)
prune = _1arg_patch(_pywrapfst.prune)
push = _1arg_patch(_pywrapfst.push)
randgen = _1arg_patch(_pywrapfst.randgen)
reverse = _1arg_patch(_pywrapfst.reverse)
shortestpath = _1arg_patch(_pywrapfst.shortestpath)
statemap = _1arg_patch(_pywrapfst.statemap)
synchronize = _1arg_patch(_pywrapfst.synchronize)


def _shortestdistance_patch(fnc):
  @functools.wraps(fnc)
  def patch(fst, *args, **kwargs):
    cdef Fst _fst = _compile_or_copy_Fst(fst)
    return fnc(_fst, *args, **kwargs)
  return patch


shortestdistance = _shortestdistance_patch(_pywrapfst.shortestdistance)


# Two-argument constructive FST operations. If just one of the two FST
# arguments has been compiled, the arc type of the compiled argument is used to
# determine the arc type of the not-yet-compiled argument.


def _compose_patch(fnc):
  @functools.wraps(fnc)
  def patch(fst1, fst2, *args, **kwargs):
    cdef Fst _fst1
    cdef Fst _fst2
    (_fst1, _fst2) = _compile_or_copy_two_Fsts(fst1, fst2)
    _maybe_arcsort(_fst1._mfst.get(), _fst2._mfst.get())
    return _init_Fst_from_MutableFst(fnc(_fst1, _fst2, *args, **kwargs))
  return patch


compose = _compose_patch(_pywrapfst.compose)
intersect = _compose_patch(_pywrapfst.intersect)


def _difference_patch(fnc):
  @functools.wraps(fnc)
  def patch(fst1, fst2, *args, **kwargs):
    cdef Fst _fst1
    cdef Fst _fst2
    (_fst1, _fst2) = _compile_or_copy_two_Fsts(fst1, fst2)
    # Makes RHS epsilon-free and deterministic.
    OptimizeDifferenceRhs(_fst2._mfst.get(), True)
    return _init_Fst_from_MutableFst(fnc(_fst1, _fst2, *args, **kwargs))
  return patch


difference = _difference_patch(_pywrapfst.difference)


# Simple comparison operations.


def _comp_patch(fnc):
  @functools.wraps(fnc)
  def patch(fst1, fst2, *args, **kwargs):
    cdef Fst _fst1
    cdef Fst _fst2
    (_fst1, _fst2) = _compile_or_copy_two_Fsts(fst1, fst2)
    return fnc(_fst1, _fst2, *args, **kwargs)
  return patch


equal = _comp_patch(_pywrapfst.equal)
equivalent = _comp_patch(_pywrapfst.equivalent)
isomorphic = _comp_patch(_pywrapfst.isomorphic)
randequivalent = _comp_patch(_pywrapfst.randequivalent)


cpdef Fst concat(fst1, fst2):
  """
  concat(fst1, fst2)

  Computes the concatenation (product) of two FSTs.

  This operation destructively concatenates the FST with other FSTs. If A
  transduces string x to y with weight a and B transduces string w to v with
  weight b, then their concatenation transduces string xw to yv with weight
  a \otimes b.

  Args:
    fst1: The first FST.
    fst2: The second FST.

  Returns:
    An FST.
  """
  cdef Fst _fst1
  cdef Fst _fst2
  (_fst1, _fst2) = _compile_or_copy_two_Fsts(fst1, fst2)
  return _fst1.concat(_fst2)


cpdef Fst replace(pairs,
                  call_arc_labeling="input",
                  return_arc_labeling="neither",
                  bool epsilon_on_replace=False,
                  int64_t return_label=0):
  """
  replace(pairs, call_arc_labeling="input", return_arc_labeling="neither",
          epsilon_on_replace=False, return_label=0)

  Recursively replaces arcs in the FST with other FST(s).

  This operation performs the dynamic replacement of arcs in one FST with
  another FST, allowing the definition of FSTs analogous to RTNs. It takes as
  input a set of pairs of a set of pairs formed by a non-terminal label and
  its corresponding FST, and a label identifying the root FST in that set.
  The resulting FST is obtained by taking the root FST and recursively replacing
  each arc having a nonterminal as output label by its corresponding FST. More
  precisely, an arc from state s to state d with (nonterminal) output label n in
  this FST is replaced by redirecting this "call" arc to the initial state of a
  copy F of the FST for n, and adding "return" arcs from each final state of F
  to d. Optional arguments control how the call and return arcs are labeled; by
  default, the only non-epsilon label is placed on the call arc.

  Args:
    pairs: An iterable of (nonterminal label, FST) pairs, where the former is an
        unsigned integer and the latter is an Fst instance.  
    call_arc_labeling: A string indicating which call arc labels should be
        non-epsilon. One of: "input" (default), "output", "both", "neither".
        This value is set to "neither" if epsilon_on_replace is True.
    return_arc_labeling: A string indicating which return arc labels should be
        non-epsilon. One of: "input", "output", "both", "neither" (default).
        This value is set to "neither" if epsilon_on_replace is True.
    epsilon_on_replace: Should call and return arcs be epsilon arcs? If True,
        this effectively overrides call_arc_labeling and return_arc_labeling,
        setting both to "neither".
    return_label: The integer label for return arcs.

  Returns:
    An FST.
  """
  # Keeps these in memory so they're not garbage-collected.
  pairs = [(label, _compile_or_copy_Fst(fst)) for (label, fst) in pairs]
  return Fst.from_pywrapfst(_replace(pairs,
                                     call_arc_labeling,
                                     return_arc_labeling,
                                     epsilon_on_replace,
                                     return_label))


# Can't be typed because of the star-args.


def union(*fsts):
  """
  union(*fsts)

  Computes the union (sum) of two or more FSTs.

  This operation computes the union (sum) of two FSTs. If A transduces string
  x to y with weight a and B transduces string w to v with weight b, then their
  union transduces x to y with weight a and w to v with weight b.

  Args:
   *fsts: Two or more input FSTs.

  Returns:
    An FST.
  """
  (fst1, *fsts2) = fsts
  cdef Fst _fst1 = _compile_or_copy_Fst(fst1)
  return _fst1.union(*fsts2)


# Pushdown transducer classes and operations.


cdef class PdtParentheses:

  """
  PdtParentheses()

  Pushdown transducer parentheses class.

  This class wraps a vector of pairs of arc labels in which the first label is
  interpreted as a "push" stack operation and the second represents the
  corresponding "pop" operation. When efficiency is desired, the push and pop
  indices should be contiguous.

  A PDT is expressed as an (Fst, PdtParentheses) pair for the purposes of all
  supported PDT operations.
  """

  cdef vector[pair[int64_t, int64_t]] _parens

  def __repr__(self):
    return f"<{self.__class__.__name__} at 0x{id(self):x}>"

  def __len__(self):
    return self._parens.size()

  def __iter__(self):
    cdef size_t _i = 0
    for _i in range(self._parens.size()):
      yield (self._parens[_i].first, self._parens[_i].second)

  cpdef PdtParentheses copy(self):
    """
    copy(self)

    Makes a copy of this PdtParentheses object.

    Returns:
      A deep copy of the PdtParentheses object.
    """
    cdef PdtParentheses result = PdtParentheses.__new__(PdtParentheses)
    result._parens = self._parens
    return result

  cpdef void add_pair(self, int64_t push, int64_t pop):
    """
    add_pair(self, push, pop)

    Adds a pair of parentheses to the set.

    Args:
      push: An arc label to be interpreted as a "push" operation.
      pop: An arc label to be interpreted as a "pop" operation.
    """
    self._parens.push_back(pair[int64_t, int64_t](push, pop))

  @classmethod
  def read(cls, filename):
    """
    PdtParentheses.read(filename)

    Reads parentheses pairs from a text file.

    This class method creates a new PdtParentheses object from a pairs of
    integer labels in a text file.

    Args:
      filename: The string location of the input file.

    Returns:
      A new PdtParentheses instance.

    Raises:
      FstIOError: Read failed.
    """
    cdef PdtParentheses result = PdtParentheses.__new__(PdtParentheses)
    if not ReadLabelPairs[int64_t](path_tostring(filename),
                                 addr(result._parens),
                                 False):
      raise FstIOError(f"Read failed: {filename}")
    return result

  cpdef void write(self, filename) except *:
    """
    write(self, filename)

    Writes parentheses pairs to text file.

    This method writes the PdtParentheses object to a text file.

    Args:
      filename: The string location of the output file.

    Raises:
      FstIOError: Write failed.
    """
    if not WriteLabelPairs[int64_t](path_tostring(filename), self._parens):
      raise FstIOError(f"Write failed: {filename}")


def pdt_compose(fst1,
                fst2,
                PdtParentheses parens,
                compose_filter="paren",
                bool left_pdt=True):
  """
  pdt_compose(fst1, fst2, parens, compose_filter="paren", left_pdt=True)

  Composes a PDT with an FST.

  This operation composes a PDT with an FST. The input PDT is defined by the
  combination of an FST and a PdtParentheses object specifying the stack
  symbols. The caller should also specify whether the left-hand or the
  right-hand FST argument is to be interpreted as a PDT.

  Args:
    fst1: The left-hand-side input FST or PDT.
    fst2: The right-hand-side input FST or PDT.
    parens: A PdtParentheses object specifying the input PDT's stack symbols.
    compose_filter: A string indicating the desired PDT composition filter; one
        of: "paren" (keeps parentheses), "expand" (expands and removes
        parentheses), "expand_paren" (expands and keeps parentheses).
    left_pdt: If true, the first argument is interpreted as a PDT and the
        second argument is interpreted as an FST; if false, the second
        argument is interpreted as a PDT and the first argument is interpreted
        as an FST.

  Returns:
    The FST component of an PDT produced by composition.

  Raises:
    FstOpError: Operation failed.
  """
  cdef Fst _fst1
  cdef Fst _fst2
  (_fst1, _fst2) = _compile_or_copy_two_Fsts(fst1, fst2)
  _maybe_arcsort(_fst1._mfst.get(), _fst2._mfst.get())
  cdef Fst result = Fst(_fst1.arc_type())
  cdef PdtComposeFilter _compose_filter = _get_pdt_compose_filter(
      tostring(compose_filter))
  cdef unique_ptr[PdtComposeOptions] _opts
  _opts.reset(new PdtComposeOptions(True, _compose_filter))
  Compose(deref(_fst1._fst),
          deref(_fst2._fst),
          parens._parens,
          result._mfst.get(),
          deref(_opts),
          left_pdt)
  return result


def pdt_expand(fst,
               PdtParentheses parens,
               bool connect=True,
               bool keep_parentheses=False,
               weight=None):
  """
  pdt_expand(fst, parens, connect=True, keep_parentheses=False, weight=None)

  Expands a bounded-stack PDT to an FST.

  This operation converts a bounded-stack PDT into the equivalent FST. The
  input PDT is defined by the combination of an FST and a PdtParentheses object
  specifying the PDT stack symbols.

  If the input PDT does not have a bounded stack, then it is impossible to
  expand the PDT into an FST and this operation will not terminate.

  Args:
    fst: The FST component of the input PDT.
    parens: A PdtParentheses object specifying the input PDT's stack symbols.
    connect: Should the output FST be trimmed?
    keep_parentheses: Should the output FST preserve parentheses arcs?
    weight: A Weight or weight string indicating the desired weight threshold;
        paths with weights below this threshold will be pruned. If omitted or
        null, no paths are pruned.

  Returns:
    An FST produced by expanding the bounded-stack PDT.

  Raises:
    FstOpError: Operation failed.
  """
  cdef Fst _fst = _compile_or_copy_Fst(fst)
  cdef Fst result = Fst(_fst.arc_type())
  cdef WeightClass _weight = _get_WeightClass_or_zero(result.weight_type(),
                                                      weight)
  cdef unique_ptr[PdtExpandOptions] _opts
  _opts.reset(new PdtExpandOptions(connect, keep_parentheses, _weight))
  Expand(deref(_fst._fst), parens._parens, result._mfst.get(), deref(_opts))
  result._check_mutating_imethod()
  return result


# Helper for the top-level pdt_replace, with an interface like _pywrapfst.


cdef object _pdt_replace(pairs,
                         pdt_parser_type="left",
                         int64_t start_paren_labels=kNoLabel,
                         left_paren_prefix="(_",
                         right_paren_prefix=")_"):
  cdef vector[LabelFstClassPair] _pairs
  cdef int64_t _label
  cdef _Fst _fst
  for (_label, _fst) in pairs:
    _pairs.push_back(LabelFstClassPair(_label, _fst._fst.get()))
  cdef Fst result_fst = Fst(_pairs[0].second.ArcType())
  cdef PdtParentheses result_parens = PdtParentheses()
  Replace(_pairs,
          result_fst._mfst.get(),
          addr(result_parens._parens),
          _pairs[0].first,
          _get_pdt_parser_type(tostring(pdt_parser_type)),
          start_paren_labels,
          tostring(left_paren_prefix),
          tostring(right_paren_prefix))
  result_fst._check_mutating_imethod()
  return (result_fst, result_parens)


def pdt_replace(pairs,
                pdt_parser_type="left",
                int64_t start_paren_labels=kNoLabel,
                left_paren_prefix="(_",
                right_paren_prefix=")_"):
  """
  pdt_replace(pairs, pdt_parser_type="left",
              int64_t start_paren_labels=NO_LABEL,
              left_paren_prefix="(_",
              right_paren_prefix=")_")

  Constructively replaces arcs in an FST with other FST(s), producing a PDT.

  This operation performs the dynamic replacement of arcs in one FST with
  another FST, allowing the definition of a PDT analogues to RTNs. The output
  PDT, defined by the combination of an FST and a PdtParentheses object
  specifying the PDT stack symbols, is the result of recursively replacing each
  arc in an input FST that matches some "non-terminal" with a corresponding
  FST, inserting parentheses where necessary. More precisely, an arc from
  state s to state d with nonterminal output label n in an input FST is
  replaced by redirecting this "call" arc to the initial state of a copy of the
  replacement FST and then adding "return" arcs from each final state of the
  replacement FST to d in the input FST. Unlike `replace`, this operation is
  capable of handling cyclic dependencies among replacement rules, which is
  accomplished by adding "push" stack symbols to "call" arcs and "pop" stack
  symbols to "return" arcs.

  Args:
    pairs: An iterable of (nonterminal label, FST) pairs, where the former is an
        unsigned integer and the latter is an Fst instance.
    pdt_parser_type: A string matching a known PdtParserType. One of: "left"
        (default), "left_sr".
    start_paren_labels: Index to use for the first inserted parentheses.
    left_paren_prefix: Prefix to attach to SymbolTable labels for inserted left
        parentheses.
    right_paren_prefix: Prefix to attach to SymbolTable labels for inserted
        right parentheses.

  Returns:
   An (Fst, PdtParentheses) pair.

  Raises:
    FstOpError: Operation failed.
  """
  # Keeps these in memory so they're not garbage-collected.
  pairs = [(label, _compile_or_copy_Fst(fst)) for (label, fst) in pairs]
  return _pdt_replace(pairs,
                      pdt_parser_type,
                      start_paren_labels,
                      left_paren_prefix,
                      right_paren_prefix)


cpdef Fst pdt_reverse(fst, PdtParentheses parens):
  """
  pdt_reverse(fst, parens)

  Reverses a PDT.

  This operation reverses an PDT. The input PDT is defined by the combination
  of an FST and a PdtParentheses object specifying the PDT stack symbols.

  Args:
    fst: The FST component of the input PDT.
    parens: A PdtParentheses object specifying the input PDT's stack symbols.

  Returns:
    An FST.
  """
  cdef Fst _fst = _compile_or_copy_Fst(fst)
  cdef Fst result = Fst(_fst.arc_type())
  Reverse(deref(_fst._fst), parens._parens, result._mfst.get())
  result._check_mutating_imethod()
  return result


cpdef pdt_shortestpath(fst,
                       PdtParentheses parens,
                       queue_type="fifo",
                       bool keep_parentheses=False,
                       bool path_gc=True):
  """
  pdt_shortestpath(fst, parens, queue_type="fifo", keep_parentheses=False,
                   path_gc=True)

  Computes the shortest path through a bounded-stack PDT.

  This operation computes the shortest path through a PDT. The input PDT is
  defined by the combination of an FST and a PdtParentheses object specifying
  the PDT stack symbols.

  Args:
    fst: The FST component of an input PDT.
    parens: A PdtParentheses object specifying the input PDT's stack symbols.
    queue_type: A string matching a known queue type; one of: "fifo" (default),
        "lifo", "state".
    keep_parentheses: Should the output FST preserve parentheses arcs?
    path_gc: Should shortest path data be garbage-collected?

  Returns:
    An FST.

  Raises:
    FstOpError: Operation failed.
  """
  cdef Fst _fst = _compile_or_copy_Fst(fst)
  cdef Fst result = Fst(_fst.arc_type())
  cdef unique_ptr[PdtShortestPathOptions] _opts
  _opts.reset(
      new PdtShortestPathOptions(_get_queue_type(tostring(queue_type)),
                                 keep_parentheses,
                                 path_gc))
  ShortestPath(deref(_fst._fst),
               parens._parens,
               result._mfst.get(),
               deref(_opts))
  result._check_mutating_imethod()
  return result


# Multi-pushdown transducer classes and operations.


cdef class MPdtParentheses:

  """
  MPdtParentheses()

  Multi-pushdown transducer parentheses class.

  This class wraps a vector of pairs of arc labels in which the first label is
  interpreted as a "push" stack operation and the second represents the
  corresponding "pop" operation, and an equally sized vector which assigns each
  pair to a stack. The library currently only permits two stacks (numbered 1
  and 2) to be used.

  A MPDT is expressed as an (Fst, MPdtParentheses) pair for the purposes of all
  supported MPDT operations.
  """

  cdef vector[pair[int64_t, int64_t]] _parens
  cdef vector[int64_t] _assign

  def __repr__(self):
    return f"<{self.__class__.__name__} at 0x{id(self):x}>"

  def __len__(self):
    return self._parens.size()

  def __iter__(self):
    cdef size_t _i = 0
    for _i in range(self._parens.size()):
      yield (self._parens[_i].first, self._parens[_i].second, self._assign[_i])

  cpdef MPdtParentheses copy(self):
    """
    copy(self)

    Makes a copy of this MPdtParentheses object.

    Returns:
      A deep copy of the MPdtParentheses object.
    """
    cdef MPdtParentheses result = MPdtParentheses.__new__(MPdtParentheses)
    result._parens = self._parens
    result._assign = self._assign
    return result

  cpdef void add_triple(self, int64_t push, int64_t pop, int64_t assignment):
    """
    add_triple(self, push, pop, assignment)

    Adds a triple of (left parenthesis, right parenthesis, stack assignment)
    triples to the object.

    Args:
      push: An arc label to be interpreted as a "push" operation.
      pop: An arc label to be interpreted as a "pop" operation.
      assignment: An arc label indicating what stack the parentheses pair is
          assigned to.
    """
    self._parens.push_back(pair[int64_t, int64_t](push, pop))
    self._assign.push_back(assignment)

  @classmethod
  def read(cls, filename):
    """
    MPdtParentheses.read(filename)

    Reads parentheses/assignment triples from a text file.

    This class method creates a new MPdtParentheses object from a pairs of
    integer labels in a text file.

    Args:
      filename: The string location of the input file.

    Returns:
      A new MPdtParentheses instance.

    Raises:
      FstIOError: Read failed.
    """
    cdef MPdtParentheses result = MPdtParentheses.__new__(MPdtParentheses)
    if not ReadLabelTriples[int64_t](path_tostring(filename),
                                   addr(result._parens),
                                   addr(result._assign),
                                   False):
      raise FstIOError(f"Read failed: {filename}")
    return result

  cpdef void write(self, filename) except *:
    """
    write(self, filename)

    Writes parentheses triples to a text file.

    This method writes the MPdtParentheses object to a text file.

    Args:
      filename: The string location of the output file.

    Raises:
      FstIOError: Write failed.
    """
    if not WriteLabelTriples[int64_t](path_tostring(filename), self._parens,
                                    self._assign):
      raise FstIOError(f"Write failed: {filename}")


cpdef Fst mpdt_compose(fst1, fst2, MPdtParentheses parens,
                       compose_filter="paren", bool left_mpdt=True):
  """
  mpdt_compose(fst1, fst2, parens, compose_filter="paren", left_mpdt=True)

  Composes a MPDT with an FST.

  This operation composes a MPDT with an FST. The input MPDT is defined by the
  combination of an FST and a MPdtParentheses object specifying the stack
  symbols and assignments. The caller should also specify whether the left-hand
  or the right-hand FST argument is to be interpreted as a MPDT.

  Args:
    fst1: The left-hand-side input FST or MPDT.
    fst2: The right-hand-side input FST or MPDT.
    parens: A MPdtParentheses object specifying the input MPDT's stack
        operations and assignments.
    compose_filter: A string indicating the desired MPDT composition filter; one
        of: "paren" (keeps parentheses), "expand" (expands and removes
        parentheses), "expand_paren" (expands and keeps parentheses).
    left_mpdt: If true, the first argument is interpreted as a MPDT and the
        second argument is interpreted as an FST; if false, the second
        argument is interpreted as a MPDT and the first argument is interpreted
        as an FST.

  Returns:
    An FST.

  Raises:
    FstOpError: Operation failed.
  """
  cdef Fst _fst1
  cdef Fst _fst2
  (_fst1, _fst2) = _compile_or_copy_two_Fsts(fst1, fst2)
  _maybe_arcsort(_fst1._mfst.get(), _fst2._mfst.get())
  cdef Fst result = Fst(_fst1.arc_type())
  cdef unique_ptr[MPdtComposeOptions] _opts
  _opts.reset(
      new MPdtComposeOptions(True,
                             _get_pdt_compose_filter(tostring(compose_filter))))
  Compose(deref(_fst1._fst),
          deref(_fst2._fst),
          parens._parens,
          parens._assign,
          result._mfst.get(),
          deref(_opts),
          left_mpdt)
  return result


cpdef Fst mpdt_expand(fst,
                      MPdtParentheses parens,
                      bool connect=True,
                      bool keep_parentheses=False):
  """
  mpdt_expand(fst, parens, connect=True, keep_parentheses=False):

  Expands a bounded-stack MPDT to an FST.

  This operation converts a bounded-stack MPDT into the equivalent FST. The
  input MPDT is defined by the combination of an FST and a MPdtParentheses
  object specifying the MPDT stack symbols and assignments.

  If the input MPDT does not have a bounded stack, then it is impossible to
  expand the MPDT into an FST and this operation will not terminate.

  Args:
    fst: The FST component of the input MPDT.
    parens: A MPdtParentheses object specifying the input PDT's stack
        symbols and assignments.
    connect: Should the output FST be trimmed?
    keep_parentheses: Should the output FST preserve parentheses arcs?

  Returns:
    An FST.

  Raises:
    FstOpError: Operation failed.
  """
  cdef Fst _fst = _compile_or_copy_Fst(fst)
  cdef Fst result = Fst(_fst.arc_type())
  cdef unique_ptr[MPdtExpandOptions] _opts
  _opts.reset(new MPdtExpandOptions(connect, keep_parentheses))
  Expand(deref(_fst._fst),
         parens._parens,
         parens._assign,
         result._mfst.get(),
         deref(_opts))
  result._check_mutating_imethod()
  return result


def mpdt_reverse(fst, MPdtParentheses parens):
  """
  mpdt_reverse(fst, parens)

  Reverses a MPDT.

  This operation reverses an MPDT. The input MPDT is defined by the combination
  of an FST and a MPdtParentheses object specifying the MPDT stack symbols
  and assignments. Unlike PDT reversal, which only modifies the FST component,
  this operation also reverses the stack assignments. assignments.

  Args:
    fst: The FST component of the input MPDT.
    parens: A MPdtParentheses object specifying the input MPDT's stack symbols
        and assignments.

  Returns:
    A (Fst, MPdtParentheses) pair.
  """
  cdef Fst _fst = _compile_or_copy_Fst(fst)
  cdef Fst result_fst = Fst(_fst.arc_type())
  cdef MPdtParentheses result_parens = parens.copy()
  Reverse(deref(_fst._fst),
          result_parens._parens,
          addr(result_parens._assign),
          result_fst._mfst.get())
  result_fst._check_mutating_imethod()
  return (result_fst, result_parens)


# Class for extracting paths from an acyclic FST.


cdef class _StringPathIterator:

  """
  _StringPathIterator(fst, input_token_type=None, output_token_type=None)

  Iterator for string paths in acyclic FST.

  This class provides an iterator over all paths (represented as pairs of
  strings and an associated path weight) through an acyclic FST. This
  operation is only feasible when the FST is acyclic. Depending on the
  requested token type, the arc labels along the input and output sides of a
  path are interpreted as UTF-8-encoded Unicode strings, raw bytes, or a
  concatenation of string labels from a symbol table. This class is normally
  created by invoking the `paths` method of `Fst`.

  Args:
    fst: input acyclic FST.
    input_token_type: An optional string indicating how the input strings are
        to be constructed from arc labels---one of: "byte" (interprets arc
        labels as raw bytes), "utf8" (interprets arc labels as Unicode code
        points), or a SymbolTable. If not set, or set to None, the value is set
        to the default token_type, which begins as "byte", but can be overridden
        for regions of code using the default_token_type context manager.
    output_token_type: An optional string indicating how the output strings are
        to be constructed from arc labels---one of: "byte" (interprets arc
        labels as raw bytes), "utf8" (interprets arc labels as Unicode code
        points), or a SymbolTable. If not set, or set to None, the value is set
        to the default token_type, which begins as "byte", but can be overridden
        for regions of code using the default_token_type context manager.

  Raises:
    FstArgError: Unknown token type.
    FstOpError: Operation failed.
  """

  cdef unique_ptr[StringPathIteratorClass] _paths

  def __repr__(self):
    return f"<{self.__class__.__name__} at 0x{id(self):x}>"

  def __init__(self, fst, input_token_type=None, output_token_type=None):
    cdef _TokenType _input_token_type
    cdef const_SymbolTable_ptr _isymbols = NULL
    if input_token_type is None:
      _input_token_type = GetDefaultTokenType()
      _isymbols = GetDefaultSymbols()
    elif isinstance(input_token_type, _pywrapfst.SymbolTableView):
      _input_token_type = _TokenType.SYMBOL
      _isymbols = (<_SymbolTableView> input_token_type)._raw_ptr_or_raise()
    else:
      _input_token_type = _get_token_type(tostring(input_token_type))
    cdef _TokenType _output_token_type
    cdef const_SymbolTable_ptr _osymbols = NULL
    if output_token_type is None:
      _output_token_type = GetDefaultTokenType()
      _osymbols = GetDefaultSymbols()
    elif isinstance(output_token_type, _pywrapfst.SymbolTableView):
      _output_token_type = _TokenType.SYMBOL
      _osymbols = (<_SymbolTableView> output_token_type)._raw_ptr_or_raise()
    else:
      _output_token_type = _get_token_type(tostring(output_token_type))
    cdef Fst _fst = _compile_or_copy_Fst(fst)
    self._paths.reset(
        new StringPathIteratorClass(deref(_fst._fst),
                                    _input_token_type,
                                    _output_token_type,
                                    _isymbols,
                                    _osymbols))
    if self._paths.get().Error():
      raise FstOpError("Operation failed")

  cpdef bool done(self):
    """
    done(self)

    Indicates whether the iterator is exhausted or not.

    Returns:
      True if the iterator is exhausted, False otherwise.
    """
    return self._paths.get().Done()

  cpdef bool error(self):
    """
    error(self)

    Indicates whether the _StringPathIterator has encountered an error.

    Returns:
      True if the _StringPathIterator is in an errorful state, False otherwise.
    """
    return self._paths.get().Error()

  def ilabels(self):
    """
    ilabels(self)

    Returns the input labels for the current path.

    Returns:
      A list of input labels for the current path.
    """
    return self._paths.get().ILabels()

  def olabels(self):
    """
    olabels(self)

    Returns the output labels for the current path.

    Returns:
      A list of output labels for the current path.
    """
    return self._paths.get().OLabels()

  cpdef string istring(self):
    """
    istring(self)

    Returns the current path's input string.

    Returns:
      The path's input string.
    """
    return self._paths.get().IString()

  def istrings(self):
    """
    istrings(self)

    Generates all input strings in the FST.

    This method returns a generator over all input strings in the path. The
    caller is responsible for resetting the iterator if desired.

    Yields:
      All input strings.
    """
    while not self._paths.get().Done():
      yield self.istring()
      self._paths.get().Next()

  def items(self):
     """
     items(self)

     Generates all (istring, ostring, weight) triples in the FST.

     This method returns a generator over all triples of input strings, 
     output strings, and path weights. The caller is responsible for resetting
     the iterator if desired.

     Yields:
        All (istring, ostring, weight) triples.
     """
     while not self._paths.get().Done():
       yield (self.istring(), self.ostring(), self.weight())
       self._paths.get().Next()

  cpdef void next(self):
    """
    next(self)

    Advances the iterator.
    """
    self._paths.get().Next()

  cpdef void reset(self):
    """
    reset(self)

    Resets the iterator to the initial position.
    """
    self._paths.get().Reset()

  cpdef string ostring(self):
    """
    ostring(self)

    Returns the current path's output string.

    Returns:
      The path's output string.
    """
    return self._paths.get().OString()

  def ostrings(self):
    """
    ostrings(self)

    Generates all output strings in the FST.

    This method returns a generator over all output strings in the path. The
    caller is responsible for resetting the iterator if desired.

    Yields:
      All output strings.
    """
    while not self._paths.get().Done():
      yield self.ostring()
      self._paths.get().Next()

  cpdef _Weight weight(self):
    """
    weight(self)

    Returns the current path's total weight.

    Returns:
      The path's Weight.
    """
    cdef _Weight weight = _Weight.__new__(_Weight)
    weight._weight.reset(new WeightClass(self._paths.get().Weight()))
    return weight

  def weights(self):
    """
    weights(self)

    Generates all path weights in the FST.

    This method returns a generator over all path weights. The caller is
    responsible for resetting the iterator if desired.

    Yields:
      All weights.
    """
    while not self._paths.get().Done():
      yield self.weight()
      self._paths.get().Next()


# Class for FAR reading and/or writing.


cdef class Far:

  """
  Far(filename, mode="r", arc_type="standard", far_type="default")

  Pynini FAR ("Fst ARchive") object.

  This class is used to either read FSTs from or write FSTs to a FAR. When
  opening a FAR for writing, the user may also specify the desired arc type
  and FAR type.

  Args:
    filename: A string indicating the filename.
    mode: FAR IO mode; one of: "r" (open for reading), "w" (open for writing).
    arc_type: Desired arc type; ignored if the FAR is opened for reading.
    far_type: Desired FAR type; ignored if the FAR is opened for reading.
  """

  cdef char _mode
  cdef string _name
  cdef FarReader _reader
  cdef FarWriter _writer

  # Instances holds either a FarReader or a FarWriter.

  def __init__(self,
               filename,
               mode="r",
               arc_type="standard",
               far_type="default"):
    self._name = path_tostring(filename)
    self._mode = tostring(mode)[0]
    if self._mode == b"r":
      self._reader = FarReader.open(self._name)
    elif self._mode == b"w":
      self._writer = FarWriter.create(self._name,
                                      arc_type=arc_type,
                                      far_type=far_type)
    else:
      raise FstArgError(f"Unknown mode: {mode}")

  def __repr__(self):
    return (f"<{self.far_type()} Far {self._name}, "
            f"mode '{self._mode:c}' at 0x{id(self):x}>")

  cdef void _check_open(self) except *:
    if self.closed():
      raise ValueError("I/O operation on closed FAR")

  cdef void _check_mode(self, char mode) except *:
    self._check_open()
    if not self._mode == mode:
      raise io.UnsupportedOperation(
          f"not {'readable' if mode == b'r' else 'writable'}")

  # API shared between FarReader and FarWriter.

  cpdef bool error(self) except *:
    """
    error(self)

    Indicates whether the FAR has encountered an error.

    Returns:
      True if the FAR is in an errorful state, False otherwise.
    """
    if self._mode == b"r":
      return self._reader.error()
    elif self._mode == b"w":
      return self._writer.error()
    else:
      return False

  cpdef string arc_type(self) except *:
    """
    arc_type(self)

    Returns a string indicating the arc type.

    Raises:
      ValueError: FAR is closed.
    """
    self._check_open()
    if self._mode == b"r":
      return self._reader.arc_type()
    elif self._mode == b"w":
      return self._writer.arc_type()

  cpdef bool closed(self):
    """
    closed(self)

    Indicates whether the FAR is closed for IO.
    """
    return self._mode == b"c"

  cpdef string far_type(self) except *:
    """far_type(self)

    Returns a string indicating the FAR type.

    Raises:
      ValueError: FAR is closed.
    """
    self._check_open()
    if self._mode == b"r":
      return self._reader.far_type()
    elif self._mode == b"w":
      return self._writer.far_type()

  cpdef string mode(self):
    """
    mode(self)

    Returns a char indicating the FAR's current mode.
    """
    return f"{self._mode:c}"

  cpdef string name(self):
    """
    name(self)

    Returns the FAR's filename.
    """
    return self._name

  # FarReader API.

  cpdef bool done(self) except *:
    """
    done(self)

    Indicates whether the iterator is exhausted or not.

    Returns:
      True if the iterator is exhausted, False otherwise.

    Raises:
      ValueError: FAR is closed.
      io.UnsupportedOperation: Cannot invoke method in current mode.
    """
    self._check_mode(b"r")
    return self._reader.done()

  cpdef bool find(self, key) except *:
    """
    find(self, key)

    Sets the current position to the first entry greater than or equal to the
    key (a string) and indicates whether or not a match was found.

    Args:
      key: A string key.

    Returns:
      True if the key was found, False otherwise.

    Raises:
      ValueError: FAR is closed.
      io.UnsupportedOperation: Cannot invoke method in current mode.
    """
    self._check_mode(b"r")
    return self._reader.find(key)

  cpdef Fst get_fst(self):
    """
    get_fst(self)

    Returns the FST at the current position. If the FST is not mutable,
    it is converted to a VectorFst.

    Returns:
      A copy of the FST at the current position.

    Raises:
      ValueError: FAR is closed.
      io.UnsupportedOperation: Cannot invoke method in current mode.
    """
    self._check_mode(b"r")
    return Fst.from_pywrapfst(self._reader.get_fst())

  cpdef string get_key(self) except *:
    """
    get_key(self)

    Returns the string key at the current position.

    Returns:
      The string key at the current position.

    Raises:
      ValueError: FAR is closed.
      io.UnsupportedOperation: Cannot invoke method in current mode.
    """
    self._check_mode(b"r")
    return self._reader.get_key()

  cpdef void next(self) except *:
    """
    next(self)

    Advances the iterator.

    Raises:
      ValueError: FAR is closed.
      io.UnsupportedOperation: Cannot invoke method in current mode.
    """
    self._check_mode(b"r")
    self._reader.next()

  cpdef void reset(self) except *:
    """
    reset(self)

    Resets the iterator to the initial position.

    Raises:
      ValueError: FAR is closed.
      io.UnsupportedOperation: Cannot invoke method in current mode.
    """
    self._check_mode(b"r")
    self._reader.reset()

  def __getitem__(self, key):
    if self.get_key() == tostring(key) or self._reader.find(key):
      return self.get_fst()
    else:
      raise KeyError(key)

  def __next__(self):
    self._check_mode(b"r")
    key, fst = self._reader.__next__()
    return (key, Fst.from_pywrapfst(fst))

  # This just registers this class as a possible iterator.
  def __iter__(self):
    return self

  # FarWriter API.

  cpdef void add(self, key, Fst fst):
    """
    add(self, key, fst)

    Adds an FST to the FAR (when open for writing).

    This methods adds an FST to the FAR which can be retrieved with the
    specified string key.

    Args:
      key: The string used to key the input FST.
      fst: The FST to write to the FAR.

    Raises:
      ValueError: FAR is closed.
      io.UnsupportedOperation: Cannot invoke method in current mode.
      FstOpError: Incompatible or invalid arc type.
    """
    self._check_mode(b"w")
    self._writer.add(key, fst)

  def __setitem__(self, key, Fst fst):
    self._check_mode(b"w")
    self._writer[key] = fst

  cpdef void close(self):
    """
    close(self)

    Closes the FAR and flushes to disk (when open for writing).
    """
    if not self.closed() and self._mode == b"w":
      self._writer.close()
    self._mode = b"c"

  # Adds support for use as a PEP-343 context manager.

  def __enter__(self):
    return self

  def __exit__(self, exc, value, tb):
    self.close()



# Classes from _pywrapfst.


from _pywrapfst import Arc
from _pywrapfst import EncodeMapper
from _pywrapfst import SymbolTable
from _pywrapfst import SymbolTableView
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



# The following wrapper converts destructive FST operations (defined as
# instance methods on the Fst class) to module-level functions which make a
# copy of the input FST and then apply the destructive operation.


def _copy_patch(fnc):
  @functools.wraps(fnc)
  def patch(arg1, *args, **kwargs):
    cdef Fst result = _compile_or_copy_Fst(arg1)
    fnc(result, *args, **kwargs)
    return result
  return patch


arcsort = _copy_patch(Fst.arcsort)
closure = _copy_patch(Fst.closure)
connect = _copy_patch(Fst.connect)
decode = _copy_patch(Fst.decode)
encode = _copy_patch(Fst.encode)
invert = _copy_patch(Fst.invert)
minimize = _copy_patch(Fst.minimize)
optimize = _copy_patch(Fst.optimize)
project = _copy_patch(Fst.project)
relabel_pairs = _copy_patch(Fst.relabel_pairs)
relabel_tables = _copy_patch(Fst.relabel_tables)
reweight = _copy_patch(Fst.reweight)
rmepsilon = _copy_patch(Fst.rmepsilon)
topsort = _copy_patch(Fst.topsort)


# Symbol table functions.


from _pywrapfst import compact_symbol_table
from _pywrapfst import merge_symbol_table


# Weight operations.


from _pywrapfst import divide
from _pywrapfst import power
from _pywrapfst import plus
from _pywrapfst import times


# Custom types.

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


# These definitions only ensure that these are defined to avoid attribute
# errors, but don't actually contain the type definitions.


import typing


CDRewriteDirection = """typing.Literal["ltr", "rtl", "sim"]"""
CDRewriteMode = """typing.Literal["obl", "opt"]"""
FarFileMode = """typing.Literal["r", "w"]"""

FstLike = """typing.Union[Fst, str]"""
TokenType = """typing.Union[SymbolTableView, typing.Literal["byte", "utf8"]]"""

