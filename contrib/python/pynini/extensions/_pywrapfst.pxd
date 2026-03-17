#cython: language_level=3
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

# See www.openfst.org for extensive documentation on this weighted
# finite-state transducer library.

from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint64_t

from libcpp cimport bool
from libcpp.memory cimport shared_ptr
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport pair
from libcpp.vector cimport vector

from cios cimport ostream
from cios cimport stringstream

cimport cpywrapfst as fst


# Exportable helper functions.


cdef string tostring(data) except *

cdef string weight_tostring(data) except *

cdef string path_tostring(data) except *

cdef fst.ComposeFilter _get_compose_filter(
    const string &compose_filter) except *

cdef fst.DeterminizeType _get_determinize_type(const string &det_type) except *

cdef fst.QueueType _get_queue_type(const string &queue_type) except *

cdef fst.RandArcSelection _get_rand_arc_selection(
    const string &replace_label_type) except *

cdef fst.ReplaceLabelType _get_replace_label_type(
    const string &replace_label_type,
    bool epsilon_on_replace) except *


# Weight.


cdef fst.WeightClass _get_WeightClass_or_one(const string &weight_type,
                                             weight_string) except *

cdef fst.WeightClass _get_WeightClass_or_zero(const string &weight_type,
                                              weight_string) except *


cdef class Weight:

  cdef unique_ptr[fst.WeightClass] _weight

  cdef void _check_weight(self) except *

  cpdef Weight copy(self)

  cpdef string to_string(self)

  cpdef string type(self)

  cpdef bool member(self)


cdef Weight _zero(weight_type)

cdef Weight _one(weight_type)

cdef Weight _no_weight(weight_type)

cdef Weight _plus(Weight lhs, Weight rhs)

cdef Weight _times(Weight lhs, Weight rhs)

cdef Weight _divide(Weight lhs, Weight rhs)

cdef Weight _power(Weight lhs, size_t n)


# SymbolTable.

ctypedef fst.SymbolTable * SymbolTable_ptr
ctypedef const fst.SymbolTable * const_SymbolTable_ptr


cdef class SymbolTableView:

  cdef const fst.SymbolTable *_raw(self)

  cdef void _raise_nonexistent(self) except *

  cdef const fst.SymbolTable *_raw_ptr_or_raise(self) except *

  cpdef int64_t available_key(self) except *

  cpdef bytes checksum(self)

  cpdef SymbolTable copy(self)

  cpdef int64_t get_nth_key(self, ssize_t pos) except *

  cpdef bytes labeled_checksum(self)

  cpdef bool member(self, key) except *

  cpdef string name(self) except *

  cpdef size_t num_symbols(self) except *

  cpdef void write(self, source) except *

  cpdef void write_text(self, source) except *

  cpdef bytes write_to_string(self)


cdef class _EncodeMapperSymbolTableView(SymbolTableView):

  # Indicates whether this view is of an input or output SymbolTable
  cdef bool _input_side

  cdef shared_ptr[fst.EncodeMapperClass] _mapper


cdef class _FstSymbolTableView(SymbolTableView):

  # Indicates whether this view is of an input or output SymbolTable
  cdef bool _input_side

  cdef shared_ptr[fst.FstClass] _fst


cdef class _MutableSymbolTable(SymbolTableView):

  cdef fst.SymbolTable *_mutable_raw(self)

  cdef fst.SymbolTable *_mutable_raw_ptr_or_raise(self) except *

  cpdef int64_t add_symbol(self, symbol, int64_t key=?) except *

  cpdef void add_table(self, SymbolTableView syms) except *

  cpdef void set_name(self, new_name) except *


cdef class _MutableFstSymbolTableView(_MutableSymbolTable):

  # Indicates whether this view is of an input or output SymbolTable
  cdef bool _input_side

  cdef shared_ptr[fst.MutableFstClass] _mfst


cdef class SymbolTable(_MutableSymbolTable):

  cdef unique_ptr[fst.SymbolTable] _smart_table


cdef _EncodeMapperSymbolTableView _init_EncodeMapperSymbolTableView(
    shared_ptr[fst.EncodeMapperClass] encoder, bool input_side)


cdef _FstSymbolTableView _init_FstSymbolTableView(shared_ptr[fst.FstClass] ifst,
                                                  bool input_side)


cdef _MutableFstSymbolTableView _init_MutableFstSymbolTableView(
    shared_ptr[fst.MutableFstClass] ifst, bool input_side)


cdef SymbolTable _init_SymbolTable(unique_ptr[fst.SymbolTable] table)


cpdef SymbolTable _read_SymbolTable_from_string(string state)


cdef class _SymbolTableIterator:

  cdef SymbolTableView _table
  cdef unique_ptr[fst.SymbolTableIterator] _siter


# EncodeMapper.


ctypedef fst.EncodeMapperClass * EncodeMapperClass_ptr


cdef class EncodeMapper:

  cdef shared_ptr[fst.EncodeMapperClass] _mapper

  cpdef string arc_type(self)

  cpdef string weight_type(self)

  cpdef uint8_t flags(self)

  cpdef void write(self, source) except *

  cpdef bytes write_to_string(self)

  cpdef _EncodeMapperSymbolTableView input_symbols(self)

  cpdef _EncodeMapperSymbolTableView output_symbols(self)

  cdef void _set_input_symbols(self, SymbolTableView syms) except *

  cdef void _set_output_symbols(self, SymbolTableView syms) except *


cdef EncodeMapper _init_EncodeMapper(EncodeMapperClass_ptr mapper)

cpdef EncodeMapper _read_EncodeMapper_from_string(string state)


# Fst.


ctypedef fst.FstClass * FstClass_ptr
ctypedef const fst.FstClass * const_FstClass_ptr
ctypedef fst.MutableFstClass * MutableFstClass_ptr
ctypedef fst.VectorFstClass * VectorFstClass_ptr


cdef class Fst:

  cdef shared_ptr[fst.FstClass] _fst

  @staticmethod
  cdef string _local_render_svg(const string &)

  cpdef string arc_type(self)

  cpdef _ArcIterator arcs(self, int64_t state)

  cpdef Fst copy(self)

  cpdef void draw(self,
                  source,
                  SymbolTableView isymbols=?,
                  SymbolTableView osymbols=?,
                  SymbolTableView ssymbols=?,
                  bool acceptor=?,
                  title=?,
                  double width=?,
                  double height=?,
                  bool portrait=?,
                  bool vertical=?,
                  double ranksep=?,
                  double nodesep=?,
                  int32_t fontsize=?,
                  int32_t precision=?,
                  float_format=?,
                  bool show_weight_one=?) except *

  cpdef Weight final(self, int64_t state)

  cpdef string fst_type(self)

  cpdef _FstSymbolTableView input_symbols(self)

  cpdef size_t num_arcs(self, int64_t state) except *

  cpdef size_t num_input_epsilons(self, int64_t state) except *

  cpdef size_t num_output_epsilons(self, int64_t state) except *

  cpdef _FstSymbolTableView output_symbols(self)

  cpdef string print(self,
                    SymbolTableView isymbols=?,
                    SymbolTableView osymbols=?,
                    SymbolTableView ssymbols=?,
                    bool acceptor=?,
                    bool show_weight_one=?,
                    missing_sym=?) except *

  cpdef int64_t start(self)

  cpdef _StateIterator states(self)

  cpdef bool verify(self)

  cpdef string weight_type(self)

  cpdef void write(self, source) except *

  cpdef bytes write_to_string(self)


cdef class MutableFst(Fst):

  cdef shared_ptr[fst.MutableFstClass] _mfst

  cdef void _check_mutating_imethod(self) except *

  cdef void _add_arc(self, int64_t state, Arc arc) except *

  cpdef int64_t add_state(self)

  cpdef void add_states(self, size_t)

  cdef void _arcsort(self, sort_type=?) except *

  cdef void _closure(self, closure_type=?)

  cdef void _concat(self, Fst fst2) except *

  cdef void _connect(self)

  cdef void _decode(self, EncodeMapper) except *

  cdef void _delete_arcs(self, int64_t state, size_t n=?) except *

  cdef void _delete_states(self, states=?) except *

  cdef void _encode(self, EncodeMapper) except *

  cdef void _invert(self)

  cdef void _minimize(self, float delta=?, bool allow_nondet=?) except *

  cpdef _MutableArcIterator mutable_arcs(self, int64_t state)

  cpdef int64_t num_states(self)

  cdef void _project(self, project_type) except *

  cdef void _prune(self, float delta=?, int64_t nstate=?, weight=?) except *

  cdef void _push(self,
                  float delta=?,
                  bool remove_total_weight=?,
                  reweight_type=?)

  cdef void _relabel_pairs(self, ipairs=?, opairs=?) except *

  cdef void _relabel_tables(self,
                            SymbolTableView old_isymbols=?,
                            SymbolTableView new_isymbols=?,
                            unknown_isymbol=?,
                            bool attach_new_isymbols=?,
                            SymbolTableView old_osymbols=?,
                            SymbolTableView new_osymbols=?,
                            unknown_osymbol=?,
                            bool attach_new_osymbols=?) except *

  cdef void _reserve_arcs(self, int64_t state, size_t n) except *

  cdef void _reserve_states(self, int64_t n)

  cdef void _reweight(self, potentials, reweight_type=?) except *

  cdef void _rmepsilon(self,
                       queue_type=?,
                       bool connect=?,
                       weight=?,
                       int64_t nstate=?,
                       float delta=?) except *

  cdef void _set_final(self, int64_t state, weight=?) except *

  cdef void _set_start(self, int64_t state) except *

  cdef void _set_input_symbols(self, SymbolTableView syms) except *

  cdef void _set_output_symbols(self, SymbolTableView syms) except *

  cdef void _topsort(self)


cdef class VectorFst(MutableFst):

    pass


# Construction helpers.


cdef Fst _init_Fst(FstClass_ptr tfst)

cdef MutableFst _init_MutableFst(MutableFstClass_ptr tfst)

cdef Fst _init_XFst(FstClass_ptr tfst)

cpdef Fst _read_Fst(source)

cpdef Fst _read_Fst_from_string(string state)


# Iterators.


cdef class Arc:

  cdef unique_ptr[fst.ArcClass] _arc

  cpdef Arc copy(self)


cdef Arc _init_Arc(const fst.ArcClass &arc)


cdef class _ArcIterator:

  cdef shared_ptr[fst.FstClass] _fst
  cdef unique_ptr[fst.ArcIteratorClass] _aiter

  cpdef bool done(self)

  cpdef uint8_t flags(self)

  cpdef void next(self)

  cpdef size_t position(self)

  cpdef void reset(self)

  cpdef void seek(self, size_t a)

  cpdef void set_flags(self, uint8_t flags, uint8_t mask)

  cdef Arc _value(self)


cdef class _MutableArcIterator:

  cdef shared_ptr[fst.MutableFstClass] _mfst
  cdef unique_ptr[fst.MutableArcIteratorClass] _aiter

  cpdef bool done(self)

  cpdef uint8_t flags(self)

  cpdef void next(self)

  cpdef size_t position(self)

  cpdef void reset(self)

  cpdef void seek(self, size_t a)

  cpdef void set_flags(self, uint8_t flags, uint8_t mask)

  cdef void _set_value(self, Arc arc)

  cdef Arc _value(self)


cdef class _StateIterator:

  cdef shared_ptr[fst.FstClass] _fst
  cdef unique_ptr[fst.StateIteratorClass] _siter

  cpdef bool done(self)

  cpdef void next(self)

  cpdef void reset(self)

  cdef int64_t _value(self)

  cpdef int64_t value(self) except *


# Constructive operations on Fst.


cdef Fst _map(Fst ifst, float delta=?, map_type=?, double power=?, weight=?)

cpdef Fst arcmap(Fst ifst, float delta=?, map_type=?, double power=?, weight=?)

cpdef MutableFst compose(Fst ifst1,
                         Fst ifst2,
                         compose_filter=?,
                         bool connect=?)

cpdef Fst convert(Fst ifst, fst_type=?)

cpdef MutableFst determinize(Fst ifst,
                             float delta=?,
                             det_type=?,
                             int64_t nstate=?,
                             int64_t subsequential_label=?,
                             weight=?,
                             bool increment_subsequential_label=?)

cpdef MutableFst difference(Fst ifst1,
                            Fst ifst2,
                            compose_filter=?,
                            bool connect=?)

cpdef MutableFst disambiguate(Fst ifst,
                              float delta=?,
                              int64_t nstate=?,
                              int64_t subsequential_label=?,
                              weight=?)

cpdef MutableFst epsnormalize(Fst ifst, eps_norm_type=?)

cpdef bool equal(Fst ifst1, Fst ifst2, float delta=?)

cpdef bool equivalent(Fst ifst1, Fst ifst2, float delta=?) except *

cpdef MutableFst intersect(Fst ifst1,
                           Fst ifst2,
                           compose_filter=?,
                           bool connect=?)

cpdef bool isomorphic(Fst ifst1, Fst ifst2, float delta=?)

cpdef MutableFst prune(Fst ifst,
                       float delta=?,
                       int64_t nstate=?,
                       weight=?)

cpdef MutableFst push(Fst ifst,
                      float delta=?,
                      bool push_weights=?,
                      bool push_labels=?,
                      bool remove_common_affix=?,
                      bool remove_total_weight=?,
                      reweight_type=?)

cpdef bool randequivalent(Fst ifst1,
                          Fst ifst2,
                          int32_t npath=?,
                          float delta=?,
                          select=?,
                          int32_t max_length=?,
                          uint64_t seed=?) except *

cpdef MutableFst randgen(Fst ifst,
                         int32_t npath=?,
                         select=?,
                         int32_t max_length=?,
                         bool remove_total_weight=?,
                         bool weighted=?,
                         uint64_t seed=?)

cpdef MutableFst replace(pairs,
                         call_arc_labeling=?,
                         return_arc_labeling=?,
                         bool epsilon_on_replace=?,
                         int64_t return_label=?)

cpdef MutableFst reverse(Fst ifst, bool require_superinitial=?)

cdef void _shortestdistance(Fst ifst,
                            vector[fst.WeightClass] *,
                            float delta=?,
                            int64_t nstate=?,
                            queue_type=?,
                            bool reverse=?) except *

cpdef MutableFst shortestpath(Fst ifst,
                              float delta=?,
                              int32_t nshortest=?,
                              int64_t nstate=?,
                              queue_type=?,
                              bool unique=?,
                              weight=?)

cpdef Fst statemap(Fst ifst, map_type)

cpdef MutableFst synchronize(Fst ifst)


# Compiler.


cdef class Compiler:

  cdef unique_ptr[stringstream] _sstrm
  cdef string _fst_type
  cdef string _arc_type
  cdef const fst.SymbolTable *_isymbols
  cdef const fst.SymbolTable *_osymbols
  cdef const fst.SymbolTable *_ssymbols
  cdef bool _acceptor
  cdef bool _keep_isymbols
  cdef bool _keep_osymbols
  cdef bool _keep_state_numbering
  cdef bool _allow_negative_labels

  cpdef Fst compile(self)

  cpdef void write(self, expression)


# FarReader.

cdef class FarReader:

  cdef unique_ptr[fst.FarReaderClass] _reader

  cpdef string arc_type(self)

  cpdef bool done(self)

  cpdef bool error(self)

  cpdef string far_type(self)

  cpdef bool find(self, key)

  cpdef Fst get_fst(self)

  cpdef string get_key(self)

  cpdef void next(self)

  cpdef void reset(self)


# FarWriter.

cdef class FarWriter:

  cdef unique_ptr[fst.FarWriterClass] _writer

  cpdef string arc_type(self)

  cdef void close(self)

  cpdef void add(self, key, Fst ifst) except *

  cpdef bool error(self)

  cpdef string far_type(self)

