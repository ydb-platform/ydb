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

from libc.stdint cimport int8_t
from libc.stdint cimport int16_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t

from libcpp cimport bool
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.utility cimport pair

from cios cimport *


cdef extern from "<fst/util.h>" nogil:

  bool FST_FLAGS_fst_error_fatal


cdef extern from "<fst/fstlib.h>" namespace "fst" nogil:

  # FST properties.
  const uint64_t kExpanded
  const uint64_t kMutable
  const uint64_t kError
  const uint64_t kAcceptor
  const uint64_t kNotAcceptor
  const uint64_t kIDeterministic
  const uint64_t kNonIDeterministic
  const uint64_t kODeterministic
  const uint64_t kNonODeterministic
  const uint64_t kEpsilons
  const uint64_t kNoEpsilons
  const uint64_t kIEpsilons
  const uint64_t kNoIEpsilons
  const uint64_t kOEpsilons
  const uint64_t kNoOEpsilons
  const uint64_t kILabelSorted
  const uint64_t kNotILabelSorted
  const uint64_t kOLabelSorted
  const uint64_t kNotOLabelSorted
  const uint64_t kWeighted
  const uint64_t kUnweighted
  const uint64_t kCyclic
  const uint64_t kAcyclic
  const uint64_t kInitialCyclic
  const uint64_t kInitialAcyclic
  const uint64_t kTopSorted
  const uint64_t kNotTopSorted
  const uint64_t kAccessible
  const uint64_t kNotAccessible
  const uint64_t kCoAccessible
  const uint64_t kNotCoAccessible
  const uint64_t kString
  const uint64_t kNotString
  const uint64_t kWeightedCycles
  const uint64_t kUnweightedCycles
  const uint64_t kNullProperties
  const uint64_t kCopyProperties
  const uint64_t kIntrinsicProperties
  const uint64_t kExtrinsicProperties
  const uint64_t kSetStartProperties
  const uint64_t kSetFinalProperties
  const uint64_t kAddStateProperties
  const uint64_t kAddArcProperties
  const uint64_t kSetArcProperties
  const uint64_t kDeleteStatesProperties
  const uint64_t kDeleteArcsProperties
  const uint64_t kStateSortProperties
  const uint64_t kArcSortProperties
  const uint64_t kILabelInvariantProperties
  const uint64_t kOLabelInvariantProperties
  const uint64_t kWeightInvariantProperties
  const uint64_t kAddSuperFinalProperties
  const uint64_t kRmSuperFinalProperties
  const uint64_t kBinaryProperties
  const uint64_t kTrinaryProperties
  const uint64_t kPosTrinaryProperties
  const uint64_t kNegTrinaryProperties
  const uint64_t kFstProperties

  # ArcIterator flags.
  const uint8_t kArcILabelValue
  const uint8_t kArcOLabelValue
  const uint8_t kArcWeightValue
  const uint8_t kArcNextStateValue
  const uint8_t kArcNoCache
  const uint8_t kArcValueFlags
  const uint8_t kArcFlags

  # EncodeMapper flags.
  const uint8_t kEncodeLabels
  const uint8_t kEncodeWeights
  const uint8_t kEncodeFlags

  # Default argument constants.
  const float kDelta
  const float kShortestDelta
  const int kNoLabel
  const int kNoStateId
  const int64_t kNoSymbol

  enum ClosureType:
    CLOSURE_STAR
    CLOSURE_PLUS

  enum ComposeFilter:
    AUTO_FILTER
    NULL_FILTER
    SEQUENCE_FILTER
    ALT_SEQUENCE_FILTER
    MATCH_FILTER
    TRIVIAL_FILTER

  cdef cppclass ComposeOptions:

    ComposeOptions(bool, ComposeFilter)

  enum DeterminizeType:
    DETERMINIZE_FUNCTIONAL
    DETERMINIZE_NONFUNCTIONAL
    DETERMINIZE_DISAMBIGUATE

  enum EncodeType:
    DECODE
    ENCODE

  enum EpsNormalizeType:
    EPS_NORM_INPUT
    EPS_NORM_OUTPUT

  # TODO(wolfsonkin): Don't do this hack if Cython gets proper enum class
  # support: https://github.com/cython/cython/issues/1603
  ctypedef enum ProjectType:
    PROJECT_INPUT "fst::ProjectType::INPUT"
    PROJECT_OUTPUT "fst::ProjectType::OUTPUT"

  enum QueueType:
    TRIVIAL_QUEUE
    FIFO_QUEUE
    LIFO_QUEUE
    SHORTEST_FIRST_QUEUE
    TOP_ORDER_QUEUE
    STATE_ORDER_QUEUE
    SCC_QUEUE
    AUTO_QUEUE
    OTHER_QUEUE

  # This is a templated struct at the C++ level, but Cython does not support
  # templated structs unless we pretend they are full-blown classes.
  cdef cppclass RandGenOptions[RandArcSelection]:

    RandGenOptions(const RandArcSelection &, int32_t, int32_t, bool, bool)

  enum ReplaceLabelType:
    REPLACE_LABEL_NEITHER
    REPLACE_LABEL_INPUT
    REPLACE_LABEL_OUTPUT
    REPLACE_LABEL_BOTH

  enum ReweightType:
    REWEIGHT_TO_INITIAL
    REWEIGHT_TO_FINAL

  cdef cppclass SymbolTableTextOptions:

    SymbolTableTextOptions(bool)

  # This is actually a nested class, but Cython doesn't need to know that.
  cdef cppclass SymbolTableIterator "fst::SymbolTable::iterator":

      SymbolTableIterator(const SymbolTableIterator &)

      cppclass value_type:

        int64_t Label()
        string Symbol()

      # When wrapped in a unique_ptr siter.Label() and siter.Symbol() are
      # ambiguous to Cython because there's no way to make the -> explicit.
      # This hacks around that.
      const value_type &Pair "operator*"()

      SymbolTableIterator &operator++()

      bool operator==(const SymbolTableIterator &, const SymbolTableIterator &)

      bool operator!=(const SymbolTableIterator &, const SymbolTableIterator &)


  # Symbol tables.
  cdef cppclass SymbolTable:

    @staticmethod
    int64_t kNoSymbol

    SymbolTable()

    SymbolTable(const string &)

    @staticmethod
    SymbolTable *Read(const string &)

    # Aliased for overload.
    @staticmethod
    SymbolTable *ReadStream "Read"(istream &, const string &)

    @staticmethod
    SymbolTable *ReadText(const string &, const SymbolTableTextOptions &)

    int64_t AddSymbol(const string &, int64_t)

    int64_t AddSymbol(const string &)

    SymbolTable *Copy()

    # Aliased for overload.
    string FindSymbol "Find"(int64_t)

    # Aliased for overload.
    int64_t FindIndex "Find"(const string &)

    # Aliased for overload.
    bool MemberSymbol "Member"(const string &)

    # Aliased for overload.
    bool MemberIndex "Member"(int64_t)

    void AddTable(const SymbolTable &)

    int64_t GetNthKey(ssize_t)

    const string &Name()

    void SetName(const string &)

    const string &CheckSum()

    const string &LabeledCheckSum()

    bool Write(ostream &)

    bool Write(const string &)

    bool WriteText(ostream &)

    bool WriteText(const string &)

    SymbolTableIterator begin()

    SymbolTableIterator end()

    int64_t AvailableKey()

    size_t NumSymbols()

  SymbolTable *CompactSymbolTable(const SymbolTable &syms)

  SymbolTable *MergeSymbolTable(const SymbolTable &, const SymbolTable &,
                                bool *)

  SymbolTable *FstReadSymbols(const string &, bool)

  # TODO(wolfsonkin): Don't do this hack if Cython gets proper enum class
  # support: https://github.com/cython/cython/issues/1603.
  ctypedef enum TokenType:
    SYMBOL "fst::TokenType::SYMBOL"
    BYTE "fst::TokenType::BYTE"
    UTF8 "fst::TokenType::UTF8"


cdef extern from "<fst/script/fstscript.h>" namespace "fst::script" nogil:

  cdef cppclass WeightClass:

    WeightClass()

    WeightClass(const WeightClass &)

    WeightClass(const string &, const string &)

    const string &Type()

    string ToString()

    bool Member()

    @staticmethod
    const WeightClass &Zero(const string &)

    @staticmethod
    const WeightClass &One(const string &)

    @staticmethod
    const WeightClass &NoWeight(const string &)

  # Alias.
  cdef bool Eq "operator=="(const WeightClass &, const WeightClass &)

  # Alias.
  cdef bool Ne "operator!="(const WeightClass &, const WeightClass &)

  cdef WeightClass Plus(const WeightClass &, const WeightClass &)

  cdef WeightClass Times(const WeightClass &, const WeightClass &)

  cdef WeightClass Divide(const WeightClass &, const WeightClass &)

  cdef WeightClass Power(const WeightClass &, size_t)

  cdef cppclass ArcClass:

    ArcClass(const ArcClass &)

    ArcClass(int64_t, int64_t, const WeightClass &, int64_t)

    int64_t ilabel
    int64_t olabel
    WeightClass weight
    int64_t nextstate

  cdef cppclass FstClass:

    FstClass(const FstClass &)

    @staticmethod
    unique_ptr[FstClass] Read(const string &)

    # Aliased for overload.
    @staticmethod
    unique_ptr[FstClass] ReadStream "Read"(istream &, const string &)

    int64_t Start()

    WeightClass Final(int64_t)

    size_t NumArcs(int64_t)

    size_t NumInputEpsilons(int64_t)

    size_t NumOutputEpsilons(int64_t)

    const string &ArcType()

    const string &FstType()

    const SymbolTable *InputSymbols()

    const SymbolTable *OutputSymbols()

    const string &WeightType()

    bool Write(const string &)

    bool Write(ostream &, const string &)

    uint64_t Properties(uint64_t, bool)

    bool ValidStateId(int64_t)

  cdef cppclass MutableFstClass(FstClass):

    bool AddArc(int64_t, const ArcClass &)

    int64_t AddState()

    void AddStates(size_t)

    bool DeleteArcs(int64_t, size_t)

    bool DeleteArcs(int64_t)

    bool DeleteStates(const vector[int64_t] &)

    void DeleteStates()

    SymbolTable *MutableInputSymbols()

    SymbolTable *MutableOutputSymbols()

    int64_t NumStates()

    bool ReserveArcs(int64_t, size_t)

    void ReserveStates(int64_t)

    bool SetStart(int64_t)

    bool SetFinal(int64_t, const WeightClass &)

    void SetInputSymbols(const SymbolTable *)

    void SetOutputSymbols(const SymbolTable *)

    void SetProperties(uint64_t, uint64_t)

  cdef cppclass VectorFstClass(MutableFstClass):

   VectorFstClass(const FstClass &)

   VectorFstClass(const string &)

  cdef cppclass EncodeMapperClass:

    EncodeMapperClass(const string &, uint32_t, EncodeType)

    # Aliased to __call__ as Cython doesn't have good support for operator().
    ArcClass __call__ "operator()"(const ArcClass &)

    const string &ArcType()

    const string &WeightType()

    uint32_t Flags()

    uint64_t Properties(uint64_t)

    @staticmethod
    unique_ptr[EncodeMapperClass] Read(const string &)

    # Aliased for overload.
    @staticmethod
    unique_ptr[EncodeMapperClass] ReadStream "Read"(istream &, const string &)

    bool Write(const string &)

    # Aliased for overload.
    bool WriteStream "Write"(ostream &, const string &)

    const SymbolTable *InputSymbols()

    const SymbolTable *OutputSymbols()

    void SetInputSymbols(const SymbolTable *)

    void SetOutputSymbols(const SymbolTable *)

  cdef cppclass ArcIteratorClass:

    ArcIteratorClass(const FstClass &, int64_t)

    bool Done()

    ArcClass Value()

    void Next()

    void Reset()

    void Seek(size_t)

    size_t Position()

    uint8_t Flags()

    void SetFlags(uint8_t, uint8_t)

  cdef cppclass MutableArcIteratorClass:

    MutableArcIteratorClass(MutableFstClass *, int64_t)

    bool Done()

    ArcClass Value()

    void Next()

    void Reset()

    void Seek(size_t)

    void SetValue(const ArcClass &)

    size_t Position()

    uint8_t Flags()

    void SetFlags(uint8_t, uint8_t)

  cdef cppclass StateIteratorClass:

    StateIteratorClass(const FstClass &)

    bool Done()

    int64_t Value()

    void Next()

    void Reset()


ctypedef pair[int64_t, const FstClass *] LabelFstClassPair

ctypedef pair[int64_t, int64_t] LabelPair


cdef extern from "<fst/script/fstscript.h>" namespace "fst::script" nogil:

  # TODO(wolfsonkin): Don't do this hack if Cython gets proper enum class
  # support: https://github.com/cython/cython/issues/1603
  ctypedef enum ArcFilterType:
    ANY_ARC_FILTER "fst::script::ArcFilterType::ANY"
    EPSILON_ARC_FILTER "fst::script::ArcFilterType::EPSILON"
    INPUT_EPSILON_ARC_FILTER "fst::script::ArcFilterType::INPUT_EPSILON"
    OUTPUT_EPSILON_ARC_FILTER "fst::script::ArcFilterType::OUTPUT_EPSILON"

  # TODO(wolfsonkin): Don't do this hack if Cython gets proper enum class
  # support: https://github.com/cython/cython/issues/1603
  ctypedef enum ArcSortType:
    ILABEL_SORT "fst::script::ArcSortType::ILABEL"
    OLABEL_SORT "fst::script::ArcSortType::OLABEL"

  cdef void ArcSort(MutableFstClass *, ArcSortType)

  cdef bool GetClosureType(const string &, ClosureType *)

  cdef void Closure(MutableFstClass *, ClosureType)

  cdef unique_ptr[FstClass] CompileInternal(istream &,
                                            const string &,
                                            const string &,
                                            const string &,
                                            const SymbolTable *,
                                            const SymbolTable *,
                                            const SymbolTable*,
                                            bool,
                                            bool,
                                            bool,
                                            bool,
                                            bool)

  cdef void Compose(FstClass &, FstClass &, MutableFstClass *,
                    const ComposeOptions &)

  cdef void Concat(MutableFstClass *, const FstClass &)

  cdef void Connect(MutableFstClass *)

  cdef unique_ptr[FstClass] Convert(const FstClass &, const string &)

  cdef void Decode(MutableFstClass *, const EncodeMapperClass &)

  cdef cppclass DeterminizeOptions:

    DeterminizeOptions(float,
                       const WeightClass &,
                       int64_t,
                       int64_t,
                       DeterminizeType,
                       bool)

  cdef void Determinize(const FstClass &,
                        MutableFstClass *,
                        const DeterminizeOptions &)

  cdef cppclass DisambiguateOptions:

    DisambiguateOptions(float, const WeightClass &, int64_t, int64_t)

  cdef void Disambiguate(const FstClass &,
                         MutableFstClass *,
                         const DisambiguateOptions &)

  cdef void Difference(const FstClass &,
                       const FstClass &,
                       MutableFstClass *,
                       const ComposeOptions &)

  cdef void Draw(const FstClass &fst,
                 const SymbolTable *,
                 const SymbolTable *,
                 const SymbolTable *,
                 bool,
                 const string &,
                 float,
                 float,
                 bool,
                 bool,
                 float,
                 float,
                 int,
                 int,
                 const string &,
                 bool,
                 ostream &,
                 const string &)

  cdef void Encode(MutableFstClass *, EncodeMapperClass *)

  cdef bool GetEpsNormalizeType(const string &, EpsNormalizeType *)

  cdef void EpsNormalize(const FstClass &, MutableFstClass *, EpsNormalizeType)

  cdef bool Equal(const FstClass &, const FstClass &, float)

  cdef bool Equivalent(const FstClass &, const FstClass &, float)

  cdef void Intersect(const FstClass &,
                      const FstClass &,
                      MutableFstClass *,
                      const ComposeOptions &)

  cdef void Invert(MutableFstClass *fst)

  cdef bool Isomorphic(const FstClass &, const FstClass &, float)

  # TODO(wolfsonkin): Don't do this hack if Cython gets proper enum class
  # support: https://github.com/cython/cython/issues/1603
  ctypedef enum MapType:
    ARC_SUM_MAPPER "fst::script::MapType::ARC_SUM"
    IDENTITY_MAPPER "fst::script::MapType::IDENTITY"
    INPUT_EPSILON_MAPPER "fst::script::MapType::INPUT_EPSILON"
    INVERT_MAPPER "fst::script::MapType::INVERT"
    OUTPUT_EPSILON_MAPPER "fst::script::MapType::OUTPUT_EPSILON"
    PLUS_MAPPER "fst::script::MapType::PLUS"
    QUANTIZE_MAPPER "fst::script::MapType::QUANTIZE"
    RMWEIGHT_MAPPER "fst::script::MapType::RMWEIGHT"
    SUPERFINAL_MAPPER "fst::script::MapType::SUPERFINAL"
    TIMES_MAPPER "fst::script::MapType::TIMES"
    TO_LOG_MAPPER "fst::script::MapType::TO_LOG"
    TO_LOG64_MAPPER "fst::script::MapType::TO_LOG64"
    TO_STD_MAPPER "fst::script::MapType::TO_STD"

  cdef unique_ptr[FstClass] Map(const FstClass &,
                                MapType,
                                float,
                                double,
                                const WeightClass &)

  cdef void Minimize(MutableFstClass *, MutableFstClass *, float, bool)

  cdef bool GetProjectType(const string &, ProjectType *)

  cdef void Project(MutableFstClass *, ProjectType)

  cdef void Print(const FstClass &,
                  ostream &,
                  const string &,
                  const SymbolTable *,
                  const SymbolTable *,
                  const SymbolTable *,
                  bool,
                  bool,
                  const string &)

  cdef void Prune(const FstClass &,
                  MutableFstClass *,
                  const WeightClass &,
                  int64_t, float)

  cdef void Prune(MutableFstClass *, const WeightClass &, int64_t, float)

  cdef void Push(const FstClass &,
                 MutableFstClass *,
                 uint8_t flags,
                 ReweightType, float)

  cdef void Push(MutableFstClass *, ReweightType, float, bool)

  # TODO(wolfsonkin): Don't do this hack if Cython gets proper enum class
  # support: https://github.com/cython/cython/issues/1603
  ctypedef enum RandArcSelection:
    UNIFORM_ARC_SELECTOR "fst::script::RandArcSelection::UNIFORM"
    LOG_PROB_ARC_SELECTOR "fst::script::RandArcSelection::LOG_PROB"
    FAST_LOG_PROB_ARC_SELECTOR "fst::script::RandArcSelection::FAST_LOG_PROB"

  cdef bool RandEquivalent(const FstClass &,
                           const FstClass &,
                           int32_t,
                           const RandGenOptions[RandArcSelection] &,
                           float,
                           uint64_t)

  cdef void RandGen(const FstClass &,
                    MutableFstClass *,
                    const RandGenOptions[RandArcSelection] &,
                    uint64_t)

  cdef void Relabel(MutableFstClass *,
                    const SymbolTable *,
                    const SymbolTable *,
                    const string &, bool,
                    const SymbolTable *,
                    const SymbolTable *,
                    const string &,
                    bool)

  cdef void Relabel(MutableFstClass *,
                    const vector[LabelPair] &,
                    const vector[LabelPair] &)

  cdef cppclass ReplaceOptions:

     ReplaceOptions(int64_t, ReplaceLabelType, ReplaceLabelType, int64_t)

  cdef void Replace(const vector[LabelFstClassPair] &,
                    MutableFstClass *,
                    const ReplaceOptions &)

  cdef void Reverse(const FstClass &, MutableFstClass *, bool)

  cdef void Reweight(MutableFstClass *,
                     const vector[WeightClass] &,
                     ReweightType)

  cdef cppclass RmEpsilonOptions:

    RmEpsilonOptions(QueueType, bool, const WeightClass &, int64_t, float)

  cdef void RmEpsilon(MutableFstClass *, const RmEpsilonOptions &)

  cdef cppclass ShortestDistanceOptions:

    ShortestDistanceOptions(QueueType, ArcFilterType, int64_t, float)

  cdef void ShortestDistance(const FstClass &,
                             vector[WeightClass] *,
                             const ShortestDistanceOptions &)

  cdef void ShortestDistance(const FstClass &,
                             vector[WeightClass] *, bool,
                             float)

  cdef cppclass ShortestPathOptions:

    ShortestPathOptions(QueueType,
                        int32_t,
                        bool,
                        float,
                        const WeightClass &,
                        int64_t)

  cdef void ShortestPath(const FstClass &,
                         MutableFstClass *,
                         const ShortestPathOptions &)

  cdef void Synchronize(const FstClass &, MutableFstClass *)

  cdef bool TopSort(MutableFstClass *)

  cdef void Union(MutableFstClass *, const vector[FstClass *] &)

  cdef bool Verify(const FstClass &)


cdef extern from "<fst/script/getters.h>" namespace "fst::script" nogil:

  cdef bool GetArcSortType(const string &, ArcSortType *)

  cdef bool GetComposeFilter(const string &, ComposeFilter *)

  cdef bool GetDeterminizeType(const string &, DeterminizeType *)

  cdef uint8_t GetEncodeFlags(bool, bool)

  cdef bool GetMapType(const string &, MapType *)

  cdef uint8_t GetPushFlags(bool, bool, bool, bool)

  cdef bool GetQueueType(const string &, QueueType *)

  cdef bool GetRandArcSelection(const string &, RandArcSelection *)

  cdef bool GetReplaceLabelType(string, bool, ReplaceLabelType *)

  cdef bool GetReweightType(const string &, ReweightType *)

  cdef bool GetTokenType(const string &, TokenType *)


cdef extern from "<fst/extensions/far/far.h>" namespace "fst" nogil:

  # TODO(wolfsonkin): Don't do this hack if Cython gets proper enum class
  # support: https://github.com/cython/cython/issues/1603
  ctypedef enum FarType:
    FAR_DEFAULT "fst::FarType::DEFAULT"
    FAR_STTABLE "fst::FarType::STTABLE"
    FAR_STLIST "fst::FarType::STLIST"
    FAR_FST "fst::FarType::FST"
    FAR_SSTABLE "fst::FarType::SSTABLE"

cdef extern from "<fst/extensions/far/getters.h>" \
    namespace "fst" nogil:

  string GetFarTypeString(FarType)


cdef extern from "<fst/extensions/far/getters.h>" \
    namespace "fst::script" nogil:

  bool GetFarType(const string &, FarType *)


cdef extern from "<fst/extensions/far/far-class.h>" \
    namespace "fst::script" nogil:

  cdef cppclass FarReaderClass:

    const string &ArcType()

    bool Done()

    bool Error()

    bool Find(const string &)

    const FstClass *GetFstClass()

    const string &GetKey()

    void Next()

    void Reset()

    FarType Type()

    # For simplicity, we always use the multiple-file one.
    @staticmethod
    unique_ptr[FarReaderClass] Open(const vector[string] &)

  cdef cppclass FarWriterClass:

    bool Add(const string &, const FstClass &)

    bool Error()

    const string &ArcType()

    FarType Type()

    @staticmethod
    unique_ptr[FarWriterClass] Create(const string &, const string &, FarType)

