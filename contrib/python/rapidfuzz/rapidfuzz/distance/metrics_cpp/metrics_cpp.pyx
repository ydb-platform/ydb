#cython: freethreading_compatible = True

from . import metrics_py
from ._initialize_cpp import Editops
import sys

from rapidfuzz cimport (
    RF_SCORER_FLAG_MULTI_STRING_CALL,
    RF_SCORER_FLAG_MULTI_STRING_INIT,
    RF_SCORER_FLAG_RESULT_F64,
    RF_SCORER_FLAG_RESULT_SIZE_T,
    RF_SCORER_FLAG_SYMMETRIC,
    RF_SCORER_NONE_IS_WORST_SCORE,
    RF_Kwargs,
    RF_Preprocess,
    RF_Scorer,
    RF_ScorerFlags,
    RF_ScorerFunc,
    RF_String,
    RF_UncachedScorerFunc
)

from ._initialize_cpp cimport Editops, RfEditops

pandas_NA = None

cdef inline void setupPandas() noexcept:
    global pandas_NA
    if pandas_NA is None:
        pandas = sys.modules.get('pandas')
        if hasattr(pandas, 'NA'):
            pandas_NA = pandas.NA

setupPandas()

# required for preprocess_strings
from array import array

from cpp_common cimport (
    CreateScorerContext,
    NoKwargsInit,
    RF_StringWrapper,
    SetFuncAttrs,
    SetScorerAttrs,
    preprocess_strings,
    get_score_cutoff_f64,
    get_score_cutoff_size_t,
    is_none
)
from libcpp.cmath cimport isnan
from libc.stdint cimport SIZE_MAX, int64_t
from libc.stdlib cimport free, malloc
from libcpp cimport bool
from cython.operator cimport dereference


cdef extern from "rapidfuzz/details/types.hpp" namespace "rapidfuzz" nogil:
    cdef struct LevenshteinWeightTable:
        size_t insert_cost
        size_t delete_cost
        size_t replace_cost

cdef extern from "metrics.hpp":
    # Levenshtein
    double levenshtein_normalized_distance_func(  const RF_String&, const RF_String&, size_t, size_t, size_t, double, double) except + nogil
    size_t levenshtein_distance_func(            const RF_String&, const RF_String&, size_t, size_t, size_t, size_t, size_t) except + nogil
    double levenshtein_normalized_similarity_func(const RF_String&, const RF_String&, size_t, size_t, size_t, double, double) except + nogil
    size_t levenshtein_similarity_func(          const RF_String&, const RF_String&, size_t, size_t, size_t, size_t, size_t) except + nogil

    RfEditops levenshtein_editops_func(const RF_String&, const RF_String&, size_t) except + nogil

    bool LevenshteinDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool LevenshteinNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool LevenshteinSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool LevenshteinNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedLevenshteinDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedLevenshteinNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedLevenshteinSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedLevenshteinNormalizedSimilarityFuncInit() nogil

    bool LevenshteinMultiStringSupport(const RF_Kwargs*) nogil

    # Damerau Levenshtein
    double damerau_levenshtein_normalized_distance_func(  const RF_String&, const RF_String&, double) except + nogil
    size_t damerau_levenshtein_distance_func(            const RF_String&, const RF_String&, size_t) except + nogil
    double damerau_levenshtein_normalized_similarity_func(const RF_String&, const RF_String&, double) except + nogil
    size_t damerau_levenshtein_similarity_func(          const RF_String&, const RF_String&, size_t) except + nogil

    bool DamerauLevenshteinDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool DamerauLevenshteinNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool DamerauLevenshteinSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool DamerauLevenshteinNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedDamerauLevenshteinDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedDamerauLevenshteinNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedDamerauLevenshteinSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedDamerauLevenshteinNormalizedSimilarityFuncInit() nogil

    # LCS
    double lcs_seq_normalized_distance_func(  const RF_String&, const RF_String&, double) except + nogil
    size_t lcs_seq_distance_func(            const RF_String&, const RF_String&, size_t) except + nogil
    double lcs_seq_normalized_similarity_func(const RF_String&, const RF_String&, double) except + nogil
    size_t lcs_seq_similarity_func(          const RF_String&, const RF_String&, size_t) except + nogil

    RfEditops lcs_seq_editops_func(const RF_String&, const RF_String&) except + nogil

    bool LCSseqDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool LCSseqNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool LCSseqSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool LCSseqNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedLCSseqDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedLCSseqNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedLCSseqSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedLCSseqNormalizedSimilarityFuncInit() nogil

    bool LCSseqMultiStringSupport(const RF_Kwargs*) nogil

    # Indel
    double indel_normalized_distance_func(  const RF_String&, const RF_String&, double) except + nogil
    size_t indel_distance_func(            const RF_String&, const RF_String&, size_t) except + nogil
    double indel_normalized_similarity_func(const RF_String&, const RF_String&, double) except + nogil
    size_t indel_similarity_func(          const RF_String&, const RF_String&, size_t) except + nogil

    RfEditops indel_editops_func(const RF_String&, const RF_String&) except + nogil

    bool IndelDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool IndelNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool IndelSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool IndelNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedIndelDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedIndelNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedIndelSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedIndelNormalizedSimilarityFuncInit() nogil

    bool IndelMultiStringSupport(const RF_Kwargs*) nogil

    # Hamming
    double hamming_normalized_distance_func(  const RF_String&, const RF_String&, bool, double) except + nogil
    size_t hamming_distance_func(            const RF_String&, const RF_String&, bool, size_t) except + nogil
    double hamming_normalized_similarity_func(const RF_String&, const RF_String&, bool, double) except + nogil
    size_t hamming_similarity_func(          const RF_String&, const RF_String&, bool, size_t) except + nogil

    bool HammingDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool HammingNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool HammingSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool HammingNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedHammingDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedHammingNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedHammingSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedHammingNormalizedSimilarityFuncInit() nogil

    RfEditops hamming_editops_func(const RF_String&, const RF_String&, bool) except + nogil

    # Optimal String Alignment
    double osa_normalized_distance_func(  const RF_String&, const RF_String&, double) except + nogil
    size_t osa_distance_func(            const RF_String&, const RF_String&, size_t) except + nogil
    double osa_normalized_similarity_func(const RF_String&, const RF_String&, double) except + nogil
    size_t osa_similarity_func(          const RF_String&, const RF_String&, size_t) except + nogil

    bool OSADistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool OSANormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool OSASimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool OSANormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedOSADistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedOSANormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedOSASimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedOSANormalizedSimilarityFuncInit() nogil

    bool OSAMultiStringSupport(const RF_Kwargs*) nogil

    # Jaro
    double jaro_normalized_distance_func(  const RF_String&, const RF_String&, double) except + nogil
    double jaro_distance_func(             const RF_String&, const RF_String&, double) except + nogil
    double jaro_normalized_similarity_func(const RF_String&, const RF_String&, double) except + nogil
    double jaro_similarity_func(           const RF_String&, const RF_String&, double) except + nogil

    bool JaroDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool JaroNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool JaroSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool JaroNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedJaroDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedJaroNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedJaroSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedJaroNormalizedSimilarityFuncInit() nogil

    bool JaroMultiStringSupport(const RF_Kwargs*) nogil

    # Jaro Winkler
    double jaro_winkler_normalized_distance_func(  const RF_String&, const RF_String&, double, double) except + nogil
    double jaro_winkler_distance_func(             const RF_String&, const RF_String&, double, double) except + nogil
    double jaro_winkler_normalized_similarity_func(const RF_String&, const RF_String&, double, double) except + nogil
    double jaro_winkler_similarity_func(           const RF_String&, const RF_String&, double, double) except + nogil

    bool JaroWinklerDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool JaroWinklerNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool JaroWinklerSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool JaroWinklerNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedJaroWinklerDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedJaroWinklerNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedJaroWinklerSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedJaroWinklerNormalizedSimilarityFuncInit() nogil

    bool JaroWinklerMultiStringSupport(const RF_Kwargs*) nogil

    # Prefix
    double prefix_normalized_distance_func(  const RF_String&, const RF_String&, double) except + nogil
    size_t prefix_distance_func(            const RF_String&, const RF_String&, size_t) except + nogil
    double prefix_normalized_similarity_func(const RF_String&, const RF_String&, double) except + nogil
    size_t prefix_similarity_func(          const RF_String&, const RF_String&, size_t) except + nogil

    bool PrefixDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool PrefixNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool PrefixSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool PrefixNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedPrefixDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedPrefixNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedPrefixSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedPrefixNormalizedSimilarityFuncInit() nogil

    # Postfix
    double postfix_normalized_distance_func(  const RF_String&, const RF_String&, double) except + nogil
    size_t postfix_distance_func(            const RF_String&, const RF_String&, size_t) except + nogil
    double postfix_normalized_similarity_func(const RF_String&, const RF_String&, double) except + nogil
    size_t postfix_similarity_func(          const RF_String&, const RF_String&, size_t) except + nogil

    bool PostfixDistanceInit(            RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool PostfixNormalizedDistanceInit(  RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool PostfixSimilarityInit(          RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    bool PostfixNormalizedSimilarityInit(RF_ScorerFunc*, const RF_Kwargs*, int64_t, const RF_String*) except False nogil
    RF_UncachedScorerFunc UncachedPostfixDistanceFuncInit(            ) nogil
    RF_UncachedScorerFunc UncachedPostfixNormalizedDistanceFuncInit(  ) nogil
    RF_UncachedScorerFunc UncachedPostfixSimilarityFuncInit(          ) nogil
    RF_UncachedScorerFunc UncachedPostfixNormalizedSimilarityFuncInit() nogil


def levenshtein_distance(s1, s2, *, weights=(1,1,1), processor=None, score_cutoff=None, score_hint=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef size_t insertion, deletion, substitution
    insertion = deletion = substitution = 1
    if weights is not None:
        insertion, deletion, substitution = weights

    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, SIZE_MAX, 0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return levenshtein_distance_func(s1_proc.string, s2_proc.string, insertion, deletion, substitution, c_score_cutoff, c_score_hint)


def levenshtein_similarity(s1, s2, *, weights=(1,1,1), processor=None, score_cutoff=None, score_hint=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef size_t insertion, deletion, substitution
    insertion = deletion = substitution = 1
    if weights is not None:
        insertion, deletion, substitution = weights

    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, 0, SIZE_MAX)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return levenshtein_similarity_func(s1_proc.string, s2_proc.string, insertion, deletion, substitution, c_score_cutoff, c_score_hint)


def levenshtein_normalized_distance(s1, s2, *, weights=(1,1,1), processor=None, score_cutoff=None, score_hint=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef size_t insertion, deletion, substitution
    insertion = deletion = substitution = 1
    if weights is not None:
        insertion, deletion, substitution = weights

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    cdef double c_score_hint = get_score_cutoff_f64(score_hint, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return levenshtein_normalized_distance_func(s1_proc.string, s2_proc.string, insertion, deletion, substitution, c_score_cutoff, c_score_hint)


def levenshtein_normalized_similarity(s1, s2, *, weights=(1,1,1), processor=None, score_cutoff=None, score_hint=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef size_t insertion, deletion, substitution
    insertion = deletion = substitution = 1
    if weights is not None:
        insertion, deletion, substitution = weights

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    cdef double c_score_hint = get_score_cutoff_f64(score_hint, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return levenshtein_normalized_similarity_func(s1_proc.string, s2_proc.string, insertion, deletion, substitution, c_score_cutoff, c_score_hint)


def levenshtein_editops(s1, s2, *, processor=None, score_hint=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)
    cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, SIZE_MAX, 0)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = levenshtein_editops_func(s1_proc.string, s2_proc.string, c_score_hint)
    return ops


def levenshtein_opcodes(s1, s2, *, processor=None, score_hint=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)
    cdef size_t c_score_hint = get_score_cutoff_size_t(score_hint, SIZE_MAX, 0)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = levenshtein_editops_func(s1_proc.string, s2_proc.string, c_score_hint)
    return ops.as_opcodes()

cdef void KwargsDeinit(RF_Kwargs* self) noexcept:
    free(<void*>self.context)

cdef bool LevenshteinKwargsInit(RF_Kwargs* self, dict kwargs) except False:
    cdef size_t insertion, deletion, substitution
    cdef LevenshteinWeightTable* weights = <LevenshteinWeightTable*>malloc(sizeof(LevenshteinWeightTable))

    if not weights:
        raise MemoryError

    insertion, deletion, substitution = kwargs.get("weights", (1, 1, 1))
    weights.insert_cost = insertion
    weights.delete_cost = deletion
    weights.replace_cost = substitution
    self.context = weights
    self.dtor = KwargsDeinit
    return True

cdef bool GetScorerFlagsLevenshteinDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    cdef LevenshteinWeightTable* weights = <LevenshteinWeightTable*>self.context
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T
    if weights.insert_cost == weights.delete_cost:
        scorer_flags.flags |= RF_SCORER_FLAG_SYMMETRIC
    if LevenshteinMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True

cdef bool GetScorerFlagsLevenshteinSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    cdef LevenshteinWeightTable* weights = <LevenshteinWeightTable*>self.context
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T
    if weights.insert_cost == weights.delete_cost:
        scorer_flags.flags |= RF_SCORER_FLAG_SYMMETRIC
    if LevenshteinMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True

cdef bool GetScorerFlagsLevenshteinNormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    cdef LevenshteinWeightTable* weights = <LevenshteinWeightTable*>self.context
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_NONE_IS_WORST_SCORE
    if weights.insert_cost == weights.delete_cost:
        scorer_flags.flags |= RF_SCORER_FLAG_SYMMETRIC
    if LevenshteinMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 0
    scorer_flags.worst_score.f64 = 1.0
    return True

cdef bool GetScorerFlagsLevenshteinNormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    cdef LevenshteinWeightTable* weights = <LevenshteinWeightTable*>self.context
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_NONE_IS_WORST_SCORE
    if weights.insert_cost == weights.delete_cost:
        scorer_flags.flags |= RF_SCORER_FLAG_SYMMETRIC
    if LevenshteinMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer LevenshteinDistanceContext = CreateScorerContext(LevenshteinKwargsInit, GetScorerFlagsLevenshteinDistance, LevenshteinDistanceInit, UncachedLevenshteinDistanceFuncInit())
SetScorerAttrs(levenshtein_distance, metrics_py.levenshtein_distance, &LevenshteinDistanceContext)

cdef RF_Scorer LevenshteinSimilarityContext = CreateScorerContext(LevenshteinKwargsInit, GetScorerFlagsLevenshteinSimilarity, LevenshteinSimilarityInit, UncachedLevenshteinSimilarityFuncInit())
SetScorerAttrs(levenshtein_similarity, metrics_py.levenshtein_similarity, &LevenshteinSimilarityContext)

cdef RF_Scorer LevenshteinNormalizedDistanceContext = CreateScorerContext(LevenshteinKwargsInit, GetScorerFlagsLevenshteinNormalizedDistance, LevenshteinNormalizedDistanceInit, UncachedLevenshteinNormalizedDistanceFuncInit())
SetScorerAttrs(levenshtein_normalized_distance, metrics_py.levenshtein_normalized_distance, &LevenshteinNormalizedDistanceContext)

cdef RF_Scorer LevenshteinNormalizedSimilarityContext = CreateScorerContext(LevenshteinKwargsInit, GetScorerFlagsLevenshteinNormalizedSimilarity, LevenshteinNormalizedSimilarityInit, UncachedLevenshteinNormalizedSimilarityFuncInit())
SetScorerAttrs(levenshtein_normalized_similarity, metrics_py.levenshtein_normalized_similarity, &LevenshteinNormalizedSimilarityContext)

SetFuncAttrs(levenshtein_editops, metrics_py.levenshtein_editops)
SetFuncAttrs(levenshtein_opcodes, metrics_py.levenshtein_opcodes)

def damerau_levenshtein_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef RF_StringWrapper s1_proc, s2_proc
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return damerau_levenshtein_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def damerau_levenshtein_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef RF_StringWrapper s1_proc, s2_proc
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return damerau_levenshtein_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def damerau_levenshtein_normalized_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return damerau_levenshtein_normalized_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def damerau_levenshtein_normalized_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return damerau_levenshtein_normalized_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)

cdef bool GetScorerFlagsDamerauLevenshteinDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True

cdef bool GetScorerFlagsDamerauLevenshteinNormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1
    return True

cdef bool GetScorerFlagsDamerauLevenshteinSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True

cdef bool GetScorerFlagsDamerauLevenshteinNormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer DamerauLevenshteinDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsDamerauLevenshteinDistance, DamerauLevenshteinDistanceInit, UncachedDamerauLevenshteinDistanceFuncInit())
SetScorerAttrs(damerau_levenshtein_distance, metrics_py.damerau_levenshtein_distance, &DamerauLevenshteinDistanceContext)

cdef RF_Scorer DamerauLevenshteinNormalizedDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsDamerauLevenshteinNormalizedDistance, DamerauLevenshteinNormalizedDistanceInit, UncachedDamerauLevenshteinNormalizedDistanceFuncInit())
SetScorerAttrs(damerau_levenshtein_normalized_distance, metrics_py.damerau_levenshtein_normalized_distance, &DamerauLevenshteinNormalizedDistanceContext)

cdef RF_Scorer DamerauLevenshteinSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsDamerauLevenshteinSimilarity, DamerauLevenshteinSimilarityInit, UncachedDamerauLevenshteinSimilarityFuncInit())
SetScorerAttrs(damerau_levenshtein_similarity, metrics_py.damerau_levenshtein_similarity, &DamerauLevenshteinSimilarityContext)

cdef RF_Scorer DamerauLevenshteinNormalizedSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsDamerauLevenshteinNormalizedSimilarity, DamerauLevenshteinNormalizedSimilarityInit, UncachedDamerauLevenshteinNormalizedSimilarityFuncInit())
SetScorerAttrs(damerau_levenshtein_normalized_similarity, metrics_py.damerau_levenshtein_normalized_similarity, &DamerauLevenshteinNormalizedSimilarityContext)


def lcs_seq_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef RF_StringWrapper s1_proc, s2_proc
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return lcs_seq_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def lcs_seq_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef RF_StringWrapper s1_proc, s2_proc
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return lcs_seq_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def lcs_seq_normalized_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return lcs_seq_normalized_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def lcs_seq_normalized_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return lcs_seq_normalized_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def lcs_seq_editops(s1, s2, *, processor=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = lcs_seq_editops_func(s1_proc.string, s2_proc.string)
    return ops


def lcs_seq_opcodes(s1, s2, *, processor=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = lcs_seq_editops_func(s1_proc.string, s2_proc.string)
    return ops.as_opcodes()


cdef bool GetScorerFlagsLCSseqDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    if LCSseqMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True

cdef bool GetScorerFlagsLCSseqNormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if LCSseqMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1
    return True

cdef bool GetScorerFlagsLCSseqSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    if LCSseqMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True

cdef bool GetScorerFlagsLCSseqNormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if LCSseqMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer LCSseqDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsLCSseqDistance, LCSseqDistanceInit, UncachedLCSseqDistanceFuncInit())
SetScorerAttrs(lcs_seq_distance, metrics_py.lcs_seq_distance, &LCSseqDistanceContext)

cdef RF_Scorer LCSseqNormalizedDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsLCSseqNormalizedDistance, LCSseqNormalizedDistanceInit, UncachedLCSseqNormalizedDistanceFuncInit())
SetScorerAttrs(lcs_seq_normalized_distance, metrics_py.lcs_seq_normalized_distance, &LCSseqNormalizedDistanceContext)

cdef RF_Scorer LCSseqSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsLCSseqSimilarity, LCSseqSimilarityInit, UncachedLCSseqSimilarityFuncInit())
SetScorerAttrs(lcs_seq_similarity, metrics_py.lcs_seq_similarity, &LCSseqSimilarityContext)

cdef RF_Scorer LCSseqNormalizedSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsLCSseqNormalizedSimilarity, LCSseqNormalizedSimilarityInit, UncachedLCSseqNormalizedSimilarityFuncInit())
SetScorerAttrs(lcs_seq_normalized_similarity, metrics_py.lcs_seq_normalized_similarity, &LCSseqNormalizedSimilarityContext)

SetFuncAttrs(lcs_seq_editops, metrics_py.lcs_seq_editops)
SetFuncAttrs(lcs_seq_opcodes, metrics_py.lcs_seq_opcodes)

def indel_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef RF_StringWrapper s1_proc, s2_proc
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return indel_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def indel_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef RF_StringWrapper s1_proc, s2_proc
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return indel_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def indel_normalized_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return indel_normalized_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def indel_normalized_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return indel_normalized_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def indel_editops(s1, s2, *, processor=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = indel_editops_func(s1_proc.string, s2_proc.string)
    return ops


def indel_opcodes(s1, s2, *, processor=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = indel_editops_func(s1_proc.string, s2_proc.string)
    return ops.as_opcodes()


cdef bool GetScorerFlagsIndelDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    if IndelMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True


cdef bool GetScorerFlagsIndelNormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if IndelMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1
    return True

cdef bool GetScorerFlagsIndelSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    if IndelMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True


cdef bool GetScorerFlagsIndelNormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if IndelMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True


cdef RF_Scorer IndelDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsIndelDistance, IndelDistanceInit, UncachedIndelDistanceFuncInit())
SetScorerAttrs(indel_distance, metrics_py.indel_distance, &IndelDistanceContext)

cdef RF_Scorer IndelNormalizedDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsIndelNormalizedDistance, IndelNormalizedDistanceInit, UncachedIndelNormalizedDistanceFuncInit())
SetScorerAttrs(indel_normalized_distance, metrics_py.indel_normalized_distance, &IndelNormalizedDistanceContext)

cdef RF_Scorer IndelSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsIndelSimilarity, IndelSimilarityInit, UncachedIndelSimilarityFuncInit())
SetScorerAttrs(indel_similarity, metrics_py.indel_similarity, &IndelSimilarityContext)

cdef RF_Scorer IndelNormalizedSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsIndelNormalizedSimilarity, IndelNormalizedSimilarityInit, UncachedIndelNormalizedSimilarityFuncInit())
SetScorerAttrs(indel_normalized_similarity, metrics_py.indel_normalized_similarity, &IndelNormalizedSimilarityContext)

SetFuncAttrs(indel_editops, metrics_py.indel_editops)
SetFuncAttrs(indel_opcodes, metrics_py.indel_opcodes)

def hamming_distance(s1, s2, *, pad=True, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return hamming_distance_func(s1_proc.string, s2_proc.string, pad, c_score_cutoff)

def hamming_similarity(s1, s2, *, pad=True, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return hamming_similarity_func(s1_proc.string, s2_proc.string, pad, c_score_cutoff)

def hamming_normalized_distance(s1, s2, *, pad=True, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return hamming_normalized_distance_func(s1_proc.string, s2_proc.string, pad, c_score_cutoff)


def hamming_normalized_similarity(s1, s2, *, pad=True, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return hamming_normalized_similarity_func(s1_proc.string, s2_proc.string, pad, c_score_cutoff)


def hamming_editops(s1, s2, *, pad=True, processor=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = hamming_editops_func(s1_proc.string, s2_proc.string, pad)
    return ops


def hamming_opcodes(s1, s2, *, pad=True, processor=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    cdef Editops ops = Editops.__new__(Editops)

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    ops.editops = hamming_editops_func(s1_proc.string, s2_proc.string, pad)
    return ops.as_opcodes()

cdef bool HammingKwargsInit(RF_Kwargs* self, dict kwargs) except False:
    cdef bool* pad = <bool*>malloc(sizeof(bool))

    if not pad:
        raise MemoryError

    pad[0] = <bool>kwargs.get("pad", True)
    self.context = pad
    self.dtor = KwargsDeinit
    return True

cdef bool GetScorerFlagsHammingDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True

cdef bool GetScorerFlagsHammingNormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1.0
    return True

cdef bool GetScorerFlagsHammingSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True

cdef bool GetScorerFlagsHammingNormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer HammingDistanceContext = CreateScorerContext(HammingKwargsInit, GetScorerFlagsHammingDistance, HammingDistanceInit, UncachedHammingDistanceFuncInit())
SetScorerAttrs(hamming_distance, metrics_py.hamming_distance, &HammingDistanceContext)

cdef RF_Scorer HammingNormalizedDistanceContext = CreateScorerContext(HammingKwargsInit, GetScorerFlagsHammingNormalizedDistance, HammingNormalizedDistanceInit, UncachedHammingNormalizedDistanceFuncInit())
SetScorerAttrs(hamming_normalized_distance, metrics_py.hamming_normalized_distance, &HammingNormalizedDistanceContext)

cdef RF_Scorer HammingSimilarityContext = CreateScorerContext(HammingKwargsInit, GetScorerFlagsHammingSimilarity, HammingSimilarityInit, UncachedHammingSimilarityFuncInit())
SetScorerAttrs(hamming_similarity, metrics_py.hamming_similarity, &HammingSimilarityContext)

cdef RF_Scorer HammingNormalizedSimilarityContext = CreateScorerContext(HammingKwargsInit, GetScorerFlagsHammingNormalizedSimilarity, HammingNormalizedSimilarityInit, UncachedHammingNormalizedSimilarityFuncInit())
SetScorerAttrs(hamming_normalized_similarity, metrics_py.hamming_normalized_similarity, &HammingNormalizedSimilarityContext)

SetFuncAttrs(hamming_editops, metrics_py.hamming_editops)
SetFuncAttrs(hamming_opcodes, metrics_py.hamming_opcodes)

def osa_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return osa_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def osa_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return osa_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def osa_normalized_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return osa_normalized_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def osa_normalized_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return osa_normalized_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)


cdef bool GetScorerFlagsOSADistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    if OSAMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True

cdef bool GetScorerFlagsOSANormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if OSAMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1.0
    return True

cdef bool GetScorerFlagsOSASimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    if OSAMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True

cdef bool GetScorerFlagsOSANormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if OSAMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer OSADistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsOSADistance, OSADistanceInit, UncachedOSADistanceFuncInit())
SetScorerAttrs(osa_distance, metrics_py.osa_distance, &OSADistanceContext)

cdef RF_Scorer OSANormalizedDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsOSANormalizedDistance, OSANormalizedDistanceInit, UncachedOSANormalizedDistanceFuncInit())
SetScorerAttrs(osa_normalized_distance, metrics_py.osa_normalized_distance, &OSANormalizedDistanceContext)

cdef RF_Scorer OSASimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsOSASimilarity, OSASimilarityInit, UncachedOSASimilarityFuncInit())
SetScorerAttrs(osa_similarity, metrics_py.osa_similarity, &OSASimilarityContext)

cdef RF_Scorer OSANormalizedSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsOSANormalizedSimilarity, OSANormalizedSimilarityInit, UncachedOSANormalizedSimilarityFuncInit())
SetScorerAttrs(osa_normalized_similarity, metrics_py.osa_normalized_similarity, &OSANormalizedSimilarityContext)

###############################################
# Jaro
###############################################

def jaro_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def jaro_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def jaro_normalized_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_normalized_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def jaro_normalized_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_normalized_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)

cdef bool GetScorerFlagsJaroDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if JaroMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1.0
    return True

cdef bool GetScorerFlagsJaroSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if JaroMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer JaroDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsJaroDistance, JaroDistanceInit, UncachedJaroDistanceFuncInit())
SetScorerAttrs(jaro_distance, metrics_py.jaro_distance, &JaroDistanceContext)
SetScorerAttrs(jaro_normalized_distance, metrics_py.jaro_normalized_distance, &JaroDistanceContext)

cdef RF_Scorer JaroSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsJaroSimilarity, JaroSimilarityInit, UncachedJaroSimilarityFuncInit())
SetScorerAttrs(jaro_similarity, metrics_py.jaro_similarity, &JaroSimilarityContext)
SetScorerAttrs(jaro_normalized_similarity, metrics_py.jaro_normalized_similarity, &JaroSimilarityContext)


###############################################
# JaroWinkler
###############################################

def jaro_winkler_distance(s1, s2, *, double prefix_weight=0.1, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    if prefix_weight > 1.0 or prefix_weight < 0.0:
        msg = "prefix_weight has to be in the range 0.0 - 1.0"
        raise ValueError(msg)

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_winkler_distance_func(s1_proc.string, s2_proc.string, prefix_weight, c_score_cutoff)

def jaro_winkler_similarity(s1, s2, *, double prefix_weight=0.1, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    if prefix_weight > 1.0 or prefix_weight < 0.0:
        msg = "prefix_weight has to be in the range 0.0 - 1.0"
        raise ValueError(msg)

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_winkler_similarity_func(s1_proc.string, s2_proc.string, prefix_weight, c_score_cutoff)

def jaro_winkler_normalized_distance(s1, s2, *, double prefix_weight=0.1, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    if prefix_weight > 1.0 or prefix_weight < 0.0:
        msg = "prefix_weight has to be in the range 0.0 - 1.0"
        raise ValueError(msg)

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_winkler_normalized_distance_func(s1_proc.string, s2_proc.string, prefix_weight, c_score_cutoff)

def jaro_winkler_normalized_similarity(s1, s2, *, double prefix_weight=0.1, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    if prefix_weight > 1.0 or prefix_weight < 0.0:
        msg = "prefix_weight has to be in the range 0.0 - 1.0"
        raise ValueError(msg)

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return jaro_winkler_normalized_similarity_func(s1_proc.string, s2_proc.string, prefix_weight, c_score_cutoff)

cdef bool JaroWinklerKwargsInit(RF_Kwargs * self, dict kwargs) except False:
    cdef double * prefix_weight = <double *> malloc(sizeof(double))

    if not prefix_weight:
        raise MemoryError

    prefix_weight[0] = kwargs.get("prefix_weight", 0.1)
    if prefix_weight[0] > 1.0 or prefix_weight[0] < 0.0:
        free(prefix_weight)
        msg = "prefix_weight has to be in the range 0.0 - 1.0"
        raise ValueError(msg)

    self.context = prefix_weight
    self.dtor = KwargsDeinit
    return True

cdef bool GetScorerFlagsJaroWinklerDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if JaroWinklerMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1.0
    return True

cdef bool GetScorerFlagsJaroWinklerSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    if JaroWinklerMultiStringSupport(self):
        scorer_flags.flags |= RF_SCORER_FLAG_MULTI_STRING_INIT

    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer JaroWinklerDistanceContext = CreateScorerContext(JaroWinklerKwargsInit, GetScorerFlagsJaroWinklerDistance, JaroWinklerDistanceInit, UncachedJaroWinklerDistanceFuncInit())
SetScorerAttrs(jaro_winkler_distance, metrics_py.jaro_winkler_distance, &JaroWinklerDistanceContext)
SetScorerAttrs(jaro_winkler_normalized_distance, metrics_py.jaro_winkler_normalized_distance, &JaroWinklerDistanceContext)

cdef RF_Scorer JaroWinklerSimilarityContext = CreateScorerContext(JaroWinklerKwargsInit, GetScorerFlagsJaroWinklerSimilarity, JaroWinklerSimilarityInit, UncachedJaroWinklerSimilarityFuncInit())
SetScorerAttrs(jaro_winkler_similarity, metrics_py.jaro_winkler_similarity, &JaroWinklerSimilarityContext)
SetScorerAttrs(jaro_winkler_normalized_similarity, metrics_py.jaro_winkler_normalized_similarity, &JaroWinklerSimilarityContext)

###############################################
# Postfix
###############################################

def postfix_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return postfix_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def postfix_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return postfix_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def postfix_normalized_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return postfix_normalized_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def postfix_normalized_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return postfix_normalized_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)

cdef bool GetScorerFlagsPostfixDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True

cdef bool GetScorerFlagsPostfixNormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1.0
    return True

cdef bool GetScorerFlagsPostfixSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True

cdef bool GetScorerFlagsPostfixNormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer PostfixDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPostfixDistance, PostfixDistanceInit, UncachedPostfixDistanceFuncInit())
SetScorerAttrs(postfix_distance, metrics_py.postfix_distance, &PostfixDistanceContext)

cdef RF_Scorer PostfixNormalizedDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPostfixNormalizedDistance, PostfixNormalizedDistanceInit, UncachedPostfixNormalizedDistanceFuncInit())
SetScorerAttrs(postfix_normalized_distance, metrics_py.postfix_normalized_distance, &PostfixNormalizedDistanceContext)

cdef RF_Scorer PostfixSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPostfixSimilarity, PostfixSimilarityInit, UncachedPostfixSimilarityFuncInit())
SetScorerAttrs(postfix_similarity, metrics_py.postfix_similarity, &PostfixSimilarityContext)

cdef RF_Scorer PostfixNormalizedSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPostfixNormalizedSimilarity, PostfixNormalizedSimilarityInit, UncachedPostfixNormalizedSimilarityFuncInit())
SetScorerAttrs(postfix_normalized_similarity, metrics_py.postfix_normalized_similarity, &PostfixNormalizedSimilarityContext)


###############################################
# Prefix
###############################################

def prefix_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, SIZE_MAX, 0)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return prefix_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def prefix_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef size_t c_score_cutoff = get_score_cutoff_size_t(score_cutoff, 0, SIZE_MAX)
    cdef RF_StringWrapper s1_proc, s2_proc

    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return prefix_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)

def prefix_normalized_distance(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 1.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 1.0, 0.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return prefix_normalized_distance_func(s1_proc.string, s2_proc.string, c_score_cutoff)


def prefix_normalized_similarity(s1, s2, *, processor=None, score_cutoff=None):
    cdef RF_StringWrapper s1_proc, s2_proc
    setupPandas()
    if is_none(s1) or is_none(s2):
        return 0.0

    cdef double c_score_cutoff = get_score_cutoff_f64(score_cutoff, 0.0, 1.0)
    preprocess_strings(s1, s2, processor, &s1_proc, &s2_proc)
    return prefix_normalized_similarity_func(s1_proc.string, s2_proc.string, c_score_cutoff)


cdef bool GetScorerFlagsPrefixDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = 0
    scorer_flags.worst_score.sizet = SIZE_MAX
    return True

cdef bool GetScorerFlagsPrefixNormalizedDistance(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 0.0
    scorer_flags.worst_score.f64 = 1.0
    return True

cdef bool GetScorerFlagsPrefixSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_SIZE_T | RF_SCORER_FLAG_SYMMETRIC
    scorer_flags.optimal_score.sizet = SIZE_MAX
    scorer_flags.worst_score.sizet = 0
    return True

cdef bool GetScorerFlagsPrefixNormalizedSimilarity(const RF_Kwargs* self, RF_ScorerFlags* scorer_flags) except False nogil:
    scorer_flags.flags = RF_SCORER_FLAG_RESULT_F64 | RF_SCORER_FLAG_SYMMETRIC | RF_SCORER_NONE_IS_WORST_SCORE
    scorer_flags.optimal_score.f64 = 1.0
    scorer_flags.worst_score.f64 = 0
    return True

cdef RF_Scorer PrefixDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPrefixDistance, PrefixDistanceInit, UncachedPrefixDistanceFuncInit())
SetScorerAttrs(prefix_distance, metrics_py.prefix_distance, &PrefixDistanceContext)

cdef RF_Scorer PrefixNormalizedDistanceContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPrefixNormalizedDistance, PrefixNormalizedDistanceInit, UncachedPrefixNormalizedDistanceFuncInit())
SetScorerAttrs(prefix_normalized_distance, metrics_py.prefix_normalized_distance, &PrefixNormalizedDistanceContext)

cdef RF_Scorer PrefixSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPrefixSimilarity, PrefixSimilarityInit, UncachedPrefixSimilarityFuncInit())
SetScorerAttrs(prefix_similarity, metrics_py.prefix_similarity, &PrefixSimilarityContext)

cdef RF_Scorer PrefixNormalizedSimilarityContext = CreateScorerContext(NoKwargsInit, GetScorerFlagsPrefixNormalizedSimilarity, PrefixNormalizedSimilarityInit, UncachedPrefixNormalizedSimilarityFuncInit())
SetScorerAttrs(prefix_normalized_similarity, metrics_py.prefix_normalized_similarity, &PrefixNormalizedSimilarityContext)
