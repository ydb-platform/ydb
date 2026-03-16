#pragma once
#include "cpp_common.hpp"

/* Levenshtein */
static inline size_t levenshtein_distance_func(const RF_String& str1, const RF_String& str2, size_t insertion,
                                               size_t deletion, size_t substitution, size_t score_cutoff,
                                               size_t score_hint)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::levenshtein_distance(s1, s2, {insertion, deletion, substitution}, score_cutoff,
                                        score_hint);
    });
}

static inline bool LevenshteinDistanceInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs, int64_t str_count,
                                           const RF_String* str)
{
    rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (weights.insert_cost == 1 && weights.delete_cost == 1 && weights.replace_cost == 1) {
        if (str_count != 1)
            return multi_distance_init<rf::experimental::MultiLevenshtein, size_t>(self, str_count, str);
    }
#endif

    return distance_init<rf::CachedLevenshtein, size_t>(self, str_count, str, weights);
}
static inline RF_UncachedScorerFunc UncachedLevenshteinDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                           size_t score_cutoff, size_t score_hint, size_t* result) {
        rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);
        *result = levenshtein_distance_func(*str1, *str2, weights.insert_cost, weights.delete_cost,
                                            weights.replace_cost, score_cutoff, score_hint);
        return true;
    };
    return scorer;
}
static inline bool LevenshteinMultiStringSupport(const RF_Kwargs* kwargs)
{
    [[maybe_unused]] rf::LevenshteinWeightTable weights =
        *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (weights.insert_cost == 1 && weights.delete_cost == 1 && weights.replace_cost == 1) return true;
#endif
    return false;
}

static inline double levenshtein_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                          size_t insertion, size_t deletion,
                                                          size_t substitution, double score_cutoff,
                                                          double score_hint)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::levenshtein_normalized_distance(s1, s2, {insertion, deletion, substitution}, score_cutoff,
                                                   score_hint);
    });
}
static inline bool LevenshteinNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs,
                                                     int64_t str_count, const RF_String* str)
{
    rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (weights.insert_cost == 1 && weights.delete_cost == 1 && weights.replace_cost == 1) {
        if (str_count != 1)
            return multi_normalized_distance_init<rf::experimental::MultiLevenshtein, double>(self, str_count,
                                                                                              str);
    }
#endif

    return normalized_distance_init<rf::CachedLevenshtein, double>(self, str_count, str, weights);
}
static inline RF_UncachedScorerFunc UncachedLevenshteinNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double score_hint, double* result) {
        rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);
        *result = levenshtein_normalized_distance_func(*str1, *str2, weights.insert_cost, weights.delete_cost,
                                                       weights.replace_cost, score_cutoff, score_hint);
        return true;
    };
    return scorer;
}

static inline size_t levenshtein_similarity_func(const RF_String& str1, const RF_String& str2,
                                                 size_t insertion, size_t deletion, size_t substitution,
                                                 size_t score_cutoff, size_t score_hint)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::levenshtein_similarity(s1, s2, {insertion, deletion, substitution}, score_cutoff,
                                          score_hint);
    });
}

static inline bool LevenshteinSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs, int64_t str_count,
                                             const RF_String* str)
{
    rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (weights.insert_cost == 1 && weights.delete_cost == 1 && weights.replace_cost == 1) {
        if (str_count != 1)
            return multi_similarity_init<rf::experimental::MultiLevenshtein, size_t>(self, str_count, str);
    }
#endif

    return similarity_init<rf::CachedLevenshtein, size_t>(self, str_count, str, weights);
}

static inline RF_UncachedScorerFunc UncachedLevenshteinSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                           size_t score_cutoff, size_t score_hint, size_t* result) {
        rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);
        *result = levenshtein_similarity_func(*str1, *str2, weights.insert_cost, weights.delete_cost,
                                              weights.replace_cost, score_cutoff, score_hint);
        return true;
    };
    return scorer;
}

static inline double levenshtein_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                            size_t insertion, size_t deletion,
                                                            size_t substitution, double score_cutoff,
                                                            double score_hint)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::levenshtein_normalized_similarity(s1, s2, {insertion, deletion, substitution},
                                                     score_cutoff, score_hint);
    });
}
static inline bool LevenshteinNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs,
                                                       int64_t str_count, const RF_String* str)
{
    rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (weights.insert_cost == 1 && weights.delete_cost == 1 && weights.replace_cost == 1) {
        if (str_count != 1)
            return multi_normalized_similarity_init<rf::experimental::MultiLevenshtein, double>(
                self, str_count, str);
    }
#endif

    return normalized_similarity_init<rf::CachedLevenshtein, double>(self, str_count, str, weights);
}
static inline RF_UncachedScorerFunc UncachedLevenshteinNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double score_hint, double* result) {
        rf::LevenshteinWeightTable weights = *static_cast<rf::LevenshteinWeightTable*>(kwargs->context);
        *result =
            levenshtein_normalized_similarity_func(*str1, *str2, weights.insert_cost, weights.delete_cost,
                                                   weights.replace_cost, score_cutoff, score_hint);
        return true;
    };
    return scorer;
}

/* Damerau Levenshtein */
static inline size_t damerau_levenshtein_distance_func(const RF_String& str1, const RF_String& str2,
                                                       size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::experimental::damerau_levenshtein_distance(s1, s2, score_cutoff);
    });
}

static inline RF_UncachedScorerFunc UncachedDamerauLevenshteinDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = damerau_levenshtein_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline bool DamerauLevenshteinDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                  const RF_String* str)
{
    return distance_init<rf::experimental::CachedDamerauLevenshtein, size_t>(self, str_count, str);
}

static inline double damerau_levenshtein_normalized_distance_func(const RF_String& str1,
                                                                  const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::experimental::damerau_levenshtein_normalized_distance(s1, s2, score_cutoff);
    });
}
static inline bool DamerauLevenshteinNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*,
                                                            int64_t str_count, const RF_String* str)
{
    return normalized_distance_init<rf::experimental::CachedDamerauLevenshtein, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedDamerauLevenshteinNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = damerau_levenshtein_normalized_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline size_t damerau_levenshtein_similarity_func(const RF_String& str1, const RF_String& str2,
                                                         size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::experimental::damerau_levenshtein_similarity(s1, s2, score_cutoff);
    });
}

static inline bool DamerauLevenshteinSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                    const RF_String* str)
{
    return similarity_init<rf::experimental::CachedDamerauLevenshtein, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedDamerauLevenshteinSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = damerau_levenshtein_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double damerau_levenshtein_normalized_similarity_func(const RF_String& str1,
                                                                    const RF_String& str2,
                                                                    double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::experimental::damerau_levenshtein_normalized_similarity(s1, s2, score_cutoff);
    });
}
static inline bool DamerauLevenshteinNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*,
                                                              int64_t str_count, const RF_String* str)
{
    return normalized_similarity_init<rf::experimental::CachedDamerauLevenshtein, double>(self, str_count,
                                                                                          str);
}

static inline RF_UncachedScorerFunc UncachedDamerauLevenshteinNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = damerau_levenshtein_normalized_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

/* Hamming */
static inline size_t hamming_distance_func(const RF_String& str1, const RF_String& str2, bool pad,
                                           size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::hamming_distance(s1, s2, pad, score_cutoff);
    });
}
static inline bool HammingDistanceInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs, int64_t str_count,
                                       const RF_String* str)
{
    bool pad = *static_cast<bool*>(kwargs->context);

    return distance_init<rf::CachedHamming, size_t>(self, str_count, str, pad);
}

static inline RF_UncachedScorerFunc UncachedHammingDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                           size_t score_cutoff, size_t, size_t* result) {
        bool pad = *static_cast<bool*>(kwargs->context);
        *result = hamming_distance_func(*str1, *str2, pad, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double hamming_normalized_distance_func(const RF_String& str1, const RF_String& str2, bool pad,
                                                      double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::hamming_normalized_distance(s1, s2, pad, score_cutoff);
    });
}
static inline bool HammingNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs,
                                                 int64_t str_count, const RF_String* str)
{
    bool pad = *static_cast<bool*>(kwargs->context);

    return normalized_distance_init<rf::CachedHamming, double>(self, str_count, str, pad);
}

static inline RF_UncachedScorerFunc UncachedHammingNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double, double* result) {
        bool pad = *static_cast<bool*>(kwargs->context);
        *result = hamming_normalized_distance_func(*str1, *str2, pad, score_cutoff);
        return true;
    };
    return scorer;
}

static inline size_t hamming_similarity_func(const RF_String& str1, const RF_String& str2, bool pad,
                                             size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::hamming_similarity(s1, s2, pad, score_cutoff);
    });
}
static inline bool HammingSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs, int64_t str_count,
                                         const RF_String* str)
{
    bool pad = *static_cast<bool*>(kwargs->context);

    return similarity_init<rf::CachedHamming, size_t>(self, str_count, str, pad);
}

static inline RF_UncachedScorerFunc UncachedHammingSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                           size_t score_cutoff, size_t, size_t* result) {
        bool pad = *static_cast<bool*>(kwargs->context);
        *result = hamming_similarity_func(*str1, *str2, pad, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double hamming_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                        bool pad, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::hamming_normalized_similarity(s1, s2, pad, score_cutoff);
    });
}
static inline bool HammingNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs,
                                                   int64_t str_count, const RF_String* str)
{
    bool pad = *static_cast<bool*>(kwargs->context);

    return normalized_similarity_init<rf::CachedHamming, double>(self, str_count, str, pad);
}

static inline RF_UncachedScorerFunc UncachedHammingNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double, double* result) {
        bool pad = *static_cast<bool*>(kwargs->context);
        *result = hamming_normalized_similarity_func(*str1, *str2, pad, score_cutoff);
        return true;
    };
    return scorer;
}

/* Indel */
static inline size_t indel_distance_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::indel_distance(s1, s2, score_cutoff);
    });
}
static inline bool IndelDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                     const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_distance_init<rf::experimental::MultiIndel, size_t>(self, str_count, str);
#endif

    return distance_init<rf::CachedIndel, size_t>(self, str_count, str);
}
static inline bool IndelMultiStringSupport(const RF_Kwargs*)
{
#ifdef RAPIDFUZZ_X64
    return true;
#else
    return false;
#endif
}

static inline RF_UncachedScorerFunc UncachedIndelDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = indel_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double indel_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                    double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::indel_normalized_distance(s1, s2, score_cutoff);
    });
}
static inline bool IndelNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                               const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_distance_init<rf::experimental::MultiIndel, double>(self, str_count, str);
#endif

    return normalized_distance_init<rf::CachedIndel, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedIndelNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = indel_normalized_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline size_t indel_similarity_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::indel_similarity(s1, s2, score_cutoff);
    });
}
static inline bool IndelSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                       const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::experimental::MultiIndel, size_t>(self, str_count, str);
#endif

    return similarity_init<rf::CachedIndel, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedIndelSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = indel_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double indel_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                      double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::indel_normalized_similarity(s1, s2, score_cutoff);
    });
}
static inline bool IndelNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                 const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_similarity_init<rf::experimental::MultiIndel, double>(self, str_count, str);
#endif

    return normalized_similarity_init<rf::CachedIndel, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedIndelNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = indel_normalized_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

/* LCSseq */
static inline size_t lcs_seq_distance_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::lcs_seq_distance(s1, s2, score_cutoff);
    });
}
static inline bool LCSseqDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                      const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_distance_init<rf::experimental::MultiLCSseq, size_t>(self, str_count, str);
#endif

    return distance_init<rf::CachedLCSseq, size_t>(self, str_count, str);
}
static inline bool LCSseqMultiStringSupport(const RF_Kwargs*)
{
#ifdef RAPIDFUZZ_X64
    return true;
#else
    return false;
#endif
}

static inline RF_UncachedScorerFunc UncachedLCSseqDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = lcs_seq_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double lcs_seq_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                      double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::lcs_seq_normalized_distance(s1, s2, score_cutoff);
    });
}
static inline bool LCSseqNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_distance_init<rf::experimental::MultiLCSseq, double>(self, str_count, str);
#endif

    return normalized_distance_init<rf::CachedLCSseq, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedLCSseqNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = lcs_seq_normalized_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline size_t lcs_seq_similarity_func(const RF_String& str1, const RF_String& str2,
                                             size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::lcs_seq_similarity(s1, s2, score_cutoff);
    });
}
static inline bool LCSseqSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                        const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::experimental::MultiLCSseq, size_t>(self, str_count, str);
#endif

    return similarity_init<rf::CachedLCSseq, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedLCSseqSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = lcs_seq_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double lcs_seq_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                        double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::lcs_seq_normalized_similarity(s1, s2, score_cutoff);
    });
}
static inline bool LCSseqNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                  const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_similarity_init<rf::experimental::MultiLCSseq, double>(self, str_count, str);
#endif

    return normalized_similarity_init<rf::CachedLCSseq, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedLCSseqNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = lcs_seq_normalized_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline rf::Editops hamming_editops_func(const RF_String& str1, const RF_String& str2, bool pad)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::hamming_editops(s1, s2, pad);
    });
}

static inline rf::Editops levenshtein_editops_func(const RF_String& str1, const RF_String& str2,
                                                   size_t score_hint)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::levenshtein_editops(s1, s2, score_hint);
    });
}

static inline rf::Editops indel_editops_func(const RF_String& str1, const RF_String& str2)
{
    return visitor(str1, str2, [](auto s1, auto s2) {
        return rf::indel_editops(s1, s2);
    });
}

static inline rf::Editops lcs_seq_editops_func(const RF_String& str1, const RF_String& str2)
{
    return visitor(str1, str2, [](auto s1, auto s2) {
        return rf::lcs_seq_editops(s1, s2);
    });
}

/* OSA */
static inline size_t osa_distance_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::osa_distance(s1, s2, score_cutoff);
    });
}

static inline bool OSADistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                   const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1) return multi_distance_init<rf::experimental::MultiOSA, size_t>(self, str_count, str);
#endif

    return distance_init<rf::CachedOSA, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedOSADistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = osa_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline bool OSAMultiStringSupport(const RF_Kwargs*)
{
#ifdef RAPIDFUZZ_X64
    return true;
#else
    return false;
#endif
}

static inline double osa_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                  double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::osa_normalized_distance(s1, s2, score_cutoff);
    });
}
static inline bool OSANormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                             const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_distance_init<rf::experimental::MultiOSA, double>(self, str_count, str);
#endif

    return normalized_distance_init<rf::CachedOSA, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedOSANormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = osa_normalized_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline size_t osa_similarity_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::osa_similarity(s1, s2, score_cutoff);
    });
}

static inline bool OSASimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                     const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::experimental::MultiOSA, size_t>(self, str_count, str);
#endif

    return similarity_init<rf::CachedOSA, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedOSASimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = osa_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double osa_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                    double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::osa_normalized_similarity(s1, s2, score_cutoff);
    });
}
static inline bool OSANormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                               const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_similarity_init<rf::experimental::MultiOSA, double>(self, str_count, str);
#endif

    return normalized_similarity_init<rf::CachedOSA, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedOSANormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = osa_normalized_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

/* Jaro */
static inline double jaro_distance_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_distance(s1, s2, score_cutoff);
    });
}
static inline bool JaroDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                    const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1) return multi_distance_init<rf::experimental::MultiJaro, double>(self, str_count, str);
#endif

    return distance_init<rf::CachedJaro, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedJaroDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = jaro_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double jaro_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                   double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_normalized_distance(s1, s2, score_cutoff);
    });
}
static inline bool JaroNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                              const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_distance_init<rf::experimental::MultiJaro, double>(self, str_count, str);
#endif

    return normalized_distance_init<rf::CachedJaro, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedJaroNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = jaro_normalized_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double jaro_similarity_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_similarity(s1, s2, score_cutoff);
    });
}
static inline bool JaroSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                      const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::experimental::MultiJaro, double>(self, str_count, str);
#endif

    return similarity_init<rf::CachedJaro, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedJaroSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = jaro_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double jaro_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                     double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_normalized_similarity(s1, s2, score_cutoff);
    });
}
static inline bool JaroNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_similarity_init<rf::experimental::MultiJaro, double>(self, str_count, str);
#endif

    return normalized_similarity_init<rf::CachedJaro, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedJaroNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = jaro_normalized_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline bool JaroMultiStringSupport(const RF_Kwargs*)
{
#ifdef RAPIDFUZZ_X64
    return true;
#else
    return false;
#endif
}

/* JaroWinkler */
static inline double jaro_winkler_distance_func(const RF_String& str1, const RF_String& str2,
                                                double prefix_weight, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_winkler_distance(s1, s2, prefix_weight, score_cutoff);
    });
}
static inline bool JaroWinklerDistanceInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs, int64_t str_count,
                                           const RF_String* str)
{
    double prefix_weight = *static_cast<double*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_distance_init<rf::experimental::MultiJaroWinkler, double>(self, str_count, str,
                                                                               prefix_weight);
#endif

    return distance_init<rf::CachedJaroWinkler, double>(self, str_count, str, prefix_weight);
}

static inline RF_UncachedScorerFunc UncachedJaroWinklerDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double, double* result) {
        double prefix_weight = *static_cast<double*>(kwargs->context);
        *result = jaro_winkler_distance_func(*str1, *str2, prefix_weight, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double jaro_winkler_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                           double prefix_weight, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_winkler_normalized_distance(s1, s2, prefix_weight, score_cutoff);
    });
}
static inline bool JaroWinklerNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs,
                                                     int64_t str_count, const RF_String* str)
{
    double prefix_weight = *static_cast<double*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_distance_init<rf::experimental::MultiJaroWinkler, double>(self, str_count,
                                                                                          str, prefix_weight);
#endif

    return normalized_distance_init<rf::CachedJaroWinkler, double>(self, str_count, str, prefix_weight);
}

static inline RF_UncachedScorerFunc UncachedJaroWinklerNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double, double* result) {
        double prefix_weight = *static_cast<double*>(kwargs->context);
        *result = jaro_winkler_normalized_distance_func(*str1, *str2, prefix_weight, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double jaro_winkler_similarity_func(const RF_String& str1, const RF_String& str2,
                                                  double prefix_weight, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_winkler_similarity(s1, s2, prefix_weight, score_cutoff);
    });
}
static inline bool JaroWinklerSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs, int64_t str_count,
                                             const RF_String* str)
{
    double prefix_weight = *static_cast<double*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::experimental::MultiJaroWinkler, double>(self, str_count, str,
                                                                                 prefix_weight);
#endif

    return similarity_init<rf::CachedJaroWinkler, double>(self, str_count, str, prefix_weight);
}

static inline RF_UncachedScorerFunc UncachedJaroWinklerSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double, double* result) {
        double prefix_weight = *static_cast<double*>(kwargs->context);
        *result = jaro_winkler_similarity_func(*str1, *str2, prefix_weight, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double jaro_winkler_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                             double prefix_weight, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::jaro_winkler_normalized_similarity(s1, s2, prefix_weight, score_cutoff);
    });
}
static inline bool JaroWinklerNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs* kwargs,
                                                       int64_t str_count, const RF_String* str)
{
    double prefix_weight = *static_cast<double*>(kwargs->context);

#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_normalized_similarity_init<rf::experimental::MultiJaroWinkler, double>(
            self, str_count, str, prefix_weight);
#endif

    return normalized_similarity_init<rf::CachedJaroWinkler, double>(self, str_count, str, prefix_weight);
}

static inline RF_UncachedScorerFunc UncachedJaroWinklerNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs,
                         double score_cutoff, double, double* result) {
        double prefix_weight = *static_cast<double*>(kwargs->context);
        *result = jaro_winkler_normalized_similarity_func(*str1, *str2, prefix_weight, score_cutoff);
        return true;
    };
    return scorer;
}

static inline bool JaroWinklerMultiStringSupport(const RF_Kwargs*)
{
#ifdef RAPIDFUZZ_X64
    return true;
#else
    return false;
#endif
}

/* Prefix */
static inline size_t prefix_distance_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::prefix_distance(s1, s2, score_cutoff);
    });
}
static inline bool PrefixDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                      const RF_String* str)
{
    return distance_init<rf::CachedPrefix, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPrefixDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = prefix_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double prefix_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                     double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::prefix_normalized_distance(s1, s2, score_cutoff);
    });
}
static inline bool PrefixNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                const RF_String* str)
{
    return normalized_distance_init<rf::CachedPrefix, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPrefixNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = prefix_normalized_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline size_t prefix_similarity_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::prefix_similarity(s1, s2, score_cutoff);
    });
}
static inline bool PrefixSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                        const RF_String* str)
{
    return similarity_init<rf::CachedPrefix, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPrefixSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = prefix_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double prefix_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                       double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::prefix_normalized_similarity(s1, s2, score_cutoff);
    });
}
static inline bool PrefixNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                  const RF_String* str)
{
    return normalized_similarity_init<rf::CachedPrefix, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPrefixNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = prefix_normalized_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

/* Postfix */
static inline size_t postfix_distance_func(const RF_String& str1, const RF_String& str2, size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::postfix_distance(s1, s2, score_cutoff);
    });
}
static inline bool PostfixDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                       const RF_String* str)
{
    return distance_init<rf::CachedPostfix, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPostfixDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = postfix_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double postfix_normalized_distance_func(const RF_String& str1, const RF_String& str2,
                                                      double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::postfix_normalized_distance(s1, s2, score_cutoff);
    });
}
static inline bool PostfixNormalizedDistanceInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                 const RF_String* str)
{
    return normalized_distance_init<rf::CachedPostfix, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPostfixNormalizedDistanceFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = postfix_normalized_distance_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline size_t postfix_similarity_func(const RF_String& str1, const RF_String& str2,
                                             size_t score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::postfix_similarity(s1, s2, score_cutoff);
    });
}
static inline bool PostfixSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                         const RF_String* str)
{
    return similarity_init<rf::CachedPostfix, size_t>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPostfixSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.sizet = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*,
                           size_t score_cutoff, size_t, size_t* result) {
        *result = postfix_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double postfix_normalized_similarity_func(const RF_String& str1, const RF_String& str2,
                                                        double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return rf::postfix_normalized_similarity(s1, s2, score_cutoff);
    });
}
static inline bool PostfixNormalizedSimilarityInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                                   const RF_String* str)
{
    return normalized_similarity_init<rf::CachedPostfix, double>(self, str_count, str);
}

static inline RF_UncachedScorerFunc UncachedPostfixNormalizedSimilarityFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = postfix_normalized_similarity_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}
