#pragma once
#include "cpp_common.hpp"

static inline double ratio_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::ratio(s1, s2, score_cutoff);
    });
}
static inline bool RatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count, const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::fuzz::experimental::MultiRatio, double>(self, str_count, str);
#endif

    return similarity_init<fuzz::CachedRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline bool RatioMultiStringSupport(const RF_Kwargs*)
{
#ifdef RAPIDFUZZ_X64
    return true;
#else
    return false;
#endif
}

static inline double partial_ratio_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::partial_ratio(s1, s2, score_cutoff);
    });
}
static inline bool PartialRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                    const RF_String* str)
{
    return similarity_init<fuzz::CachedPartialRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedPartialRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = partial_ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline rf::ScoreAlignment<double> partial_ratio_alignment_func(const RF_String& str1, const RF_String& str2,
                                                        double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::partial_ratio_alignment(s1, s2, score_cutoff);
    });
}

static inline double token_sort_ratio_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::token_sort_ratio(s1, s2, score_cutoff);
    });
}
static inline bool TokenSortRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                      const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::fuzz::experimental::MultiTokenSortRatio, double>(self, str_count,
                                                                                          str);
#endif

    return similarity_init<fuzz::CachedTokenSortRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedTokenSortRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = token_sort_ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double token_set_ratio_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::token_set_ratio(s1, s2, score_cutoff);
    });
}
static inline bool TokenSetRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                     const RF_String* str)
{
    return similarity_init<fuzz::CachedTokenSetRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedTokenSetRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = token_set_ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double token_ratio_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::token_ratio(s1, s2, score_cutoff);
    });
}
static inline bool TokenRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                  const RF_String* str)
{
    return similarity_init<fuzz::CachedTokenRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedTokenRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = token_ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double partial_token_sort_ratio_func(const RF_String& str1, const RF_String& str2,
                                                   double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::partial_token_sort_ratio(s1, s2, score_cutoff);
    });
}
static inline bool PartialTokenSortRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                             const RF_String* str)
{
    return similarity_init<fuzz::CachedPartialTokenSortRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedPartialTokenSortRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = partial_token_sort_ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double partial_token_set_ratio_func(const RF_String& str1, const RF_String& str2,
                                                  double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::partial_token_set_ratio(s1, s2, score_cutoff);
    });
}
static inline bool PartialTokenSetRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                            const RF_String* str)
{
    return similarity_init<fuzz::CachedPartialTokenSetRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedPartialTokenSetRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = partial_token_set_ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double partial_token_ratio_func(const RF_String& str1, const RF_String& str2,
                                              double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::partial_token_ratio(s1, s2, score_cutoff);
    });
}
static inline bool PartialTokenRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count,
                                         const RF_String* str)
{
    return similarity_init<fuzz::CachedPartialTokenRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedPartialTokenRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = partial_token_ratio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double WRatio_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::WRatio(s1, s2, score_cutoff);
    });
}
static inline bool WRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count, const RF_String* str)
{
    return similarity_init<fuzz::CachedWRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedWRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = WRatio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}

static inline double QRatio_func(const RF_String& str1, const RF_String& str2, double score_cutoff)
{
    return visitor(str1, str2, [&](auto s1, auto s2) {
        return fuzz::QRatio(s1, s2, score_cutoff);
    });
}
static inline bool QRatioInit(RF_ScorerFunc* self, const RF_Kwargs*, int64_t str_count, const RF_String* str)
{
#ifdef RAPIDFUZZ_X64
    if (str_count != 1)
        return multi_similarity_init<rf::fuzz::experimental::MultiQRatio, double>(self, str_count, str);
#endif

    return similarity_init<fuzz::CachedQRatio, double>(self, str_count, str);
}
static inline RF_UncachedScorerFunc UncachedQRatioFuncInit()
{
    RF_UncachedScorerFunc scorer;
    scorer.call.f64 = [](const RF_String* str1, const RF_String* str2, const RF_Kwargs*, double score_cutoff,
                         double, double* result) {
        *result = QRatio_func(*str1, *str2, score_cutoff);
        return true;
    };
    return scorer;
}
