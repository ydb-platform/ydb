#pragma once

#include "request_discriminator.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NKikimr::NJaegerTracing {

struct TThrottlingSettings {
    ui64 MaxTracesPerMinute;
    ui64 MaxTracesBurst;
};

template<class TSampling, class TThrottling>
struct TSamplingRule {
    ui8 Level;
    TSampling Sampler;
    TThrottling Throttler;

    template<class TFunc>
    auto MapSampler(TFunc&& f) const {
        using TNewSamplingType = std::invoke_result_t<TFunc, const TSampling&>;

        return TSamplingRule<TNewSamplingType, TThrottling> {
            .Level = Level,
            .Sampler = std::forward<TFunc>(f)(Sampler),
            .Throttler = Throttler,
        };
    }

    template<class TFunc>
    auto MapThrottler(TFunc&& f) const {
        using TNewThrottlingType = std::invoke_result_t<TFunc, const TThrottling&>;

        return TSamplingRule<TSampling, TNewThrottlingType> {
            .Level = Level,
            .Sampler = Sampler,
            .Throttler = std::forward<TFunc>(f)(Throttler),
        };
    }
};

template<class TThrottling>
struct TExternalThrottlingRule {
    TThrottling Throttler;

    template<class TFunc>
    auto MapThrottler(TFunc&& f) const {
        using TNewThrottlingType = std::invoke_result_t<TFunc, const TThrottling&>;

        return TExternalThrottlingRule<TNewThrottlingType> {
            .Throttler = std::forward<TFunc>(f)(Throttler),
        };
    }
};

template<class T>
struct TRequestTypeRules {
    TStackVec<T, 4> Global;
    THashMap<TString, TStackVec<T, 4>> DatabaseRules;
};

template<class T>
using TRulesContainer = std::array<TRequestTypeRules<T>, kRequestTypesCnt>;

template<class TSampling, class TThrottling>
struct TSettings {
public:
    TRulesContainer<TSamplingRule<TSampling, TThrottling>> SamplingRules;
    TRulesContainer<TExternalThrottlingRule<TThrottling>> ExternalThrottlingRules;

    template<class TFunc>
    auto MapSampler(TFunc&& f) const {
        using TNewSamplingType = std::invoke_result_t<TFunc, const TSampling&>;

        return TSettings<TNewSamplingType, TThrottling> {
            .SamplingRules = MapContainerValues(
                SamplingRules,
                [&f](const auto& v) {
                    return v.MapSampler(f);
                }
            ),
            .ExternalThrottlingRules = ExternalThrottlingRules,
        };
    }

    template<class TFunc>
    auto MapThrottler(TFunc&& f) const {
        using TNewThrottlingType = std::invoke_result_t<TFunc, const TThrottling&>;

        return TSettings<TSampling, TNewThrottlingType> {
            .SamplingRules = MapContainerValues(
                SamplingRules,
                [&f](const auto& v) {
                    return v.MapThrottler(f);
                }
            ),
            .ExternalThrottlingRules = MapContainerValues(
                ExternalThrottlingRules,
                [&f](const auto& v) {
                    return v.MapThrottler(f);
                }
            ),
        };
    }

private:
    template<class T, size_t OnStack, class TFunc>
    static auto MapValues(const TStackVec<T, OnStack>& v, TFunc&& f) {
        using TResultValue = std::invoke_result_t<TFunc, const T&>;

        TStackVec<TResultValue, OnStack> result;
        result.reserve(v.size());
        for (const auto& item : v) {
            result.push_back(f(item));
        }
        return result;
    }

    template<class TKey, class TValue, class TFunc>
    static auto MapValues(const THashMap<TKey, TValue>& m, TFunc&& f) {
        using TResultValue = std::invoke_result_t<TFunc, const TValue&>;

        THashMap<TKey, TResultValue> result;
        result.reserve(m.size());
        for (const auto& [key, value] : m) {
            result.emplace(key, f(value));
        }
        return result;
    }

    template<class T, size_t Size, class TFunc>
    static auto MapValues(const std::array<T, Size>& v, TFunc&& f) {
        using TResultValue = std::invoke_result_t<TFunc, const T&>;

        return [&v, &f]<size_t... I>(std::index_sequence<I...>) -> std::array<TResultValue, Size> {
            return { f(v[I])...};
        }(std::make_index_sequence<Size>());
    }

    template<class T, class TFunc>
    static TRulesContainer<std::invoke_result_t<TFunc, const T&>> MapContainerValues(const TRulesContainer<T>& v, TFunc&& f) {
        using TResultValue = std::invoke_result_t<TFunc, const T&>;

        return MapValues(
            v,
            [&f](const TRequestTypeRules<T>& reqTypeRules) {
                return TRequestTypeRules<TResultValue> {
                    .Global = MapValues(reqTypeRules.Global, f),
                    .DatabaseRules = MapValues(
                        reqTypeRules.DatabaseRules,
                        [&f](const auto& dbSamplingRules) {
                            return MapValues(dbSamplingRules, f);
                        }
                    ),
                };
            }
        );
    }
};

} // namespace NKikimr::NJaegerTracing
