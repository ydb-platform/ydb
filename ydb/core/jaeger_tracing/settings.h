#pragma once

#include "request_discriminator.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NKikimr::NJaegerTracing {

namespace NPrivate {

template<class T, size_t OnStack, class TFunc>
auto MapValues(const TStackVec<T, OnStack>& v, TFunc&& f) {
    using TResultValue = std::invoke_result_t<TFunc, const T&>;

    TStackVec<TResultValue, OnStack> result;
    result.reserve(v.size());
    for (const auto& item : v) {
        result.push_back(f(item));
    }
    return result;
}

template<class TKey, class TValue, class TFunc>
auto MapValues(const THashMap<TKey, TValue>& m, TFunc&& f) {
    using TResultValue = std::invoke_result_t<TFunc, const TValue&>;

    THashMap<TKey, TResultValue> result;
    result.reserve(m.size());
    for (const auto& [key, value] : m) {
        result.emplace(key, f(value));
        // result[key] = f(value);
    }
    return result;
}

// TODO: remove if unused
template<class T, size_t Size, class TFunc>
auto MapValues(const std::array<T, Size>& v, TFunc&& f) {
    using TResultValue = std::invoke_result_t<TFunc, const T&>;

    return [&v, &f]<size_t... I>(std::index_sequence<I...>) -> std::array<TResultValue, Size> {
        return { f(v[I])...};
    }(std::make_index_sequence<Size>());
}

} // namespace NPrivate

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
            .TThrottler = std::forward<TFunc>(f)(Throttler),
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
            .TThrottler = std::forward<TFunc>(f)(Throttler),
        };
    }
};

template<class TSampling, class TThrottling>
struct TSettings {
private:
    template<class T>
    using TContainer = std::array<THashMap<TMaybe<TString>, TStackVec<T, 4>>, kRequestTypesCnt>;

    template<class T, class TFunc>
    static auto MapValues(const TContainer<T>& v, TFunc&& f) {
        return NPrivate::MapValues(
            v,
            [&f](const auto& reqTypeSamplingRules) {
                return NPrivate::MapValues(
                    reqTypeSamplingRules,
                    [&f](const auto& dbSamplingRules) {
                        return NPrivate::MapValues(dbSamplingRules, f);
                    }
                );
            }
        );
    }

public:
    TContainer<TSamplingRule<TSampling, TThrottling>> SamplingRules;
    TContainer<TExternalThrottlingRule<TThrottling>> ExternalThrottlingRules;

    template<class TFunc>
    auto MapSampler(TFunc&& f) const {
        using TNewSamplingType = std::invoke_result_t<TFunc, const TSampling&>;

        return TSettings<TNewSamplingType, TThrottling> {
            .SamplingRules = MapValues(
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
            .SamplingRules = MapValues(
                SamplingRules,
                [&f](const auto& v) {
                    return v.MapThrottler(f);
                }
            ),
            .ExternalThrottlingRules = MapValues(
                ExternalThrottlingRules,
                [&f](const auto& v) {
                    return v.MapThrottler(f);
                }
            ),
        };
    }
};

} // namespace NKikimr::NJaegerTracing
