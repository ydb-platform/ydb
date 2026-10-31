#pragma once

#include "storage.h"

#include <ydb/core/tx/general_cache/usage/config.h>

#include <ydb/library/conclusion/result.h>

#include <util/generic/hash_set.h>
#include <util/generic/serialized_enum.h>

namespace NKikimr::NOlap::NBlobOperations {

inline NKikimr::NGeneralCache::NPublic::TCacheRequestClass GetCacheRequestClass(
    const EConsumer consumer, const NKikimr::NGeneralCache::NPublic::TConfig& config) {
    return config.Classify(::ToString(consumer));
}

inline TConclusionStatus ValidateCacheConsumerQuotas(const NKikimr::NGeneralCache::NPublic::TConfig& config) {
    THashSet<TString> allowed;
    for (const EConsumer consumer : GetEnumAllValues<EConsumer>()) {
        if (consumer == EConsumer::COUNT) {
            continue;
        }
        allowed.emplace(::ToString(consumer));
    }
    return config.ValidateNames(allowed);
}

}   // namespace NKikimr::NOlap::NBlobOperations
