#pragma once

#include <ydb/core/tx/general_cache/usage/config.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimrConfig {
class TGeneralCacheConfig;
}

namespace NKikimr::NOlap::NGeneralCache {

THashSet<TString> GetAllowedCacheConsumers();

NKikimrConfig::TGeneralCacheConfig ApplyCacheConsumerWeights(
    NKikimrConfig::TGeneralCacheConfig config, const bool weightsEnabled, const bool useDefaultConsumers);

// An invalid consumer list is dropped on its own, the limits from the config are kept.
::NKikimr::NGeneralCache::NPublic::TConfig BuildCacheConfig(
    const TString& cacheName, const NKikimrConfig::TGeneralCacheConfig& config, const bool weightsEnabled, const bool useDefaultConsumers);

}   // namespace NKikimr::NOlap::NGeneralCache
