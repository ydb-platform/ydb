#include "consumers.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>

#include <util/generic/serialized_enum.h>
#include <util/stream/output.h>

namespace NKikimr::NOlap::NGeneralCache {

THashSet<TString> GetAllowedCacheConsumers() {
    using EConsumer = NBlobOperations::EConsumer;
    THashSet<TString> allowed;
    for (const EConsumer consumer : GetEnumAllValues<EConsumer>()) {
        if (consumer == EConsumer::COUNT || consumer == EConsumer::UNDEFINED) {
            continue;
        }
        allowed.emplace(::ToString(consumer));
    }
    return allowed;
}

NKikimrConfig::TGeneralCacheConfig ApplyCacheConsumerWeights(
    NKikimrConfig::TGeneralCacheConfig config, const bool weightsEnabled, const bool useDefaultConsumers) {
    if (!weightsEnabled) {
        if (config.ConsumersSize()) {
            Cerr << "Warning: general cache consumers are ignored while EnableCsGeneralCacheConsumerWeights is off" << Endl;
        }
        config.ClearConsumers();
        return config;
    }
    THashSet<TString> configured;
    for (const auto& consumer : config.GetConsumers()) {
        configured.emplace(consumer.GetName());
    }
    const auto addMissing = [&](const TString& name, const ui32 weight) {
        if (configured.emplace(name).second) {
            auto& item = *config.AddConsumers();
            item.SetName(name);
            item.SetWeight(weight);
        }
    };
    if (useDefaultConsumers) {
        using EConsumer = NBlobOperations::EConsumer;
        addMissing(::ToString(EConsumer::SCAN), 8);
        addMissing(::ToString(EConsumer::SYS_VIEW_SCAN), 8);
        addMissing(::ToString(EConsumer::DUPLICATE_FILTERING), 8);
        addMissing(::ToString(EConsumer::GENERAL_COMPACTION), 2);
    }
    // An unlisted consumer gets weight 0 only when "*" is set to 0 explicitly.
    if (config.ConsumersSize()) {
        addMissing("*", 1);
    }
    return config;
}

::NKikimr::NGeneralCache::NPublic::TConfig BuildCacheConfig(
    const TString& cacheName, const NKikimrConfig::TGeneralCacheConfig& config, const bool weightsEnabled, const bool useDefaultConsumers) {
    using TConfig = ::NKikimr::NGeneralCache::NPublic::TConfig;
    const auto allowed = GetAllowedCacheConsumers();
    auto withWeights = ApplyCacheConsumerWeights(config, weightsEnabled, useDefaultConsumers);
    auto parsed = TConfig::BuildFromProto(withWeights, allowed);
    if (parsed.IsSuccess()) {
        return parsed.DetachResult();
    }
    Cerr << "Error: cannot parse " << cacheName << " consumers, they are ignored: " << parsed.GetErrorMessage() << Endl;
    withWeights.ClearConsumers();
    auto withoutConsumers = TConfig::BuildFromProto(withWeights, allowed);
    Y_ABORT_UNLESS(withoutConsumers.IsSuccess(), "%s", withoutConsumers.GetErrorMessage().c_str());
    return withoutConsumers.DetachResult();
}

}   // namespace NKikimr::NOlap::NGeneralCache
