#include "config.h"

#include <util/string/builder.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NGeneralCache::NPublic {

TConclusionStatus TConfig::DeserializeFromProto(const NKikimrConfig::TGeneralCacheConfig& config) {
    if (config.HasDirectInflightSourceLimit()) {
        DirectInflightSourceLimit = config.GetDirectInflightSourceLimit();
    }
    if (config.HasDirectInflightGlobalLimit()) {
        DirectInflightGlobalLimit = config.GetDirectInflightGlobalLimit();
    }
    if (config.HasMemoryLimit()) {
        MemoryLimit = config.GetMemoryLimit();
    }
    THashSet<TString> seen;
    ui64 nextQueueId = 1;
    for (const auto& consumer : config.GetConsumers()) {
        if (!consumer.HasName() || consumer.GetName().empty()) {
            return TConclusionStatus::Fail("general cache consumer name is empty");
        }
        const TString& name = consumer.GetName();
        if (!seen.emplace(name).second) {
            return TConclusionStatus::Fail(TStringBuilder() << "duplicate general cache consumer " << name);
        }
        TCacheRequestClass requestClass;
        requestClass.Priority = consumer.GetPriority();
        requestClass.Weight = consumer.GetWeight();
        if (requestClass.Weight > 0) {
            requestClass.QueueId = nextQueueId++;
        }
        if (name == "*") {
            AnyConsumer = requestClass;
        } else {
            Consumers.emplace(name, requestClass);
        }
    }
    return TConclusionStatus::Success();
}

TCacheRequestClass TConfig::Classify(const TString& consumerName) const {
    if (const auto* found = Consumers.FindPtr(consumerName)) {
        return *found;
    }
    if (AnyConsumer) {
        return *AnyConsumer;
    }
    return {};
}

TConclusionStatus TConfig::ValidateNames(const THashSet<TString>& allowedNames) const {
    for (const auto& [name, requestClass] : Consumers) {
        Y_UNUSED(requestClass);
        if (!allowedNames.contains(name)) {
            return TConclusionStatus::Fail(TStringBuilder() << "unknown general cache consumer " << name);
        }
    }
    return TConclusionStatus::Success();
}

TConclusion<TConfig> TConfig::BuildFromProto(const NKikimrConfig::TGeneralCacheConfig& protoConfig) {
    TConfig config;
    auto conclusion = config.DeserializeFromProto(protoConfig);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    return config;
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "MemoryLimit=" << MemoryLimit << ";";
    sb << "DirectInflightSourceLimit=" << DirectInflightSourceLimit << ";";
    sb << "DirectInflightGlobalLimit=" << DirectInflightGlobalLimit << ";";
    for (const auto& [name, requestClass] : Consumers) {
        sb << name << "{Priority=" << requestClass.Priority << ";Weight=" << requestClass.Weight << ";};";
    }
    if (AnyConsumer) {
        sb << "*{Priority=" << AnyConsumer->Priority << ";Weight=" << AnyConsumer->Weight << ";};";
    }
    sb << "}";
    return sb;
}

}   // namespace NKikimr::NGeneralCache::NPublic
