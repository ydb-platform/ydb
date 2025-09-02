#include "config.h"

#include <util/string/builder.h>

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
    sb << "}";
    return sb;
}

}   // namespace NKikimr::NGeneralCache::NPublic
