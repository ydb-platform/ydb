#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimrConfig {
    class TGeneralCacheConfig;
}

namespace NKikimr::NGeneralCache::NPublic {

struct TCacheRequestClass {
    ui32 Priority = 0;
    ui32 Weight = 0;
    ui64 QueueId = 0;
};

class TConfig {
private:
    YDB_READONLY_DEF(std::optional<ui64>, MemoryLimit);
    YDB_READONLY(ui64, DirectInflightSourceLimit, 2000);
    YDB_READONLY(ui64, DirectInflightGlobalLimit, 20000);
    THashMap<TString, TCacheRequestClass> Consumers;
    std::optional<TCacheRequestClass> AnyConsumer;

    TConfig() = default;
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TGeneralCacheConfig& config);

public:
    static TConfig BuildDefault() {
        static TConfig result = TConfig();
        return result;
    }

    static TConclusion<TConfig> BuildFromProto(const NKikimrConfig::TGeneralCacheConfig& protoConfig);

    TCacheRequestClass Classify(const TString& consumerName) const;
    [[nodiscard]] TConclusionStatus ValidateNames(const THashSet<TString>& allowedNames) const;
    TString DebugString() const;
};

}   // namespace NKikimr::NGeneralCache::NPublic
