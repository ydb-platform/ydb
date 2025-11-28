#pragma once
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NGeneralCache::NPublic {

class TConfig {
private:
    YDB_READONLY_DEF(std::optional<ui64>, MemoryLimit);
    YDB_READONLY(ui64, DirectInflightSourceLimit, 2000);
    YDB_READONLY(ui64, DirectInflightGlobalLimit, 20000);

    TConfig() = default;
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TGeneralCacheConfig& config);

public:
    static TConfig BuildDefault() {
        static TConfig result = TConfig();
        return result;
    }

    static TConclusion<TConfig> BuildFromProto(const NKikimrConfig::TGeneralCacheConfig& protoConfig);

    TString DebugString() const;
};

}   // namespace NKikimr::NGeneralCache::NPublic
