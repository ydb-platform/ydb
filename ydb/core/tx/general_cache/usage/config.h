#pragma once
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NGeneralCache::NPublic {

class TConfig {
private:
    YDB_READONLY_DEF(ui32, MemoryLimit, ((ui64)1 << 30));
    YDB_READONLY_DEF(ui32, DirectInflightLimit, 1000);

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
