#pragma once
#include <ydb/library/accessor/accessor.h>

#include <optional>

namespace NKikimrConfig {
    class TGroupedMemoryLimiterConfig;
}

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TConfig {
private:
    YDB_READONLY(bool, Enabled, true);
    YDB_READONLY_DEF(std::optional<ui64>, MemoryLimit);
    YDB_READONLY_DEF(std::optional<ui64>, HardMemoryLimit);
    YDB_READONLY(ui64, CountBuckets, 1);
    YDB_READONLY_DEF(std::optional<double>, UnrestrictedSoftLimitCoefficient);
    YDB_READONLY(ui32, MaxUnrestrictedGroupsPerScope, 1);

public:

    static TConfig BuildDisabledConfig() {
        TConfig result;
        result.Enabled = false;
        return result;
    }

    bool IsEnabled() const {
        return Enabled;
    }
    bool IsUnrestrictedEnabled() const {
        return UnrestrictedSoftLimitCoefficient.has_value();
    }
    std::optional<ui64> MakeUnrestrictedSoftBytes(const std::optional<ui64>& hardBytes) const;
    bool DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config);
    TString DebugString() const;
};

}
