#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TConfig {
private:
    YDB_READONLY(bool, Enabled, true);
    YDB_READONLY_DEF(std::optional<ui64>, MemoryLimit);
    YDB_READONLY_DEF(std::optional<ui64>, HardMemoryLimit);
    YDB_READONLY(ui64, CountBuckets, 1);

public:

    static TConfig BuildDisabledConfig() {
        TConfig result;
        result.Enabled = false;
        return result;
    }

    bool IsEnabled() const {
        return Enabled;
    }
    bool DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config);
    TString DebugString() const;
};

}
