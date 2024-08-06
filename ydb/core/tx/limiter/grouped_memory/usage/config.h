#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TConfig {
private:
    YDB_READONLY(bool, Enabled, true);
    YDB_READONLY(ui64, MemoryLimit, ui64(3) << 30);

public:

    bool IsEnabled() const {
        return Enabled;
    }
    bool DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config);
    TString DebugString() const;
};

}
