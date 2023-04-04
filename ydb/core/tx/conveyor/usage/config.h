#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NConveyor {

class TConfig {
private:
    YDB_OPT(ui32, WorkersCount);
    YDB_READONLY(ui32, QueueSizeLimit, 256);
    YDB_READONLY_FLAG(Enabled, true);
public:
    bool DeserializeFromProto(const NKikimrConfig::TConveyorConfig& config);
};

}
