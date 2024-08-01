#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NConveyor {

class TConfig {
private:
    YDB_OPT(double, WorkersCount);
    YDB_READONLY(ui32, QueueSizeLimit, 256 * 1024);
    YDB_READONLY_FLAG(Enabled, true);
    YDB_OPT(double, DefaultFractionOfThreadsCount);
public:
    bool DeserializeFromProto(const NKikimrConfig::TConveyorConfig& config);
    ui32 GetWorkersCountForConveyor(const ui32 poolThreadsCount) const;
    double GetWorkerCPUUsage(const ui32 workerIdx) const;
    TString DebugString() const;
};

}
