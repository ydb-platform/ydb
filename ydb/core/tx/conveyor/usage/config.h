#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

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

class TCPULimitsConfig {
    YDB_OPT(double, CPUGroupThreadsLimit);
    YDB_READONLY_DEF(TString, CPUGroupName);
public:
    TCPULimitsConfig() = default;
    TCPULimitsConfig(const double cpuGroupThreadsLimit, const TString& cpuGroupName);

    TConclusionStatus DeserializeFromProto(const NKikimrTxDataShard::TEvKqpScan& config);
    TString DebugString() const;
};

}
