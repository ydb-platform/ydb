#pragma once

#include "target_with_stream.h"
#include <ydb/core/tx/replication/service/worker.h>
#include <library/cpp/sliding_window/sliding_window.h>

#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

class TTransferStats : public TReplication::ITargetStats {
    struct TMultiSlidingWindow {
        NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>> Minute;
        NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>> Hour;

        TMultiSlidingWindow()
            : Minute(TDuration::Minutes(1), 100)
            , Hour(TDuration::Hours(1), 100)
        {
        }
    };
    struct TWorkserStats {
        NKikimrReplication::EWorkOperation Operation;
        TInstant LastChange = TInstant::Zero();
        ui32 ReadOffset;
        ui32 Partition;
        TDuration Uptime = TDuration::Zero();
        ui32 RestartsCount;
        TMultiSlidingWindow Restarts;
        TMultiSlidingWindow ReadBytes;
        TMultiSlidingWindow ReadMessages;
        TMultiSlidingWindow WriteBytes;
        TMultiSlidingWindow WriteRows;
        TMultiSlidingWindow DecompressionCpuTime;
        TMultiSlidingWindow ProcessingCpuTime;
        ui64 ReadTime = 0;
        ui64 ReadCpu = 0;
        ui64 DecompressTime = 0;
        ui64 DecompressCpu = 0;
        ui64 ProcessingTime = 0;
        ui64 ProcessingCpu = 0;
        ui64 WriteTime = 0;
        ui64 WriteCpu = 0;
    };

public:
    void FillToProto(NKikimrReplication::TEvDescribeReplicationResult& destination, bool includeDetailed) const override;

    THashMap<ui64, TWorkserStats> WorkersStats;
    TDuration MinUptime = TDuration::Zero();
    TMultiSlidingWindow ReadBytes;
    TMultiSlidingWindow ReadMessages;
    TMultiSlidingWindow WriteBytes;
    TMultiSlidingWindow WriteRows;
    TMultiSlidingWindow DecompressionCpuTime;
    TMultiSlidingWindow ProcessingCpuTime;
    TInstant StartTime;
    ui64 ReadTime = 0;
    ui64 ReadCpu = 0;
    ui64 DecompressTime = 0;
    ui64 DecompressCpu = 0;
    ui64 ProcessingTime = 0;
    ui64 ProcessingCpu = 0;
    ui64 WriteTime = 0;
    ui64 WriteCpu = 0;

    TTransferStats(TInstant startTime)
        : StartTime(startTime)
    {
    }
};

class TTargetTransfer: public TTargetWithStream {
public:
    struct TTransferConfig : public TConfigBase {
        using TPtr = std::shared_ptr<TTransferConfig>;

        TTransferConfig(const TString& srcPath, const TString& dstPath, const TString& transformLambda, const TString& runAsUser, const TString& directoryPath);

        const TString& GetTransformLambda() const;
        const TString& GetRunAsUser() const;
        const TString& GetDirectoryPath() const;
        ui64 GetCountersLevel() const override;

    private:
        TString TransformLambda;
        TString RunAsUser;
        TString DirectoryPath;
        ui64 CountersLevel = 2; //ToDo - get level via YQL
    };

    explicit TTargetTransfer(TReplication* replication,
        ui64 id, const IConfig::TPtr& config);

    void UpdateConfig(const NKikimrReplication::TReplicationConfig&) override;

    void Progress(const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

    TString GetStreamPath() const override;
    void UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& stats, NMonitoring::TDynamicCounterPtr counters) override;
    const TReplication::ITargetStats* GetStats() const override;

private:
    TActorId StreamConsumerRemover;
    std::unique_ptr<TTransferStats> Stats;
    NMonitoring::TDynamicCounterPtr DynamicCounters;
};
}
