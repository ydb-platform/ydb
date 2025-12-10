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
        TInstant StartTime = TInstant::Zero();
        TInstant ChangeStateTime = TInstant::Zero();
        ui32 RestartsCount;
        TMultiSlidingWindow Restarts;
        TMultiSlidingWindow ReadBytes;
        TMultiSlidingWindow ReadMessages;
        TMultiSlidingWindow WriteBytes;
        TMultiSlidingWindow WriteRows;
        TMultiSlidingWindow DecompressionCpuTime;
        TMultiSlidingWindow ProcessingCpuTime;
    };

public:
    void FillToProto(NKikimrReplication::TEvDescribeReplicationResult& destination, bool includeDetailed) const override;

    THashMap<ui64, TWorkserStats> WorkersStats;
    TMultiSlidingWindow ReadBytes;
    TMultiSlidingWindow ReadMessages;
    TMultiSlidingWindow WriteBytes;
    TMultiSlidingWindow WriteRows;
    TMultiSlidingWindow DecompressionCpuTime;
    TMultiSlidingWindow ProcessingCpuTime;
    TInstant CollectionStartTime;
    TInstant LastWorkerStartTime = TInstant::Zero();

    TTransferStats(TInstant startTime)
        : CollectionStartTime(startTime)
    {
    }
};

class TTargetTransfer: public TTargetWithStream {
    using TBase = TTargetWithStream;

public: struct TTransferConfig: public TConfigBase {
        using TPtr = std::shared_ptr<TTransferConfig>;

        TTransferConfig(const TString& srcPath, const TString& dstPath, const TString& transformLambda, const TString& runAsUser, const TString& directoryPath);

        const TString& GetTransformLambda() const;
        const TString& GetRunAsUser() const;
        const TString& GetDirectoryPath() const;

    private:
        TString TransformLambda;
        TString RunAsUser;
        TString DirectoryPath;
    };

    explicit TTargetTransfer(TReplication* replication,
        ui64 id, const IConfig::TPtr& config);

    void UpdateConfig(const NKikimrReplication::TReplicationConfig&) override;

    void Progress(const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

    TString GetStreamPath() const override;
    void UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& stats, NMonitoring::TDynamicCounterPtr counters) override;
    const TReplication::ITargetStats* GetStats() const override;
    void RemoveWorker(ui64 id) override;

private:
    struct TCounters {
        NMonitoring::TDynamicCounterPtr AggeregatedCounters;

        NMonitoring::TDynamicCounters::TCounterPtr ReadTime;
        NMonitoring::TDynamicCounters::TCounterPtr WriteTime;
        NMonitoring::TDynamicCounters::TCounterPtr DecompressionCpuTime;
        NMonitoring::TDynamicCounters::TCounterPtr ProcessingCpuTime;
        NMonitoring::TDynamicCounters::TCounterPtr WriteBytes;
        NMonitoring::TDynamicCounters::TCounterPtr WriteRows;
        NMonitoring::TDynamicCounters::TCounterPtr WriteErrors;
        NMonitoring::TDynamicCounters::TCounterPtr MinWorkerUptime;
        NMonitoring::TDynamicCounters::TCounterPtr Restarts;

        TCounters(NMonitoring::TDynamicCounterPtr counters, const TString& transferId)
            : AggeregatedCounters(counters->GetSubgroup("counters", "transfer")->GetSubgroup("host", "")
                                          ->GetSubgroup("transfer_id", transferId))
            , ReadTime(AggeregatedCounters->GetCounter("transfer.read.duration_milliseconds", true))
            , WriteTime(AggeregatedCounters->GetCounter("transfer.write.duration_milliseconds", true))
            , DecompressionCpuTime(AggeregatedCounters->GetCounter("transfer.decompress.cpu_elapsed_microseconds", true))
            , ProcessingCpuTime(AggeregatedCounters->GetCounter("transfer.process.cpu_elapsed_microseconds", true))
            , WriteBytes(AggeregatedCounters->GetCounter("transfer.write_bytes", true))
            , WriteRows(AggeregatedCounters->GetCounter("transfer.write_rows", true))
            , WriteErrors(AggeregatedCounters->GetCounter("transfer.write_errors", true))
            , MinWorkerUptime(AggeregatedCounters->GetCounter("transfer.worker_uptime_milliseconds_min", false))
            , Restarts(AggeregatedCounters->GetCounter("transfer.worker_restarts", true))
        {
        }
    };
    TMaybe<TCounters> Counters;

    TActorId StreamConsumerRemover;
    std::unique_ptr<TTransferStats> Stats;
    ui64 MetricsLevel = 0;
    TString Name;
};
}
