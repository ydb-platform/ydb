#pragma once

#include "target_base.h"

#include <ydb/core/protos/replication.pb.h>

#include <library/cpp/sliding_window/sliding_window.h>

namespace NKikimr::NReplication::NController {

using TMetricsConfig = NKikimrProto::NMetricsConfig::TMetricsConfig;

extern const TString ReplicationConsumerName;

class TTargetWithStreamStats: public TTargetBaseStats {
protected:
    struct TMultiSlidingWindow {
        NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>> Minute;
        NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>> Hour;

        TMultiSlidingWindow()
            : Minute(TDuration::Minutes(1), 100)
            , Hour(TDuration::Hours(1), 100)
        {
        }

        void Add(ui64 value) {
            Minute.Update(value, Now());
            Hour.Update(value, Now());
        }

        void ToProto(auto& destination, ui64 multiplier) const {
            destination.mutable_avg_per_minute()->set_seconds(Minute.GetValue() * multiplier);
            destination.mutable_avg_per_hour()->set_seconds(Hour.GetValue() * multiplier);
        }
    };


public:
    TTargetWithStreamStats(TInstant startTime);

    bool UpdateWithSingleStatsItem(ui64 workerId, ui64 key, i64 value) override;
    void RemoveWorker(ui64 workerId) override;

    void Serialize(NKikimrReplication::TEvDescribeReplicationResult& destination, bool detailed) const override;

public:
    TMultiSlidingWindow ReadBytes;
    TMultiSlidingWindow ReadMessages;
    TMultiSlidingWindow WriteBytes;
    TMultiSlidingWindow WriteRows;
    TMultiSlidingWindow DecompressionCpuTime;
    TInstant CollectionStartTime;
};

struct TTragetWithStreamCounters {
    NMonitoring::TDynamicCounterPtr CountersGroup;

    NMonitoring::TDynamicCounters::TCounterPtr ReadTime;
    NMonitoring::TDynamicCounters::TCounterPtr WriteTime;
    NMonitoring::TDynamicCounters::TCounterPtr DecompressionCpuTime;
    NMonitoring::TDynamicCounters::TCounterPtr WriteBytes;
    NMonitoring::TDynamicCounters::TCounterPtr WriteRows;
    NMonitoring::TDynamicCounters::TCounterPtr WriteErrors;

    virtual bool UpdateWithSingleStatsItem(ui64 workerId, ui64 key, i64 value);
    virtual ~TTragetWithStreamCounters() = default;
};

class TTargetWithStream: public TTargetBase {
public:
    template <typename... Args>
    explicit TTargetWithStream(Args&&... args)
        : TTargetBase(std::forward<Args>(args)...)
        , Location(GetLocation())
    {
        SetStreamState(EStreamState::Creating);
    }

    void Progress(const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

    void WorkerStatusChanged(ui64 workerId, ui64 status) override;
    void UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& newStats) override;

    const TReplication::ITargetStats* GetStats() const override;

    IActor* CreateWorkerRegistar(const TActorContext& ctx) const override;

protected:
    NKikimrReplication::TReplicationLocationConfig Location;
    std::unique_ptr<TTargetWithStreamStats> Stats;
    std::unique_ptr<TTragetWithStreamCounters> Counters;

private:
    bool NameAssignmentInProcess = false;
    TActorId StreamCreator;
    TActorId StreamRemover;

}; // TTargetWithStream

}
