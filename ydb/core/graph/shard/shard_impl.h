#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/graph/shard/protos/counters_shard.pb.h>
#include <ydb/core/graph/api/events.h>
#include <ydb/core/cms/console/console.h>
#include "backends.h"

namespace NKikimr {
namespace NGraph {

class TGraphShard : public TActor<TGraphShard>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    TGraphShard(TTabletStorageInfo* info, const TActorId& tablet);
    TString GetLogPrefix() const;

    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext&) override {}
    void OnDetach(const TActorContext&) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext&) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;
    void OnReadyToWork();
    void ApplyConfig(const NKikimrConfig::TGraphConfig& config);
    static void MergeMetrics(TMetricsData& data, const NKikimrGraph::TEvSendMetrics& src);
    static void MergeHistogram(TMetricsData& data, const NKikimrGraph::THistogramMetric& src);
    static void MergeArithmetic(TMetricsData& data, const NKikimrGraph::TArithmeticMetric& src);
    static void AggregateMetrics(TMetricsData& data);
    static void AggregateHistogram(std::unordered_map<TString, double>& values, const TString& name, const std::map<ui64, ui64>& histogram);

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev);
    void Handle(TEvSubDomain::TEvConfigure::TPtr& ev);
    void Handle(TEvGraph::TEvSendMetrics::TPtr& ev);
    void Handle(TEvGraph::TEvGetMetrics::TPtr& ev);
    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);

//protected:
    std::vector<TAggregateSettings> AggregateSettings = { // proper ordering is important
        {.PeriodToStart = TDuration::Days(7)}, // clear
        {.PeriodToStart = TDuration::Hours(24), .SampleSize = TDuration::Seconds(60)}, // downsample
        {.PeriodToStart = TDuration::Hours(1), .SampleSize = TDuration::Seconds(10)} // downsample
    };

    void ExecuteTxInitSchema();
    void ExecuteTxStartup();
    void ExecuteTxMonitoring(NMon::TEvRemoteHttpInfo::TPtr ev);
    void ExecuteTxStoreMetrics(TMetricsData&& data);
    void ExecuteTxAggregateData(const TAggregateSettings& settings);
    void ExecuteTxGetMetrics(TEvGraph::TEvGetMetrics::TPtr ev);
    void ExecuteTxChangeBackend(EBackendType backend);

    STATEFN(StateWork);

    TDuration AggregateCheckPeriod = TDuration::Minutes(1); // how often we could issue an aggregate operation
    TMetricsData MetricsData; // current accumulated metrics, ready to be stored
    TInstant AggregateTimestamp; // last time of aggregate operation
    EBackendType BackendType = EBackendType::Memory;
    TMemoryBackend MemoryBackend;
    TLocalBackend LocalBackend;
};

} // NGraph
} // NKikimr
