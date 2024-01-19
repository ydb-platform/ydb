#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/graph/shard/protos/counters_shard.pb.h>
#include <ydb/core/graph/api/events.h>
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

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev);
    void Handle(TEvSubDomain::TEvConfigure::TPtr& ev);
    void Handle(TEvGraph::TEvSendMetrics::TPtr& ev);
    void Handle(TEvGraph::TEvGetMetrics::TPtr& ev);

//protected:
    void ExecuteTxInitSchema();
    void ExecuteTxStartup();
    void ExecuteTxMonitoring(NMon::TEvRemoteHttpInfo::TPtr ev);
    void ExecuteTxStoreMetrics(TMetricsData&& data);
    void ExecuteTxClearData();
    void ExecuteTxGetMetrics(TEvGraph::TEvGetMetrics::TPtr ev);
    void ExecuteTxChangeBackend(EBackendType backend);

    STATEFN(StateWork);

    // how often we could issue a clear operation
    static constexpr TDuration DURATION_CLEAR_PERIOD = TDuration::Minutes(1);
    // after what size of metrics data we issue a clear operation
    static constexpr TDuration DURATION_CLEAR_TRIGGER = TDuration::Hours(25);
    // the maximum size of metrics data to keep
    static constexpr TDuration DURATION_TO_KEEP = TDuration::Hours(24);

    TMetricsData MetricsData; // current accumulated metrics, ready to be stored
    TInstant StartTimestamp; // the earliest point of metrics
    TInstant ClearTimestamp; // last time of clear operation
    EBackendType BackendType = EBackendType::Memory;
    TMemoryBackend MemoryBackend;
    TLocalBackend LocalBackend;
};

} // NGraph
} // NKikimr
