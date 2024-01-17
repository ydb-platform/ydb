#pragma once

#include "schema.h"

#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/counters_sysview_processor.pb.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/db_counters.h>
#include <ydb/core/sys_view/service/query_interval.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/util/memory_track.h>

namespace NKikimr {
namespace NSysView {

static constexpr char MemoryLabelResults[] = "SysViewProcessor/Results";

class TSysViewProcessor : public TActor<TSysViewProcessor>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SYSTEM_VIEW_PROCESSOR;
    }

    TSysViewProcessor(const NActors::TActorId& tablet, TTabletStorageInfo* info, EProcessorMode processorMode);

private:
    using Schema = TProcessorSchema;
    using TTxBase = NTabletFlatExecutor::TTransactionBase<TSysViewProcessor>;

    struct TTxInitSchema;
    struct TTxInit;
    struct TTxConfigure;
    struct TTxCollect;
    struct TTxAggregate;
    struct TTxIntervalSummary;
    struct TTxIntervalMetrics;
    struct TTxTopPartitions;

    struct TEvPrivate {
        enum EEv {
            EvCollect = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvAggregate,
            EvSendRequests,
            EvProcess,
            EvApplyCounters,
            EvApplyLabeledCounters,
            EvSendNavigate,
            EvEnd
        };

        struct TEvCollect : public TEventLocal<TEvCollect, EvCollect> {};

        struct TEvAggregate : public TEventLocal<TEvAggregate, EvAggregate> {};

        struct TEvSendRequests : public TEventLocal<TEvSendRequests, EvSendRequests> {};

        struct TEvProcess : public TEventLocal<TEvProcess, EvProcess> {};

        struct TEvApplyCounters : public TEventLocal<TEvApplyCounters, EvApplyCounters> {};

        struct TEvApplyLabeledCounters : public TEventLocal<TEvApplyLabeledCounters, EvApplyLabeledCounters> {};

        struct TEvSendNavigate : public TEventLocal<TEvSendNavigate, EvSendNavigate> {};
    };

    struct TTopQuery {
        TQueryHash Hash;
        ui64 Value;
        TNodeId NodeId;
        THolder<NKikimrSysView::TQueryStats> Stats;
    };
    using TQueryTop = std::vector<TTopQuery>;

    using TPartitionTop = std::vector<THolder<NKikimrSysView::TTopPartitionsInfo>>;

    using THistoryKey = std::pair<ui64, ui32>;

    template <typename TEntry>
    using TResultMap = std::map<THistoryKey, TEntry, std::less<THistoryKey>,
        NActors::NMemory::TAlloc<std::pair<const THistoryKey, TEntry>, MemoryLabelResults>>;

    using TResultStatsMap = TResultMap<NKikimrSysView::TQueryStats>;

    using TResultPartitionsMap = TResultMap<NKikimrSysView::TTopPartitionsInfo>;

    struct TQueryToMetrics {
        NKikimrSysView::TQueryMetrics Metrics;
        TString Text;
    };

private:
    static bool TopQueryCompare(const TTopQuery& l, const TTopQuery& r) {
        return l.Value == r.Value ? l.Hash > r.Hash : l.Value > r.Value;
    }

    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override;

    NTabletFlatExecutor::ITransaction* CreateTxInitSchema();
    NTabletFlatExecutor::ITransaction* CreateTxInit();

    void Handle(TEvSysView::TEvConfigureProcessor::TPtr& ev);
    void Handle(TEvPrivate::TEvCollect::TPtr& ev);
    void Handle(TEvPrivate::TEvAggregate::TPtr& ev);
    void Handle(TEvPrivate::TEvSendRequests::TPtr& ev);
    void Handle(TEvPrivate::TEvProcess::TPtr& ev);
    void Handle(TEvSysView::TEvIntervalQuerySummary::TPtr& ev);
    void Handle(TEvSysView::TEvGetIntervalMetricsResponse::TPtr& ev);
    void Handle(TEvSysView::TEvSendTopPartitions::TPtr& ev);

    void Handle(TEvSysView::TEvGetQueryMetricsRequest::TPtr& ev);
    void Handle(TEvSysView::TEvGetTopPartitionsRequest::TPtr& ev);

    void Handle(TEvSysView::TEvSendDbCountersRequest::TPtr& ev);
    void Handle(TEvSysView::TEvSendDbLabeledCountersRequest::TPtr& ev);
    void Handle(TEvPrivate::TEvApplyCounters::TPtr& ev);
    void Handle(TEvPrivate::TEvApplyLabeledCounters::TPtr& ev);
    void Handle(TEvPrivate::TEvSendNavigate::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev);

    void Handle(TEvents::TEvUndelivered::TPtr& ev);
    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);

    void PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value);
    void PersistDatabase(NIceDb::TNiceDb& db);
    void PersistStage(NIceDb::TNiceDb& db);
    void PersistIntervalEnd(NIceDb::TNiceDb& db);

    template <typename TSchema>
    void PersistQueryTopResults(NIceDb::TNiceDb& db,
        TQueryTop& top, TResultStatsMap& results, TInstant intervalEnd);
    void PersistQueryResults(NIceDb::TNiceDb& db);

    template <typename TSchema>
    void PersistPartitionTopResults(NIceDb::TNiceDb& db,
        TPartitionTop& top, TResultPartitionsMap& results, TInstant intervalEnd);
    void PersistPartitionResults(NIceDb::TNiceDb& db);

    void ScheduleAggregate();
    void ScheduleCollect();
    void ScheduleSendRequests();
    void ScheduleApplyCounters();
    void ScheduleApplyLabeledCounters();
    void ScheduleSendNavigate();

    template <typename TSchema, typename TMap>
    void CutHistory(NIceDb::TNiceDb& db, TMap& results, TDuration historySize);

    static TInstant EndOfHourInterval(TInstant intervalEnd);

    void ClearIntervalSummaries(NIceDb::TNiceDb& db);

    void Reset(NIceDb::TNiceDb& db, const TActorContext& ctx);

    void SendRequests();
    void IgnoreFailure(TNodeId nodeId);

    static void EntryToProto(NKikimrSysView::TQueryMetricsEntry& dst, const TQueryToMetrics& src);
    static void EntryToProto(NKikimrSysView::TQueryStatsEntry& dst, const NKikimrSysView::TQueryStats& src);
    static void EntryToProto(NKikimrSysView::TTopPartitionsEntry& dst, const NKikimrSysView::TTopPartitionsInfo& src);

    template <typename TResponse>
    void ReplyOverloaded(const NActors::TActorId& sender);

    template <typename TMap, typename TRequest, typename TResponse>
    void Reply(typename TRequest::TPtr& ev);


    TIntrusivePtr<IDbCounters> CreateCountersForService(NKikimrSysView::EDbCountersService service);
    void AttachExternalCounters();
    void AttachInternalCounters();
    void DetachExternalCounters();
    void DetachInternalCounters();
    void SendNavigate();

    STFUNC(StateInit) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSysView::TEvConfigureProcessor, Handle);
            IgnoreFunc(TEvSysView::TEvIntervalQuerySummary);
            IgnoreFunc(TEvSysView::TEvGetIntervalMetricsResponse);
            IgnoreFunc(TEvSysView::TEvGetQueryMetricsRequest);
            IgnoreFunc(TEvSysView::TEvSendTopPartitions);
            IgnoreFunc(TEvSysView::TEvGetTopPartitionsRequest);
            IgnoreFunc(TEvSysView::TEvSendDbCountersRequest);
            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                        "TSysViewProcessor StateInit unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
                }
        }
    }

    STFUNC(StateOffline) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSysView::TEvConfigureProcessor, Handle);
            IgnoreFunc(TEvSysView::TEvIntervalQuerySummary);
            IgnoreFunc(TEvSysView::TEvGetIntervalMetricsResponse);
            IgnoreFunc(TEvSysView::TEvGetQueryMetricsRequest);
            IgnoreFunc(TEvSysView::TEvSendTopPartitions);
            IgnoreFunc(TEvSysView::TEvGetTopPartitionsRequest);
            IgnoreFunc(TEvSysView::TEvSendDbCountersRequest);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                        "TSysViewProcessor StateOffline unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
                }
        }
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSysView::TEvConfigureProcessor, Handle);
            hFunc(TEvPrivate::TEvCollect, Handle);
            hFunc(TEvPrivate::TEvAggregate, Handle);
            hFunc(TEvPrivate::TEvSendRequests, Handle);
            hFunc(TEvPrivate::TEvProcess, Handle);
            hFunc(TEvSysView::TEvIntervalQuerySummary, Handle);
            hFunc(TEvSysView::TEvGetIntervalMetricsResponse, Handle);
            hFunc(TEvSysView::TEvGetQueryMetricsRequest, Handle);
            hFunc(TEvSysView::TEvSendTopPartitions, Handle);
            hFunc(TEvSysView::TEvGetTopPartitionsRequest, Handle);
            hFunc(TEvSysView::TEvSendDbCountersRequest, Handle);
            hFunc(TEvSysView::TEvSendDbLabeledCountersRequest, Handle);
            hFunc(TEvPrivate::TEvApplyCounters, Handle);
            hFunc(TEvPrivate::TEvApplyLabeledCounters, Handle);
            hFunc(TEvPrivate::TEvSendNavigate, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    LOG_CRIT(*TlsActivationContext  , NKikimrServices::SYSTEM_VIEWS,
                        "TSysViewProcessor StateWork unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
                }
        }
    }

private:
    // limit on number of distinct queries when gathering summaries
    static constexpr size_t DistinctQueriesLimit = 1024;
    // limit on number of queries to aggregate metrics
    static constexpr size_t TopCountLimit = 256;
    // limit on number of concurrent metrics requests from services
    static constexpr size_t MaxInFlightRequests = 16;
    // limit on scan batch size
    static constexpr size_t BatchSizeLimit = 4 << 20;
    // interval of db counters processing
    static constexpr TDuration ProcessCountersInterval = TDuration::Seconds(5);
    // interval of db labeled counters processing
    static constexpr TDuration ProcessLabeledCountersInterval = TDuration::Seconds(60);
    // interval of sending next navigate request
    static constexpr TDuration SendNavigateInterval = TDuration::Seconds(5);

    const TDuration TotalInterval;
    const TDuration CollectInterval;

    TString Database;

    TInstant IntervalEnd;

    enum EStage {
        COLLECT,
        AGGREGATE
    };
    EStage CurrentStage = COLLECT;

    // IntervalSummaries
    struct TQueryToNodes {
        ui64 Cpu = 0;
        std::vector<std::pair<TNodeId, ui64>> Nodes; // nodeId, cpu
    };
    std::unordered_map<TQueryHash, TQueryToNodes> Queries;
    std::multimap<ui64, TQueryHash> ByCpu;
    std::unordered_set<TNodeId> SummaryNodes;

    // IntervalMetrics
    std::unordered_map<TQueryHash, TQueryToMetrics> QueryMetrics;

    // NodesToRequest
    using THashVector = std::vector<TQueryHash>;
    struct TNodeToQueries {
        TNodeId NodeId = 0;
        THashVector Hashes;
        THashVector TextsToGet;
        THashVector ByDuration;
        THashVector ByReadBytes;
        THashVector ByCpuTime;
        THashVector ByRequestUnits;
    };
    std::vector<TNodeToQueries> NodesToRequest;
    std::unordered_map<TNodeId, TNodeToQueries> NodesInFlight;

    // IntervalTops
    TQueryTop ByDurationMinute;
    TQueryTop ByReadBytesMinute;
    TQueryTop ByCpuTimeMinute;
    TQueryTop ByRequestUnitsMinute;
    TQueryTop ByDurationHour;
    TQueryTop ByReadBytesHour;
    TQueryTop ByCpuTimeHour;
    TQueryTop ByRequestUnitsHour;

    // Metrics...
    using TResultMetricsMap = TResultMap<TQueryToMetrics>;

    TResultMetricsMap MetricsOneMinute;
    TResultMetricsMap MetricsOneHour;

    // TopBy...
    TResultStatsMap TopByDurationOneMinute;
    TResultStatsMap TopByDurationOneHour;
    TResultStatsMap TopByReadBytesOneMinute;
    TResultStatsMap TopByReadBytesOneHour;
    TResultStatsMap TopByCpuTimeOneMinute;
    TResultStatsMap TopByCpuTimeOneHour;
    TResultStatsMap TopByRequestUnitsOneMinute;
    TResultStatsMap TopByRequestUnitsOneHour;

    // IntervalPartitionTops
    TPartitionTop PartitionTopMinute;
    TPartitionTop PartitionTopHour;

    // TopPartitions...
    TResultPartitionsMap TopPartitionsOneMinute;
    TResultPartitionsMap TopPartitionsOneHour;

    // limited queue of user requests
    static constexpr size_t PendingRequestsLimit = 5;

    using TVariantRequestPtr = std::variant<
        TEvSysView::TEvGetQueryMetricsRequest::TPtr,
        TEvSysView::TEvGetTopPartitionsRequest::TPtr>;

    std::queue<TVariantRequestPtr> PendingRequests;
    bool ProcessInFly = false;

    // db counters
    TString CloudId;
    TString FolderId;
    TString DatabaseId;

    ::NMonitoring::TDynamicCounterPtr ExternalGroup;
    ::NMonitoring::TDynamicCounterPtr LabeledGroup;
    std::unordered_map<TString, ::NMonitoring::TDynamicCounterPtr> InternalGroups;

    using TDbCountersServiceMap = std::unordered_map<NKikimrSysView::EDbCountersService,
        NKikimr::NSysView::TDbServiceCounters>;

    struct TNodeCountersState {
        TDbCountersServiceMap Simple; // only simple counters
        ui64 Generation = 0;
        size_t FreshCount = 0;
    };
    std::unordered_map<TNodeId, TNodeCountersState> NodeCountersStates;
    std::unordered_map<TNodeId, TNodeCountersState> NodeLabeledCountersStates;
    TDbCountersServiceMap AggregatedCountersState;
    TDbCountersServiceMap AggregatedLabeledState;

    std::unordered_map<NKikimrSysView::EDbCountersService, TIntrusivePtr<IDbCounters>> Counters;
};

} // NSysView
} // NKikimr

