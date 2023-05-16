#pragma once
#include "kqp_compute_events.h"
#include "kqp_compute_actor.h"
#include "kqp_scan_events.h"
#include "kqp_compute_state.h"
#include "kqp_scan_compute_manager.h"
#include "kqp_scan_common.h"

#include <ydb/core/base/events.h>

#include <ydb/core/kqp/runtime/kqp_scan_data_meta.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/wilson/wilson_trace.h>
#include <library/cpp/actors/core/interconnect.h>

namespace NKikimr::NKqp::NScanPrivate {

class TKqpScanFetcherActor: public NActors::TActorBootstrapped<TKqpScanFetcherActor> {
private:
    using TBase = NActors::TActorBootstrapped<TKqpScanFetcherActor>;
    struct TEvPrivate {
        enum EEv {
            EvRetryShard = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvRetryShard: public TEventLocal<TEvRetryShard, EvRetryShard> {
        private:
            explicit TEvRetryShard(const ui64 tabletId)
                : TabletId(tabletId) {
            }
        public:
            ui64 TabletId = 0;
            ui32 Generation = 0;

            TEvRetryShard(const ui64 tabletId, const ui32 generation)
                : TabletId(tabletId)
                , Generation(generation) {
            }
        };
    };
    NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta Meta;
    const NMiniKQL::TScanDataMetaFull ScanDataMeta;
    const NYql::NDq::TComputeRuntimeSettings RuntimeSettings;
    const NYql::NDq::TTxId TxId;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SCAN_COMPUTE_ACTOR;
    }

    TKqpScanFetcherActor(const NKikimrKqp::TKqpSnapshot& snapshot, const NYql::NDq::TComputeRuntimeSettings& settings,
        std::vector<NActors::TActorId>&& computeActors, const ui64 txId,
        const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta,
        const TShardsScanningPolicy& shardsScanningPolicy, TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId);

    static TVector<TSerializedTableRange> BuildSerializedTableRanges(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta& readData);

    void Bootstrap();

    STATEFN(StateFunc) {
        auto gTimeFull = KqpComputeActorSpan.StartStackTimeGuard("processing");
        auto gTime = KqpComputeActorSpan.StartStackTimeGuard("event_" + ::ToString(ev->GetTypeRewrite()));
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpCompute::TEvScanInitActor, HandleExecute);
                hFunc(TEvKqpCompute::TEvScanData, HandleExecute);
                hFunc(TEvKqpCompute::TEvScanError, HandleExecute);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleExecute);
                hFunc(TEvPrivate::TEvRetryShard, HandleExecute);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleExecute);
                hFunc(TEvents::TEvUndelivered, HandleExecute);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleExecute);
                hFunc(TEvScanExchange::TEvTerminateFromCompute, HandleExecute);
                hFunc(TEvScanExchange::TEvAckData, HandleExecute);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
                default:
                    StopOnError("unexpected message on data fetching: " + ::ToString(ev->GetTypeRewrite()));
            }
        } catch (...) {
            StopOnError("unexpected exception: " + CurrentExceptionMessage());
        }
    }

    void HandleExecute(TEvScanExchange::TEvAckData::TPtr& ev);

    void HandleExecute(TEvScanExchange::TEvTerminateFromCompute::TPtr& ev);

private:

    std::vector<NActors::TActorId> ComputeActorIds;

    void StopOnError(const TString& errorMessage) const;
    bool SendGlobalFail(const NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssuesIds::EIssueCode issueCode, const TString& message) const;

    bool SendGlobalFail(const NYql::NDqProto::EComputeState state, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues) const;

    bool ProvideDataToCompute(TEvKqpCompute::TEvScanData& msg, TShardState::TPtr state);

    bool SendScanFinished();

    THolder<TEvDataShard::TEvKqpScan> BuildEvKqpScan(const ui32 scanId, const ui32 gen, const TSmallVec<TSerializedTableRange>& ranges) const;

    void HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev);

    void HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev);

    void ProcessPendingScanDataItem(TEvKqpCompute::TEvScanData::TPtr& ev, const TInstant& enqueuedAt);

    void ProcessScanData();

    void HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev);

    void HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);

    void HandleExecute(TEvPrivate::TEvRetryShard::TPtr& ev);

    void HandleExecute(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);

    void HandleExecute(TEvents::TEvUndelivered::TPtr& ev);

    void HandleExecute(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);

private:

    bool StartTableScan();

    void StartReadShard(TShardState::TPtr state);

    bool SendScanDataAck(TShardState::TPtr state);

    void SendStartScanRequest(TShardState::TPtr state, ui32 gen);

    void RetryDeliveryProblem(TShardState::TPtr state);

    void TerminateExpiredScan(const TActorId& actorId, TStringBuf msg);

    void TerminateChunk(TShardState::TPtr sState) {
        if (!sState->ActorId) {
            return;
        }
        auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Cancel non actual scan");
        Send(sState->ActorId, abortEv.Release());
    }

    void ResolveNextShard() {
        if (!PendingResolveShards.empty()) {
            auto& state = PendingResolveShards.front();
            ResolveShard(state);
        }
    }

    void EnqueueResolveShard(TShardState::TPtr state);

    void DoAckAvailableWaiting();

    bool StopReadChunk(const TShardState& state);

    void ResolveShard(TShardState& state);

private:
    void PassAway() override {
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        for (ui32 nodeId : TrackingNodes) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }

        TBase::PassAway();
    }

    template<class TMessage>
    TShardState::TPtr GetShardState(const TMessage& msg, const TActorId& scanActorId) {
        if (!InFlightShards.IsActive()) {
            return nullptr;
        }
        ui32 generation;
        if constexpr (std::is_same_v<TMessage, NKikimrKqp::TEvScanError>) {
            generation = msg.GetGeneration();
        } else if constexpr (std::is_same_v<TMessage, NKikimrKqp::TEvScanInitActor>) {
            generation = msg.GetGeneration();
        } else {
            generation = msg.Generation;
        }
        return GetShardStateByGeneration(generation, scanActorId);
    }

    TShardState::TPtr GetShardStateByGeneration(const ui32 generation, const TActorId& scanActorId);

private:
    TString LogPrefix;
    NKikimrKqp::TKqpSnapshot Snapshot;
    TShardsScanningPolicy ShardsScanningPolicy;
    TIntrusivePtr<TKqpCounters> Counters;
    TScannedDataStats Stats;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    std::deque<std::pair<TEvKqpCompute::TEvScanData::TPtr, TInstant>> PendingScanData;
    std::deque<TShardState> PendingShards;
    std::deque<TShardState> PendingResolveShards;

    NWilson::TProfileSpan KqpComputeActorSpan;
    TInFlightShards InFlightShards;
    TInFlightComputes InFlightComputes;
    ui32 ScansCounter = 0;
    ui32 TotalRetries = 0;

    std::set<ui32> TrackingNodes;
    ui32 MaxInFlight = 1024;
    bool IsAggregationRequest = false;
};

}
