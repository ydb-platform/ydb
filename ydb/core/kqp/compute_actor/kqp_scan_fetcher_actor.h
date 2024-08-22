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

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr::NKqp::NScanPrivate {

class TKqpScanFetcherActor: public NActors::TActorBootstrapped<TKqpScanFetcherActor>, public IExternalObjectsProvider {
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
        return NKikimrServices::TActivity::KQP_SCAN_FETCH_ACTOR;
    }

    TKqpScanFetcherActor(const NKikimrKqp::TKqpSnapshot& snapshot, const NYql::NDq::TComputeRuntimeSettings& settings,
        std::vector<NActors::TActorId>&& computeActors, const ui64 txId,
        const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta,
        const TShardsScanningPolicy& shardsScanningPolicy, TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId);

    static TVector<TSerializedTableRange> BuildSerializedTableRanges(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta& readData);

    void Bootstrap();

    STATEFN(StateFunc) {
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
                    AFL_VERIFY_DEBUG(false)("event_type", ev->GetTypeName());
                    StopOnError("unexpected message on data fetching: " + ev->GetTypeName());
            }
        } catch (...) {
            StopOnError("unexpected exception: " + CurrentExceptionMessage());
        }
    }

    void HandleExecute(TEvScanExchange::TEvAckData::TPtr& ev);

    void HandleExecute(TEvScanExchange::TEvTerminateFromCompute::TPtr& ev);

private:

    void CheckFinish();
    ui32 GetShardsInProgressCount() const;

    std::vector<NActors::TActorId> ComputeActorIds;

    void StopOnError(const TString& errorMessage) const;
    bool SendGlobalFail(const NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssuesIds::EIssueCode issueCode, const TString& message) const;

    bool SendGlobalFail(const NYql::NDqProto::EComputeState state, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues) const;

    bool SendScanFinished();

    virtual std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> BuildEvKqpScan(const ui32 scanId, const ui32 gen, const TSmallVec<TSerializedTableRange>& ranges) const override;
    virtual const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const override {
        return KeyColumnTypes;
    }


    void HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev);

    void HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev);

    void ProcessPendingScanDataItem(TEvKqpCompute::TEvScanData::TPtr& ev, const TInstant& enqueuedAt) noexcept;

    void ProcessScanData();

    void HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev);

    void HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);

    void HandleExecute(TEvPrivate::TEvRetryShard::TPtr& ev);

    void HandleExecute(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);

    void HandleExecute(TEvents::TEvUndelivered::TPtr& ev);

    void HandleExecute(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);

private:

    void StartTableScan();

    void RetryDeliveryProblem(TShardState::TPtr state);

    void ResolveNextShard() {
        if (!PendingResolveShards.empty()) {
            auto& state = PendingResolveShards.front();
            ResolveShard(state);
        }
    }

    void EnqueueResolveShard(const TShardState::TPtr& state);

    void DoAckAvailableWaiting();

    void ResolveShard(TShardState& state);

private:
    void PassAway() override {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

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

    static inline TAtomicCounter ScanIdCounter = 0;
    const ui64 ScanId = ScanIdCounter.Inc();

    TInFlightShards InFlightShards;
    TInFlightComputes InFlightComputes;
    ui32 TotalRetries = 0;

    std::set<ui32> TrackingNodes;
    ui32 MaxInFlight = 1024;
    bool IsAggregationRequest = false;
};

}
