#pragma once
#include "kqp_compute_actor.h"
#include "kqp_compute_events.h"
#include "kqp_compute_state.h"
#include "kqp_scan_common.h"
#include "kqp_scan_compute_manager.h"
#include "kqp_scan_events.h"

#include <ydb/core/base/events.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/runtime/kqp_scan_data_meta.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

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
    const TString Database;
    const NYql::NDq::TTxId TxId;
    const TMaybe<ui64> LockTxId;
    const ui32 LockNodeId;
    const TMaybe<NKikimrDataEvents::ELockMode> LockMode;
    const TCPULimits CPULimits;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SCAN_FETCH_ACTOR;
    }

    TKqpScanFetcherActor(const NKikimrKqp::TKqpSnapshot& snapshot, const NYql::NDq::TComputeRuntimeSettings& settings,
        std::vector<NActors::TActorId>&& computeActors, const ui64 txId, const TMaybe<ui64> lockTxId, const ui32 lockNodeId,
        const TMaybe<NKikimrDataEvents::ELockMode> lockMode, const TString& database,
        const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta, const TShardsScanningPolicy& shardsScanningPolicy,
        TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId, const TCPULimits& cpuLimits);

    static TVector<TSerializedTableRange> BuildSerializedTableRanges(
        const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta& readData);

    void Bootstrap();

    STATEFN(StateFunc) {
        NActors::TLogContextGuard lGuard =
            NActors::TLogContextBuilder::Build()("self_id", SelfId())("scan_id", ScanId)("tx_id", std::get<ui64>(TxId));
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
                hFunc(NActors::TEvents::TEvWakeup, HandleExecute);
                hFunc(NActors::NMon::TEvHttpInfo, OnMonitoringPage)
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

    void HandleExecute(NActors::TEvents::TEvWakeup::TPtr& ev);

private:
    void CheckFinish();
    ui32 GetShardsInProgressCount() const;

    std::vector<NActors::TActorId> ComputeActorIds;

    void StopOnError(const TString& errorMessage) const;
    bool SendGlobalFail(
        const NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssuesIds::EIssueCode issueCode, const TString& message) const;

    bool SendGlobalFail(
        const NYql::NDqProto::EComputeState state, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues) const;

    bool SendScanFinished();

    virtual std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> BuildEvKqpScan(const ui32 scanId, const ui32 gen,
        const TSmallVec<TSerializedTableRange>& ranges, const std::optional<NKikimrKqp::TEvKqpScanCursor>& cursor) const override;
    virtual const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const override {
        return KeyColumnTypes;
    }

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

    void OnMonitoringPage(NActors::NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        const auto elapsed = TInstant::Now() - RegistrationStartTime;
        const double elapsedSec = elapsed.SecondsFloat();

        HTML(str) {
            PRE() {
                str << "TKqpScanFetcherActor, SelfId=" << SelfId() << Endl;
                str << "ScanId: " << ScanId << ", TxId: " << std::get<ui64>(TxId) << Endl;
                str << "Elapsed: " << elapsed << Endl;
                str << "PendingScanData: " << PendingScanData.size()
                    << ", PendingShards: " << PendingShards.size()
                    << ", PendingResolveShards: " << PendingResolveShards.size() << Endl;
                str << "InFlightShards(Scans/Shards): " << InFlightShards.GetScansCount()
                    << "/" << InFlightShards.GetShardsCount()
                    << ", PacksToSendCount: " << InFlightComputes.GetPacksToSendCount() << Endl;
                str << "BlocksReceived: " << BlocksReceived
                    << ", TotalBytesReceived: " << TotalBytesReceived << Endl;
                if (BlocksReceived > 0) {
                    str << "AvgBlockSize: " << (TotalBytesReceived / BlocksReceived) << " bytes" << Endl;
                }
                if (elapsedSec > 0) {
                    str << "Throughput: " << (ui64)(TotalBytesReceived / elapsedSec) << " bytes/sec" << Endl;
                }
            }

            str << Endl << "Compute Actor(s):" << Endl;
            TABLE_SORTABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH_ATTRS({{"title", "Compute actor receiving data from this fetcher"}}) { str << "ActorId"; }
                        TABLEH_ATTRS({{"title", "Last FreeSpace value reported by compute actor"}}) { str << "FreeSpace"; }
                        TABLEH_ATTRS({{"title", "Total chunks sent to compute actor"}}) { str << "DataChunksSent"; }
                        TABLEH_ATTRS({{"title", "Accepted acks (only when compute was not free)"}}) { str << "AcksReceived"; }
                        TABLEH_ATTRS({{"title", "DataChunksSent minus AcksReceived: events stuck in compute mailbox"}}) { str << "Sent-Acked"; }
                        TABLEH_ATTRS({{"title", "All acks from compute including ones ignored because compute was already free"}}) { str << "TotalAcksFromCompute"; }
                        TABLEH_ATTRS({{"title", "Packs queued locally, waiting for ack before sending"}}) { str << "DataQueue"; }
                    }
                }
                TABLEBODY() {
                    InFlightComputes.ForEachCompute([&](const TActorId& actorId, const TInFlightComputes::TComputeActorInfo& info) {
                        TABLER() {
                            TABLED() {
                                HREF(ActorLink(actorId)) { str << actorId; }
                            }
                            TABLED() { str << info.GetFreeSpace(); }
                            TABLED() { str << info.GetDataChunksSent(); }
                            TABLED() { str << info.GetAcksReceived(); }
                            TABLED() { str << (i64)info.GetDataChunksSent() - (i64)info.GetAcksReceived(); }
                            TABLED() { str << info.GetTotalAcksFromCompute(); }
                            TABLED() { str << info.GetPacksToSendCount(); }
                        }
                    });
                }
            }

            str << Endl << "Shard Scanner(s):" << Endl;
            TABLE_SORTABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH_ATTRS({{"title", "DataShard tablet serving this key range"}}) { str << "TabletId"; }
                        TABLEH_ATTRS({{"title", "Scan actor on the shard side"}}) { str << "ActorId"; }
                        TABLEH_ATTRS({{"title", "Cumulative time chunks waited in queue for a free compute actor"}}) { str << "WaitOutputTime"; }
                        TABLEH_ATTRS({{"title", "Shard has finished scanning its key range"}}) { str << "Finished"; }
                    }
                }
                TABLEBODY() {
                    InFlightShards.ForEachScanner([&](ui64 tabletId, const TShardScannerInfo& scanner) {
                        TABLER() {
                            TABLED() {
                                HREF(TabletLink(tabletId)) { str << tabletId; }
                            }
                            TABLED() {
                                if (scanner.HasActorId()) {
                                    HREF(ActorLink(scanner.GetActorId())) { str << scanner.GetActorIdStr(); }
                                } else {
                                    str << "none";
                                }
                            }
                            TABLED() { str << scanner.GetWaitOutputTime(); }
                            TABLED() { str << scanner.IsFinished(); }
                        }
                    });
                }
            }
        }
        this->Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(str.Str()));
    }

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
    const NKqp::ETableKind TableKind = NKqp::ETableKind::Unknown;

    std::set<ui32> TrackingNodes;
    ui32 MaxInFlight = 1024;
    bool IsAggregationRequest = false;
    bool RegistrationFinished = false;
    TInstant RegistrationStartTime;

    ui64 BlocksReceived = 0;
    ui64 TotalBytesReceived = 0;
};

}   // namespace NKikimr::NKqp::NScanPrivate
