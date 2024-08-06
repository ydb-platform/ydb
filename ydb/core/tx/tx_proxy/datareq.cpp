#include "proxy.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/balance_coverage/balance_coverage_builder.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/base/row_version.h>

#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/queue.h>

#ifndef KIKIMR_DATAREQ_WATCHDOG_PERIOD
#define KIKIMR_DATAREQ_WATCHDOG_PERIOD 10000
#endif

namespace NKikimr {
namespace NTxProxy {

const ui32 MaxDatashardProgramSize = 48 * 1024 * 1024; // 48 MB
const ui32 ShardCancelDeadlineShiftSec = 60;

namespace {
    static constexpr TDuration MinReattachDelay = TDuration::MilliSeconds(10);
    static constexpr TDuration MaxReattachDelay = TDuration::MilliSeconds(100);
    static constexpr TDuration MaxReattachDuration = TDuration::Seconds(4);
}

struct TFlatMKQLRequest : public TThrRefBase {
    TAutoPtr<NMiniKQL::IEngineFlat> Engine;
    ui64 LockTxId;
    bool NeedDiagnostics;
    bool LlvmRuntime;
    bool CollectStats;
    bool ReadOnlyProgram;
    TMaybe<ui64> PerShardKeysSizeLimitBytes;
    NKikimrTxUserProxy::TMiniKQLTransaction::TLimits Limits;
    TRowVersion Snapshot = TRowVersion::Min();

    TMap<ui64, TAutoPtr<TBalanceCoverageBuilder>> BalanceCoverageBuilders;

    NMiniKQL::IEngineFlat::EResult EngineResultStatusCode;
    NMiniKQL::IEngineFlat::EStatus EngineResponseStatus;
    TString EngineResponse;
    NKikimrMiniKQL::TResult EngineEvaluatedResponse;

    TFlatMKQLRequest()
        : LockTxId(0)
        , NeedDiagnostics(false)
        , LlvmRuntime(false)
        , CollectStats(false)
        , ReadOnlyProgram(false)
        , EngineResultStatusCode(NMiniKQL::IEngineFlat::EResult::Unknown)
        , EngineResponseStatus(NMiniKQL::IEngineFlat::EStatus::Unknown)
    {}

    void RequestAdditionResults(ui64 txId) {
        auto lockTxId = Engine->GetLockTxId();
        if (lockTxId) {
            LockTxId = *lockTxId ? *lockTxId : txId;
            Y_ABORT_UNLESS(LockTxId);
        }

        NeedDiagnostics = Engine->HasDiagnosticsRequest();
    }
};

// Class used to merge key ranges and arrange shards for ordered scans.
class TKeySpace {
public:
    TKeySpace();

    void Initialize(bool ordered, TConstArrayRef<NScheme::TTypeInfo> keyTypes, const TTableRange &range);

    const TVector<NScheme::TTypeInfo> &GetKeyTypes() { return KeyTypes; }

    void AddRange(const NKikimrTx::TKeyRange &range, ui64 shard);
    bool IsFull() const;

    const TQueue<ui64> &GetShardsQueue() const { return ShardsQueue; }
    bool IsShardsQueueEmpty() const { return ShardsQueue.empty(); }
    ui64 ShardsQueueFront() const { return ShardsQueue.front(); }
    void ShardsQueuePop() { ShardsQueue.pop(); }
    void FlushShardsToQueue();

    const TSerializedTableRange &GetSpace() const { return SpaceRange; }

private:
    class TRange : public TSerializedTableRange {
    public:
        TRange() = default;
        TRange(const TRange &other) = default;

        TRange(const NKikimrTx::TKeyRange &range)
            : TSerializedTableRange(range)
        {}

        TRange &operator=(const TRange &other) = default;

        TVector<ui64> Shards;
    };
    using TRanges = TList<TRange>;

    bool IsGreater(const TConstArrayRef<TCell> &lhs, const TConstArrayRef<TCell> &rhs) const;
    void TryToMergeRange(TRanges::iterator it);
    bool TryToMergeWithPrev(TRanges::iterator it);

    TVector<NScheme::TTypeInfo> KeyTypes;
    TSerializedTableRange SpaceRange;
    TRanges Ranges;
    bool OrderedQueue;
    TQueue<ui64> ShardsQueue;
    TSerializedCellVec QueuePoint;
};

struct TReadTableRequest : public TThrRefBase {
    struct TQuotaRequest {
        TActorId Sender;
        ui64 ShardId;
    };

    TString TablePath;
    TTableId TableId;
    bool Ordered;
    bool AllowDuplicates;
    TVector<TTableColumnInfo> Columns;
    NKikimrTxUserProxy::TKeyRange Range;
    TString ResponseData;
    ui64 ResponseDataFrom;
    TKeySpace KeySpace;
    THashMap<ui64, TActorId> ClearanceSenders;
    THashMap<ui64, TActorId> StreamingShards;
    TSerializedCellVec FromValues;
    TSerializedCellVec ToValues;
    THolder<TKeyDesc> KeyDesc;
    bool RowsLimited;
    ui64 RowsRemain;
    THashMap<ui64, TQuotaRequest> QuotaRequests;
    ui64 QuotaRequestId;
    ui32 RequestVersion;
    ui32 ResponseVersion = NKikimrTxUserProxy::TReadTableTransaction::UNSPECIFIED;
    TRowVersion Snapshot = TRowVersion::Max();

    TReadTableRequest(const NKikimrTxUserProxy::TReadTableTransaction &tx)
        : TablePath(tx.GetPath())
        , Ordered(tx.GetOrdered())
        , AllowDuplicates(tx.GetAllowDuplicates())
        , Range(tx.GetKeyRange())
        , RowsLimited(tx.GetRowLimit() > 0)
        , RowsRemain(tx.GetRowLimit())
        , QuotaRequestId(1)
        , RequestVersion(tx.HasApiVersion() ? tx.GetApiVersion() : (ui32)NKikimrTxUserProxy::TReadTableTransaction::UNSPECIFIED)
    {
        for (auto &col : tx.GetColumns()) {
            Columns.emplace_back(col, 0, NScheme::TTypeInfo(0));
        }

        if (tx.HasSnapshotStep() && tx.HasSnapshotTxId()) {
            Snapshot.Step = tx.GetSnapshotStep();
            Snapshot.TxId = tx.GetSnapshotTxId();
        }
    }
};

class TDataReq : public TActor<TDataReq> {
public:
    enum class EParseRangeKeyExp {
        NONE,
        TO_NULL
    };

    enum class ECoordinatorStatus {
        Unknown,
        Waiting,
        Planned,
        Declined,
    };

    struct TReattachState {
        TDuration Delay;
        TInstant Deadline;
        ui64 Cookie = 0;
        bool Reattaching = false;

        bool ShouldReattach(TInstant now) {
            ++Cookie; // invalidate any previous cookie

            if (!Reattaching) {
                Deadline = now + MaxReattachDuration;
                Delay = TDuration::Zero();
                Reattaching = true;
                return true;
            }

            TDuration left = Deadline - now;
            if (!left) {
                Reattaching = false;
                return false;
            }

            Delay *= 2.0;
            if (Delay < MinReattachDelay) {
                Delay = MinReattachDelay;
            } else if (Delay > MaxReattachDelay) {
                Delay = MaxReattachDelay;
            }

            // Add ±10% jitter
            Delay *= 0.9 + 0.2 * TAppData::RandomProvider->GenRandReal4();
            if (Delay > left) {
                Delay = left;
            }

            return true;
        }

        void Reattached() {
            Reattaching = false;
        }
    };

    struct TPerTablet {
        enum {
            AffectedRead = 1 << 0,
            AffectedWrite = 1 << 1,
        };

#define DATA_REQ_PER_TABLET_STATUS_MAP(XX) \
            XX(StatusUnknown, 128) \
            XX(StatusWait, 0) \
            XX(StatusPrepared, 1) \
            XX(StatusError, 2) \
            XX(StatusFinished, 3)

        enum class ETabletStatus {
            DATA_REQ_PER_TABLET_STATUS_MAP(ENUM_VALUE_GEN)
        };

        ui64 MinStep = 0;
        ui64 MaxStep = 0;
        ui32 AffectedFlags = 0;
        ETabletStatus TabletStatus = ETabletStatus::StatusUnknown;
        ui64 ReadSize = 0;
        ui64 ReplySize = 0;
        ui64 ProgramSize = 0;
        ui64 IncomingReadSetsSize = 0;
        ui64 OutgoingReadSetsSize = 0;
        bool StreamCleared = false;
        bool Restarting = false;
        size_t RestartCount = 0;
        TTableId TableId;
        THolder<NKikimrQueryStats::TTxStats> Stats;
        TReattachState ReattachState;
    };
private:

    struct TEvPrivate {
        enum EEv {
            EvProxyDataReqOngoingTransactionsWatchdog = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvReattachToShard,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvProxyDataReqOngoingTransactionsWatchdog : public TEventLocal<TEvProxyDataReqOngoingTransactionsWatchdog, EvProxyDataReqOngoingTransactionsWatchdog> {};

        struct TEvReattachToShard : public TEventLocal<TEvReattachToShard, EvReattachToShard> {
            const ui64 TabletId;

            TEvReattachToShard(ui64 tabletId)
                : TabletId(tabletId)
            { }
        };
    };

    const TTxProxyServices Services;
    const ui64 TxId;
    ui64 SelectedCoordinator;

    ui64 ProxyFlags;
    TDuration ExecTimeoutPeriod;
    TDuration CancelAfter;
    TSchedulerCookieHolder ExecTimeoutCookieHolder;

    TActorId RequestSource;
    ui32 TxFlags;
    bool CanUseFollower;
    bool StreamResponse;

    TString DatabaseName;
    TIntrusivePtr<TFlatMKQLRequest> FlatMKQLRequest;
    TIntrusivePtr<TReadTableRequest> ReadTableRequest;
    TString DatashardErrors;
    TVector<ui64> ComplainingDatashards;
    TVector<TString> UnresolvedKeys;
    TAutoPtr<const NACLib::TUserToken> UserToken;

    THashMap<ui64, TPerTablet> PerTablet;
    NYql::TIssueManager IssueManager;
    TTablePathHashSet InvalidatedTables;
    ui64 TabletsLeft; // todo: add scale modifiers
    ui64 TabletErrors;

    ui64 AggrMinStep;
    ui64 AggrMaxStep;

    ui64 PlanStep;
    ECoordinatorStatus CoordinatorStatus = ECoordinatorStatus::Unknown;

    ui64 ResultsReceivedCount;
    ui64 ResultsReceivedSize;

    TRequestControls RequestControls;

    TInstant WallClockAccepted;
    TInstant WallClockResolveStarted;
    TInstant WallClockResolved;
    TInstant WallClockAfterBuild;
    TInstant WallClockPrepared;
    TInstant WallClockPlanned;

    TDuration ElapsedPrepareExec;
    TDuration ElapsedExecExec;
    TDuration ElapsedPrepareComplete;
    TDuration ElapsedExecComplete;

    TInstant WallClockFirstPrepareReply;
    TInstant WallClockLastPrepareReply;

    TInstant WallClockMinPrepareArrive;
    TInstant WallClockMaxPrepareArrive;
    TDuration WallClockMinPrepareComplete;
    TDuration WallClockMaxPrepareComplete;

    TInstant WallClockFirstExecReply;
    TInstant WallClockLastExecReply;

    TDuration CpuTime;

    TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> PrepareFlatMKQLRequest(TStringBuf miniKQLProgram, TStringBuf miniKQLParams, const TActorContext &ctx);
    void ProcessFlatMKQLResolve(NSchemeCache::TSchemeCacheRequest *cacheRequest, const TActorContext &ctx);
    void ContinueFlatMKQLResolve(const TActorContext &ctx);
    void ProcessReadTableResolve(NSchemeCache::TSchemeCacheRequest *cacheRequest, const TActorContext &ctx);

    TIntrusivePtr<TTxProxyMon> TxProxyMon;

    void Die(const TActorContext &ctx) override {
        --*TxProxyMon->DataReqInFly;

        Send(Services.FollowerPipeCache, new TEvPipeCache::TEvUnlink(0));
        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));

        ProcessStreamClearance(false, ctx);
        if (ReadTableRequest) {
            for (auto &tr : ReadTableRequest->StreamingShards) {
                ctx.Send(tr.second, new TEvTxProcessing::TEvInterruptTransaction(TxId));
            }
        }

        TActor::Die(ctx);
    }

    static TInstant Now() {
        return AppData()->TimeProvider->Now();
    }

    void ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, NKikimrIssues::TStatusIds::EStatusCode code, bool reportIssues, const TActorContext &ctx);
    void MarkShardError(ui64 tabletId, TDataReq::TPerTablet &perTablet, bool invalidateDistCache, const TActorContext &ctx);
    void TryToInvalidateTable(TTableId tableId, const TActorContext &ctx);

    void RegisterPlan(const TActorContext &ctx);
    void MergeResult(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx);
    void MakeFlatMKQLResponse(const TActorContext &ctx, const NCpuTime::TCpuTimer& timer);

    void ProcessStreamResponseData(TEvDataShard::TEvProposeTransactionResult::TPtr &ev,
                                   const TActorContext &ctx);
    void FinishShardStream(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx);
    void FinishStreamResponse(const TActorContext &ctx);

    ui64 SelectCoordinator(NSchemeCache::TSchemeCacheRequest &cacheRequest, const TActorContext &ctx);
    const TDomainsInfo::TDomain& SelectDomain(NSchemeCache::TSchemeCacheRequest &cacheRequest, const TActorContext &ctx);

    void HandleWatchdog(const TActorContext &ctx);

    void HandleUndeliveredResolve(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvTxProxyReq::TEvMakeRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvProposeTransactionRestart::TPtr &ev, const TActorContext &ctx);
    void HandlePrepare(TEvDataShard::TEvProposeTransactionAttachResult::TPtr &ev, const TActorContext &ctx);
    void HandlePrepare(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx);
    void HandlePrepareErrors(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx);
    void HandlePrepareErrorTimeout(const TActorContext &ctx);
    void HandlePlan(TEvDataShard::TEvProposeTransactionAttachResult::TPtr &ev, const TActorContext &ctx);
    void HandlePlan(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvGetReadTableSinkStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProxy::TEvProposeTransactionStatus::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvStreamClearanceRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvStreamIsDead::TPtr &ev, const TActorContext &ctx);
    void HandleResolve(TEvTxProcessing::TEvStreamIsDead::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvStreamQuotaRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvStreamQuotaResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvStreamQuotaRelease::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvPrivate::TEvReattachToShard::TPtr &ev, const TActorContext &ctx);
    void HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx);
    void HandlePrepareErrors(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx);
    void HandlePlan(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx);
    void HandleExecTimeout(const TActorContext &ctx);
    void HandleExecTimeoutResolve(const TActorContext &ctx);

    void ExtractDatashardErrors(const NKikimrTxDataShard::TEvProposeTransactionResult & record);
    void CancelProposal(ui64 exceptTablet);
    void FailProposedRequest(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, TString errMsg, const TActorContext &ctx);

    void SendStreamClearanceResponse(ui64 shard, bool cleared, const TActorContext &ctx);
    void ProcessNextStreamClearance(bool cleared, const TActorContext &ctx);
    void ProcessStreamClearance(bool cleared, const TActorContext &ctx);

    bool ParseRangeKey(const NKikimrMiniKQL::TParams &proto,
                          TConstArrayRef<NScheme::TTypeInfo> keyType,
                          TSerializedCellVec &buf,
                          EParseRangeKeyExp exp);

    bool CheckDomainLocality(NSchemeCache::TSchemeCacheRequest &cacheRequest);
    void BuildTxStats(NKikimrQueryStats::TTxStats& stats);
    bool IsReadOnlyRequest() const;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_REQ_PROXY;
    }

    TDataReq(const TTxProxyServices &services, ui64 txid, const TIntrusivePtr<TTxProxyMon> mon,
             const TRequestControls& requestControls)
        : TActor(&TThis::StateWaitInit)
        , Services(services)
        , TxId(txid)
        , SelectedCoordinator(0)
        , ProxyFlags(0)
        , TxFlags(0)
        , CanUseFollower(true)
        , TabletsLeft(0)
        , TabletErrors(0)
        , AggrMinStep(0)
        , AggrMaxStep(Max<ui64>())
        , PlanStep(0)
        , ResultsReceivedCount(0)
        , ResultsReceivedSize(0)
        , RequestControls(requestControls)
        , WallClockAccepted(TInstant::MicroSeconds(0))
        , WallClockResolveStarted(TInstant::MicroSeconds(0))
        , WallClockResolved(TInstant::MicroSeconds(0))
        , WallClockPrepared(TInstant::MicroSeconds(0))
        , WallClockPlanned(TInstant::MicroSeconds(0))
        , TxProxyMon(mon)
    {
        ++*TxProxyMon->DataReqInFly;
    }

    STFUNC(StateWaitInit) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvTxProxyReq::TEvMakeRequest, Handle);
        }
    }

    STFUNC(StateWaitResolve) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvTxProcessing::TEvStreamIsDead, HandleResolve);
            HFuncTraced(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFuncTraced(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            HFuncTraced(TEvents::TEvUndelivered, HandleUndeliveredResolve); // we must wait for resolve completion
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeoutResolve); // we must wait for resolve completion to keep key description
            CFunc(TEvPrivate::EvProxyDataReqOngoingTransactionsWatchdog, HandleWatchdog);
        }
    }

    STFUNC(StateWaitSnapshot) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamIsDead, Handle);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
            CFunc(TEvPrivate::EvProxyDataReqOngoingTransactionsWatchdog, HandleWatchdog);
        }
    }

    // resolve zombie state to keep shared key desc
    STFUNC(StateResolveTimeout) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType, Die);
            CFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult::EventType, Die);
        }
    }

    STFUNC(StateWaitPrepare) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvDataShard::TEvGetReadTableSinkStateRequest, Handle);
            HFuncTraced(TEvDataShard::TEvGetReadTableStreamStateRequest, Handle);
            HFuncTraced(TEvDataShard::TEvProposeTransactionResult, HandlePrepare);
            HFuncTraced(TEvDataShard::TEvProposeTransactionRestart, Handle);
            HFuncTraced(TEvDataShard::TEvProposeTransactionAttachResult, HandlePrepare);
            HFuncTraced(TEvTxProcessing::TEvStreamClearanceRequest, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamIsDead, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamQuotaRequest, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamQuotaResponse, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamQuotaRelease, Handle);
            HFuncTraced(TEvPipeCache::TEvDeliveryProblem, HandlePrepare);
            HFuncTraced(TEvPrivate::TEvReattachToShard, Handle);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
            CFunc(TEvPrivate::EvProxyDataReqOngoingTransactionsWatchdog, HandleWatchdog);
        }
    }

    STFUNC(StatePrepareErrors) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvDataShard::TEvProposeTransactionResult, HandlePrepareErrors);
            HFuncTraced(TEvTxProcessing::TEvStreamIsDead, Handle);
            HFuncTraced(TEvPipeCache::TEvDeliveryProblem, HandlePrepareErrors);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandlePrepareErrorTimeout);
        }
    }

    STFUNC(StateWaitPlan) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvDataShard::TEvGetReadTableSinkStateRequest, Handle);
            HFuncTraced(TEvDataShard::TEvGetReadTableStreamStateRequest, Handle);
            HFuncTraced(TEvTxProxy::TEvProposeTransactionStatus, Handle);
            HFuncTraced(TEvDataShard::TEvProposeTransactionResult, HandlePlan);
            HFuncTraced(TEvDataShard::TEvProposeTransactionRestart, Handle);
            HFuncTraced(TEvDataShard::TEvProposeTransactionAttachResult, HandlePlan);
            HFuncTraced(TEvTxProcessing::TEvStreamClearanceRequest, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamIsDead, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamQuotaRequest, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamQuotaResponse, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamQuotaRelease, Handle);
            HFuncTraced(TEvPipeCache::TEvDeliveryProblem, HandlePlan);
            HFuncTraced(TEvPrivate::TEvReattachToShard, Handle);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
            CFunc(TEvPrivate::EvProxyDataReqOngoingTransactionsWatchdog, HandleWatchdog);
        }
    }
};

TKeySpace::TKeySpace()
    : OrderedQueue(false)
{
}

void TKeySpace::Initialize(bool ordered,
                           TConstArrayRef<NScheme::TTypeInfo> keyTypes,
                           const TTableRange &range)
{
    SpaceRange.From = TSerializedCellVec(range.From);
    SpaceRange.FromInclusive = range.InclusiveFrom;
    SpaceRange.To = TSerializedCellVec(range.To);
    SpaceRange.ToInclusive = range.InclusiveTo;

    // +INF should not be included
    if (SpaceRange.To.GetCells().empty())
        SpaceRange.ToInclusive = false;

    OrderedQueue = ordered;
    KeyTypes.assign(keyTypes.begin(), keyTypes.end());
    QueuePoint = SpaceRange.From;
}

bool TKeySpace::IsGreater(const TConstArrayRef<TCell> &lhs, const TConstArrayRef<TCell> &rhs) const
{
    return CompareBorders<true, true>(lhs, rhs, true, true, KeyTypes) > 0;
}

void TKeySpace::AddRange(const NKikimrTx::TKeyRange &range, ui64 shard)
{
    TRange newRange(range);
    newRange.Shards.push_back(shard);

    const auto &fromCells = newRange.From.GetCells();

    TRanges::iterator it = Ranges.begin();
    for ( ; ; ++it) {
        if (it == Ranges.end()
            || IsGreater(it->From.GetCells(), fromCells)) {
            it = Ranges.insert(it, std::move(newRange));
            break;
        }
    }

    // Advance shards queue.
    if (OrderedQueue) {
        auto point = it;
        while (point != Ranges.end()
               && !IsGreater(point->From.GetCells(), QueuePoint.GetCells())
               && IsGreater(point->To.GetCells(), QueuePoint.GetCells())) {
            for (auto s : point->Shards)
                ShardsQueue.push(s);
            QueuePoint = point->To;
            ++point;
        }
    } else {
        ShardsQueue.push(shard);
    }

    TryToMergeRange(it);
}

bool TKeySpace::IsFull() const
{
    if (Ranges.size() != 1)
        return false;

    auto &range = Ranges.front();

    Y_DEBUG_ABORT_UNLESS(range.FromInclusive);
    Y_DEBUG_ABORT_UNLESS(!range.ToInclusive);

    if (IsGreater(range.From.GetCells(), SpaceRange.From.GetCells()))
        return false;

    if (IsGreater(SpaceRange.To.GetCells(), range.To.GetCells()))
        return false;
    else if (SpaceRange.To.GetBuffer() == range.To.GetBuffer()
             && SpaceRange.ToInclusive)
        return false;

    Y_DEBUG_ABORT_UNLESS(!OrderedQueue || !IsGreater(SpaceRange.To.GetCells(), QueuePoint.GetCells()));

    return true;
}

void TKeySpace::FlushShardsToQueue()
{
    if (!OrderedQueue)
        return;

    TRanges::iterator it;

    for (it = Ranges.begin(); it != Ranges.end(); ++it) {
        if (IsGreater(it->From.GetCells(), QueuePoint.GetCells()))
            break;
    }

    while (it != Ranges.end()) {
        for (auto s : it->Shards)
            ShardsQueue.push(s);
        ++it;
    }
}

void TKeySpace::TryToMergeRange(TRanges::iterator it)
{
    while (TryToMergeWithPrev(it)) {
    }
    TryToMergeWithPrev(++it);
}

bool TKeySpace::TryToMergeWithPrev(TRanges::iterator it)
{
    if (it == Ranges.begin() || it == Ranges.end())
        return false;

    auto prev = it;
    --prev;

    const auto &toCells = prev->To.GetCells();
    const auto &fromCells = it->From.GetCells();

    // Check range is covered by the prev.
    if (!IsGreater(it->To.GetCells(), toCells)) {
        *it = std::move(*prev);
        Ranges.erase(prev);
        return true;
    }

    if (toCells.size() != fromCells.size())
        return false;

    if (CompareTypedCellVectors(toCells.data(), fromCells.data(), KeyTypes.data(), toCells.size()) == 0
        && (it->FromInclusive || it->ToInclusive)) {
        it->FromInclusive = prev->FromInclusive;
        it->From = prev->From;
        it->Shards.insert(it->Shards.begin(), prev->Shards.begin(), prev->Shards.end());
        Ranges.erase(prev);
        return true;
    }

    return false;
}

void TDataReq::ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, NKikimrIssues::TStatusIds::EStatusCode code, bool reportIssues, const TActorContext &ctx) {
    auto *x = new TEvTxUserProxy::TEvProposeTransactionStatus(status);
    x->Record.SetTxId(TxId);

    if (reportIssues && IssueManager.GetIssues()) {
        IssuesToMessage(IssueManager.GetIssues(), x->Record.MutableIssues());
        IssueManager.Reset();
    }

    x->Record.SetStatusCode(code);

    for (auto& unresolvedKey : UnresolvedKeys) {
        x->Record.AddUnresolvedKeys(unresolvedKey);
    }

    if (PlanStep)
        x->Record.SetStep(PlanStep);

    if (FlatMKQLRequest) {
        x->Record.SetExecutionEngineStatus((ui32)FlatMKQLRequest->EngineResultStatusCode);
        if (FlatMKQLRequest->EngineResponseStatus != NMiniKQL::IEngineFlat::EStatus::Unknown)
            x->Record.SetExecutionEngineResponseStatus((ui32)FlatMKQLRequest->EngineResponseStatus);
        if (!FlatMKQLRequest->EngineResponse.empty())
            x->Record.SetExecutionEngineResponse(FlatMKQLRequest->EngineResponse);
        x->Record.MutableExecutionEngineEvaluatedResponse()->Swap(&FlatMKQLRequest->EngineEvaluatedResponse);
        if (FlatMKQLRequest->Engine) {
            auto errors = FlatMKQLRequest->Engine->GetErrors();
            if (!errors.empty()) {
                x->Record.SetMiniKQLErrors(errors);
            }
        }
    }

    if (ReadTableRequest) {
        x->Record.SetSerializedReadTableResponse(ReadTableRequest->ResponseData);
        x->Record.SetDataShardTabletId(ReadTableRequest->ResponseDataFrom);
        x->Record.SetReadTableResponseVersion(ReadTableRequest->ResponseVersion);
    }

    if (!DatashardErrors.empty())
        x->Record.SetDataShardErrors(DatashardErrors);

    if (const ui32 cs = ComplainingDatashards.size()) {
        x->Record.MutableComplainingDataShards()->Reserve(cs);
        for (auto ds : ComplainingDatashards)
            x->Record.AddComplainingDataShards(ds);
    }

    if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyTrackWallClock) {
        auto *timings = x->Record.MutableTimings();
        if (WallClockAccepted.GetValue())
            timings->SetWallClockAccepted(WallClockAccepted.MicroSeconds());
        if (WallClockResolved.GetValue())
            timings->SetWallClockResolved(WallClockResolved.MicroSeconds());
        if (WallClockPrepared.GetValue())
            timings->SetWallClockPrepared(WallClockPrepared.MicroSeconds());
        if (WallClockPlanned.GetValue())
            timings->SetWallClockPlanned(WallClockPlanned.MicroSeconds());
        if (ElapsedExecExec.GetValue())
            timings->SetElapsedExecExec(ElapsedExecExec.MicroSeconds());
        if (ElapsedExecComplete.GetValue())
            timings->SetElapsedExecComplete(ElapsedExecComplete.MicroSeconds());
        if (ElapsedPrepareExec.GetValue())
            timings->SetElapsedPrepareExec(ElapsedExecExec.MicroSeconds());
        if (ElapsedPrepareComplete.GetValue())
            timings->SetElapsedPrepareComplete(ElapsedExecComplete.MicroSeconds());
        timings->SetWallClockNow(Now().MicroSeconds());
    }

    TInstant wallClockEnd = Now();
    TDuration prepareTime = WallClockPrepared.GetValue() ? WallClockPrepared - WallClockAccepted : TDuration::Zero();
    TDuration executeTime = WallClockPrepared.GetValue() ? wallClockEnd - WallClockPrepared : wallClockEnd - WallClockAccepted;
    TDuration totalTime = wallClockEnd - WallClockAccepted;

    (*TxProxyMon->ResultsReceivedCount) += ResultsReceivedCount;
    (*TxProxyMon->ResultsReceivedSize) += ResultsReceivedSize;

    auto fnGetTableIdByShard = [&](ui64 shard) -> TString {
        const auto* e = PerTablet.FindPtr(shard);
        if (!e)
            return "";
        return Sprintf("%" PRIu64 "/%" PRIu64, e->TableId.PathId.OwnerId, e->TableId.PathId.LocalPathId);
    };

    if (FlatMKQLRequest && FlatMKQLRequest->CollectStats) {
        auto* stats = x->Record.MutableTxStats();
        BuildTxStats(*stats);
        stats->SetDurationUs(totalTime.MicroSeconds());
    }

    switch (status) {
    case TEvTxUserProxy::TResultStatus::ProxyAccepted:
    case TEvTxUserProxy::TResultStatus::ProxyResolved:
    case TEvTxUserProxy::TResultStatus::ProxyPrepared:
    case TEvTxUserProxy::TResultStatus::CoordinatorPlanned:
    case TEvTxUserProxy::TResultStatus::ExecComplete:
    case TEvTxUserProxy::TResultStatus::ExecAborted:
    case TEvTxUserProxy::TResultStatus::ExecAlready:
        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_INFO, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " RESPONSE Status# " << TEvTxUserProxy::TResultStatus::Str(status)
            << "  prepare time: " << prepareTime.ToString()
            << "  execute time: " << executeTime.ToString()
            << "  total time: " << totalTime.ToString()
            << "  marker# P13");

        TxProxyMon->ReportStatusOK->Inc();

        TxProxyMon->TxPrepareTimeHgram->Collect(prepareTime.MilliSeconds());
        TxProxyMon->TxExecuteTimeHgram->Collect(executeTime.MilliSeconds());
        TxProxyMon->TxTotalTimeHgram->Collect(totalTime.MilliSeconds());

        if (PerTablet.size() > 1) {
            if (WallClockFirstPrepareReply.GetValue()) {
                TxProxyMon->TxPrepareSpreadHgram->Collect((WallClockLastPrepareReply - WallClockFirstPrepareReply).MilliSeconds());
                TxProxyMon->TxPrepareArriveSpreadHgram->Collect((WallClockMaxPrepareArrive - WallClockMinPrepareArrive).MilliSeconds());
                TxProxyMon->TxPrepareCompleteSpreadHgram->Collect((WallClockMaxPrepareComplete - WallClockMinPrepareComplete).MilliSeconds());
            }
            if (WallClockFirstExecReply.GetValue()) {
                TxProxyMon->TxExecSpreadHgram->Collect((WallClockLastExecReply - WallClockFirstExecReply).MilliSeconds());
            }
        }
        break;
    case TEvTxUserProxy::TResultStatus::ProxyShardTryLater:
    case TEvTxUserProxy::TResultStatus::ProxyShardOverloaded:
        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_NOTICE, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " RESPONSE Status# " << TEvTxUserProxy::TResultStatus::Str(status)
            << " shard: " << ComplainingDatashards.front()
            << " table: " << fnGetTableIdByShard(ComplainingDatashards.front())
            << "  marker# P13a");
        TxProxyMon->ReportStatusNotOK->Inc();
        break;
    case TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable:
    case TEvTxUserProxy::TResultStatus::ProxyShardUnknown:
        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_INFO, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " RESPONSE Status# " << TEvTxUserProxy::TResultStatus::Str(status)
            << " shard: " << (ComplainingDatashards ? ComplainingDatashards.front() : 0)
            << "  marker# P13b");
        TxProxyMon->ReportStatusNotOK->Inc();
        break;
    case TEvTxUserProxy::TResultStatus::ExecResponseData:
        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_DEBUG, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " RESPONSE Status# " << TEvTxUserProxy::TResultStatus::Str(status)
            << "  marker# P13d");
        TxProxyMon->ReportStatusStreamData->Inc();
        break;
    default:
        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " RESPONSE Status# " << TEvTxUserProxy::TResultStatus::Str(status)
            << "  marker# P13c");
        TxProxyMon->ReportStatusNotOK->Inc();
        break;
    }

    ctx.Send(RequestSource, x); // todo: error tracking
}

void Aggregate(NKikimrQueryStats::TReadOpStats& aggr, const NKikimrQueryStats::TReadOpStats& stats) {
    aggr.SetCount(aggr.GetCount() + stats.GetCount());
    aggr.SetRows(aggr.GetRows() + stats.GetRows());
    aggr.SetBytes(aggr.GetBytes() + stats.GetBytes());
}

void Aggregate(NKikimrQueryStats::TWriteOpStats& aggr, const NKikimrQueryStats::TWriteOpStats& stats) {
    aggr.SetCount(aggr.GetCount() + stats.GetCount());
    aggr.SetRows(aggr.GetRows() + stats.GetRows());
    aggr.SetBytes(aggr.GetBytes() + stats.GetBytes());
}

void Aggregate(NKikimrQueryStats::TTableAccessStats& aggr, const NKikimrQueryStats::TTableAccessStats& stats) {
    if (stats.GetSelectRow().GetCount()) {
        Aggregate(*aggr.MutableSelectRow(), stats.GetSelectRow());
    }
    if (stats.GetSelectRange().GetCount()) {
        Aggregate(*aggr.MutableSelectRange(), stats.GetSelectRange());
    }
    if (stats.GetUpdateRow().GetCount()) {
        Aggregate(*aggr.MutableUpdateRow(), stats.GetUpdateRow());
    }
    if (stats.GetEraseRow().GetCount()) {
        Aggregate(*aggr.MutableEraseRow(), stats.GetEraseRow());
    }
}

void TDataReq::BuildTxStats(NKikimrQueryStats::TTxStats& stats) {
    TTablePathHashMap<NKikimrQueryStats::TTableAccessStats> byTable;

    for (const auto& shard : PerTablet) {
        if (!shard.second.Stats)
            continue;

        for (const auto& table : shard.second.Stats->GetTableAccessStats()) {
            TTableId tableId(table.GetTableInfo().GetSchemeshardId(), table.GetTableInfo().GetPathId());
            auto& tableStats = byTable[tableId];
            if (!tableStats.HasTableInfo()) {
                tableStats.MutableTableInfo()->SetSchemeshardId(tableId.PathId.OwnerId);
                tableStats.MutableTableInfo()->SetPathId(tableId.PathId.LocalPathId);
                tableStats.MutableTableInfo()->SetName(table.GetTableInfo().GetName());
            }
            Aggregate(tableStats, table);
            tableStats.SetShardCount(tableStats.GetShardCount() + 1);
        }
        if (shard.second.Stats->PerShardStatsSize() == 1) {
            auto shardStats = stats.AddPerShardStats();
            shardStats->CopyFrom(shard.second.Stats->GetPerShardStats(0));
            shardStats->SetOutgoingReadSetsCount(shard.second.OutgoingReadSetsSize);
            shardStats->SetProgramSize(shard.second.ProgramSize);
            shardStats->SetReplySize(shard.second.ReplySize);
        }
    }

    for (auto& tableStats : byTable) {
        stats.AddTableAccessStats()->Swap(&tableStats.second);
    }

    stats.SetComputeCpuTimeUsec(CpuTime.MicroSeconds());
}

void TDataReq::ProcessFlatMKQLResolve(NSchemeCache::TSchemeCacheRequest *cacheRequest, const TActorContext &ctx) {
    NMiniKQL::IEngineFlat &engine = *FlatMKQLRequest->Engine;

    // Restore DbKeys
    auto &keyDescriptions = engine.GetDbKeys();
    Y_ABORT_UNLESS(keyDescriptions.size() == cacheRequest->ResultSet.size());
    for (size_t index = 0; index < keyDescriptions.size(); ++index) {
        keyDescriptions[index] = std::move(cacheRequest->ResultSet[index].KeyDescription);
    }

    auto beforeBuild = Now();
    NMiniKQL::IEngineFlat::TShardLimits shardLimits(RequestControls.MaxShardCount, RequestControls.MaxReadSetCount);
    if (FlatMKQLRequest->Limits.GetAffectedShardsLimit()) {
        shardLimits.ShardCount = std::min(shardLimits.ShardCount, FlatMKQLRequest->Limits.GetAffectedShardsLimit());
    }
    if (FlatMKQLRequest->Limits.GetReadsetCountLimit()) {
        shardLimits.RSCount = std::min(shardLimits.RSCount, FlatMKQLRequest->Limits.GetReadsetCountLimit());
    }
    ui32 rsCount = 0;
    FlatMKQLRequest->EngineResultStatusCode = engine.PrepareShardPrograms(shardLimits, &rsCount);
    auto afterBuild = Now();
    WallClockAfterBuild = afterBuild;

    if (FlatMKQLRequest->EngineResultStatusCode != NMiniKQL::IEngineFlat::EResult::Ok) {
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ENGINE_ERROR));
        ReportStatus(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest,
            NKikimrIssues::TStatusIds::BAD_REQUEST, true, ctx);
        TxProxyMon->MiniKQLWrongRequest->Inc();
        return Die(ctx);
    }

    FlatMKQLRequest->ReadOnlyProgram = engine.IsReadOnlyProgram();

    TxProxyMon->TxPrepareBuildShardProgramsHgram->Collect((afterBuild - beforeBuild).MicroSeconds());

    if (engine.GetAffectedShardCount() > 1 || FlatMKQLRequest->Snapshot) // TODO KIKIMR-11912
        CanUseFollower = false;

    // Check if we want to use snapshot even when caller didn't provide one
    const bool forceSnapshot = (
            FlatMKQLRequest->ReadOnlyProgram &&
            !FlatMKQLRequest->Snapshot &&
            rsCount == 0 &&
            engine.GetAffectedShardCount() > 1 &&
            ((TxFlags & NTxDataShard::TTxFlags::ForceOnline) == 0) &&
            !DatabaseName.empty());

    if (forceSnapshot) {
        Send(NLongTxService::MakeLongTxServiceID(ctx.SelfID.NodeId()),
            new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(DatabaseName));
        Become(&TThis::StateWaitSnapshot);
        return;
    }

    ContinueFlatMKQLResolve(ctx);
}

void TDataReq::Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);
        IssueManager.RaiseIssues(issues);
        ReportStatus(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError,
            NKikimrIssues::TStatusIds::ERROR,
            true, ctx);
        Die(ctx);
        return;
    }

    // Update timestamp: snapshot creation should not be included in send time histogram
    WallClockAfterBuild = Now();

    Y_ABORT_UNLESS(FlatMKQLRequest);
    FlatMKQLRequest->Snapshot = TRowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());
    ContinueFlatMKQLResolve(ctx);
}

void TDataReq::ContinueFlatMKQLResolve(const TActorContext &ctx) {
    NMiniKQL::IEngineFlat &engine = *FlatMKQLRequest->Engine;
    auto &keyDescriptions = engine.GetDbKeys();

    TDuration shardCancelAfter = ExecTimeoutPeriod;
    if (CancelAfter) {
        shardCancelAfter = Min(shardCancelAfter, CancelAfter);
    }

    TInstant shardCancelDeadline = WallClockAccepted + shardCancelAfter
        + TDuration::Seconds(ShardCancelDeadlineShiftSec);

    for (ui32 shx = 0, affectedShards = engine.GetAffectedShardCount(); shx != affectedShards; ++shx) {
        NMiniKQL::IEngineFlat::TShardData shardData;
        const auto shardDataRes = engine.GetAffectedShard(shx, shardData);
        Y_ABORT_UNLESS(shardDataRes == NMiniKQL::IEngineFlat::EResult::Ok);

        NKikimrTxDataShard::TDataTransaction dataTransaction;
        dataTransaction.SetMiniKQL(shardData.Program);
        dataTransaction.SetImmediate(shardData.Immediate || FlatMKQLRequest->Snapshot && FlatMKQLRequest->ReadOnlyProgram);
        dataTransaction.SetReadOnly(FlatMKQLRequest->ReadOnlyProgram);
        dataTransaction.SetCancelAfterMs(shardCancelAfter.MilliSeconds());
        dataTransaction.SetCancelDeadlineMs(shardCancelDeadline.MilliSeconds());
        dataTransaction.SetCollectStats(FlatMKQLRequest->CollectStats);
        if (FlatMKQLRequest->LockTxId)
            dataTransaction.SetLockTxId(FlatMKQLRequest->LockTxId);
        if (FlatMKQLRequest->NeedDiagnostics)
            dataTransaction.SetNeedDiagnostics(true);
        if (FlatMKQLRequest->LlvmRuntime)
            dataTransaction.SetLlvmRuntime(true);
        if (FlatMKQLRequest->PerShardKeysSizeLimitBytes)
            dataTransaction.SetPerShardKeysSizeLimitBytes(*FlatMKQLRequest->PerShardKeysSizeLimitBytes);
        const TString transactionBuffer = dataTransaction.SerializeAsString();

        if (transactionBuffer.size() > MaxDatashardProgramSize) {
            TString error = TStringBuilder() << "Datashard program size limit exceeded ("
                << transactionBuffer.size() << " > " << MaxDatashardProgramSize << ")";

            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, error
                << ", actor: " << ctx.SelfID.ToString()
                << ", txId: " << TxId
                << ", shard: " << shardData.ShardId);

            for (ui32 i = 0; i < shx; ++i) {
                auto result = engine.GetAffectedShard(i, shardData);
                if (result == NMiniKQL::IEngineFlat::EResult::Ok) {
                    CancelProposal(shardData.ShardId);
                }
            }

            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::SHARD_PROGRAM_SIZE_EXCEEDED, error));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError,
                NKikimrIssues::TStatusIds::QUERY_ERROR, true, ctx);
            return Die(ctx);
        }

        const auto affectedType = shardData.HasWrites ? TPerTablet::AffectedWrite : TPerTablet::AffectedRead;

        TPerTablet &perTablet = PerTablet[shardData.ShardId];
        Y_ABORT_UNLESS(perTablet.TabletStatus == TPerTablet::ETabletStatus::StatusUnknown);
        perTablet.TabletStatus = TPerTablet::ETabletStatus::StatusWait;
        perTablet.ProgramSize = transactionBuffer.size();
        ++TabletsLeft;

        perTablet.AffectedFlags |= affectedType;

        // we would need shard -> table mapping for scheme cache invalidation on errors
        for (const auto& keyDescription : keyDescriptions) {
            for (auto& partition : keyDescription->GetPartitions()) {
                if (auto *x = PerTablet.FindPtr(partition.ShardId)) {
                    x->TableId = keyDescription->TableId;
                }
            }
        }

        TxProxyMon->MiniKQLResolveSentToShard->Inc();

        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " SEND TEvProposeTransaction to datashard " << shardData.ShardId
            << " with " << shardData.Program.size() << " bytes program"
            << " affected shards " << engine.GetAffectedShardCount()
            << " followers " << (CanUseFollower ? "allowed" : "disallowed") << " marker# P4");

        const TActorId pipeCache = CanUseFollower ? Services.FollowerPipeCache : Services.LeaderPipeCache;
        TEvDataShard::TEvProposeTransaction* ev;
        if (FlatMKQLRequest->Snapshot && FlatMKQLRequest->ReadOnlyProgram) {
            ev = new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_DATA,
                ctx.SelfID, TxId, transactionBuffer, FlatMKQLRequest->Snapshot, TxFlags | NTxDataShard::TTxFlags::Immediate);
        } else {
            ev = new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_DATA,
                ctx.SelfID, TxId, transactionBuffer, TxFlags | (shardData.Immediate ? NTxDataShard::TTxFlags::Immediate : 0));
        }

        Send(pipeCache, new TEvPipeCache::TEvForward(ev, shardData.ShardId, true));

        FlatMKQLRequest->BalanceCoverageBuilders[shardData.ShardId] = new TBalanceCoverageBuilder();
    }

    engine.AfterShardProgramsExtracted();
    TxProxyMon->TxPrepareSendShardProgramsHgram->Collect((Now() - WallClockAfterBuild).MicroSeconds());

    Become(&TThis::StateWaitPrepare);
}

void TDataReq::ProcessReadTableResolve(NSchemeCache::TSchemeCacheRequest *cacheRequest, const TActorContext &ctx)
{
    auto &entry = cacheRequest->ResultSet[0];
    ReadTableRequest->KeyDesc = std::move(entry.KeyDescription);

    bool singleShard = ReadTableRequest->KeyDesc->GetPartitions().size() == 1;
    CanUseFollower = false;

    bool immediate = singleShard;

    if (!ReadTableRequest->Snapshot.IsMax()) {
        // Snapshot reads don't need any planning
        immediate = true;
    }

    for (auto& partition : ReadTableRequest->KeyDesc->GetPartitions()) {
        NKikimrTxDataShard::TDataTransaction dataTransaction;
        dataTransaction.SetStreamResponse(StreamResponse);
        dataTransaction.SetImmediate(immediate);
        dataTransaction.SetReadOnly(true);
        ActorIdToProto(SelfId(), dataTransaction.MutableSink());
        auto &tx = *dataTransaction.MutableReadTableTransaction();
        tx.MutableTableId()->SetOwnerId(ReadTableRequest->KeyDesc->TableId.PathId.OwnerId);
        tx.MutableTableId()->SetTableId(ReadTableRequest->KeyDesc->TableId.PathId.LocalPathId);
        tx.SetApiVersion(ReadTableRequest->RequestVersion);
        for (auto &col : ReadTableRequest->Columns) {
            auto &c = *tx.AddColumns();
            c.SetId(col.Id);
            c.SetName(col.Name);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(col.PType, col.PTypeMod);
            c.SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *c.MutableTypeInfo() = *columnType.TypeInfo;
            }
        }
        auto &range = *tx.MutableRange();
        ReadTableRequest->KeySpace.GetSpace().Serialize(range);

        // Normalize range's From/ToInclusive
        if (range.GetFrom().empty() && !range.GetFromInclusive()) {
            range.SetFromInclusive(true);
        }
        if (range.GetTo().empty() && !range.GetToInclusive()) {
            range.SetToInclusive(true);
        }

        if (!ReadTableRequest->Snapshot.IsMax()) {
            tx.SetSnapshotStep(ReadTableRequest->Snapshot.Step);
            tx.SetSnapshotTxId(ReadTableRequest->Snapshot.TxId);
        }

        const TString transactionBuffer = dataTransaction.SerializeAsString();

        TPerTablet &perTablet = PerTablet[partition.ShardId];
        perTablet.TableId = ReadTableRequest->KeyDesc->TableId;
        perTablet.TabletStatus = TPerTablet::ETabletStatus::StatusWait;
        perTablet.AffectedFlags = TPerTablet::AffectedRead;
        ++TabletsLeft;

        TxProxyMon->ReadTableResolveSentToShard->Inc();

        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " SEND TEvProposeTransaction to datashard " << partition.ShardId
            << " with read table request"
            << " affected shards " << ReadTableRequest->KeyDesc->GetPartitions().size()
            << " followers " << (CanUseFollower ? "allowed" : "disallowed") << " marker# P4b");

        const TActorId pipeCache = CanUseFollower ? Services.FollowerPipeCache : Services.LeaderPipeCache;

        Send(pipeCache, new TEvPipeCache::TEvForward(
                new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_SCAN,
                    ctx.SelfID, TxId, transactionBuffer,
                    TxFlags | (immediate ? NTxDataShard::TTxFlags::Immediate : 0)),
                partition.ShardId, true));
    }

    Become(&TThis::StateWaitPrepare);
}

TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> TDataReq::PrepareFlatMKQLRequest(TStringBuf miniKQLProgram, TStringBuf miniKQLParams, const TActorContext &ctx) {
    Y_UNUSED(ctx);

    TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());

    *TxProxyMon->MiniKQLParamsSize += miniKQLParams.size();
    *TxProxyMon->MiniKQLProgramSize += miniKQLProgram.size();

    auto beforeSetProgram = Now();
    FlatMKQLRequest->EngineResultStatusCode = FlatMKQLRequest->Engine->SetProgram(miniKQLProgram, miniKQLParams);
    TxProxyMon->TxPrepareSetProgramHgram->Collect((Now() - beforeSetProgram).MicroSeconds());

    if (FlatMKQLRequest->EngineResultStatusCode != NMiniKQL::IEngineFlat::EResult::Ok)
        return nullptr;

    WallClockResolveStarted = Now();
    auto &keyDescriptions = FlatMKQLRequest->Engine->GetDbKeys();

    // check keys and set use follower flag
    CanUseFollower = true;
    request->ResultSet.reserve(keyDescriptions.size());
    for (auto &keyd : keyDescriptions) {
        if (keyd->RowOperation != TKeyDesc::ERowOperation::Read || keyd->ReadTarget.GetMode() != TReadTarget::EMode::Follower) {
            CanUseFollower = false;
            LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor " << ctx.SelfID.ToString() << " txid " << TxId
                << " disallow followers cause of operation " << (ui32)keyd->RowOperation
                << " read target mode " << (ui32)keyd->ReadTarget.GetMode());
        }
        request->ResultSet.emplace_back(std::move(keyd));
    }

    return new TEvTxProxySchemeCache::TEvResolveKeySet(request);
}

void TDataReq::TryToInvalidateTable(TTableId tableId, const TActorContext &ctx) {
    const bool notYetInvalidated = InvalidatedTables.insert(tableId).second;
    if (notYetInvalidated)
        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(tableId, TActorId()));
}

void TDataReq::MarkShardError(ui64 shardId, TDataReq::TPerTablet &perTablet, bool invalidateDistCache, const TActorContext &ctx) {
    if (perTablet.TabletStatus == TDataReq::TPerTablet::ETabletStatus::StatusError)
        return;

    perTablet.TabletStatus = TDataReq::TPerTablet::ETabletStatus::StatusError;

    if (invalidateDistCache)
        TryToInvalidateTable(perTablet.TableId, ctx);

    Y_UNUSED(shardId);

    if (++TabletErrors == TabletsLeft) {
        LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " invalidateDistCache: " << invalidateDistCache
            << " DIE TDataReq MarkShardError TabletsLeft# " << TabletsLeft);
        TxProxyMon->MarkShardError->Inc();
        return Die(ctx);
    }
}

void TDataReq::Handle(TEvTxProxyReq::TEvMakeRequest::TPtr &ev, const TActorContext &ctx) {
    RequestControls.Reqister(ctx);

    TEvTxProxyReq::TEvMakeRequest *msg = ev->Get();
    const NKikimrTxUserProxy::TEvProposeTransaction &record = msg->Ev->Get()->Record;
    Y_ABORT_UNLESS(record.HasTransaction());

    ProxyFlags = record.HasProxyFlags() ? record.GetProxyFlags() : 0;
    ExecTimeoutPeriod = record.HasExecTimeoutPeriod()
        ? TDuration::MilliSeconds(record.GetExecTimeoutPeriod())
        : TDuration::MilliSeconds(RequestControls.DefaultTimeoutMs);
    if (ExecTimeoutPeriod.Minutes() > 60) {
        LOG_WARN_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                           "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                           << " huge ExecTimeoutPeriod requested " << ExecTimeoutPeriod.ToString()
                           << ", trimming to 30 min");
        ExecTimeoutPeriod = TDuration::Minutes(30);
    }

    CancelAfter = TDuration::MilliSeconds(record.GetCancelAfterMs());
    if (CancelAfter.Hours() > 8) {
        LOG_WARN_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                            << " huge CancelAfter duration " << CancelAfter.ToString()
                            << ", disabling CancelAfter");
        CancelAfter = {};
    }

    WallClockAccepted = Now();
    ctx.Schedule(TDuration::MilliSeconds(KIKIMR_DATAREQ_WATCHDOG_PERIOD), new TEvPrivate::TEvProxyDataReqOngoingTransactionsWatchdog());

    // Schedule execution timeout
    {
        TAutoPtr<IEventHandle> wakeupEv(new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));
        ExecTimeoutCookieHolder.Reset(ISchedulerCookie::Make2Way());

        CreateLongTimer(ctx, ExecTimeoutPeriod, wakeupEv, AppData(ctx)->SystemPoolId, ExecTimeoutCookieHolder.Get());
    }

    const NKikimrTxUserProxy::TTransaction &txbody = record.GetTransaction();
    RequestSource = msg->Ev->Sender;
    TxFlags = txbody.GetFlags() & ~NTxDataShard::TTxFlags::Immediate; // Ignore external immediate flag
    StreamResponse = record.GetStreamResponse();

    // Subscribe for TEvStreamIsDead event.
    if (StreamResponse)
        ctx.Send(RequestSource, new TEvents::TEvSubscribe, IEventHandle::FlagTrackDelivery);

    LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " Cookie# " << (ui64)ev->Cookie
        << " txid# " << TxId << " HANDLE TDataReq marker# P1");

    if (!record.GetUserToken().empty()) {
        UserToken = new NACLib::TUserToken(record.GetUserToken());
    }

    // For read table transaction we need to resolve table path.
    if (txbody.HasReadTableTransaction()) {
        ReadTableRequest = new TReadTableRequest(txbody.GetReadTableTransaction());
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        request->DatabaseName = DatabaseName = record.GetDatabaseName();

        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(ReadTableRequest->TablePath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;
        request->ResultSet.push_back(entry);

        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
        Become(&TThis::StateWaitResolve);
        return;
    }

    TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq;

    NCpuTime::TCpuTimer timer(CpuTime);
    if (txbody.HasMiniKQLTransaction()) {
        const auto& mkqlTxBody = txbody.GetMiniKQLTransaction();

        const TAppData* appData = AppData(ctx);
        const auto functionRegistry = appData->FunctionRegistry;

        if (mkqlTxBody.GetFlatMKQL()) {
            FlatMKQLRequest = new TFlatMKQLRequest;
            FlatMKQLRequest->LlvmRuntime = mkqlTxBody.GetLlvmRuntime();
            FlatMKQLRequest->CollectStats = mkqlTxBody.GetCollectStats();
            if (mkqlTxBody.HasPerShardKeysSizeLimitBytes()) {
                FlatMKQLRequest->PerShardKeysSizeLimitBytes = mkqlTxBody.GetPerShardKeysSizeLimitBytes();
            }
            if (mkqlTxBody.HasLimits()) {
                FlatMKQLRequest->Limits.CopyFrom(mkqlTxBody.GetLimits());
            }
            if (mkqlTxBody.HasSnapshotStep() && mkqlTxBody.HasSnapshotTxId())
                FlatMKQLRequest->Snapshot = TRowVersion(mkqlTxBody.GetSnapshotStep(), mkqlTxBody.GetSnapshotTxId());
            NMiniKQL::TEngineFlatSettings settings(NMiniKQL::IEngineFlat::EProtocol::V1, functionRegistry,
                                                   *TAppData::RandomProvider, *TAppData::TimeProvider,
                                                   nullptr, TxProxyMon->AllocPoolCounters);
            settings.EvaluateResultType = mkqlTxBody.GetEvaluateResultType();
            settings.EvaluateResultValue = mkqlTxBody.GetEvaluateResultValue();
            if (FlatMKQLRequest->LlvmRuntime) {
                LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                    "Using LLVM runtime to execute transaction: " << TxId);
                settings.LlvmRuntime = true;
            }
            if (ctx.LoggerSettings()->Satisfies(NLog::PRI_DEBUG, NKikimrServices::MINIKQL_ENGINE, TxId)) {
                auto actorSystem = ctx.ExecutorThread.ActorSystem;
                auto txId = TxId;
                settings.BacktraceWriter = [txId, actorSystem](const char* operation, ui32 line, const TBackTrace* backtrace) {
                    LOG_DEBUG_SAMPLED_BY(*actorSystem, NKikimrServices::MINIKQL_ENGINE, txId,
                        "Proxy data request, txId: %" PRIu64 ", %s (%" PRIu32 ")\n%s",
                        txId, operation, line, backtrace ? backtrace->PrintToString().data() : "");
                };
                settings.LogErrorWriter = [txId, actorSystem](const TString& message) {
                    LOG_ERROR_S(*actorSystem, NKikimrServices::MINIKQL_ENGINE, "Proxy data request, txId: "
                        << txId << ", engine error: " << message);
                };
            }

            if ((TxFlags & NTxDataShard::TTxFlags::ForceOnline) != 0) {
                settings.ForceOnline = true;
            }

            FlatMKQLRequest->Engine = NMiniKQL::CreateEngineFlat(settings);
            FlatMKQLRequest->Engine->SetStepTxId({ 0, TxId });

            TString program = mkqlTxBody.GetProgram().GetBin();
            TString params = mkqlTxBody.GetParams().GetBin();
            resolveReq = PrepareFlatMKQLRequest(program, params, ctx);

            FlatMKQLRequest->RequestAdditionResults(TxId);
        }
    }

    if (!resolveReq || !resolveReq->Request) {
        ReportStatus(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest,
            NKikimrIssues::TStatusIds::BAD_REQUEST, true, ctx);
        TxProxyMon->MakeRequestWrongRequest->Inc();
        return Die(ctx);
    } else if (resolveReq->Request->ResultSet.empty()) {
        TxProxyMon->MakeRequestEmptyAffectedSet->Inc();
        if (FlatMKQLRequest) {
            FlatMKQLRequest->EngineResultStatusCode = FlatMKQLRequest->Engine->PrepareShardPrograms(
                NMiniKQL::IEngineFlat::TShardLimits(0, 0));
            if (FlatMKQLRequest->EngineResultStatusCode != NMiniKQL::IEngineFlat::EResult::Ok) {
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ENGINE_ERROR, "could not prepare shard programs"));
                ReportStatus(
                    TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest,
                    NKikimrIssues::TStatusIds::QUERY_ERROR, true, ctx);
                TxProxyMon->MiniKQLWrongRequest->Inc();
                return Die(ctx);
            }
            FlatMKQLRequest->Engine->AfterShardProgramsExtracted();
            return MakeFlatMKQLResponse(ctx, timer);
        } else {
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ENGINE_ERROR, "empty affected set"));
            ReportStatus(
                TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::EmptyAffectedSet,
                NKikimrIssues::TStatusIds::QUERY_ERROR, true, ctx);
            return Die(ctx);
        }
    } else {
        if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportAccepted)
            ReportStatus(
                TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyAccepted,
                NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
    }

    resolveReq->Request->DatabaseName = DatabaseName = record.GetDatabaseName();
    TxProxyMon->MakeRequestProxyAccepted->Inc();
    LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " SEND to# " << Services.SchemeCache.ToString() << " TSchemeCache with "
        << resolveReq->Request->ResultSet.size() << " scheme entries. DataReq marker# P2" );

    ctx.Send(Services.SchemeCache, resolveReq.Release());
    Become(&TThis::StateWaitResolve);
}

void TDataReq::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx) {
    TEvTxProxySchemeCache::TEvNavigateKeySetResult *msg = ev->Get();
    NSchemeCache::TSchemeCacheNavigate *resp = msg->Request.Get();

    LOG_LOG_S_SAMPLED_BY(ctx, (resp->ErrorCount == 0 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR),
                         NKikimrServices::TX_PROXY, TxId,
                         "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                         << " HANDLE EvNavigateKeySetResult TDataReq marker# P3b ErrorCount# "
                         << resp->ErrorCount);

    if (resp->ErrorCount > 0) {
        const TString errorExplanation = "unresolved table: " + ReadTableRequest->TablePath;
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, errorExplanation));

        UnresolvedKeys.push_back(errorExplanation);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, true, ctx);

        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    Y_ABORT_UNLESS(ReadTableRequest);
    bool projection = !ReadTableRequest->Columns.empty();
    TMap<TString, size_t> colNames;
    for (size_t i = 0; i < ReadTableRequest->Columns.size(); ++i) {
        auto &col = ReadTableRequest->Columns[i];
        if (colNames.contains(col.Name)) {
            const TString errorExplanation = "duplicated columns are not supported: " + col.Name;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, errorExplanation));
            UnresolvedKeys.push_back(errorExplanation);
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, true, ctx);

            TxProxyMon->ResolveKeySetWrongRequest->Inc();

            return Die(ctx);
        }
        colNames[col.Name] = i;
    }

    auto &res = resp->ResultSet[0];
    ReadTableRequest->TableId = res.TableId;

    if (res.TableId.IsSystemView()) {
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR,
            Sprintf("Table '%s' is a system view. Read table is not supported", ReadTableRequest->TablePath.data())));
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, true, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    TVector<NScheme::TTypeInfo> keyColumnTypes(res.Columns.size());
    TVector<TKeyDesc::TColumnOp> columns(res.Columns.size());
    size_t keySize = 0;
    size_t no = 0;

    for (auto &entry : res.Columns) {
        auto &col = entry.second;

        if (col.KeyOrder != -1) {
            keyColumnTypes[col.KeyOrder] = col.PType;
            ++keySize;
        }

        columns[no].Column = col.Id;
        columns[no].Operation = TKeyDesc::EColumnOperation::Read;
        columns[no].ExpectedType = col.PType;
        ++no;

        if (projection) {
            if (colNames.contains(col.Name)) {
                ReadTableRequest->Columns[colNames[col.Name]] = col;
                colNames.erase(col.Name);
            }
        } else {
            ReadTableRequest->Columns.push_back(col);
        }
    }

    // Report unresolved columns.
    if (!colNames.empty()) {
        for (auto entry: colNames) {
            const TString &errorExplanation = "unresolved column: " + entry.first;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, errorExplanation));
            UnresolvedKeys.push_back(errorExplanation);
        }
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, true, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    // Parse range.
    TConstArrayRef<NScheme::TTypeInfo> keyTypes(keyColumnTypes.data(), keySize);
    // Fix KeyRanges
    bool fromInclusive = ReadTableRequest->Range.GetFromInclusive();
    EParseRangeKeyExp fromExpand = EParseRangeKeyExp::TO_NULL;
    bool toInclusive = ReadTableRequest->Range.GetToInclusive();
    EParseRangeKeyExp toExpand = EParseRangeKeyExp::NONE;
    if (ReadTableRequest->RequestVersion == NKikimrTxUserProxy::TReadTableTransaction::YDB_V1) {
        if (!ReadTableRequest->Range.HasFrom()) {
            fromExpand = EParseRangeKeyExp::TO_NULL;
        } else {
            fromExpand = fromInclusive ? EParseRangeKeyExp::TO_NULL : EParseRangeKeyExp::NONE;
        }

        if (!ReadTableRequest->Range.HasTo()) {
            toExpand = EParseRangeKeyExp::NONE;
        } else {
            toExpand = toInclusive ? EParseRangeKeyExp::NONE : EParseRangeKeyExp::TO_NULL;
        }
    }

    if (!ParseRangeKey(ReadTableRequest->Range.GetFrom(), keyTypes,
                       ReadTableRequest->FromValues, fromExpand)
        || !ParseRangeKey(ReadTableRequest->Range.GetTo(), keyTypes,
                          ReadTableRequest->ToValues, toExpand)) {
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::KEY_PARSE_ERROR, "could not parse key string"));
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::QUERY_ERROR, true, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    if (!ReadTableRequest->FromValues.GetCells() && !ReadTableRequest->Range.HasFromInclusive()) {
        fromInclusive = true; // default to inclusive -inf
    }
    if (!ReadTableRequest->ToValues.GetCells() && !ReadTableRequest->Range.HasToInclusive()) {
        toInclusive = true; // default to inclusive +inf
    }

    TTableRange range(ReadTableRequest->FromValues.GetCells(),
                      fromInclusive,
                      ReadTableRequest->ToValues.GetCells(),
                      toInclusive);

    if (range.IsEmptyRange({keyTypes.begin(), keyTypes.end()})) {
        const TString errorExplanation = "empty range requested";
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::EMPTY_OP_RANGE, errorExplanation));
        UnresolvedKeys.push_back(errorExplanation);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::QUERY_ERROR, true, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    ReadTableRequest->KeyDesc.Reset(new TKeyDesc(res.TableId, range, TKeyDesc::ERowOperation::Read,
                                             keyTypes, columns));
    ReadTableRequest->KeySpace.Initialize(ReadTableRequest->Ordered, keyTypes, range);

    WallClockResolveStarted = Now();

    TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest);
    request->DomainOwnerId = res.DomainInfo->ExtractSchemeShard();
    request->ResultSet.emplace_back(std::move(ReadTableRequest->KeyDesc));
    ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvResolveKeySet(request));
}

void TDataReq::Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev, const TActorContext &ctx) {
    TEvTxProxySchemeCache::TEvResolveKeySetResult *msg = ev->Get();
    NSchemeCache::TSchemeCacheRequest *request = msg->Request.Get();

    LOG_LOG_S_SAMPLED_BY(ctx, (request->ErrorCount == 0 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR),
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " HANDLE EvResolveKeySetResult TDataReq marker# P3 ErrorCount# " << request->ErrorCount);

    TxProxyMon->CacheRequestLatency->Collect((Now() - WallClockAccepted).MilliSeconds());
    WallClockResolved = Now();

    if (request->ErrorCount > 0) {
        bool gotHardResolveError = false;
        for (const auto &x : request->ResultSet) {
            if ((ui32)x.Status < (ui32) NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                TryToInvalidateTable(x.KeyDescription->TableId, ctx);

                TStringStream ss;
                switch (x.Status) {
                    case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                        gotHardResolveError = true;
                        ss << "table not exists: " << x.KeyDescription->TableId;
                        break;
                    case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                        gotHardResolveError = true;
                        ss << "type check error: " << x.KeyDescription->TableId;
                        break;
                    default:
                        ss << "unresolved table: " << x.KeyDescription->TableId << ". Status: " << x.Status;
                        break;
                }

                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, ss.Str()));
                UnresolvedKeys.push_back(ss.Str());
            }
        }

        if (gotHardResolveError) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, true, ctx);
            TxProxyMon->ResolveKeySetWrongRequest->Inc();
        } else {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        }

        return Die(ctx);
    }

    if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportResolved)
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);

    TxProxyMon->TxPrepareResolveHgram->Collect((WallClockResolved - WallClockResolveStarted).MicroSeconds());

    NCpuTime::TCpuTimer timer(CpuTime);
    for (const NSchemeCache::TSchemeCacheRequest::TEntry& entry : request->ResultSet) {
        ui32 access = 0;
        switch (entry.KeyDescription->RowOperation) {
        case TKeyDesc::ERowOperation::Update:
            access |= NACLib::EAccessRights::UpdateRow;
            break;
        case TKeyDesc::ERowOperation::Read:
            access |= NACLib::EAccessRights::SelectRow;
            break;
        case TKeyDesc::ERowOperation::Erase:
            access |= NACLib::EAccessRights::EraseRow;
            break;
        default:
            break;
        }
        if (access != 0
                && UserToken != nullptr
                && entry.KeyDescription->Status == TKeyDesc::EStatus::Ok
                && entry.KeyDescription->SecurityObject != nullptr
                && !entry.KeyDescription->SecurityObject->CheckAccess(access, *UserToken)) {
            TStringStream explanation;
            explanation << "Access denied for " << UserToken->GetUserSID()
                << " with access " << NACLib::AccessRightsToString(access)
                << " to tableId# " << entry.KeyDescription->TableId;

            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, explanation.Str());
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, explanation.Str()));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, NKikimrIssues::TStatusIds::ACCESS_DENIED, true, ctx);
            return Die(ctx);
        }

        if (FlatMKQLRequest && entry.Kind == NSchemeCache::TSchemeCacheRequest::KindAsyncIndexTable) {
            TMaybe<TString> error;

            if (entry.KeyDescription->RowOperation != TKeyDesc::ERowOperation::Read) {
                error = TStringBuilder() << "Non-read operations can't be performed on async index table"
                    << ": " << entry.KeyDescription->TableId;
            } else if (entry.KeyDescription->ReadTarget.GetMode() != TReadTarget::EMode::Follower) {
                error = TStringBuilder() << "Read operation can be performed on async index table"
                    << ": " << entry.KeyDescription->TableId << " only with StaleRO isolation level";
            }

            if (error) {
                LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, *error);
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, *error));
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::NOTSUPPORTED, true, ctx);
                return Die(ctx);
            }
        }
    }

    if (!CheckDomainLocality(*request)) {
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DOMAIN_LOCALITY_ERROR));
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError, NKikimrIssues::TStatusIds::BAD_REQUEST, true, ctx);
        TxProxyMon->ResolveKeySetDomainLocalityFail->Inc();
        return Die(ctx);
    }

    SelectedCoordinator = SelectCoordinator(*request, ctx);

    if (ReadTableRequest) {
        TxProxyMon->ResolveKeySetReadTableSuccess->Inc();
        ProcessReadTableResolve(request, ctx);
    } else if (FlatMKQLRequest) {
        TxProxyMon->ResolveKeySetMiniKQLSuccess->Inc();
        ProcessFlatMKQLResolve(request, ctx);
    } else {
        Y_ABORT("No request");
    }
}

void TDataReq::Handle(TEvPrivate::TEvReattachToShard::TPtr &ev, const TActorContext &ctx) {
    const ui64 tabletId = ev->Get()->TabletId;
    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);
    Y_ABORT_UNLESS(perTablet);

    LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_INFO,
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID << " txid# " << TxId << " sending reattach to shard " << tabletId);

    // Try to reattach transaction to a new tablet
    const TActorId pipeCache = CanUseFollower ? Services.FollowerPipeCache : Services.LeaderPipeCache;

    Send(pipeCache, new TEvPipeCache::TEvForward(
                new TEvDataShard::TEvProposeTransactionAttach(tabletId, TxId),
                tabletId, true), 0, ++perTablet->ReattachState.Cookie);
}

void TDataReq::HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
    TEvPipeCache::TEvDeliveryProblem *msg = ev->Get();
    TPerTablet *perTablet = PerTablet.FindPtr(msg->TabletId);
    Y_ABORT_UNLESS(perTablet);

    bool wasRestarting = std::exchange(perTablet->Restarting, false);

    // We can only be sure tx was not prepared if initial propose was not delivered
    bool notPrepared = msg->NotDelivered && !perTablet->RestartCount;

    // Disconnected while waiting for initial propose response
    if (perTablet->TabletStatus == TPerTablet::ETabletStatus::StatusWait) {
        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << msg->TabletId << " delivery problem"
            << " (waiting, notDelivered=" << msg->NotDelivered << ", notPrepared=" << notPrepared << ")");

        ComplainingDatashards.push_back(msg->TabletId);
        CancelProposal(notPrepared ? msg->TabletId : 0);

        if (notPrepared) {
            TStringStream explanation;
            explanation << "could not deliver program to shard " << msg->TabletId << " with txid# " << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, explanation.Str()));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        } else if (wasRestarting) {
            // We are waiting for propose and have a restarting flag, which means shard was
            // persisting our tx. We did not receive a reply, so we cannot be sure if it
            // succeeded or not, but we know that it could not apply any side effects, since
            // we don't start transaction planning until prepare phase is complete.
            TStringStream explanation;
            explanation << "could not prepare program at shard " << msg->TabletId << " with txid# " << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, explanation.Str()));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        } else {
            TStringStream explanation;
            explanation << "tx state unknown for shard " << msg->TabletId << " with txid# " << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation.Str()));
            auto status = IsReadOnlyRequest()
                ? TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable
                : TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown;
            ReportStatus(status, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
        }

        Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
        TxProxyMon->ClientConnectedError->Inc();
        return HandlePrepareErrors(ev, ctx);
    }

    // Disconnected while waiting for other shards to prepare
    if (perTablet->TabletStatus == TPerTablet::ETabletStatus::StatusPrepared) {
        if (!ReadTableRequest &&
            (wasRestarting || perTablet->ReattachState.Reattaching) &&
            perTablet->ReattachState.ShouldReattach(ctx.Now()))
        {
            LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_DEBUG,
                NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << msg->TabletId
                    << " delivery problem (already prepared, reattaching in " << perTablet->ReattachState.Delay << ")");
            ctx.Schedule(perTablet->ReattachState.Delay, new TEvPrivate::TEvReattachToShard(msg->TabletId));
            ++perTablet->RestartCount;
            return;
        }

        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << msg->TabletId << " delivery problem (already prepared)"
                << (msg->NotDelivered ? " last message not delivered" : ""));

        ComplainingDatashards.push_back(msg->TabletId);
        CancelProposal(0);

        TStringStream explanation;
        explanation << "tx state unknown for shard " << msg->TabletId << " with txid# " << TxId;
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation.Str()));
        auto status = IsReadOnlyRequest()
            ? TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable
            : TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown;
        ReportStatus(status, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);

        Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
        TxProxyMon->ClientConnectedError->Inc();
        return HandlePrepareErrors(ev, ctx);
    }
}

void TDataReq::HandlePrepareErrors(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
    TEvPipeCache::TEvDeliveryProblem *msg = ev->Get();
    TPerTablet *perTablet = PerTablet.FindPtr(msg->TabletId);
    Y_ABORT_UNLESS(perTablet);

    if (perTablet->TabletStatus == TPerTablet::ETabletStatus::StatusWait) {
        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << msg->TabletId << " delivery problem (gathering prepare errors)");

        return MarkShardError(msg->TabletId, *perTablet, true, ctx);
    }
}


void TDataReq::HandlePrepare(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    TEvDataShard::TEvProposeTransactionResult *msg = ev->Get();
    const NKikimrTxDataShard::TEvProposeTransactionResult &record = msg->Record;

    const ui64 tabletId = msg->GetOrigin();
    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);
    Y_ABORT_UNLESS(perTablet);

    LOG_LOG_S_SAMPLED_BY(ctx, (msg->GetStatus() != NKikimrTxDataShard::TEvProposeTransactionResult::ERROR ?
        NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR),
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " HANDLE Prepare TEvProposeTransactionResult TDataReq TabletStatus# " << perTablet->TabletStatus
        << " GetStatus# " << msg->GetStatus()
        << " shard id " << tabletId
        << " read size " << record.GetReadSize()
        << " out readset size " << record.OutgoingReadSetInfoSize()
        << " marker# P6");

    WallClockLastPrepareReply = Now();
    const TInstant reportedArriveTime = TInstant::MicroSeconds(record.GetPrepareArriveTime());
    const TDuration completionDelta = WallClockLastPrepareReply - reportedArriveTime;
    WallClockMinPrepareArrive = WallClockMinPrepareArrive.GetValue() ? Min(WallClockMinPrepareArrive, reportedArriveTime) : reportedArriveTime;
    WallClockMaxPrepareArrive = Max(WallClockMaxPrepareArrive, reportedArriveTime);
    WallClockMinPrepareComplete = WallClockMaxPrepareComplete.GetValue() ? Min(WallClockMaxPrepareComplete, completionDelta) : completionDelta;
    WallClockMaxPrepareComplete = Max(WallClockMaxPrepareComplete, completionDelta);

    if (WallClockFirstPrepareReply.GetValue() == 0)
        WallClockFirstPrepareReply = WallClockLastPrepareReply;

    if (perTablet->TabletStatus == TPerTablet::ETabletStatus::StatusPrepared) { // do nothing
        TxProxyMon->TxResultTabletPrepared->Inc();
        return;
    }

    switch (msg->GetStatus()) {
    case NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED:
    {
        perTablet->TabletStatus = TPerTablet::ETabletStatus::StatusPrepared;
        perTablet->MinStep = record.GetMinStep();
        perTablet->MaxStep = record.GetMaxStep();
        perTablet->ReadSize = record.GetReadSize();
        perTablet->ReplySize = record.GetReplySize();
        perTablet->OutgoingReadSetsSize = record.OutgoingReadSetInfoSize();
        for (size_t i = 0; i < record.OutgoingReadSetInfoSize(); ++i) {
            auto& rs = record.GetOutgoingReadSetInfo(i);
            ui64 targetTabletId = rs.GetShardId();
            ui64 size = rs.GetSize();
            TPerTablet* targetTablet = PerTablet.FindPtr(targetTabletId);
            Y_ABORT_UNLESS(targetTablet);
            targetTablet->IncomingReadSetsSize += size;
        }

        AggrMaxStep = Min(AggrMaxStep, perTablet->MaxStep);
        AggrMinStep = Max(AggrMinStep, perTablet->MinStep);

        TxProxyMon->TxResultPrepared->Inc();

        if (record.HasExecLatency())
            ElapsedPrepareExec = Max<TDuration>(ElapsedPrepareExec, TDuration::MilliSeconds(record.GetExecLatency()));
        if (record.HasProposeLatency())
            ElapsedPrepareComplete = Max<TDuration>(ElapsedPrepareComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

        ui64 privateCoordinator = TCoordinators(TVector<ui64>(
                                                    record.GetDomainCoordinators().begin(),
                                                    record.GetDomainCoordinators().end()))
                                      .Select(TxId);

        if (!SelectedCoordinator) {
            SelectedCoordinator = privateCoordinator;
        }

        if (!SelectedCoordinator || SelectedCoordinator != privateCoordinator) {
            CancelProposal(tabletId);
            TStringBuilder explanation;
            explanation << "tx state canceled, unable to choose coordinator neither by resolved keys nor by TEvProposeTransactionResult from datashard txid#" << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR, explanation));
            auto errorCode =TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError;
            if (SelectedCoordinator == 0) {
                errorCode =  TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorUnknown;
            }
            ReportStatus(errorCode, NKikimrIssues::TStatusIds::INTERNAL_ERROR, true, ctx);
            TxProxyMon->TxResultAborted->Inc();
            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                        " HANDLE Prepare TEvProposeTransactionResult TDataReq "
                        " all DataShards are prepared successful, "
                        " but we unable to choose coordinator neither by resolved keys nor by TEvProposeTransactionResult from datashard, "
                        " tx canceled"
                                << ", actorId: " << ctx.SelfID.ToString()
                                << ", txid: " << TxId
                                << ", coordinator selected at resolve keys state: " << SelectedCoordinator
                                << ", coordinator selected at propose result state: " << privateCoordinator);

            return Die(ctx);
        }

        if (--TabletsLeft != 0)
            return;

        return RegisterPlan(ctx);
    }
    case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE:
        perTablet->TabletStatus = TPerTablet::ETabletStatus::StatusFinished;

        if (record.HasExecLatency())
            ElapsedExecExec = Max<TDuration>(ElapsedExecExec, TDuration::MilliSeconds(record.GetExecLatency()));
        if (record.HasProposeLatency())
            ElapsedExecComplete = Max<TDuration>(ElapsedExecComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

        TxProxyMon->TxResultComplete->Inc();
        return MergeResult(ev, ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA:
        ProcessStreamResponseData(ev, ctx);
        return;
    case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR: {
        ExtractDatashardErrors(record);
        CancelProposal(tabletId);
        bool schemeChanged = false;
        for (const auto& e : record.GetError()) {
            if (e.GetKind() == NKikimrTxDataShard::TError::SCHEME_CHANGED) {
                schemeChanged = true;
            }
        }
        if (schemeChanged) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        } else {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        }
        Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
        TxProxyMon->TxResultError->Inc();
        return HandlePrepareErrors(ev, ctx);
    }
    case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);
        TxProxyMon->TxResultAborted->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER:
        ExtractDatashardErrors(record);
        CancelProposal(tabletId);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        TxProxyMon->TxResultShardTryLater->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED:
        ExtractDatashardErrors(record);
        CancelProposal(tabletId);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded, NKikimrIssues::TStatusIds::OVERLOADED, true, ctx);
        TxProxyMon->TxResultShardOverloaded->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR:
        ExtractDatashardErrors(record);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->TxResultExecError->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
        ExtractDatashardErrors(record);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->TxResultResultUnavailable->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
        ExtractDatashardErrors(record);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->TxResultCancelled->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST:
        ExtractDatashardErrors(record);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, NKikimrIssues::TStatusIds::BAD_REQUEST, true, ctx);
        TxProxyMon->TxResultCancelled->Inc();
        return Die(ctx);
    default:
        // everything other is hard error
        ExtractDatashardErrors(record);
        CancelProposal(tabletId);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->TxResultFatal->Inc();
        return Die(ctx);
    }
}

void TDataReq::CancelProposal(ui64 exceptTablet) {
    if (CanUseFollower)
        return;

    for (const auto &x : PerTablet)
        if (x.first != exceptTablet) {
            Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                new TEvDataShard::TEvCancelTransactionProposal(TxId),
                x.first,
                false
            ));
        }
}

void TDataReq::ExtractDatashardErrors(const NKikimrTxDataShard::TEvProposeTransactionResult & record) {
    TString allErrors;
    for (const auto &er : record.GetError()) {
        allErrors += Sprintf("[%s] %s\n", NKikimrTxDataShard::TError_EKind_Name(er.GetKind()).data(), er.GetReason().data());
    }

    DatashardErrors = allErrors;
    ComplainingDatashards.push_back(record.GetOrigin());
}

void TDataReq::HandlePrepareErrors(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    TEvDataShard::TEvProposeTransactionResult *msg = ev->Get();
    const NKikimrTxDataShard::TEvProposeTransactionResult &record = msg->Record;

    const ui64 tabletId = msg->GetOrigin();
    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);
    Y_ABORT_UNLESS(perTablet);

    LOG_LOG_S_SAMPLED_BY(ctx, (msg->GetStatus() != NKikimrTxDataShard::TEvProposeTransactionResult::ERROR ?
        NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR),
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " HANDLE PrepareErrors TEvProposeTransactionResult TDataReq TabletStatus# " << perTablet->TabletStatus
        << " shard id " << tabletId);

    if (perTablet->TabletStatus != TPerTablet::ETabletStatus::StatusWait) // do nothing for already processed cases
        return;

    switch (msg->GetStatus()) {
    case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR:
        for (const auto &er : record.GetError()) {
            const NKikimrTxDataShard::TError::EKind errorKind = er.GetKind();
            switch (errorKind) {
                case NKikimrTxDataShard::TError::SCHEME_ERROR:
                case NKikimrTxDataShard::TError::WRONG_PAYLOAD_TYPE:
                case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
                case NKikimrTxDataShard::TError::SCHEME_CHANGED:
                    return MarkShardError(tabletId, *perTablet, true, ctx);
                default:
                    break;
            }
        }
        [[fallthrough]];
    default:
        return MarkShardError(tabletId, *perTablet, false, ctx);
    }
}

void TDataReq::HandlePrepareErrorTimeout(const TActorContext &ctx) {
    TxProxyMon->PrepareErrorTimeout->Inc();
    return Die(ctx);
}

void TDataReq::Handle(TEvTxProxy::TEvProposeTransactionStatus::TPtr &ev, const TActorContext &ctx) {
    // from coordinator
    TEvTxProxy::TEvProposeTransactionStatus *msg = ev->Get();
    const NKikimrTx::TEvProposeTransactionStatus &record = msg->Record;
    switch (msg->GetStatus()) {
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
        TxProxyMon->ClientTxStatusAccepted->Inc();
        // nop
        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE TEvProposeTransactionStatus TDataReq marker# P11 Status# " <<  msg->GetStatus());
        break;
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
        TxProxyMon->ClientTxStatusProcessed->Inc();
        // nop
        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE TEvProposeTransactionStatus TDataReq marker# P11 Status# " <<  msg->GetStatus());
        break;
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
        TxProxyMon->ClientTxStatusConfirmed->Inc();
        // nop
        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE TEvProposeTransactionStatus TDataReq marker# P11 Status# " <<  msg->GetStatus());
        break;
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
        // ok
        PlanStep = record.GetStepId();
        CoordinatorStatus = ECoordinatorStatus::Planned;

        TxProxyMon->ClientTxStatusPlanned->Inc();
        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE TEvProposeTransactionStatus TDataReq marker# P10 Status# " << msg->GetStatus());
        WallClockPlanned = Now();
        if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportPlanned)
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorPlanned, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
        break;
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting: // TODO: retry
        // cancel proposal only for defined cases and fall through for generic error handling
        CancelProposal(0);
        [[fallthrough]];
    default:
        // smth goes wrong
        LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE TEvProposeTransactionStatus TDataReq marker# P9 Status# " << msg->GetStatus());
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
        return Die(ctx);
    }
}

void TDataReq::Handle(TEvDataShard::TEvProposeTransactionRestart::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    const auto &record = ev->Get()->Record;
    const ui64 tabletId = record.GetTabletId();

    LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_DEBUG,
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << tabletId
        << " may restart transaction in the next generation");

    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);
    Y_ABORT_UNLESS(perTablet);

    perTablet->Restarting = true;
}

void TDataReq::HandlePrepare(TEvDataShard::TEvProposeTransactionAttachResult::TPtr &ev, const TActorContext &ctx) {
    const auto &record = ev->Get()->Record;
    const ui64 tabletId = record.GetTabletId();

    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);
    Y_ABORT_UNLESS(perTablet);

    if (ev->Cookie != perTablet->ReattachState.Cookie) {
        return;
    }

    switch (perTablet->TabletStatus) {
        case TPerTablet::ETabletStatus::StatusUnknown:
        case TPerTablet::ETabletStatus::StatusError:
        case TPerTablet::ETabletStatus::StatusFinished:
            // We are not interested in this shard
            return;

        default:
            break;
    }

    if (record.GetStatus() == NKikimrProto::OK) {
        // Transaction still exists at this shard
        perTablet->ReattachState.Reattached();
        return;
    }

    LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << tabletId
        << " transaction lost during reconnect: " << record.GetStatus());

    ComplainingDatashards.push_back(tabletId);
    CancelProposal(tabletId);

    TStringStream explanation;
    explanation << "tx state unknown for shard " << tabletId << " with txid# " << TxId;
    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation.Str()));
    auto status = IsReadOnlyRequest()
        ? TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable
        : TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown;
    ReportStatus(status, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);

    Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
    TxProxyMon->ClientConnectedError->Inc();
    return MarkShardError(tabletId, *perTablet, true, ctx);
}

void TDataReq::HandlePlan(TEvDataShard::TEvProposeTransactionAttachResult::TPtr &ev, const TActorContext &ctx) {
    const auto &record = ev->Get()->Record;
    const ui64 tabletId = record.GetTabletId();

    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);
    Y_ABORT_UNLESS(perTablet);

    if (ev->Cookie != perTablet->ReattachState.Cookie) {
        return;
    }

    switch (perTablet->TabletStatus) {
        case TPerTablet::ETabletStatus::StatusUnknown:
        case TPerTablet::ETabletStatus::StatusError:
        case TPerTablet::ETabletStatus::StatusFinished:
            // We are not interested in this shard
            return;

        default:
            break;
    }

    if (record.GetStatus() == NKikimrProto::OK) {
        // Transaction still exists at this shard
        perTablet->ReattachState.Reattached();
        return;
    }

    LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << tabletId << " transaction lost during reconnect");

    ComplainingDatashards.push_back(tabletId);

    TStringStream explanation;
    explanation << "tx state unknown for shard " << tabletId << " with txid#" << TxId;
    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation.Str()));

    auto status = IsReadOnlyRequest()
        ? TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable
        : TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown;
    ReportStatus(status, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
    TxProxyMon->ClientConnectedError->Inc();

    return Die(ctx);
}

void TDataReq::HandlePlan(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    // from tablets
    TEvDataShard::TEvProposeTransactionResult *msg = ev->Get();
    const auto &record = msg->Record;

    const ui64 tabletId = msg->GetOrigin();
    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);

    LOG_LOG_S_SAMPLED_BY(ctx, ((msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE ||
        msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED ||
        msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA)
        ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR),
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " HANDLE Plan TEvProposeTransactionResult TDataReq GetStatus# " << msg->GetStatus()
        << " shard id " << tabletId
        << " marker# P12");

    if (record.HasExecLatency())
        ElapsedExecExec = Max<TDuration>(ElapsedExecExec, TDuration::MilliSeconds(record.GetExecLatency()));
    if (record.HasProposeLatency())
        ElapsedExecComplete = Max<TDuration>(ElapsedExecComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

    switch (msg->GetStatus()) {
    case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE:
        if (Y_LIKELY(perTablet)) {
            perTablet->TabletStatus = TPerTablet::ETabletStatus::StatusFinished;
        }
        TxProxyMon->PlanClientTxResultComplete->Inc();
        return MergeResult(ev, ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA:
        ProcessStreamResponseData(ev, ctx);
        return;
    case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);
        TxProxyMon->PlanClientTxResultAborted->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
        ExtractDatashardErrors(record);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->PlanClientTxResultResultUnavailable->Inc();
        return Die(ctx);
    case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
        ExtractDatashardErrors(record);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->PlanClientTxResultCancelled->Inc();
        return Die(ctx);
    default:
        ExtractDatashardErrors(record);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->PlanClientTxResultExecError->Inc();
        return Die(ctx);
    }
}

void TDataReq::HandlePlan(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
    TEvPipeCache::TEvDeliveryProblem *msg = ev->Get();

    if (msg->TabletId == SelectedCoordinator) {
        if (msg->NotDelivered) {
            LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
                NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " not delivered to coordinator"
                << " coordinator id " << msg->TabletId << " marker# P8");

            TStringStream explanation;
            explanation << "tx failed to plan with txid#" << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_DECLINED_BY_COORDINATOR, explanation.Str()));

            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
            TxProxyMon->PlanCoordinatorDeclined->Inc();
        } else if (CoordinatorStatus == ECoordinatorStatus::Planned) {
            // We lost pipe to coordinator, but we already know tx is planned
            return;
        } else {
            LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
                NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " delivery problem to coordinator"
                << " coordinator id " << msg->TabletId << " marker# P8b");

            TStringStream explanation;
            explanation << "tx state unknown, lost pipe with selected tx coordinator with txid#" << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation.Str()));

            auto status = IsReadOnlyRequest()
                ? TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined
                : TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorUnknown;

            ReportStatus(status, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
            TxProxyMon->PlanClientDestroyed->Inc();
        }

        return Die(ctx);
    } else if (TPerTablet *perTablet = PerTablet.FindPtr(msg->TabletId)) {
        bool wasRestarting = std::exchange(perTablet->Restarting, false);
        switch (perTablet->TabletStatus) {
            case TPerTablet::ETabletStatus::StatusUnknown:
            case TPerTablet::ETabletStatus::StatusWait:
                // should be impossible, just handle as if it's an error
                LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                    "Actor# " << ctx.SelfID << " txid# " << TxId << " shard " << msg->TabletId
                        << " has unexpected state " << perTablet->TabletStatus
                        << " on delivery problem in planning state");
                wasRestarting = false;
                break;
            case TPerTablet::ETabletStatus::StatusPrepared:
                break; // handled below
            default:
                return; // we no longer care about this shard
        }

        if (!ReadTableRequest &&
            (wasRestarting || perTablet->ReattachState.Reattaching) &&
            perTablet->ReattachState.ShouldReattach(ctx.Now()))
        {
            LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_DEBUG,
                NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << msg->TabletId
                    << " lost pipe while waiting for reply (reattaching in " << perTablet->ReattachState.Delay << ")");
            ctx.Schedule(perTablet->ReattachState.Delay, new TEvPrivate::TEvReattachToShard(msg->TabletId));
            ++perTablet->RestartCount;
            return;
        }

        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " shard " << msg->TabletId << " lost pipe while waiting for reply"
                << (msg->NotDelivered ? " (last message not delivered)" : ""));

        ComplainingDatashards.push_back(msg->TabletId);

        TStringStream explanation;
        explanation << "tx state unknown for shard " << msg->TabletId << " with txid#" << TxId;
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation.Str()));

        auto status = IsReadOnlyRequest()
            ? TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable
            : TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown;
        ReportStatus(status, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
        TxProxyMon->ClientConnectedError->Inc();

        return Die(ctx);
    }

    LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
        NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " lost pipe with unknown endpoint, ignoring");
}

void TDataReq::Handle(TEvDataShard::TEvGetReadTableSinkStateRequest::TPtr &ev, const TActorContext &ctx) {
    auto *response = new TEvDataShard::TEvGetReadTableSinkStateResponse;

    if (!ReadTableRequest) {
        response->Record.MutableStatus()->SetCode(Ydb::StatusIds::GENERIC_ERROR);
        auto *issue = response->Record.MutableStatus()->AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message("not a ReadTable request");
        ctx.Send(ev->Sender, response);
        return;
    }

    auto &rec = response->Record;
    rec.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    rec.SetTxId(TxId);
    rec.SetWallClockAccepted(WallClockAccepted.GetValue());
    rec.SetWallClockResolveStarted(WallClockResolveStarted.GetValue());
    rec.SetWallClockResolved(WallClockResolved.GetValue());
    rec.SetWallClockPrepared(WallClockPrepared.GetValue());
    rec.SetWallClockPlanned(WallClockPlanned.GetValue());
    rec.SetExecTimeoutPeriod(ExecTimeoutPeriod.GetValue());
    rec.SetSelectedCoordinator(SelectedCoordinator);
    rec.SetRequestSource(ToString(RequestSource));
    rec.SetRequestVersion(ReadTableRequest->RequestVersion);
    rec.SetResponseVersion(ReadTableRequest->ResponseVersion);
    for (auto &pr : ReadTableRequest->ClearanceSenders)
        rec.AddClearanceRequests()->SetId(pr.first);
    for (auto &pr : ReadTableRequest->QuotaRequests)
        rec.AddQuotaRequests()->SetId(pr.second.ShardId);
    for (auto &pr : ReadTableRequest->StreamingShards)
        rec.AddStreamingShards()->SetId(pr.first);
    auto queue = ReadTableRequest->KeySpace.GetShardsQueue();
    while (!queue.empty()) {
        rec.AddShardsQueue()->SetId(queue.front());
        queue.pop();
    }
    rec.SetOrdered(ReadTableRequest->Ordered);
    rec.SetRowsLimited(ReadTableRequest->RowsLimited);
    rec.SetRowsRemain(ReadTableRequest->RowsRemain);

    ctx.Send(ev->Sender, response);
}

void TDataReq::Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr &ev, const TActorContext &ctx) {
    if (ReadTableRequest) {
        ctx.Send(ev->Forward(RequestSource));
    } else {
        auto *response = new TEvDataShard::TEvGetReadTableStreamStateResponse;
        response->Record.MutableStatus()->SetCode(Ydb::StatusIds::GENERIC_ERROR);
        auto *issue = response->Record.MutableStatus()->AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message("Proxy actor doesn't process ReadTable request");
        ctx.Send(ev->Sender, response);
    }
}

void TDataReq::Handle(TEvTxProcessing::TEvStreamClearanceRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;
    ui64 shard = rec.GetShardId();

    Y_ABORT_UNLESS(ReadTableRequest);

    // Handle shard restart. For now temporary snapshots are used by scan transaction
    // and therefore any shard restart may cause inconsistent response.
    if (ReadTableRequest->ClearanceSenders.contains(shard) || PerTablet[shard].StreamCleared) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                        "Cannot recover from shard restart, shard: " << shard << ", txid: " << TxId);

            // We must send response to current request too
            auto response = MakeHolder<TEvTxProcessing::TEvStreamClearanceResponse>();
            response->Record.SetTxId(TxId);
            response->Record.SetCleared(false);
            ctx.Send(ev->Sender, response.Release());

            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::ERROR, true, ctx);
            Die(ctx);
            return;
        }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Got clearance request, shard: " << rec.GetShardId()
                << ", txid: " << rec.GetTxId());

    ctx.Send(ev->Sender, new TEvTxProcessing::TEvStreamClearancePending(TxId));

    ReadTableRequest->ClearanceSenders.emplace(shard, ev->Sender);
    ReadTableRequest->KeySpace.AddRange(rec.GetKeyRange(), shard);

    if (ReadTableRequest->KeySpace.IsFull()) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "Collected all clerance requests, txid: " << TxId);
    }

    ProcessStreamClearance(true, ctx);
}

void TDataReq::Handle(TEvTxProcessing::TEvStreamIsDead::TPtr &ev, const TActorContext &ctx)
{
    Y_UNUSED(ev);
    Y_ABORT_UNLESS(ReadTableRequest);

    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Abort read table transaction because stream is dead txid: " << TxId);

    ReportStatus(TEvTxUserProxy::TResultStatus::ExecComplete, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
    Die(ctx);
}

void TDataReq::HandleResolve(TEvTxProcessing::TEvStreamIsDead::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Y_ABORT_UNLESS(ReadTableRequest);
    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
        "Abort read table transaction because stream is dead txid: " << TxId);

    ReportStatus(TEvTxUserProxy::TResultStatus::ExecComplete, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
    Become(&TThis::StateResolveTimeout);
}

void TDataReq::Handle(TEvTxProcessing::TEvStreamQuotaRequest::TPtr &ev, const TActorContext &ctx)
{
    Y_DEBUG_ABORT_UNLESS(ReadTableRequest);

    auto id = ReadTableRequest->QuotaRequestId++;
    TReadTableRequest::TQuotaRequest req{ev->Sender, ev->Get()->Record.GetShardId()};
    ReadTableRequest->QuotaRequests.insert(std::make_pair(id, req));
    ctx.Send(RequestSource, ev->Release().Release(), 0, id);
}

void TDataReq::Handle(TEvTxProcessing::TEvStreamQuotaResponse::TPtr &ev, const TActorContext &ctx)
{
    Y_DEBUG_ABORT_UNLESS(ReadTableRequest);

    auto it = ReadTableRequest->QuotaRequests.find(ev->Cookie);
    Y_DEBUG_ABORT_UNLESS(it != ReadTableRequest->QuotaRequests.end());

    if (ReadTableRequest->RowsLimited)
        ev->Get()->Record.SetRowLimit(ReadTableRequest->RowsRemain);

    ctx.Send(it->second.Sender, ev->Release().Release());
    ReadTableRequest->QuotaRequests.erase(it);
}

void TDataReq::Handle(TEvTxProcessing::TEvStreamQuotaRelease::TPtr &ev, const TActorContext &ctx)
{
    ctx.Send(ev->Forward(RequestSource));
}

void TDataReq::HandleExecTimeoutResolve(const TActorContext &ctx) {
    LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " HANDLE ExecTimeout TDataReq");
    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
    TxProxyMon->ExecTimeout->Inc();
    Become(&TThis::StateResolveTimeout);
}

void TDataReq::HandleExecTimeout(const TActorContext &ctx) {
    LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " HANDLE ExecTimeout TDataReq");
    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
    TxProxyMon->ExecTimeout->Inc();
    return Die(ctx);
}

void TDataReq::MergeResult(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    NKikimrTxDataShard::TEvProposeTransactionResult &record = ev->Get()->Record;

    ResultsReceivedCount++;
    ResultsReceivedSize += record.GetTxResult().size();

    WallClockLastExecReply = Now();
    if (WallClockFirstExecReply.GetValue() == 0)
        WallClockFirstExecReply = WallClockLastExecReply;

    const ui64 tabletId = record.GetOrigin();
    TPerTablet *perTablet = PerTablet.FindPtr(tabletId);

    if (FlatMKQLRequest && FlatMKQLRequest->CollectStats) {
        perTablet->Stats.Reset(new NKikimrQueryStats::TTxStats);
        perTablet->Stats->Swap(record.MutableTxStats());
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "Got stats for txid: " << TxId << " datashard: " << tabletId << " " << *perTablet->Stats);
    }

    if (StreamResponse) {
        return FinishShardStream(ev, ctx);
    }

    Y_ABORT_UNLESS(FlatMKQLRequest);
    NCpuTime::TCpuTimer timer;
    NMiniKQL::IEngineFlat &engine = *FlatMKQLRequest->Engine;

    for (const auto& lock : record.GetTxLocks()) {
        engine.AddTxLock(NMiniKQL::IEngineFlat::TTxLock(
            lock.GetLockId(),
            lock.GetDataShard(),
            lock.GetGeneration(),
            lock.GetCounter(),
            lock.GetSchemeShard(),
            lock.GetPathId()));
    }

    if (record.HasTabletInfo()) {
        const auto& info = record.GetTabletInfo();

        NMiniKQL::IEngineFlat::TTabletInfo::TTxInfo txInfo;
        txInfo.StepTxId = {record.GetStep(), record.GetTxId()};
        txInfo.Status = record.GetStatus();
        txInfo.PrepareArriveTime = TInstant::MicroSeconds(record.GetPrepareArriveTime());
        txInfo.ProposeLatency = TDuration::MilliSeconds(record.GetProposeLatency());
        txInfo.ExecLatency = TDuration::MilliSeconds(record.GetExecLatency());

        engine.AddTabletInfo(NMiniKQL::IEngineFlat::TTabletInfo(
            info.GetTabletId(),
            std::pair<ui64, ui64>(info.GetActorId().GetRawX1(), info.GetActorId().GetRawX2()),
            info.GetGeneration(),
            info.GetStep(),
            info.GetIsFollower(),
            std::move(txInfo)
        ));
    }

    const ui64 originShard = record.GetOrigin();
    auto builderIt = FlatMKQLRequest->BalanceCoverageBuilders.find(originShard);
    if (builderIt != FlatMKQLRequest->BalanceCoverageBuilders.end()) {
        if (builderIt->second->AddResult(record.GetBalanceTrackList())) {
            engine.AddShardReply(originShard, record.GetTxResult());
            if (builderIt->second->IsComplete()) {
                engine.FinalizeOriginReplies(originShard);
                FlatMKQLRequest->BalanceCoverageBuilders.erase(builderIt);
            }
        }
    }

    if (FlatMKQLRequest->BalanceCoverageBuilders.empty()) {
        return MakeFlatMKQLResponse(ctx, timer);
    } else {
        CpuTime += timer.GetTime();
    }
}

void TDataReq::MakeFlatMKQLResponse(const TActorContext &ctx, const NCpuTime::TCpuTimer& timer) {
    NMiniKQL::IEngineFlat &engine = *FlatMKQLRequest->Engine;
    engine.SetStepTxId({ PlanStep, TxId });

    engine.SetDeadline(WallClockAccepted + ExecTimeoutPeriod);
    if (FlatMKQLRequest->Limits.GetComputeNodeMemoryLimitBytes()) {
        engine.SetMemoryLimit(FlatMKQLRequest->Limits.GetComputeNodeMemoryLimitBytes());
    }

    engine.BuildResult();
    FlatMKQLRequest->EngineResponseStatus = engine.GetStatus();

    switch (FlatMKQLRequest->EngineResponseStatus) {
    case NMiniKQL::IEngineFlat::EStatus::Unknown:
    case NMiniKQL::IEngineFlat::EStatus::Error:
        LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " MergeResult ExecError TDataReq marker# P16");
        CpuTime += timer.GetTime();
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::ERROR, true, ctx);
        TxProxyMon->MergeResultMiniKQLExecError->Inc();
        return Die(ctx);
    case NMiniKQL::IEngineFlat::EStatus::Complete:
    case NMiniKQL::IEngineFlat::EStatus::Aborted: {
        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " MergeResult ExecComplete TDataReq marker# P17");

        auto fillResult = engine.FillResultValue(FlatMKQLRequest->EngineEvaluatedResponse);
        switch (fillResult) {
        case NMiniKQL::IEngineFlat::EResult::Ok:
            CpuTime += timer.GetTime();
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);
            TxProxyMon->MergeResultMiniKQLExecComplete->Inc();
            break;
        case NMiniKQL::IEngineFlat::EResult::ResultTooBig:
            LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " MergeResult Result too large TDataReq marker# P18");

            FlatMKQLRequest->EngineResultStatusCode = fillResult;
            CpuTime += timer.GetTime();
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable, NKikimrIssues::TStatusIds::ERROR, true, ctx);
            TxProxyMon->MergeResultMiniKQLExecError->Inc();
            break;
        case NMiniKQL::IEngineFlat::EResult::Cancelled:
            LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " MergeResult Execution was cancelled TDataReq marker# P20");

            CpuTime += timer.GetTime();
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
            TxProxyMon->ExecTimeout->Inc();
            break;
        default:
            LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " MergeResult Error: " << (ui32)fillResult << " TDataReq marker# P19");
            FlatMKQLRequest->EngineResultStatusCode = fillResult;
            CpuTime += timer.GetTime();
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::ERROR, true, ctx);
            TxProxyMon->MergeResultMiniKQLExecError->Inc();
            break;
        }

        return Die(ctx);
    }
    default:
        Y_ABORT("unknown engine status# %" PRIu32 " txid# %" PRIu64, (ui32)FlatMKQLRequest->EngineResponseStatus, (ui64)TxId);
    }
}

void TDataReq::ProcessStreamResponseData(TEvDataShard::TEvProposeTransactionResult::TPtr &ev,
                                         const TActorContext &ctx)
{
    Y_DEBUG_ABORT_UNLESS(ReadTableRequest);

    ctx.Send(ev->Sender, new TEvTxProcessing::TEvStreamDataAck);

    auto &rec = ev->Get()->Record;
    ReadTableRequest->ResponseData = rec.GetTxResult();
    ReadTableRequest->ResponseDataFrom = rec.GetOrigin();
    if (rec.HasApiVersion()) {
        ReadTableRequest->ResponseVersion = rec.GetApiVersion();
    }

    if (ReadTableRequest->RowsLimited) {
        auto rows = rec.RowOffsetsSize();
        if (rows > ReadTableRequest->RowsRemain) {
            ReadTableRequest->ResponseData.resize(rec.GetRowOffsets(ReadTableRequest->RowsRemain));
            ReadTableRequest->RowsRemain = 0;
        } else {
            ReadTableRequest->RowsRemain -= rows;
        }
    }

    ReportStatus(TEvTxUserProxy::TResultStatus::ExecResponseData, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);

    if (ReadTableRequest->RowsLimited && !ReadTableRequest->RowsRemain)
        FinishStreamResponse(ctx);
}

void TDataReq::FinishShardStream(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {

    auto &rec = ev->Get()->Record;
    auto shard = rec.GetOrigin();

    Y_DEBUG_ABORT_UNLESS(ReadTableRequest->StreamingShards.contains(shard));
    ReadTableRequest->StreamingShards.erase(shard);

    if (ReadTableRequest->KeySpace.IsFull()
        && ReadTableRequest->StreamingShards.empty()
        && ReadTableRequest->KeySpace.IsShardsQueueEmpty()) {
        FinishStreamResponse(ctx);
    } else {
        ProcessStreamClearance(true, ctx);
    }
}

void TDataReq::FinishStreamResponse(const TActorContext &ctx) {
    ReportStatus(TEvTxUserProxy::TResultStatus::ExecComplete, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);
    Die(ctx);
}

NSchemeCache::TDomainInfo::TPtr FindDomainInfo(NSchemeCache::TSchemeCacheRequest &cacheRequest) {
    for (const auto& entry :cacheRequest.ResultSet) {
        if (entry.DomainInfo) {
            return entry.DomainInfo;
        }
    }
    return nullptr;
}

ui64 GetFirstTablet(NSchemeCache::TSchemeCacheRequest &cacheRequest) {
    Y_ABORT_UNLESS(!cacheRequest.ResultSet.empty());

    NSchemeCache::TSchemeCacheRequest::TEntry& firstEntry= *cacheRequest.ResultSet.begin();
    NKikimr::TKeyDesc& firstKey = *firstEntry.KeyDescription;
    Y_ABORT_UNLESS(!firstKey.GetPartitions().empty());
    return firstKey.GetPartitions().begin()->ShardId;
}

const TDomainsInfo::TDomain& TDataReq::SelectDomain(NSchemeCache::TSchemeCacheRequest& /*cacheRequest*/, const TActorContext &ctx) {
    return *AppData(ctx)->DomainsInfo->GetDomain();
}

ui64 TDataReq::SelectCoordinator(NSchemeCache::TSchemeCacheRequest &cacheRequest, const TActorContext &ctx) {
    auto domainInfo = FindDomainInfo(cacheRequest);
    if (domainInfo) {
        return domainInfo->Coordinators.Select(TxId);
    }

    // no tablets keys are found in requests keys
    // it take place when a transaction have only checks locks
    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Actor# " << ctx.SelfID.ToString() <<
                " txid# " << TxId <<
                " SelectCoordinator unable to choose coordinator from resolved keys," <<
                " will try to pick it from TEvProposeTransactionResult from datashard");
    return 0;
}

void TDataReq::FailProposedRequest(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, TString errMsg, const TActorContext &ctx) {
    LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " FailProposedRequest: " << errMsg << " Status# " << status);

    DatashardErrors = errMsg;
    // Cancel the Tx on all shards (so we pass invalid tablet id)
    CancelProposal(0);
    ReportStatus(status, NKikimrIssues::TStatusIds::ERROR, true, ctx);
    Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
    TxProxyMon->TxResultError->Inc();
}

bool TDataReq::CheckDomainLocality(NSchemeCache::TSchemeCacheRequest &cacheRequest) {
    NSchemeCache::TDomainInfo::TPtr domainInfo;
    for (const auto& entry :cacheRequest.ResultSet) {
        if (TSysTables::IsSystemTable(entry.KeyDescription->TableId)) {
            continue;
        }

        Y_ABORT_UNLESS(entry.DomainInfo);

        if (!domainInfo) {
            domainInfo = entry.DomainInfo;
            continue;
        }

        if (domainInfo->DomainKey != entry.DomainInfo->DomainKey) {
            return false;
        }
    }

    return true;
}

void TDataReq::RegisterPlan(const TActorContext &ctx) {
    WallClockPrepared = Now();
    TDomainsInfo *domainsInfo = AppData(ctx)->DomainsInfo.Get();
    Y_ABORT_UNLESS(domainsInfo);

    ui64 totalReadSize = 0;
    for (const auto &xp : PerTablet) {
        totalReadSize += xp.second.ReadSize;
    }

    // Check reply size
    ui64 sizeLimit = RequestControls.PerRequestDataSizeLimit;
    if (FlatMKQLRequest && FlatMKQLRequest->Limits.GetTotalReadSizeLimitBytes()) {
        sizeLimit = sizeLimit
            ? std::min(sizeLimit, FlatMKQLRequest->Limits.GetTotalReadSizeLimitBytes())
            : FlatMKQLRequest->Limits.GetTotalReadSizeLimitBytes();
    }

    if (totalReadSize > sizeLimit) {
        FailProposedRequest(
                    TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError,
                    Sprintf("Transaction total read size %" PRIu64 " exceeded limit %" PRIu64, totalReadSize, sizeLimit),
                    ctx);
        return;
    }

    // Check per tablet incoming read set size
    sizeLimit = RequestControls.PerShardIncomingReadSetSizeLimit;
    for (const auto &xp : PerTablet) {
        ui64 targetTabletId = xp.first;
        ui64 rsSize = xp.second.IncomingReadSetsSize;
        if (rsSize > sizeLimit) {
            ComplainingDatashards.push_back(targetTabletId);
            FailProposedRequest(
                        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError,
                        Sprintf("Transaction incoming read set size %" PRIu64 " for tablet %" PRIu64 " exceeded limit %" PRIu64,
                                rsSize, targetTabletId, sizeLimit),
                        ctx);
            return;
        }
    }

    if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportPrepared)
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyPrepared, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);

    Y_ABORT_UNLESS(SelectedCoordinator, "shouldn't be run with null SelectedCoordinator");
    TAutoPtr<TEvTxProxy::TEvProposeTransaction> req(new TEvTxProxy::TEvProposeTransaction(SelectedCoordinator, TxId, 0,
        AggrMinStep, AggrMaxStep));

    auto *reqAffectedSet = req->Record.MutableTransaction()->MutableAffectedSet();
    reqAffectedSet->Reserve(PerTablet.size());

    for (const auto &xp : PerTablet) {
        auto x = reqAffectedSet->Add();
        x->SetTabletId(xp.first);
        x->SetFlags(xp.second.AffectedFlags);
    }

    LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
        "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
        << " SEND EvProposeTransaction to# " << SelectedCoordinator << " Coordinator marker# P7 ");

    Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(req.Release(), SelectedCoordinator, true));
    CoordinatorStatus = ECoordinatorStatus::Waiting;
    Become(&TThis::StateWaitPlan);
}

void TDataReq::HandleUndeliveredResolve(TEvents::TEvUndelivered::TPtr &, const TActorContext &ctx) {
    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, "unexpected event delivery problem"));
    ReportStatus(TEvTxUserProxy::TResultStatus::Unknown, NKikimrIssues::TStatusIds::INTERNAL_ERROR, true, ctx);
    Become(&TThis::StateResolveTimeout);
}

void TDataReq::Handle(TEvents::TEvUndelivered::TPtr &, const TActorContext &ctx) {
    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, "unexpected event delivery problem"));
    ReportStatus(TEvTxUserProxy::TResultStatus::Unknown, NKikimrIssues::TStatusIds::INTERNAL_ERROR, true, ctx);
    return Die(ctx);
}

void TDataReq::HandleWatchdog(const TActorContext &ctx) {
    const TDuration fromStart = Now() - this->WallClockAccepted;
    LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_INFO, NKikimrServices::TX_PROXY, TxId,
              "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
              << " Transactions still running for " << fromStart);
    ctx.Schedule(TDuration::MilliSeconds(KIKIMR_DATAREQ_WATCHDOG_PERIOD), new TEvPrivate::TEvProxyDataReqOngoingTransactionsWatchdog());
}

void TDataReq::SendStreamClearanceResponse(ui64 shard, bool cleared, const TActorContext &ctx)
{
    TAutoPtr<TEvTxProcessing::TEvStreamClearanceResponse> response
        = new TEvTxProcessing::TEvStreamClearanceResponse;
    response->Record.SetTxId(TxId);
    response->Record.SetCleared(cleared);

    // For unordered streams we may get multiple entries for the same
    // shard in clearance queue due to shard restarts. Avoid multiple
    // responses by removing sender from the senders map.
    auto it = ReadTableRequest->ClearanceSenders.find(shard);
    if (it == ReadTableRequest->ClearanceSenders.end()) {
        LOG_WARN_S(ctx, NKikimrServices::TX_PROXY,
                   "No sender for clearance request, shard: " << shard
                   << ", txid: " << TxId << ", cleared: " << cleared);
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Send stream clearance, shard: " << shard
                << ", txid: " << TxId << ", cleared: " << cleared);

    ctx.Send(it->second, response.Release());

    if (cleared) {
        ReadTableRequest->StreamingShards.insert(*it);
        PerTablet[shard].StreamCleared = true;
    }

    ReadTableRequest->ClearanceSenders.erase(it);
}

void TDataReq::ProcessNextStreamClearance(bool cleared, const TActorContext &ctx)
{
    Y_ABORT_UNLESS(ReadTableRequest);
    Y_DEBUG_ABORT_UNLESS(!ReadTableRequest->KeySpace.IsShardsQueueEmpty());

    auto shard = ReadTableRequest->KeySpace.ShardsQueueFront();
    ReadTableRequest->KeySpace.ShardsQueuePop();

    SendStreamClearanceResponse(shard, cleared, ctx);
}

void TDataReq::ProcessStreamClearance(bool cleared, const TActorContext &ctx)
{
    if (!ReadTableRequest)
        return;

    if (cleared) {
        auto &cfg = AppData(ctx)->StreamingConfig.GetOutputStreamConfig();

        ui32 limit = ReadTableRequest->Ordered ? 1 : cfg.GetMaxStreamingShards();
        while (!ReadTableRequest->KeySpace.IsShardsQueueEmpty()
               && limit > ReadTableRequest->StreamingShards.size()) {
            ProcessNextStreamClearance(cleared, ctx);
        }
    } else {
        if (ReadTableRequest->Ordered && !ReadTableRequest->KeySpace.IsFull()) {
            ReadTableRequest->KeySpace.FlushShardsToQueue();
        }

        while (!ReadTableRequest->KeySpace.IsShardsQueueEmpty())
            ProcessNextStreamClearance(cleared, ctx);
    }
}

bool TDataReq::ParseRangeKey(const NKikimrMiniKQL::TParams &proto,
                             TConstArrayRef<NScheme::TTypeInfo> keyType,
                             TSerializedCellVec &buf,
                             EParseRangeKeyExp exp)
{
    TVector<TCell> key;
    TVector<TString> memoryOwner;
    if (proto.HasValue()) {
        if (!proto.HasType()) {
            UnresolvedKeys.push_back("No type was specified in the range key tuple");
            return false;
        }

        auto& value = proto.GetValue();
        auto& type = proto.GetType();
        TString errStr;
        bool res = NMiniKQL::CellsFromTuple(&type, value, keyType, {}, true, key, errStr, memoryOwner);
        if (!res) {
            UnresolvedKeys.push_back("Failed to parse range key tuple: " + errStr);
            return false;
        }
    }

    switch (exp) {
        case EParseRangeKeyExp::TO_NULL:
            key.resize(keyType.size());
        break;
        case EParseRangeKeyExp::NONE:
        break;
    }

    buf = TSerializedCellVec(key);
    return true;
}

bool TDataReq::IsReadOnlyRequest() const {
    if (FlatMKQLRequest) {
        return FlatMKQLRequest->ReadOnlyProgram;
    } else if (ReadTableRequest) {
        return true;
    }

    Y_ABORT("No request");
}

IActor* CreateTxProxyDataReq(const TTxProxyServices &services, const ui64 txid, const TIntrusivePtr<NKikimr::NTxProxy::TTxProxyMon>& mon,
                             const TRequestControls& requestControls) {
    return new NTxProxy::TDataReq(services, txid, mon, requestControls);
}

}}


#define STATUS_TO_STRING_IMPL_ITEM(name, ...) \
    case NKikimr::NTxProxy::TDataReq::TPerTablet::ETabletStatus::name: \
        o << #name; \
        return;

template<>
inline void Out<NKikimr::NTxProxy::TDataReq::TPerTablet::ETabletStatus>(IOutputStream& o,
        NKikimr::NTxProxy::TDataReq::TPerTablet::ETabletStatus x) {
    switch (x) {
        DATA_REQ_PER_TABLET_STATUS_MAP(STATUS_TO_STRING_IMPL_ITEM)
    default:
        o << static_cast<int>(x);
        return;
    }
}

#undef STATUS_TO_STRING_IMPL_ITEM
