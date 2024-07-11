#include "datashard_impl.h"
#include "datashard_txs.h"
#include "datashard_locks_db.h"
#include "probes.h"

#include <ydb/core/base/interconnect_channels.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/protos/datashard_config.pb.h>

#include <ydb/library/actors/core/monotonic_provider.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr {

IActor* CreateDataShard(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NDataShard::TDataShard(tablet, info);
}

namespace NDataShard {

using namespace NSchemeShard;
using namespace NTabletFlatExecutor;

// NOTE: We really want to batch log records by default in datashards!
// But in unittests we want to test both scenarios
bool gAllowLogBatchingDefaultValue = true;

TDuration gDbStatsReportInterval = TDuration::Seconds(10);
ui64 gDbStatsDataSizeResolution = 10*1024*1024;
ui64 gDbStatsRowCountResolution = 100000;

// The first byte is 0x01 so it would fail to parse as an internal tablet protobuf
TStringBuf SnapshotTransferReadSetMagic("\x01SRS", 4);


/**
 * A special subclass of TMiniKQLFactory that uses correct row versions for writes
 */
class TDataShardMiniKQLFactory : public NMiniKQL::TMiniKQLFactory {
public:
    TDataShardMiniKQLFactory(TDataShard* self)
        : Self(self)
    { }

    TRowVersion GetWriteVersion(const TTableId& tableId) const override {
        using Schema = TDataShard::Schema;

        Y_VERIFY_S(tableId.PathId.OwnerId == Self->TabletID(),
            "Unexpected table " << tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
            << " for datashard " << Self->TabletID()
            << " in a local minikql tx");

        if (tableId.PathId.LocalPathId < Schema::MinLocalTid) {
            // System tables are not versioned
            return TRowVersion::Min();
        }

        // Write user tables with a minimal safe version (avoiding snapshots)
        return Self->GetLocalReadWriteVersions().WriteVersion;
    }

    TRowVersion GetReadVersion(const TTableId& tableId) const override {
        using Schema = TDataShard::Schema;

        Y_VERIFY_S(tableId.PathId.OwnerId == Self->TabletID(),
                   "Unexpected table " << tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
                                       << " for datashard " << Self->TabletID()
                                       << " in a local minikql tx");

        if (tableId.PathId.LocalPathId < Schema::MinLocalTid) {
            // System tables are not versioned
            return TRowVersion::Max();
        }

        return Self->GetLocalReadWriteVersions().ReadVersion;
    }

private:
    TDataShard* const Self;
};


class TDatashardKeySampler : public NMiniKQL::IKeyAccessSampler {
    TDataShard& Self;
public:
    TDatashardKeySampler(TDataShard& self) : Self(self)
    {}
    void AddSample(const TTableId& tableId, const TArrayRef<const TCell>& key) override {
        Self.SampleKeyAccess(tableId, key);
    }
};


TDataShard::TDataShard(const TActorId &tablet, TTabletStorageInfo *info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new TDataShardMiniKQLFactory(this))
    , PipeClientCacheConfig(new NTabletPipe::TBoundedClientCacheConfig())
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(PipeClientCacheConfig, GetPipeClientConfig()))
    , ResendReadSetPipeTracker(*PipeClientCache)
    , SchemeShardPipeRetryPolicy({})
    , PathOwnerId(INVALID_TABLET_ID)
    , CurrentSchemeShardId(INVALID_TABLET_ID)
    , LastKnownMediator(INVALID_TABLET_ID)
    , RegistrationSended(false)
    , LoanReturnTracker(info->TabletID)
    , MvccSwitchState(TSwitchState::READY)
    , SplitSnapshotStarted(false)
    , SplitSrcSnapshotSender(this)
    , DstSplitOpId(0)
    , SrcSplitOpId(0)
    , State(TShardState::Uninitialized)
    , LastLocalTid(Schema::MinLocalTid)
    , NextSeqno(1)
    , NextChangeRecordOrder(1)
    , LastChangeRecordGroup(1)
    , TxReadSizeLimit(0)
    , StatisticsDisabled(0)
    , DisabledKeySampler(new NMiniKQL::TNoopKeySampler())
    , EnabledKeySampler(new TDatashardKeySampler(*this))
    , CurrentKeySampler(DisabledKeySampler)
    , TransQueue(this)
    , OutReadSets(this)
    , Pipeline(this)
    , SysLocks(this)
    , SnapshotManager(this)
    , SchemaSnapshotManager(this)
    , VolatileTxManager(this)
    , ConflictsCache(this)
    , DisableByKeyFilter(0, 0, 1)
    , MaxTxInFly(15000, 0, 100000)
    , MaxTxLagMilliseconds(5*60*1000, 0, 30*24*3600*1000ll)
    , CanCancelROWithReadSets(0, 0, 1)
    , PerShardReadSizeLimit(5368709120, 0, 107374182400)
    , CpuUsageReportThreshlodPercent(60, -1, 146)
    , CpuUsageReportIntervalSeconds(60, 0, 365*86400)
    , HighDataSizeReportThreshlodBytes(10ull<<30, -1, Max<i64>())
    , HighDataSizeReportIntervalSeconds(60, 0, 365*86400)
    , DataTxProfileLogThresholdMs(0, 0, 86400000)
    , DataTxProfileBufferThresholdMs(0, 0, 86400000)
    , DataTxProfileBufferSize(0, 1000, 100)
    , BackupReadAheadLo(0, 0, 64*1024*1024)
    , BackupReadAheadHi(0, 0, 128*1024*1024)
    , TtlReadAheadLo(0, 0, 64*1024*1024)
    , TtlReadAheadHi(0, 0, 128*1024*1024)
    , EnableLockedWrites(1, 0, 1)
    , MaxLockedWritesPerKey(1000, 0, 1000000)
    , EnableLeaderLeases(1, 0, 1)
    , MinLeaderLeaseDurationUs(250000, 1000, 5000000)
    , DataShardSysTables(InitDataShardSysTables(this))
    , ChangeSenderActivator(info->TabletID)
    , ChangeExchangeSplitter(this)
{
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.Get();

    RegisterDataShardProbes();
}

NTabletPipe::TClientConfig TDataShard::GetPipeClientConfig() {
    NTabletPipe::TClientConfig config;
    config.CheckAliveness = true;
    config.RetryPolicy = {
        .RetryLimitCount = 30,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(500),
        .BackoffMultiplier = 2,
    };
    return config;
}

void TDataShard::OnDetach(const TActorContext &ctx) {
    Cleanup(ctx);
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "OnDetach: " << TabletID());
    return Die(ctx);
}

void TDataShard::OnTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx) {
    const auto* msg = ev->Get();

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "OnTabletStop: " << TabletID() << " reason = " << msg->GetReason());

    if (!IsFollower() && GetState() == TShardState::Ready) {
        if (!Stopping) {
            Stopping = true;
            OnStopGuardStarting(ctx);
            Execute(new TTxStopGuard(this), ctx);
        }

        switch (msg->GetReason()) {
            case TEvTablet::TEvTabletStop::ReasonStop:
            case TEvTablet::TEvTabletStop::ReasonDemoted:
            case TEvTablet::TEvTabletStop::ReasonIsolated:
                // Keep trying to stop gracefully
                return;

            case TEvTablet::TEvTabletStop::ReasonUnknown:
            case TEvTablet::TEvTabletStop::ReasonStorageBlocked:
            case TEvTablet::TEvTabletStop::ReasonStorageFailure:
                // New commits are impossible, stop immediately
                break;
        }
    } else {
        Stopping = true;
    }

    return TTabletExecutedFlat::OnTabletStop(ev, ctx);
}

void TDataShard::TTxStopGuard::Complete(const TActorContext &ctx) {
    Self->OnStopGuardComplete(ctx);
}

void TDataShard::OnStopGuardStarting(const TActorContext &ctx) {
    // Handle immediate ops that have completed BuildAndWaitDependencies
    for (const auto &kv : Pipeline.GetImmediateOps()) {
        const auto &op = kv.second;
        if (op->OnStopping(*this, ctx)) {
            Pipeline.AddCandidateOp(op);
            PlanQueue.Progress(ctx);
        }
    }

    // Handle prepared ops by notifying about imminent shutdown
    for (const auto &kv : TransQueue.GetTxsInFly()) {
        const auto &op = kv.second;
        if (op->OnStopping(*this, ctx)) {
            Pipeline.AddCandidateOp(op);
            PlanQueue.Progress(ctx);
        }
    }
}

void TDataShard::OnStopGuardComplete(const TActorContext &ctx) {
    // We have cleanly completed the last commit
    ctx.Send(Tablet(), new TEvTablet::TEvTabletStopped());
}

void TDataShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "OnTabletDead: " << TabletID());
    Cleanup(ctx);
    return Die(ctx);
}

void TDataShard::Cleanup(const TActorContext& ctx) {
    //PipeClientCache->Detach(ctx);
    if (RegistrationSended) {
        ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvUnsubscribeReadStep());
        ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvUnregisterTablet(TabletID()));
    }

    if (Pipeline.HasRestore()) {
        auto op = Pipeline.FindOp(Pipeline.CurrentSchemaTxId());
        if (op && op->IsWaitingForAsyncJob()) {
            TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
            Y_ABORT_UNLESS(tx);
            tx->KillAsyncJobActor(ctx);
        }
    }
}

void TDataShard::IcbRegister() {
    if (!IcbRegistered) {
        auto* appData = AppData();

        appData->Icb->RegisterSharedControl(DisableByKeyFilter, "DataShardControls.DisableByKeyFilter");
        appData->Icb->RegisterSharedControl(MaxTxInFly, "DataShardControls.MaxTxInFly");
        appData->Icb->RegisterSharedControl(MaxTxLagMilliseconds, "DataShardControls.MaxTxLagMilliseconds");
        appData->Icb->RegisterSharedControl(DataTxProfileLogThresholdMs, "DataShardControls.DataTxProfile.LogThresholdMs");
        appData->Icb->RegisterSharedControl(DataTxProfileBufferThresholdMs, "DataShardControls.DataTxProfile.BufferThresholdMs");
        appData->Icb->RegisterSharedControl(DataTxProfileBufferSize, "DataShardControls.DataTxProfile.BufferSize");

        appData->Icb->RegisterSharedControl(CanCancelROWithReadSets, "DataShardControls.CanCancelROWithReadSets");
        appData->Icb->RegisterSharedControl(PerShardReadSizeLimit, "TxLimitControls.PerShardReadSizeLimit");
        appData->Icb->RegisterSharedControl(CpuUsageReportThreshlodPercent, "DataShardControls.CpuUsageReportThreshlodPercent");
        appData->Icb->RegisterSharedControl(CpuUsageReportIntervalSeconds, "DataShardControls.CpuUsageReportIntervalSeconds");
        appData->Icb->RegisterSharedControl(HighDataSizeReportThreshlodBytes, "DataShardControls.HighDataSizeReportThreshlodBytes");
        appData->Icb->RegisterSharedControl(HighDataSizeReportIntervalSeconds, "DataShardControls.HighDataSizeReportIntervalSeconds");

        appData->Icb->RegisterSharedControl(BackupReadAheadLo, "DataShardControls.BackupReadAheadLo");
        appData->Icb->RegisterSharedControl(BackupReadAheadHi, "DataShardControls.BackupReadAheadHi");

        appData->Icb->RegisterSharedControl(TtlReadAheadLo, "DataShardControls.TtlReadAheadLo");
        appData->Icb->RegisterSharedControl(TtlReadAheadHi, "DataShardControls.TtlReadAheadHi");

        appData->Icb->RegisterSharedControl(EnableLockedWrites, "DataShardControls.EnableLockedWrites");
        appData->Icb->RegisterSharedControl(MaxLockedWritesPerKey, "DataShardControls.MaxLockedWritesPerKey");

        appData->Icb->RegisterSharedControl(EnableLeaderLeases, "DataShardControls.EnableLeaderLeases");
        appData->Icb->RegisterSharedControl(MinLeaderLeaseDurationUs, "DataShardControls.MinLeaderLeaseDurationUs");

        IcbRegistered = true;
    }
}

bool TDataShard::ReadOnlyLeaseEnabled() {
    IcbRegister();
    ui64 value = EnableLeaderLeases;
    return value != 0;
}

TDuration TDataShard::ReadOnlyLeaseDuration() {
    IcbRegister();
    ui64 value = MinLeaderLeaseDurationUs;
    return TDuration::MicroSeconds(value);
}

void TDataShard::OnActivateExecutor(const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TDataShard::OnActivateExecutor: tablet " << TabletID() << " actor " << ctx.SelfID);

    IcbRegister();

    // OnActivateExecutor might be called multiple times for a follower
    // but the counters should be initialized only once
    if (TabletCountersPtr) {
        Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    }
    Y_ABORT_UNLESS(TabletCounters);

    AllocCounters = TAlignedPagePoolCounters(AppData(ctx)->Counters, "datashard");

    if (!Executor()->GetStats().IsFollower) {
        Execute(CreateTxInitSchema(), ctx);
        Become(&TThis::StateInactive);
    } else {
        SyncConfig();
        State = TShardState::Readonly;
        FollowerState = { };
        Become(&TThis::StateWorkAsFollower);
        SignalTabletActive(ctx);
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "Follower switched to work state: " << TabletID());
    }
}

void TDataShard::SwitchToWork(const TActorContext &ctx) {
    if (IsMvccEnabled() && (
        SnapshotManager.GetPerformedUnprotectedReads() ||
        SnapshotManager.GetImmediateWriteEdge().Step > SnapshotManager.GetCompleteEdge().Step))
    {
        // We will need to wait until mediator state is fully restored before
        // processing new immediate transactions.
        MediatorStateWaiting = true;
        CheckMediatorStateRestored();
    }

    SyncConfig();
    PlanQueue.Progress(ctx);
    OutReadSets.ResendAll(ctx);

    Become(&TThis::StateWork);
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "Switched to work state "
         << DatashardStateName(State) << " tabletId " << TabletID());

    if (State == TShardState::Ready && DstSplitDescription) {
        // This shard was created as a result of split/merge (and not e.g. copy table)
        // Signal executor that it should compact borrowed garbage even if this
        // shard has no private data.
        for (const auto& pr : TableInfos) {
            Executor()->AllowBorrowedGarbageCompaction(pr.second->LocalTid);
        }
    }

    // Cleanup any removed snapshots from the previous generation
    Execute(new TTxCleanupRemovedSnapshots(this), ctx);

    if (State != TShardState::Offline) {
        VolatileTxManager.Start(ctx);
    }

    SignalTabletActive(ctx);
    DoPeriodicTasks(ctx);

    NotifySchemeshard(ctx);
    CheckInitiateBorrowedPartsReturn(ctx);
    CheckStateChange(ctx);
}

void TDataShard::SyncConfig() {
    PipeClientCacheConfig->ClientPoolLimit = PipeClientCachePoolLimit();
    PipeClientCache->PopWhileOverflow();
    // TODO[serxa]: dynamic prepared in fly
    //3=SetDynamicPreparedInFly(Config.GetFlowControl().GetPreparedInFlyMax());
}

void TDataShard::SendRegistrationRequestTimeCast(const TActorContext &ctx) {
    if (RegistrationSended)
        return;

    if (!ProcessingParams) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID()
            << " not sending time cast registration request in state "
            << DatashardStateName(State)
            << ": missing processing params");
        return;
    }

    if (State == TShardState::WaitScheme ||
        State == TShardState::SplitDstReceivingSnapshot)
    {
        // We don't have all the necessary info yet
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID()
            << " not sending time cast registration request in state "
            << DatashardStateName(State));
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "Send registration request to time cast "
         << DatashardStateName(State) << " tabletId " << TabletID()
         << " mediators count is " << ProcessingParams->MediatorsSize()
         << " coordinators count is " << ProcessingParams->CoordinatorsSize()
         << " buckets per mediator " << ProcessingParams->GetTimeCastBucketsPerMediator());

    RegistrationSended = true;
    ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvRegisterTablet(TabletID(), *ProcessingParams));

    // Subscribe to all known coordinators
    for (ui64 coordinatorId : ProcessingParams->GetCoordinators()) {
        size_t index = CoordinatorSubscriptions.size();
        auto res = CoordinatorSubscriptionById.emplace(coordinatorId, index);
        if (res.second) {
            auto& subscription = CoordinatorSubscriptions.emplace_back();
            subscription.CoordinatorId = coordinatorId;
            ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvSubscribeReadStep(coordinatorId));
            ++CoordinatorSubscriptionsPending;
        }
    }
}

class TDataShard::TSendArbiterReadSets final : public IVolatileTxCallback {
public:
    TSendArbiterReadSets(TDataShard* self, TVector<THolder<TEvTxProcessing::TEvReadSet>>&& readSets)
        : Self(self)
        , ReadSets(std::move(readSets))
    {}

    void OnCommit(ui64) override {
        // The transaction is persistent and committed
        // Arbiter must now send its outgoing readsets
        Self->SendReadSets(TActivationContext::ActorContextFor(Self->SelfId()), std::move(ReadSets));
    }

    void OnAbort(ui64) override {
        // ReadSets are persistently replaced on abort and sent by volatile tx manager
        // Previously generated readsets must be ignored
    }

private:
    TDataShard* Self;
    TVector<THolder<TEvTxProcessing::TEvReadSet>> ReadSets;
};

void TDataShard::PrepareAndSaveOutReadSets(ui64 step,
                                                  ui64 txId,
                                                  const TMap<std::pair<ui64, ui64>, TString>& txOutReadSets,
                                                  TVector<THolder<TEvTxProcessing::TEvReadSet>> &preparedRS,
                                                  TTransactionContext &txc,
                                                  const TActorContext& ctx)
{
    NIceDb::TNiceDb db(txc.DB);
    OutReadSets.Cleanup(db, ctx);
    if (txOutReadSets.empty())
        return;

    auto* info = VolatileTxManager.FindByTxId(txId);
    if (info && !(info->IsArbiter && info->State != EVolatileTxState::Committed)) {
        info = nullptr;
    }

    ui64 prevSeqno = NextSeqno;
    for (auto& kv : txOutReadSets) {
        ui64 source = kv.first.first;
        ui64 target = kv.first.second;
        TReadSetKey rsKey(txId, TabletID(), source, target);
        if (! OutReadSets.Has(rsKey)) {
            ui64 seqno = NextSeqno++;
            OutReadSets.SaveReadSet(db, seqno, step, rsKey, kv.second);
            preparedRS.push_back(PrepareReadSet(step, txId, source, target, kv.second, seqno));
            if (info) {
                // ReadSet seqnos that must be replaced on abort
                info->ArbiterReadSets.push_back(seqno);
            }
        }
    }

    if (NextSeqno != prevSeqno) {
        PersistSys(db, Schema::Sys_NextSeqno, NextSeqno);
    }

    if (info) {
        VolatileTxManager.AttachVolatileTxCallback(txId, new TSendArbiterReadSets(this, std::move(preparedRS)));
        preparedRS.clear();
    }
}

void TDataShard::SendDelayedAcks(const TActorContext& ctx, TVector<THolder<IEventHandle>>& delayedAcks) const {
    for (auto& x : delayedAcks) {
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD,
                  "Send delayed Ack RS Ack at %" PRIu64 " %s",
                  TabletID(), x->ToString().data());
        ctx.ExecutorThread.Send(x.Release());
        IncCounter(COUNTER_ACK_SENT_DELAYED);
    }

    delayedAcks.clear();
}

void TDataShard::GetCleanupReplies(const TOperation::TPtr& op, std::vector<std::unique_ptr<IEventHandle>>& cleanupReplies) {
    if (!op->HasOutputData()) {
        // There are no replies
        return;
    }

    auto& delayedAcks = op->DelayedAcks();
    for (auto& x : delayedAcks) {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
            "Cleanup TxId# " << op->GetTxId() << " at " << TabletID() << " Ack RS " << x->ToString());
        cleanupReplies.emplace_back(x.Release());
        IncCounter(COUNTER_ACK_SENT_DELAYED);
    }
    delayedAcks.clear();

    auto& expectedReadSets = op->ExpectedReadSets();
    for (auto& x : expectedReadSets) {
        for (const auto& recipient : x.second) {
            cleanupReplies.push_back(GenerateReadSetNoData(recipient, op->GetStep(), op->GetTxId(), x.first.first, x.first.second));
        }
    }
    expectedReadSets.clear();
}

void TDataShard::SendConfirmedReplies(TMonotonic ts, std::vector<std::unique_ptr<IEventHandle>>&& replies) {
    if (replies.empty()) {
        return;
    }

    struct TState : public TThrRefBase {
        std::vector<std::unique_ptr<IEventHandle>> Replies;

        TState(std::vector<std::unique_ptr<IEventHandle>>&& replies)
            : Replies(std::move(replies))
        {}
    };

    Executor()->ConfirmReadOnlyLease(ts,
        [state = MakeIntrusive<TState>(std::move(replies))] {
            for (auto& ev : state->Replies) {
                TActivationContext::Send(std::move(ev));
            }
        });
}

void TDataShard::SendCommittedReplies(std::vector<std::unique_ptr<IEventHandle>>&& replies) {
    for (auto& ev : replies) {
        TActivationContext::Send(std::move(ev));
    }
}

class TDataShard::TWaitVolatileDependencies final : public IVolatileTxCallback {
public:
    TWaitVolatileDependencies(
            TDataShard* self, const absl::flat_hash_set<ui64>& dependencies,
            const TActorId& target,
            std::unique_ptr<IEventBase> event,
            ui64 cookie)
        : Self(self)
        , Dependencies(dependencies)
        , Target(target)
        , Event(std::move(event))
        , Cookie(cookie)
    { }

    void OnCommit(ui64 txId) override {
        Dependencies.erase(txId);
        if (Dependencies.empty()) {
            Finish();
        }
    }

    void OnAbort(ui64 txId) override {
        Dependencies.erase(txId);
        if (Dependencies.empty()) {
            Finish();
        }
    }

    void Finish() {
        Self->Send(Target, Event.release(), 0, Cookie);
    }

private:
    TDataShard* Self;
    absl::flat_hash_set<ui64> Dependencies;
    TActorId Target;
    std::unique_ptr<IEventBase> Event;
    ui64 Cookie;
};

void TDataShard::WaitVolatileDependenciesThenSend(
        const absl::flat_hash_set<ui64>& dependencies,
        const TActorId& target, std::unique_ptr<IEventBase> event,
        ui64 cookie)
{
    Y_ABORT_UNLESS(!dependencies.empty(), "Unexpected empty dependencies");
    auto callback = MakeIntrusive<TWaitVolatileDependencies>(this, dependencies, target, std::move(event), cookie);
    for (ui64 txId : dependencies) {
        bool ok = VolatileTxManager.AttachVolatileTxCallback(txId, callback);
        Y_VERIFY_S(ok, "Unexpected failure to attach callback to volatile tx " << txId);
    }
}

class TDataShard::TSendVolatileResult final : public IVolatileTxCallback {
public:
    TSendVolatileResult(
            TDataShard* self, TOutputOpData::TResultPtr result,
            const TActorId& target,
            ui64 step, ui64 txId)
        : Self(self)
        , Result(std::move(result))
        , Target(target)
        , Step(step)
        , TxId(txId)
    { }

    void OnCommit(ui64) override {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Complete [" << Step << " : " << TxId << "] from " << Self->TabletID()
                    << " at tablet " << Self->TabletID() << " send result to client "
                    << Target <<  ", exec latency: " << Result->Record.GetExecLatency()
                    << " ms, propose latency: " << Result->Record.GetProposeLatency() << " ms");

        ui64 resultSize = Result->GetTxResult().size();
        ui32 flags = IEventHandle::MakeFlags(TInterconnectChannels::GetTabletChannel(resultSize), 0);
        LWTRACK(ProposeTransactionSendResult, Result->Orbit);
        Self->Send(Target, Result.Release(), flags);
    }

    void OnAbort(ui64 txId) override {
        Result->Record.ClearTxResult();
        Result->Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED);
        Result->AddError(NKikimrTxDataShard::TError::EXECUTION_CANCELLED, "Distributed transaction aborted due to commit failure");
        OnCommit(txId);
    }

private:
    TDataShard* Self;
    TOutputOpData::TResultPtr Result;
    TActorId Target;
    ui64 Step;
    ui64 TxId;
};

void TDataShard::SendResult(const TActorContext &ctx,
                                   TOutputOpData::TResultPtr &res,
                                   const TActorId &target,
                                   ui64 step,
                                   ui64 txId)
{
    Y_ABORT_UNLESS(txId == res->GetTxId(), "%" PRIu64 " vs %" PRIu64, txId, res->GetTxId());

    if (VolatileTxManager.FindByTxId(txId)) {
        // This is a volatile transaction, and we need to wait until it is resolved
        bool ok = VolatileTxManager.AttachVolatileTxCallback(txId,
            new TSendVolatileResult(this, std::move(res), target, step, txId));
        Y_ABORT_UNLESS(ok);
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Complete [" << step << " : " << txId << "] from " << TabletID()
                << " at tablet " << TabletID() << " send result to client "
                << target <<  ", exec latency: " << res->Record.GetExecLatency()
                << " ms, propose latency: " << res->Record.GetProposeLatency() << " ms");

    ui64 resultSize = res->GetTxResult().size();
    ui32 flags = IEventHandle::MakeFlags(TInterconnectChannels::GetTabletChannel(resultSize), 0);
    LWTRACK(ProposeTransactionSendResult, res->Orbit);
    ctx.Send(target, res.Release(), flags);
}

void TDataShard::FillExecutionStats(const TExecutionProfile& execProfile, TEvDataShard::TEvProposeTransactionResult& result) const {
    TDuration totalCpuTime;
    for (const auto& unit : execProfile.UnitProfiles) {
        totalCpuTime += unit.second.ExecuteTime;
        totalCpuTime += unit.second.CompleteTime;
    }
    result.Record.MutableTxStats()->MutablePerShardStats()->Clear();
    auto& stats = *result.Record.MutableTxStats()->AddPerShardStats();
    stats.SetShardId(TabletID());
    stats.SetCpuTimeUsec(totalCpuTime.MicroSeconds());
}

ui64 TDataShard::AllocateChangeRecordOrder(NIceDb::TNiceDb& db, ui64 count) {
    const ui64 result = NextChangeRecordOrder;
    NextChangeRecordOrder = result + count;
    PersistSys(db, Schema::Sys_NextChangeRecordOrder, NextChangeRecordOrder);

    return result;
}

ui64 TDataShard::AllocateChangeRecordGroup(NIceDb::TNiceDb& db) {
    const ui64 now = TInstant::Now().MicroSeconds();
    const ui64 result = now > LastChangeRecordGroup ? now : (LastChangeRecordGroup + 1);

    LastChangeRecordGroup = result;
    PersistSys(db, Schema::Sys_LastChangeRecordGroup, LastChangeRecordGroup);

    return result;
}

ui64 TDataShard::GetNextChangeRecordLockOffset(ui64 lockId) {
    auto it = LockChangeRecords.find(lockId);
    if (it == LockChangeRecords.end() || it->second.Changes.empty()) {
        return 0;
    }

    return it->second.Changes.back().LockOffset + 1;
}

void TDataShard::PersistChangeRecord(NIceDb::TNiceDb& db, const TChangeRecord& record) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "PersistChangeRecord"
        << ": record: " << record
        << ", at tablet: " << TabletID());

    ui64 lockId = record.GetLockId();
    if (lockId == 0) {
        db.Table<Schema::ChangeRecords>().Key(record.GetOrder()).Update(
            NIceDb::TUpdate<Schema::ChangeRecords::Group>(record.GetGroup()),
            NIceDb::TUpdate<Schema::ChangeRecords::PlanStep>(record.GetStep()),
            NIceDb::TUpdate<Schema::ChangeRecords::TxId>(record.GetTxId()),
            NIceDb::TUpdate<Schema::ChangeRecords::PathOwnerId>(record.GetPathId().OwnerId),
            NIceDb::TUpdate<Schema::ChangeRecords::LocalPathId>(record.GetPathId().LocalPathId),
            NIceDb::TUpdate<Schema::ChangeRecords::BodySize>(record.GetBody().size()),
            NIceDb::TUpdate<Schema::ChangeRecords::SchemaVersion>(record.GetSchemaVersion()),
            NIceDb::TUpdate<Schema::ChangeRecords::TableOwnerId>(record.GetTableId().OwnerId),
            NIceDb::TUpdate<Schema::ChangeRecords::TablePathId>(record.GetTableId().LocalPathId));
        db.Table<Schema::ChangeRecordDetails>().Key(record.GetOrder()).Update(
            NIceDb::TUpdate<Schema::ChangeRecordDetails::Kind>(record.GetKind()),
            NIceDb::TUpdate<Schema::ChangeRecordDetails::Body>(record.GetBody()),
            NIceDb::TUpdate<Schema::ChangeRecordDetails::Source>(record.GetSource()));

        auto res = ChangesQueue.emplace(record.GetOrder(), record);
        Y_VERIFY_S(res.second, "Duplicate change record: " << record.GetOrder());

        if (res.first->second.SchemaVersion) {
            res.first->second.SchemaSnapshotAcquired = SchemaSnapshotManager.AcquireReference(
                TSchemaSnapshotKey(res.first->second.TableId, res.first->second.SchemaVersion));
        }

        if (CommittingChangeRecords.empty()) {
            db.GetDatabase().OnCommit([this] {
                CommittingChangeRecords.clear();
            });
            db.GetDatabase().OnRollback([this] {
                for (const auto order : CommittingChangeRecords) {
                    auto cIt = ChangesQueue.find(order);
                    Y_VERIFY_S(cIt != ChangesQueue.end(), "Cannot find change record: " << order);

                    if (cIt->second.SchemaSnapshotAcquired) {
                        const auto snapshotKey = TSchemaSnapshotKey(cIt->second.TableId, cIt->second.SchemaVersion);
                        if (const auto last = SchemaSnapshotManager.ReleaseReference(snapshotKey)) {
                            ScheduleRemoveSchemaSnapshot(snapshotKey);
                        }
                    }

                    ChangesQueue.erase(cIt);
                }

                CommittingChangeRecords.clear();
            });
        }

        CommittingChangeRecords.push_back(record.GetOrder());
    } else {
        auto& state = LockChangeRecords[lockId];
        Y_ABORT_UNLESS(state.Changes.empty() || state.Changes.back().LockOffset < record.GetLockOffset(),
            "Lock records must be added in their lock offset order");

        if (state.Changes.size() == state.PersistentCount) {
            db.GetDatabase().OnCommit([this, lockId] {
                // We mark all added records as persistent
                auto it = LockChangeRecords.find(lockId);
                Y_ABORT_UNLESS(it != LockChangeRecords.end());
                it->second.PersistentCount = it->second.Changes.size();
            });
            db.GetDatabase().OnRollback([this, lockId] {
                // We remove all change records that have not been committed
                auto it = LockChangeRecords.find(lockId);
                Y_ABORT_UNLESS(it != LockChangeRecords.end());
                it->second.Changes.erase(
                    it->second.Changes.begin() + it->second.PersistentCount,
                    it->second.Changes.end());
                if (it->second.Changes.empty()) {
                    LockChangeRecords.erase(it);
                }
            });
        }

        state.Changes.push_back(IDataShardChangeCollector::TChange{
            .Order = record.GetOrder(),
            .Group = record.GetGroup(),
            .Step = record.GetStep(),
            .TxId = record.GetTxId(),
            .PathId = record.GetPathId(),
            .BodySize = record.GetBody().size(),
            .TableId = record.GetTableId(),
            .SchemaVersion = record.GetSchemaVersion(),
            .LockId = record.GetLockId(),
            .LockOffset = record.GetLockOffset(),
        });

        db.Table<Schema::LockChangeRecords>().Key(record.GetLockId(), record.GetLockOffset()).Update(
            NIceDb::TUpdate<Schema::LockChangeRecords::PathOwnerId>(record.GetPathId().OwnerId),
            NIceDb::TUpdate<Schema::LockChangeRecords::LocalPathId>(record.GetPathId().LocalPathId),
            NIceDb::TUpdate<Schema::LockChangeRecords::BodySize>(record.GetBody().size()),
            NIceDb::TUpdate<Schema::LockChangeRecords::SchemaVersion>(record.GetSchemaVersion()),
            NIceDb::TUpdate<Schema::LockChangeRecords::TableOwnerId>(record.GetTableId().OwnerId),
            NIceDb::TUpdate<Schema::LockChangeRecords::TablePathId>(record.GetTableId().LocalPathId));
        db.Table<Schema::LockChangeRecordDetails>().Key(record.GetLockId(), record.GetLockOffset()).Update(
            NIceDb::TUpdate<Schema::LockChangeRecordDetails::Kind>(record.GetKind()),
            NIceDb::TUpdate<Schema::LockChangeRecordDetails::Body>(record.GetBody()),
            NIceDb::TUpdate<Schema::LockChangeRecordDetails::Source>(record.GetSource()));
    }
}

bool TDataShard::HasLockChangeRecords(ui64 lockId) const {
    auto it = LockChangeRecords.find(lockId);
    return it != LockChangeRecords.end() && !it->second.Changes.empty();
}

void TDataShard::CommitLockChangeRecords(NIceDb::TNiceDb& db, ui64 lockId, ui64 group, const TRowVersion& rowVersion, TVector<IDataShardChangeCollector::TChange>& collected) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "CommitLockChangeRecords"
        << ": lockId# " << lockId
        << ", group# " << group
        << ", version# " << rowVersion
        << ", at tablet: " << TabletID());

    auto it = LockChangeRecords.find(lockId);
    Y_VERIFY_S(it != LockChangeRecords.end() && !it->second.Changes.empty(), "Cannot commit lock " << lockId << " change records: there are no pending change records");

    ui64 count = it->second.Changes.back().LockOffset + 1;
    ui64 order = AllocateChangeRecordOrder(db, count);

    // Transform uncommitted changes into their committed form
    collected.reserve(collected.size() + it->second.Changes.size());
    for (const auto& change : it->second.Changes) {
        auto committed = change;
        committed.Order = order + change.LockOffset;
        committed.Group = group;
        committed.Step = rowVersion.Step;
        committed.TxId = rowVersion.TxId;
        collected.push_back(committed);

        auto res = ChangesQueue.emplace(committed.Order, committed);
        Y_VERIFY_S(res.second, "Duplicate change record: " << committed.Order);

        if (res.first->second.SchemaVersion) {
            res.first->second.SchemaSnapshotAcquired = SchemaSnapshotManager.AcquireReference(
                TSchemaSnapshotKey(res.first->second.TableId, res.first->second.SchemaVersion));
        }
    }

    Y_VERIFY_S(!CommittedLockChangeRecords.contains(lockId), "Cannot commit lock " << lockId << " more than once");

    auto& entry = CommittedLockChangeRecords[lockId];
    Y_VERIFY_S(entry.Order == Max<ui64>(), "Cannot commit lock " << lockId << " change records multiple times");
    entry.Order = order;
    entry.Group = group;
    entry.Step = rowVersion.Step;
    entry.TxId = rowVersion.TxId;
    entry.Count = it->second.Changes.size();

    db.Table<Schema::ChangeRecordCommits>().Key(order).Update(
        NIceDb::TUpdate<Schema::ChangeRecordCommits::LockId>(lockId),
        NIceDb::TUpdate<Schema::ChangeRecordCommits::Group>(group),
        NIceDb::TUpdate<Schema::ChangeRecordCommits::PlanStep>(rowVersion.Step),
        NIceDb::TUpdate<Schema::ChangeRecordCommits::TxId>(rowVersion.TxId));

    db.GetDatabase().OnCommit([this, lockId]() {
        // We expect operation to enqueue transformed change records,
        // so we no longer need original uncommitted records.
        auto it = LockChangeRecords.find(lockId);
        Y_VERIFY_S(it != LockChangeRecords.end(), "Unexpected failure to find lockId# " << lockId);
        LockChangeRecords.erase(it);
    });
    db.GetDatabase().OnRollback([this, lockId]() {
        auto it = CommittedLockChangeRecords.find(lockId);
        Y_VERIFY_S(it != CommittedLockChangeRecords.end(), "Unexpected failure to find lockId# " << lockId);

        for (size_t i = 0; i < it->second.Count; ++i) {
            const ui64 order = it->second.Order + i;

            auto cIt = ChangesQueue.find(order);
            Y_VERIFY_S(cIt != ChangesQueue.end(), "Cannot find change record: " << order);

            if (cIt->second.SchemaSnapshotAcquired) {
                const auto snapshotKey = TSchemaSnapshotKey(cIt->second.TableId, cIt->second.SchemaVersion);
                if (const auto last = SchemaSnapshotManager.ReleaseReference(snapshotKey)) {
                    ScheduleRemoveSchemaSnapshot(snapshotKey);
                }
            }

            ChangesQueue.erase(cIt);
        }

        CommittedLockChangeRecords.erase(it);
    });
}

void TDataShard::MoveChangeRecord(NIceDb::TNiceDb& db, ui64 order, const TPathId& pathId) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "MoveChangeRecord"
        << ": order: " << order
        << ": pathId: " << pathId
        << ", at tablet: " << TabletID());

    db.Table<Schema::ChangeRecords>().Key(order).Update(
        NIceDb::TUpdate<Schema::ChangeRecords::PathOwnerId>(pathId.OwnerId),
        NIceDb::TUpdate<Schema::ChangeRecords::LocalPathId>(pathId.LocalPathId));
}

void TDataShard::MoveChangeRecord(NIceDb::TNiceDb& db, ui64 lockId, ui64 lockOffset, const TPathId& pathId) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "MoveChangeRecord"
        << ": lockId: " << lockId
        << ", lockOffset: " << lockOffset
        << ": pathId: " << pathId
        << ", at tablet: " << TabletID());

    db.Table<Schema::LockChangeRecords>().Key(lockId, lockOffset).Update(
        NIceDb::TUpdate<Schema::LockChangeRecords::PathOwnerId>(pathId.OwnerId),
        NIceDb::TUpdate<Schema::LockChangeRecords::LocalPathId>(pathId.LocalPathId));
}

void TDataShard::RemoveChangeRecord(NIceDb::TNiceDb& db, ui64 order) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "RemoveChangeRecord"
        << ": order: " << order
        << ", at tablet: " << TabletID());

    auto it = ChangesQueue.find(order);
    if (it == ChangesQueue.end()) {
        return;
    }

    const auto& record = it->second;

    if (record.LockId) {
        db.Table<Schema::LockChangeRecords>().Key(record.LockId, record.LockOffset).Delete();
        db.Table<Schema::LockChangeRecordDetails>().Key(record.LockId, record.LockOffset).Delete();
        // Delete ChangeRecordCommits row when the last record is removed
        auto it = CommittedLockChangeRecords.find(record.LockId);
        if (it != CommittedLockChangeRecords.end()) {
            Y_DEBUG_ABORT_UNLESS(it->second.Count > 0);
            if (it->second.Count > 0 && 0 == --it->second.Count) {
                db.Table<Schema::ChangeRecordCommits>().Key(it->second.Order).Delete();
                CommittedLockChangeRecords.erase(it);
                LockChangeRecords.erase(record.LockId);
            }
        }
    } else {
        db.Table<Schema::ChangeRecords>().Key(order).Delete();
        db.Table<Schema::ChangeRecordDetails>().Key(order).Delete();
    }

    Y_ABORT_UNLESS(record.BodySize <= ChangesQueueBytes);
    ChangesQueueBytes -= record.BodySize;

    if (record.SchemaSnapshotAcquired) {
        const auto snapshotKey = TSchemaSnapshotKey(record.TableId, record.SchemaVersion);
        if (const bool last = SchemaSnapshotManager.ReleaseReference(snapshotKey)) {
            ScheduleRemoveSchemaSnapshot(snapshotKey);
        }
    }

    if (auto rIt = ChangeQueueReservations.find(record.ReservationCookie); rIt != ChangeQueueReservations.end()) {
        --ChangeQueueReservedCapacity;
        if (!--rIt->second) {
            ChangeQueueReservations.erase(rIt);
        }
    }

    UpdateChangeExchangeLag(AppData()->TimeProvider->Now());
    ChangesQueue.erase(it);

    IncCounter(COUNTER_CHANGE_RECORDS_REMOVED);
    SetCounter(COUNTER_CHANGE_QUEUE_SIZE, ChangesQueue.size());
    SetCounter(COUNTER_CHANGE_QUEUE_RESERVED_CAPACITY, ChangeQueueReservedCapacity);

    CheckChangesQueueNoOverflow();
}

void TDataShard::EnqueueChangeRecords(TVector<IDataShardChangeCollector::TChange>&& records, ui64 cookie, bool afterMove) {
    if (!records) {
        return;
    }

    if (OutChangeSenderSuspended) {
        LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Cannot enqueue change records"
            << ": change sender suspended"
            << ", at tablet: " << TabletID()
            << ", records: " << JoinSeq(", ", records));
        return;
    }

    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "EnqueueChangeRecords"
        << ": at tablet: " << TabletID()
        << ", records: " << JoinSeq(", ", records));

    const auto now = AppData()->TimeProvider->Now();
    TVector<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo> forward(Reserve(records.size()));
    for (const auto& record : records) {
        auto it = ChangesQueue.find(record.Order);
        if (it == ChangesQueue.end()) {
            Y_ABORT_UNLESS(afterMove);
            continue;
        }

        forward.emplace_back(record.Order, record.PathId, record.BodySize);

        it->second.EnqueuedAt = now;
        it->second.ReservationCookie = cookie;
        ChangesList.PushBack(&it->second);

        Y_ABORT_UNLESS(ChangesQueueBytes <= (Max<ui64>() - record.BodySize));
        ChangesQueueBytes += record.BodySize;
    }

    if (auto it = ChangeQueueReservations.find(cookie); it != ChangeQueueReservations.end()) {
        Y_ABORT_UNLESS(!afterMove);
        ChangeQueueReservedCapacity -= it->second;
        ChangeQueueReservedCapacity += records.size();
    }

    UpdateChangeExchangeLag(now);
    IncCounter(COUNTER_CHANGE_RECORDS_ENQUEUED, forward.size());
    SetCounter(COUNTER_CHANGE_QUEUE_SIZE, ChangesQueue.size());
    SetCounter(COUNTER_CHANGE_QUEUE_RESERVED_CAPACITY, ChangeQueueReservedCapacity);

    Y_ABORT_UNLESS(OutChangeSender);
    Send(OutChangeSender, new NChangeExchange::TEvChangeExchange::TEvEnqueueRecords(std::move(forward)));
}

ui32 TDataShard::GetFreeChangeQueueCapacity(ui64 cookie) {
    const auto sizeLimit = AppData()->DataShardConfig.GetChangesQueueItemsLimit();
    if (sizeLimit < ChangesQueue.size()) {
        return 0;
    }

    const auto free = Min(sizeLimit - ChangesQueue.size(), Max(sizeLimit / 2, 1ul));

    ui32 reserved = ChangeQueueReservedCapacity;
    if (auto it = ChangeQueueReservations.find(cookie); it != ChangeQueueReservations.end()) {
        reserved -= it->second;
    }

    if (free < reserved) {
        return 0;
    }

    return free - reserved;
}

ui64 TDataShard::ReserveChangeQueueCapacity(ui32 capacity) {
    const auto sizeLimit = AppData()->DataShardConfig.GetChangesQueueItemsLimit();
    if (Max(sizeLimit / 2, 1ul) < ChangeQueueReservedCapacity) {
        return 0;
    }

    const auto cookie = NextChangeQueueReservationCookie++;
    ChangeQueueReservations.emplace(cookie, capacity);
    ChangeQueueReservedCapacity += capacity;
    SetCounter(COUNTER_CHANGE_QUEUE_RESERVED_CAPACITY, ChangeQueueReservedCapacity);

    return cookie;
}

void TDataShard::UpdateChangeExchangeLag(TInstant now) {
    if (!ChangesList.Empty()) {
        const auto* front = ChangesList.Front();
        SetCounter(COUNTER_CHANGE_DATA_LAG, Max(now - front->CreatedAt, TDuration::Zero()).MilliSeconds());
        SetCounter(COUNTER_CHANGE_DELIVERY_LAG, (now - front->EnqueuedAt).MilliSeconds());
    } else {
        SetCounter(COUNTER_CHANGE_DATA_LAG, 0);
        SetCounter(COUNTER_CHANGE_DELIVERY_LAG, 0);
    }
}

void TDataShard::CreateChangeSender(const TActorContext& ctx) {
    Y_ABORT_UNLESS(!OutChangeSender);
    OutChangeSender = Register(NDataShard::CreateChangeSender(this));

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Change sender created"
        << ": at tablet: " << TabletID()
        << ", actorId: " << OutChangeSender);
}

void TDataShard::MaybeActivateChangeSender(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Trying to activate change sender"
        << ": at tablet: " << TabletID());

    OutChangeSenderSuspended = false;

    if (ReceiveActivationsFrom) {
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "Cannot activate change sender"
            << ": at tablet: " << TabletID()
            << ", wait to activation from: " << JoinSeq(", ", ReceiveActivationsFrom));
        return;
    }

    switch (State) {
    case TShardState::WaitScheme:
    case TShardState::SplitDstReceivingSnapshot:
    case TShardState::Offline:
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "Cannot activate change sender"
            << ": at tablet: " << TabletID()
            << ", state: " << DatashardStateName(State));
        return;

    case TShardState::SplitSrcMakeSnapshot:
    case TShardState::SplitSrcSendingSnapshot:
    case TShardState::SplitSrcWaitForPartitioningChanged:
    case TShardState::PreOffline:
        if (!ChangesQueue) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "Cannot activate change sender"
                << ": at tablet: " << TabletID()
                << ", state: " << DatashardStateName(State)
                << ", queue size: " << ChangesQueue.size());
            return;
        }
        break;
    }

    Y_ABORT_UNLESS(OutChangeSender);
    Send(OutChangeSender, new TEvChangeExchange::TEvActivateSender());

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "Change sender activated"
        << ": at tablet: " << TabletID());
}

void TDataShard::KillChangeSender(const TActorContext& ctx) {
    if (OutChangeSender) {
        Send(std::exchange(OutChangeSender, TActorId()), new TEvents::TEvPoison());

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "Change sender killed"
            << ": at tablet: " << TabletID());
    }
}

void TDataShard::SuspendChangeSender(const TActorContext& ctx) {
    KillChangeSender(ctx);
    OutChangeSenderSuspended = true;
}

bool TDataShard::LoadChangeRecords(NIceDb::TNiceDb& db, TVector<IDataShardChangeCollector::TChange>& records) {
    using Schema = TDataShard::Schema;

    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "LoadChangeRecords"
        << ": QueueSize: " << ChangesQueue.size()
        << ", at tablet: " << TabletID());

    records.reserve(ChangesQueue.size());

    auto rowset = db.Table<Schema::ChangeRecords>().Range().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const ui64 order = rowset.GetValue<Schema::ChangeRecords::Order>();
        const ui64 group = rowset.GetValue<Schema::ChangeRecords::Group>();
        const ui64 step = rowset.GetValue<Schema::ChangeRecords::PlanStep>();
        const ui64 txId = rowset.GetValue<Schema::ChangeRecords::TxId>();
        const ui64 bodySize = rowset.GetValue<Schema::ChangeRecords::BodySize>();
        const ui64 schemaVersion = rowset.GetValue<Schema::ChangeRecords::SchemaVersion>();
        const auto pathId = TPathId(
            rowset.GetValue<Schema::ChangeRecords::PathOwnerId>(),
            rowset.GetValue<Schema::ChangeRecords::LocalPathId>()
        );
        const auto tableId = TPathId(
            rowset.GetValue<Schema::ChangeRecords::TableOwnerId>(),
            rowset.GetValue<Schema::ChangeRecords::TablePathId>()
        );

        records.push_back(IDataShardChangeCollector::TChange{
            .Order = order,
            .Group = group,
            .Step = step,
            .TxId = txId,
            .PathId = pathId,
            .BodySize = bodySize,
            .TableId = tableId,
            .SchemaVersion = schemaVersion,
        });

        auto res = ChangesQueue.emplace(records.back().Order, records.back());
        Y_VERIFY_S(res.second, "Duplicate change record: " << records.back().Order);

        if (res.first->second.SchemaVersion) {
            res.first->second.SchemaSnapshotAcquired = SchemaSnapshotManager.AcquireReference(
                TSchemaSnapshotKey(res.first->second.TableId, res.first->second.SchemaVersion));
        }

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

bool TDataShard::LoadLockChangeRecords(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "LoadLockChangeRecords"
        << " at tablet: " << TabletID());

    auto rowset = db.Table<Schema::LockChangeRecords>().Range().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const ui64 lockId = rowset.GetValue<Schema::LockChangeRecords::LockId>();
        const ui64 lockOffset = rowset.GetValue<Schema::LockChangeRecords::LockOffset>();
        const ui64 bodySize = rowset.GetValue<Schema::LockChangeRecords::BodySize>();
        const ui64 schemaVersion = rowset.GetValue<Schema::LockChangeRecords::SchemaVersion>();
        const auto pathId = TPathId(
            rowset.GetValue<Schema::LockChangeRecords::PathOwnerId>(),
            rowset.GetValue<Schema::LockChangeRecords::LocalPathId>()
        );
        const auto tableId = TPathId(
            rowset.GetValue<Schema::LockChangeRecords::TableOwnerId>(),
            rowset.GetValue<Schema::LockChangeRecords::TablePathId>()
        );

        auto& state = LockChangeRecords[lockId];

        state.Changes.push_back(IDataShardChangeCollector::TChange{
            .Order = Max<ui64>(),
            .Group = 0,
            .Step = 0,
            .TxId = 0,
            .PathId = pathId,
            .BodySize = bodySize,
            .TableId = tableId,
            .SchemaVersion = schemaVersion,
            .LockId = lockId,
            .LockOffset = lockOffset,
        });
        state.PersistentCount = state.Changes.size();

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

bool TDataShard::LoadChangeRecordCommits(NIceDb::TNiceDb& db, TVector<IDataShardChangeCollector::TChange>& records) {
    using Schema = TDataShard::Schema;

    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "LoadChangeRecordCommits"
        << " at tablet: " << TabletID());

    bool needSort = false;

    auto rowset = db.Table<Schema::ChangeRecordCommits>().Range().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const ui64 order = rowset.GetValue<Schema::ChangeRecordCommits::Order>();
        const ui64 lockId = rowset.GetValue<Schema::ChangeRecordCommits::LockId>();
        const ui64 group = rowset.GetValue<Schema::ChangeRecordCommits::Group>();
        const ui64 step = rowset.GetValue<Schema::ChangeRecordCommits::PlanStep>();
        const ui64 txId = rowset.GetValue<Schema::ChangeRecordCommits::TxId>();

        auto& entry = CommittedLockChangeRecords[lockId];
        entry.Order = order;
        entry.Group = group;
        entry.Step = step;
        entry.TxId = txId;

        for (auto& record : LockChangeRecords[lockId].Changes) {
            records.push_back(IDataShardChangeCollector::TChange{
                .Order = order + record.LockOffset,
                .Group = group,
                .Step = step,
                .TxId = txId,
                .PathId = record.PathId,
                .BodySize = record.BodySize,
                .TableId = record.TableId,
                .SchemaVersion = record.SchemaVersion,
                .LockId = record.LockId,
                .LockOffset = record.LockOffset,
            });
            entry.Count++;
            needSort = true;

            auto res = ChangesQueue.emplace(records.back().Order, records.back());
            Y_VERIFY_S(res.second, "Duplicate change record: " << records.back().Order);

            if (res.first->second.SchemaVersion) {
                res.first->second.SchemaSnapshotAcquired = SchemaSnapshotManager.AcquireReference(
                    TSchemaSnapshotKey(res.first->second.TableId, res.first->second.SchemaVersion));
            }
        }

        LockChangeRecords.erase(lockId);

        if (!rowset.Next()) {
            return false;
        }
    }

    if (needSort) {
        std::sort(records.begin(), records.end(), [](const auto& a, const auto& b) -> bool {
            return a.Order < b.Order;
        });
    }

    return true;
}

void TDataShard::ScheduleRemoveLockChanges(ui64 lockId) {
    if (LockChangeRecords.contains(lockId) && !CommittedLockChangeRecords.contains(lockId)) {
        bool wasEmpty = PendingLockChangeRecordsToRemove.empty();
        PendingLockChangeRecordsToRemove.push_back(lockId);
        if (wasEmpty) {
            Send(SelfId(), new TEvPrivate::TEvRemoveLockChangeRecords);
        }
    }
}

void TDataShard::ScheduleRemoveAbandonedLockChanges() {
    bool wasEmpty = PendingLockChangeRecordsToRemove.empty();

    for (const auto& pr : LockChangeRecords) {
        ui64 lockId = pr.first;

        if (CommittedLockChangeRecords.contains(lockId)) {
            // Skip committed lock changes
            continue;
        }

        auto lock = SysLocksTable().GetRawLock(lockId);
        if (lock && lock->IsPersistent()) {
            // Skip lock changes attached to persistent locks
            continue;
        }

        if (auto* info = VolatileTxManager.FindByCommitTxId(lockId)) {
            // Skip lock changes attached to volatile transactions
            continue;
        }

        PendingLockChangeRecordsToRemove.push_back(lockId);
    }

    if (wasEmpty && !PendingLockChangeRecordsToRemove.empty()) {
        Send(SelfId(), new TEvPrivate::TEvRemoveLockChangeRecords);
    }
}

void TDataShard::ScheduleRemoveSchemaSnapshot(const TSchemaSnapshotKey& key) {
    Y_ABORT_UNLESS(!SchemaSnapshotManager.HasReference(key));

    const auto* snapshot = SchemaSnapshotManager.FindSnapshot(key);
    Y_ABORT_UNLESS(snapshot);

    auto it = TableInfos.find(key.PathId);
    if (it == TableInfos.end()) {
        Y_DEBUG_ABORT_UNLESS(State == TShardState::PreOffline);
        return;
    }

    if (snapshot->Schema->GetTableSchemaVersion() < it->second->GetTableSchemaVersion()) {
        bool wasEmpty = PendingSchemaSnapshotsToGc.empty();
        PendingSchemaSnapshotsToGc.push_back(key);
        if (wasEmpty) {
            Send(SelfId(), new TEvPrivate::TEvRemoveSchemaSnapshots);
        }
    }
}

void TDataShard::ScheduleRemoveAbandonedSchemaSnapshots() {
    bool wasEmpty = PendingSchemaSnapshotsToGc.empty();

    for (const auto& [key, snapshot] : SchemaSnapshotManager.GetSnapshots()) {
        auto it = TableInfos.find(key.PathId);
        if (it == TableInfos.end()) {
            Y_DEBUG_ABORT_UNLESS(State == TShardState::PreOffline);
            break;
        }
        if (SchemaSnapshotManager.HasReference(key)) {
            continue;
        }
        if (snapshot.Schema->GetTableSchemaVersion() >= it->second->GetTableSchemaVersion()) {
            continue;
        }

        PendingSchemaSnapshotsToGc.push_back(key);
    }

    if (wasEmpty && !PendingSchemaSnapshotsToGc.empty()) {
        Send(SelfId(), new TEvPrivate::TEvRemoveSchemaSnapshots);
    }
}

void TDataShard::PersistSchemeTxResult(NIceDb::TNiceDb &db, const TSchemaOperation &op) {
    db.Table<Schema::SchemaOperations>().Key(op.TxId).Update(
        NIceDb::TUpdate<Schema::SchemaOperations::Success>(op.Success),
        NIceDb::TUpdate<Schema::SchemaOperations::Error>(op.Error),
        NIceDb::TUpdate<Schema::SchemaOperations::DataSize>(op.BytesProcessed),
        NIceDb::TUpdate<Schema::SchemaOperations::Rows>(op.RowsProcessed)
    );
}

void TDataShard::NotifySchemeshard(const TActorContext& ctx, ui64 txId) {
    if (!txId) {
        for (const auto& op : TransQueue.GetSchemaOperations())
            NotifySchemeshard(ctx, op.first);
        return;
    }

    TSchemaOperation * op = TransQueue.FindSchemaTx(txId);
    if (!op || !op->Done)
        return;

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
               TabletID() << " Sending notify to schemeshard " << op->TabletId
                << " txId " << txId << " state " << DatashardStateName(State) << " TxInFly " << TxInFly());

    if (op->IsDrop()) {
        Y_VERIFY_S(State == TShardState::PreOffline,
                   TabletID() << " is in wrong state (" << State << ") for drop");
        Y_VERIFY_S(!TxInFly(),
                   TabletID() << " has " << TxInFly() << " txs in-fly "
                   << TransQueue.TxInFlyToString());
    }

    THolder<TEvDataShard::TEvSchemaChanged> event =
        THolder(new TEvDataShard::TEvSchemaChanged(ctx.SelfID, TabletID(), State, op->TxId, op->PlanStep, Generation()));

    switch (op->Type) {
        case TSchemaOperation::ETypeBackup:
        case TSchemaOperation::ETypeRestore: {
            auto* result = event->Record.MutableOpResult();
            result->SetSuccess(op->Success);
            result->SetExplain(op->Error);
            result->SetBytesProcessed(op->BytesProcessed);
            result->SetRowsProcessed(op->RowsProcessed);
            break;
        }
        default:
            break;
    }

    SendViaSchemeshardPipe(ctx, op->TabletId, THolder(event.Release()));
}

bool TDataShard::CheckMediatorAuthorisation(ui64 mediatorId) {
    if (!ProcessingParams || 0 == ProcessingParams->MediatorsSize()) {
        return true;
    }

    auto it = std::find(ProcessingParams->GetMediators().begin(),
                        ProcessingParams->GetMediators().end(),
                        mediatorId);

    return it != ProcessingParams->GetMediators().end();
}

void TDataShard::PersistSys(NIceDb::TNiceDb &db, ui64 key, const TString &value) const {
    db.Table<Schema::Sys>().Key(key).Update(NIceDb::TUpdate<Schema::Sys::Bytes>(value));
}

void TDataShard::PersistSys(NIceDb::TNiceDb& db, ui64 key, ui64 value) const {
    db.Table<Schema::Sys>().Key(key).Update(NIceDb::TUpdate<Schema::Sys::Uint64>(value));
}

void TDataShard::PersistSys(NIceDb::TNiceDb& db, ui64 key, ui32 value) const {
    db.Table<Schema::Sys>().Key(key).Update(NIceDb::TUpdate<Schema::Sys::Uint64>(value));
}

void TDataShard::PersistSys(NIceDb::TNiceDb& db, ui64 key, bool value) const {
    db.Table<Schema::Sys>().Key(key).Update(NIceDb::TUpdate<Schema::Sys::Uint64>(value ? 1 : 0));
}

void TDataShard::PersistUserTable(NIceDb::TNiceDb& db, ui64 tableId, const TUserTable& tableInfo) {
    db.Table<Schema::UserTables>().Key(tableId).Update(
        NIceDb::TUpdate<Schema::UserTables::LocalTid>(tableInfo.LocalTid),
        NIceDb::TUpdate<Schema::UserTables::ShadowTid>(tableInfo.ShadowTid),
        NIceDb::TUpdate<Schema::UserTables::Schema>(tableInfo.GetSchema()));
}

void TDataShard::PersistUserTableFullCompactionTs(NIceDb::TNiceDb& db, ui64 tableId, ui64 ts) {
    db.Table<Schema::UserTablesStats>().Key(tableId).Update<Schema::UserTablesStats::FullCompactionTs>(ts);
}

void TDataShard::PersistMoveUserTable(NIceDb::TNiceDb& db, ui64 prevTableId, ui64 tableId, const TUserTable& tableInfo) {
    db.Table<Schema::UserTables>().Key(prevTableId).Delete();
    PersistUserTable(db, tableId, tableInfo);

    db.Table<Schema::UserTablesStats>().Key(prevTableId).Delete();
    if (tableInfo.Stats.LastFullCompaction) {
        PersistUserTableFullCompactionTs(db, tableId, tableInfo.Stats.LastFullCompaction.Seconds());
    }
}

TUserTable::TPtr TDataShard::AlterTableSchemaVersion(
    const TActorContext&, TTransactionContext& txc,
    const TPathId& pathId, const ui64 tableSchemaVersion, bool persist)
{

    Y_ABORT_UNLESS(GetPathOwnerId() == pathId.OwnerId);
    ui64 tableId = pathId.LocalPathId;

    Y_ABORT_UNLESS(TableInfos.contains(tableId));
    auto oldTableInfo = TableInfos[tableId];
    Y_ABORT_UNLESS(oldTableInfo);

    TUserTable::TPtr newTableInfo = new TUserTable(*oldTableInfo);
    newTableInfo->SetTableSchemaVersion(tableSchemaVersion);

    Y_VERIFY_DEBUG_S(oldTableInfo->GetTableSchemaVersion() < newTableInfo->GetTableSchemaVersion(),
                     "pathId " << pathId
                     << "old version " << oldTableInfo->GetTableSchemaVersion()
                     << "new version " << newTableInfo->GetTableSchemaVersion());

    if (persist) {
        NIceDb::TNiceDb db(txc.DB);
        PersistUserTable(db, tableId, *newTableInfo);
    }

    return newTableInfo;
}

TUserTable::TPtr TDataShard::AlterTableAddIndex(
    const TActorContext& ctx, TTransactionContext& txc,
    const TPathId& pathId, ui64 tableSchemaVersion,
    const NKikimrSchemeOp::TIndexDescription& indexDesc)
{
    auto tableInfo = AlterTableSchemaVersion(ctx, txc, pathId, tableSchemaVersion, false);
    tableInfo->AddIndex(indexDesc);

    NIceDb::TNiceDb db(txc.DB);
    PersistUserTable(db, pathId.LocalPathId, *tableInfo);

    return tableInfo;
}

TUserTable::TPtr TDataShard::AlterTableDropIndex(
    const TActorContext& ctx, TTransactionContext& txc,
    const TPathId& pathId, ui64 tableSchemaVersion,
    const TPathId& indexPathId)
{
    auto tableInfo = AlterTableSchemaVersion(ctx, txc, pathId, tableSchemaVersion, false);
    tableInfo->DropIndex(indexPathId);

    NIceDb::TNiceDb db(txc.DB);
    PersistUserTable(db, pathId.LocalPathId, *tableInfo);

    return tableInfo;
}

TUserTable::TPtr TDataShard::AlterTableAddCdcStream(
    const TActorContext& ctx, TTransactionContext& txc,
    const TPathId& pathId, ui64 tableSchemaVersion,
    const NKikimrSchemeOp::TCdcStreamDescription& streamDesc)
{
    auto tableInfo = AlterTableSchemaVersion(ctx, txc, pathId, tableSchemaVersion, false);
    tableInfo->AddCdcStream(streamDesc);

    NIceDb::TNiceDb db(txc.DB);
    PersistUserTable(db, pathId.LocalPathId, *tableInfo);

    return tableInfo;
}

TUserTable::TPtr TDataShard::AlterTableSwitchCdcStreamState(
    const TActorContext& ctx, TTransactionContext& txc,
    const TPathId& pathId, ui64 tableSchemaVersion,
    const TPathId& streamPathId, NKikimrSchemeOp::ECdcStreamState state)
{
    auto tableInfo = AlterTableSchemaVersion(ctx, txc, pathId, tableSchemaVersion, false);
    tableInfo->SwitchCdcStreamState(streamPathId, state);

    NIceDb::TNiceDb db(txc.DB);
    PersistUserTable(db, pathId.LocalPathId, *tableInfo);

    return tableInfo;
}

TUserTable::TPtr TDataShard::AlterTableDropCdcStream(
    const TActorContext& ctx, TTransactionContext& txc,
    const TPathId& pathId, ui64 tableSchemaVersion,
    const TPathId& streamPathId)
{
    auto tableInfo = AlterTableSchemaVersion(ctx, txc, pathId, tableSchemaVersion, false);
    tableInfo->DropCdcStream(streamPathId);

    NIceDb::TNiceDb db(txc.DB);
    PersistUserTable(db, pathId.LocalPathId, *tableInfo);

    return tableInfo;
}

void TDataShard::AddSchemaSnapshot(const TPathId& pathId, ui64 tableSchemaVersion, ui64 step, ui64 txId,
    TTransactionContext& txc, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Add schema snapshot"
        << ": pathId# " << pathId
        << ", version# " << tableSchemaVersion
        << ", step# " << step
        << ", txId# " << txId
        << ", at tablet# " << TabletID());

    Y_ABORT_UNLESS(GetPathOwnerId() == pathId.OwnerId);
    Y_ABORT_UNLESS(TableInfos.contains(pathId.LocalPathId));
    auto tableInfo = TableInfos[pathId.LocalPathId];

    const auto key = TSchemaSnapshotKey(pathId, tableSchemaVersion);
    SchemaSnapshotManager.AddSnapshot(txc.DB, key, TSchemaSnapshot(tableInfo, step, txId));

    const auto& snapshots = SchemaSnapshotManager.GetSnapshots();
    for (auto it = snapshots.lower_bound(TSchemaSnapshotKey(pathId, 1)); it != snapshots.end(); ++it) {
        if (it->first == key) {
            break;
        }
        if (!SchemaSnapshotManager.HasReference(it->first)) {
            ScheduleRemoveSchemaSnapshot(it->first);
        }
    }
}

void TDataShard::PersistLastLoanTableTid(NIceDb::TNiceDb& db, ui32 localTid) {
    LastLoanTableTid = localTid;
    PersistSys(db, Schema::Sys_LastLoanTableTid, LastLoanTableTid);
}

TUserTable::TPtr TDataShard::CreateUserTable(TTransactionContext& txc,
    const NKikimrSchemeOp::TTableDescription& tableScheme)
{
    const TString mainTableName = TDataShard::Schema::UserTablePrefix + tableScheme.GetName();
    ui64 tableId = tableScheme.GetId_Deprecated();
    if (tableScheme.HasPathId()) {
        Y_ABORT_UNLESS(GetPathOwnerId() == tableScheme.GetPathId().GetOwnerId() || GetPathOwnerId() == INVALID_TABLET_ID);
        tableId = tableScheme.GetPathId().GetLocalId();
    }
    ui32 localTid = ++LastLocalTid;
    ui32 shadowTid = tableScheme.GetPartitionConfig().GetShadowData() ? ++LastLocalTid : 0;
    TUserTable::TPtr tableInfo = new TUserTable(localTid, tableScheme, shadowTid);

    tableInfo->ApplyCreate(txc, mainTableName, tableScheme.GetPartitionConfig());

    if (shadowTid) {
        const TString shadowTableName = TDataShard::Schema::ShadowTablePrefix + tableScheme.GetName();
        tableInfo->ApplyCreateShadow(txc, shadowTableName, tableScheme.GetPartitionConfig());
    }

    NIceDb::TNiceDb db(txc.DB);

    auto& partConfig = tableScheme.GetPartitionConfig();
    if (partConfig.HasTxReadSizeLimit()) {
        TxReadSizeLimit = partConfig.GetTxReadSizeLimit();
        PersistSys(db, Schema::Sys_TxReadSizeLimit, TxReadSizeLimit);
    }
    if (partConfig.HasDisableStatisticsCalculation()) {
        StatisticsDisabled = partConfig.GetDisableStatisticsCalculation() ? 1 : 0;
        PersistSys(db, Schema::Sys_StatisticsDisabled, StatisticsDisabled);
    }

    Pipeline.UpdateConfig(db, partConfig.GetPipelineConfig());

    if (partConfig.HasKeepSnapshotTimeout())
        SnapshotManager.SetKeepSnapshotTimeout(db, partConfig.GetKeepSnapshotTimeout());

    PersistSys(db, Schema::Sys_LastLocalTid, LastLocalTid);
    PersistUserTable(db, tableId, *tableInfo);

    return tableInfo;
}

THashMap<TPathId, TPathId> TDataShard::GetRemapIndexes(const NKikimrTxDataShard::TMoveTable& move) {
    THashMap<TPathId, TPathId> remap;
    for (const auto& item: move.GetReMapIndexes()) {
        const auto prevId = PathIdFromPathId(item.GetSrcPathId());
        const auto newId = PathIdFromPathId(item.GetDstPathId());
        remap[prevId] = newId;
    }
    return remap;
}

TUserTable::TPtr TDataShard::MoveUserTable(TOperation::TPtr op, const NKikimrTxDataShard::TMoveTable& move,
    const TActorContext& ctx, TTransactionContext& txc)
{
    const auto prevId = PathIdFromPathId(move.GetPathId());
    const auto newId = PathIdFromPathId(move.GetDstPathId());

    Y_ABORT_UNLESS(GetPathOwnerId() == prevId.OwnerId);
    Y_ABORT_UNLESS(TableInfos.contains(prevId.LocalPathId));

    const auto version = move.GetTableSchemaVersion();
    Y_ABORT_UNLESS(version);

    auto newTableInfo = AlterTableSchemaVersion(ctx, txc, prevId, version, false);
    newTableInfo->SetPath(move.GetDstPath());

    const THashMap<TPathId, TPathId> remap = GetRemapIndexes(move);

    NKikimrSchemeOp::TTableDescription schema;
    newTableInfo->GetSchema(schema);
    for (auto& indexDesc: *schema.MutableTableIndexes()) {
        Y_ABORT_UNLESS(indexDesc.HasPathOwnerId() && indexDesc.HasLocalPathId());
        auto prevPathId = TPathId(indexDesc.GetPathOwnerId(), indexDesc.GetLocalPathId());
        Y_VERIFY_S(remap.contains(prevPathId), "no rule how to move index with pathId " << prevPathId); // we should remap all indexes
        auto newPathId = remap.at(prevPathId);

        indexDesc.SetPathOwnerId(newPathId.OwnerId);
        indexDesc.SetLocalPathId(newPathId.LocalPathId);

        newTableInfo->Indexes[newPathId] = newTableInfo->Indexes[prevPathId];
        newTableInfo->Indexes.erase(prevPathId);
    }
    newTableInfo->SetSchema(schema);

    //NOTE: Stats building is bound to table id, but move-table changes table id,
    // so already built stats couldn't be inherited by moved table
    // and have to be rebuilt from the ground up
    newTableInfo->StatsUpdateInProgress = false;
    newTableInfo->StatsNeedUpdate = true;

    TDataShardLocksDb locksDb(*this, txc);

    RemoveUserTable(prevId, &locksDb);
    AddUserTable(newId, newTableInfo);

    for (auto& [_, record] : ChangesQueue) {
        if (record.TableId == prevId) {
            record.TableId = newId;
        }
    }

    SnapshotManager.RenameSnapshots(txc.DB, prevId, newId);
    SchemaSnapshotManager.RenameSnapshots(txc.DB, prevId, newId);
    if (newTableInfo->NeedSchemaSnapshots()) {
        AddSchemaSnapshot(newId, version, op->GetStep(), op->GetTxId(), txc, ctx);
    }

    NIceDb::TNiceDb db(txc.DB);
    PersistMoveUserTable(db, prevId.LocalPathId, newId.LocalPathId, *newTableInfo);
    PersistOwnerPathId(newId.OwnerId, txc);

    return newTableInfo;
}

TUserTable::TPtr TDataShard::MoveUserIndex(TOperation::TPtr op, const NKikimrTxDataShard::TMoveIndex& move,
    const TActorContext& ctx, TTransactionContext& txc)
{
    const auto pathId = PathIdFromPathId(move.GetPathId());

    Y_ABORT_UNLESS(GetPathOwnerId() == pathId.OwnerId);
    Y_ABORT_UNLESS(TableInfos.contains(pathId.LocalPathId));

    const auto version = move.GetTableSchemaVersion();
    Y_ABORT_UNLESS(version);

    auto newTableInfo = AlterTableSchemaVersion(ctx, txc, pathId, version, false);

    NKikimrSchemeOp::TTableDescription schema;
    newTableInfo->GetSchema(schema);

    if (move.GetReMapIndex().HasReplacedPathId()) {
        const auto oldPathId = PathIdFromPathId(move.GetReMapIndex().GetReplacedPathId());
        newTableInfo->Indexes.erase(oldPathId);

        size_t id = 0;
        bool found = false;
        for (auto& indexDesc: *schema.MutableTableIndexes()) {
            Y_ABORT_UNLESS(indexDesc.HasPathOwnerId() && indexDesc.HasLocalPathId());
            auto pathId = TPathId(indexDesc.GetPathOwnerId(), indexDesc.GetLocalPathId());
            if (oldPathId == pathId) {
                found = true;
                break;
            } else {
                id++;
            }
        }

        if (found) {
            schema.MutableTableIndexes()->DeleteSubrange(id, 1);
        }
    }

    const auto remapPrevId = PathIdFromPathId(move.GetReMapIndex().GetSrcPathId());
    const auto remapNewId = PathIdFromPathId(move.GetReMapIndex().GetDstPathId());
    Y_ABORT_UNLESS(move.GetReMapIndex().HasDstName());
    const auto dstIndexName = move.GetReMapIndex().GetDstName();

    for (auto& indexDesc: *schema.MutableTableIndexes()) {
        Y_ABORT_UNLESS(indexDesc.HasPathOwnerId() && indexDesc.HasLocalPathId());
        auto prevPathId = TPathId(indexDesc.GetPathOwnerId(), indexDesc.GetLocalPathId());
        if (remapPrevId != prevPathId) {
            continue;
        }

        indexDesc.SetPathOwnerId(remapNewId.OwnerId);
        indexDesc.SetLocalPathId(remapNewId.LocalPathId);

        newTableInfo->Indexes[remapNewId] = newTableInfo->Indexes[prevPathId];
        newTableInfo->Indexes.erase(prevPathId);

        Y_ABORT_UNLESS(move.GetReMapIndex().HasDstName());
        indexDesc.SetName(dstIndexName);
        newTableInfo->Indexes[remapNewId].Name = dstIndexName;
    }

    newTableInfo->SetSchema(schema);

    AddUserTable(pathId, newTableInfo);

    if (newTableInfo->NeedSchemaSnapshots()) {
        AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
    }

    NIceDb::TNiceDb db(txc.DB);
    PersistUserTable(db, pathId.LocalPathId, *newTableInfo);

    return newTableInfo;
}

TUserTable::TPtr TDataShard::AlterUserTable(const TActorContext& ctx, TTransactionContext& txc,
                                                   const NKikimrSchemeOp::TTableDescription& alter)
{
    ui64 tableId = alter.GetId_Deprecated();
    if (alter.HasPathId()) {
        Y_ABORT_UNLESS(GetPathOwnerId() == alter.GetPathId().GetOwnerId());
        tableId = alter.GetPathId().GetLocalId();
    }
    TUserTable::TCPtr oldTable = TableInfos[tableId];
    Y_ABORT_UNLESS(oldTable);

    TUserTable::TPtr tableInfo = new TUserTable(*oldTable, alter);
    TString strError;
    tableInfo->ApplyAlter(txc, *oldTable, alter, strError);
    if (strError) {
        LOG_ERROR(ctx, NKikimrServices::TX_DATASHARD,
            "Cannot alter datashard %" PRIu64 " for table %" PRIu64 ": %s",
            TabletID(), tableId, strError.data());
    }

    NIceDb::TNiceDb db(txc.DB);

    if (alter.HasPartitionConfig()) {
        // We are going to update table schema and save it
        NKikimrSchemeOp::TTableDescription tableDescr;
        tableInfo->GetSchema(tableDescr);

        const auto& configDelta = alter.GetPartitionConfig();
        auto& config = *tableDescr.MutablePartitionConfig();

        if (configDelta.HasFreezeState()) {
            auto cmd = configDelta.GetFreezeState();
            State = cmd == NKikimrSchemeOp::EFreezeState::Freeze ? TShardState::Frozen : TShardState::Ready;
            PersistSys(db, Schema::Sys_State, State);
        }

        if (configDelta.HasTxReadSizeLimit()) {
            config.SetTxReadSizeLimit(configDelta.GetTxReadSizeLimit());
            TxReadSizeLimit = configDelta.GetTxReadSizeLimit();
            PersistSys(db, Schema::Sys_TxReadSizeLimit, TxReadSizeLimit);
        }

        if (configDelta.HasDisableStatisticsCalculation()) {
            StatisticsDisabled = configDelta.GetDisableStatisticsCalculation() ? 1 : 0;
            PersistSys(db, Schema::Sys_StatisticsDisabled, StatisticsDisabled);
        }

        if (configDelta.HasPipelineConfig()) {
            config.ClearPipelineConfig();
            config.MutablePipelineConfig()->CopyFrom(configDelta.GetPipelineConfig());
            Pipeline.UpdateConfig(db, configDelta.GetPipelineConfig());
        }

        tableInfo->SetSchema(tableDescr);

        if (configDelta.HasKeepSnapshotTimeout())
            SnapshotManager.SetKeepSnapshotTimeout(db, configDelta.GetKeepSnapshotTimeout());
    }

    PersistUserTable(db, tableId, *tableInfo);

    return tableInfo;
}

void TDataShard::DropUserTable(TTransactionContext& txc, ui64 tableId) {
    auto ti = TableInfos.find(tableId);
    Y_ABORT_UNLESS(ti != TableInfos.end(), "Table with id %" PRIu64 " doesn't exist on this datashard", tableId);

    NIceDb::TNiceDb db(txc.DB);
    txc.DB.NoMoreReadsForTx();
    txc.DB.Alter().DropTable(ti->second->LocalTid);
    if (ti->second->ShadowTid) {
        txc.DB.Alter().DropTable(ti->second->ShadowTid);
    }
    db.Table<Schema::UserTables>().Key(ti->first).Delete();
    db.Table<Schema::UserTablesStats>().Key(ti->first).Delete();

    TableInfos.erase(ti);
}

void TDataShard::DropAllUserTables(TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    txc.DB.NoMoreReadsForTx();

    // All scheme changes must happen first
    for (const auto& ti : TableInfos) {
        txc.DB.Alter().DropTable(ti.second->LocalTid);
        if (ti.second->ShadowTid) {
            txc.DB.Alter().DropTable(ti.second->ShadowTid);
        }
    }

    // Now remove all snapshots and their info
    SnapshotManager.PersistRemoveAllSnapshots(db);
    for (const auto& ti : TableInfos) {
        db.Table<Schema::UserTables>().Key(ti.first).Delete();
        db.Table<Schema::UserTablesStats>().Key(ti.first).Delete();
    }

    TableInfos.clear();
}

// Deletes user table and all system tables that are transfered during split/merge
// This allows their borrowed parts to be returned to the owner tablet
void TDataShard::PurgeTxTables(TTransactionContext& txc) {
    TVector<ui32> tablesToDrop = {
        Schema::TxMain::TableId,
        Schema::TxDetails::TableId,
        Schema::InReadSets::TableId,
        Schema::PlanQueue::TableId,
        Schema::DeadlineQueue::TableId
    };
    for (ui32 ti : tablesToDrop) {
        txc.DB.Alter().DropTable(ti);
    }

    DropAllUserTables(txc);
}

void TDataShard::SnapshotComplete(TIntrusivePtr<NTabletFlatExecutor::TTableSnapshotContext> snapContext, const TActorContext &ctx) {
    if (auto txSnapContext = dynamic_cast<TTxTableSnapshotContext*>(snapContext.Get())) {
        auto stepOrder = txSnapContext->GetStepOrder();
        auto op = Pipeline.GetActiveOp(stepOrder.TxId);

        Y_DEBUG_ABORT_UNLESS(op, "The Tx that requested snapshot must be active!");
        if (!op) {
            LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD,
                       "Got snapshot for missing operation " << stepOrder
                       << " at " << TabletID());
            return;
        }

        Y_ABORT_UNLESS(txSnapContext->TablesToSnapshot().size() == 1,
                 "Currently only 1 table can be snapshotted");
        ui32 tableId = txSnapContext->TablesToSnapshot()[0];

        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD,
                  "Got snapshot in active state at %" PRIu64 " for table %" PRIu32 " txId %" PRIu64,
                  TabletID(), tableId, stepOrder.TxId);

        op->AddInputSnapshot(snapContext);
        Pipeline.AddCandidateOp(op);
        PlanQueue.Progress(ctx);
        return;
    }

    if (auto splitSnapContext = dynamic_cast<TSplitSnapshotContext*>(snapContext.Get())) {
        Execute(CreateTxSplitSnapshotComplete(splitSnapContext), ctx);
        return;
    }

    Y_ABORT("Unexpected table snapshot context");
}

TUserTable::TSpecialUpdate TDataShard::SpecialUpdates(const NTable::TDatabase& db, const TTableId& tableId) const {
    Y_ABORT_UNLESS(tableId.PathId.OwnerId == PathOwnerId, "%" PRIu64 " vs %" PRIu64,
             tableId.PathId.OwnerId, PathOwnerId);

    auto it = TableInfos.find(tableId.PathId.LocalPathId);
    Y_ABORT_UNLESS(it != TableInfos.end());
    const TUserTable& tableInfo = *it->second;
    Y_ABORT_UNLESS(tableInfo.LocalTid != Max<ui32>());

    TUserTable::TSpecialUpdate ret;

    if (tableInfo.SpecialColTablet != Max<ui32>()) {
        ret.ColIdTablet = tableInfo.SpecialColTablet;
        ret.Tablet = TabletID();

        ret.HasUpdates = true;
    }

    if (tableInfo.SpecialColEpoch != Max<ui32>() || tableInfo.SpecialColUpdateNo != Max<ui32>()) {
        auto dbChange = db.Head(tableInfo.LocalTid);
        ret.ColIdEpoch = tableInfo.SpecialColEpoch;
        ret.ColIdUpdateNo = tableInfo.SpecialColUpdateNo;

        ret.Epoch = dbChange.Epoch.ToCounter();
        ret.UpdateNo = dbChange.Serial;

        ret.HasUpdates = true;
    }

    return ret;
}

void TDataShard::SetTableAccessTime(const TTableId& tableId, TInstant ts) {
    Y_ABORT_UNLESS(!TSysTables::IsSystemTable(tableId));
    auto iter = TableInfos.find(tableId.PathId.LocalPathId);
    Y_ABORT_UNLESS(iter != TableInfos.end());
    iter->second->Stats.AccessTime = ts;
}

void TDataShard::SetTableUpdateTime(const TTableId& tableId, TInstant ts) {
    Y_ABORT_UNLESS(!TSysTables::IsSystemTable(tableId));
    auto iter = TableInfos.find(tableId.PathId.LocalPathId);
    Y_ABORT_UNLESS(iter != TableInfos.end());
    iter->second->Stats.AccessTime = ts;
    iter->second->Stats.UpdateTime = ts;
}

void TDataShard::SampleKeyAccess(const TTableId& tableId, const TArrayRef<const TCell>& row) {
    Y_ABORT_UNLESS(!TSysTables::IsSystemTable(tableId));

    auto iter = TableInfos.find(tableId.PathId.LocalPathId);
    Y_ABORT_UNLESS(iter != TableInfos.end());

    const ui64 samplingKeyPrefixSize = row.size();
    TArrayRef<const TCell> key(row.data(), samplingKeyPrefixSize);
    iter->second->Stats.AccessStats.Add(key);
}

NMiniKQL::IKeyAccessSampler::TPtr TDataShard::GetKeyAccessSampler() {
    return CurrentKeySampler;
}

void TDataShard::EnableKeyAccessSampling(const TActorContext &ctx, TInstant until) {
    if (CurrentKeySampler == DisabledKeySampler) {
        for (auto& table : TableInfos) {
            table.second->Stats.AccessStats.Clear();
        }
        CurrentKeySampler = EnabledKeySampler;
        StartedKeyAccessSamplingAt = AppData(ctx)->TimeProvider->Now();
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "Started key access sampling at datashard: " << TabletID());
    } else {
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "Extended key access sampling at datashard: " << TabletID());
    }
    StopKeyAccessSamplingAt = until;
}

bool TDataShard::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) {
    if (!Executor() || !Executor()->GetStats().IsActive)
        return false;

    if (!ev)
        return true;

    LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "Handle TEvRemoteHttpInfo: %s", ev->Get()->Query.data());

    auto cgi = ev->Get()->Cgi();

    if (const auto& action = cgi.Get("action")) {
        if (action == "cleanup-borrowed-parts") {
            Execute(CreateTxMonitoringCleanupBorrowedParts(this, ev), ctx);
            return true;
        }

        if (action == "reset-schema-version") {
            Execute(CreateTxMonitoringResetSchemaVersion(this, ev), ctx);
            return true;
        }

        if (action == "key-access-sample") {
            TDuration duration = TDuration::Seconds(120);
            EnableKeyAccessSampling(ctx, ctx.Now() + duration);
            ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes("Enabled key access sampling for " + duration.ToString()));
            return true;
        }

        ctx.Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPNOTFOUND));
        return true;
    }

    if (const auto& page = cgi.Get("page")) {
        if (page == "main") {
            // fallthrough
        } else if (page == "change-sender") {
            if (OutChangeSender) {
                ctx.Send(ev->Forward(OutChangeSender));
                return true;
            } else {
                ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes("Change sender is not running"));
                return true;
            }
        } else {
            ctx.Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPNOTFOUND));
            return true;
        }
    }

    Execute(CreateTxMonitoring(this, ev), ctx);
    return true;
}

ui64 TDataShard::GetMemoryUsage() const {
    ui64 res = sizeof(TDataShard) + (20 << 10); //basic value
    res += Pipeline.GetInactiveTxSize();
    return res;
}

bool TDataShard::ByKeyFilterDisabled() const {
    return DisableByKeyFilter;
}

bool TDataShard::AllowCancelROwithReadsets() const {
    return CanCancelROWithReadSets;
}

bool TDataShard::IsMvccEnabled() const {
    return SnapshotManager.IsMvccEnabled();
}

TReadWriteVersions TDataShard::GetLocalReadWriteVersions() const {
    if (IsFollower())
        return {TRowVersion::Max(), TRowVersion::Max()};

    if (!IsMvccEnabled())
        return {TRowVersion::Max(), SnapshotManager.GetMinWriteVersion()};

    TRowVersion edge = Max(
            SnapshotManager.GetCompleteEdge(),
            SnapshotManager.GetIncompleteEdge(),
            SnapshotManager.GetUnprotectedReadEdge());

    if (auto nextOp = Pipeline.GetNextPlannedOp(edge.Step, edge.TxId))
        return TRowVersion(nextOp->GetStep(), nextOp->GetTxId());

    TRowVersion maxEdge(edge.Step, ::Max<ui64>());

    return Max(maxEdge, edge.Next(), SnapshotManager.GetImmediateWriteEdge());
}

TRowVersion TDataShard::GetMvccTxVersion(EMvccTxMode mode, TOperation* op) const {
    Y_DEBUG_ABORT_UNLESS(IsMvccEnabled());

    if (op) {
        if (op->IsMvccSnapshotRead()) {
            return op->GetMvccSnapshot();
        }

        if (op->GetStep()) {
            return TRowVersion(op->GetStep(), op->GetTxId());
        }
    }

    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "GetMvccTxVersion at " << TabletID()
        << " CompleteEdge# " << SnapshotManager.GetCompleteEdge()
        << " IncompleteEdge# " << SnapshotManager.GetIncompleteEdge()
        << " UnprotectedReadEdge# " << SnapshotManager.GetUnprotectedReadEdge()
        << " ImmediateWriteEdge# " << SnapshotManager.GetImmediateWriteEdge()
        << " ImmediateWriteEdgeReplied# " << SnapshotManager.GetImmediateWriteEdgeReplied());

    TRowVersion version = [&]() {
        TRowVersion edge;
        TRowVersion readEdge = Max(
                SnapshotManager.GetCompleteEdge(),
                SnapshotManager.GetUnprotectedReadEdge());
        TRowVersion writeEdge = Max(readEdge, SnapshotManager.GetIncompleteEdge());
        switch (mode) {
            case EMvccTxMode::ReadOnly:
                // With read-only transactions we don't need reads to include
                // changes made at the incomplete edge, as that is a point where
                // distributed transactions performed some reads, not writes.
                // Since incomplete transactions are still inflight, the actual
                // version will stick to the first incomplete transaction is queue,
                // effectively reading non-repeatable state before that transaction.
                edge = readEdge;
                break;
            case EMvccTxMode::ReadWrite:
                // With read-write transactions we must choose a point that is
                // greater than both complete and incomplete edges. The reason
                // is that incomplete transactions performed some reads at that
                // point and these snapshot points must be repeatable.
                // Note that as soon as the first write past the IncompleteEdge
                // happens it cements all distributed transactions up to that point
                // as complete, so all future reads and writes are guaranteed to
                // include that point as well.
                edge = writeEdge;
                break;
        }

        // If there's any planned operation that is above our edge, it would be a
        // suitable version for a new immediate operation. We effectively try to
        // execute "before" that point if possible.
        if (auto nextOp = Pipeline.GetNextPlannedOp(edge.Step, edge.TxId))
            return TRowVersion(nextOp->GetStep(), nextOp->GetTxId());

        // Normally we stick transactions to the end of the last known mediator step
        // Note this calculations only happen when we don't have distributed
        // transactions left in queue, and we won't have any more transactions
        // up to the current mediator time. The mediator time itself may be stale,
        // in which case we may have evidence of its higher value via complete and
        // incomplete edges above.
        const ui64 mediatorStep = Max(MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : 0, writeEdge.Step);
        TRowVersion mediatorEdge(mediatorStep, ::Max<ui64>());

        switch (mode) {
            case EMvccTxMode::ReadOnly: {
                // We read at the end of the current step
                return mediatorEdge;
            }

            case EMvccTxMode::ReadWrite: {
                // We write at the end of the current step, or the start of the next step when that's protected
                return Max(mediatorEdge, writeEdge.Next());
            }
        }

        Y_ABORT("unreachable");
    }();

    switch (mode) {
        case EMvccTxMode::ReadOnly: {
            // We must read all writes we have replied to already
            return Max(version, SnapshotManager.GetImmediateWriteEdgeReplied());
        }

        case EMvccTxMode::ReadWrite: {
            // We must never go backwards in our single-shard writes
            return Max(version, SnapshotManager.GetImmediateWriteEdge());
        }
    }

    Y_ABORT("unreachable");
}

TReadWriteVersions TDataShard::GetReadWriteVersions(TOperation* op) const {
    if (IsFollower()) {
        return {TRowVersion::Max(), TRowVersion::Max()};
    }

    if (!IsMvccEnabled())
        return {TRowVersion::Max(), SnapshotManager.GetMinWriteVersion()};

    if (op) {
        if (!op->MvccReadWriteVersion) {
            op->MvccReadWriteVersion = GetMvccTxVersion(op->IsReadOnly() ? EMvccTxMode::ReadOnly : EMvccTxMode::ReadWrite, op);
        }

        return *op->MvccReadWriteVersion;
    }

    return GetMvccTxVersion(EMvccTxMode::ReadWrite, nullptr);
}

TDataShard::TPromotePostExecuteEdges TDataShard::PromoteImmediatePostExecuteEdges(
        const TRowVersion& version, EPromotePostExecuteEdges mode, TTransactionContext& txc)
{
    Y_ABORT_UNLESS(!IsFollower(), "Unexpected attempt to promote edges on a follower");

    TPromotePostExecuteEdges res;

    res.HadWrites |= Pipeline.MarkPlannedLogicallyCompleteUpTo(version, txc);

    switch (mode) {
        case EPromotePostExecuteEdges::ReadOnly:
            // We want read-only immediate transactions to be readonly, thus
            // don't promote the complete edge unnecessarily. On restarts we
            // will assume anything written is potentially replied anyway,
            // even if it has never been read.
            break;

        case EPromotePostExecuteEdges::RepeatableRead: {
            // We want to use unprotected reads, but we need to make sure it's properly marked first
            if (!SnapshotManager.GetPerformedUnprotectedReads()) {
                SnapshotManager.SetPerformedUnprotectedReads(true, txc);
                res.HadWrites = true;
            }
            if (!res.HadWrites && !SnapshotManager.IsPerformedUnprotectedReadsCommitted()) {
                // We need to wait for completion until the flag is committed
                res.WaitCompletion = true;
            }
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "PromoteImmediatePostExecuteEdges at " << TabletID()
                << " promoting UnprotectedReadEdge to " << version);
            SnapshotManager.PromoteUnprotectedReadEdge(version);

            // Make sure pending distributed transactions are marked incomplete,
            // since we just protected up to and including version from writes,
            // we need to make sure new immediate conflicting writes are blocked
            // and don't perform writes with out-of-order versions.
            res.HadWrites |= Pipeline.MarkPlannedLogicallyIncompleteUpTo(version, txc);

            // We want to promote the complete edge when protected reads are
            // used or when we're already writing something anyway.
            if (res.HadWrites) {
                res.HadWrites |= SnapshotManager.PromoteCompleteEdge(version, txc);
                if (!res.HadWrites && SnapshotManager.GetCommittedCompleteEdge() < version) {
                    // We need to wait for completion because some other transaction
                    // has moved complete edge, but it's not committed yet.
                    res.WaitCompletion = true;
                }
            }

            break;
        }

        case EPromotePostExecuteEdges::ReadWrite: {
            if (version.Step <= GetMaxObservedStep()) {
                res.HadWrites |= SnapshotManager.PromoteCompleteEdge(version.Step, txc);
            }
            res.HadWrites |= SnapshotManager.PromoteImmediateWriteEdge(version, txc);
            if (res.HadWrites) {
                // Promoting write edges may promote read edge
                PromoteFollowerReadEdge(txc);
            }
            break;
        }
    }

    return res;
}

ui64 TDataShard::GetMaxObservedStep() const {
    return Max(
        Pipeline.GetLastPlannedTx().Step,
        SnapshotManager.GetCompleteEdge().Step,
        SnapshotManager.GetIncompleteEdge().Step,
        SnapshotManager.GetUnprotectedReadEdge().Step,
        MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : 0);
}

void TDataShard::SendImmediateWriteResult(
        const TRowVersion& version, const TActorId& target, IEventBase* event, ui64 cookie)
{
    const ui64 step = version.Step;
    const ui64 observedStep = GetMaxObservedStep();
    if (step <= observedStep) {
        SnapshotManager.PromoteImmediateWriteEdgeReplied(version);
        Send(target, event, 0, cookie);
        return;
    }

    MediatorDelayedReplies.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(version),
        std::forward_as_tuple(target, THolder<IEventBase>(event), cookie));

    // Try to subscribe to the next step, when needed
    if (MediatorTimeCastEntry && (MediatorTimeCastWaitingSteps.empty() || step < *MediatorTimeCastWaitingSteps.begin())) {
        MediatorTimeCastWaitingSteps.insert(step);
        Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), step));
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Waiting for PlanStep# " << step << " from mediator time cast");
    }
}

TMonotonic TDataShard::ConfirmReadOnlyLease() {
    if (IsFollower() || !ReadOnlyLeaseEnabled()) {
        // Do nothing and return an empty timestamp
        return {};
    }

    TMonotonic ts = AppData()->MonotonicTimeProvider->Now();
    Executor()->ConfirmReadOnlyLease(ts);
    return ts;
}

void TDataShard::ConfirmReadOnlyLease(TMonotonic ts) {
    if (IsFollower() || !ReadOnlyLeaseEnabled()) {
        // Do nothing
        return;
    }

    Executor()->ConfirmReadOnlyLease(ts);
}

void TDataShard::SendWithConfirmedReadOnlyLease(
    TMonotonic ts,
    const TActorId& target,
    IEventBase* event,
    ui64 cookie,
    const TActorId& sessionId)
{
    if (IsFollower() || !ReadOnlyLeaseEnabled()) {
        // Send possibly stale result (legacy behavior)
        if (!sessionId) {
            Send(target, event, 0, cookie);
        } else {
            SendViaSession(sessionId, target, SelfId(), event);
        }
        return;
    }

    struct TSendState : public TThrRefBase {
        THolder<IEventHandle> Ev;

        TSendState(const TActorId& sessionId, const TActorId& target, const TActorId& src, IEventBase* event, ui64 cookie)
        {
            const ui32 flags = 0;
            Ev = MakeHolder<IEventHandle>(target, src, event, flags, cookie);

            if (sessionId) {
                Ev->Rewrite(TEvInterconnect::EvForward, sessionId);
            }
        }
    };

    if (!ts) {
        ts = AppData()->MonotonicTimeProvider->Now();
    }

    Executor()->ConfirmReadOnlyLease(ts,
        [state = MakeIntrusive<TSendState>(sessionId, target, SelfId(), event, cookie)] {
            TActivationContext::Send(state->Ev.Release());
    });
}

void TDataShard::SendWithConfirmedReadOnlyLease(
    const TActorId& target,
    IEventBase* event,
    ui64 cookie,
    const TActorId& sessionId)
{
    SendWithConfirmedReadOnlyLease(TMonotonic::Zero(), target, event, cookie, sessionId);
}

void TDataShard::Handle(TEvPrivate::TEvConfirmReadonlyLease::TPtr& ev, const TActorContext&) {
    SendWithConfirmedReadOnlyLease(ev->Get()->Timestamp, ev->Sender, new TEvPrivate::TEvReadonlyLeaseConfirmation, ev->Cookie);
}

void TDataShard::SendImmediateReadResult(
    TMonotonic readTime,
    const TActorId& target,
    IEventBase* event,
    ui64 cookie,
    const TActorId& sessionId)
{
    SendWithConfirmedReadOnlyLease(readTime, target, event, cookie, sessionId);
}

void TDataShard::SendImmediateReadResult(
    const TActorId& target,
    IEventBase* event,
    ui64 cookie,
    const TActorId& sessionId)
{
    SendWithConfirmedReadOnlyLease(TMonotonic::Zero(), target, event, cookie, sessionId);
}

void TDataShard::SendAfterMediatorStepActivate(ui64 mediatorStep, const TActorContext& ctx) {
    for (auto it = MediatorDelayedReplies.begin(); it != MediatorDelayedReplies.end();) {
        const ui64 step = it->first.Step;

        if (SrcSplitDescription) {
            if (State == TShardState::SplitSrcSendingSnapshot ||
                State == TShardState::SplitSrcWaitForPartitioningChanged ||
                State == TShardState::PreOffline ||
                State == TShardState::Offline)
            {
                // We cannot send replies, since dst shard is now in charge
                // of keeping track of acknowledged writes. So we expect
                // split src logic to reboot this shard later.
                break;
            }
        }

        if (step <= mediatorStep) {
            SnapshotManager.PromoteImmediateWriteEdgeReplied(it->first);
            Send(it->second.Target, it->second.Event.Release(), 0, it->second.Cookie);
            it = MediatorDelayedReplies.erase(it);
            continue;
        }

        // Try to subscribe to the next step, when needed
        if (MediatorTimeCastEntry && (MediatorTimeCastWaitingSteps.empty() || step < *MediatorTimeCastWaitingSteps.begin())) {
            MediatorTimeCastWaitingSteps.insert(step);
            Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), step));
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Waiting for PlanStep# " << step << " from mediator time cast");
        }
        break;
    }

    if (Pipeline.HasPredictedPlan()) {
        ui64 nextStep = Pipeline.NextPredictedPlanStep();
        if (nextStep <= mediatorStep) {
            SchedulePlanPredictedTxs();
        } else {
            WaitPredictedPlanStep(nextStep);
        }
    }

    if (IsMvccEnabled()) {
        PromoteFollowerReadEdge();
    }

    EmitHeartbeats();
}

class TDataShard::TTxMediatorStateRestored : public TTransactionBase<TDataShard> {
public:
    TTxMediatorStateRestored(TDataShard* self, ui64 readStep, ui64 observedStep)
        : TTransactionBase(self)
        , ReadStep(readStep)
        , ObservedStep(observedStep)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Y_ABORT_UNLESS(Self->MediatorStateRestoreTxPending);
        Self->MediatorStateRestoreTxPending = false;

        Self->FinishMediatorStateRestore(txc, ReadStep, ObservedStep);
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const ui64 ReadStep;
    const ui64 ObservedStep;
};

void TDataShard::CheckMediatorStateRestored() {
    if (!MediatorStateWaiting ||
        !RegistrationSended ||
        !MediatorTimeCastEntry ||
        CoordinatorSubscriptionsPending > 0 && CoordinatorPrevReadStepMax == Max<ui64>())
    {
        // We are not waiting or not ready to make a decision
        return;
    }

    if (MediatorStateRestoreTxPending) {
        // We already made a decision and are waiting for transaction to execute
        return;
    }

    // CoordinatorPrevReadStepMax shows us what is the next minimum step that
    // may be acquired as a snapshot. This tells us that no previous read
    // could have happened after this step, even if it was acquired, since it
    // would have been waiting until mediator time advances to that step.
    // CoordinatorPrevReadStepMin shows us the maximum step that could have
    // been acquired at coordinators before we subscribed, however this does
    // not include possible local snapshots that could have been acquired by a
    // previous generation during iterator reads, so we have to always use
    // CoordinatorPrevReadStepMax as a worst case possible readStep.
    // Note we always need to wait for CoordinatorPrevReadStepMax even without
    // local snapshots, because previous generation may have observed it and
    // may have replied to immediate writes at that step, and new immediate
    // HEAD reads must include that in their results.
    const ui64 waitStep = CoordinatorPrevReadStepMax;
    const ui64 readStep = CoordinatorPrevReadStepMax;
    const ui64 observedStep = GetMaxObservedStep();
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "CheckMediatorStateRestored at " << TabletID() << ":"
        << " waitStep# " << waitStep
        << " readStep# " << readStep
        << " observedStep# " << observedStep);

    // WARNING: we must perform this check BEFORE we update unprotected read edge
    // We may enter this code path multiple times, and we expect that the above
    // read step may be refined while we wait based on pessimistic backup step.
    if (observedStep < waitStep) {
        // We need to wait until we observe mediator step that is at least
        // as large as the step we found.
        if (MediatorTimeCastWaitingSteps.insert(waitStep).second) {
            Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), waitStep));
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Waiting for PlanStep# " << waitStep << " from mediator time cast");
        }
        return;
    }

    MediatorStateRestoreTxPending = true;
    Execute(new TTxMediatorStateRestored(this, readStep, observedStep));
}

void TDataShard::FinishMediatorStateRestore(TTransactionContext& txc, ui64 readStep, ui64 observedStep) {
    Y_ABORT_UNLESS(MediatorStateWaiting);

    // Using the inferred last read step we restore the pessimistic unprotected
    // read edge. Note we only need to do so if there have actually been any
    // unprotected reads in this datashard history. We also need to make sure
    // this edge is at least one smaller than ImmediateWriteEdge when we know
    // we started unconfirmed immediate writes in the last generation.
    if (SnapshotManager.GetPerformedUnprotectedReads()) {
        const TRowVersion lastReadEdge(readStep, Max<ui64>());
        const TRowVersion preImmediateWriteEdge =
            SnapshotManager.GetImmediateWriteEdge().Step > SnapshotManager.GetCompleteEdge().Step
            ? SnapshotManager.GetImmediateWriteEdge().Prev()
            : TRowVersion::Min();
        const TRowVersion edge = Max(lastReadEdge, preImmediateWriteEdge);
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "CheckMediatorStateRestored at " << TabletID()
            << " promoting UnprotectedReadEdge to " << edge);
        Pipeline.MarkPlannedLogicallyCompleteUpTo(edge, txc);
        Pipeline.MarkPlannedLogicallyIncompleteUpTo(edge, txc);
        SnapshotManager.PromoteUnprotectedReadEdge(edge);
    }

    // Promote the replied immediate write edge up to the currently observed step
    // This is needed to make sure we read any potentially replied immediate
    // writes before the restart, and conversely don't accidentally read any
    // data that is definitely not replied yet.
    if (SnapshotManager.GetImmediateWriteEdgeReplied() < SnapshotManager.GetImmediateWriteEdge()) {
        const ui64 writeStep = SnapshotManager.GetImmediateWriteEdge().Step;
        const TRowVersion edge(observedStep, Max<ui64>());
        SnapshotManager.PromoteImmediateWriteEdgeReplied(
            Min(edge, SnapshotManager.GetImmediateWriteEdge()));
        // Try to ensure writes become visible sooner rather than later
        if (edge.Step < writeStep) {
            if (MediatorTimeCastWaitingSteps.insert(writeStep).second) {
                Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), writeStep));
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Waiting for PlanStep# " << writeStep << " from mediator time cast");
            }
        }
    }

    MediatorStateWaiting = false;

    // Resend all waiting messages
    TVector<THolder<IEventHandle>> msgs;
    msgs.swap(MediatorStateWaitingMsgs);
    for (auto& ev : msgs) {
        TActivationContext::Send(ev.Release());
    }
}

NKikimrTxDataShard::TError::EKind ConvertErrCode(NMiniKQL::IEngineFlat::EResult code) {
    using EResult = NMiniKQL::IEngineFlat::EResult;

    switch (code) {
    case EResult::Ok:
        return NKikimrTxDataShard::TError::OK;
    case EResult::SnapshotNotReady:
        return NKikimrTxDataShard::TError::SNAPSHOT_NOT_READY_YET;
    case EResult::SchemeChanged:
        return NKikimrTxDataShard::TError::SCHEME_CHANGED;
    case EResult::IsReadonly:
        return NKikimrTxDataShard::TError::READONLY;
    case EResult::KeyError:
        return NKikimrTxDataShard::TError::SCHEME_ERROR;
    case EResult::ProgramError:
        return NKikimrTxDataShard::TError::PROGRAM_ERROR;
    case EResult::TooManyData:
        return NKikimrTxDataShard::TError::READ_SIZE_EXECEEDED;
    case EResult::SnapshotNotExist:
        return NKikimrTxDataShard::TError::SNAPSHOT_NOT_EXIST;
    case EResult::ResultTooBig:
        return NKikimrTxDataShard::TError::REPLY_SIZE_EXCEEDED;
    case EResult::Cancelled:
        return NKikimrTxDataShard::TError::EXECUTION_CANCELLED;
    default:
        return NKikimrTxDataShard::TError::UNKNOWN;
    }
}

Ydb::StatusIds::StatusCode ConvertToYdbStatusCode(NKikimrTxDataShard::TError::EKind code) {
    switch (code) {
        case NKikimrTxDataShard::TError::OK:
            return Ydb::StatusIds::SUCCESS;
        case NKikimrTxDataShard::TError::BAD_TX_KIND:
        case NKikimrTxDataShard::TError::SCHEME_ERROR:
        case NKikimrTxDataShard::TError::WRONG_PAYLOAD_TYPE:
        case NKikimrTxDataShard::TError::LEAF_REQUIRED:
        case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
        case NKikimrTxDataShard::TError::PROGRAM_ERROR:
        case NKikimrTxDataShard::TError::OUT_OF_SPACE:
        case NKikimrTxDataShard::TError::READ_SIZE_EXECEEDED:
        case NKikimrTxDataShard::TError::SHARD_IS_BLOCKED:
        case NKikimrTxDataShard::TError::UNKNOWN:
        case NKikimrTxDataShard::TError::REPLY_SIZE_EXCEEDED:
        case NKikimrTxDataShard::TError::EXECUTION_CANCELLED:
        case NKikimrTxDataShard::TError::DISK_SPACE_EXHAUSTED:
            return Ydb::StatusIds::INTERNAL_ERROR;
        case NKikimrTxDataShard::TError::BAD_ARGUMENT:
        case NKikimrTxDataShard::TError::READONLY:
        case NKikimrTxDataShard::TError::SNAPSHOT_NOT_READY_YET:
        case NKikimrTxDataShard::TError::SCHEME_CHANGED:
        case NKikimrTxDataShard::TError::DUPLICATED_SNAPSHOT_POLICY:
        case NKikimrTxDataShard::TError::MISSING_SNAPSHOT_POLICY:
            return Ydb::StatusIds::BAD_REQUEST;
        case NKikimrTxDataShard::TError::SNAPSHOT_NOT_EXIST:
            return Ydb::StatusIds::NOT_FOUND;
        default:
            return Ydb::StatusIds::GENERIC_ERROR;
    }
}

void TDataShard::Handle(TEvents::TEvGone::TPtr &ev) {
    Actors.erase(ev->Sender);
}

void TDataShard::Handle(TEvDataShard::TEvGetShardState::TPtr &ev, const TActorContext &ctx) {
    Execute(new TTxGetShardState(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvSchemaChangedResult::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Handle TEvSchemaChangedResult " << ev->Get()->Record.GetTxId()
                << "  datashard " << TabletID()
                << " state " << DatashardStateName(State));
    Execute(CreateTxSchemaChanged(ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvStateChangedResult::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Handle TEvStateChangedResult "
                << "  datashard " << TabletID()
                << " state " << DatashardStateName(State));
    // TODO: implement
    NTabletPipe::CloseAndForgetClient(SelfId(), StateReportPipe);
}

bool TDataShard::CheckDataTxReject(const TString& opDescr,
                                          const TActorContext &ctx,
                                          NKikimrTxDataShard::TEvProposeTransactionResult::EStatus &rejectStatus,
                                          ERejectReasons &rejectReasons,
                                          TString &rejectDescription)
{
    bool reject = false;
    rejectStatus = NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED;
    rejectReasons = ERejectReasons::None;
    TVector<TString> rejectDescriptions;

    // In v0.5 reject all transactions on split Src after receiving EvSplit
    if (State == TShardState::SplitSrcWaitForNoTxInFlight ||
        State == TShardState::SplitSrcMakeSnapshot ||
        State == TShardState::SplitSrcSendingSnapshot ||
        State == TShardState::SplitSrcWaitForPartitioningChanged) {
        reject = true;
        rejectReasons |= ERejectReasons::WrongState;
        rejectDescriptions.push_back(TStringBuilder()
            << "is in process of split opId " << SrcSplitOpId
            << " state " << DatashardStateName(State)
            << " (wrong shard state)");
    } else if (State == TShardState::SplitDstReceivingSnapshot) {
        reject = true;
        rejectReasons |= ERejectReasons::WrongState;
        rejectDescriptions.push_back(TStringBuilder()
            << "is in process of split opId " << DstSplitOpId
            << " state " << DatashardStateName(State));
    } else if (State == TShardState::WaitScheme) {
        reject = true;
        rejectReasons |= ERejectReasons::WrongState;
        rejectDescriptions.push_back("is not created yet");
    } else if (State == TShardState::PreOffline || State == TShardState::Offline) {
        reject = true;
        rejectStatus = NKikimrTxDataShard::TEvProposeTransactionResult::ERROR;
        rejectReasons |= ERejectReasons::WrongState;
        rejectDescriptions.push_back("is in a pre/offline state assuming this is due to a finished split (wrong shard state)");
    } else if (MvccSwitchState == TSwitchState::SWITCHING) {
        reject = true;
        rejectReasons |= ERejectReasons::WrongState;
        rejectDescriptions.push_back(TStringBuilder()
            << "is in process of mvcc state change"
            << " state " << DatashardStateName(State));
    }

    if (Pipeline.HasDrop()) {
        reject = true;
        rejectStatus = NKikimrTxDataShard::TEvProposeTransactionResult::ERROR;
        rejectReasons |= ERejectReasons::Dropping;
        rejectDescriptions.push_back("is in process of drop");
    }

    ui64 txInfly = TxInFly();
    TDuration lag = GetDataTxCompleteLag();
    if (txInfly > 1 && lag > TDuration::MilliSeconds(MaxTxLagMilliseconds)) {
        reject = true;
        rejectReasons |= ERejectReasons::OverloadByLag;
        rejectDescriptions.push_back(TStringBuilder()
            << "lags behind, lag: " << lag
            << " in-flight tx count: " << txInfly);
    }

    const float rejectProbabilty = Executor()->GetRejectProbability();
    if (!reject && rejectProbabilty > 0) {
        float rnd = AppData(ctx)->RandomProvider->GenRandReal2();
        reject |= (rnd < rejectProbabilty);
        if (reject) {
            rejectReasons |= ERejectReasons::OverloadByProbability;
            rejectDescriptions.push_back("decided to reject due to given RejectProbability");
        }
    }

    size_t totalInFly =
        ReadIteratorsInFly() + TxInFly() + ImmediateInFly() + MediatorStateWaitingMsgs.size()
            + ProposeQueue.Size() + TxWaiting();
    if (totalInFly > GetMaxTxInFly()) {
        reject = true;
        rejectReasons |= ERejectReasons::OverloadByTxInFly;
        rejectDescriptions.push_back("MaxTxInFly was exceeded");
    }

    if (!reject && Stopping) {
        reject = true;
        rejectReasons |= ERejectReasons::WrongState;
        rejectDescriptions.push_back("is restarting");
    }

    if (!reject) {
        for (auto& it : TableInfos) {
            if (it.second->IsBackup) {
                reject = true;
                rejectStatus = NKikimrTxDataShard::TEvProposeTransactionResult::ERROR;
                rejectReasons |= ERejectReasons::WrongState;
                rejectDescriptions.push_back("is a backup table");
                break;
            }
        }
    }

    if (reject) {
        rejectDescription = TStringBuilder()
            << "Rejecting " << opDescr
            << " because datashard " << TabletID() << ": "
            << JoinSeq("; ", rejectDescriptions);
    }

    return reject;
}

bool TDataShard::CheckDataTxRejectAndReply(const TEvDataShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx)
{
    auto* msg = ev->Get();
    switch (msg->GetTxKind()) {
        case NKikimrTxDataShard::TX_KIND_DATA:
        case NKikimrTxDataShard::TX_KIND_SCAN:
        case NKikimrTxDataShard::TX_KIND_SNAPSHOT:
        case NKikimrTxDataShard::TX_KIND_DISTRIBUTED_ERASE:
        case NKikimrTxDataShard::TX_KIND_COMMIT_WRITES:
            break;
        default:
            return false;
    }

    TString txDescr = TStringBuilder() << "data TxId " << msg->GetTxId();

    NKikimrTxDataShard::TEvProposeTransactionResult::EStatus rejectStatus;
    ERejectReasons rejectReasons;
    TString rejectDescription;
    bool reject = CheckDataTxReject(txDescr, ctx, rejectStatus, rejectReasons, rejectDescription);

    if (reject) {
        LWTRACK(ProposeTransactionReject, msg->Orbit);
        THolder<TEvDataShard::TEvProposeTransactionResult> result =
            THolder(new TEvDataShard::TEvProposeTransactionResult(msg->GetTxKind(),
                                                            TabletID(),
                                                            msg->GetTxId(),
                                                            rejectStatus));

        result->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, rejectDescription);
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, rejectDescription);

        ctx.Send(ev->Sender, result.Release());
        IncCounter(COUNTER_PREPARE_OVERLOADED);
        IncCounter(COUNTER_PREPARE_COMPLETE);
        return true;
    }

    return false;
}

bool TDataShard::CheckDataTxRejectAndReply(const NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx)
{
    auto* msg = ev->Get();
    TString txDescr = TStringBuilder() << "data TxId " << msg->GetTxId();

    NKikimrTxDataShard::TEvProposeTransactionResult::EStatus rejectStatus;
    ERejectReasons rejectReasons;
    TString rejectDescription;
    bool reject = CheckDataTxReject(txDescr, ctx, rejectStatus, rejectReasons, rejectDescription);

    if (reject) {
        LWTRACK(ProposeTransactionReject, msg->GetOrbit());
        NKikimrDataEvents::TEvWriteResult::EStatus status;
        switch (rejectStatus) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED:
                status = NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED;
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR:
                status = NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR;
                break;
            default:
                Y_FAIL_S("Unexpected rejectStatus " << rejectStatus);
        }
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), msg->GetTxId(), status, rejectDescription);

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, rejectDescription);

        ctx.Send(ev->Sender, result.release());
        IncCounter(COUNTER_WRITE_OVERLOADED);
        IncCounter(COUNTER_WRITE_COMPLETE);
        return true;
    }

    return false;
}
void TDataShard::UpdateProposeQueueSize() const {
    SetCounter(COUNTER_PROPOSE_QUEUE_SIZE, MediatorStateWaitingMsgs.size() + ProposeQueue.Size() + DelayedProposeQueue.size() + Pipeline.WaitingTxs());
    SetCounter(COUNTER_READ_ITERATORS_WAITING, Pipeline.WaitingReadIterators());
}

void TDataShard::Handle(TEvDataShard::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
    auto* msg = ev->Get();
    LWTRACK(ProposeTransactionRequest, msg->Orbit);

    if (CheckDataTxRejectAndReply(ev, ctx)) {
        IncCounter(COUNTER_PREPARE_REQUEST);
        return;
    }

    // Check if we need to delay an immediate transaction
    if (MediatorStateWaiting &&
        (ev->Get()->GetFlags() & TTxFlags::Immediate) &&
        !(ev->Get()->GetFlags() & TTxFlags::ForceOnline))
    {
        // We cannot calculate correct version until we restore mediator state
        LWTRACK(ProposeTransactionWaitMediatorState, msg->Orbit);
        MediatorStateWaitingMsgs.emplace_back(ev.Release());
        UpdateProposeQueueSize();
        return;
    }

    if (Pipeline.HasProposeDelayers()) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "Handle TEvProposeTransaction delayed at " << TabletID() << " until dependency graph is restored");
        LWTRACK(ProposeTransactionWaitDelayers, msg->Orbit);
        DelayedProposeQueue.emplace_back().Reset(ev.Release());
        UpdateProposeQueueSize();
        return;
    }

    if (CheckTxNeedWait(ev)) {
         LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "Handle TEvProposeTransaction delayed at " << TabletID() << " until interesting plan step will come");
        if (Pipeline.AddWaitingTxOp(ev, ctx)) {
            UpdateProposeQueueSize();
            return;
        }
    }

    IncCounter(COUNTER_PREPARE_REQUEST);

    switch (ev->Get()->GetTxKind()) {
    case NKikimrTxDataShard::TX_KIND_DATA:
    case NKikimrTxDataShard::TX_KIND_SCAN:
    case NKikimrTxDataShard::TX_KIND_SNAPSHOT:
    case NKikimrTxDataShard::TX_KIND_DISTRIBUTED_ERASE:
    case NKikimrTxDataShard::TX_KIND_COMMIT_WRITES:
        ProposeTransaction(std::move(ev), ctx);
        return;
    case NKikimrTxDataShard::TX_KIND_SCHEME:
        ProposeTransaction(std::move(ev), ctx);
        return;
    default:
        break;
    }

    THolder<TEvDataShard::TEvProposeTransactionResult> result
        = THolder(new TEvDataShard::TEvProposeTransactionResult(ev->Get()->GetTxKind(),
                                                        TabletID(),
                                                        ev->Get()->GetTxId(),
                                                        NKikimrTxDataShard::TEvProposeTransactionResult::ERROR));
    result->AddError(NKikimrTxDataShard::TError::BAD_TX_KIND, "Unknown kind of transaction");
    ctx.Send(ev->Sender, result.Release());
    IncCounter(COUNTER_PREPARE_ERROR);
    IncCounter(COUNTER_PREPARE_COMPLETE);

    // TODO[serxa]: wake up! dont sleep! maybe...
    //Executor()->WakeUp(ctx);
}

void TDataShard::Handle(TEvDataShard::TEvProposeTransactionAttach::TPtr &ev, const TActorContext &ctx) {
    const auto &record = ev->Get()->Record;
    const ui64 txId = record.GetTxId();
    NKikimrProto::EReplyStatus status = NKikimrProto::NODATA;

    auto op = TransQueue.FindTxInFly(txId);
    if (!op) {
        op = Pipeline.FindCompletingOp(txId);
    }

    if (op && op->GetTarget() == ev->Sender && !op->IsImmediate() && op->HasStoredFlag() && !op->HasResultSentFlag()) {
        // This transaction is expected to send reply eventually
        status = NKikimrProto::OK;
    }

    ctx.Send(ev->Sender, new TEvDataShard::TEvProposeTransactionAttachResult(TabletID(), txId, status), 0, ev->Cookie);
}

void TDataShard::HandleAsFollower(TEvDataShard::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
    auto* msg = ev->Get();
    LWTRACK(ProposeTransactionRequest, msg->Orbit);

    IncCounter(COUNTER_PREPARE_REQUEST);

    if (TxInFly() > GetMaxTxInFly()) {
        THolder<TEvDataShard::TEvProposeTransactionResult> result =
            THolder(new TEvDataShard::TEvProposeTransactionResult(ev->Get()->GetTxKind(), TabletID(),
                ev->Get()->GetTxId(), NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED));
        ctx.Send(ev->Sender, result.Release());
        IncCounter(COUNTER_PREPARE_OVERLOADED);
        IncCounter(COUNTER_PREPARE_COMPLETE);
        return;
    }

    if (ev->Get()->GetTxKind() == NKikimrTxDataShard::TX_KIND_DATA) {
        ProposeTransaction(std::move(ev), ctx);
        return;
    }

    THolder<TEvDataShard::TEvProposeTransactionResult> result
        = THolder(new TEvDataShard::TEvProposeTransactionResult(ev->Get()->GetTxKind(),
                                                        TabletID(),
                                                        ev->Get()->GetTxId(),
                                                        NKikimrTxDataShard::TEvProposeTransactionResult::ERROR));
    result->AddError(NKikimrTxDataShard::TError::BAD_TX_KIND, "Unsupported transaction kind");
    ctx.Send(ev->Sender, result.Release());
    IncCounter(COUNTER_PREPARE_ERROR);
    IncCounter(COUNTER_PREPARE_COMPLETE);
}

void TDataShard::CheckDelayedProposeQueue(const TActorContext &ctx) {
    if (DelayedProposeQueue && !Pipeline.HasProposeDelayers()) {
        for (auto& ev : DelayedProposeQueue) {
            ctx.ExecutorThread.Send(ev.Release());
        }
        DelayedProposeQueue.clear();
        DelayedProposeQueue.shrink_to_fit();
        UpdateProposeQueueSize();
    }
}

void TDataShard::ProposeTransaction(TEvDataShard::TEvProposeTransaction::TPtr &&ev, const TActorContext &ctx) {
    auto* msg = ev->Get();

    // This transaction may run in immediate mode
    bool mayRunImmediate = (msg->GetFlags() & TTxFlags::Immediate) && !(msg->GetFlags() & TTxFlags::ForceOnline) &&
        msg->GetTxKind() == NKikimrTxDataShard::TX_KIND_DATA;

    if (mayRunImmediate) {
        // Enqueue immediate transactions so they don't starve existing operations
        LWTRACK(ProposeTransactionEnqueue, msg->Orbit);
        ProposeQueue.Enqueue(IEventHandle::Upcast<TEvDataShard::TEvProposeTransaction>(std::move(ev)), TAppData::TimeProvider->Now(), NextTieBreakerIndex++, ctx);
        UpdateProposeQueueSize();
    } else {
        // Prepare planned transactions as soon as possible
        NWilson::TSpan datashardTransactionSpan(TWilsonTablet::TabletTopLevel, std::move(ev->TraceId), "Datashard.Transaction", NWilson::EFlags::AUTO_END);
        if (datashardTransactionSpan) {
            datashardTransactionSpan.Attribute("Shard", std::to_string(TabletID()));
        }

        Execute(new TTxProposeTransactionBase(this, std::move(ev), TAppData::TimeProvider->Now(), NextTieBreakerIndex++, /* delayed */ false, std::move(datashardTransactionSpan)), ctx);
    }
}

void TDataShard::ProposeTransaction(NEvents::TDataEvents::TEvWrite::TPtr&& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    // This transaction may run in immediate mode
    bool mayRunImmediate = record.txmode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE;

    if (mayRunImmediate) {
        // Enqueue immediate transactions so they don't starve existing operations
        LWTRACK(ProposeTransactionEnqueue, msg->GetOrbit());
        ProposeQueue.Enqueue(IEventHandle::Upcast<NEvents::TDataEvents::TEvWrite>(std::move(ev)), TAppData::TimeProvider->Now(), NextTieBreakerIndex++, ctx);
        UpdateProposeQueueSize();
    } else {
        // Prepare planned transactions as soon as possible
        NWilson::TSpan datashardTransactionSpan(TWilsonTablet::TabletTopLevel, std::move(ev->TraceId), "Datashard.WriteTransaction", NWilson::EFlags::AUTO_END);
        if (datashardTransactionSpan) {
            datashardTransactionSpan.Attribute("Shard", std::to_string(TabletID()));
        }

        Execute(new TTxWrite(this, std::move(ev), TAppData::TimeProvider->Now(), NextTieBreakerIndex++, /* delayed */ false, std::move(datashardTransactionSpan)), ctx);
    }
}

void TDataShard::Handle(TEvTxProcessing::TEvPlanStep::TPtr &ev, const TActorContext &ctx) {
    ui64 srcMediatorId = ev->Get()->Record.GetMediatorID();
    if (!CheckMediatorAuthorisation(srcMediatorId)) {
        LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD, "tablet " << TabletID() <<
                   " receive PlanStep " << ev->Get()->Record.GetStep() <<
                   " from unauthorized mediator " << srcMediatorId);
        HandlePoison(ctx);
        return;
    }

    Execute(new TTxPlanStep(this, ev), ctx);
}

void TDataShard::Handle(TEvTxProcessing::TEvReadSet::TPtr &ev, const TActorContext &ctx) {
    ui64 sender = ev->Get()->Record.GetTabletSource();
    ui64 dest = ev->Get()->Record.GetTabletDest();
    ui64 producer = ev->Get()->Record.GetTabletProducer();
    ui64 txId = ev->Get()->Record.GetTxId();
    LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "Receive RS at %" PRIu64 " source %" PRIu64 " dest %" PRIu64 " producer %" PRIu64 " txId %" PRIu64,
              TabletID(), sender, dest, producer, txId);
    IncCounter(COUNTER_READSET_RECEIVED_COUNT);
    IncCounter(COUNTER_READSET_RECEIVED_SIZE, ev->Get()->Record.GetReadSet().size());
    Execute(new TTxReadSet(this, ev), ctx);
}

void TDataShard::Handle(TEvTxProcessing::TEvReadSetAck::TPtr &ev, const TActorContext &ctx) {
    OutReadSets.SaveAck(ctx, ev->Release());

    // progress one more Tx to force delayed schema operations
    if (Pipeline.HasSchemaOperation() && OutReadSets.Empty()) {
        // TODO: wait for empty OutRS in a separate unit?
        Pipeline.AddCandidateUnit(EExecutionUnitKind::PlanQueue);
        PlanQueue.Progress(ctx);
    }

    CheckStateChange(ctx);
}

void TDataShard::Handle(TEvPrivate::TEvProgressTransaction::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    IncCounter(COUNTER_TX_PROGRESS_EV);
    ExecuteProgressTx(ctx);
}

void TDataShard::Handle(TEvPrivate::TEvDelayedProposeTransaction::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    IncCounter(COUNTER_PROPOSE_QUEUE_EV);

    if (ProposeQueue) {
        auto item = ProposeQueue.Dequeue();
        UpdateProposeQueueSize();

        TDuration latency = TAppData::TimeProvider->Now() - item.ReceivedAt;
        IncCounter(COUNTER_PROPOSE_QUEUE_LATENCY, latency);

        if (!item.Cancelled) {
            // N.B. we don't call ProposeQueue.Reset(), tx will Ack() on its first Execute()

            switch (item.Event->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    auto event = IEventHandle::Downcast<TEvDataShard::TEvProposeTransaction>(std::move(item.Event));
                    NWilson::TSpan datashardTransactionSpan(TWilsonTablet::TabletTopLevel, std::move(event->TraceId), "Datashard.Transaction", NWilson::EFlags::AUTO_END);
                    if (datashardTransactionSpan) {
                        datashardTransactionSpan.Attribute("Shard", std::to_string(TabletID()));
                    }
                    
                    Execute(new TTxProposeTransactionBase(this, std::move(event), item.ReceivedAt, item.TieBreakerIndex, /* delayed */ true, std::move(datashardTransactionSpan)), ctx);
                    return;
                }
                case NEvents::TDataEvents::TEvWrite::EventType: {
                    auto event = IEventHandle::Downcast<NEvents::TDataEvents::TEvWrite>(std::move(item.Event));
                    NWilson::TSpan datashardTransactionSpan(TWilsonTablet::TabletTopLevel, std::move(event->TraceId), "Datashard.WriteTransaction", NWilson::EFlags::AUTO_END);
                    if (datashardTransactionSpan) {
                        datashardTransactionSpan.Attribute("Shard", std::to_string(TabletID()));
                    }

                    Execute(new TTxWrite(this, std::move(event), item.ReceivedAt, item.TieBreakerIndex, /* delayed */ true, std::move(datashardTransactionSpan)), ctx);
                    return;
                }
                default:
                    Y_FAIL_S("Unexpected event type " << item.Event->GetTypeRewrite());
            }
        }

        TActorId target = item.Event->Sender;
        ui64 cookie = item.Event->Cookie;
        switch (item.Event->GetTypeRewrite()) {
            case TEvDataShard::TEvProposeTransaction::EventType: {
                auto* msg = item.Event->Get<TEvDataShard::TEvProposeTransaction>();
                auto kind = msg->GetTxKind();
                auto txId = msg->GetTxId();
                auto result = new TEvDataShard::TEvProposeTransactionResult(
                    kind, TabletID(), txId,
                    NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED);
                ctx.Send(target, result, 0, cookie);
                break;
            }
            case NEvents::TDataEvents::TEvWrite::EventType: {
                auto* msg = item.Event->Get<NEvents::TDataEvents::TEvWrite>();
                auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), msg->GetTxId(), NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Canceled");
                ctx.Send(target, result.release(), 0, cookie);
                break;
            }
            default:
                Y_FAIL_S("Unexpected event type " << item.Event->GetTypeRewrite());
        }

        
    }

    // N.B. Ack directly since we didn't start any delayed transactions
    ProposeQueue.Ack(ctx);
}

void TDataShard::Handle(TEvPrivate::TEvProgressResendReadSet::TPtr &ev, const TActorContext &ctx) {
    ResendReadSetQueue.Reset(ctx);
    Execute(new TTxProgressResendRS(this, ev->Get()->Seqno), ctx);
}

void TDataShard::Handle(TEvPrivate::TEvRegisterScanActor::TPtr &ev, const TActorContext &ctx) {
    ui64 txId = ev->Get()->TxId;
    auto op = Pipeline.FindOp(txId);

    if (!op) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                   "Cannot find op " << txId << " to register scan actor");
        return;
    }

    if (!op->IsReadTable()) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                   "Cannot register scan actor for op " << txId
                   << " of kind " << op->GetKind());
        return;
    }

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    tx->SetScanActor(ev->Sender);
}

void TDataShard::Handle(TEvPrivate::TEvScanStats::TPtr& ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);

    TabletCounters->Cumulative()[COUNTER_SCANNED_ROWS].Increment(ev->Get()->Rows);
    TabletCounters->Cumulative()[COUNTER_SCANNED_BYTES].Increment(ev->Get()->Bytes);
}

void TDataShard::Handle(TEvPrivate::TEvPersistScanState::TPtr& ev, const TActorContext &ctx) {
    TabletCounters->Cumulative()[COUNTER_SCANNED_ROWS].Increment(ev->Get()->Rows);
    TabletCounters->Cumulative()[COUNTER_SCANNED_BYTES].Increment(ev->Get()->Bytes);
    Execute(new TTxStoreScanState(this, ev), ctx);
}

void TDataShard::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
    Y_ABORT_UNLESS(ev->Get()->Leader, "Unexpectedly connected to follower of tablet %" PRIu64, ev->Get()->TabletId);

    if (ev->Get()->ClientId == SchemeShardPipe) {
        if (!TransQueue.HasNotAckedSchemaTx()) {
            LOG_ERROR(ctx, NKikimrServices::TX_DATASHARD,
                "Datashard's schemeshard pipe connected while no messages to sent at %" PRIu64, TabletID());
        }
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            SchemeShardPipe = TActorId();
            NotifySchemeshard(ctx);
        }
        return;
    }

    if (ev->Get()->ClientId == StateReportPipe) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            StateReportPipe = TActorId();
            ReportState(ctx, State);
        }
        return;
    }

    if (ev->Get()->ClientId == DbStatsReportPipe) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            DbStatsReportPipe = TActorId();
        }
        return;
    }

    if (ev->Get()->ClientId == TableResolvePipe) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TableResolvePipe = TActorId();
            ResolveTablePath(ctx);
        }
        return;
    }

    if (LoanReturnTracker.Has(ev->Get()->TabletId, ev->Get()->ClientId)) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            if (!ev->Get()->Dead) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                          "Resending loan returns from " << TabletID() << " to " << ev->Get()->TabletId);
                LoanReturnTracker.ResendLoans(ev->Get()->TabletId, ctx);
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                          "Auto-Acking loan returns to dead " << ev->Get()->TabletId << " from " << TabletID());
                LoanReturnTracker.AutoAckLoans(ev->Get()->TabletId, ctx);
            }
        }
        return;
    }

    // Resend split-related messages in needed
    if (SplitSrcSnapshotSender.Has(ev->Get()->TabletId, ev->Get()->ClientId)) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            SplitSrcSnapshotSender.DoSend(ev->Get()->TabletId, ctx);
        }
        return;
    }

    if (ChangeSenderActivator.Has(ev->Get()->TabletId, ev->Get()->ClientId)) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            if (!ev->Get()->Dead) {
                ChangeSenderActivator.DoSend(ev->Get()->TabletId, ctx);
            } else {
                ChangeSenderActivator.AutoAck(ev->Get()->TabletId, ctx);
            }
        }
        return;
    }

    if (!PipeClientCache->OnConnect(ev)) {
        if (ev->Get()->Dead) {
            AckRSToDeletedTablet(ev->Get()->TabletId, ctx);
        } else {
            LOG_NOTICE(ctx, NKikimrServices::TX_DATASHARD, "Failed to connect to tablet %" PRIu64 " from tablet %" PRIu64, ev->Get()->TabletId, TabletID());
            RestartPipeRS(ev->Get()->TabletId, ctx);
        }
    } else {
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "Connected to tablet %" PRIu64 " from tablet %" PRIu64, ev->Get()->TabletId, TabletID());
    }
}

void TDataShard::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
    if (ev->Get()->ClientId == SchemeShardPipe) {
        if (!TransQueue.HasNotAckedSchemaTx()) {
            LOG_ERROR(ctx, NKikimrServices::TX_DATASHARD,
                "Datashard's schemeshard pipe destroyed while no messages to sent at %" PRIu64, TabletID());
        }
        SchemeShardPipe = TActorId();
        NotifySchemeshard(ctx);
        return;
    }

    if (ev->Get()->ClientId == StateReportPipe) {
        StateReportPipe = TActorId();
        ReportState(ctx, State);
        return;
    }

    if (ev->Get()->ClientId == DbStatsReportPipe) {
        DbStatsReportPipe = TActorId();
        return;
    }

    // Resend loan-related messages in needed
    if (LoanReturnTracker.Has(ev->Get()->TabletId, ev->Get()->ClientId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Resending loan returns from " << TabletID() << " to " << ev->Get()->TabletId);
        LoanReturnTracker.ResendLoans(ev->Get()->TabletId, ctx);
        return;
    }

    // Resend split-related messages in needed
    if (SplitSrcSnapshotSender.Has(ev->Get()->TabletId, ev->Get()->ClientId)) {
        SplitSrcSnapshotSender.DoSend(ev->Get()->TabletId, ctx);
        return;
    }

    if (ChangeSenderActivator.Has(ev->Get()->TabletId, ev->Get()->ClientId)) {
        ChangeSenderActivator.DoSend(ev->Get()->TabletId, ctx);
        return;
    }

    LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "Client pipe to tablet %" PRIu64 " from %" PRIu64 " is reset", ev->Get()->TabletId, TabletID());
    PipeClientCache->OnDisconnect(ev);
    RestartPipeRS(ev->Get()->TabletId, ctx);
}

void TDataShard::RestartPipeRS(ui64 tabletId, const TActorContext& ctx) {
    for (auto seqno : ResendReadSetPipeTracker.FindTx(tabletId)) {
        if (seqno == Max<ui64>()) {
            OutReadSets.ResendExpectations(tabletId, ctx);
            continue;
        }

        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "Pipe reset to tablet %" PRIu64 " caused resend of readset %" PRIu64
            " at tablet %" PRIu64, tabletId, seqno, TabletID());

        ResendReadSetQueue.Progress(seqno, ctx);
    }
}

void TDataShard::AckRSToDeletedTablet(ui64 tabletId, const TActorContext& ctx) {
    bool detachExpectations = false;
    for (auto seqno : ResendReadSetPipeTracker.FindTx(tabletId)) {
        if (seqno == Max<ui64>()) {
            AbortExpectationsFromDeletedTablet(tabletId, OutReadSets.RemoveExpectations(tabletId));
            detachExpectations = true;
            continue;
        }

        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "Pipe reset to dead tablet %" PRIu64 " caused ack of readset %" PRIu64
            " at tablet %" PRIu64, tabletId, seqno, TabletID());

        OutReadSets.AckForDeletedDestination(tabletId, seqno, ctx);

        // progress one more Tx to force delayed schema operations
        if (Pipeline.HasSchemaOperation() && OutReadSets.Empty()) {
            // TODO: wait for empty OutRS in a separate unit?
            Pipeline.AddCandidateUnit(EExecutionUnitKind::PlanQueue);
            PlanQueue.Progress(ctx);
        }
    }

    if (detachExpectations) {
        ResendReadSetPipeTracker.DetachTablet(Max<ui64>(), tabletId, 0, ctx);
    }

    CheckStateChange(ctx);
}

void TDataShard::AbortExpectationsFromDeletedTablet(ui64 tabletId, THashMap<ui64, ui64>&& expectations) {
    for (auto& pr : expectations) {
        auto* info = VolatileTxManager.FindByTxId(pr.first);
        if (info && info->State == EVolatileTxState::Waiting && info->Participants.contains(tabletId)) {
            VolatileTxManager.AbortWaitingTransaction(info);
        }
    }
}

void TDataShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Server connected at "
        << (Executor()->GetStats().IsFollower ? "follower " : "leader ")
        << "tablet# " << ev->Get()->TabletId
        << ", clientId# " << ev->Get()->ClientId
        << ", serverId# " << ev->Get()->ServerId
        << ", sessionId# " << ev->InterconnectSession);

    auto res = PipeServers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(ev->Get()->ServerId),
        std::forward_as_tuple());
    Y_VERIFY_DEBUG_S(res.second,
        "Unexpected TEvServerConnected for " << ev->Get()->ServerId);

    res.first->second.InterconnectSession = ev->Get()->InterconnectSession;
}

void TDataShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Server disconnected at "
        << (Executor()->GetStats().IsFollower ? "follower " : "leader ")
        << "tablet# " << ev->Get()->TabletId
        << ", clientId# " << ev->Get()->ClientId
        << ", serverId# " << ev->Get()->ServerId
        << ", sessionId# " << ev->InterconnectSession);

    auto it = PipeServers.find(ev->Get()->ServerId);
    Y_VERIFY_DEBUG_S(it != PipeServers.end(),
        "Unexpected TEvServerDisconnected for " << ev->Get()->ServerId);

    DiscardOverloadSubscribers(it->second);

    PipeServers.erase(it);
}

void TDataShard::Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Got TEvMediatorTimecast::TEvRegisterTabletResult at " << TabletID()
                << " time " << ev->Get()->Entry->Get(TabletID()));
    Y_ABORT_UNLESS(ev->Get()->TabletId == TabletID());
    MediatorTimeCastEntry = ev->Get()->Entry;
    Y_ABORT_UNLESS(MediatorTimeCastEntry);

    SendAfterMediatorStepActivate(MediatorTimeCastEntry->Get(TabletID()), ctx);

    Pipeline.ActivateWaitingTxOps(ctx);

    CheckMediatorStateRestored();
}

void TDataShard::Handle(TEvMediatorTimecast::TEvSubscribeReadStepResult::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Got TEvMediatorTimecast::TEvSubscribeReadStepResult at " << TabletID()
                << " coordinator " << msg->CoordinatorId
                << " last step " << msg->LastReadStep
                << " next step " << msg->NextReadStep);
    auto it = CoordinatorSubscriptionById.find(msg->CoordinatorId);
    Y_VERIFY_S(it != CoordinatorSubscriptionById.end(),
        "Unexpected TEvSubscribeReadStepResult for coordinator " << msg->CoordinatorId);
    size_t index = it->second;
    auto& subscription = CoordinatorSubscriptions.at(index);
    subscription.ReadStep = msg->ReadStep;
    CoordinatorPrevReadStepMin = Max(CoordinatorPrevReadStepMin, msg->LastReadStep);
    CoordinatorPrevReadStepMax = Min(CoordinatorPrevReadStepMax, msg->NextReadStep);
    --CoordinatorSubscriptionsPending;
    CheckMediatorStateRestored();
}

void TDataShard::Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    Y_ABORT_UNLESS(msg->TabletId == TabletID());

    Y_ABORT_UNLESS(MediatorTimeCastEntry);
    ui64 step = MediatorTimeCastEntry->Get(TabletID());
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Notified by mediator time cast with PlanStep# " << step << " at tablet " << TabletID());

    for (auto it = MediatorTimeCastWaitingSteps.begin(); it != MediatorTimeCastWaitingSteps.end() && *it <= step;)
        it = MediatorTimeCastWaitingSteps.erase(it);

    SendAfterMediatorStepActivate(step, ctx);

    Pipeline.ActivateWaitingTxOps(ctx);

    CheckMediatorStateRestored();
}

void TDataShard::Handle(TEvPrivate::TEvMediatorRestoreBackup::TPtr&, const TActorContext&) {
    Y_ABORT("This code path was always no-op and no longer used");
}

bool TDataShard::WaitPlanStep(ui64 step) {
    if (step <= Pipeline.GetLastPlannedTx().Step)
        return false;

    if (step <= SnapshotManager.GetCompleteEdge().Step)
        return false;

    if (MediatorTimeCastEntry && step <= MediatorTimeCastEntry->Get(TabletID()))
        return false;

    if (!RegistrationSended)
        return false;

    if (MediatorTimeCastWaitingSteps.empty() || step < *MediatorTimeCastWaitingSteps.begin()) {
        MediatorTimeCastWaitingSteps.insert(step);
        Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), step));
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Waiting for PlanStep# " << step << " from mediator time cast");
        return true;
    }

    return false;
}

void TDataShard::WaitPredictedPlanStep(ui64 step) {
    if (!MediatorTimeCastEntry) {
        return;
    }

    if (step <= MediatorTimeCastEntry->Get(TabletID())) {
        // This step is ready, schedule a transaction plan
        SchedulePlanPredictedTxs();
        return;
    }

    if (MediatorTimeCastWaitingSteps.empty() || step < *MediatorTimeCastWaitingSteps.begin()) {
        MediatorTimeCastWaitingSteps.insert(step);
        Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), step));
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Waiting for PlanStep# " << step << " from mediator time cast");
    }
}

bool TDataShard::CheckTxNeedWait() const {
    if (MvccSwitchState == TSwitchState::SWITCHING) {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "New transaction needs to wait because of mvcc state switching");
        return true;
    }

    return false;
}

bool TDataShard::CheckTxNeedWait(const TEvDataShard::TEvProposeTransaction::TPtr& ev) const {
    if (CheckTxNeedWait()) {
        return true;
    }

    auto* msg = ev->Get();
    auto& rec = msg->Record;
    if (rec.HasMvccSnapshot()) {
        TRowVersion rowVersion(rec.GetMvccSnapshot().GetStep(), rec.GetMvccSnapshot().GetTxId());
        TRowVersion unreadableEdge = Pipeline.GetUnreadableEdge();
        if (rowVersion >= unreadableEdge) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "New transaction reads from " << rowVersion << " which is not before unreadable edge " << unreadableEdge);
            LWTRACK(ProposeTransactionWaitSnapshot, msg->Orbit, rowVersion.Step, rowVersion.TxId);
            return true;
        }
    }

    return false;
}

bool TDataShard::CheckChangesQueueOverflow(ui64 cookie) const {
    const auto* appData = AppData();
    const auto sizeLimit = appData->DataShardConfig.GetChangesQueueItemsLimit();
    const auto bytesLimit = appData->DataShardConfig.GetChangesQueueBytesLimit();

    ui32 reserved = ChangeQueueReservedCapacity;
    if (auto it = ChangeQueueReservations.find(cookie); it != ChangeQueueReservations.end()) {
        reserved -= it->second;
    }

    return (ChangesQueue.size() + reserved) >= sizeLimit || ChangesQueueBytes >= bytesLimit;
}

void TDataShard::CheckChangesQueueNoOverflow(ui64 cookie) {
    if (OverloadSubscribersByReason[RejectReasonIndex(ERejectReason::ChangesQueueOverflow)]) {
        const auto* appData = AppData();
        const auto sizeLimit = appData->DataShardConfig.GetChangesQueueItemsLimit();
        const auto bytesLimit = appData->DataShardConfig.GetChangesQueueBytesLimit();

        ui32 reserved = ChangeQueueReservedCapacity;
        if (auto it = ChangeQueueReservations.find(cookie); it != ChangeQueueReservations.end()) {
            reserved -= it->second;
        }

        if ((ChangesQueue.size() + reserved) < sizeLimit && ChangesQueueBytes < bytesLimit) {
            NotifyOverloadSubscribers(ERejectReason::ChangesQueueOverflow);
        }
    }
}

void TDataShard::DoPeriodicTasks(const TActorContext &ctx) {
    UpdateLagCounters(ctx);
    UpdateChangeExchangeLag(ctx.Now());
    UpdateTableStats(ctx);
    SendPeriodicTableStats(ctx);
    CollectCpuUsage(ctx);

    if (CurrentKeySampler == EnabledKeySampler && ctx.Now() > StopKeyAccessSamplingAt) {
        CurrentKeySampler = DisabledKeySampler;
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "Stoped key access sampling at datashard: " << TabletID());
    }

    if (!PeriodicWakeupPending) {
        PeriodicWakeupPending = true;
        ctx.Schedule(TDuration::Seconds(5), new TEvPrivate::TEvPeriodicWakeup());
    }
}

void TDataShard::DoPeriodicTasks(TEvPrivate::TEvPeriodicWakeup::TPtr&, const TActorContext &ctx) {
    Y_ABORT_UNLESS(PeriodicWakeupPending, "Unexpected TEvPeriodicWakeup message");
    PeriodicWakeupPending = false;
    DoPeriodicTasks(ctx);
}

void TDataShard::UpdateLagCounters(const TActorContext &ctx) {
    TDuration dataTxCompleteLag = GetDataTxCompleteLag();
    TabletCounters->Simple()[COUNTER_TX_COMPLETE_LAG].Set(dataTxCompleteLag.MilliSeconds());
    if (dataTxCompleteLag > TDuration::Minutes(5)) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                   "Tx completion lag (" << dataTxCompleteLag << ") is > 5 min on tablet "
                   << TabletID());
    }

    TDuration scanTxCompleteLag = GetScanTxCompleteLag();
    TabletCounters->Simple()[COUNTER_SCAN_TX_COMPLETE_LAG].Set(scanTxCompleteLag.MilliSeconds());
    if (scanTxCompleteLag > TDuration::Hours(1)) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                   "Scan completion lag (" << scanTxCompleteLag << ") is > 1 hour on tablet "
                   << TabletID());
    }
}

void TDataShard::FillSplitTrajectory(ui64 origin, NKikimrTx::TBalanceTrackList& tracks) {
    Y_UNUSED(origin);
    Y_UNUSED(tracks);
}

THolder<TEvTxProcessing::TEvReadSet>
TDataShard::PrepareReadSet(ui64 step, ui64 txId, ui64 source, ui64 target,
                                  const TString& body, ui64 seqno)
{
    auto ev = MakeHolder<TEvTxProcessing::TEvReadSet>(step, txId, source, target, TabletID(), body, seqno);
    if (source != TabletID())
        FillSplitTrajectory(source, *ev->Record.MutableBalanceTrackList());
    return ev;
}

THolder<TEvTxProcessing::TEvReadSet>
TDataShard::PrepareReadSetExpectation(ui64 step, ui64 txId, ui64 source, ui64 target)
{
    // We want to notify the target that we expect a readset, there's no data and no ack needed so no seqno
    auto ev = MakeHolder<TEvTxProcessing::TEvReadSet>(step, txId, source, target, TabletID());
    ev->Record.SetFlags(
        NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET |
        NKikimrTx::TEvReadSet::FLAG_NO_DATA |
        NKikimrTx::TEvReadSet::FLAG_NO_ACK);
    if (source != TabletID())
        FillSplitTrajectory(source, *ev->Record.MutableBalanceTrackList());
    return ev;
}

void TDataShard::SendReadSet(
        const TActorContext& ctx,
        THolder<TEvTxProcessing::TEvReadSet>&& rs)
{
    ui64 txId = rs->Record.GetTxId();
    ui64 source = rs->Record.GetTabletSource();
    ui64 target = rs->Record.GetTabletDest();

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Send RS at " << TabletID() << " from " << source << " to " << target << " txId " << txId);

    IncCounter(COUNTER_READSET_SENT_COUNT);
    IncCounter(COUNTER_READSET_SENT_SIZE, rs->Record.GetReadSet().size());

    PipeClientCache->Send(ctx, target, rs.Release());
}

void TDataShard::SendReadSet(const TActorContext& ctx, ui64 step,
                                    ui64 txId, ui64 source, ui64 target,
                                    const TString& body, ui64 seqno)
{
    auto ev = PrepareReadSet(step, txId, source, target, body, seqno);
    SendReadSet(ctx, std::move(ev));
}

bool TDataShard::AddExpectation(ui64 target, ui64 step, ui64 txId) {
    bool hadExpectations = OutReadSets.HasExpectations(target);
    bool added = OutReadSets.AddExpectation(target, step, txId);
    if (!hadExpectations) {
        ResendReadSetPipeTracker.AttachTablet(Max<ui64>(), target);
    }
    return added;
}

bool TDataShard::RemoveExpectation(ui64 target, ui64 txId) {
    bool removed = OutReadSets.RemoveExpectation(target, txId);
    if (removed && !OutReadSets.HasExpectations(target)) {
        auto ctx = ActorContext();
        ResendReadSetPipeTracker.DetachTablet(Max<ui64>(), target, 0, ctx);
    }

    // progress one more tx to force delayed schema operations
    if (removed && OutReadSets.Empty() && Pipeline.HasSchemaOperation()) {
        // TODO: wait for empty OutRS in a separate unit?
        auto ctx = ActorContext();
        Pipeline.AddCandidateUnit(EExecutionUnitKind::PlanQueue);
        PlanQueue.Progress(ctx);
    }

    return removed;
}

void TDataShard::SendReadSetExpectation(const TActorContext& ctx, ui64 step, ui64 txId,
                                        ui64 source, ui64 target)
{
    auto ev = PrepareReadSetExpectation(step, txId, source, target);
    PipeClientCache->Send(ctx, target, ev.Release());
}

std::unique_ptr<IEventHandle> TDataShard::GenerateReadSetNoData(const TActorId& recipient, ui64 step, ui64 txId, ui64 source, ui64 target)
{
    auto msg = std::make_unique<TEvTxProcessing::TEvReadSet>(step, txId, source, target, TabletID());
    msg->Record.SetFlags(
        NKikimrTx::TEvReadSet::FLAG_NO_DATA |
        NKikimrTx::TEvReadSet::FLAG_NO_ACK);
    if (source != TabletID()) {
        FillSplitTrajectory(source, *msg->Record.MutableBalanceTrackList());
    }

    return std::make_unique<IEventHandle>(recipient, SelfId(), msg.release());
}

void TDataShard::SendReadSetNoData(const TActorContext& ctx, const TActorId& recipient, ui64 step, ui64 txId, ui64 source, ui64 target)
{
    Y_UNUSED(ctx);
    auto ev = GenerateReadSetNoData(recipient, step, txId, source, target);

    struct TSendState : public TThrRefBase {
        std::unique_ptr<IEventHandle> Event;

        TSendState(std::unique_ptr<IEventHandle>&& event)
            : Event(std::move(event))
        { }
    };

    // FIXME: we can probably avoid lease confirmation here
    Executor()->ConfirmReadOnlyLease(
        [state = MakeIntrusive<TSendState>(std::move(ev))] {
            TActivationContext::Send(std::move(state->Event));
        });
}

bool TDataShard::ProcessReadSetExpectation(TEvTxProcessing::TEvReadSet::TPtr& ev) {
    const auto& record = ev->Get()->Record;

    // Check if we already have a pending readset from dest to source
    TReadSetKey rsKey(record.GetTxId(), TabletID(), record.GetTabletDest(), record.GetTabletSource());
    if (OutReadSets.Has(rsKey)) {
        return true;
    }

    if (IsStateActive()) {
        // When we have a pending op, remember that readset from dest to source is expected
        if (auto op = Pipeline.FindOp(record.GetTxId())) {
            auto key = std::make_pair(record.GetTabletDest(), record.GetTabletSource());
            op->ExpectedReadSets()[key].push_back(ev->Sender);
            return true;
        }
    }

    // In all other cases we want to reply with no data
    return false;
}

void TDataShard::SendReadSets(const TActorContext& ctx,
                                     TVector<THolder<TEvTxProcessing::TEvReadSet>> &&readsets)
{
    TPendingPipeTrackerCommands pendingPipeTrackerCommands;

    for (auto &rs : readsets) {
        ui64 target = rs->Record.GetTabletDest();
        ui64 seqno = rs->Record.GetSeqno();

        pendingPipeTrackerCommands.AttachTablet(seqno, target);
        SendReadSet(ctx, std::move(rs));
    }

    pendingPipeTrackerCommands.Apply(ResendReadSetPipeTracker, ctx);
    readsets.clear();
}

void TDataShard::ResendReadSet(const TActorContext& ctx, ui64 step, ui64 txId, ui64 source, ui64 target,
                                      const TString& body, ui64 seqNo)
{
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
               "Resend RS at " << TabletID() << " from " << source << " to " << target << " txId " << txId);

    SendReadSet(ctx, step, txId, source, target, body, seqNo);
    ResendReadSetPipeTracker.AttachTablet(seqNo, target);
}

void TDataShard::UpdateLastSchemeOpSeqNo(const TSchemeOpSeqNo &newSeqNo,
                                                TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    if (LastSchemeOpSeqNo < newSeqNo) {
        LastSchemeOpSeqNo = newSeqNo;
        PersistSys(db, Schema::Sys_LastSchemeShardGeneration, LastSchemeOpSeqNo.Generation);
        PersistSys(db, Schema::Sys_LastSchemeShardRound, LastSchemeOpSeqNo.Round);
    }
}

void TDataShard::ResetLastSchemeOpSeqNo(TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    LastSchemeOpSeqNo = TSchemeOpSeqNo();
    PersistSys(db, Schema::Sys_LastSchemeShardGeneration, LastSchemeOpSeqNo.Generation);
    PersistSys(db, Schema::Sys_LastSchemeShardRound, LastSchemeOpSeqNo.Round);
}

void TDataShard::PersistProcessingParams(const NKikimrSubDomains::TProcessingParams &params,
                                                NTabletFlatExecutor::TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    ProcessingParams.reset(new NKikimrSubDomains::TProcessingParams());
    ProcessingParams->CopyFrom(params);
    PersistSys(db, TDataShard::Schema::Sys_SubDomainInfo,
               ProcessingParams->SerializeAsString());
}

void TDataShard::PersistCurrentSchemeShardId(ui64 id,
                                                   NTabletFlatExecutor::TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    CurrentSchemeShardId = id;
    PersistSys(db, TDataShard::Schema::Sys_CurrentSchemeShardId, id);
}

void TDataShard::PersistSubDomainPathId(ui64 ownerId, ui64 localPathId,
                                               NTabletFlatExecutor::TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    SubDomainPathId.emplace(ownerId, localPathId);
    PersistSys(db, Schema::Sys_SubDomainOwnerId, ownerId);
    PersistSys(db, Schema::Sys_SubDomainLocalPathId, localPathId);
}

void TDataShard::PersistOwnerPathId(ui64 id,
                                           NTabletFlatExecutor::TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    PathOwnerId = id;
    PersistSys(db, TDataShard::Schema::Sys_PathOwnerId, id);
}

void TDataShard::ResolveTablePath(const TActorContext &ctx)
{
    if (State != TShardState::Ready)
        return;

    for (auto& [pathId, info] : TableInfos) {
        TString reason = "empty path";

        if (info->Path) {
            NKikimrSchemeOp::TTableDescription desc;
            info->GetSchema(desc);

            if (desc.GetName() == ExtractBase(desc.GetPath())) {
                continue;
            }

            reason = "buggy path";
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Resolve path at " << TabletID()
            << ": reason# " << reason);

        if (!TableResolvePipe) {
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = SchemeShardPipeRetryPolicy;
            TableResolvePipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, CurrentSchemeShardId, clientConfig));
        }

        auto event = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(PathOwnerId, pathId);
        event->Record.MutableOptions()->SetReturnPartitioningInfo(false);
        event->Record.MutableOptions()->SetReturnPartitionConfig(false);
        event->Record.MutableOptions()->SetReturnChildren(false);
        NTabletPipe::SendData(ctx, TableResolvePipe, event.Release());
    }
}

void TDataShard::SerializeHistogram(const TUserTable &tinfo,
                                           const NTable::THistogram &histogram,
                                           NKikimrTxDataShard::TEvGetDataHistogramResponse::THistogram &hist)
{
    for (auto &item : histogram) {
        auto &rec = *hist.AddItems();
        rec.SetValue(item.Value);

        TSerializedCellVec key(item.EndKey);
        for (ui32 ki = 0; ki < tinfo.KeyColumnIds.size(); ++ki) {
            DbgPrintValue(*rec.AddKeyValues(), key.GetCells()[ki], tinfo.KeyColumnTypes[ki]);
        }
    }
}

void TDataShard::SerializeKeySample(const TUserTable &tinfo,
                                           const NTable::TKeyAccessSample &keySample,
                                           NKikimrTxDataShard::TEvGetDataHistogramResponse::THistogram &hist)
{
    THashMap<TString, ui64> accessCounts;

    for (auto &key : keySample.GetSample()) {
        accessCounts[key.first]++;
        // TODO: count access kinds separately
    }

    for (auto &item : accessCounts) {
        auto &rec = *hist.AddItems();
        rec.SetValue(item.second);

        TSerializedCellVec key(item.first);
        for (ui32 ki = 0; ki < tinfo.KeyColumnIds.size() && ki < key.GetCells().size(); ++ki) {
            DbgPrintValue(*rec.AddKeyValues(), key.GetCells()[ki], tinfo.KeyColumnTypes[ki]);
        }
    }
    Sort(hist.MutableItems()->begin(), hist.MutableItems()->end(),
         [] (const auto& a, const auto& b) { return a.GetValue() > b.GetValue(); });
}


void TDataShard::Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr ev, const TActorContext &ctx) {
    const auto &rec = ev->Get()->GetRecord();

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Got scheme resolve result at " << TabletID() << ": "
                << rec.ShortDebugString());

    ui64 pathId = rec.GetPathId();
    if (!TableInfos.contains(pathId)) {
        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Shard " << TabletID() << " got describe result for unknown table "
                    << pathId);
        return;
    }

    if (!rec.GetPath()) {
        LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD,
                   "Shard " << TabletID() << " couldn't get path for table "
                   << pathId << " with status " << rec.GetStatus());
        return;
    }
    Execute(new TTxStoreTablePath(this, pathId, rec.GetPath()), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvGetInfoRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    Execute(CreateTxGetInfo(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvListOperationsRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    Execute(CreateTxListOperations(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvGetOperationRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    Execute(CreateTxGetOperation(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvGetDataHistogramRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    auto *response = new TEvDataShard::TEvGetDataHistogramResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
    const auto& rec = ev->Get()->Record;

    if (rec.GetCollectKeySampleMs() > 0) {
        EnableKeyAccessSampling(ctx,
            AppData(ctx)->TimeProvider->Now() + TDuration::MilliSeconds(rec.GetCollectKeySampleMs()));
    }

    if (rec.GetActualData()) {
        if (CurrentKeySampler == DisabledKeySampler) {
            // datashard stores expired stats
            ctx.Send(ev->Sender, response);
            return;
        }
    }

    for (const auto &pr : TableInfos) {
        const auto &tinfo = *pr.second;
        const NTable::TStats &stats = tinfo.Stats.DataStats;

        auto &hist = *response->Record.AddTableHistograms();
        hist.SetTableName(pr.second->Name);
        for (ui32 ki : tinfo.KeyColumnIds)
            hist.AddKeyNames(tinfo.Columns.FindPtr(ki)->Name);
        SerializeHistogram(tinfo, stats.DataSizeHistogram, *hist.MutableSizeHistogram());
        SerializeHistogram(tinfo, stats.RowCountHistogram, *hist.MutableCountHistogram());
        SerializeKeySample(tinfo, tinfo.Stats.AccessStats, *hist.MutableKeyAccessSample());
    }

    ctx.Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvGetReadTableSinkStateRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    ui64 txId = ev->Get()->Record.GetTxId();
    auto op = Pipeline.FindOp(txId);
    if (!op) {
        auto *response = new TEvDataShard::TEvGetReadTableSinkStateResponse;
        SetStatusError(response->Record, Ydb::StatusIds::NOT_FOUND,
                       TStringBuilder() << "Cannot find operation "
                       << txId << " on shard " << TabletID());
        ctx.Send(ev->Sender, response);
        return;
    }

    if (op->GetKind() != EOperationKind::ReadTable) {
        auto *response = new TEvDataShard::TEvGetReadTableSinkStateResponse;
        SetStatusError(response->Record, Ydb::StatusIds::BAD_REQUEST,
                       TStringBuilder() << "Cannot get sink state for tx of kind "
                       << op->GetKind());
        ctx.Send(ev->Sender, response);
        return;
    }

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());
    ctx.Send(ev->Forward(tx->GetStreamSink()));
}

void TDataShard::Handle(TEvDataShard::TEvGetReadTableScanStateRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    ui64 txId = ev->Get()->Record.GetTxId();
    auto op = Pipeline.FindOp(txId);
    if (!op) {
        auto *response = new TEvDataShard::TEvGetReadTableScanStateResponse;
        SetStatusError(response->Record, Ydb::StatusIds::NOT_FOUND,
                       TStringBuilder() << "Cannot find operation "
                       << txId << " on shard " << TabletID());
        ctx.Send(ev->Sender, response);
        return;
    }

    if (op->GetKind() != EOperationKind::ReadTable) {
        auto *response = new TEvDataShard::TEvGetReadTableScanStateResponse;
        SetStatusError(response->Record, Ydb::StatusIds::BAD_REQUEST,
                       TStringBuilder() << "Cannot get scan state for tx of kind "
                       << op->GetKind());
        ctx.Send(ev->Sender, response);
        return;
    }

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());
    ctx.Send(ev->Forward(tx->GetStreamSink()));

    if (!tx->GetScanActor()) {
        auto *response = new TEvDataShard::TEvGetReadTableScanStateResponse;
        SetStatusError(response->Record, Ydb::StatusIds::GENERIC_ERROR,
                       TStringBuilder() << "Operation has no registered scan actor");
        ctx.Send(ev->Sender, response);
        return;
    }

    ctx.Send(ev->Forward(tx->GetScanActor()));
}

void TDataShard::Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    ui64 txId = ev->Get()->Record.GetTxId();
    auto op = Pipeline.FindOp(txId);
    if (!op) {
        auto *response = new TEvDataShard::TEvGetReadTableStreamStateResponse;
        SetStatusError(response->Record, Ydb::StatusIds::NOT_FOUND,
                       TStringBuilder() << "Cannot find operation "
                       << txId << " on shard " << TabletID());
        ctx.Send(ev->Sender, response);
        return;
    }

    if (op->GetKind() != EOperationKind::ReadTable) {
        auto *response = new TEvDataShard::TEvGetReadTableStreamStateResponse;
        SetStatusError(response->Record, Ydb::StatusIds::BAD_REQUEST,
                       TStringBuilder() << "Cannot get stream state for tx of kind "
                       << op->GetKind());
        ctx.Send(ev->Sender, response);
        return;
    }

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());
    ctx.Send(ev->Forward(tx->GetStreamSink()));
}

void TDataShard::Handle(TEvDataShard::TEvGetRSInfoRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    auto *response = new TEvDataShard::TEvGetRSInfoResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    for (auto &pr : OutReadSets.CurrentReadSets) {
        auto &rs = *response->Record.AddOutReadSets();
        rs.SetTxId(pr.second.TxId);
        rs.SetOrigin(pr.second.Origin);
        rs.SetSource(pr.second.From);
        rs.SetDestination(pr.second.To);
        rs.SetSeqNo(pr.first);
    }

    for (auto &p : OutReadSets.ReadSetAcks) {
        auto &rec = p->Record;
        auto &ack = *response->Record.AddOutRSAcks();
        ack.SetTxId(rec.GetTxId());
        ack.SetStep(rec.GetStep());
        ack.SetOrigin(rec.GetTabletConsumer());
        ack.SetSource(rec.GetTabletSource());
        ack.SetDestination(rec.GetTabletDest());
        ack.SetSeqNo(rec.GetSeqno());
    }

    for (auto &pr : Pipeline.GetDelayedAcks()) {
        for (auto &ack : pr.second) {
            auto *ev = ack->CastAsLocal<TEvTxProcessing::TEvReadSetAck>();
            if (ev) {
                auto &rec = ev->Record;
                auto &ack = *response->Record.AddDelayedRSAcks();
                ack.SetTxId(rec.GetTxId());
                ack.SetStep(rec.GetStep());
                ack.SetOrigin(rec.GetTabletConsumer());
                ack.SetSource(rec.GetTabletSource());
                ack.SetDestination(rec.GetTabletDest());
                ack.SetSeqNo(rec.GetSeqno());
            }
        }
    }

    ctx.Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvGetSlowOpProfilesRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    auto *response = new TEvDataShard::TEvGetSlowOpProfilesResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
    Pipeline.FillStoredExecutionProfiles(response->Record);
    ctx.Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvRefreshVolatileSnapshotRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxRefreshVolatileSnapshot(this, std::move(ev)), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvDiscardVolatileSnapshotRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDiscardVolatileSnapshot(this, std::move(ev)), ctx);
}

void TDataShard::Handle(TEvents::TEvUndelivered::TPtr &ev,
                               const TActorContext &ctx)
{
    auto op = Pipeline.FindOp(ev->Cookie);
    if (op) {
        op->AddInputEvent(ev.Release());
        Pipeline.AddCandidateOp(op);
        PlanQueue.Progress(ctx);
        return;
    }

    switch (ev->Get()->SourceType) {
        case TEvents::TEvSubscribe::EventType:
            ReadIteratorsOnNodeDisconnected(ev->Sender, ctx);
            break;
        default:
            ;
    }
}

void TDataShard::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev,
                               const TActorContext &ctx)
{
    const ui32 nodeId = ev->Get()->NodeId;

    LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
                 "Shard " << TabletID() << " disconnected from node " << nodeId);

    Pipeline.ProcessDisconnected(nodeId);
    PlanQueue.Progress(ctx);

    ReadIteratorsOnNodeDisconnected(ev->Sender, ctx);
}

void TDataShard::Handle(TEvDataShard::TEvMigrateSchemeShardRequest::TPtr& ev,
                               const TActorContext& ctx)
{
    Execute(new TTxMigrateSchemeShard(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvCancelBackup::TPtr& ev, const TActorContext& ctx)
{
    TOperation::TPtr op = Pipeline.FindOp(ev->Get()->Record.GetBackupTxId());
    if (op) {
        ForwardEventToOperation(ev, op, ctx);
    }
}

void TDataShard::Handle(TEvDataShard::TEvCancelRestore::TPtr& ev, const TActorContext& ctx)
{
    TOperation::TPtr op = Pipeline.FindOp(ev->Get()->Record.GetRestoreTxId());
    if (op) {
        ForwardEventToOperation(ev, op, ctx);
    }
}

void TDataShard::Handle(TEvDataShard::TEvGetS3Upload::TPtr& ev, const TActorContext& ctx)
{
    Execute(new TTxGetS3Upload(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvStoreS3UploadId::TPtr& ev, const TActorContext& ctx)
{
    Execute(new TTxStoreS3UploadId(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvChangeS3UploadStatus::TPtr& ev, const TActorContext& ctx)
{
    Execute(new TTxChangeS3UploadStatus(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvGetS3DownloadInfo::TPtr& ev, const TActorContext& ctx)
{
    Execute(new TTxGetS3DownloadInfo(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvStoreS3DownloadInfo::TPtr& ev, const TActorContext& ctx)
{
    Execute(new TTxStoreS3DownloadInfo(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev, const TActorContext& ctx)
{
    const float rejectProbabilty = Executor()->GetRejectProbability();
    if (rejectProbabilty > 0) {
        const float rnd = AppData(ctx)->RandomProvider->GenRandReal2();
        if (rnd < rejectProbabilty) {
            DelayedS3UploadRows.emplace_back().Reset(ev.Release());
            IncCounter(COUNTER_BULK_UPSERT_OVERLOADED);
            return;
        }
    }

    Execute(new TTxS3UploadRows(this, ev), ctx);
}

void TDataShard::ScanComplete(NTable::EAbort,
                                     TAutoPtr<IDestructable> prod,
                                     ui64 cookie,
                                     const TActorContext &ctx)
{
    if (auto* noTxScan = dynamic_cast<INoTxScan*>(prod.Get())) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Non-transactinal scan complete at "
                    << TabletID());

        noTxScan->OnFinished(this);
        prod.Destroy();
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "FullScan complete at " << TabletID());

        auto op = Pipeline.FindOp(cookie);
        if (op) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Found op"
                << ": cookie: " << cookie
                << ", at: "<< TabletID());

            if (op->IsWaitingForScan()) {
                op->SetScanResult(prod);
                Pipeline.AddCandidateOp(op);
            }
        } else {
            if (InFlightCondErase && InFlightCondErase.TxId == cookie) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Conditional erase complete"
                    << ": cookie: " << cookie
                    << ", at: "<< TabletID());

                InFlightCondErase.Clear();
            } else if (CdcStreamScanManager.Has(cookie)) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Cdc stream scan complete"
                    << ": cookie: " << cookie
                    << ", at: "<< TabletID());

                CdcStreamScanManager.Complete(cookie);
            } else if (!Pipeline.FinishStreamingTx(cookie)) {
                LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                            "Scan complete at " << TabletID() << " for unknown tx " << cookie);
            }
        }
    }

    // Continue current Tx
    PlanQueue.Progress(ctx);
}

void TDataShard::Handle(TEvPrivate::TEvAsyncJobComplete::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "AsyncJob complete"
        << " at " << TabletID());

    auto op = Pipeline.FindOp(ev->Cookie);
    if (op) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Found op"
            << " at "<< TabletID()
            << " cookie " << ev->Cookie);

        if (op->IsWaitingForAsyncJob()) {
            op->SetAsyncJobResult(ev->Get()->Prod);
            Pipeline.AddCandidateOp(op);
        }
    } else {
        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "AsyncJob complete"
            << " at " << TabletID()
            << " for unknown tx " << ev->Cookie);
    }

    // Continue current Tx
    PlanQueue.Progress(ctx);
}

void TDataShard::Handle(TEvPrivate::TEvRestartOperation::TPtr &ev, const TActorContext &ctx) {
    const auto txId = ev->Get()->TxId;

    if (auto op = Pipeline.FindOp(txId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Restart op: " << txId
            << " at " << TabletID());

        if (op->IsWaitingForRestart()) {
            op->ResetWaitingForRestartFlag();
            Pipeline.AddCandidateOp(op);
        }
    }

    // Continue current Tx
    PlanQueue.Progress(ctx);
}

bool TDataShard::ReassignChannelsEnabled() const {
    return true;
}

void TDataShard::ExecuteProgressTx(const TActorContext& ctx) {
    Execute(new TTxProgressTransaction(this, {}, {}), ctx);
}

void TDataShard::ExecuteProgressTx(TOperation::TPtr op, const TActorContext& ctx) {
    Y_ABORT_UNLESS(op->IsInProgress());
    NWilson::TTraceId traceId = op->GetTraceId();
    Execute(new TTxProgressTransaction(this, std::move(op), std::move(traceId)), ctx);
}

TDuration TDataShard::CleanupTimeout() const {
    const TDuration pipelineTimeout = Pipeline.CleanupTimeout();
    const TDuration snapshotTimeout = SnapshotManager.CleanupTimeout();
    const TDuration minTimeout = TDuration::Seconds(1);
    const TDuration maxTimeout = TDuration::MilliSeconds(DefaultTxStepDeadline() / 2);
    return Max(minTimeout, Min(pipelineTimeout, snapshotTimeout, maxTimeout));
}

class TDataShard::TTxGetRemovedRowVersions : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxGetRemovedRowVersions(TDataShard* self, TEvDataShard::TEvGetRemovedRowVersions::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        auto pathId = Ev->Get()->PathId;
        auto it = pathId ? Self->GetUserTables().find(pathId.LocalPathId) : Self->GetUserTables().begin();
        Y_ABORT_UNLESS(it != Self->GetUserTables().end());

        Reply = MakeHolder<TEvDataShard::TEvGetRemovedRowVersionsResult>(txc.DB.GetRemovedRowVersions(it->second->LocalTid));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Ev->Sender, Reply.Release(), 0, Ev->Cookie);
    }

private:
    TEvDataShard::TEvGetRemovedRowVersions::TPtr Ev;
    THolder<TEvDataShard::TEvGetRemovedRowVersionsResult> Reply;
};

void TDataShard::Handle(TEvDataShard::TEvGetRemovedRowVersions::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxGetRemovedRowVersions(this, std::move(ev)), ctx);
}

void SendViaSession(const TActorId& sessionId,
                    const TActorId& target,
                    const TActorId& src,
                    IEventBase* event,
                    ui32 flags,
                    ui64 cookie)
{
    THolder<IEventHandle> ev = MakeHolder<IEventHandle>(target, src, event, flags, cookie);

    if (sessionId) {
        ev->Rewrite(TEvInterconnect::EvForward, sessionId);
    }

    TActivationContext::Send(ev.Release());
}

class TBreakWriteConflictsTxObserver : public NTable::ITransactionObserver {
    friend class TBreakWriteConflictsTxObserverVolatileDependenciesGuard;

public:
    TBreakWriteConflictsTxObserver(TDataShard* self)
        : Self(self)
    {
    }

    void OnSkipUncommitted(ui64 txId) override {
        Y_ABORT_UNLESS(VolatileDependencies);
        Self->BreakWriteConflict(txId, *VolatileDependencies);
    }

    void OnSkipCommitted(const TRowVersion&) override {
        // nothing
    }

    void OnSkipCommitted(const TRowVersion&, ui64) override {
        // nothing
    }

    void OnApplyCommitted(const TRowVersion&) override {
        // nothing
    }

    void OnApplyCommitted(const TRowVersion&, ui64) override {
        // nothing
    }

private:
    TDataShard* Self;
    absl::flat_hash_set<ui64>* VolatileDependencies = nullptr;
};

class TBreakWriteConflictsTxObserverVolatileDependenciesGuard {
public:
    TBreakWriteConflictsTxObserverVolatileDependenciesGuard(
            TBreakWriteConflictsTxObserver* observer,
            absl::flat_hash_set<ui64>& volatileDependencies)
        : Observer(observer)
    {
        Y_ABORT_UNLESS(!Observer->VolatileDependencies);
        Observer->VolatileDependencies = &volatileDependencies;
    }

    ~TBreakWriteConflictsTxObserverVolatileDependenciesGuard() {
        Observer->VolatileDependencies = nullptr;
    }

private:
    TBreakWriteConflictsTxObserver* const Observer;
};

bool TDataShard::BreakWriteConflicts(NTable::TDatabase& db, const TTableId& tableId,
        TArrayRef<const TCell> keyCells, absl::flat_hash_set<ui64>& volatileDependencies)
{
    const auto localTid = GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTid);

    if (auto* cached = GetConflictsCache().GetTableCache(localTid).FindUncommittedWrites(keyCells)) {
        for (ui64 txId : *cached) {
            BreakWriteConflict(txId, volatileDependencies);
        }
        return true;
    }

    if (!BreakWriteConflictsTxObserver) {
        BreakWriteConflictsTxObserver = new TBreakWriteConflictsTxObserver(this);
    }

    TBreakWriteConflictsTxObserverVolatileDependenciesGuard guard(
        static_cast<TBreakWriteConflictsTxObserver*>(BreakWriteConflictsTxObserver.Get()),
        volatileDependencies);

    // We are not actually interested in the row version, we only need to
    // detect uncommitted transaction skips on the path to that version.
    auto res = db.SelectRowVersion(
        localTid, keyCells, /* readFlags */ 0,
        nullptr,
        BreakWriteConflictsTxObserver);

    if (res.Ready == NTable::EReady::Page) {
        return false;
    }

    return true;
}

void TDataShard::BreakWriteConflict(ui64 txId, absl::flat_hash_set<ui64>& volatileDependencies) {
    if (auto* info = GetVolatileTxManager().FindByCommitTxId(txId)) {
        if (info->State != EVolatileTxState::Aborting) {
            volatileDependencies.insert(txId);
        }
    } else {
        SysLocksTable().BreakLock(txId);
    }
}

class TDataShard::TTxGetOpenTxs : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxGetOpenTxs(TDataShard* self, TEvDataShard::TEvGetOpenTxs::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        auto pathId = Ev->Get()->PathId;
        auto it = pathId ? Self->GetUserTables().find(pathId.LocalPathId) : Self->GetUserTables().begin();
        Y_ABORT_UNLESS(it != Self->GetUserTables().end());

        auto openTxs = txc.DB.GetOpenTxs(it->second->LocalTid);

        Reply = MakeHolder<TEvDataShard::TEvGetOpenTxsResult>(pathId, std::move(openTxs));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Ev->Sender, Reply.Release(), 0, Ev->Cookie);
    }

private:
    TEvDataShard::TEvGetOpenTxs::TPtr Ev;
    THolder<TEvDataShard::TEvGetOpenTxsResult> Reply;
};

void TDataShard::Handle(TEvDataShard::TEvGetOpenTxs::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxGetOpenTxs(this, std::move(ev)), ctx);
}

void TDataShard::Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev, const TActorContext& ctx) {
    auto op = Pipeline.FindOp(ev->Cookie);
    if (op && op->HasWaitingForGlobalTxIdFlag()) {
        Pipeline.ProvideGlobalTxId(op, ev->Get()->TxId);
        Pipeline.AddCandidateOp(op);
        PlanQueue.Progress(ctx);
    }
}


} // NDataShard

TString TEvDataShard::TEvRead::ToString() const {
    TStringStream ss;
    ss << TBase::ToString();
    if (!Keys.empty()) {
        ss << " KeysSize: " << Keys.size();
    }
    if (!Ranges.empty()) {
        ss << " RangesSize: " << Ranges.size();
    }
    return ss.Str();
}

NActors::IEventBase* TEvDataShard::TEvRead::Load(TEventSerializedData* data) {
    auto* base = TBase::Load(data);
    auto* event = static_cast<TEvRead*>(base);
    auto& record = event->Record;

    event->Keys.reserve(record.KeysSize());
    for (const auto& key: record.GetKeys()) {
        event->Keys.emplace_back(key);
    }

    event->Ranges.reserve(record.RangesSize());
    for (const auto& range: record.GetRanges()) {
        event->Ranges.emplace_back(range);
    }

    return base;
}

// really ugly hacky, because Record is not mutable and calling members are const
void TEvDataShard::TEvRead::FillRecord() {
    if (!Keys.empty()) {
        Record.MutableKeys()->Reserve(Keys.size());
        for (auto& key: Keys) {
            Record.AddKeys(key.ReleaseBuffer());
        }
        Keys.clear();
    }

    if (!Ranges.empty()) {
        Record.MutableRanges()->Reserve(Ranges.size());
        for (auto& range: Ranges) {
            auto* pbRange = Record.AddRanges();
            range.Serialize(*pbRange);
        }
        Ranges.clear();
    }
}

TString TEvDataShard::TEvReadResult::ToString() const {
    TStringStream ss;
    ss << TBase::ToString();

    if (ArrowBatch) {
        ss << " ArrowRows: " << ArrowBatch->num_rows()
           << " ArrowCols: " << ArrowBatch->num_columns();
    }

    if (!Rows.empty()) {
        ss << " RowsSize: " << Rows.size();
    }

    return ss.Str();
}

NActors::IEventBase* TEvDataShard::TEvReadResult::Load(TEventSerializedData* data) {
    auto* base = TBase::Load(data);
    auto* event = static_cast<TEvReadResult*>(base);
    auto& record = event->Record;

    if (record.HasArrowBatch()) {
        const auto& batch = record.GetArrowBatch();
        auto schema = NArrow::DeserializeSchema(batch.GetSchema());
        event->ArrowBatch = NArrow::DeserializeBatch(batch.GetBatch(), schema);
        record.ClearArrowBatch();
    } else if (record.HasCellVec()) {
        auto& batch = *record.MutableCellVec();
        event->RowsSerialized.reserve(batch.RowsSize());
        for (auto& row: *batch.MutableRows()) {
            event->RowsSerialized.emplace_back(std::move(row));
        }
        record.ClearCellVec();
    }

    return base;
}

void TEvDataShard::TEvReadResult::FillRecord() {
    if (ArrowBatch) {
        auto* protoBatch = Record.MutableArrowBatch();
        protoBatch->SetSchema(NArrow::SerializeSchema(*ArrowBatch->schema()));
        protoBatch->SetBatch(NArrow::SerializeBatchNoCompression(ArrowBatch));
        ArrowBatch.reset();
        return;
    }

    if (!Batch.empty()) {
        auto* protoBatch = Record.MutableCellVec();
        protoBatch->MutableRows()->Reserve(Batch.Size());
        for (const auto& row: Batch) {
            protoBatch->AddRows(TSerializedCellVec::Serialize(row));
        }
        Batch = {};
        return;
    }

    if (!Rows.empty()) {
        auto* protoBatch = Record.MutableCellVec();
        protoBatch->MutableRows()->Reserve(Rows.size());
        for (const auto& row: Rows) {
            protoBatch->AddRows(TSerializedCellVec::Serialize(row));
        }
        Rows.clear();
        return;
    }
}

std::shared_ptr<arrow::RecordBatch> TEvDataShard::TEvReadResult::GetArrowBatch() const {
    return const_cast<TEvDataShard::TEvReadResult*>(this)->GetArrowBatch();
}

std::shared_ptr<arrow::RecordBatch> TEvDataShard::TEvReadResult::GetArrowBatch() {
    if (ArrowBatch)
        return ArrowBatch;

    if (Record.GetRowCount() == 0)
        return nullptr;

    ArrowBatch = NArrow::CreateNoColumnsBatch(Record.GetRowCount());
    return ArrowBatch;
}

} // NKikimr
