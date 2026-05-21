#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/persqueue/public/config.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

// IMPORTANT: this actor is registered via RegisterWithSameMailbox(), which
// guarantees that its handlers run serialized with SchemeShard's own tablet
// transactions in the same mailbox. That is what makes direct reads of SS->*
// fields here safe — they never race with a tablet transaction modifying the
// schema state. Do NOT change the registration mechanism without revisiting
// state access here.
class TRollingUpdateSolomonActor: public TActorBootstrapped<TRollingUpdateSolomonActor> {
public:
    static constexpr TDuration RequestTimeout = TDuration::Seconds(60);
    static constexpr ui64 MaxConsecutiveFailures = 10;

private:
    TSchemeShard* const SS;
    const TActorId SchemeShardActorId;
    const TOperationId OperationId;
    TVector<TShardIdx> Shards;
    ui64 CurrentShardPos = 0;
    TTabletId CurrentHive = InvalidTabletId;
    TActorId PipeClient;
    TTabletId ExpectedTabletId = InvalidTabletId;
    bool WaitingCreateReply = false;
    bool WaitingCreationResult = false;
    ui64 ConsecutiveFailures = 0;
    ui64 WakeupSeqNo = 0;
    ui64 ActiveWakeupSeqNo = 0;

    TString DebugHint() const {
        return TStringBuilder()
            << "TRollingUpdateSolomonActor"
            << ", operationId: " << OperationId;
    }

    bool CheckOperation() const {
        const TTxState* txState = SS->FindTx(OperationId);
        return txState
            && txState->TxType == TTxState::TxAlterSolomonVolume
            && txState->State == TTxState::RollingUpdateParts;
    }

    void ClosePipe() {
        if (PipeClient) {
            NTabletPipe::CloseClient(SelfId(), PipeClient);
            PipeClient = {};
        }
        CurrentHive = InvalidTabletId;
    }

    void ScheduleTimeout(const TActorContext& ctx) {
        ActiveWakeupSeqNo = ++WakeupSeqNo;
        ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup(ActiveWakeupSeqNo));
    }

    void Finish(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " finished successfully");

        ctx.Send(SchemeShardActorId,
                 new TEvPrivate::TEvSolomonRollingUpdateDone(OperationId, /*success=*/true));
        PassAway();
    }

    void Fail(const TActorContext& ctx, const TString& error) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " giving up: " << error);

        ctx.Send(SchemeShardActorId,
                 new TEvPrivate::TEvSolomonRollingUpdateDone(OperationId, /*success=*/false, error));
        PassAway();
    }

    void RetryCurrent(const TActorContext& ctx) {
        ClosePipe();
        WaitingCreateReply = false;
        WaitingCreationResult = false;
        ExpectedTabletId = InvalidTabletId;
        SendCurrent(ctx);
    }

    bool NoteFailureAndShouldGiveUp(const TActorContext& ctx, const TString& reason) {
        ++ConsecutiveFailures;
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " transient failure (" << ConsecutiveFailures
                               << "/" << MaxConsecutiveFailures << "): " << reason);
        return ConsecutiveFailures >= MaxConsecutiveFailures;
    }

    void OpenPipeAndSend(const TActorContext& ctx, TTabletId hive, THolder<TEvHive::TEvCreateTablet> request) {
        CurrentHive = hive;
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        PipeClient = Register(NTabletPipe::CreateClient(SelfId(), ui64(CurrentHive), pipeConfig));
        WaitingCreateReply = true;
        ScheduleTimeout(ctx);
        NTabletPipe::SendData(SelfId(), PipeClient, request.Release());
    }

    void SendCurrent(const TActorContext& ctx) {
        if (!CheckOperation()) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " stale operation, stop");
            PassAway();
            return;
        }

        if (CurrentShardPos >= Shards.size()) {
            Finish(ctx);
            return;
        }

        const TShardIdx shardIdx = Shards[CurrentShardPos];
        TTxState* txState = SS->FindTx(OperationId);
        auto path = SS->PathsById.at(txState->TargetPathId);
        auto ev = CreateEvCreateTablet(path, shardIdx, SS);
        const TTabletId hive = SS->ResolveHive(shardIdx);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " send rolling CreateTablet"
                                << ", shardIdx: " << shardIdx
                                << ", hive: " << hive
                                << ", pos: " << CurrentShardPos
                                << "/" << Shards.size()
                                << ", msg: " << ev->Record.ShortDebugString());

        OpenPipeAndSend(ctx, hive, std::move(ev));
    }

public:
    TRollingUpdateSolomonActor(TSchemeShard* ss, TOperationId operationId)
        : SS(ss)
        , SchemeShardActorId(ss->SelfId())
        , OperationId(operationId)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!CheckOperation()) {
            PassAway();
            return;
        }

        const TTxState* txState = SS->FindTx(OperationId);
        for (const auto& shard : txState->Shards) {
            if (shard.Operation == TTxState::RollingUpdateParts) {
                Shards.push_back(shard.Idx);
            }
        }

        Become(&TRollingUpdateSolomonActor::StateWork);
        SendCurrent(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHive::TEvCreateTabletReply, Handle);
            HFunc(TEvHive::TEvTabletCreationResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            default:
                break;
        }
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        // Ignore stale timeouts from previously-cancelled requests.
        if (ev->Get()->Tag != ActiveWakeupSeqNo) {
            return;
        }
        if (!WaitingCreateReply && !WaitingCreationResult) {
            return;
        }
        if (!CheckOperation()) {
            PassAway();
            return;
        }
        if (NoteFailureAndShouldGiveUp(ctx, "request timed out")) {
            Fail(ctx, "Hive request timed out repeatedly");
            return;
        }
        RetryCurrent(ctx);
    }

    void Handle(TEvHive::TEvCreateTabletReply::TPtr& ev, const TActorContext& ctx) {
        if (!CheckOperation()) {
            PassAway();
            return;
        }
        if (!WaitingCreateReply || CurrentShardPos >= Shards.size()) {
            // Stale reply from a previously-cancelled request: ignore.
            return;
        }

        const auto& record = ev->Get()->Record;
        const TShardIdx shardIdx(record.GetOwner(), TLocalShardIdx(record.GetOwnerIdx()));
        const TShardIdx expectedShardIdx = Shards[CurrentShardPos];
        if (shardIdx != expectedShardIdx) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " got CreateTabletReply for unexpected shard"
                                   << ", expected: " << expectedShardIdx
                                   << ", got: " << shardIdx
                                   << ", message: " << record.ShortDebugString());
            return;
        }

        const auto status = record.GetStatus();
        if (status == NKikimrProto::INVALID_OWNER) {
            const auto redirectTo = TTabletId(record.GetForwardRequest().GetHiveTabletId());
            if (redirectTo == InvalidTabletId) {
                if (NoteFailureAndShouldGiveUp(ctx, "INVALID_OWNER with empty redirect")) {
                    Fail(ctx, "Hive returned INVALID_OWNER without a forward target");
                    return;
                }
                RetryCurrent(ctx);
                return;
            }
            ClosePipe();
            auto path = SS->PathsById.at(SS->FindTx(OperationId)->TargetPathId);
            auto request = CreateEvCreateTablet(path, expectedShardIdx, SS);

            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " redirect CreateTablet"
                                    << ", shardIdx: " << expectedShardIdx
                                    << ", hive: " << redirectTo);

            OpenPipeAndSend(ctx, redirectTo, std::move(request));
            return;
        }

        if (status == NKikimrProto::BLOCKED) {
            // Hive is blocking this request; it will retry internally and send
            // another reply later. Keep waiting (timeout will fire if it doesn't).
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         DebugHint() << " CreateTablet BLOCKED"
                                     << ", shardIdx: " << expectedShardIdx
                                     << ", message: " << record.ShortDebugString());
            return;
        }

        if (status == NKikimrProto::ALREADY) {
            // Real Hive returns ALREADY when the tablet exists with the same
            // configuration and it does NOT send a follow-up TEvTabletCreationResult
            // (nothing was actually re-booted). This is the common case after a
            // SchemeShard restart that re-walks already-processed shards. Treat
            // it as success and move on.
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " shard already up-to-date"
                                   << ", shardIdx: " << expectedShardIdx
                                   << ", tabletId: " << record.GetTabletID());
            ConsecutiveFailures = 0;
            ++CurrentShardPos;
            ClosePipe();
            WaitingCreateReply = false;
            ExpectedTabletId = InvalidTabletId;
            SendCurrent(ctx);
            return;
        }

        if (status != NKikimrProto::OK) {
            const TString reason = TStringBuilder()
                << "Hive returned " << NKikimrProto::EReplyStatus_Name(status)
                << " for shard " << expectedShardIdx;
            if (NoteFailureAndShouldGiveUp(ctx, reason)) {
                Fail(ctx, reason);
                return;
            }
            RetryCurrent(ctx);
            return;
        }

        const auto tabletId = TTabletId(record.GetTabletID());
        if (tabletId == InvalidTabletId) {
            const TString reason = "Hive returned OK with InvalidTabletId";
            if (NoteFailureAndShouldGiveUp(ctx, reason)) {
                Fail(ctx, reason);
                return;
            }
            RetryCurrent(ctx);
            return;
        }

        ConsecutiveFailures = 0;
        ExpectedTabletId = tabletId;
        WaitingCreateReply = false;
        WaitingCreationResult = true;
        // Keep timeout armed — we now wait for TabletCreationResult.
        ScheduleTimeout(ctx);
    }

    void Handle(TEvHive::TEvTabletCreationResult::TPtr& ev, const TActorContext& ctx) {
        if (!CheckOperation()) {
            PassAway();
            return;
        }
        if (!WaitingCreationResult || CurrentShardPos >= Shards.size()) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const auto tabletId = TTabletId(record.GetTabletID());
        if (tabletId != ExpectedTabletId) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " got TabletCreationResult for unexpected tablet"
                                   << ", expected: " << ExpectedTabletId
                                   << ", got: " << tabletId
                                   << ", message: " << record.ShortDebugString());
            return;
        }

        const auto status = record.GetStatus();
        if (status != NKikimrProto::OK) {
            const TString reason = TStringBuilder()
                << "TabletCreationResult status " << NKikimrProto::EReplyStatus_Name(status)
                << " for tabletId " << tabletId;
            if (NoteFailureAndShouldGiveUp(ctx, reason)) {
                Fail(ctx, reason);
                return;
            }
            RetryCurrent(ctx);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " tablet updated"
                               << ", shardIdx: " << Shards[CurrentShardPos]
                               << ", tabletId: " << tabletId);

        ConsecutiveFailures = 0;
        ++CurrentShardPos;
        ClosePipe();
        ExpectedTabletId = InvalidTabletId;
        WaitingCreationResult = false;
        SendCurrent(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->ClientId != PipeClient) {
            return;
        }

        if (ev->Get()->Status != NKikimrProto::OK) {
            const TString reason = TStringBuilder()
                << "pipe connect failed to hive " << ev->Get()->TabletId
                << " status " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status);
            if (NoteFailureAndShouldGiveUp(ctx, reason)) {
                Fail(ctx, reason);
                return;
            }
            RetryCurrent(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->ClientId != PipeClient) {
            return;
        }

        const TString reason = TStringBuilder()
            << "pipe disconnected from hive " << ev->Get()->TabletId;
        if (NoteFailureAndShouldGiveUp(ctx, reason)) {
            Fail(ctx, reason);
            return;
        }
        RetryCurrent(ctx);
    }

    void PassAway() override {
        ClosePipe();
        TActorBootstrapped<TRollingUpdateSolomonActor>::PassAway();
    }
};

class TRollingUpdateParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterSolomon TRollingUpdateParts"
            << ", operationId: " << OperationId;
    }

public:
    TRollingUpdateParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
                       {TEvHive::TEvCreateTabletReply::EventType,
                        TEvHive::TEvTabletCreationResult::EventType,
                        TEvHive::TEvAdoptTabletReply::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSolomonVolume);
        Y_ABORT_UNLESS(txState->State == TTxState::RollingUpdateParts);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->SelfTabletId());

        txState->ClearShardsInProgress();
        txState->UpdateShardsInProgress(TTxState::RollingUpdateParts);
        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
            return true;
        }

        context.Ctx.RegisterWithSameMailbox(new TRollingUpdateSolomonActor(context.SS, OperationId));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvSolomonRollingUpdateDone::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSolomonRollingUpdateDone"
                               << ", success: " << ev->Get()->Success
                               << ", error: " << ev->Get()->Error);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState || txState->State != TTxState::RollingUpdateParts) {
            return false;
        }

        if (!ev->Get()->Success) {
            // Rolling update could not finish (e.g. Hive permanently rejected
            // a shard). We do NOT have an abort path for AlterSolomon today,
            // so the safest action is to keep retrying: schedule a fresh
            // rolling-update actor by re-activating the same state. The
            // transient-failure budget inside the actor protects against
            // tight loops; here we just re-arm.
            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " rolling update reported failure, will retry"
                                    << ", error: " << ev->Get()->Error);
            txState->ClearShardsInProgress();
            context.OnComplete.ActivateTx(OperationId);
            return false;
        }

        txState->ClearShardsInProgress();

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        return true;
    }
};

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterSolomon TConfigureParts"
            << ", operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
                       {TEvHive::TEvCreateTabletReply::EventType, TEvHive::TEvAdoptTabletReply::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet# " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSolomonVolume);

        auto solomon = context.SS->SolomonVolumes[txState->TargetPathId];
        Y_VERIFY_S(solomon, "solomon volume is null. PathId: " << txState->TargetPathId);
        Y_VERIFY_S(solomon->AlterData, "solomon volume alter data is null. PathId: " << txState->TargetPathId);

        for (const auto& shard: txState->Shards) {
            auto solomonPartition = solomon->AlterData->Partitions[shard.Idx];
            Y_VERIFY_S(solomonPartition, "rtmr partitions is null shard idx: " << shard.Idx << " Path: " << txState->TargetPathId);

            auto tabletId = context.SS->ShardInfos[shard.Idx].TabletID;

            if (solomonPartition->TabletId != InvalidTabletId && tabletId != solomonPartition->TabletId) {
                Y_FAIL_S("Solomon partition tablet id mismatch"
                    << ": expected: " << solomonPartition->TabletId
                    << ", got: " << tabletId);
            }

            solomonPartition->TabletId = tabletId;
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        return true;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterSolomon TPropose"
            << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
                       {TEvHive::TEvCreateTabletReply::EventType, TEvHive::TEvAdoptTabletReply::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if(!txState) {
            return false;
        }

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        auto solomon = context.SS->SolomonVolumes[txState->TargetPathId];
        Y_VERIFY_S(solomon, "solomon volume is null. PathId: " << txState->TargetPathId);
        Y_VERIFY_S(solomon->AlterData, "solomon volume alter data is null. PathId: " << txState->TargetPathId);

        context.SS->TabletCounters->Simple()[COUNTER_SOLOMON_PARTITIONS_COUNT].Sub(solomon->Partitions.size());
        context.SS->TabletCounters->Simple()[COUNTER_SOLOMON_PARTITIONS_COUNT].Add(solomon->AlterData->Partitions.size());

        context.SS->PersistSolomonVolume(db, txState->TargetPathId, solomon->AlterData);
        context.SS->SolomonVolumes[txState->TargetPathId] = solomon->AlterData;

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSolomonVolume);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TAlterSolomon: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::RollingUpdateParts:
            return TTxState::CreateParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::RollingUpdateParts:
            return MakeHolder<TRollingUpdateParts>(OperationId);
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alter = Transaction.GetAlterSolomonVolume();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();
        const ui32 channelProfileId = alter.GetChannelProfileId();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSolomon Propose"
                         << ", path: "<< parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", channelProfileId: " << channelProfileId
                         << ", at schemeshard: " << ssId);

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        TString errStr;

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsSolomon()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (checks) {
                TSolomonVolumeInfo::TPtr solomon = context.SS->SolomonVolumes.at(path.Base()->PathId);
                if (alter.GetPartitionCount() > solomon->Partitions.size()) {
                    const ui64 shardsToCreate = alter.GetPartitionCount() - solomon->Partitions.size();

                    checks
                        .ShardsLimit(shardsToCreate)
                        .PathShardsLimit(shardsToCreate);
                }

            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TSolomonVolumeInfo::TPtr solomon = context.SS->SolomonVolumes.at(path.Base()->PathId);

        if (!alter.HasPartitionCount() && !alter.GetUpdateChannelsBinding()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Empty alter");
            return result;
        }

        if (alter.GetUpdateChannelsBinding() && !AppData()->FeatureFlags.GetAllowUpdateChannelsBindingOfSolomonPartitions()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "Updating of channels binding is not available");
            return result;
        }

        if (alter.HasPartitionCount()) {
            if (alter.GetPartitionCount() < solomon->Partitions.size()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "solomon volume has more shards than requested");
                return result;
            }

            if (alter.GetPartitionCount() == solomon->Partitions.size() && !alter.HasStorageConfig()) {
                result->SetError(NKikimrScheme::StatusSuccess, "solomon volume has already the same shards as requested");
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TChannelsBindings channelsBinding;
        bool isResolved = false;
        if (alter.HasStorageConfig()) {
            isResolved = context.SS->ResolveSolomonChannels(alter.GetStorageConfig(), path.GetPathIdForDomain(), channelsBinding);
        } else {
            if (!alter.HasChannelProfileId()) {
                result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, "set channel profile id, please");
                return result;
            }
            isResolved = context.SS->ResolveSolomonChannels(alter.GetChannelProfileId(), path.GetPathIdForDomain(), channelsBinding);
        }
        if (!isResolved) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct channel binding with the storage pool");
            return result;
        }

        result->SetPathId(path.Base()->PathId.LocalPathId);

        TSolomonVolumeInfo::TPtr alterSolomon = solomon->CreateAlter();

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterSolomonVolume,  path.Base()->PathId);

        TShardInfo solomonPartitionInfo = TShardInfo::SolomonPartitionInfo(OperationId.GetTxId(), path.Base()->PathId);
        solomonPartitionInfo.BindedChannels = channelsBinding;

        path.Base()->LastTxId = OperationId.GetTxId();
        path.Base()->PathState = TPathElement::EPathState::EPathStateAlter;

        context.SS->PersistLastTxId(db, path.Base());

        if (alter.GetUpdateChannelsBinding()) {
            txState.Shards.reserve(alter.HasPartitionCount() ? alter.GetPartitionCount() : solomon->Partitions.size());
        } else {
            Y_ABORT_UNLESS(alter.HasPartitionCount());
            txState.Shards.reserve(alter.GetPartitionCount() - solomon->Partitions.size());
        }

        if (alter.GetUpdateChannelsBinding()) {
            for (const auto& [shardIdx, partitionInfo] : solomon->Partitions) {
                txState.Shards.emplace_back(shardIdx, TTabletTypes::KeyValue, TTxState::RollingUpdateParts);

                auto& shardInfo = context.SS->ShardInfos.at(shardIdx);
                shardInfo.CurrentTxId = OperationId.GetTxId();
                shardInfo.BindedChannels = channelsBinding;

                context.SS->PersistShardMapping(db, shardIdx, partitionInfo->TabletId, path.Base()->PathId, OperationId.GetTxId(), solomonPartitionInfo.TabletType);
                context.SS->PersistChannelsBinding(db, shardIdx, channelsBinding);
            }
        }

        if (alter.HasPartitionCount()) {
            const ui64 shardsToCreate = alter.GetPartitionCount() - solomon->Partitions.size();

            for (ui64 i = 0; i < shardsToCreate; ++i) {
                const auto shardIdx = context.SS->RegisterShardInfo(solomonPartitionInfo);
                context.SS->PersistShardMapping(db, shardIdx, InvalidTabletId, path.Base()->PathId, OperationId.GetTxId(), solomonPartitionInfo.TabletType);
                context.SS->PersistChannelsBinding(db, shardIdx, channelsBinding);

                alterSolomon->Partitions[shardIdx] = new TSolomonPartitionInfo(solomon->Partitions.size() + i);
                txState.Shards.emplace_back(shardIdx, TTabletTypes::KeyValue, TTxState::CreateParts);
            }
            context.SS->PersistUpdateNextShardIdx(db);

            path.Base()->IncShardsInside(shardsToCreate);
        }

        solomon->AlterData = alterSolomon;
        context.SS->PersistAlterSolomonVolume(db, path.Base()->PathId, solomon);

        const auto firstState = alter.GetUpdateChannelsBinding()
            ? TTxState::RollingUpdateParts
            : TTxState::CreateParts;

        context.SS->ChangeTxState(db, OperationId, firstState);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);

        path.DomainInfo()->AddInternalShards(txState, context.SS);

        SetState(firstState);
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterSolomon");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSolomon AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterSolomon(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterSolomon>(id, tx);
}

ISubOperation::TPtr CreateAlterSolomon(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterSolomon>(id, state);
}

}
