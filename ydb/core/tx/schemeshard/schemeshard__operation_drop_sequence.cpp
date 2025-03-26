#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/sequenceshard/public/events.h>

namespace NKikimr::NSchemeShard {

namespace {

class TDropParts: public TSubOperationState {
private:
    TOperationId OperationId;

private:
    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropSequence TDropParts"
                << " operationId#" << OperationId;
    }

public:
    TDropParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TDropSequence TDropParts HandleReply TEvDropSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvDropSequenceResult::SUCCESS:
            case NKikimrTxSequenceShard::TEvDropSequenceResult::SEQUENCE_NOT_FOUND:
                // Treat expected status as success
                break;

            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TDropSequence TDropParts HandleReply ignoring unexpected TEvDropSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropSequence);
        Y_ABORT_UNLESS(txState->State == TTxState::DropParts);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TDropSequence TDropParts HandleReply ignoring duplicate TEvDropSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->TargetPathId);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropSequence);

        TPathId pathId = txState->TargetPathId;

        txState->ClearShardsInProgress();

        for (auto& shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[shard.Idx].TabletID;

            Y_ABORT_UNLESS(shard.TabletType == ETabletType::SequenceShard);

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvDropSequence>(pathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, txState->TargetPathId, event.Release());

            // Wait for results from this shard
            txState->ShardsInProgress.insert(shardIdx);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " Propose drop at sequence shard"
                                    << " tabletId# " << tabletId
                                    << " pathId# " << pathId);
        }

        Y_ABORT_UNLESS(!txState->ShardsInProgress.empty());
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

private:
    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropSequence TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            NSequenceShard::TEvSequenceShard::TEvDropSequenceResult::EventType,
        });
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << " at schemeshard: " << ssId
                               << ", stepId: " << step);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropSequence);

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        Y_VERIFY_S(context.SS->PathsById.contains(path->ParentPathId),
                   "no parent with id: " <<  path->ParentPathId << " for node with id: " << path->PathId);
        auto parentDir = context.SS->PathsById.at(path->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);

        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        parentDir->DecAliveChildren();

        context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Add(-path->UserAttrs->Size());
        context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.SS->ClearDescribePathCaches(path);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.SS->PersistSequenceRemove(db, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << " at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropSequence);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TDropSequence: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::DropParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::DropParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::DropParts:
            return MakeHolder<TDropParts>(OperationId);
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

        const auto& drop = Transaction.GetDrop();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropSequence Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << drop.GetId()
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath path = drop.HasId()
            ? TPath::Init(context.SS->MakeLocalId(drop.GetId()), context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);

        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsSequence()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved() && path->IsSequence() && (path->PlannedToDrop() || path->Dropped())) {
                    result->SetPathDropTxId(ui64(path->DropTxId));
                    result->SetPathId(path->PathId.LocalPathId);
                }
                return result;
            }
        }

        TPath parent = path.Parent();
        {
            TPath::TChecker checks = parent.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCommonSensePath();

            if (checks) {
                if (parent->IsTable()) {
                    // allow immediately inside a normal table
                    if (parent.IsUnderOperation()) {
                        checks.IsUnderTheSameOperation(OperationId.GetTxId()); // allowed only as part of consistent operations
                    }
                } else {
                    checks
                        .NotUnderDeleting()
                        .IsLikeDirectory();
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(context.SS->Sequences.contains(path->PathId));
        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(path->PathId);
        Y_ABORT_UNLESS(!sequenceInfo->AlterData);

        if (parent->IsTable() && !parent.IsUnderDeleting()) {
            // TODO: we probably want some kind of usage refcount
            auto tableInfo = context.SS->Tables.at(parent->PathId);
            if (tableInfo->IsUsingSequence(path->Name)) {
                TString explain = TStringBuilder() << "cannot delete sequence " << path->Name
                    << " used by table " << parent.PathString();
                // FIXME: status is consistent with rmdir, but is it correct?
                result->SetError(NKikimrScheme::StatusNameConflict, explain);
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropSequence, path->PathId);
        txState.State = TTxState::DropParts;
        // Dirty hack: drop step must not be zero because 0 is treated as "hasn't been dropped"
        txState.MinStep = TStepId(1);

        NIceDb::TNiceDb db(context.GetDB());

        for (const auto& shardIdxProto : sequenceInfo->Sharding.GetSequenceShards()) {
            TShardIdx shardIdx = FromProto(shardIdxProto);
            Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
            txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::DropParts);
            // N.B. we don't change or persist CurrentTxId for the shard unlike most operations
            // The reason is that we don't operate on the shard itself, only on objects inside the shard
        }

        context.OnComplete.ActivateTx(OperationId);

        path->PathState = TPathElement::EPathState::EPathStateDrop;
        path->DropTxId = OperationId.GetTxId();
        path->LastTxId = OperationId.GetTxId();
        context.SS->PersistLastTxId(db, path.Base());

        context.SS->PersistTxState(db, OperationId);

        context.SS->TabletCounters->Simple()[COUNTER_SEQUENCE_COUNT].Add(-1);

        ++parent->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parent.Base());
        context.SS->ClearDescribePathCaches(parent.Base());
        context.SS->ClearDescribePathCaches(path.Base());

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parent->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);
        }

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TDropSequence");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropSequence AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        Y_ABORT_UNLESS(path);

        if (path->Dropped()) {
            // We don't really need to do anything
        }

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

ISubOperation::TPtr CreateDropSequence(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropSequence>(id ,tx);
}

ISubOperation::TPtr CreateDropSequence(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TDropSequence>(id, state);
}

}
