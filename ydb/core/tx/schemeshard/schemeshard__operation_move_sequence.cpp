#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/tx/sequenceshard/public/events.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

void MarkSrcDropped(NIceDb::TNiceDb& db,
                    TOperationContext& context,
                    TOperationId operationId,
                    const TTxState& txState,
                    TPath& srcPath)
{
    Y_ABORT_UNLESS(!srcPath->Dropped());
    Y_ABORT_UNLESS(txState.PlanStep);
    srcPath->SetDropped(txState.PlanStep, operationId.GetTxId());
    context.SS->PersistDropStep(db, srcPath->PathId, txState.PlanStep, operationId);

    DecAliveChildrenDirect(operationId, srcPath.Parent().Base(), context);
    srcPath.DomainInfo()->DecPathsInside(context.SS);

    IncParentDirAlterVersionWithRepublish(operationId, srcPath, context);
}

class TConfigureParts : public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveSequence TConfigureParts"
                << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType
        });
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TConfigureParts HandleReply TEvCreateSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS:
            case NKikimrTxSequenceShard::TEvCreateSequenceResult::SEQUENCE_ALREADY_EXISTS:
                // Treat expected status as success
                break;

            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TConfigureParts HandleReply ignoring unexpected TEvCreateSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TMoveSequence TConfigureParts HandleReply ignoring duplicate TEvCreateSequenceResult"
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
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TConfigureParts ProgressState"
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);
        Y_ABORT_UNLESS(!txState->Shards.empty());

        txState->ClearShardsInProgress();

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos.at(shardIdx).TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::SequenceShard);
            Y_ABORT_UNLESS(tabletId != InvalidTabletId);

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvCreateSequence>(txState->TargetPathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());
            event->Record.SetFrozen(true);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TMoveSequence TConfigureParts ProgressState"
                        << " sending TEvCreateSequence to tablet " << tabletId
                        << " operationId# " << OperationId
                        << " at tablet " << ssId);

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, txState->TargetPathId, event.Release());

            // Wait for results from this shard
            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;
    TTxState::ETxState& NextState;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TMoveSequence TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id, TTxState::ETxState& nextState)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType, NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType
        });
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        auto step = TStepId(ev->Get()->StepId);
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);

        auto srcPath = TPath::Init(txState->SourcePathId, context.SS);
        auto dstPath = TPath::Init(txState->TargetPathId, context.SS);

        TPathId pathId = txState->TargetPathId;

        Y_VERIFY_S(context.SS->Sequences.contains(pathId), "Sequence not found. PathId: " << pathId);
        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(pathId);
        Y_ABORT_UNLESS(sequenceInfo);
        TSequenceInfo::TPtr alterData = sequenceInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        context.SS->Sequences[pathId] = alterData;
        context.SS->PersistSequenceAlterRemove(db, pathId);
        context.SS->PersistSequence(db, pathId, *alterData);

        dstPath->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        dstPath.DomainInfo()->IncPathsInside(context.SS);

        dstPath.Activate();
        IncParentDirAlterVersionWithRepublish(OperationId, dstPath, context);

        NextState = TTxState::WaitShadowPathPublication;
        context.SS->ChangeTxState(db, OperationId, TTxState::WaitShadowPathPublication);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TWaitRenamedPathPublication: public TSubOperationState {
private:
    TOperationId OperationId;

    TPathId ActivePathId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveSequence TWaitRenamedPathPublication"
                << " operationId: " << OperationId;
    }

public:
    TWaitRenamedPathPublication(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType, TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvPrivate::TEvCompletePublication::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompletePublication"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet# " << ssId);

        Y_ABORT_UNLESS(ActivePathId == ev->Get()->PathId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet# " << ssId);

        TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);

        if (srcPath.IsActive()) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << ", no renaming has been detected for this operation");

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);
            return true;
        }

        auto activePath = TPath::Resolve(srcPath.PathString(), context.SS);
        Y_ABORT_UNLESS(activePath.IsResolved());

        Y_ABORT_UNLESS(activePath != srcPath);

        ActivePathId = activePath->PathId;
        context.OnComplete.PublishAndWaitPublication(OperationId, activePath->PathId);

        return false;
    }
};


class TDeleteTableBarrier: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveSequence TDeleteTableBarrier"
                << " operationId: " << OperationId;
    }

public:
    TDeleteTableBarrier(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvPrivate::TEvCompletePublication::EventType,
            TEvHive::TEvCreateTabletReply::EventType, NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType, TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompleteBarrier"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet# " << ssId);

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);
        MarkSrcDropped(db, context, OperationId, *txState, srcPath);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedMoveSequence);
        return true;
    }
    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        Y_ABORT_UNLESS(txState->PlanStep);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet# " << ssId);

        context.OnComplete.Barrier(OperationId, "RenamePathBarrier");

        return false;
    }
};

class TProposedMoveSequence : public TSubOperationState {
private:
    TOperationId OperationId;
    NKikimrTxSequenceShard::TEvGetSequenceResult GetSequenceResult;
    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveSequence TProposedMoveSequence"
                << " operationId# " << OperationId;
    }
    void UpdateSequenceDescription(NKikimrSchemeOp::TSequenceDescription& descr) {
        descr.SetStartValue(GetSequenceResult.GetStartValue());
        descr.SetMinValue(GetSequenceResult.GetMinValue());
        descr.SetMaxValue(GetSequenceResult.GetMaxValue());
        descr.SetCache(GetSequenceResult.GetCache());
        descr.SetIncrement(GetSequenceResult.GetIncrement());
        descr.SetCycle(GetSequenceResult.GetCycle());
        auto* setValMsg = descr.MutableSetVal();
        setValMsg->SetNextValue(GetSequenceResult.GetNextValue());
        setValMsg->SetNextUsed(GetSequenceResult.GetNextUsed());
    }
public:
    TProposedMoveSequence(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvPrivate::TEvOperationPlan::EventType,
            TEvPrivate::TEvCompleteBarrier::EventType,
            TEvPrivate::TEvCompletePublication::EventType,
            NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType,
            TEvHive::TEvCreateTabletReply::EventType
        });
    }
    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TProposedMoveSequence HandleReply TEvRestoreSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
        switch (status) {
            case NKikimrTxSequenceShard::TEvRestoreSequenceResult::SUCCESS:
            case NKikimrTxSequenceShard::TEvRestoreSequenceResult::SEQUENCE_ALREADY_ACTIVE: break;
            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TProposedMoveSequence HandleReply ignoring unexpected TEvRestoreSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }
        TTxState* txState = context.SS->FindTx(OperationId);
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->TargetPathId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ProposedMoveSequence);
        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TMoveSequence TProposedMoveSequence HandleReply ignoring duplicate TEvRestoreSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }
        if (!txState->ShardsInProgress.empty()) {
            return false;
        }

        TPathId pathId = txState->TargetPathId;

        NIceDb::TNiceDb db(context.GetDB());

        auto sequenceInfo = context.SS->Sequences.at(pathId);
        UpdateSequenceDescription(sequenceInfo->Description);

        context.SS->PersistSequence(db, pathId, *sequenceInfo);

        context.SS->ChangeTxState(db, OperationId, TTxState::DropParts);
        return true;
    }
    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TProposedMoveSequence HandleReply TEvGetSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
        switch (status) {
            case NKikimrTxSequenceShard::TEvGetSequenceResult::SUCCESS: break;
            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TProposedMoveSequence HandleReply ignoring unexpected TEvGetSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ProposedMoveSequence);
        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->SourcePathId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TMoveSequence TProposedMoveSequence HandleReply ignoring duplicate TEvGetSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }
        if (!txState->ShardsInProgress.empty()) {
            return false;
        }
        GetSequenceResult = ev->Get()->Record;
        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto currentTabletId = context.SS->ShardInfos.at(shardIdx).TabletID;
            Y_ABORT_UNLESS(currentTabletId != InvalidTabletId);
            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvRestoreSequence>(
                txState->TargetPathId, GetSequenceResult);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TMoveSequence TProposedMoveSequence ProgressState"
                        << " sending TEvRestoreSequence to tablet " << currentTabletId
                        << " operationId# " << OperationId
                        << " at tablet " << ssId);
            context.OnComplete.BindMsgToPipe(OperationId, currentTabletId, txState->TargetPathId, event.Release());
            // Wait for results from this shard
            txState->ShardsInProgress.insert(shardIdx);
        }
        return false;
    }
    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TProposedMoveSequence ProgressState"
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);
        Y_ABORT_UNLESS(!txState->Shards.empty());
        Y_ABORT_UNLESS(txState->SourcePathId != InvalidPathId);
        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos.at(shardIdx).TabletID;
            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvGetSequence>(txState->SourcePathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TMoveSequence TProposedMoveSequence ProgressState"
                        << " sending TEvGetSequence to tablet " << tabletId
                        << " operationId# " << OperationId
                        << " at tablet " << ssId);
            context.OnComplete.BindMsgToPipe(OperationId, tabletId, txState->SourcePathId, event.Release());
            txState->ShardsInProgress.insert(shardIdx);
        }
        return false;
    }
};

class TDropParts: public TSubOperationState {
private:
    TOperationId OperationId;

private:
    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveSequence TDropParts"
                << " operationId# " << OperationId;
    }

public:
    TDropParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvPrivate::TEvOperationPlan::EventType,
            TEvPrivate::TEvCompleteBarrier::EventType,
            TEvPrivate::TEvCompletePublication::EventType,
            NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType,
            TEvHive::TEvCreateTabletReply::EventType,
            NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::EventType,
            NSequenceShard::TEvSequenceShard::TEvGetSequenceResult::EventType
        });
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TMoveSequence TDropParts HandleReply TEvDropSequenceResult"
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
                    "TMoveSequence TDropParts HandleReply ignoring unexpected TEvDropSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);
        Y_ABORT_UNLESS(txState->State == TTxState::DropParts);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TMoveSequence TDropParts HandleReply ignoring duplicate TEvDropSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->SourcePathId);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());

            context.SS->PersistSequenceRemove(db, txState->SourcePathId);

            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);

        TPathId pathId = txState->SourcePathId;

        txState->ClearShardsInProgress();

        for (auto& shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[shard.Idx].TabletID;

            Y_ABORT_UNLESS(shard.TabletType == ETabletType::SequenceShard);

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvDropSequence>(pathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, txState->SourcePathId, event.Release());

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

class TDone: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TMoveSequence TDone"
            << ", operationId: " << OperationId;
    }
public:
    TDone(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveSequence);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", SourcePathId: " << txState->SourcePathId
                               << ", TargetPathId: " << txState->TargetPathId
                               << ", at schemeshard: " << ssId);

        // clear resources on src
        NIceDb::TNiceDb db(context.GetDB());

        TPathElement::TPtr dstPath = context.SS->PathsById.at(txState->TargetPathId);
        context.OnComplete.ReleasePathState(OperationId, dstPath->PathId, TPathElement::EPathState::EPathStateNoChanges);

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};

class TMoveSequence: public TSubOperation {
    TTxState::ETxState AfterPropose = TTxState::Invalid;

    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return AfterPropose;
        case TTxState::WaitShadowPathPublication:
            return TTxState::DeletePathBarrier;
        case TTxState::DeletePathBarrier:
            return TTxState::ProposedMoveSequence;
        case TTxState::ProposedMoveSequence:
            return TTxState::DropParts;
        case TTxState::DropParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        using TPtr = TSubOperationState::TPtr;

        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TPtr(new TCreateParts(OperationId));
        case TTxState::ConfigureParts:
            return TPtr(new TConfigureParts(OperationId));
        case TTxState::Propose:
            return TPtr(new TPropose(OperationId, AfterPropose));
        case TTxState::WaitShadowPathPublication:
            return MakeHolder<TWaitRenamedPathPublication>(OperationId);
        case TTxState::DeletePathBarrier:
            return MakeHolder<TDeleteTableBarrier>(OperationId);
        case TTxState::ProposedMoveSequence:
            return TPtr(new TProposedMoveSequence(OperationId));
        case TTxState::DropParts:
            return MakeHolder<TDropParts>(OperationId);
        case TTxState::Done:
            return TPtr(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& moveSequence = Transaction.GetMoveSequence();

        const TString& srcPathStr = moveSequence.GetSrcPath();
        const TString& dstPathStr = moveSequence.GetDstPath();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveSequence Propose"
                         << ", from: "<< srcPathStr
                         << ", to: " << dstPathStr
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        TString errStr;

        TPath srcPath = TPath::Resolve(srcPathStr, context.SS);
        {
            TPath::TChecker checks = srcPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsSequence()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        {
            TPath srcParentPath = srcPath.Parent();

            TPath::TChecker checks = srcParentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .NotAsyncReplicaTable();

            if (checks) {
                if (srcParentPath->IsTable()) {
                    // allow immediately inside a normal table
                    if (srcParentPath.IsUnderOperation()) {
                        checks.IsUnderTheSameOperation(OperationId.GetTxId()); // allowed only as part of consistent operations
                    }
                } else {
                    // otherwise don't allow unexpected object types
                    checks.IsLikeDirectory();
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath dstPath = TPath::Resolve(dstPathStr, context.SS);
        TPath dstParentPath = dstPath.Parent();

        {
            TPath::TChecker checks = dstParentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved();

            if (dstParentPath.IsUnderOperation()) {
                checks
                    .IsUnderTheSameOperation(OperationId.GetTxId());
            } else {
                checks
                    .NotUnderOperation();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        {
            TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved();

                    if (dstPath.IsUnderDeleting()) {
                        checks
                            .IsUnderDeleting()
                            .IsUnderTheSameOperation(OperationId.GetTxId());
                    } else if (dstPath.IsUnderMoving()) {
                        // it means that dstPath is free enough to be the move destination
                        checks
                            .IsUnderMoving()
                            .IsUnderTheSameOperation(OperationId.GetTxId());
                    } else {
                        checks
                            .IsDeleted()
                            .NotUnderOperation()
                            .FailOnExist(TPathElement::EPathType::EPathTypeTable, acceptExisted);
                    }
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .DepthLimit()
                    .IsValidLeafName(context.UserToken.Get())
                    .IsTheSameDomain(srcPath)
                    .DirChildrenLimit()
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(srcPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        TPathId allocatedPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, allocatedPathId);
        context.MemChanges.GrabPath(context.SS, dstParentPath.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->ParentPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabNewSequence(context.SS, allocatedPathId);

        context.DbChanges.PersistPath(allocatedPathId);
        context.DbChanges.PersistPath(dstParentPath.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->ParentPathId);
        context.DbChanges.PersistSequence(allocatedPathId);
        context.DbChanges.PersistAlterSequence(allocatedPathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(srcPath.Base()->Owner, allocatedPathId, /*allowInactivePath*/ true);
        result->SetPathId(dstPath->PathId.LocalPathId);
        dstPath->CreateTxId = OperationId.GetTxId();
        dstPath->LastTxId = OperationId.GetTxId();
        dstPath->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath->PathType = TPathElement::EPathType::EPathTypeSequence;
        if (!acl.empty()) {
            dstPath->ApplyACL(acl);
        }

        IncAliveChildrenSafeWithUndo(OperationId, dstParentPath, context); // for correct discard of ChildrenExist prop

        srcPath.Base()->PathState = TPathElement::EPathState::EPathStateMoving;
        srcPath.Base()->LastTxId = OperationId.GetTxId();

        if (dstParentPath->HasActiveChanges()) {
            TTxId parentTxId = dstParentPath->PlannedToCreate() ? dstParentPath->CreateTxId : dstParentPath->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        TTxState& txState =
            context.SS->CreateTx(OperationId, TTxState::TxMoveSequence, dstPath.Base()->PathId, srcPath.Base()->PathId);
        txState.State = TTxState::CreateParts;

        Y_ABORT_UNLESS(context.SS->Sequences.contains(srcPath.Base()->PathId));
        TSequenceInfo::TPtr srcSequence = context.SS->Sequences.at(srcPath.Base()->PathId);
        Y_ABORT_UNLESS(!srcSequence->Sharding.GetSequenceShards().empty());

        const auto& protoSequenceShard = *srcSequence->Sharding.GetSequenceShards().rbegin();
        TShardIdx sequenceShard = FromProto(protoSequenceShard);

        TSequenceInfo::TPtr sequenceInfo = new TSequenceInfo(0);
        sequenceInfo->AlterData = srcSequence->CreateNextVersion();

        txState.Shards.emplace_back(sequenceShard, ETabletType::SequenceShard, TTxState::ConfigureParts);
        auto& shardInfo = context.SS->ShardInfos.at(sequenceShard);
        if (shardInfo.CurrentTxId != OperationId.GetTxId()) {
            context.OnComplete.Dependence(shardInfo.CurrentTxId, OperationId.GetTxId());
        }

        {
            auto* p = sequenceInfo->AlterData->Sharding.AddSequenceShards();
            p->SetOwnerId(sequenceShard.GetOwnerId());
            p->SetLocalId(ui64(sequenceShard.GetLocalId()));
        }

        context.SS->Sequences[dstPath.Base()->PathId] = sequenceInfo;

        context.SS->IncrementPathDbRefCount(dstPath.Base()->PathId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);
        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, srcPath, context.SS, context.OnComplete);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveSequence AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveSequence AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateMoveSequence(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TMoveSequence>(id, tx);
}

ISubOperation::TPtr CreateMoveSequence(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TMoveSequence>(id, state);
}

}
