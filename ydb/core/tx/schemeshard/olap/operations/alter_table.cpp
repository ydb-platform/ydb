#include "alter/abstract/object.h"
#include "alter/abstract/update.h"
#include "alter/abstract/evolution.h"
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TAlterColumnTable TConfigureParts operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
         return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                   << " at tabletId# " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);

        TPathId pathId = txState->TargetPathId;
        TPath path = TPath::Init(pathId, context.SS);
        TString pathString = path.PathString();

        auto tableInfo = context.SS->ColumnTables.GetVerifiedPtr(pathId);
        std::shared_ptr<ISSEntityEvolution> evolution;
        {
            TEvolutionInitializationContext eContext(&context);
            evolution = tableInfo->BuildEvolution(pathId, eContext).DetachResult();
        }
        TColumnTableInfo::TPtr alterData = tableInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        TSimpleErrorCollector errors;
        TEntityInitializationContext iContext(&context);

        txState->ClearShardsInProgress();
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto seqNo = context.SS->StartRound(*txState);
        context.SS->FillSeqNo(tx, seqNo);

        for (auto& shard : txState->Shards) {
            const TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            AFL_VERIFY(shard.TabletType == ETabletType::ColumnShard);
            auto txShard = evolution->GetShardTxBody(tx, (ui64)tabletId);
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                evolution->GetShardTransactionKind(),
                context.SS->TabletID(),
                context.Ctx.SelfID,
                ui64(OperationId.GetTxId()),
                txShard.SerializeAsString(),
                context.SS->SelectProcessingParams(txState->TargetPathId));

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " Propose modify scheme on shard"
                                    << " tabletId: " << tabletId);
        }

        txState->UpdateShardsInProgress();
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterColumnTable TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType,
             TEvColumnShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvOperationPlan"
                     << " at tablet: " << ssId
                     << ", stepId: " << step);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable); 

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        auto tableInfo = context.SS->ColumnTables.TakeAlterVerified(pathId);
        context.SS->PersistColumnTableAlterRemove(db, pathId);
        context.SS->PersistColumnTable(db, pathId, *tableInfo);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        if (parentDir->IsLikeDirectory()) {
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
        }
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        ++path->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, path);
        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable); 

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;
            shardSet.insert(tablet);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
        return false;
    }
};

class TProposedWaitParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterColumnTable TProposedWaitParts"
                << " operationId#" << OperationId;
    }

public:
    TProposedWaitParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType,
             TEvColumnShard::TEvProposeTransactionResult::EventType,
             TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable); 
        auto shardId = TTabletId(ev->Get()->Record.GetOrigin());
        auto shardIdx = context.SS->MustGetShardIdx(shardId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        txState->ShardsInProgress.erase(shardIdx);
        return txState->ShardsInProgress.empty();
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);
        txState->ClearShardsInProgress();

        for (auto& shard : txState->Shards) {
            TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            switch (shard.TabletType) {
                case ETabletType::ColumnShard: {
                    auto event = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(ui64(OperationId.GetTxId()));

                    context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());
                    txState->ShardsInProgress.insert(shard.Idx);
                    break;
                }
                default: {
                    Y_ABORT("unexpected tablet type");
                }
            }

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " wait for NotifyTxCompletionResult"
                                    << " tabletId: " << tabletId);
        }

        return false;
    }
};

class TAlterColumnTable: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<TProposedWaitParts>(OperationId);
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

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.HasAlterColumnTable() ? Transaction.GetAlterColumnTable().GetName() : Transaction.GetAlterTable().GetName();
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterColumnTable Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsColumnTable()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto tableInfo = context.SS->ColumnTables.GetVerifiedPtr(path.Base()->PathId);

        if (tableInfo->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Table is not created yet");
            return result;
        }
        if (tableInfo->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }
        TProposeErrorCollector errors(*result);
        std::shared_ptr<ISSEntity> originalEntity;
        {
            TEntityInitializationContext iContext(&context);
            auto conclusion = tableInfo->BuildEntity(path.Base()->PathId, iContext);
            if (conclusion.IsFail()) {
                errors.AddError(conclusion.GetErrorMessage());
                return result;
            }
            originalEntity = conclusion.DetachResult();
        }

        std::shared_ptr<ISSEntityUpdate> update;
        {
            TUpdateInitializationContext uContext(&context, &Transaction);
            TConclusion<std::shared_ptr<ISSEntityUpdate>> conclusion = originalEntity->CreateUpdate(uContext, originalEntity);
            if (conclusion.IsFail()) {
                errors.AddError(conclusion.GetErrorMessage());
                return result;
            }
            update = conclusion.DetachResult();
        }

        std::shared_ptr<ISSEntityEvolution> evolution;
        {
            auto conclusion = update->BuildEvolution(update);
            if (conclusion.IsFail()) {
                errors.AddError(conclusion.GetErrorMessage());
                return result;
            }
            evolution = conclusion.DetachResult();
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterColumnTable, path->PathId);
        txState.State = TTxState::ConfigureParts;

        // TODO: we need to know all shards where this table is currently active
        for (ui64 columnShardId : tableInfo->GetColumnShards()) {
            auto tabletId = TTabletId(columnShardId);
            auto shardIdx = context.SS->TabletIdToShardIdx.at(tabletId);

            Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
            txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

            context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
            context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
        }

        path->LastTxId = OperationId.GetTxId();
        path->PathState = TPathElement::EPathState::EPathStateAlter;
        context.SS->PersistLastTxId(db, path.Base());

        {
            TEvolutionStartContext startContext(&path, &context, &db);
            auto status = evolution->StartEvolution(startContext);
            if (status.IsFail()) {
                errors.AddError(status.GetErrorMessage());
                return result;
            }
        }

        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterColumnTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterColumnTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterColumnTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<NOlap::NAlter::TAlterColumnTable>(id, tx);
}

ISubOperation::TPtr CreateAlterColumnTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<NOlap::NAlter::TAlterColumnTable>(id, state);
}

}
