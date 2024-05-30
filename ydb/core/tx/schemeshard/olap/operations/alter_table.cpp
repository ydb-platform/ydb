#include "alter/abstract/object.h"
#include "alter/abstract/update.h"
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

        std::shared_ptr<ISSEntity> originalEntity = ISSEntity::GetEntityVerified(context, path);
        TUpdateRestoreContext urContext(originalEntity.get(), &context, (ui64)OperationId.GetTxId());
        std::shared_ptr<ISSEntityUpdate> update = originalEntity->RestoreUpdateVerified(urContext);

        TSimpleErrorCollector errors;
        TEntityInitializationContext iContext(&context);

        txState->ClearShardsInProgress();
        auto seqNo = context.SS->StartRound(*txState);

        for (auto& shard : txState->Shards) {
            const TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            AFL_VERIFY(shard.TabletType == ETabletType::ColumnShard);
            auto txShardString = update->GetShardTxBodyString((ui64)tabletId, seqNo);
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                update->GetShardTransactionKind(),
                context.SS->TabletID(),
                context.Ctx.SelfID,
                ui64(OperationId.GetTxId()),
                txShardString,
                context.SS->SelectProcessingParams(txState->TargetPathId), seqNo);

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
        TPath objPath = TPath::Init(pathId, context.SS);

        NIceDb::TNiceDb db(context.GetDB());

        std::shared_ptr<ISSEntity> originalEntity = ISSEntity::GetEntityVerified(context, objPath);
        TUpdateRestoreContext urContext(originalEntity.get(), &context, (ui64)OperationId.GetTxId());
        std::shared_ptr<ISSEntityUpdate> update = originalEntity->RestoreUpdateVerified(urContext);

        TUpdateFinishContext fContext(&objPath, &context, &db, NKikimr::NOlap::TSnapshot(ev->Get()->StepId, ev->Get()->TxId));
        update->Finish(fContext).Validate();

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

        TProposeErrorCollector errors(*result);
        TEntityInitializationContext iContext(&context);
        std::shared_ptr<ISSEntity> originalEntity = ISSEntity::GetEntityVerified(context, path);

        std::shared_ptr<ISSEntityUpdate> update;
        {
            TUpdateInitializationContext uContext(&*originalEntity, &context, &Transaction, (ui64)OperationId.GetTxId());
            TConclusion<std::shared_ptr<ISSEntityUpdate>> conclusion = originalEntity->CreateUpdate(uContext);
            if (conclusion.IsFail()) {
                errors.AddError(conclusion.GetErrorMessage());
                return result;
            }
            update = conclusion.DetachResult();
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        if (update->GetShardIds().size()) {
            TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterColumnTable, path->PathId);
            txState.State = TTxState::ConfigureParts;

            for (ui64 columnShardId : update->GetShardIds()) {
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
                TUpdateStartContext startContext(&path, &context, &db);
                auto status = update->Start(startContext);
                if (status.IsFail()) {
                    errors.AddError(status.GetErrorMessage());
                    return result;
                }
            }
            context.SS->PersistTxState(db, OperationId);

            context.OnComplete.ActivateTx(OperationId);

            SetState(NextState());
        } else {
            {
                {
                    TUpdateStartContext startContext(&path, &context, &db);
                    auto status = update->Start(startContext);
                    if (status.IsFail()) {
                        errors.AddError(status.GetErrorMessage());
                        return result;
                    }
                }
                {
                    TUpdateFinishContext fContext(&path, &context, &db, {});
                    auto status = update->Finish(fContext);
                    if (status.IsFail()) {
                        errors.AddError(status.GetErrorMessage());
                        return result;
                    }
                }
            }
            result->SetStatus(NKikimrScheme::StatusSuccess);
            SetState(TTxState::Done);
        }

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
