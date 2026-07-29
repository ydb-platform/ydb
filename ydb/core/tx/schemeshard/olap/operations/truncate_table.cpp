#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/base/subdomain.h>

namespace NKikimr::NSchemeShard {

namespace {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TTruncateColumnTable TConfigureParts"
                << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
         return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateColumnTable);

        TPathId pathId = txState->TargetPathId;

        txState->ClearShardsInProgress();

        auto seqNo = context.SS->StartRound(*txState);

        TString columnShardTxBody;
        {
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);

            auto* truncate = tx.MutableTruncateTable();
            truncate->SetPathId(pathId.LocalPathId);

            Y_ABORT_UNLESS(tx.SerializeToString(&columnShardTxBody));
        }

        for (auto& shard : txState->Shards) {
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::ColumnShard);

            TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;

            {
                auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                    NKikimrTxColumnShard::TX_KIND_SCHEMA,
                    context.SS->TabletID(),
                    context.Ctx.SelfID,
                    ui64(OperationId.GetTxId()),
                    columnShardTxBody, seqNo,
                    context.SS->SelectProcessingParams(txState->TargetPathId),
                    0,
                    0);

                context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());
            }

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " Propose truncate on shard"
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
                << "TTruncateColumnTable TPropose"
                << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvColumnShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << " at schemeshard: " << ssId
                               << ", stepId: " << step);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateColumnTable);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        // Bump the column table's AlterVersion
        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(txState->TargetPathId));
        auto tableInfo = context.SS->ColumnTables.GetVerifiedPtr(txState->TargetPathId);
        tableInfo->AlterVersion += 1;
        context.SS->PersistColumnTable(db, txState->TargetPathId, *tableInfo);

        context.SS->ClearDescribePathCaches(context.SS->PathsById.at(txState->TargetPathId));
        context.OnComplete.PublishToSchemeBoard(OperationId, txState->TargetPathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << " at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateColumnTable);

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(idx));
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
                << "TTruncateColumnTable TProposedWaitParts"
                << " operationId# " << OperationId;
    }

public:
    TProposedWaitParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvColumnShard::TEvProposeTransactionResult::EventType,
             TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateColumnTable);

        auto shardId = TTabletId(ev->Get()->Record.GetOrigin());
        auto shardIdx = context.SS->MustGetShardIdx(shardId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        txState->ShardsInProgress.erase(shardIdx);
        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << " at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateColumnTable);

        txState->ClearShardsInProgress();

        for (auto& shard : txState->Shards) {
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::ColumnShard);

            TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            auto event = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(ui64(OperationId.GetTxId()));

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());
            txState->ShardsInProgress.insert(shard.Idx);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " wait for NotifyTxCompletionResult"
                                    << " tabletId: " << tabletId);
        }

        return false;
    }
};

class TTruncateColumnTable: public TSubOperation {
public:
    using TSubOperation::TSubOperation;

    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
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
        case TTxState::Waiting:
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

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& truncateTableOp = Transaction.GetTruncateTable();
        const auto stringTablePath = NKikimr::JoinPath({Transaction.GetWorkingDir(), truncateTableOp.GetTableName()});

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTruncateColumnTable Propose"
                         << ", path: " << stringTablePath
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!AppData()->FeatureFlags.GetEnableTruncateColumnTable()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "TRUNCATE TABLE is not supported for column tables");
            return result;
        }

        // TRUNCATE for column tables swaps the table to a freshly generated internal path id on the column
        // shard. This requires GenerateInternalPathId mode: otherwise the internal path id is forced to equal
        // the SchemeShard local path id and the generated-id counter (MaxInternalPathId) is not persisted
        // across restarts, which would break the table or lead to internal path id reuse on later truncations.
        if (!AppData()->ColumnShardConfig.GetGenerateInternalPathId()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed,
                "TRUNCATE TABLE for column tables requires GenerateInternalPathId to be enabled");
            return result;
        }

        TPath tablePath = TPath::Resolve(stringTablePath, context.SS);
        {
            TPath::TChecker checks = tablePath.Check();
            checks
                .IsResolved()
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .NotDeleted()
                .IsColumnTable()
                .NotReadOnlyColumnTable()
                .NotUnderDeleting()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        {
            TString errStr;
            if (!context.SS->CheckApplyIf(Transaction, errStr)) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
                return result;
            }
        }

        {
            TString errStr;
            if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
                result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
                return result;
            }
        }

        // Begin create local transaction
        auto opTxId = OperationId.GetTxId();

        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(tablePath.Base()->PathId));
        auto tableInfo = context.SS->ColumnTables.GetVerified(tablePath.Base()->PathId);

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxTruncateColumnTable, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        if (tableInfo->IsStandalone()) {
            NIceDb::TNiceDb db(context.GetDB());
            for (auto shardIdx : tableInfo->BuildOwnedColumnShardsVerified()) {
                Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
                txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

                context.SS->ShardInfos[shardIdx].CurrentTxId = opTxId;
                context.SS->PersistShardTx(db, shardIdx, opTxId);
            }
        } else {
            auto storePathId = tableInfo->GetOlapStorePathIdVerified();
            TPath storePath = TPath::Init(storePathId, context.SS);
            {
                TPath::TChecker checks = storePath.Check();
                checks
                    .NotEmpty()
                    .IsResolved()
                    .IsOlapStore()
                    .NotUnderOperation();

                if (!checks) {
                    result->SetError(checks.GetStatus(), checks.GetError());
                    return result;
                }
            }

            Y_ABORT_UNLESS(context.SS->OlapStores.contains(storePathId));
            TOlapStoreInfo::TPtr storeInfo = context.SS->OlapStores.at(storePathId);

            Y_ABORT_UNLESS(storeInfo->ColumnTables.contains(tablePath->PathId));
            storeInfo->ColumnTablesUnderOperation.insert(tablePath->PathId);

            // Sequentially chain operations in the same olap store
            if (context.SS->Operations.contains(storePath.Base()->LastTxId)) {
                context.OnComplete.Dependence(storePath.Base()->LastTxId, opTxId);
            }
            storePath.Base()->LastTxId = opTxId;

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->PersistLastTxId(db, storePath.Base());

            for (ui64 columnShardId : tableInfo->GetColumnShards()) {
                auto tabletId = TTabletId(columnShardId);
                auto shardIdx = context.SS->TabletIdToShardIdx.at(tabletId);

                Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
                txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

                context.SS->ShardInfos[shardIdx].CurrentTxId = opTxId;
                context.SS->PersistShardTx(db, shardIdx, opTxId);
            }
        }

        tablePath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
        tablePath.Base()->LastTxId = opTxId;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistLastTxId(db, tablePath.Base());
        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        context.SS->ClearDescribePathCaches(tablePath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, tablePath.Base()->PathId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTruncateColumnTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

} // anonymous namespace

ISubOperation::TPtr CreateTruncateColumnTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TTruncateColumnTable>(id, tx);
}

ISubOperation::TPtr CreateTruncateColumnTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TTruncateColumnTable>(id, state);
}

} // namespace NKikimr::NSchemeShard
