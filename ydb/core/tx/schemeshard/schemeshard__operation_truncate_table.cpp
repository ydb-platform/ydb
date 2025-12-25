#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/subdomain.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TConfigureParts"
            << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message# " << ev->Get()->Record.ShortDebugString());

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable);

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " UpdatePartitioningForTableModification");
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }

        txState->ClearShardsInProgress();

        TPath tablePath = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));

        Y_ABORT_UNLESS(txState->Shards.size());
        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            auto idx = txState->Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            TPathId targetPathId = txState->TargetPathId;

            TString txBody;
            {
                auto seqNo = context.SS->StartRound(*txState);

                NKikimrTxDataShard::TFlatSchemeTransaction tx;
                context.SS->FillSeqNo(tx, seqNo);
                auto truncateTable = tx.MutableTruncateTable();
                targetPathId.ToProto(truncateTable->MutablePathId());
                Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
            }

            auto event = context.SS->MakeDataShardProposal(targetPathId, OperationId, txBody, context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, event.Release());
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

       LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint()
                     << " HandleReply TEvSchemaChanged"
                     << " at tablet: " << ssId);
 
        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", stepId: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        const auto path = TPath::Init(txState->TargetPathId, context.SS);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable);

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

class TTruncateTable: public TSubOperation {
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
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        // Begin check conditions
        const TTabletId ssId = context.SS->SelfTabletId();

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        if (!AppData()->FeatureFlags.GetEnableTruncateTable()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "TRUNCATE TABLE statement is not supported");
            return result;
        }

        const auto& op = Transaction.GetTruncateTable();
        const auto stringTablePath = NKikimr::JoinPath({Transaction.GetWorkingDir(), op.GetTableName()});
        TPath tablePath = TPath::Resolve(stringTablePath, context.SS);
        {
            TPath::TChecker checks = tablePath.Check();
            checks
                .IsResolved()
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .NotDeleted()
                .NotBackupTable()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation()
                .IsTable();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }

            // In the initial implementation, we forbid the table to have cdc streams and asynchronous indexes.
            // It will be fixed in the near future.
            for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
                TPath srcChildPath = tablePath.Child(childName);

                if (srcChildPath.IsDeleted()) {
                    continue;
                }

                if (srcChildPath->IsCdcStream()) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed,
                        "Cannot truncate table with CDC streams");
                    return result;
                }
                
                if (srcChildPath.IsTableIndex(NKikimrSchemeOp::EIndexTypeGlobalAsync)) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed,
                        "Cannot truncate table with async indexes");
                    return result;
                }

            }
        }

        TString errStr;
        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }
        // End check conditions

        // Begin create local transaction
        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);
        Y_ABORT_UNLESS(table->GetPartitions().size());

        {
            tablePath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
            tablePath.Base()->LastTxId = OperationId.GetTxId();

            NIceDb::TNiceDb db(context.GetDB());

            TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxTruncateTable, tablePath.Base()->PathId);
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);

            context.SS->PersistPath(db, tablePath.Base()->PathId);

            auto persistShard = [&](const TTableShardInfo& shard) {
                auto shardIdx = shard.ShardIdx;
                TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];
                txState.Shards.emplace_back(shardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
                shardInfo.CurrentTxId = OperationId.GetTxId();
                context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
            };

            for (const auto& shard : table->GetPartitions()) {
                persistShard(shard);
            }

            context.SS->PersistTxState(db, OperationId);

            IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, tablePath, context.SS, context.OnComplete);

            for (auto splitTx : table->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
            }

            context.OnComplete.ActivateTx(OperationId);
            SetState(NextState());
            Y_ABORT_UNLESS(txState.Shards.size());
        }
        // End create local transaction
        // (but this does not mean that the local transaction has been executed or started)

        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTruncateTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

void DfsOnTableChildrenTree(TOperationId opId, const TTxTransaction& tx, TOperationContext& context, const TPathId& currentPathId, TVector<ISubOperation::TPtr>& result) {
    TPath vertexPath = TPath::Init(currentPathId, context.SS);

    bool isTable = vertexPath->IsTable();
    bool isIndexParent = vertexPath.Base()->IsTableIndex();

    if (isTable) {
        const auto workingDir = TString(NKikimr::ExtractParent(vertexPath.PathString()));
        const auto tableName = TString(NKikimr::ExtractBase(vertexPath.PathString()));

        {
            auto modifycheme = TransactionTemplate(workingDir, NKikimrSchemeOp::EOperationType::ESchemeOpTruncateTable);
            modifycheme.SetInternal(tx.GetInternal());
            auto truncateTable = modifycheme.MutableTruncateTable();
            truncateTable->SetTableName(tableName);
            result.push_back(CreateTruncateTable(NextPartId(opId, result), modifycheme));
        }
    }

    if (isTable || isIndexParent) {
        for (const auto& [childName, childPathId] : vertexPath.Base()->GetChildren()) {
            Y_UNUSED(childName);
            DfsOnTableChildrenTree(opId, tx, context, childPathId, result);
        }
    }
}

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateTruncateTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TTruncateTable>(id, tx);
}

ISubOperation::TPtr CreateTruncateTable(TOperationId opId, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TTruncateTable>(opId, state);
}

TVector<ISubOperation::TPtr> CreateConsistentTruncateTable(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    const auto& op = tx.GetTruncateTable();
    const auto stringMainTablePath = NKikimr::JoinPath({tx.GetWorkingDir(), op.GetTableName()});
    TPath mainTablePath = TPath::Resolve(stringMainTablePath, context.SS);

    TVector<ISubOperation::TPtr> result = {};

    // 'IsResolved' check is necessary because the dfs implementation expects that the vertex coming into the function exists.
    // If we do not do this check, then the Y_VERIFY may fire if the path does not exist.

    // Since one check was added, it makes sense to add all the checks at once, 
    // so as not to generate a large set of sub-operations in case even the main table is "wrong".
    TPath::TChecker checks = mainTablePath.Check();
    checks
        .IsResolved()
        .NotEmpty()
        .NotUnderDomainUpgrade()
        .NotDeleted()
        .NotBackupTable()
        .NotUnderOperation()
        .IsTable();

    if (!checks) {
        // We cannot return empty vector of suboperations because of technical features of schemeshard's work
        result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        return result;
    }

    DfsOnTableChildrenTree(opId, tx, context, mainTablePath.Base()->PathId, result);

    return result;
}

}

