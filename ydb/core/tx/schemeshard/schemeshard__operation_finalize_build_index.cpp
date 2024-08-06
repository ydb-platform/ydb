#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TFinalizeBuildIndex TConfigureParts"
            << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message: " << ev->Get()->Record.ShortDebugString());

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << " at tabletId# " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxFinalizeBuildIndex);
        Y_ABORT_UNLESS(txState->BuildIndexId);

        TPathId pathId = txState->TargetPathId;
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);

        txState->ClearShardsInProgress();

        const TTxId snapshotTxId = context.SS->TablesWithSnapshots.at(pathId);
        const TStepId snapshotStepId = context.SS->SnapshotsStepIds.at(snapshotTxId);

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            TShardIdx shardIdx = txState->Shards[i].Idx;
            TTabletId datashardId = context.SS->ShardInfos[shardIdx].TabletID;

            auto seqNo = context.SS->StartRound(*txState);

            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            auto* op = tx.MutableFinalizeBuildIndex();
            PathIdFromPathId(pathId, op->MutablePathId());

            op->SetSnapshotTxId(ui64(snapshotTxId));
            op->SetSnapshotStep(ui64(snapshotStepId));
            op->SetTableSchemaVersion(table->AlterVersion+1);
            op->SetBuildIndexId(ui64(txState->BuildIndexId));
            if (txState->BuildIndexOutcome) {
                op->MutableOutcome()->CopyFrom(*txState->BuildIndexOutcome);
            }

            context.SS->FillSeqNo(tx, seqNo);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " SEND TFlatSchemeTransaction to datashard: " << datashardId
                                    << " with drop snapshot request"
                                    << " operationId: " << OperationId
                                    << " seqNo: " << seqNo
                                    << " at schemeshard: " << ssId);

            auto event = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, tx.SerializeAsString(), context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shardIdx, event.Release());
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
            << "TFinalizeBuildIndex TPropose"
            << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemaChanged"
                               << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvSchemaChanged"
                                << " triggered early"
                                << ", message: " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << " at tablet: " << ssId
                               << ", stepId: " << step);

        TTxState* txState = context.SS->FindTx(OperationId);

        Y_ABORT_UNLESS(txState->TxType == TTxState::TxFinalizeBuildIndex);

        NIceDb::TNiceDb db(context.GetDB());
        TPathId tableId = txState->TargetPathId;
        TTxId snapshotTxId = context.SS->TablesWithSnapshots.at(tableId);
        context.SS->SnapshotsStepIds.erase(snapshotTxId);
        context.SS->SnapshotTables.at(snapshotTxId).erase(tableId);
        if (context.SS->SnapshotTables.at(snapshotTxId).empty()) {
            context.SS->SnapshotTables.erase(snapshotTxId);
        }
        context.SS->TablesWithSnapshots.erase(tableId);

        context.SS->PersistDropSnapshot(db, snapshotTxId, tableId);

        const TTableInfo::TPtr tableInfo = context.SS->Tables.at(txState->TargetPathId);
        tableInfo->AlterVersion += 1;

        for (auto& [cId, cInfo]: tableInfo->Columns) {
            if (cInfo.IsDropped()) {
                continue;
            }

            if (cInfo.IsBuildInProgress) {
                LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply ProgressState"
                                    << " at tablet: " << ssId
                                    << " terminating build column process at column "
                                    << cInfo.Name);

                cInfo.IsBuildInProgress = false;
                context.SS->PersistTableFinishColumnBuilding(db, txState->TargetPathId, tableInfo, cId);
            } else if (cInfo.IsCheckingNotNullInProgress) {
                const auto& outcome = txState->BuildIndexOutcome;
                bool isError = (outcome && outcome->HasCancel() && outcome->GetCancel().HasCancelByCheckingNotNull() && outcome->GetCancel().GetCancelByCheckingNotNull());
                LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply ProgressState"
                                    << " at tablet: " << ssId
                                    << " terminating checking not null process at column "
                                    << cInfo.Name
                                    << (isError ? "null value was found" : "null values were not found"));

                cInfo.IsCheckingNotNullInProgress = false;
                cInfo.NotNull = !isError;
                context.SS->PersistTableFinishCheckingNotNull(db, txState->TargetPathId, tableInfo, cId);
            }
        }

        context.SS->PersistTableAlterVersion(db, txState->TargetPathId, tableInfo);

        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Sub(1);

        auto tablePath = context.SS->PathsById.at(tableId);
        context.SS->ClearDescribePathCaches(tablePath);
        context.OnComplete.PublishToSchemeBoard(OperationId, tableId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply ProgressState"
                               << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxFinalizeBuildIndex);

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

class TCreateTxShards: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TFinalizeBuildIndex TCreateTxShards"
            << " operationId: " << OperationId;
    }

public:
    TCreateTxShards(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " ProgressState"
                                   << " SourceTablePartitioningChangedForModification"
                                   << ", tx type: " << TTxState::TypeName(txState->TxType));
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }


        NIceDb::TNiceDb db(context.GetDB());

        context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);

        return true;
    }
};

class TFinalizeBuildIndex: public TSubOperation {
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
        case TTxState::CreateParts:
            return MakeHolder<TCreateTxShards>(OperationId);
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

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        auto finalizeMainTable = Transaction.GetFinalizeBuildIndexMainTable();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& tableName = finalizeMainTable.GetTableName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TFinalizeBuildIndex Propose"
                         << ", path: " << parentPathStr << "/" << tableName
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(tableName);

        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath parent = path.Parent();
        {
            TPath::TChecker checks = parent.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }


        TString errStr;

        TPathElement::TPtr pathEl = path.Base();
        TPathId tablePathId = pathEl->PathId;
        result->SetPathId(tablePathId.LocalPathId);

        if (!context.SS->CheckLocks(path.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        if (!context.SS->TablesWithSnapshots.contains(tablePathId)) {
            errStr = TStringBuilder()
                << "No snapshot presents for table"
                << ", tableId:" << tablePathId
                << ", txId: " << OperationId.GetTxId();
            result->SetError(TEvSchemeShard::EStatus::StatusPathDoesNotExist, errStr);
            return result;
        }

        TTxId snapshotTxId = context.SS->TablesWithSnapshots.at(tablePathId);
        if (TTxId(finalizeMainTable.GetSnapshotTxId()) != snapshotTxId) {
            errStr = TStringBuilder()
                << "No snapshot with requested txId presents for table"
                << ", tableId:" << tablePathId
                << ", txId: " << OperationId.GetTxId()
                << ", requested snapshotTxId: " << finalizeMainTable.GetSnapshotTxId()
                << ", snapshotTxId: " << snapshotTxId
                << ", snapshotStepId: " << context.SS->SnapshotsStepIds.at(snapshotTxId);
            result->SetError(TEvSchemeShard::EStatus::StatusPathDoesNotExist, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        pathEl->LastTxId = OperationId.GetTxId();
        pathEl->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxFinalizeBuildIndex, tablePathId);
        txState.BuildIndexId = TTxId(finalizeMainTable.GetBuildIndexId());

        if (finalizeMainTable.HasOutcome()) {
            txState.BuildIndexOutcome = std::make_shared<NKikimrSchemeOp::TBuildIndexOutcome>();
            txState.BuildIndexOutcome->CopyFrom(finalizeMainTable.GetOutcome());
        }

        context.SS->PersistTxState(db, OperationId);

        TTableInfo::TPtr table = context.SS->Tables.at(tablePathId);
        Y_ABORT_UNLESS(table->GetSplitOpsInFlight().empty());

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TFinalizeBuildIndex");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TFinalizeBuildIndex AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TFinalizeBuildIndex>(id, tx);
}

ISubOperation::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TFinalizeBuildIndex>(id, state);
}

}
