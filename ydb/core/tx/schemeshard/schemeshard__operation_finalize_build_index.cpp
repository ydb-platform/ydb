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
        Y_VERIFY(txState->TxType == TTxState::TxFinalizeBuildIndex);
        Y_VERIFY(txState->BuildIndexId);

        TPathId pathId = txState->TargetPathId;
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);

        txState->ClearShardsInProgress();

        const TTxId snapshotTxId = context.SS->TablesWithSnaphots.at(pathId);
        const TStepId snapshotStepid = context.SS->SnapshotsStepIds.at(snapshotTxId);

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            TShardIdx shardIdx = txState->Shards[i].Idx;
            TTabletId datashardId = context.SS->ShardInfos[shardIdx].TabletID;

            auto seqNo = context.SS->StartRound(*txState);

            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            auto* op = tx.MutableFinalizeBuildIndex();
            PathIdFromPathId(pathId, op->MutablePathId());

            op->SetSnapshotTxId(ui64(snapshotTxId));
            op->SetSnapshotStep(ui64(snapshotStepid));
            op->SetTableSchemaVersion(table->AlterVersion+1);
            op->SetBuildIndexId(ui64(txState->BuildIndexId));
            if (txState->BuildIndexOutcome) {
                op->MutableOutcome()->CopyFrom(*txState->BuildIndexOutcome);
            }

            context.SS->FillSeqNo(tx, seqNo);

            TString txBody;
            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " SEND TFlatSchemeTransaction to datashard: " << datashardId
                                    << " with drop snapshot request"
                                    << " operationId: " << OperationId
                                    << " seqNo: " << seqNo
                                    << " at schemeshard: " << ssId);


            THolder<TEvDataShard::TEvProposeTransaction> event =
                THolder(new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_SCHEME,
                                                        context.SS->TabletID(),
                                                        context.Ctx.SelfID,
                                                        ui64(OperationId.GetTxId()),
                                                        txBody,
                                                        context.SS->SelectProcessingPrarams(txState->TargetPathId)));

            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shardIdx,  event.Release());
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
        Y_VERIFY(txState->TxType == TTxState::TxFinalizeBuildIndex);

        NIceDb::TNiceDb db(context.GetDB());
        TPathId tableId = txState->TargetPathId;
        TTxId snapshotTxId = context.SS->TablesWithSnaphots.at(tableId);
        context.SS->SnapshotsStepIds.erase(snapshotTxId);
        context.SS->SnapshotTables.at(snapshotTxId).erase(tableId);
        if (context.SS->SnapshotTables.at(snapshotTxId).empty()) {
            context.SS->SnapshotTables.erase(snapshotTxId);
        }
        context.SS->TablesWithSnaphots.erase(tableId);

        context.SS->PersistDropSnapshot(db, snapshotTxId, tableId);

        const TTableInfo::TPtr tableInfo = context.SS->Tables.at(txState->TargetPathId);
        tableInfo->AlterVersion += 1;
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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxFinalizeBuildIndex);

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
        Y_VERIFY(txState);

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
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
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
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return THolder(new TCreateTxShards(OperationId));
        case TTxState::ConfigureParts:
            return THolder(new TConfigureParts(OperationId));
        case TTxState::Propose:
            return THolder(new TPropose(OperationId));
        case TTxState::ProposedWaitParts:
            return THolder(new NTableState::TProposedWaitParts(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TFinalizeBuildIndex(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
          , Transaction(tx)
    {
    }

    TFinalizeBuildIndex(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
          , State(state)
    {
        SetState(SelectStateFunc(state));
    }

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

        if (!context.SS->TablesWithSnaphots.contains(tablePathId)) {
            errStr = TStringBuilder()
                << "No snapshot presents for table"
                << ", tableId:" << tablePathId
                << ", txId: " << OperationId.GetTxId();
            result->SetError(TEvSchemeShard::EStatus::StatusPathDoesNotExist, errStr);
            return result;
        }

        TTxId shapshotTxId = context.SS->TablesWithSnaphots.at(tablePathId);
        if (TTxId(finalizeMainTable.GetSnapshotTxId()) != shapshotTxId) {
            errStr = TStringBuilder()
                << "No snapshot with requested txId presents for table"
                << ", tableId:" << tablePathId
                << ", txId: " << OperationId.GetTxId()
                << ", requested snapshotTxId: " << finalizeMainTable.GetSnapshotTxId()
                << ", snapshotTxId: " << shapshotTxId
                << ", snapshotStepId: " << context.SS->SnapshotsStepIds.at(shapshotTxId);
            result->SetError(TEvSchemeShard::EStatus::StatusPathDoesNotExist, errStr);
            return result;
        }
        if (!context.SS->CheckInFlightLimit(TTxState::TxFinalizeBuildIndex, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
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
        Y_VERIFY(table->GetSplitOpsInFlight().empty());

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TFinalizeBuildIndex");
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

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, const TTxTransaction& tx) {
    return new TFinalizeBuildIndex(id, tx);
}

ISubOperationBase::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TFinalizeBuildIndex(id, state);
}

}
}
