#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

// Consistently publish shadow data and create snapshot of a table in one transaction
// Used for example in unique index build to validate index state while allowing online changes

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TPrepareIndexValidation TConfigureParts"
            << " operationId# " << OperationId;
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxPrepareIndexValidation);

        TPathId pathId = txState->TargetPathId;
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);

        txState->ClearShardsInProgress();

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            TShardIdx shardIdx = txState->Shards[i].Idx;
            TTabletId datashardId = context.SS->ShardInfos[shardIdx].TabletID;

            auto seqNo = context.SS->StartRound(*txState);

            NKikimrTxDataShard::TFlatSchemeTransaction tx;

            auto* createSnapshot = tx.MutablePrepareIndexValidation();
            pathId.ToProto(createSnapshot->MutableIndexId());
            createSnapshot->SetSnapshotName("Snapshot0");
            createSnapshot->SetTableSchemaVersion(table->AlterVersion+1);

            context.SS->FillSeqNo(tx, seqNo);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " SEND TFlatSchemeTransaction to datashard: " << datashardId
                                    << " with publish shadow data request"
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
            << "TPrepareIndexValidation TPropose"
            << " operationId# " << OperationId;
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxPrepareIndexValidation);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->SnapshotsStepIds[OperationId.GetTxId()] = step;
        context.SS->PersistSnapshotStepId(db, OperationId.GetTxId(), step);

        const TTableInfo::TPtr tableInfo = context.SS->Tables.at(txState->TargetPathId);
        tableInfo->AlterVersion += 1;
        tableInfo->MutablePartitionConfig().SetShadowData(false);
        tableInfo->MutablePartitionConfig().MutableCompactionPolicy()->SetKeepEraseMarkers(false);
        context.SS->PersistTableAlterVersion(db, txState->TargetPathId, tableInfo);

        auto tablePath = context.SS->PathsById.at(txState->TargetPathId);
        context.SS->ClearDescribePathCaches(tablePath);
        context.OnComplete.PublishToSchemeBoard(OperationId, txState->TargetPathId);

        if (context.SS->Indexes.contains(tablePath->ParentPathId)) {
            auto parentDir = context.SS->PathsById.at(tablePath->ParentPathId);
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
            context.SS->ClearDescribePathCaches(parentDir);
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
        }

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxPrepareIndexValidation);

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
            << "TPrepareIndexValidation TCreateTxShards"
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
                               << ", at tablet# " << ssId);

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

class TPrepareIndexValidation: public TSubOperation {
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

        auto& publish = Transaction.GetPrepareIndexValidation();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& tableName = publish.GetTableName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TPrepareIndexValidation Propose"
                         << ", path: " << parentPathStr << "/" << tableName
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(tableName);

        if (!Transaction.GetInternal()) {
            result->SetError(NKikimrScheme::EStatus::StatusNameConflict, "PrepareIndexValidation is an internal operation");
            return result;
        }

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
                .NotUnderOperation()
                .IsInsideTableIndexPath();

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

        {
            Y_ABORT_UNLESS(context.SS->Tables.contains(tablePathId));
            TTableInfo::TPtr tableInfo = context.SS->Tables.at(tablePathId);
            const NKikimrSchemeOp::TPartitionConfig &srcPartitionConfig = tableInfo->PartitionConfig();
            if (!srcPartitionConfig.GetShadowData()) {
                errStr = TStringBuilder()
                    << "Shadow data is not enabled for table"
                    << ", tableId:" << tablePathId
                    << ", txId: " << OperationId.GetTxId();
                result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
                return result;
            }
        }

        if (context.SS->TablesWithSnapshots.contains(tablePathId)) {
            errStr = TStringBuilder()
                << "Snapshots already present for table"
                << ", tableId:" << tablePathId
                << ", txId: " << OperationId.GetTxId();
            result->SetError(TEvSchemeShard::EStatus::StatusAlreadyExists, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        pathEl->LastTxId = OperationId.GetTxId();
        pathEl->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        context.SS->CreateTx(OperationId, TTxState::TxPrepareIndexValidation, tablePathId);

        context.SS->PersistTxState(db, OperationId);

        TTableInfo::TPtr table = context.SS->Tables.at(tablePathId);
        Y_ABORT_UNLESS(table->GetSplitOpsInFlight().empty());

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.DbChanges.PersistTableSnapshot(tablePathId, OperationId.GetTxId());
        context.SS->TablesWithSnapshots.emplace(tablePathId, OperationId.GetTxId());
        context.SS->SnapshotTables[OperationId.GetTxId()].insert(tablePathId);
        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Add(1);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TPrepareIndexValidation");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TPrepareIndexValidation AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreatePrepareIndexValidation(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TPrepareIndexValidation>(id, tx);
}

ISubOperation::TPtr CreatePrepareIndexValidation(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TPrepareIndexValidation>(id, state);
}

}
