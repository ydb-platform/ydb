#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

// Takes a snapshot of the main table and switches the vector index to WriteOnly.
//
// The snapshot is created by the datashard via the regular InitiateBuildIndex flat
// transaction, so no datashard-side scheme operation is needed. The index state is
// switched at the operation's plan step, i.e. at exactly the same step as the
// snapshot, which makes the WriteOnly updates and the delta scan cover disjoint
// ranges of the main table history.
class TConfigureParts: public TSubOperationState {
    virtual const char* Name() const override final { return "TConfigureParts"; }

private:
    const TOperationId OperationId;

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages({TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "");
        YDB_LOG_DEBUG_CTX(context.Ctx, "",
            {"message", ev->Get()->Record.ShortDebugString()},
        );

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "");

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxPrepareVectorIndex);

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));

        NKikimrTxDataShard::TFlatSchemeTransaction txTemplate;
        auto snapshot = txTemplate.MutableCreatePersistentSnapshot();
        snapshot->SetOwnerId(pathId.OwnerId);
        snapshot->SetPathId(pathId.LocalPathId);
        snapshot->SetName("VectorIndexDeltaSnapshot");

        txState->ClearShardsInProgress();

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            TShardIdx shardIdx = txState->Shards[i].Idx;
            TTabletId datashardId = context.SS->ShardInfos[shardIdx].TabletID;

            auto seqNo = context.SS->StartRound(*txState);

            NKikimrTxDataShard::TFlatSchemeTransaction tx(txTemplate);
            context.SS->FillSeqNo(tx, seqNo);

            YDB_LOG_DEBUG_CTX(context.Ctx, "sending TFlatSchemeTransaction to datashard with prepare vector index request",
                {"datashard", datashardId},
                {"seqNo", seqNo},
            );

            auto event = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, tx.SerializeAsString(), context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shardIdx, event.Release());
        }

        txState->UpdateShardsInProgress();
        return false;
    }
};

class TPropose: public TSubOperationState {
    virtual const char* Name() const override final { return "TPropose"; }

private:
    const TOperationId OperationId;

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages({
            TEvHive::TEvCreateTabletReply::EventType,
            TEvDataShard::TEvProposeTransactionResult::EventType,
        });
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        const auto& evRecord = ev->Get()->Record;

        YDB_LOG_INFO_CTX(context.Ctx, "");
        YDB_LOG_DEBUG_CTX(context.Ctx, "triggered early",
            {"message", evRecord.ShortDebugString()},
        );

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);

        YDB_LOG_INFO_CTX(context.Ctx, "",
            {"step", step},
        );

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxPrepareVectorIndex);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->SnapshotsStepIds[OperationId.GetTxId()] = step;
        context.SS->PersistSnapshotStepId(db, OperationId.GetTxId(), step);

        auto tablePath = context.SS->PathsById.at(txState->TargetPathId);
        // The index metadata is a part of the table description, so bump the table
        // directory version to make SchemeCache refetch it after the state change.
        ++tablePath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, tablePath);
        context.SS->ClearDescribePathCaches(tablePath);
        context.OnComplete.PublishToSchemeBoard(OperationId, tablePath->PathId);

        // Apply the prepared index state at the snapshot step. The index path id is
        // kept in BuildIndexId (persisted with the tx state) to select exactly the
        // index this operation was created for.
        const auto indexLocalId = ui64(txState->BuildIndexId);
        for (const auto& [childName, childPathId] : tablePath->GetChildren()) {
            Y_UNUSED(childName);
            if (ui64(childPathId.LocalPathId) != indexLocalId) {
                continue;
            }
            if (!context.SS->PathsById.contains(childPathId)) {
                continue;
            }
            auto childPath = context.SS->PathsById.at(childPathId);
            if (!childPath->IsTableIndex() || childPath->Dropped()) {
                continue;
            }
            if (!context.SS->Indexes.contains(childPathId)) {
                continue;
            }
            auto index = context.SS->Indexes.at(childPathId);
            if (!index->AlterData || index->AlterData->State != TTableIndexInfo::EState::EIndexStateWriteOnly) {
                continue;
            }
            context.SS->PersistTableIndex(db, childPathId);
            context.SS->Indexes.Set(childPathId, index->AlterData);
            context.SS->ClearDescribePathCaches(childPath);
            context.OnComplete.PublishToSchemeBoard(OperationId, childPathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "");

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxPrepareVectorIndex);

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
    virtual const char* Name() const override final { return "TCreateTxShards"; }

private:
    const TOperationId OperationId;

public:
    TCreateTxShards(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages({});
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        YDB_LOG_INFO_CTX(context.Ctx, "",
            {"txType", TTxState::TypeName(txState->TxType)},
        );

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            YDB_LOG_INFO_CTX(context.Ctx, "SourceTablePartitioningChangedForModification",
                {"txType", TTxState::TypeName(txState->TxType)},
            );
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);

        return true;
    }
};

class TPrepareVectorIndex: public TSubOperation {
    virtual const char* Name() const override final { return "TPrepareVectorIndex"; }

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

    THolder<TProposeResponse> Propose(const TString&, TProposeContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        auto schema = Transaction.GetPrepareVectorIndex();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& indexName = schema.GetTableName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "",
            {"path", TStringBuilder() << parentPathStr << "/" << indexName},
        );

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsTable();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        NSchemeShard::TPath indexPath = parentPath.Child(indexName);
        {
            NSchemeShard::TPath::TChecker checks = indexPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotUnderDeleting()
                .IsTableIndex();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (indexPath.IsResolved()) {
                    result->SetPathId(ui64(indexPath.Base()->PathId.LocalPathId));
                }
                return result;
            }
        }

        TPathElement::TPtr pathEl = parentPath.Base();
        TPathId tablePathId = pathEl->PathId;
        result->SetPathId(tablePathId.LocalPathId);

        TString errStr;
        if (!context.SS->CheckLocks(tablePathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto* index = context.SS->Indexes.FindPtr(indexPath.Base()->PathId);
        if (!index) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Unknown index");
            return result;
        }
        if ((*index)->State != TTableIndexInfo::EState::EIndexStateInvalid &&
            (*index)->State != TTableIndexInfo::EState::EIndexStateWriteOnly)
        {
            result->SetError(NKikimrScheme::StatusPreconditionFailed,
                TStringBuilder() << "Index must be Invalid or WriteOnly, but is "
                    << NKikimrSchemeOp::EIndexState_Name((*index)->State));
            return result;
        }

        NKikimrScheme::EStatus status;
        if (!context.SS->CanCreateSnapshot(tablePathId, OperationId.GetTxId(), status, errStr)) {
            result->SetError(status, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, tablePathId);
        context.MemChanges.GrabNewTableSnapshot(context.SS, tablePathId, OperationId.GetTxId());
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabIndex(context.SS, indexPath.Base()->PathId);

        context.DbChanges.PersistTableSnapshot(tablePathId, OperationId.GetTxId());
        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistAlterIndex(indexPath.Base()->PathId);

        pathEl->LastTxId = OperationId.GetTxId();
        pathEl->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        {
            TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(indexPath.Base()->PathId);
            TTableIndexInfo::TPtr newIndexData = indexData->CreateNextVersion();
            Y_ABORT_UNLESS(newIndexData);
            newIndexData->State = TTableIndexInfo::EState::EIndexStateWriteOnly;
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxPrepareVectorIndex, tablePathId);
        txState.State = TTxState::CreateParts;
        // Remember the index this operation was created for. BuildIndexId is not
        // used by PrepareVectorIndex otherwise and is persisted with the tx state.
        txState.BuildIndexId = TTxId(ui64(indexPath.Base()->PathId.LocalPathId));

        TTableInfo::TPtr table = context.SS->Tables.at(tablePathId);
        for (auto splitTx: table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
        }

        context.SS->AddTableSnapshot(tablePathId, OperationId.GetTxId());
        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Add(1);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TProposeContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "");
        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Sub(1);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TPrepareVectorIndex AbortUnsafe",
            {"operationId", OperationId},
            {"forceDropId", forceDropTxId},
            {"schemeshard", context.SS->TabletID()},
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreatePrepareVectorIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TPrepareVectorIndex>(id, tx);
}

ISubOperation::TPtr CreatePrepareVectorIndex(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TPrepareVectorIndex>(id, state);
}

}

#undef YDB_LOG_THIS_FILE_COMPONENT
