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

NKikimrSchemeOp::TColumnTableSchema BumpedColumnSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    NKikimrSchemeOp::TColumnTableSchema bumped = schema;
    const ui64 nextVersion = bumped.GetVersion() + 1;
    bumped.SetVersion(nextVersion);
    bumped.SetMinWriteSchemaVersion(nextVersion);
    return bumped;
}

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

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxInitializeBuildIndex);

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        if (context.SS->ColumnTables.contains(pathId)) {
            auto columnInfo = context.SS->ColumnTables.GetVerified(pathId);
            Y_ABORT_UNLESS(columnInfo->IsStandalone());
            const auto bumpedSchema = BumpedColumnSchema(columnInfo->Description.GetSchema());

            txState->ClearShardsInProgress();
            auto seqNo = context.SS->StartRound(*txState);
            for (ui32 i = 0; i < txState->Shards.size(); ++i) {
                const TShardIdx shardIdx = txState->Shards[i].Idx;
                const TTabletId tabletId = context.SS->ShardInfos[shardIdx].TabletID;

                NKikimrTxColumnShard::TSchemaTxBody schemaTx;
                schemaTx.MutableSeqNo()->SetGeneration(seqNo.Generation);
                schemaTx.MutableSeqNo()->SetRound(seqNo.Round);
                auto* alter = schemaTx.MutableAlterTable();
                alter->SetPathId(pathId.LocalPathId);
                *alter->MutableSchema() = bumpedSchema;

                auto event = context.SS->MakeColumnShardProposal(
                    pathId, OperationId, seqNo, schemaTx.SerializeAsString(), context.Ctx);
                context.OnComplete.BindMsgToPipe(OperationId, tabletId, shardIdx, event.Release());
            }
            txState->UpdateShardsInProgress();
            return false;
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        const TTableInfo::TPtr tableInfo = context.SS->Tables.at(pathId);

        NKikimrTxDataShard::TFlatSchemeTransaction txTemplate;
        auto initiate = txTemplate.MutableInitiateBuildIndex();
        pathId.ToProto(initiate->MutablePathId());
        initiate->SetSnapshotName("Snapshot0");
        initiate->SetTableSchemaVersion(tableInfo->AlterVersion + 1);

        bool found = false;
        for (const auto& [childName, childPathId] : path->GetChildren()) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);

            if (!childPath->IsTableIndex() || childPath->Dropped() || childPath->PlannedToDrop()) {
                continue;
            }

            Y_ABORT_UNLESS(context.SS->Indexes.contains(childPathId));
            auto index = context.SS->Indexes.at(childPathId);

            if (index->State != TTableIndexInfo::EState::EIndexStateInvalid) {
                // doesn't exist yet so its state is invalid
                continue;
            }

            Y_VERIFY_S(!found, "Too many indexes are planned to create"
                << ": found# " << TPathId(initiate->GetIndexDescription().GetPathOwnerId(),
                    initiate->GetIndexDescription().GetLocalPathId())
                << ", another# " << childPathId);
            found = true;

            Y_ABORT_UNLESS(index->AlterData);
            context.SS->DescribeTableIndex(childPathId, childName, index->AlterData, false, false,
                *initiate->MutableIndexDescription()
            );
        }

        txState->ClearShardsInProgress();

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            TShardIdx shardIdx = txState->Shards[i].Idx;
            TTabletId datashardId = context.SS->ShardInfos[shardIdx].TabletID;

            auto seqNo = context.SS->StartRound(*txState);

            NKikimrTxDataShard::TFlatSchemeTransaction tx(txTemplate);
            context.SS->FillSeqNo(tx, seqNo);

            YDB_LOG_DEBUG_CTX(context.Ctx, "sending TFlatSchemeTransaction to datashard with create snapshot request",
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
            TEvColumnShard::TEvProposeTransactionResult::EventType,
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxInitializeBuildIndex);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->SnapshotsStepIds[OperationId.GetTxId()] = step;
        context.SS->PersistSnapshotStepId(db, OperationId.GetTxId(), step);

        if (context.SS->ColumnTables.contains(txState->TargetPathId)) {
            const auto columnInfo = context.SS->ColumnTables.GetVerified(txState->TargetPathId);
            *columnInfo->Description.MutableSchema() = BumpedColumnSchema(columnInfo->Description.GetSchema());
            context.SS->PersistColumnTable(db, txState->TargetPathId, *columnInfo);
        } else {
            const TTableInfo::TPtr tableInfo = context.SS->Tables.at(txState->TargetPathId);
            tableInfo->AlterVersion += 1;
            context.SS->PersistTableAlterVersion(db, txState->TargetPathId, tableInfo);
        }

        auto tablePath = context.SS->PathsById.at(txState->TargetPathId);
        context.SS->ClearDescribePathCaches(tablePath);
        context.OnComplete.PublishToSchemeBoard(OperationId, tablePath->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "");

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxInitializeBuildIndex);

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

        if (context.SS->Tables.contains(txState->TargetPathId) &&
            NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
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

class TInitializeBuildIndex: public TSubOperation {
    virtual const char* Name() const override final { return "TInitializeBuildIndex"; }

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

        auto schema = Transaction.GetInitiateBuildIndexMainTable();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& tableName = schema.GetTableName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "",
            {"path", TStringBuilder() << parentPathStr << "/" << tableName},
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
                .IsLikeDirectory();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        NSchemeShard::TPath dstPath = parentPath.Child(tableName);
        const bool columnTable = dstPath.IsResolved() && dstPath->IsColumnTable();
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotUnderDeleting()
                .NotUnderOperation()
                .IsCommonSensePath();
            if (columnTable) {
                checks.IsColumnTable();
            } else {
                checks.IsTable();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TPathElement::TPtr pathEl = dstPath.Base();
        TPathId tablePathId = pathEl->PathId;
        result->SetPathId(tablePathId.LocalPathId);

        TString errStr;
        if (!context.SS->CheckLocks(dstPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
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

        context.DbChanges.PersistTableSnapshot(tablePathId, OperationId.GetTxId());
        context.DbChanges.PersistTxState(OperationId);

        if (columnTable) {
            const auto columnInfo = context.SS->ColumnTables.GetVerified(tablePathId);
            if (!columnInfo->IsStandalone()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "Building a synchronous index is supported for standalone column tables");
                return result;
            }
            if (columnInfo->BuildOwnedColumnShardsVerified().empty()) {
                result->SetError(NKikimrScheme::StatusSchemeError, "Column table has no shards to snapshot");
                return result;
            }
        }

        pathEl->LastTxId = OperationId.GetTxId();
        pathEl->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxInitializeBuildIndex, tablePathId);
        txState.State = TTxState::CreateParts;

        if (columnTable) {
            const auto columnInfo = context.SS->ColumnTables.GetVerified(tablePathId);
            for (const auto& shardIdx : columnInfo->BuildOwnedColumnShardsVerified()) {
                const auto& shardInfo = context.SS->ShardInfos.at(shardIdx);
                txState.Shards.emplace_back(shardIdx, shardInfo.TabletType, TTxState::ConfigureParts);
            }
        } else {
            TTableInfo::TPtr table = context.SS->Tables.at(tablePathId);
            for (auto splitTx: table->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
            }
        }

        context.SS->TablesWithSnapshots.emplace(tablePathId, OperationId.GetTxId());
        context.SS->SnapshotTables[OperationId.GetTxId()].insert(tablePathId);
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
        YDB_LOG_NOTICE_CTX(context.Ctx, "TInitializeBuildIndex AbortUnsafe",
            {"operationId", OperationId},
            {"forceDropId", forceDropTxId},
            {"schemeshard", context.SS->TabletID()},
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateInitializeBuildIndexMainTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TInitializeBuildIndex>(id, tx);
}

ISubOperation::TPtr CreateInitializeBuildIndexMainTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TInitializeBuildIndex>(id, state);
}

}

#undef YDB_LOG_THIS_FILE_COMPONENT
