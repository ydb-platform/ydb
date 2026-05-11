#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TInitializeBuildIndex TConfigureParts"
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

        YDBLOG_CTX_INFO(context.Ctx, " HandleReply TEvProposeTransactionResult at tabletId# ",
            {"#_DebugHint()", DebugHint()},
            {"tabletId", ssId});
        YDBLOG_CTX_DEBUG(context.Ctx, " HandleReply TEvProposeTransactionResult message: ",
            {"#_DebugHint()", DebugHint()},
            {"message", ev->Get()->Record.ShortDebugString()});

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        YDBLOG_CTX_INFO(context.Ctx, " ProgressState at tabletId# ",
            {"#_DebugHint()", DebugHint()},
            {"tabletId", ssId});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxInitializeBuildIndex);

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

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

            YDBLOG_CTX_DEBUG(context.Ctx, " ProgressState SEND TFlatSchemeTransaction to datashard:  with create snapshot request operationId:  seqNo:  at schemeshard: ",
                {"#_DebugHint()", DebugHint()},
                {"datashard", datashardId},
                {"operationId", OperationId},
                {"seqNo", seqNo},
                {"schemeshard", ssId});

            auto event = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, tx.SerializeAsString(), context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shardIdx, event.Release());
        }

        txState->UpdateShardsInProgress();
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TInitializeBuildIndex TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
            TEvDataShard::TEvProposeTransactionResult::EventType,
        });
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        YDBLOG_CTX_INFO(context.Ctx, " HandleReply TEvSchemaChanged at tablet: ",
            {"#_DebugHint()", DebugHint()},
            {"tablet", ssId});
        YDBLOG_CTX_DEBUG(context.Ctx, " HandleReply TEvSchemaChanged triggered early, message: ",
            {"#_DebugHint()", DebugHint()},
            {"message", evRecord.ShortDebugString()});

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        YDBLOG_CTX_INFO(context.Ctx, " HandleReply TEvOperationPlan at tablet: , stepId: ",
            {"#_DebugHint()", DebugHint()},
            {"tablet", ssId},
            {"stepId", step});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxInitializeBuildIndex);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->SnapshotsStepIds[OperationId.GetTxId()] = step;
        context.SS->PersistSnapshotStepId(db, OperationId.GetTxId(), step);

        const TTableInfo::TPtr tableInfo = context.SS->Tables.at(txState->TargetPathId);
        tableInfo->AlterVersion += 1;
        context.SS->PersistTableAlterVersion(db, txState->TargetPathId, tableInfo);

        auto tablePath = context.SS->PathsById.at(txState->TargetPathId);
        context.SS->ClearDescribePathCaches(tablePath);
        context.OnComplete.PublishToSchemeBoard(OperationId, tablePath->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        YDBLOG_CTX_INFO(context.Ctx, " HandleReply ProgressState at tablet: ",
            {"#_DebugHint()", DebugHint()},
            {"tablet", ssId});

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
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TInitializeBuildIndex TCreateTxShards"
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

        YDBLOG_CTX_INFO(context.Ctx, " ProgressState, operation type: , at tablet# ",
            {"#_DebugHint()", DebugHint()},
            {"type", TTxState::TypeName(txState->TxType)},
            {"tablet", ssId});

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            YDBLOG_CTX_INFO(context.Ctx, " ProgressState SourceTablePartitioningChangedForModification, tx type: ",
                {"#_DebugHint()", DebugHint()},
                {"type", TTxState::TypeName(txState->TxType)});
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }


        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);

        return true;
    }
};

class TInitializeBuildIndex: public TSubOperation {
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

        auto schema = Transaction.GetInitiateBuildIndexMainTable();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& tableName = schema.GetTableName();

        YDBLOG_CTX_NOTICE(context.Ctx, "TInitializeBuildIndex Propose, path: /, opId: , at schemeshard: ",
            {"path", parentPathStr},
            {"#_tableName", tableName},
            {"opId", OperationId},
            {"schemeshard", ssId});

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
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotUnderDeleting()
                .NotUnderOperation()
                .IsCommonSensePath()
                .IsTable();

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

        pathEl->LastTxId = OperationId.GetTxId();
        pathEl->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxInitializeBuildIndex, tablePathId);
        txState.State = TTxState::CreateParts;

        TTableInfo::TPtr table = context.SS->Tables.at(tablePathId);
        for (auto splitTx: table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
        }

        context.SS->TablesWithSnapshots.emplace(tablePathId, OperationId.GetTxId());
        context.SS->SnapshotTables[OperationId.GetTxId()].insert(tablePathId);
        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Add(1);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        YDBLOG_CTX_NOTICE(context.Ctx, "TInitializeBuildIndex AbortPropose, opId: , at schemeshard: ",
            {"opId", OperationId},
            {"schemeshard", context.SS->TabletID()});
        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Sub(1);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDBLOG_CTX_NOTICE(context.Ctx, "TInitializeBuildIndex AbortUnsafe, opId: , forceDropId: , at schemeshard: ",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"schemeshard", context.SS->TabletID()});

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
