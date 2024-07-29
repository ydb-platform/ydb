#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropIndexAtMainTable TConfigureParts"
            << " operationId#" << OperationId;
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

        if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
            return false;
        }

        return true;
    }


    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTableIndexAtMainTable);

        //fill txShards
        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " UpdatePartitioningForTableModification");
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }

        txState->ClearShardsInProgress();

        TString txBody;
        {
            TPathId pathId = txState->TargetPathId;
            Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
            TPathElement::TPtr path = context.SS->PathsById.at(pathId);
            Y_ABORT_UNLESS(path);

            Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
            TTableInfo::TPtr table = context.SS->Tables.at(pathId);
            Y_ABORT_UNLESS(table);

            auto seqNo = context.SS->StartRound(*txState);

            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            context.SS->FillSeqNo(tx, seqNo);

            auto notice = tx.MutableDropIndexNotice();
            PathIdFromPathId(pathId, notice->MutablePathId());
            notice->SetTableSchemaVersion(table->AlterVersion + 1);

            bool found = false;
            for (const auto& [_, childPathId] : path->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
                auto childPath = context.SS->PathsById.at(childPathId);

                if (!childPath->IsTableIndex() || !childPath->PlannedToDrop()) {
                    continue;
                }

                Y_VERIFY_S(!found, "Too many indexes are planned to drop"
                    << ": found# " << PathIdFromPathId(notice->GetIndexPathId())
                    << ", another# " << childPathId);
                found = true;

                PathIdFromPathId(childPathId, notice->MutableIndexPathId());
            }

            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
        }

        Y_ABORT_UNLESS(txState->Shards.size());
        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            auto idx = txState->Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            auto event = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, txBody, context.Ctx);
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
            << "TDropIndexAtMainTable TPropose"
            << " operationId#" << OperationId;
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
                   DebugHint() << " HandleReply TEvDataShard::TEvSchemaChanged"
                               << " triggers early, save it"
                               << ", at schemeshard: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTableIndexAtMainTable);

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        table->AlterVersion += 1;
        context.SS->PersistTableAlterVersion(db, pathId, table);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTableIndexAtMainTable);

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

class TDropIndexAtMainTable: public TSubOperation {
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

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        auto dropOperation = Transaction.GetDropIndex();

        const TString workingDir = Transaction.GetWorkingDir();
        const TString mainTableName = dropOperation.GetTableName();
        const TString indexName = dropOperation.GetIndexName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropIndexAtMainTable Propose"
                         << ", path: " << workingDir << "/" << mainTableName
                         << ", index name: " << indexName
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropIndexAtMainTable Propose"
                        << ", message: " << Transaction.ShortDebugString()
                        << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));


        if (!dropOperation.HasIndexName() && !indexName) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "No index name present");
            return result;
        }

        TPath tablePath = TPath::Resolve(workingDir, context.SS).Dive(mainTableName);
        {
            TPath::TChecker checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TString errStr;

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        TPath indexPath = tablePath.Child(indexName);
        {
            TPath::TChecker checks = indexPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTableIndex()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_ABORT_UNLESS(table->AlterVersion != 0);
        Y_ABORT_UNLESS(!table->AlterData);

        Y_ABORT_UNLESS(context.SS->Indexes.contains(indexPath.Base()->PathId));
        TTableIndexInfo::TPtr index = context.SS->Indexes.at(indexPath.Base()->PathId);

        Y_ABORT_UNLESS(index->AlterVersion != 0);
        Y_ABORT_UNLESS(!index->AlterData);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropTableIndexAtMainTable, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;
        // do not fill txShards until all splits are done

        tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        tablePath.Base()->LastTxId = OperationId.GetTxId();

        for (auto splitTx: table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropIndexAtMainTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropIndexAtMainTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropTableIndexAtMainTable(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TDropIndexAtMainTable>(id, state);
}

ISubOperation::TPtr CreateDropTableIndexAtMainTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropIndexAtMainTable>(id, tx);
}

TVector<ISubOperation::TPtr> CreateDropIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex);

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CreateDropIndex"
                    << ", message: " << tx.ShortDebugString()
                    << ", at schemeshard: " << context.SS->TabletID());

    auto dropOperation = tx.GetDropIndex();

    const TString workingDir = tx.GetWorkingDir();
    const TString mainTableName = dropOperation.GetTableName();
    const TString indexName = dropOperation.GetIndexName();

    TPath workingDirPath = TPath::Resolve(workingDir, context.SS);

    TPath mainTablePath = workingDirPath.Child(mainTableName);
    {
        TPath::TChecker checks = mainTablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotAsyncReplicaTable()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    TPath indexPath = mainTablePath.Child(indexName);
    {
        TPath::TChecker checks = indexPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTableIndex()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(nextId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(mainTablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(nextId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    TVector<ISubOperation::TPtr> result;

    {
        auto mainTableIndexDropping = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndexAtMainTable);
        auto operation = mainTableIndexDropping.MutableDropIndex();
        operation->SetTableName(mainTablePath.LeafName());
        operation->SetIndexName(indexPath.LeafName());

        result.push_back(CreateDropTableIndexAtMainTable(NextPartId(nextId, result), mainTableIndexDropping));
    }

    {
        auto indexDropping = TransactionTemplate(mainTablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
        auto operation = indexDropping.MutableDrop();
        operation->SetName(ToString(indexPath.Base()->Name));

        result.push_back(CreateDropTableIndex(NextPartId(nextId, result), indexDropping));
    }

    for (const auto& [childName, childPathId] : indexPath.Base()->GetChildren()) {
        TPath child = indexPath.Child(childName);
        if (child.IsDeleted()) {
            continue;
        }

        Y_ABORT_UNLESS(child.Base()->IsTable());

        auto implTableDropping = TransactionTemplate(indexPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
        auto operation = implTableDropping.MutableDrop();
        operation->SetName(child.LeafName());

        result.push_back(CreateDropTable(NextPartId(nextId, result), implTableDropping));
        if (auto reject = CascadeDropTableChildren(result, nextId, child)) {
            return {reject};
        }
    }

    return result;
}

}
