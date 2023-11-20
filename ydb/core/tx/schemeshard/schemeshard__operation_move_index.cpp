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
            << "TUpdateMainTableOnIndexMove TConfigureParts"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxUpdateMainTableOnIndexMove);

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

            auto notice = tx.MutableMoveIndex();
            PathIdFromPathId(pathId, notice->MutablePathId());
            notice->SetTableSchemaVersion(table->AlterVersion + 1);

            auto remap = notice->MutableReMapIndex();

            auto opId = OperationId;
            while (true) {
                opId.second += 1;
                TTxState* txState = context.SS->TxInFlight.FindPtr(opId);
                if (!txState) {
                    TStringStream msg;
                    msg << "txState for opId: " << opId
                        << " has not been found, cur opId: " << OperationId;
                    Y_ABORT("%s", msg.Str().data());
                }
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            DebugHint() << " Trying to find txState with TxMoveTableIndex type"
                             << " cur opId: " << opId
                             << ", type: " << (int)txState->TxType);

                if (txState->TxType == TTxState::TxMoveTableIndex) {
                    TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);
                    auto parent = srcPath.Parent();
                    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                                DebugHint() << " Checking pathId for "
                                 << "opId: " << opId
                                 << ", type: " << (int)txState->TxType
                                 << ", parent pathId: " << pathId);
                    if (pathId == parent.Base()->PathId) {
                        PathIdFromPathId(txState->SourcePathId, remap->MutableSrcPathId());
                        PathIdFromPathId(txState->TargetPathId, remap->MutableDstPathId());
                        auto targetIndexName = context.SS->PathsById.at(txState->TargetPathId);

                        for (const auto& [_, childPathId] : path->GetChildren()) {
                            Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
                            auto childPath = context.SS->PathsById.at(childPathId);

                            if (childPath->Name == targetIndexName->Name) {
                                PathIdFromPathId(childPathId, remap->MutableReplacedPathId());
                                remap->SetDstName(childPath->Name);
                            }
                        }
                        break;
                    }
                }
            }
            Y_ABORT_UNLESS(remap->HasSrcPathId());
            Y_ABORT_UNLESS(remap->HasDstPathId());
            Y_ABORT_UNLESS(remap->HasDstName());

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
            << "TMoveIndex TPropose"
            << ", operationId: " << OperationId;
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
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxUpdateMainTableOnIndexMove);

        TPath path = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(path.IsResolved());

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxUpdateMainTableOnIndexMove);
        Y_ABORT_UNLESS(txState->MinStep);

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;
            shardSet.insert(tablet);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, std::move(shardSet));
        return false;
    }
};

class TDeleteTableBarrier: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveIndex TDeleteTableBarrier"
                << " operationId: " << OperationId;
    }

public:
    TDeleteTableBarrier(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType, TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvDataShard::TEvSchemaChanged"
                               << ", save it"
                               << ", at schemeshard: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate:TEvCompleteBarrier"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet" << ssId);


        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);


        TPath path = TPath::Init(txState->TargetPathId, context.SS);
        TTableInfo::TPtr table = context.SS->Tables.at(txState->TargetPathId);

        Y_ABORT_UNLESS(txState->PlanStep);

        NIceDb::TNiceDb db(context.GetDB());
        table->AlterVersion += 1;

        context.SS->PersistTableAlterVersion(db, path->PathId, table);

        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);
        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        context.OnComplete.Barrier(OperationId, "RenamePathBarrier");
        return false;
    }
};

class TUpdateMainTableOnIndexMove: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::DeletePathBarrier;
        case TTxState::DeletePathBarrier:
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
        case TTxState::DeletePathBarrier:
            return MakeHolder<TDeleteTableBarrier>(OperationId);
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

        auto opDescr = Transaction.GetAlterTable();

        const TString workingDir = Transaction.GetWorkingDir();
        const TString mainTableName = opDescr.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TUpdateMainTableOnIndexMove Propose"
                         << ", path: " << workingDir << "/" << mainTableName
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TUpdateMainTableOnIndexMove Propose"
                        << ", message: " << Transaction.ShortDebugString()
                        << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

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

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_ABORT_UNLESS(table->AlterVersion != 0);
        Y_ABORT_UNLESS(!table->AlterData);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxUpdateMainTableOnIndexMove, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

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
                     "TUpdateMainTableOnIndexMove AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TUpdateMainTableOnIndexMove AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }

};

}

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateConsistentMoveIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex);

    TVector<ISubOperation::TPtr> result;

    if (!context.SS->EnableMoveIndex) {
        TString errStr = "Move index is not supported yet";
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
    }

    const auto& moving = tx.GetMoveIndex();
    const auto& mainTable = moving.GetTablePath();
    const auto& srcIndex = moving.GetSrcPath();
    const auto& dstIndex = moving.GetDstPath();

    {
        TString errStr;
        if (!context.SS->CheckApplyIf(tx, errStr)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
        }
    }

    bool allowOverwrite = moving.HasAllowOverwrite() && moving.GetAllowOverwrite();

    TPath mainTablePath = TPath::Resolve(mainTable, context.SS);
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

    TPath workingDirPath = mainTablePath.Parent();

    {
        TStringBuilder explain = TStringBuilder() << "fail checks";

        if (!context.SS->CheckLocks(mainTablePath.Base()->PathId, tx, explain)) {
            return {CreateReject(nextId, NKikimrScheme::StatusMultipleModifications, explain)};
        }
    }

    TPath srcIndexPath = mainTablePath.Child(srcIndex);
    {
        TPath::TChecker checks = srcIndexPath.Check();
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

    TPath dstIndexPath = mainTablePath.Child(dstIndex);

    {
        auto mainTableAlter = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);
        auto operation = mainTableAlter.MutableAlterTable();
        operation->SetName(mainTablePath.LeafName());
        result.push_back(new TUpdateMainTableOnIndexMove(NextPartId(nextId, result), mainTableAlter));
    }

    {
        TPath::TChecker checks = dstIndexPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderOperation()
            .IsTableIndex();

        if (checks) {
            if (!allowOverwrite) {
                TString errStr = TStringBuilder()
                    << "Index " << dstIndex
                    << "exists, but overwrite flag has not been set";
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError, errStr)};
            }
            {
                auto indexDropping = TransactionTemplate(mainTablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
                auto operation = indexDropping.MutableDrop();
                operation->SetName(dstIndex);

                result.push_back(CreateDropTableIndex(NextPartId(nextId, result), indexDropping));
            }

            for (const auto& items: dstIndexPath.Base()->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(items.second));
                auto implPath = context.SS->PathsById.at(items.second);
                if (implPath->Dropped()) {
                    continue;
                }

                auto implTable = context.SS->PathsById.at(items.second);
                Y_ABORT_UNLESS(implTable->IsTable());

                auto implTableDropping = TransactionTemplate(dstIndexPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                auto operation = implTableDropping.MutableDrop();
                operation->SetName(items.first);

                result.push_back(CreateDropTable(NextPartId(nextId, result), implTableDropping));
            }
        }
    }

    result.push_back(CreateMoveTableIndex(NextPartId(nextId, result), MoveTableIndexTask(srcIndexPath, dstIndexPath)));

    TString srcImplTableName = srcIndexPath.Base()->GetChildren().begin()->first;
    TPath srcImplTable = srcIndexPath.Child(srcImplTableName);

    Y_ABORT_UNLESS(srcImplTable.Base()->PathId == srcIndexPath.Base()->GetChildren().begin()->second);

    TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

    result.push_back(CreateMoveTable(NextPartId(nextId, result), MoveTableTask(srcImplTable, dstImplTable)));
    return result;
}

ISubOperation::TPtr CreateUpdateMainTableOnIndexMove(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TUpdateMainTableOnIndexMove>(id, tx);
}

ISubOperation::TPtr CreateUpdateMainTableOnIndexMove(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TUpdateMainTableOnIndexMove>(id, state);
}

}
