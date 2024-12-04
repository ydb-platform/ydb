#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard_utils.h"  // for TransactionTemplate

#include "schemeshard__operation_create_cdc_stream.h"

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

void DoCreateLock(const TOperationId opId, const TPath& workingDirPath, const TPath& tablePath, TVector<ISubOperation::TPtr>& result)
{
    auto outTx = TransactionTemplate(workingDirPath.PathString(),
        NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock);
    outTx.SetFailOnExist(false);
    outTx.SetInternal(true);
    auto cfg = outTx.MutableLockConfig();
    cfg->SetName(tablePath.LeafName());

    result.push_back(CreateLock(NextPartId(opId, result), outTx));
}

void DoDropLock(const TOperationId opId, const TPath& workingDirPath, const TPath& tablePath, TVector<ISubOperation::TPtr>& result)
{
    auto outTx = TransactionTemplate(workingDirPath.PathString(),
        NKikimrSchemeOp::EOperationType::ESchemeOpDropLock);
    outTx.SetFailOnExist(true);
    outTx.SetInternal(true);
    auto cfg = outTx.MutableLockConfig();
    cfg->SetName(tablePath.LeafName());
    outTx.MutableLockGuard()->SetOwnerTxId(ui64(opId.GetTxId()));

    result.push_back(DropLock(NextPartId(opId, result), outTx));
}

namespace NIncrRestore {

class TConfigurePartsAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NIncrRestoreState::TConfigurePartsAtTable"
            << " operationId: " << OperationId;
    }

    static bool IsExpectedTxType(TTxState::ETxType txType) {
        switch (txType) {
        case TTxState::TxRestoreIncrementalBackupAtTable:
            return true;
        default:
            return false;
        }
    }

protected:
    void FillNotice(
        const TPathId& pathId,
        NKikimrTxDataShard::TFlatSchemeTransaction& tx,
        TOperationContext& context) const
    {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        tx.MutableCreateIncrementalRestoreSrc()->CopyFrom(RestoreOp);
    }

public:
    explicit TConfigurePartsAtTable(TOperationId id, const NKikimrSchemeOp::TRestoreIncrementalBackup& restoreOp)
        : OperationId(id)
        , RestoreOp(restoreOp)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
        const auto& pathId = txState->TargetPathId;

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }

        NKikimrTxDataShard::TFlatSchemeTransaction tx;
        context.SS->FillSeqNo(tx, context.SS->StartRound(*txState));
        FillNotice(pathId, tx, context);

        txState->ClearShardsInProgress();
        Y_ABORT_UNLESS(txState->Shards.size());

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            const auto& idx = txState->Shards[i].Idx;
            const auto datashardId = context.SS->ShardInfos[idx].TabletID;
            auto ev = context.SS->MakeDataShardProposal(pathId, OperationId, tx.SerializeAsString(), context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, ev.Release());
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply " << ev->Get()->ToString()
                               << ", at schemeshard: " << context.SS->TabletID());

        if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
            return false;
        }

        return true;
    }

private:
    const TOperationId OperationId;
    const NKikimrSchemeOp::TRestoreIncrementalBackup RestoreOp;
}; // TConfigurePartsAtTable

class TProposeAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NIncrRestoreState::TProposeAtTable"
            << " operationId: " << OperationId;
    }

    static bool IsExpectedTxType(TTxState::ETxType txType) {
        switch (txType) {
        case TTxState::TxRestoreIncrementalBackupAtTable:
            return true;
        default:
            return false;
        }
    }

public:
    explicit TProposeAtTable(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            shardSet.insert(context.SS->ShardInfos.at(shard.Idx).TabletID);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
        return false;
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " TEvDataShard::TEvSchemaChanged"
                               << " triggers early, save it"
                               << ", at schemeshard: " << context.SS->TabletID());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << ev->Get()->StepId
                               << ", at schemeshard: " << context.SS->TabletID());

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        table->AlterVersion += 1;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistTableAlterVersion(db, pathId, table);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);

        return true;
    }

protected:
    const TOperationId OperationId;

}; // TProposeAtTable

class TNewRestoreFromAtTable: public TSubOperation {
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
            return MakeHolder<NIncrRestore::TConfigurePartsAtTable>(OperationId, Transaction.GetRestoreIncrementalBackup());
        case TTxState::Propose:
            return MakeHolder<NIncrRestore::TProposeAtTable>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    explicit TNewRestoreFromAtTable(TOperationId id, const TTxTransaction& tx)
        : TSubOperation(id, tx)
    {
    }

    explicit TNewRestoreFromAtTable(TOperationId id, TTxState::ETxState state)
        : TSubOperation(id, state)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetRestoreIncrementalBackup();
        const auto& tableName = op.GetSrcTableName();
        const auto& dstTableName = op.GetDstTableName();

        LOG_N("TNewRestoreFromAtTable Propose"
            << ": opId# " << OperationId
            << ", src# " << workingDir << "/" << tableName
            << ", dst# " << workingDir << "/" << dstTableName);

        auto result = MakeHolder<TProposeResponse>(
            NKikimrScheme::StatusAccepted,
            ui64(OperationId.GetTxId()),
            context.SS->TabletID());

        const auto workingDirPath = TPath::Resolve(workingDir, context.SS);
        {
            const auto checks = workingDirPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsLikeDirectory()
                .NotUnderDeleting()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto tablePath = workingDirPath.Child(tableName);
        {
            const auto checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsUnderTheSameOperation(OperationId.GetTxId()); // lock op

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto dstTablePath = workingDirPath.Child(dstTableName);
        {
            const auto checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsUnderTheSameOperation(OperationId.GetTxId()); // lock op

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

        // we do not need snapshot as far as source table is under operation
        // and guaranteed to be unchanged

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_ABORT_UNLESS(table->AlterVersion != 0);
        Y_ABORT_UNLESS(!table->AlterData);

        const auto txType = TTxState::TxRestoreIncrementalBackupAtTable;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, txType, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateOutgoingIncrementalRestore;
        tablePath.Base()->LastTxId = OperationId.GetTxId();

        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TNewRestoreFromAtTable AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TNewRestoreFromAtTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

}; // TNewRestoreFromAtTable

} // namespace NIncrRestore

TVector<ISubOperation::TPtr> CreateRestoreIncrementalBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackup);

    LOG_N("CreateRestoreIncrementalBackup"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& restoreOp = tx.GetRestoreIncrementalBackup();
    const auto& srcTableName = restoreOp.GetSrcTableName();
    const auto& dstTableName = restoreOp.GetDstTableName();

    const auto srcTablePath = workingDirPath.Child(srcTableName);
    {
        const auto checks = srcTablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    const auto dstTablePath = workingDirPath.Child(dstTableName);
    {
        const auto checks = srcTablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }


    Y_ABORT_UNLESS(context.SS->Tables.contains(srcTablePath.Base()->PathId));
    auto srcTable = context.SS->Tables.at(srcTablePath.Base()->PathId);

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(srcTablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    // check dst locks
    // lock dst

    TVector<TString> boundaries;
    const auto& partitions = srcTable->GetPartitions();
    boundaries.reserve(partitions.size() - 1);

    for (ui32 i = 0; i < partitions.size(); ++i) {
        const auto& partition = partitions.at(i);
        if (i != partitions.size() - 1) {
            boundaries.push_back(partition.EndOfRange);
        }
    }

    TVector<ISubOperation::TPtr> result;

    DoCreateLock(opId, workingDirPath, srcTablePath, result);
    DoCreateLock(opId, workingDirPath, dstTablePath, result);

    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackupAtTable);
        outTx.MutableRestoreIncrementalBackup()->CopyFrom(restoreOp);
        auto& restoreOp = *outTx.MutableRestoreIncrementalBackup();
        PathIdFromPathId(srcTablePath.Base()->PathId, restoreOp.MutableSrcPathId());
        PathIdFromPathId(dstTablePath.Base()->PathId, restoreOp.MutableDstPathId());
        result.push_back(CreateRestoreIncrementalBackupAtTable(NextPartId(opId, result), outTx));
    }

    DoDropLock(opId, workingDirPath, dstTablePath, result);
    DoDropLock(opId, workingDirPath, srcTablePath, result);

    return result;
}

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<NIncrRestore::TNewRestoreFromAtTable>(id, tx);
}

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<NIncrRestore::TNewRestoreFromAtTable>(id, state);
}

} // namespace NKikimr::NSchemeShard
