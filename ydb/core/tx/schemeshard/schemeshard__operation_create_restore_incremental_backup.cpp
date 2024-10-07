#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

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

        auto& op = *tx.MutableCreateIncrementalRestoreSrc();
        op.MutableSrcPathId()->CopyFrom(RestoreOp.GetSrcPathIds(SeqNo));
        op.SetSrcTableName(RestoreOp.GetSrcTableNames(SeqNo));
        op.MutableDstPathId()->CopyFrom(RestoreOp.GetDstPathId());
        op.SetDstTableName(RestoreOp.GetDstTableName());
    }

public:
    explicit TConfigurePartsAtTable(
            TOperationId id,
            const NKikimrSchemeOp::TRestoreMultipleIncrementalBackups& restoreOp,
            ui64 seqNo)
        : OperationId(id)
        , RestoreOp(restoreOp)
        , SeqNo(seqNo)
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
    const NKikimrSchemeOp::TRestoreMultipleIncrementalBackups RestoreOp;
    const ui64 SeqNo;
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

class TProposedWaitParts: public TSubOperationState {
private:
    TOperationId OperationId;
    const TTxState::ETxState NextState;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NTableState::TProposedWaitParts"
                << " operationId# " << OperationId;
    }

public:
    TProposedWaitParts(TOperationId id, TTxState::ETxState nextState = TTxState::Done)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(),
            { TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType }
        );
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemaChanged"
                               << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvSchemaChanged"
                                << " at tablet: " << ssId
                                << " message: " << evRecord.ShortDebugString());

        if (!NTableState::CollectSchemaChanged(OperationId, ev, context)) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply TEvSchemaChanged"
                                    << " CollectSchemaChanged: false");
            return false;
        }

        Y_ABORT_UNLESS(context.SS->FindTx(OperationId));
        TTxState& txState = *context.SS->FindTx(OperationId);

        if (!txState.ReadyForNotifications) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply TEvSchemaChanged"
                                    << " ReadyForNotifications: false");
            return false;
        }

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        NIceDb::TNiceDb db(context.GetDB());

        txState->ClearShardsInProgress();
        for (TTxState::TShardOperation& shard : txState->Shards) {
            if (shard.Operation < TTxState::ProposedWaitParts) {
                shard.Operation = TTxState::ProposedWaitParts;
                context.SS->PersistUpdateTxShard(db, OperationId, shard.Idx, shard.Operation);
            }
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            context.OnComplete.RouteByTablet(OperationId,  context.SS->ShardInfos.at(shard.Idx).TabletID);
        }
        txState->UpdateShardsInProgress(TTxState::ProposedWaitParts);

        // Move all notifications that were already received
        // NOTE: SchemeChangeNotification is sent form DS after it has got PlanStep from coordinator and the schema tx has completed
        // At that moment the SS might not have received PlanStep from coordinator yet (this message might be still on its way to SS)
        // So we are going to accumulate SchemeChangeNotification that are received before this Tx switches to WaitParts state
        txState->AcceptPendingSchemeNotification();

        // Got notifications from all datashards?
        if (txState->ShardsInProgress.empty()) {
            NTableState::AckAllSchemaChanges(OperationId, *txState, context);
            context.SS->ChangeTxState(db, OperationId, NextState);
            return true;
        }

        return false;
    }
};

class TNewRestoreFromAtTable: public TBetterSubOperation {
    static TTxState::ETxState InitialState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state, TOperationContext& context) const override {
        switch (state) {
        case TTxState::Waiting:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts: {
            auto* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);
            // Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
            ++(txState->LoopSeqNo);
            if (txState->LoopSeqNo < Transaction.GetRestoreMultipleIncrementalBackups().SrcPathIdsSize()) {
                txState->TargetPathId = PathIdFromPathId(Transaction.GetRestoreMultipleIncrementalBackups().GetSrcPathIds(txState->LoopSeqNo));
                txState->TxShardsListFinalized = false;
                // TODO preserve TxState
                return TTxState::ConfigureParts;
            }
            return TTxState::Done;
        }
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state, TOperationContext& context) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts: {
            auto* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);
            // Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
            return MakeHolder<NIncrRestore::TConfigurePartsAtTable>(OperationId, Transaction.GetRestoreMultipleIncrementalBackups(), txState->LoopSeqNo);
        }
        case TTxState::Propose:
            return MakeHolder<NIncrRestore::TProposeAtTable>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<NIncrRestore::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    public:
        explicit TNewRestoreFromAtTable(TOperationId id, const TTxTransaction& tx)
        : TBetterSubOperation(id, tx)
    {
    }

    explicit TNewRestoreFromAtTable(TOperationId id, TTxState::ETxState state)
        : TBetterSubOperation(id, state)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetRestoreMultipleIncrementalBackups();
        const auto& tableName = op.GetSrcTableNames(0);
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
                .IsCommonSensePath();
                // .IsUnderTheSameOperation(OperationId.GetTxId()); // lock op

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
                .IsCommonSensePath();
                // .IsUnderTheSameOperation(OperationId.GetTxId()); // lock op

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

        // Cerr << " <-- <-- <-- " << (op.GetSeqNo() > 0 ? "dep" : "nodep") << tableName << " from " << OperationId.GetTxId().GetValue() << " to " << OperationId.GetTxId().GetValue() - 1 << Endl;

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

        SetState(InitialState(), context);
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

    Y_ABORT("Intentnionally broken");

    LOG_N("CreateRestoreIncrementalBackup"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& restoreOp = tx.GetRestoreIncrementalBackup();
    const auto& srcTableName = restoreOp.GetSrcTableName();
    const auto& dstTableName = restoreOp.GetDstTableName();

    const auto srcTablePathX = workingDirPath.Child(srcTableName);
    const auto srcTablePath = TPath::Resolve(srcTablePathX.PathString(), context.SS);
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

    const auto dstTablePathX = workingDirPath.Child(dstTableName);
    const auto dstTablePath = TPath::Resolve(dstTablePathX.PathString(), context.SS);
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

    TVector<ISubOperation::TPtr> result;

    DoCreateLock(opId, srcTablePath.Parent(), srcTablePath, result);
    DoCreateLock(opId, dstTablePath.Parent(), dstTablePath, result);

    {
        auto outTx = TransactionTemplate(srcTablePath.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackupAtTable);
        outTx.MutableRestoreIncrementalBackup()->CopyFrom(restoreOp);
        auto& restoreOp = *outTx.MutableRestoreIncrementalBackup();
        restoreOp.SetSrcTableName(srcTablePath.LeafName());
        restoreOp.SetDstTableName(dstTablePath.LeafName());
        PathIdFromPathId(srcTablePath.Base()->PathId, restoreOp.MutableSrcPathId());
        PathIdFromPathId(dstTablePath.Base()->PathId, restoreOp.MutableDstPathId());
        result.push_back(CreateRestoreIncrementalBackupAtTable(NextPartId(opId, result), outTx));
    }

    DoDropLock(opId, srcTablePath.Parent(), srcTablePath, result);
    DoDropLock(opId, dstTablePath.Parent(), dstTablePath, result);

    return result;
}

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<NIncrRestore::TNewRestoreFromAtTable>(id, tx);
}

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, TTxState::ETxState state, TOperationContext& context) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<NIncrRestore::TNewRestoreFromAtTable>(id, state, context);
}

bool CreateRestoreMultipleIncrementalBackups(
    TOperationId opId,
    const TTxTransaction& tx,
    TOperationContext& context,
    TVector<ISubOperation::TPtr>& result)
{
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpRestoreMultipleIncrementalBackups);

    LOG_N("CreateRestoreMultipleIncrementalBackups"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& restoreOp = tx.GetRestoreMultipleIncrementalBackups();
    const auto& dstTableName = restoreOp.GetDstTableName();

    TVector<TPath> srcPaths;

    for (const auto& srcTableName : restoreOp.GetSrcTableNames()) {
        const auto srcTablePathX = workingDirPath.Child(srcTableName);
        const auto srcTablePath = TPath::Resolve(srcTablePathX.PathString(), context.SS);
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
                // .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
                return false;
            }
        }
        srcPaths.push_back(srcTablePath);
    }

    const auto dstTablePathX = workingDirPath.Child(dstTableName);
    const auto dstTablePath = TPath::Resolve(dstTablePathX.PathString(), context.SS);
    {
        const auto checks = dstTablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            // .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
            return false;
        }
    }


    for (auto& srcTablePath : srcPaths) {
        Y_ABORT_UNLESS(context.SS->Tables.contains(srcTablePath.Base()->PathId));
        auto srcTable = context.SS->Tables.at(srcTablePath.Base()->PathId);
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
        return false;
    }

    for (auto& srcTablePath : srcPaths) {
        if (!context.SS->CheckLocks(srcTablePath.Base()->PathId, tx, errStr)) {
            result = {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
            return false;
        }
    }

    // check dst locks
    // lock dst

    // for (auto& srcTablePath : srcPaths) {
    //     DoCreateLock(opId, srcTablePath.Parent(), srcTablePath, result);
    // }
    // DoCreateLock(opId, dstTablePath.Parent(), dstTablePath, result);

    {
        auto outTx = TransactionTemplate(srcPaths[0].Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackupAtTable);
        auto& restoreOp = *outTx.MutableRestoreMultipleIncrementalBackups();
        restoreOp.SetDstTableName(dstTablePath.LeafName());
        PathIdFromPathId(dstTablePath.Base()->PathId, restoreOp.MutableDstPathId());

        for (const auto& srcTablePath : srcPaths) {
            restoreOp.AddSrcTableNames(srcTablePath.LeafName());
            PathIdFromPathId(srcTablePath.Base()->PathId, restoreOp.AddSrcPathIds());
        }

        result.push_back(CreateRestoreIncrementalBackupAtTable(NextPartId(opId, result), outTx));
    }

    // for (auto& srcTablePath : srcPaths) {
    //     DoDropLock(opId, srcTablePath.Parent(), srcTablePath, result);
    // }
    // DoDropLock(opId, dstTablePath.Parent(), dstTablePath, result);

    return true;
}

TVector<ISubOperation::TPtr> CreateRestoreMultipleIncrementalBackups(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;
    CreateRestoreMultipleIncrementalBackups(opId, tx, context, result);
    return result;
}

} // namespace NKikimr::NSchemeShard
