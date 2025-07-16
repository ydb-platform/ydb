#include "schemeshard__operation_base.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_states.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace NIncrRestore {

class TConfigurePartsAtTable : public TSubOperationState {
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
        op.MutableSrcPathId()->CopyFrom(RestoreOp.GetSrcPathIds(0));
        op.SetSrcTablePath(RestoreOp.GetSrcTablePaths(0));
        pathId.ToProto(op.MutableDstPathId());
        op.SetDstTablePath(RestoreOp.GetDstTablePath());
    }

public:
    explicit TConfigurePartsAtTable(
            TOperationId id,
            const NKikimrSchemeOp::TRestoreMultipleIncrementalBackups& restoreOp)
        : OperationId(id)
        , RestoreOp(restoreOp)
    {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint() << " Constructed op# " << restoreOp.DebugString());
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

        FillNotice(txState->SourcePathId, tx, context);

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
}; // TConfigurePartsAtTable

class TProposeAtTable : public TSubOperationState {
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
    explicit TProposeAtTable(
            TOperationId id,
            const NKikimrSchemeOp::TRestoreMultipleIncrementalBackups& restoreOp)
        : OperationId(id)
    {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint() << " Constructed op# " << restoreOp.DebugString());
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

class TDone: public TSubOperationState {
private:
    static bool IsExpectedTxType(TTxState::ETxType txType) {
        switch (txType) {
        case TTxState::TxRestoreIncrementalBackupAtTable:
            return true;
        default:
            return false;
        }
    }

    TString DebugHint() const override {
        return TStringBuilder()
            << "TRestoreMultipleIncrementalBackups TDone"
            << ", operationId: " << OperationId;
    }
public:
    explicit TDone(
            TOperationId id,
            const NKikimrSchemeOp::TRestoreMultipleIncrementalBackups& restoreOp)
        : OperationId(id)
        , RestoreOp(restoreOp)
    {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint() << " Constructed op# " << restoreOp.DebugString());
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }


    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[" << context.SS->SelfTabletId() << "] " << DebugHint() << " ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));

        for (const auto& pathId : RestoreOp.GetSrcPathIds()) {
            context.OnComplete.ReleasePathState(OperationId, TPathId::FromProto(pathId), TPathElement::EPathState::EPathStateNoChanges);
        }

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }

private:
    const TOperationId OperationId;
    const NKikimrSchemeOp::TRestoreMultipleIncrementalBackups RestoreOp;
};

class TNewRestoreFromAtTable : public TSubOperationWithContext {
    using TSubOperationWithContext::SelectStateFunc;
    using TSubOperationWithContext::NextState;

    static TTxState::ETxState InitialState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState) const override {
        Y_ABORT("unreachable");
    }

    TTxState::ETxState NextState(TTxState::ETxState state, TOperationContext&) const {
        switch (state) {
        case TTxState::Waiting:
            return TTxState::CopyTableBarrier;
        case TTxState::CopyTableBarrier:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts: {
            return TTxState::Done;
        }
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state, TOperationContext&) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CopyTableBarrier:
            return MakeHolder<TWaitCopyTableBarrier>(OperationId, "NIncrRestoreState", TTxState::ConfigureParts);
        case TTxState::ConfigureParts:
            return MakeHolder<NIncrRestore::TConfigurePartsAtTable>(OperationId, Transaction.GetRestoreMultipleIncrementalBackups());
        case TTxState::Propose:
            return MakeHolder<NIncrRestore::TProposeAtTable>(OperationId, Transaction.GetRestoreMultipleIncrementalBackups());
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<NIncrRestore::TDone>(OperationId, Transaction.GetRestoreMultipleIncrementalBackups());
        default:
            return nullptr;
        }
    }

public:
    explicit TNewRestoreFromAtTable(TOperationId id, const TTxTransaction& tx)
        : TSubOperationWithContext(id, tx)
    {
    }

    explicit TNewRestoreFromAtTable(TOperationId id, TTxState::ETxState state)
        : TSubOperationWithContext(id, state)
    {
    }

    void StateDone(TOperationContext& context) override {
        if (GetState() == TTxState::Done) {
            return;
        }
        
        TTxState::ETxState nextState;
        nextState = NextState(GetState(), context);
        
        SetState(nextState, context);
        
        if (nextState != TTxState::Invalid) {
            context.OnComplete.ActivateTx(OperationId);
        }
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetRestoreMultipleIncrementalBackups();
        const auto& srcTablePathStrs = op.GetSrcTablePaths();
        const auto& dstTablePathStr = op.GetDstTablePath();

        LOG_N("TNewRestoreFromAtTable Propose"
            << ": opId# " << OperationId
            << ", srcs# " << "[" << Join(",", srcTablePathStrs) << "]"
            << ", dst# " << dstTablePathStr);

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

        for (const auto& tablePathStr : srcTablePathStrs) {
            const auto tablePath = TPath::Resolve(tablePathStr, context.SS);
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

                if (!checks) {
                    result->SetError(checks.GetStatus(), checks.GetError());
                    return result;
                }
            }
        }

        const auto dstTablePath = TPath::Resolve(dstTablePathStr, context.SS);
        {
            const auto checks = dstTablePath.Check();
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

        const auto txType = TTxState::TxRestoreIncrementalBackupAtTable;

        auto guard = context.DbGuard();

        for (const auto& tablePathStr : srcTablePathStrs) {
            const auto tablePath = TPath::Resolve(tablePathStr, context.SS);

            context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
            context.DbChanges.PersistPath(tablePath.Base()->PathId);

            Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
            auto table = context.SS->Tables.at(tablePath.Base()->PathId);

            Y_ABORT_UNLESS(table->AlterVersion != 0);
            Y_ABORT_UNLESS(!table->AlterData);

            tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateOutgoingIncrementalRestore;
            tablePath.Base()->LastTxId = OperationId.GetTxId();

            for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
            }
        }

        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        const auto firstTablePath = TPath::Resolve(srcTablePathStrs.at(0), context.SS);
        auto& txState = context.SS->CreateTx(OperationId, txType, firstTablePath.Base()->PathId, dstTablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TNewRestoreFromAtTable Propose"
                << " opId# " << OperationId
                << " workingDir# " << workingDir
                << " dstTablePath# " << dstTablePath.PathString()
                << " pathId# " << dstTablePath.Base()->PathId
                );

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

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<NIncrRestore::TNewRestoreFromAtTable>(id, tx);
}

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<NIncrRestore::TNewRestoreFromAtTable>(id, state);
}

bool CreateRestoreMultipleIncrementalBackups(
    TOperationId opId,
    const TTxTransaction& tx,
    TOperationContext& context,
    bool dstCreatedInSameOp,
    TVector<ISubOperation::TPtr>& result)
{
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpRestoreMultipleIncrementalBackups);

    LOG_N("CreateRestoreMultipleIncrementalBackups"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& restoreOp = tx.GetRestoreMultipleIncrementalBackups();
    const auto& dstTablePathStr = restoreOp.GetDstTablePath();

    TVector<TPath> srcPaths;

    for (const auto& srcTablePathStr : restoreOp.GetSrcTablePaths()) {
        const auto srcTablePath =  TPath::Resolve(srcTablePathStr, context.SS);
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
                .IsCommonSensePath();

            if (!checks) {
                result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
                return false;
            }
        }
        srcPaths.push_back(srcTablePath);
    }

    const auto dstTablePath = TPath::Resolve(dstTablePathStr, context.SS);
    {
        const auto checks = dstTablePath.Check();
        if (!dstCreatedInSameOp) {
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotUnderDeleting()
                .IsCommonSensePath();
        } else {
            checks
                .FailOnExist(TPathElement::EPathType::EPathTypeTable, false);
        }

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

    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackupAtTable);
        auto& restoreOp = *outTx.MutableRestoreMultipleIncrementalBackups();
        restoreOp.SetDstTablePath(dstTablePath.PathString());

        for (const auto& srcTablePath : srcPaths) {
            restoreOp.AddSrcTablePaths(srcTablePath.PathString());
            srcTablePath.Base()->PathId.ToProto(restoreOp.AddSrcPathIds());
        }

        result.push_back(CreateRestoreIncrementalBackupAtTable(NextPartId(opId, result), outTx));
    }

    return true;
}

TVector<ISubOperation::TPtr> CreateRestoreMultipleIncrementalBackups(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;
    CreateRestoreMultipleIncrementalBackups(opId, tx, context, false, result);
    return result;
}

} // namespace NKikimr::NSchemeShard
