#include "schemeshard__operation_alter_cdc_stream.h"

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace NCdc {

namespace {

class TPropose: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "AlterCdcStream TPropose"
            << " opId# " << OperationId << " ";
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterCdcStream);

        // TODO(KIKIMR-12278): shards

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterCdcStream);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(pathId));
        auto stream = context.SS->CdcStreams.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->PersistCdcStream(db, pathId);
        context.SS->CdcStreams[pathId] = stream->AlterData;

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
    const TOperationId OperationId;

}; // TPropose

class TAlterCdcStream: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetAlterCdcStream();
        const auto& streamName = op.GetStreamName();

        LOG_N("TAlterCdcStream Propose"
            << ": opId# " << OperationId
            << ", stream# " << workingDir << "/" << streamName);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const auto streamPath = TPath::Resolve(workingDir, context.SS).Dive(streamName);
        {
            const auto checks = streamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .NotUnderDeleting();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto tablePath = streamPath.Parent();
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
                .IsCommonSensePath()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(streamPath.Base()->PathId));
        auto stream = context.SS->CdcStreams.at(streamPath.Base()->PathId);

        TCdcStreamInfo::EState requiredState = TCdcStreamInfo::EState::ECdcStreamStateInvalid;
        TCdcStreamInfo::EState newState = TCdcStreamInfo::EState::ECdcStreamStateInvalid;

        switch (op.GetActionCase()) {
        case NKikimrSchemeOp::TAlterCdcStream::kDisable:
            requiredState = TCdcStreamInfo::EState::ECdcStreamStateDisabled;
            if (stream->State == TCdcStreamInfo::EState::ECdcStreamStateReady) {
                newState = requiredState;
            }
            break;
        case NKikimrSchemeOp::TAlterCdcStream::kGetReady:
            requiredState = TCdcStreamInfo::EState::ECdcStreamStateReady;
            if (stream->State == TCdcStreamInfo::EState::ECdcStreamStateScan) {
                newState = requiredState;
            }
            break;
        default:
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "Unknown action: " << static_cast<ui32>(op.GetActionCase()));
            return result;
        }

        if (newState == TCdcStreamInfo::EState::ECdcStreamStateInvalid) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder()
                << "Cannot switch state"
                << ": from# " << stream->State
                << ", to# " << requiredState);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, streamPath.Base()->PathId);
        context.MemChanges.GrabCdcStream(context.SS, streamPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(streamPath.Base()->PathId);
        context.DbChanges.PersistAlterCdcStream(streamPath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        auto streamAlter = stream->CreateNextVersion();
        Y_ABORT_UNLESS(streamAlter);
        streamAlter->State = newState;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterCdcStream, streamPath.Base()->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        streamPath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
        streamPath.Base()->LastTxId = OperationId.GetTxId();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterCdcStream AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TAlterCdcStream AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

}; // TAlterCdcStream

class TConfigurePartsAtTable: public NCdcStreamState::TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        auto& notice = *tx.MutableAlterCdcStreamNotice();
        PathIdFromPathId(pathId, notice.MutablePathId());
        notice.SetTableSchemaVersion(table->AlterVersion + 1);

        bool found = false;
        for (const auto& [childName, childPathId] : path->GetChildren()) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);

            if (!childPath->IsCdcStream() || childPath->Dropped() || childPath->NormalState()) {
                continue;
            }

            Y_ABORT_UNLESS(context.SS->CdcStreams.contains(childPathId));
            auto stream = context.SS->CdcStreams.at(childPathId);

            Y_VERIFY_S(!found, "Too many cdc streams are planned to alter"
                << ": found# " << PathIdFromPathId(notice.GetStreamDescription().GetPathId())
                << ", another# " << childPathId);
            found = true;

            Y_ABORT_UNLESS(stream->AlterData);
            context.SS->DescribeCdcStream(childPathId, childName, stream->AlterData, *notice.MutableStreamDescription());
        }
    }

public:
    using NCdcStreamState::TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTable

class TConfigurePartsAtTableDropSnapshot: public TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        TConfigurePartsAtTable::FillNotice(pathId, tx, context);

        Y_ABORT_UNLESS(context.SS->TablesWithSnapshots.contains(pathId));
        const auto snapshotTxId = context.SS->TablesWithSnapshots.at(pathId);

        Y_ABORT_UNLESS(context.SS->SnapshotsStepIds.contains(snapshotTxId));
        const auto snapshotStep = context.SS->SnapshotsStepIds.at(snapshotTxId);

        Y_ABORT_UNLESS(tx.HasAlterCdcStreamNotice());
        auto& notice = *tx.MutableAlterCdcStreamNotice();

        notice.MutableDropSnapshot()->SetStep(ui64(snapshotStep));
        notice.MutableDropSnapshot()->SetTxId(ui64(snapshotTxId));
    }

public:
    using TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTableDropSnapshot

class TAlterCdcStreamAtTable: public TSubOperation {
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

        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            if (DropSnapshot) {
                return MakeHolder<TConfigurePartsAtTableDropSnapshot>(OperationId);
            } else {
                return MakeHolder<TConfigurePartsAtTable>(OperationId);
            }
        case TTxState::Propose:
            if (DropSnapshot) {
                return MakeHolder<NCdcStreamState::TProposeAtTableDropSnapshot>(OperationId);
            } else {
                return MakeHolder<NCdcStreamState::TProposeAtTable>(OperationId);
            }
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    explicit TAlterCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot)
        : TSubOperation(id, tx)
        , DropSnapshot(dropSnapshot)
    {
    }

    explicit TAlterCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot)
        : TSubOperation(id, state)
        , DropSnapshot(dropSnapshot)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetAlterCdcStream();
        const auto& tableName = op.GetTableName();
        const auto& streamName = op.GetStreamName();

        LOG_N("TAlterCdcStreamAtTable Propose"
            << ": opId# " << OperationId
            << ", stream# " << workingDir << "/" << tableName << "/" << streamName);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const auto tablePath = TPath::Resolve(workingDir, context.SS).Dive(tableName);
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
                .IsCommonSensePath()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto streamPath = tablePath.Child(streamName);
        {
            const auto checks = streamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId());

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

        if (DropSnapshot && !context.SS->TablesWithSnapshots.contains(tablePath.Base()->PathId)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder() << "Table has no snapshots"
                << ": pathId# " << tablePath.Base()->PathId);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_ABORT_UNLESS(table->AlterVersion != 0);
        Y_ABORT_UNLESS(!table->AlterData);

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(streamPath.Base()->PathId));
        auto stream = context.SS->CdcStreams.at(streamPath.Base()->PathId);

        Y_ABORT_UNLESS(stream->AlterVersion != 0);
        Y_ABORT_UNLESS(stream->AlterData);

        const auto txType = DropSnapshot
            ? TTxState::TxAlterCdcStreamAtTableDropSnapshot
            : TTxState::TxAlterCdcStreamAtTable;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, txType, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        tablePath.Base()->LastTxId = OperationId.GetTxId();

        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterCdcStreamAtTable AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TAlterCdcStreamAtTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

private:
    const bool DropSnapshot;

}; // TAlterCdcStreamAtTable

} // anonymous

std::variant<TStreamPaths, ISubOperation::TPtr> DoAlterStreamPathChecks(
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName)
{
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
            .IsCommonSensePath()
            .NotUnderOperation();

        if (!checks) {
            return CreateReject(opId, checks.GetStatus(), checks.GetError());
        }
    }

    const auto streamPath = tablePath.Child(streamName);
    {
        const auto checks = streamPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsCdcStream()
            .NotUnderOperation();

        if (!checks) {
            return CreateReject(opId, checks.GetStatus(), checks.GetError());
        }
    }

    return TStreamPaths{tablePath, streamPath};
}

void DoAlterStream(
    const NKikimrSchemeOp::TAlterCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    TVector<ISubOperation::TPtr>& result)
{
    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamImpl);
        outTx.MutableAlterCdcStream()->CopyFrom(op);

        if (op.HasGetReady()) {
            outTx.MutableLockGuard()->SetOwnerTxId(op.GetGetReady().GetLockTxId());
        }

        result.push_back(CreateAlterCdcStreamImpl(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamAtTable);
        outTx.MutableAlterCdcStream()->CopyFrom(op);

        if (op.HasGetReady()) {
            outTx.MutableLockGuard()->SetOwnerTxId(op.GetGetReady().GetLockTxId());
        }

        result.push_back(CreateAlterCdcStreamAtTable(NextPartId(opId, result), outTx, op.HasGetReady()));
    }
}

} // namespace NCdc

using namespace NCdc;

ISubOperation::TPtr CreateAlterCdcStreamImpl(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterCdcStream>(id, tx);
}

ISubOperation::TPtr CreateAlterCdcStreamImpl(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TAlterCdcStream>(id, state);
}

ISubOperation::TPtr CreateAlterCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot) {
    return MakeSubOperation<TAlterCdcStreamAtTable>(id, tx, dropSnapshot);
}

ISubOperation::TPtr CreateAlterCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot) {
    return MakeSubOperation<TAlterCdcStreamAtTable>(id, state, dropSnapshot);
}

TVector<ISubOperation::TPtr> CreateAlterCdcStream(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream);

    LOG_D("CreateAlterCdcStream"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto& op = tx.GetAlterCdcStream();
    const auto& tableName = op.GetTableName();
    const auto& streamName = op.GetStreamName();

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    const auto checksResult = DoAlterStreamPathChecks(opId, workingDirPath, tableName, streamName);
    if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
        return {std::get<ISubOperation::TPtr>(checksResult)};
    }

    const auto [tablePath, streamPath] = std::get<TStreamPaths>(checksResult);

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    TVector<ISubOperation::TPtr> result;

    DoAlterStream(op, opId, workingDirPath, tablePath, result);

    if (op.HasGetReady()) {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropLock);
        outTx.SetFailOnExist(true);
        outTx.SetInternal(true);
        outTx.MutableLockConfig()->SetName(tablePath.LeafName());
        outTx.MutableLockGuard()->SetOwnerTxId(op.GetGetReady().GetLockTxId());

        result.push_back(DropLock(NextPartId(opId, result), outTx));
    }

    return result;
}

}
