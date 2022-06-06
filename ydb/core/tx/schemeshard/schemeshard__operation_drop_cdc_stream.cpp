#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr {
namespace NSchemeShard {

namespace {

class TPropose: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "DropCdcStream TPropose"
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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxDropCdcStream);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxDropCdcStream);
        const auto& pathId = txState->TargetPathId;

        Y_VERIFY(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_VERIFY(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->PersistDropStep(db, pathId, step, OperationId);
        context.SS->PersistRemoveCdcStream(db, pathId);

        Y_VERIFY(context.SS->PathsById.contains(path->ParentPathId));
        auto parent = context.SS->PathsById.at(path->ParentPathId);

        context.SS->ResolveDomainInfo(pathId)->DecPathsInside();
        parent->DecAliveChildren();

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
    const TOperationId OperationId;

}; // TPropose

class TDropCdcStream: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    static TTxState::ETxState NextState(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Propose:
            return THolder(new TPropose(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    explicit TDropCdcStream(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
        , State(TTxState::Invalid)
    {
    }

    explicit TDropCdcStream(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetDrop();
        const auto& streamName = op.GetName();

        LOG_N("TDropCdcStream Propose"
            << ": opId# " << OperationId
            << ", stream# " << workingDir << "/" << streamName);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const auto streamPath = op.HasId()
            ? TPath::Init(context.SS->MakeLocalId(op.GetId()), context.SS)
            : TPath::Resolve(workingDir, context.SS).Dive(streamName);
        {
            const auto checks = streamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                TString explain = TStringBuilder() << "path checks failed, path: " << streamPath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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
                .IsCommonSensePath()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId());

            if (!checks) {
                TString explain = TStringBuilder() << "path checks failed, path: " << tablePath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        auto guard = context.DbGuard();
        context.DbChanges.PersistTxState(OperationId);

        Y_VERIFY(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxDropCdcStream, streamPath.Base()->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        streamPath.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        streamPath.Base()->DropTxId = OperationId.GetTxId();
        streamPath.Base()->LastTxId = OperationId.GetTxId();

        context.SS->TabletCounters->Simple()[COUNTER_CDC_STREAMS_COUNT].Sub(1);
        context.SS->ClearDescribePathCaches(streamPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, streamPath.Base()->PathId);
        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TDropCdcStream");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TDropCdcStream AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

private:
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State;

}; // TDropCdcStream

class TConfigurePartsAtTable: public NCdcStreamState::TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_VERIFY(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        auto& notice = *tx.MutableDropCdcStreamNotice();
        PathIdFromPathId(pathId, notice.MutablePathId());
        notice.SetTableSchemaVersion(table->AlterVersion + 1);

        bool found = false;
        for (const auto& [_, childPathId] : path->GetChildren()) {
            Y_VERIFY(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);

            if (!childPath->IsCdcStream() || !childPath->PlannedToDrop()) {
                continue;
            }

            Y_VERIFY_S(!found, "Too many cdc streams are planned to drop"
                << ": found# " << PathIdFromPathId(notice.GetStreamPathId())
                << ", another# " << childPathId);
            found = true;

            PathIdFromPathId(childPathId, notice.MutableStreamPathId());
        }
    }

public:
    using NCdcStreamState::TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTable

class TDropCdcStreamAtTable: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    static TTxState::ETxState NextState(TTxState::ETxState state) {
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

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return THolder(new TConfigurePartsAtTable(OperationId));
        case TTxState::Propose:
            return THolder(new NCdcStreamState::TProposeAtTable(OperationId));
        case TTxState::ProposedWaitParts:
            return THolder(new NTableState::TProposedWaitParts(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    explicit TDropCdcStreamAtTable(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
        , State(TTxState::Invalid)
    {
    }

    explicit TDropCdcStreamAtTable(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetDropCdcStream();
        const auto& tableName = op.GetTableName();
        const auto& streamName = op.GetStreamName();

        LOG_N("TDropCdcStreamAtTable Propose"
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
                .IsCommonSensePath()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                TString explain = TStringBuilder() << "path checks failed, path: " << tablePath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                TString explain = TStringBuilder() << "path checks failed, path: " << streamPath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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

        context.DbChanges.PersistTxState(OperationId);

        Y_VERIFY(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_VERIFY(table->AlterVersion != 0);
        Y_VERIFY(!table->AlterData);

        Y_VERIFY(context.SS->CdcStreams.contains(streamPath.Base()->PathId));
        auto stream = context.SS->CdcStreams.at(streamPath.Base()->PathId);

        Y_VERIFY(stream->AlterVersion != 0);
        Y_VERIFY(!stream->AlterData);

        Y_VERIFY(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxDropCdcStreamAtTable, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        tablePath.Base()->LastTxId = OperationId.GetTxId();

        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TDropCdcStreamAtTable");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TDropCdcStreamAtTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

private:
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State;

}; // TDropCdcStreamAtTable

} // anonymous

ISubOperationBase::TPtr CreateDropCdcStreamImpl(TOperationId id, const TTxTransaction& tx) {
    return new TDropCdcStream(id, tx);
}

ISubOperationBase::TPtr CreateDropCdcStreamImpl(TOperationId id, TTxState::ETxState state) {
    return new TDropCdcStream(id, state);
}

ISubOperationBase::TPtr CreateDropCdcStreamAtTable(TOperationId id, const TTxTransaction& tx) {
    return new TDropCdcStreamAtTable(id, tx);
}

ISubOperationBase::TPtr CreateDropCdcStreamAtTable(TOperationId id, TTxState::ETxState state) {
    return new TDropCdcStreamAtTable(id, state);
}

TVector<ISubOperationBase::TPtr> CreateDropCdcStream(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_VERIFY(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream);

    LOG_D("CreateDropCdcStream"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto& op = tx.GetDropCdcStream();
    const auto& tableName = op.GetTableName();
    const auto& streamName = op.GetStreamName();

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

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
            .IsCommonSensePath()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            TString explain = TStringBuilder() << "path checks failed, path: " << tablePath.PathString();
            const auto status = checks.GetStatus(&explain);
            return {CreateReject(opId, status, explain)};
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
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            TString explain = TStringBuilder() << "path checks failed, path: " << streamPath.PathString();
            const auto status = checks.GetStatus(&explain);
            return {CreateReject(opId, status, explain)};
        }
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    TVector<ISubOperationBase::TPtr> result;

    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamAtTable);
        outTx.MutableDropCdcStream()->CopyFrom(op);

        result.push_back(CreateDropCdcStreamAtTable(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl);
        outTx.MutableDrop()->SetName(streamPath.Base()->Name);

        result.push_back(CreateDropCdcStreamImpl(NextPartId(opId, result), outTx));
    }

    for (const auto& [name, pathId] : streamPath.Base()->GetChildren()) {
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        auto implPath = context.SS->PathsById.at(pathId);

        if (implPath->Dropped()) {
            continue;
        }

        auto streamImpl = context.SS->PathsById.at(pathId);
        Y_VERIFY(streamImpl->IsPQGroup());

        auto outTx = TransactionTemplate(streamPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);
        outTx.MutableDrop()->SetName(name);

        result.push_back(CreateDropPQ(NextPartId(opId, result), outTx));
    }

    return result;
}

} // NSchemeShard
} // NKikimr
