#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

class TPropose: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateLock TPropose"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateLock);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Done);

        return true;
    }

private:
    const TOperationId OperationId;

}; // TPropose

class TCreateLock: public TSubOperation {
    const bool ProposeToCoordinator;

    TTxState::ETxState NextState() const {
        if (ProposeToCoordinator) {
            return TTxState::Propose;
        } else {
            return TTxState::Done;
        }
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
            return NextState();
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
    explicit TCreateLock(TOperationId id, const TTxTransaction& tx)
        : TSubOperation(id, tx)
        , ProposeToCoordinator(AppData()->FeatureFlags.GetEnableChangefeedInitialScan())
    {
    }

    explicit TCreateLock(TOperationId id, TTxState::ETxState state)
        : TSubOperation(id, state)
        , ProposeToCoordinator(AppData()->FeatureFlags.GetEnableChangefeedInitialScan())
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetLockConfig();

        LOG_N("TCreateLock Propose"
            << ": opId# " << OperationId
            << ", path# " << workingDir << "/" << op.GetName());

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        if (!Transaction.HasLockConfig()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "no locking config present");
            return result;
        }

        const auto parentPath = TPath::Resolve(workingDir, context.SS);
        {
            const auto checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsLikeDirectory();

            if (checks && !parentPath.IsTableIndex()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto tablePath = parentPath.Child(op.GetName());
        {
            const auto checks = tablePath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .NotUnderOperation()
                .IsTable()
                .NotAsyncReplicaTable();

            if (checks && !parentPath.IsTableIndex()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (tablePath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(tablePath.Base()->CreateTxId));
                    result->SetPathId(tablePath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        const auto pathId = tablePath.Base()->PathId;
        result->SetPathId(pathId.LocalPathId);

        if (tablePath.LockedBy() == OperationId.GetTxId()) {
            result->SetError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "path checks failed"
                << ", path already locked by this operation"
                << ", path: " << tablePath.PathString());
            return result;
        }

        TString errStr;
        if (!context.SS->CheckLocks(pathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabNewLongLock(context.SS, pathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistLongLock(pathId, OperationId.GetTxId());
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateLock, pathId);
        txState.State = ProposeToCoordinator ? TTxState::Propose : TTxState::Done;

        tablePath.Base()->LastTxId = OperationId.GetTxId();
        tablePath.Base()->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }

        context.SS->LockedPaths[pathId] = OperationId.GetTxId();
        context.SS->TabletCounters->Simple()[COUNTER_LOCKS_COUNT].Add(1);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateLock AbortPropose"
            << ": opId# " << OperationId);
        context.SS->TabletCounters->Simple()[COUNTER_LOCKS_COUNT].Sub(1);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateLock AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

ISubOperation::TPtr CreateLock(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateLock>(id, tx);
}

ISubOperation::TPtr CreateLock(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateLock>(id, state);
}

}
