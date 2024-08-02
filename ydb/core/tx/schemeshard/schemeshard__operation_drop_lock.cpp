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
            << "TDropLock TPropose"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropLock);

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

class TDropLock: public TSubOperation {
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
    explicit TDropLock(TOperationId id, const TTxTransaction& tx)
        : TSubOperation(id, tx)
        , ProposeToCoordinator(AppData()->FeatureFlags.GetEnableChangefeedInitialScan())
    {
    }

    explicit TDropLock(TOperationId id, TTxState::ETxState state)
        : TSubOperation(id, state)
        , ProposeToCoordinator(AppData()->FeatureFlags.GetEnableChangefeedInitialScan())
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetLockConfig();

        LOG_N("TDropLock Propose"
            << ": opId# " << OperationId
            << ", path# " << workingDir << "/" << op.GetName());

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

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

        const auto dstPath = parentPath.Child(op.GetName());
        {
            const auto checks = dstPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotUnderDeleting();

            if (checks) {
                if (!parentPath.IsTableIndex()) {
                    checks.IsCommonSensePath();
                }
                if (dstPath.IsUnderOperation()) { // may be part of a consistent operation
                    checks.IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks.NotUnderOperation();
                }
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

        const auto& lockguard = Transaction.GetLockGuard();
        const auto lockOwner = TTxId(lockguard.GetOwnerTxId());
        if (!lockguard.HasOwnerTxId() || !lockOwner) {
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, TStringBuilder() << "path checks failed"
                << ", lock owner tx id not set"
                << ", path: " << dstPath.PathString());
            return result;
        }

        const auto pathId = dstPath.Base()->PathId;
        result->SetPathId(pathId.LocalPathId);

        if (!dstPath.LockedBy()) {
            result->SetError(TEvSchemeShard::EStatus::StatusAlreadyExists, TStringBuilder() << "path checks failed"
                << ", path already unlocked"
                << ", path: " << dstPath.PathString());
            return result;
        }

        TString errStr;
        if (!context.SS->CheckLocks(pathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabLongLock(context.SS, pathId, lockOwner);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistUnLock(pathId);
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropLock, pathId);
        txState.State = ProposeToCoordinator ? TTxState::Propose : TTxState::Done;

        dstPath.Base()->LastTxId = OperationId.GetTxId();
        dstPath.Base()->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        if (dstPath.Base()->IsTable()) {
            auto table = context.SS->Tables.at(pathId);
            Y_DEBUG_ABORT_UNLESS(table->GetSplitOpsInFlight().size() == 0);

            for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
            }
        }

        auto lockedBy = context.SS->LockedPaths[pathId];
        Y_ABORT_UNLESS(lockedBy == lockOwner);
        context.SS->LockedPaths.erase(pathId);
        context.SS->TabletCounters->Simple()[COUNTER_LOCKS_COUNT].Sub(1);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TDropLock AbortPropose"
            << ": opId# " << OperationId);
        context.SS->TabletCounters->Simple()[COUNTER_LOCKS_COUNT].Add(1);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TDropLock AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

ISubOperation::TPtr DropLock(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropLock>(id, tx);
}

ISubOperation::TPtr DropLock(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropLock>(id, state);
}

}
