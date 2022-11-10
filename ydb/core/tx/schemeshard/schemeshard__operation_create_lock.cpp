#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

class TCreateLock: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State;

    TTxState::ETxState NextState() {
        return TTxState::Done;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Waiting:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Waiting:
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
    explicit TCreateLock(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
        , State(TTxState::Invalid)
    {
    }

    explicit TCreateLock(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
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
                .IsCommonSensePath()
                .IsLikeDirectory();

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
                .IsCommonSensePath();

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
            result->SetError(TEvSchemeShard::EStatus::StatusAlreadyExists, TStringBuilder() << "path checks failed"
                << ", path already locked by this operation"
                << ", path: " << tablePath.PathString());
            return result;
        }

        TString errStr;
        if (!context.SS->CheckLocks(pathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        if (!context.SS->CheckInFlightLimit(TTxState::TxCreateLock, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        tablePath.Base()->LastTxId = OperationId.GetTxId();
        tablePath.Base()->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateLock, pathId);
        txState.State = TTxState::Done;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistTxState(db, OperationId);

        auto table = context.SS->Tables.at(pathId);
        for (const auto& splitTx : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
        }

        context.SS->LockedPaths[pathId] = OperationId.GetTxId();
        context.SS->PersistLongLock(db, OperationId.GetTxId(), pathId);
        context.SS->TabletCounters->Simple()[COUNTER_LOCKS_COUNT].Add(1);

        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TCreateLock");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateLock AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

ISubOperationBase::TPtr CreateLock(TOperationId id, const TTxTransaction& tx) {
    return new TCreateLock(id, tx);
}

ISubOperationBase::TPtr CreateLock(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TCreateLock(id, state);
}

}
