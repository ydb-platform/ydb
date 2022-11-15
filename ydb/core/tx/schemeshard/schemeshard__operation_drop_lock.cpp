#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TDropLock: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Done;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
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

        auto schema = Transaction.GetLockConfig();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = schema.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropLock Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
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

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotUnderDeleting()
                .NotUnderOperation()
                .IsCommonSensePath();

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
        auto lockOwner = TTxId(lockguard.GetOwnerTxId());
        if (!lockguard.HasOwnerTxId() || !lockOwner) {
            TString explain = TStringBuilder()
                << "lock owner tx id unset"
                << " path: " << dstPath.PathString();
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, explain);
            return result;
        }

        TPathElement::TPtr pathEl = dstPath.Base();
        TPathId pathId = pathEl->PathId;
        result->SetPathId(pathId.LocalPathId);

        if (!dstPath.LockedBy()) {
            TString explain = TStringBuilder()
                << "dst path fail checks"
                << ", path already unlocked"
                << ", path: " << dstPath.PathString();
            result->SetError(TEvSchemeShard::EStatus::StatusAlreadyExists, explain);
            return result;
        }

        TString errStr;
        if (!context.SS->CheckLocks(pathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        pathEl->LastTxId = OperationId.GetTxId();
        pathEl->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropLock, pathId);
        txState.State = TTxState::Done;
        context.SS->PersistTxState(db, OperationId);

        if (pathEl->IsTable()) {
            TTableInfo::TPtr table = context.SS->Tables.at(pathId);
            for (auto splitTx: table->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
            }
            Y_VERIFY_DEBUG(table->GetSplitOpsInFlight().size() == 0);
        }

        auto lockedBy = context.SS->LockedPaths[pathId];
        Y_VERIFY(lockedBy == lockOwner);
        context.SS->LockedPaths.erase(pathId);

        context.SS->PersistUnLock(db, pathId);
        context.SS->TabletCounters->Simple()[COUNTER_LOCKS_COUNT].Sub(1);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TDropLock");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropLock AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperationBase::TPtr DropLock(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropLock>(id, tx);
}

ISubOperationBase::TPtr DropLock(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TDropLock>(id, state);
}

}
