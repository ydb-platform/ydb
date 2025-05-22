#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropView TPropose"
            << ", opId: " << OperationId;
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    { }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << " ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropView);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << " HandleReply TEvOperationPlan"
            << ", step: " << step
        );

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropView);

        TPathId pathId = txState->TargetPathId;
        auto path = context.SS->PathsById.at(pathId);
        auto parentDir = context.SS->PathsById.at(path->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        DecAliveChildrenDirect(OperationId, parentDir, context); // for correct discard of ChildrenExist prop

        context.SS->TabletCounters->Simple()[COUNTER_VIEW_COUNT].Sub(1);
        context.SS->PersistRemoveView(db, pathId);

        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.SS->ClearDescribePathCaches(path);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);

        return true;
    }
};

class TDropView: public TSubOperation {
    TTxState::ETxState NextState() const {
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
        const ui64 ssId = context.SS->TabletID();
        const auto& drop = Transaction.GetDrop();

        const TString& workingDir = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        LOG_N("TDropView Propose"
            << ", opId: " << OperationId
            << ", path: " << workingDir << "/" << name
        );

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ssId);

        TPath path = drop.HasId()
            ? TPath::Init(context.SS->MakeLocalId(drop.GetId()), context.SS)
            : TPath::Resolve(workingDir, context.SS).Dive(name);
        {
            auto checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsView()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved()
                    && path.Base()->IsView()
                    && (path.Base()->PlannedToDrop() || path.Base()->Dropped())
                    ) {
                    result->SetPathDropTxId(ui64(path.Base()->DropTxId));
                    result->SetPathId(path.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TViewInfo::TPtr view = context.SS->Views.Value(path->PathId, nullptr);
        Y_ABORT_UNLESS(view);

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        const auto pathId = path.Base()->PathId;
        result->SetPathId(pathId.LocalPathId);

        auto guard = context.DbGuard();
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, path->ParentPathId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(path->ParentPathId);
        context.DbChanges.PersistTxState(OperationId);

        path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        path.Base()->DropTxId = OperationId.GetTxId();
        path.Base()->LastTxId = OperationId.GetTxId();

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropView, path.Base()->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TDropView AbortPropose"
            << ", opId: " << OperationId
        );
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TDropView AbortUnsafe"
            << ", opId: " << OperationId
            << ", txId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropView(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropView>(id, tx);
}

ISubOperation::TPtr CreateDropView(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropView>(id, state);
}

}

