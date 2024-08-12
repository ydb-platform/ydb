#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateView::TPropose"
            << ", opId: " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {}

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << " ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateView);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << " HandleReply TEvPrivate::TEvOperationPlan"
                << ", step: " << step
        );

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateView);

        context.SS->TabletCounters->Simple()[COUNTER_VIEW_COUNT].Add(1);
        
        const auto pathId = txState->TargetPathId;
        auto path = TPath::Init(pathId, context.SS);

        NIceDb::TNiceDb db(context.GetDB());

        path.Base()->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }
};

TViewInfo::TPtr CreateView(const NKikimrSchemeOp::TViewDescription& desc) {
    TViewInfo::TPtr viewInfo = new TViewInfo;
    viewInfo->AlterVersion = 1;
    viewInfo->QueryText = desc.GetQueryText();
    return viewInfo;
}

class TCreateView: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
            case TTxState::Waiting:
            case TTxState::Propose:
                return TTxState::Done;
            default:
                return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
            case TTxState::Waiting:
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

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisting = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& viewDescription = Transaction.GetCreateView();

        const TString& name = viewDescription.GetName();
        
        LOG_N("TCreateView Propose"
                            << ", path: " << parentPathStr << "/" << name
                            << ", opId: " << OperationId
        );

        LOG_D("TCreateView Propose"
                            << ", path: " << parentPathStr << "/" << name
                            << ", opId: " << OperationId
                            << ", viewDescription: " << viewDescription.ShortDebugString()
        );

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        const auto parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            const auto checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory()
                .FailOnRestrictedCreateInTempZone();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            const auto checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeView, acceptExisting);
            } else {
                checks
                    .NotEmpty();
            }

            if (checks) {
                checks
                    .IsValidLeafName()
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
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

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        const auto viewPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, viewPathId);
        context.MemChanges.GrabPath(context.SS, parentPath->PathId);
        context.MemChanges.GrabNewView(context.SS, viewPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(viewPathId);
        context.DbChanges.PersistPath(parentPath->PathId);
        context.DbChanges.PersistView(viewPathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(owner, viewPathId);
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
        result->SetPathId(viewPathId.LocalPathId);

        TPathElement::TPtr viewPath = dstPath.Base();
        viewPath->PathState = TPathElement::EPathState::EPathStateCreate;
        viewPath->PathType = TPathElement::EPathType::EPathTypeView;
        viewPath->CreateTxId = OperationId.GetTxId();
        viewPath->LastTxId = OperationId.GetTxId();
        if (!acl.empty()) {
            viewPath->ApplyACL(acl);
        }

        TViewInfo::TPtr viewInfo = CreateView(viewDescription);
        if (!viewInfo) {
            result->SetStatus(NKikimrScheme::StatusInvalidParameter);
            return result;
        }
        context.SS->Views[viewPathId] = viewInfo;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateView, viewPathId);
        txState.State = TTxState::Propose;
        context.OnComplete.ActivateTx(OperationId);

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateView AbortPropose"
            << ", opId: " << OperationId
        );
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateView AbortUnsafe"
                            << ", opId: " << OperationId
                            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewView(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateView>(id, tx);
}

ISubOperation::TPtr CreateNewView(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateView>(id, state);
}

}
