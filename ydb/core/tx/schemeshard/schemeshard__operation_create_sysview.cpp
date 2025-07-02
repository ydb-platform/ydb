#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)
#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->SelfTabletId() << "] " << stream)

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose : public TSubOperationState {
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateSysView::TPropose"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSysView);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSysView);

        context.SS->TabletCounters->Simple()[COUNTER_SYS_VIEW_COUNT].Add(1);

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

TSysViewInfo::TPtr CreateSysView(const NKikimrSchemeOp::TSysViewDescription& desc) {
    TSysViewInfo::TPtr sysViewInfo = new TSysViewInfo;
    sysViewInfo->AlterVersion = 1;
    sysViewInfo->Type = desc.GetType();
    return sysViewInfo;
}

class TCreateSysView : public TSubOperation {
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
        const auto& sysViewDescription = Transaction.GetCreateSysView();

        const TString& name = sysViewDescription.GetName();

        LOG_N("TCreateSysView Propose"
            << ", path: " << parentPathStr << "/" << name
            << ", opId: " << OperationId
        );

        LOG_D("TCreateSysView Propose"
            << ", path: " << parentPathStr << "/" << name
            << ", opId: " << OperationId
            << ", sysViewDescription: " << sysViewDescription.ShortDebugString()
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
                .IsSysViewDirectory();

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
                    .FailOnExist(TPathElement::EPathType::EPathTypeSysView, acceptExisting);
            } else {
                checks
                    .NotEmpty();
            }

            if (checks) {
                checks
                    .IsValidLeafName(context.UserToken.Get())
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

        if (!NKikimrSysView::ESysViewType_IsValid(sysViewDescription.GetType())) {
            errStr = TStringBuilder()
                << "error: unsupported system view type "
                << static_cast<uint32_t>(sysViewDescription.GetType());
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
        }

        auto guard = context.DbGuard();
        const auto sysViewPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, sysViewPathId);
        context.MemChanges.GrabPath(context.SS, parentPath->PathId);
        context.MemChanges.GrabNewSysView(context.SS, sysViewPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(sysViewPathId);
        context.DbChanges.PersistPath(parentPath->PathId);
        context.DbChanges.PersistSysView(sysViewPathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(owner, sysViewPathId);
        dstPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenSafeWithUndo(OperationId, parentPath, context); // for correct discard of ChildrenExist prop

        result->SetPathId(sysViewPathId.LocalPathId);

        TPathElement::TPtr sysViewPath = dstPath.Base();
        sysViewPath->PathState = TPathElement::EPathState::EPathStateCreate;
        sysViewPath->PathType = TPathElement::EPathType::EPathTypeSysView;
        sysViewPath->CreateTxId = OperationId.GetTxId();
        sysViewPath->LastTxId = OperationId.GetTxId();
        if (!acl.empty()) {
            sysViewPath->ApplyACL(acl);
        }

        TSysViewInfo::TPtr sysViewInfo = CreateSysView(sysViewDescription);
        context.SS->SysViews[sysViewPathId] = sysViewInfo;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateSysView, sysViewPathId);
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
        LOG_N("TCreateSysView AbortPropose"
            << ", opId: " << OperationId
        );
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateSysView AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateSysView>;

namespace NOperation {

template <>
std::optional<TString> GetTargetName<TTag>(TTag, const TTxTransaction& tx) {
    return tx.GetCreateSysView().GetName();
}

template <>
bool SetName<TTag>(TTag, TTxTransaction& tx, const TString& name) {
    tx.MutableCreateSysView()->SetName(name);
    return true;
}

} // namespace NOperation

ISubOperation::TPtr CreateNewSysView(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateSysView>(id, tx);
}

ISubOperation::TPtr CreateNewSysView(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateSysView>(id, state);
}

}
