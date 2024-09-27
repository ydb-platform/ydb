#include "schemeshard__operation_common_abstract_object.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"


namespace NKikimr::NSchemeShard {

namespace {

class TPropose : public TSubOperationState {
public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {}

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        LOG_I(DebugHint() << "HandleReply TEvOperationPlan: step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterAbstractObject);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);
        context.SS->ClearDescribePathCaches(pathPtr);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterAbstractObject);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TAlterAbstractObject TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TAlterAbstractObject : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TString& acl) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .FailOnWrongType(TPathElement::EPathType::EPathTypeAbstractObject)
            .IsValidLeafName()
            .DepthLimit()
            .PathsLimit()
            .DirChildrenLimit()
            .IsValidACL(acl);

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (dstPath.IsResolved()) {
                result->SetPathCreateTxId(static_cast<ui64>(dstPath.Base()->CreateTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    TPathElement::TPtr ReplaceAbstractObjectPathElement(const TPath& dstPath) const {
        TPathElement::TPtr abstractObject = dstPath.Base();

        abstractObject->PathState = TPathElement::EPathState::EPathStateAlter;
        abstractObject->LastTxId  = OperationId.GetTxId();

        return abstractObject;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& abstractObjectDescription = Transaction.GetModifyAbstractObject();
        const TString& name = abstractObjectDescription.GetObject();
        LOG_N("TAlterAbstractObject Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NAbstractObject::IsParentPathValid(result, parentPath));

        const TPath& dstPath = parentPath.Child(name);
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl));
        RETURN_RESULT_UNLESS(NAbstractObject::IsApplyIfChecksPassed(Transaction, result, context));

        const auto& oldAbstractObjectInfo = context.SS->AbstractObjects.Value(dstPath->PathId, nullptr);
        Y_ABORT_UNLESS(oldAbstractObjectInfo);

        const auto buildResult = NAbstractObject::BuildObjectMetadata(abstractObjectDescription, *context.SS, nullptr);
        if (buildResult.IsFail()) {
            result->SetStatus(NKikimrScheme::EStatus::StatusInvalidParameter, buildResult.GetErrorMessage());
            return result;
        }

        const TAbstractObjectInfo::TPtr abstractObjectInfo = NAbstractObject::ModifyAbstractObject(buildResult.GetResult(), oldAbstractObjectInfo);
        Y_ABORT_UNLESS(abstractObjectInfo);

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
        const TPathElement::TPtr abstractObject = ReplaceAbstractObjectPathElement(dstPath);
        NAbstractObject::CreateTransaction(OperationId, context, abstractObject->PathId, TTxState::TxAlterAbstractObject);
        NAbstractObject::RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        NAbstractObject::AdvanceTransactionStateToPropose(OperationId, context, db);
        NAbstractObject::PersistAbstractObject(OperationId, context, db, abstractObject, abstractObjectInfo, acl);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterAbstractObject AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TAlterAbstractObject");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterAbstractObject AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateAlterAbstractObject(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterAbstractObject>(id, tx);
}

ISubOperation::TPtr CreateAlterAbstractObject(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterAbstractObject>(id, state);
}

}  // namespace NKikimr::NSchemeShard
