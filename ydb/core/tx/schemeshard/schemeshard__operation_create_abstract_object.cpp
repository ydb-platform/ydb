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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateAbstractObject);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->TabletCounters->Simple()[COUNTER_ABSTRACT_OBJECT_COUNT].Add(1);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateAbstractObject);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TCreateAbstractObject TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TCreateAbstractObject : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TString& acl, bool acceptExisted) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard();
        if (dstPath.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeAbstractObject, acceptExisted);
        } else {
            checks
                .NotEmpty()
                .NotResolved();
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
                result->SetPathCreateTxId(static_cast<ui64>(dstPath.Base()->CreateTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    static void AddPathInSchemeShard(const THolder<TProposeResponse>& result, TPath& dstPath, const TString& owner) {
        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
    }

    TPathElement::TPtr CreateAbstractObjectPathElement(const TPath& dstPath) const {
        TPathElement::TPtr abstractObject = dstPath.Base();

        abstractObject->CreateTxId = OperationId.GetTxId();
        abstractObject->PathType = TPathElement::EPathType::EPathTypeAbstractObject;
        abstractObject->PathState = TPathElement::EPathState::EPathStateCreate;
        abstractObject->LastTxId  = OperationId.GetTxId();

        return abstractObject;
    }

    static void UpdatePathSizeCounts(const TPath& parentPath, const TPath& dstPath) {
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& abstractObjectDescription = Transaction.GetModifyAbstractObject();
        const TString& name = abstractObjectDescription.GetObject();
        LOG_N("TCreateAbstractObject Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NAbstractObject::IsParentPathValid(result, parentPath, abstractObjectDescription.GetType()));

        TPath dstPath = parentPath.Child(name);
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl, !Transaction.GetFailOnExist()));
        RETURN_RESULT_UNLESS(NAbstractObject::IsApplyIfChecksPassed(Transaction, result, context));

        const auto buildResult = NAbstractObject::BuildObjectMetadata(abstractObjectDescription, *context.SS, nullptr);
        if (buildResult.IsFail()) {
            result->SetStatus(NKikimrScheme::EStatus::StatusInvalidParameter, buildResult.GetErrorMessage());
            return result;
        }

        const auto dependenciesOrError = NAbstractObject::ValidateOperation(
                name, buildResult.GetResult(), NMetadata::NModifications::IOperationsManager::EActivityType::Create, *context.SS);
        if (dependenciesOrError.IsFail()) {
            result->SetStatus(NKikimrScheme::EStatus::StatusInvalidParameter, dependenciesOrError.GetErrorMessage());
            return result;
        }

        const TAbstractObjectInfo::TPtr abstractObjectInfo = NAbstractObject::CreateAbstractObject(buildResult.GetResult(), 1, {});

        AddPathInSchemeShard(result, dstPath, owner);
        const TPathElement::TPtr abstractObject = CreateAbstractObjectPathElement(dstPath);
        NAbstractObject::CreateTransaction(OperationId, context, abstractObject->PathId, TTxState::TxCreateAbstractObject);
        NAbstractObject::RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        NAbstractObject::AdvanceTransactionStateToPropose(OperationId, context, db);
        NAbstractObject::PersistAbstractObject(OperationId, context, db, abstractObject, abstractObjectInfo, acl);
        NAbstractObject::PersistReferences(abstractObject->PathId, dependenciesOrError.GetResult(), {}, context, db);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        UpdatePathSizeCounts(parentPath, dstPath);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateAbstractObject AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateAbstractObject");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateAbstractObject AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateNewAbstractObject(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateAbstractObject>(id, tx);
}

ISubOperation::TPtr CreateNewAbstractObject(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateAbstractObject>(id, state);
}

}  // namespace NKikimr::NSchemeShard
