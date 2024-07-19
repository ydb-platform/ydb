#include "schemeshard__operation_common_resource_pool.h"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterResourcePool);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterResourcePool);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TAlterResourcePool TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TAlterResourcePool : public TSubOperation {
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
            .FailOnWrongType(TPathElement::EPathType::EPathTypeResourcePool)
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

    TPathElement::TPtr ReplaceResourcePoolPathElement(const TPath& dstPath) const {
        TPathElement::TPtr resourcePool = dstPath.Base();

        resourcePool->PathState = TPathElement::EPathState::EPathStateAlter;
        resourcePool->LastTxId  = OperationId.GetTxId();

        return resourcePool;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& resourcePoolDescription = Transaction.GetCreateResourcePool();
        const TString& name = resourcePoolDescription.GetName();
        LOG_N("TAlterResourcePool Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        if (context.SS->IsServerlessDomain(TPath::Init(context.SS->RootPathId(), context.SS))) {
            if (!context.SS->EnableResourcePoolsOnServerless) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "Resource pools are disabled for serverless domains. Please contact your system administrator to enable it");
                return result;
            }
        }

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NResourcePool::IsParentPathValid(result, parentPath));

        const TPath& dstPath = parentPath.Child(name);
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl));
        RETURN_RESULT_UNLESS(NResourcePool::IsApplyIfChecksPassed(Transaction, result, context));
        RETURN_RESULT_UNLESS(NResourcePool::IsDescriptionValid(result, resourcePoolDescription));

        const auto& oldResourcePoolInfo = context.SS->ResourcePools.Value(dstPath->PathId, nullptr);
        Y_ABORT_UNLESS(oldResourcePoolInfo);
        const TResourcePoolInfo::TPtr resourcePoolInfo = NResourcePool::ModifyResourcePool(resourcePoolDescription, oldResourcePoolInfo);
        Y_ABORT_UNLESS(resourcePoolInfo);

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
        const TPathElement::TPtr resourcePool = ReplaceResourcePoolPathElement(dstPath);
        NResourcePool::CreateTransaction(OperationId, context, resourcePool->PathId, TTxState::TxAlterResourcePool);
        NResourcePool::RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        NResourcePool::AdvanceTransactionStateToPropose(OperationId, context, db);
        NResourcePool::PersistResourcePool(OperationId, context, db, resourcePool, resourcePoolInfo, acl);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterResourcePool AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TAlterResourcePool");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterResourcePool AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateAlterResourcePool(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterResourcePool>(id, tx);
}

ISubOperation::TPtr CreateAlterResourcePool(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterResourcePool>(id, state);
}

}  // namespace NKikimr::NSchemeShard
