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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateResourcePool);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->TabletCounters->Simple()[COUNTER_RESOURCE_POOL_COUNT].Add(1);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateResourcePool);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TCreateResourcePool TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TCreateResourcePool : public TSubOperation {
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
                .FailOnExist(TPathElement::EPathType::EPathTypeResourcePool, acceptExisted);
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

    TPathElement::TPtr CreateResourcePoolPathElement(const TPath& dstPath) const {
        TPathElement::TPtr resourcePool = dstPath.Base();

        resourcePool->CreateTxId = OperationId.GetTxId();
        resourcePool->PathType = TPathElement::EPathType::EPathTypeResourcePool;
        resourcePool->PathState = TPathElement::EPathState::EPathStateCreate;
        resourcePool->LastTxId  = OperationId.GetTxId();

        return resourcePool;
    }

    static void UpdatePathSizeCounts(const TPath& parentPath, const TPath& dstPath) {
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& resourcePoolDescription = Transaction.GetCreateResourcePool();
        const TString& name = resourcePoolDescription.GetName();
        LOG_N("TCreateResourcePool Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

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

        TPath dstPath = parentPath.Child(name);
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl, !Transaction.GetFailOnExist()));
        RETURN_RESULT_UNLESS(NResourcePool::IsApplyIfChecksPassed(Transaction, result, context));
        RETURN_RESULT_UNLESS(NResourcePool::IsDescriptionValid(result, resourcePoolDescription));

        const TResourcePoolInfo::TPtr resourcePoolInfo = NResourcePool::CreateResourcePool(resourcePoolDescription, 1);
        Y_ABORT_UNLESS(resourcePoolInfo);

        AddPathInSchemeShard(result, dstPath, owner);
        const TPathElement::TPtr resourcePool = CreateResourcePoolPathElement(dstPath);
        NResourcePool::CreateTransaction(OperationId, context, resourcePool->PathId, TTxState::TxCreateResourcePool);
        NResourcePool::RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        NResourcePool::AdvanceTransactionStateToPropose(OperationId, context, db);
        NResourcePool::PersistResourcePool(OperationId, context, db, resourcePool, resourcePoolInfo, acl);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        UpdatePathSizeCounts(parentPath, dstPath);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateResourcePool AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateResourcePool");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateResourcePool AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateNewResourcePool(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateResourcePool>(id, tx);
}

ISubOperation::TPtr CreateNewResourcePool(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateResourcePool>(id, state);
}

}  // namespace NKikimr::NSchemeShard
