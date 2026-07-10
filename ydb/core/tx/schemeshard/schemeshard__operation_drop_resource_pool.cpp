#include "schemeshard__operation_common.h"
#include "schemeshard__operation_common_resource_pool.h"
#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

namespace {

class TPropose : public TSubOperationState {
public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {}

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        YDB_LOG_INFO_CTX(context.Ctx, "HandleReply TEvOperationPlan",
            {"#_context.SS->TabletID", context.SS->TabletID()},
            {"debugHint", DebugHint()},
            {"step", step});

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropResourcePool);

        const TPathId& pathId = txState->TargetPathId;
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);
        const TPathElement::TPtr parentDirPtr = context.SS->PathsById.at(pathPtr->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!pathPtr->Dropped());
        pathPtr->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        context.SS->PersistRemoveResourcePool(db, pathId);

        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        DecAliveChildrenDirect(OperationId, parentDirPtr, context); // for correct discard of ChildrenExist prop
        context.SS->TabletCounters->Simple()[COUNTER_RESOURCE_POOL_COUNT].Sub(1);

        ++parentDirPtr->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDirPtr);
        context.SS->ClearDescribePathCaches(parentDirPtr);
        context.SS->ClearDescribePathCaches(pathPtr);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDirPtr->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "ProgressState",
            {"#_context.SS->TabletID", context.SS->TabletID()},
            {"debugHint", DebugHint()});

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropResourcePool);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TDropResourcePool TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TDropResourcePool : public TSubOperation {
    static TTxState::ETxState NextState() {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TOperationContext& context, const TPath& dstPath) {
        auto checks = dstPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsResourcePool()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (checks) {
            const TResourcePoolInfo::TPtr resourcePool = context.SS->ResourcePools.Value(dstPath->PathId, nullptr);
            if (!resourcePool) {
                result->SetError(NKikimrScheme::StatusSchemeError, "Resource pool doesn't exist");
                return false;
            }
        }

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (dstPath.IsResolved() && dstPath.Base()->IsResourcePool() && (dstPath.Base()->PlannedToDrop() || dstPath.Base()->Dropped())) {
                result->SetPathDropTxId(ui64(dstPath.Base()->DropTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    void CreateTransaction(const TOperationContext& context, const TPathId& resourcePoolPathId) const {
        TTxState& txState = NResourcePool::CreateTransaction(OperationId, context, resourcePoolPathId, TTxState::TxDropResourcePool);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);
    }

    void DropResourcePoolPathElement(const TPath& dstPath) const {
        TPathElement::TPtr resourcePool = dstPath.Base();

        resourcePool->PathState = TPathElement::EPathState::EPathStateDrop;
        resourcePool->DropTxId = OperationId.GetTxId();
        resourcePool->LastTxId = OperationId.GetTxId();
    }

    void PersistDropResourcePool(const TOperationContext& context, const TPath& dstPath) const {
        const TPathId& pathId = dstPath.Base()->PathId;

        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, dstPath->ParentPathId);
        context.MemChanges.GrabResourcePool(context.SS, pathId);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(dstPath->ParentPathId);
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& dropDescription = Transaction.GetDrop();
        const TString& name = dropDescription.GetName();
        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropResourcePool Propose: ",
            {"#_context.SS->TabletID", context.SS->TabletID()},
            {"opId", OperationId},
            {"path", parentPathStr},
            {"name", name});

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& dstPath = dropDescription.HasId()
            ? TPath::Init(context.SS->MakeLocalId(dropDescription.GetId()), context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, context, dstPath));
        RETURN_RESULT_UNLESS(NResourcePool::IsApplyIfChecksPassed(Transaction, result, context));

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        auto guard = context.DbGuard();
        PersistDropResourcePool(context, dstPath);
        CreateTransaction(context, dstPath.Base()->PathId);
        DropResourcePoolPathElement(dstPath);

        context.OnComplete.ActivateTx(OperationId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropResourcePool AbortPropose",
            {"#_context.SS->TabletID", context.SS->TabletID()},
            {"opId", OperationId});
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropResourcePool AbortUnsafe",
            {"#_context.SS->TabletID", context.SS->TabletID()},
            {"opId", OperationId},
            {"txId", forceDropTxId});
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateDropResourcePool(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropResourcePool>(id, tx);
}

ISubOperation::TPtr CreateDropResourcePool(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropResourcePool>(id, state);
}

}  // namespace NKikimr::NSchemeShard
