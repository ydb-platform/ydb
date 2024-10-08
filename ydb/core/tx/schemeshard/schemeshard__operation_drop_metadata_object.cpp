#include "schemeshard__operation_common_metadata_object.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/schemeshard/operations/metadata/abstract/update.h>

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropMetadataObject);

        const TPathId& pathId = txState->TargetPathId;
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);
        const TPathElement::TPtr parentDirPtr = context.SS->PathsById.at(pathPtr->ParentPathId);
    
        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!pathPtr->Dropped());
        pathPtr->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);

        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside();
        parentDirPtr->DecAliveChildren();

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
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropMetadataObject);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TDropMetadataObject TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TDropMetadataObject : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TOperationContext& /*context*/, const TPath& dstPath, const TPathElement::EPathType expectedPathType) {
        auto checks = dstPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (checks) {
            if (dstPath.Base()->PathType != expectedPathType) {
                result->SetError(NKikimrScheme::StatusSchemeError, "Object has different type");
                return false;
            }
        }

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (dstPath.IsResolved() && dstPath.Base()->PathType == expectedPathType && (dstPath.Base()->PlannedToDrop() || dstPath.Base()->Dropped())) {
                result->SetPathDropTxId(ui64(dstPath.Base()->DropTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    void CreateTransaction(const TOperationContext& context, const TPathId& objectPathId) const {
        TTxState& txState = NMetadataObject::CreateTransaction(OperationId, context, objectPathId, TTxState::TxDropMetadataObject);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);
    }

    void DropPathElement(const TPath& dstPath) const {
        TPathElement::TPtr object = dstPath.Base();

        object->PathState = TPathElement::EPathState::EPathStateDrop;
        object->DropTxId = OperationId.GetTxId();
        object->LastTxId = OperationId.GetTxId();
    }

    void PersistDropPath(const TOperationContext& context, const TPath& dstPath) const {
        const TPathId& pathId = dstPath.Base()->PathId;

        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, dstPath->ParentPathId);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(dstPath->ParentPathId);
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        auto update = NOperations::TMetadataUpdate::MakeUpdate(Transaction);
        AFL_VERIFY(update);

        LOG_N("TDropMetadataObject Propose: opId# " << OperationId << ", path# " << update->GetPathString());

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        TPath dstPath = TPath::Resolve(update->GetPathString(), context.SS);
        TPath parentPath = dstPath.Parent();
        RETURN_RESULT_UNLESS(NMetadataObject::IsParentPathValid(result, parentPath));
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, context, dstPath, update->GetObjectPathType()));
        RETURN_RESULT_UNLESS(NMetadataObject::IsApplyIfChecksPassed(Transaction, result, context));

        std::shared_ptr<NOperations::ISSEntity> originalEntity;
        if (dstPath.IsResolved()) {
            originalEntity = update->MakeEntity(dstPath->PathId);
            if (auto status = originalEntity->Initialize(NOperations::TEntityInitializationContext(&context)); status.IsFail()) {
                result->SetError(NKikimrScheme::StatusSchemeError, status.GetErrorMessage());
                return result;
            }
        }

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        NIceDb::TNiceDb db(context.GetDB());
        NOperations::TUpdateStartContext executionContext(&dstPath, &context, &db);
        if (auto status = update->Start(executionContext); status.IsFail()) {
            result->SetError(NKikimrScheme::StatusSchemeError, status.GetErrorMessage());
            return result;
        }

        auto guard = context.DbGuard();
        PersistDropPath(context, dstPath);
        CreateTransaction(context, dstPath.Base()->PathId);
        DropPathElement(dstPath);

        context.OnComplete.ActivateTx(OperationId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TDropMetadataObject AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TDropMetadataObject");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TDropMetadataObject AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateDropMetadataObject(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropMetadataObject>(id, tx);
}

ISubOperation::TPtr CreateDropMetadataObject(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropMetadataObject>(id, state);
}

}  // namespace NKikimr::NSchemeShard
