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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterMetadataObject);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterMetadataObject);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TAlterMetadataObject TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TAlterMetadataObject : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TString& acl, const TPathElement::EPathType expectedPathType) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .FailOnWrongType(expectedPathType)
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

    TPathElement::TPtr ReplacePathElement(const TPath& dstPath) const {
        TPathElement::TPtr object = dstPath.Base();

        object->PathState = TPathElement::EPathState::EPathStateAlter;
        object->LastTxId  = OperationId.GetTxId();

        return object;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        auto update = NOperations::TMetadataUpdate::MakeUpdate(Transaction);
        AFL_VERIFY(update);

        LOG_N("TAlterMetadataObject Propose: opId# " << OperationId << ", path# " << update->GetPathString());

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        TPath dstPath = TPath::Resolve(update->GetPathString(), context.SS);
        TPath parentPath = dstPath.Parent();
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(NMetadataObject::IsParentPathValid(result, parentPath));
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl, update->GetObjectPathType()));
        RETURN_RESULT_UNLESS(NMetadataObject::IsApplyIfChecksPassed(Transaction, result, context));

        std::shared_ptr<NOperations::ISSEntity> originalEntity;
        originalEntity = update->MakeEntity(dstPath->PathId);
        if (auto status = originalEntity->Initialize(NOperations::TEntityInitializationContext(&context)); status.IsFail()) {
            result->SetError(NKikimrScheme::StatusSchemeError, status.GetErrorMessage());
            return result;
        }

        NOperations::TUpdateInitializationContext initializationContext(&context, &Transaction, OperationId.GetTxId().GetValue(), originalEntity.get());
        if (auto status = update->Initialize(initializationContext); status.IsFail()) {
            result->SetError(NKikimrScheme::StatusSchemeError, status.GetErrorMessage());
            return result;
        }

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
        const TPathElement::TPtr object = ReplacePathElement(dstPath);
        NMetadataObject::CreateTransaction(OperationId, context, object->PathId, TTxState::TxAlterMetadataObject);
        NMetadataObject::RegisterParentPathDependencies(OperationId, context, parentPath);

        NIceDb::TNiceDb db(context.GetDB());
        NOperations::TUpdateStartContext executionContext(&dstPath, &context, &db);
        if (auto status = update->Start(executionContext); status.IsFail()) {
            result->SetError(NKikimrScheme::StatusSchemeError, status.GetErrorMessage());
            return result;
        }

        NMetadataObject::AdvanceTransactionStateToPropose(OperationId, context, db);
        NMetadataObject::PersistOperation(OperationId, context, db, object, acl, false);
        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);
        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterMetadataObject AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TAlterMetadataObject");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterMetadataObject AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateAlterMetadataObject(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterMetadataObject>(id, tx);
}

ISubOperation::TPtr CreateAlterMetadataObject(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterMetadataObject>(id, state);
}

}  // namespace NKikimr::NSchemeShard
