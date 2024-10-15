#include "schemeshard__operation_common.h"
#include "schemeshard__operation_common_metadata_object.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/schemeshard/operations/metadata/abstract/object.h>
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateMetadataObject);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateMetadataObject);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TCreateMetadataObject TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TCreateMetadataObject : public TSubOperation {
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

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TString& acl, bool acceptExisted, const TPathElement::EPathType expectedPathType) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard();
        if (dstPath.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(expectedPathType, acceptExisted);
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

    TPathElement::TPtr CreatePathElement(const TPath& dstPath, const TPathElement::EPathType pathType) const {
        TPathElement::TPtr object = dstPath.Base();

        object->CreateTxId = OperationId.GetTxId();
        object->PathType = pathType;
        object->PathState = TPathElement::EPathState::EPathStateCreate;
        object->LastTxId  = OperationId.GetTxId();

        return object;
    }

    static void UpdatePathSizeCounts(const TPath& parentPath, const TPath& dstPath) {
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString pathString = NMetadataObject::GetDestinationPath(Transaction);
        LOG_N("TCreateMetadataObject Propose: opId# " << OperationId << ", path# " << pathString);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        auto update = NOperations::TMetadataUpdateCreate::TFactory::MakeHolder(
            Transaction.GetCreateMetadataObject().GetProperties().GetPropertiesImplCase());

        TPath dstPath = TPath::Resolve(pathString, context.SS);
        TPath parentPath = dstPath.Parent();
        const TString& acl = Transaction.GetModifyACL().GetDiffACL();
        RETURN_RESULT_UNLESS(NMetadataObject::IsParentPathValid(result, parentPath));
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl, !Transaction.GetFailOnExist(), update->GetObjectPathType()));
        RETURN_RESULT_UNLESS(NMetadataObject::IsApplyIfChecksPassed(Transaction, result, context));
        const bool exists = dstPath.IsResolved();

        if (!IsEqualPaths(parentPath.PathString(), update->GetStorageDirectory())) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed,
                "Object must be placed in " + update->GetStorageDirectory() + ", got :" + parentPath.PathString());
            return result;
        }

        AddPathInSchemeShard(result, dstPath, owner);
        const TPathElement::TPtr object = CreatePathElement(dstPath, update->GetObjectPathType());
        NMetadataObject::CreateTransaction(OperationId, context, object->PathId, TTxState::TxCreateMetadataObject);
        NMetadataObject::RegisterParentPathDependencies(OperationId, context, parentPath);

        std::shared_ptr<NOperations::ISSEntity> originalEntity =
            exists ? NOperations::TMetadataEntity::GetEntity(context, dstPath).DetachResult() : update->MakeEntity(object->PathId);

        NOperations::TUpdateInitializationContext initializationContext(
            originalEntity.get(), &context, &Transaction, OperationId.GetTxId().GetValue());
        if (auto status = update->Initialize(initializationContext); status.IsFail()) {
            result->SetError(NKikimrScheme::StatusSchemeError, status.GetErrorMessage());
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());
        NOperations::TUpdateStartContext executionContext(&dstPath, &context, &db);
        if (auto status = update->Start(executionContext); status.IsFail()) {
            result->SetError(NKikimrScheme::StatusSchemeError, status.GetErrorMessage());
            return result;
        }

        NMetadataObject::AdvanceTransactionStateToPropose(OperationId, context, db);
        NMetadataObject::PersistOperation(OperationId, context, db, dstPath.Base(), acl, !exists);
        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);
        UpdatePathSizeCounts(parentPath, dstPath);
        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateMetadataObject AbortPropose: opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateMetadataObject");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateMetadataObject AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateNewMetadataObject(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateMetadataObject>(id, tx);
}

ISubOperation::TPtr CreateNewMetadataObject(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateMetadataObject>(id, state);
}

}  // namespace NKikimr::NSchemeShard
