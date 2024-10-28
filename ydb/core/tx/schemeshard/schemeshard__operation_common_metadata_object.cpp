#include "schemeshard__operation_common_metadata_object.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard::NMetadataObject {

TPath::TChecker IsParentPathValid(const TPath& parentPath) {
    auto checks = parentPath.Check();
    checks.NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .NotUnderDeleting()
        .IsCommonSensePath()
        .IsLikeDirectory();

    return std::move(checks);
}

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath) {
    const auto checks = IsParentPathValid(parentPath);
    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
        return false;
    }
    return true;
}

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context) {
    TString errorStr;
    if (!context.SS->CheckApplyIf(transaction, errorStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
        return false;
    }
    return true;
}

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& pathId, TTxState::ETxType txType) {
    Y_ABORT_UNLESS(!context.SS->FindTx(operationId));
    TTxState& txState = context.SS->CreateTx(operationId, txType, pathId);
    txState.Shards.clear();
    return txState;
}

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath) {
    if (parentPath.Base()->HasActiveChanges()) {
        const TTxId parentTxId = parentPath.Base()->PlannedToCreate()
                                    ? parentPath.Base()->CreateTxId
                                    : parentPath.Base()->LastTxId;
        context.OnComplete.Dependence(parentTxId, operationId.GetTxId());
    }
}

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db) {
    context.SS->ChangeTxState(db, operationId, TTxState::Propose);
    context.OnComplete.ActivateTx(operationId);
}

void PersistOperation(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& objectPath, const TString& acl, const bool created) {
    const auto& pathId = objectPath->PathId;

    if (created) {
        context.SS->IncrementPathDbRefCount(pathId);
    }

    if (!acl.empty()) {
        objectPath->ApplyACL(acl);
    }

    context.SS->PersistPath(db, pathId);
    context.SS->PersistTxState(db, operationId);
}

}   // namespace NKikimr::NSchemeShard::NMetadataObject
