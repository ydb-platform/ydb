#include "schemeshard__operation_common_resource_pool.h"
#include "schemeshard_impl.h"

#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NSchemeShard::NAbstractObject {

namespace {

NMetadata::IClassBehaviour::TPtr GetBehaviourVerified(const TString& typeId) {
    const auto cBehaviour = NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(typeId));
    AFL_VERIFY(cBehaviour)("type_id", typeId);
    return cBehaviour;
}

NMetadata::NModifications::IObjectManager::TPtr GetObjectManagerVerified(const TString& typeId) {
    const auto manager = GetBehaviourVerified(typeId)->GetObjectManager();
    AFL_VERIFY(manager)("type_id", typeId);
    return manager;
}

NMetadata::NModifications::TSchemeObjectOperationsManager::TPtr GetOperationsManagerVerified(const TString& typeId) {
    const auto abstractManager = GetBehaviourVerified(typeId)->GetOperationsManager();
    AFL_VERIFY(abstractManager)("type_id", typeId);
    const auto schemeManager = std::dynamic_pointer_cast<NMetadata::NModifications::TSchemeObjectOperationsManager>(abstractManager);
    AFL_VERIFY(schemeManager)("type_id", typeId);
    return schemeManager;
}

}   // namespace

TPath::TChecker IsParentPathValid(const TPath& parentPath) {
    auto checks = parentPath.Check();
    checks.NotUnderDomainUpgrade().IsAtLocalSchemeShard().IsResolved().NotDeleted().NotUnderDeleting().IsCommonSensePath().IsLikeDirectory();

    return std::move(checks);
}

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath, const TString& typeId) {
    const TString& abstractObjectsDir = GetBehaviourVerified(typeId)->GetStorageTablePath();
    if (parentPath.PathString() != abstractObjectsDir) {
        result->SetError(NKikimrScheme::EStatus::StatusSchemeError,
            TStringBuilder() << typeId << " objects shoud be placed in " << abstractObjectsDir << ", got " << parentPath.PathString());
        return false;
    }

    const auto checks = IsParentPathValid(parentPath);
    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
    }

    return static_cast<bool>(checks);
}

TConclusion<NMetadata::NModifications::TBaseObject::TPtr> BuildObjectMetadata(const NKikimrSchemeOp::TModifyAbstractObject& description,
    TSchemeShard& context, const NMetadata::NModifications::TBaseObject::TPtr& oldMetadata) {
    const TString& typeId = description.GetType();

    NYql::TObjectSettingsImpl settings;
    if (!settings.DeserializeFromProto(description)) {
        return TConclusionStatus::Fail("Can't deserialize object");
    }

    const auto patch = GetOperationsManagerVerified(typeId)->BuildPatchFromSettings(settings, context);
    if (patch.IsFail()) {
        return patch;
    }

    const auto objectManager = GetObjectManagerVerified(typeId);
    if (oldMetadata) {
        return objectManager->ApplyPatch(oldMetadata, patch.GetResult());
    } else {
        if (auto result = objectManager->DeserializeFromRecord(patch.GetResult())) {
            return result;
        }
        return TConclusionStatus::Fail("Can't deserialize object");
    }
}

TConclusionStatus ValidateOperation(const TString& name, const NMetadata::NModifications::TBaseObject::TPtr& object,
    const NMetadata::NModifications::IOperationsManager::EActivityType activity, TSchemeShard& context) {
    return GetOperationsManagerVerified(object->GetObjectManager()->GetTypeId())->ValidateOperation(name, object, activity, context);
}

TAbstractObjectInfo::TPtr CreateAbstractObject(const NMetadata::NModifications::TBaseObject::TPtr& metadata, const ui64 alterVersion) {
    return MakeIntrusive<TAbstractObjectInfo>(alterVersion, metadata);
}

TAbstractObjectInfo::TPtr ModifyAbstractObject(
    const NMetadata::NModifications::TBaseObject::TPtr& metadata, const TAbstractObjectInfo::TPtr oldAbstractObjectInfo) {
    AFL_VERIFY(oldAbstractObjectInfo);
    return CreateAbstractObject(metadata, oldAbstractObjectInfo->AlterVersion + 1);
}

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context) {
    TString errorStr;
    if (!context.SS->CheckApplyIf(transaction, errorStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
        return false;
    }
    return true;
}

TTxState& CreateTransaction(
    const TOperationId& operationId, const TOperationContext& context, const TPathId& abstractObjectPathId, TTxState::ETxType txType) {
    Y_ABORT_UNLESS(!context.SS->FindTx(operationId));
    TTxState& txState = context.SS->CreateTx(operationId, txType, abstractObjectPathId);
    txState.Shards.clear();
    return txState;
}

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath) {
    if (parentPath.Base()->HasActiveChanges()) {
        const TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
        context.OnComplete.Dependence(parentTxId, operationId.GetTxId());
    }
}

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db) {
    context.SS->ChangeTxState(db, operationId, TTxState::Propose);
    context.OnComplete.ActivateTx(operationId);
}

void PersistAbstractObject(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db,
    const TPathElement::TPtr& abstractObjectPath, const TAbstractObjectInfo::TPtr& abstractObjectInfo, const TString& acl) {
    const auto& abstractObjectPathId = abstractObjectPath->PathId;

    if (!context.SS->AbstractObjects.contains(abstractObjectPathId)) {
        context.SS->IncrementPathDbRefCount(abstractObjectPathId);
    }
    context.SS->AbstractObjects[abstractObjectPathId] = abstractObjectInfo;

    if (!acl.empty()) {
        abstractObjectPath->ApplyACL(acl);
    }

    context.SS->PersistPath(db, abstractObjectPathId);
    context.SS->PersistAbstractObject(db, abstractObjectPathId, abstractObjectInfo);
    context.SS->PersistTxState(db, operationId);
}

}   // namespace NKikimr::NSchemeShard::NAbstractObject
