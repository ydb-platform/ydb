#include "manager.h"
#include "initializer.h"

namespace NKikimr::NMetadata::NInitializer {

void TManager::DoPrepareObjectsBeforeModification(std::vector<TDBInitialization>&& objects,
    NModifications::IAlterPreparationController<TDBInitialization>::TPtr controller,
    const TInternalModificationContext& /*context*/) const
{
    controller->OnPreparationFinished(std::move(objects));
}

NModifications::TOperationParsingResult TManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& /*settings*/,
    TInternalModificationContext& /*context*/) const {
    NInternal::TTableRecord result;
    return result;
}

NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus TManager::DoPrepare(NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const NYql::TObjectSettingsImpl& /*settings*/,
    const NMetadata::IClassBehaviour::TPtr& /*manager*/, NMetadata::NModifications::IOperationsManager::TInternalModificationContext& /*context*/) const {
    return NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus::Fail(
        "Prepare operations for INITIALIZATION objects are not supported");
}

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> TManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& /*schemeOperation*/,
        const ui32 /*nodeId*/, const NMetadata::IClassBehaviour::TPtr& /*manager*/, const IOperationsManager::TExternalModificationContext& /*context*/) const {
    return NThreading::MakeFuture(NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus::Fail(
        "Execution of prepare operations for INITIALIZATION objects is not supported"));
}

}
