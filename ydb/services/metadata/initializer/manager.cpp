#include "initializer.h"
#include "manager.h"

namespace NKikimr::NMetadata::NInitializer {

void TManager::DoPrepareObjectsBeforeModification(std::vector<TDBInitialization>&& objects,
    NModifications::IAlterPreparationController<TDBInitialization>::TPtr controller, const TInternalModificationContext& /*context*/,
    const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    controller->OnPreparationFinished(std::move(objects));
}

NModifications::TOperationParsingResult TManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& /*settings*/, TInternalModificationContext& /*context*/) const {
    NInternal::TTableRecord result;
    return result;
}

}   // namespace NKikimr::NMetadata::NInitializer
