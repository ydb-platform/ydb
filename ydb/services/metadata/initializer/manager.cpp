#include "manager.h"
#include "initializer.h"

namespace NKikimr::NMetadata::NInitializer {

void TManager::DoPrepareObjectsBeforeModification(std::vector<TDBInitialization>&& objects,
    NModifications::IAlterPreparationController<TDBInitialization>::TPtr controller,
    const TModificationContext& /*context*/) const
{
    controller->OnPreparationFinished(std::move(objects));
}

NModifications::TOperationParsingResult TManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& /*settings*/,
    const TModificationContext& /*context*/) const {
    NInternal::TTableRecord result;
    return result;
}

}
