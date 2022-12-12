#include "manager.h"
#include "initializer.h"

namespace NKikimr::NMetadata::NInitializer {

void TManager::DoPrepareObjectsBeforeModification(std::vector<TDBInitialization>&& objects,
    NModifications::IAlterPreparationController<TDBInitialization>::TPtr controller,
    const TModificationContext& /*context*/) const
{
    controller->PreparationFinished(std::move(objects));
}

NModifications::TOperationParsingResult TManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& /*settings*/,
    const TModificationContext& /*context*/) const {
    NInternal::TTableRecord result;
    return result;
}

NModifications::TTableSchema TManager::ConstructActualSchema() const {
    NModifications::TTableSchema result;
    result.AddColumn(true, NInternal::TYDBColumn::Bytes(TDBInitialization::TDecoder::ComponentId))
        .AddColumn(true, NInternal::TYDBColumn::Bytes(TDBInitialization::TDecoder::ModificationId))
        .AddColumn(false, NInternal::TYDBColumn::UInt32(TDBInitialization::TDecoder::Instant));
    return result;
}

}
