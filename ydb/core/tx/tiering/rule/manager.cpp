#include "manager.h"
#include "initializer.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

void TTieringRulesManager::DoPrepareObjectsBeforeModification(std::vector<TTieringRule>&& objects,
    NMetadata::NModifications::IAlterPreparationController<TTieringRule>::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TModificationContext& context) const {
    TActivationContext::Register(new TRulePreparationActor(std::move(objects), controller, context));
}

NMetadata::NModifications::TOperationParsingResult TTieringRulesManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings,
    const NMetadata::NModifications::IOperationsManager::TModificationContext& /*context*/) const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTieringRule::TDecoder::TieringRuleId, NMetadata::NInternal::TYDBValue::Bytes(settings.GetObjectId()));
    {
        auto it = settings.GetFeatures().find(TTieringRule::TDecoder::DefaultColumn);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TTieringRule::TDecoder::DefaultColumn, NMetadata::NInternal::TYDBValue::Bytes(it->second));
        }
    }
    {
        auto it = settings.GetFeatures().find(TTieringRule::TDecoder::Description);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TTieringRule::TDecoder::Description, NMetadata::NInternal::TYDBValue::Bytes(it->second));
        }
    }
    return result;
}

NMetadata::NModifications::TTableSchema TTieringRulesManager::ConstructActualSchema() const {
    NMetadata::NModifications::TTableSchema result;
    result.AddColumn(true, NMetadata::NInternal::TYDBColumn::Bytes(TTieringRule::TDecoder::TieringRuleId))
        .AddColumn(false, NMetadata::NInternal::TYDBColumn::Bytes(TTieringRule::TDecoder::DefaultColumn))
        .AddColumn(false, NMetadata::NInternal::TYDBColumn::Bytes(TTieringRule::TDecoder::Description));
    return result;
}

}
