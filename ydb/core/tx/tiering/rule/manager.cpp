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
    result.SetColumn(TTieringRule::TDecoder::TieringRuleId, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    {
        auto it = settings.GetFeatures().find(TTieringRule::TDecoder::DefaultColumn);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TTieringRule::TDecoder::DefaultColumn, NMetadata::NInternal::TYDBValue::Utf8(it->second));
        }
    }
    {
        auto it = settings.GetFeatures().find(TTieringRule::TDecoder::Description);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TTieringRule::TDecoder::Description, NMetadata::NInternal::TYDBValue::Utf8(it->second));
        }
    }
    return result;
}

}
