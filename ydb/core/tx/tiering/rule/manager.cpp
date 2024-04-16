#include "manager.h"
#include "initializer.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

void TTieringRulesManager::DoPrepareObjectsBeforeModification(std::vector<TTieringRule>&& objects,
    NMetadata::NModifications::IAlterPreparationController<TTieringRule>::TPtr controller,
    const TInternalModificationContext& context) const {
    TActivationContext::Register(new TRulePreparationActor(std::move(objects), controller, context));
}

NMetadata::NModifications::TOperationParsingResult TTieringRulesManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings,
    TInternalModificationContext& /*context*/) const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTieringRule::TDecoder::TieringRuleId, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    if (settings.GetObjectId().StartsWith("$")) {
        return TConclusionStatus::Fail("tiering rule name cannot contains '$' character");
    }
    {
        auto fValue = settings.GetFeaturesExtractor().Extract(TTieringRule::TDecoder::DefaultColumn);
        if (fValue) {
            result.SetColumn(TTieringRule::TDecoder::DefaultColumn, NMetadata::NInternal::TYDBValue::Utf8(*fValue));
        }
    }
    {
        auto fValue = settings.GetFeaturesExtractor().Extract(TTieringRule::TDecoder::Description);
        if (fValue) {
            result.SetColumn(TTieringRule::TDecoder::Description, NMetadata::NInternal::TYDBValue::Utf8(*fValue));
        }
    }
    return result;
}

}
