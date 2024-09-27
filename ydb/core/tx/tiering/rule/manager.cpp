#include "manager.h"
#include "initializer.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

TTieringRulesManager::TOperationParsingResult TTieringRulesManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& /*context*/) const {
    if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        return TConclusionStatus::Fail("Tiering functionality is disabled for OLAP tables.");
    }

    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTieringRule::TDecoder::TieringRuleId, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    if (settings.GetObjectId().StartsWith("$") || settings.GetObjectId().StartsWith("_")) {
        return TConclusionStatus::Fail("tiering rule cannot start with '$', '_' characters");
    }
    {
        auto fValue = settings.GetFeaturesExtractor().Extract(TTieringRule::TDecoder::DefaultColumn);
        if (fValue) {
            if (fValue->Empty()) {
                return TConclusionStatus::Fail("defaultColumn cannot be empty");
            }
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

void TTieringRulesManager::DoPreprocessSettings(
    const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IPreprocessingController::TPtr controller) const {
    AFL_VERIFY(context.GetExternalData().GetActorSystem())("type_id", "TIERING_RULE");
    context.GetExternalData().GetActorSystem()->Register(new TRulePreprocessingActor(settings, controller, context));
}
}
