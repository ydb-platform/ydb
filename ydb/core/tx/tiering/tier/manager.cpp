#include "manager.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

TTiersManager::TOperationParsingResult TTiersManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& /*context*/) const {
    if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        return TConclusionStatus::Fail("Tiering functionality is disabled for OLAP tables.");
    }

    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTierConfig::TDecoder::TierName, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    if (settings.GetObjectId().StartsWith("$") || settings.GetObjectId().StartsWith("_")) {
        return TConclusionStatus::Fail("tier name cannot start with '$', '_' characters");
    }
    {
        const std::optional<TString>& fConfig = settings.GetFeaturesExtractor().Extract(TTierConfig::TDecoder::TierConfig);
        if (fConfig) {
            NKikimrSchemeOp::TStorageTierConfig proto;
            if (!proto.ParseFromString(*fConfig)) {
                return TConclusionStatus::Fail("incorrect proto format");
            }
            result.SetColumn(TTierConfig::TDecoder::TierConfig, NMetadata::NInternal::TYDBValue::Utf8(proto.DebugString()));
        }
    }
    // TODO: Validate tier against schema
    return result;

}

void TTiersManager::DoPreprocessSettings(
    const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IPreprocessingController::TPtr controller) const {
    TActivationContext::Register(new TTierPreprocessingActor(settings, controller, context));
}
}
