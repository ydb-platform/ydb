#include "manager.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

void TTiersManager::DoPreprocessSettings(
    const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IPreprocessingController::TPtr controller) const {
    TActivationContext::Register(new TTierPreprocessingActor(settings, controller, context));
}

TTiersManager::TOperationParsingResult TTiersManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& /*context*/) const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTierConfig::TDecoder::TierName, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
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
    return result;
}

TConclusion<TTiersManager::TObjectDependencies> TTiersManager::DoValidateOperation(
    const TString& objectId, const NMetadata::NModifications::TBaseObject::TPtr& /*object*/, EActivityType /*activity*/,
    NSchemeShard::TSchemeShard& /*context*/) const {
    if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        return TConclusionStatus::Fail("Tiering functionality is disabled for OLAP tables.");
    }

    if (objectId.StartsWith("$") || objectId.StartsWith("_")) {
        return TConclusionStatus::Fail("tier name cannot start with '$', '_' characters");
    }

    return TObjectDependencies();
}
}
