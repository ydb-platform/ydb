#include "manager.h"

#include <ydb/core/tx/tiering/tier/object.h>

namespace NKikimr::NColumnShard::NTiers {

void TTieringRulesManager::DoPreprocessSettings(
    const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IPreprocessingController::TPtr controller) const {
    AFL_VERIFY(context.GetExternalData().GetActorSystem())("type_id", "TIERING_RULE");
    controller->OnPreprocessingFinished(settings);
}

TTieringRulesManager::TOperationParsingResult TTieringRulesManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& /*context*/) const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTieringRule::TDecoder::TieringRuleId, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
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

TConclusion<TTieringRulesManager::TObjectDependencies> TTieringRulesManager::DoValidateOperation(const TString& objectId,
    const NMetadata::NModifications::TBaseObject::TPtr& object, EActivityType activity, NSchemeShard::TSchemeShard& context) const {
    if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        return TConclusionStatus::Fail("Tiering functionality is disabled for OLAP tables.");
    }

    const std::shared_ptr<TTieringRule> tiering = std::dynamic_pointer_cast<TTieringRule>(object);

    if (activity == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
        if (const auto& tables = context.ColumnTables.GetTablesWithTiering(objectId); !tables.empty()) {
            return TConclusionStatus::Fail("Tiering is used by a column table.");
        }
    } else {
        if (objectId.StartsWith("$") || objectId.StartsWith("_")) {
            return TConclusionStatus::Fail("tiering rule cannot start with '$', '_' characters");
        }

        if (tiering->GetDefaultColumn().Empty()) {
            return TConclusionStatus::Fail("defaultColumn cannot be empty");
        }
    }

    THashSet<TPathId> tiers;
    const TString tierStoragePath = TTierConfig::GetBehaviour()->GetStorageTablePath();
    for (const auto& interval : tiering->GetIntervals()) {
        const TString& tierId = interval.GetTierName();
        const NSchemeShard::TPath tierPath = NSchemeShard::TPath::Resolve(JoinPath(TVector({ tierStoragePath, tierId })), &context);
        {
            auto checks = tierPath.Check();
            checks.NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsAbstractObject();
            if (!checks) {
                return TConclusionStatus::Fail(checks.GetError());
            }
        }
        const auto* tier = context.AbstractObjects.FindPtr(tierPath->PathId);
        if (!(*tier)->Is<TTierConfig>()) {
            return TConclusionStatus::Fail("Not a tier: " + tierId);
        }
        tiers.emplace(tierPath->PathId);
    }

    return tiers;
}
}
