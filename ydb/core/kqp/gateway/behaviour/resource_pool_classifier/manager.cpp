#include "manager.h"
#include "checker.h"

#include <ydb/core/base/path.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>


namespace NKikimr::NKqp {

namespace {

using namespace NResourcePool;

NMetadata::NInternal::TTableRecord GetResourcePoolClassifierRecord(const NYql::TObjectSettingsImpl& settings, const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context) {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TResourcePoolClassifierConfig::TDecoder::Database, NMetadata::NInternal::TYDBValue::Utf8(CanonizePath(context.GetExternalData().GetDatabase())));
    result.SetColumn(TResourcePoolClassifierConfig::TDecoder::Name, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    return result;
}

}  // anonymous namespace

NMetadata::NModifications::TOperationParsingResult TResourcePoolClassifierManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context) const {
    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
            case EActivityType::Alter:
                return FillResourcePoolClassifierInfo(settings, context);
            case EActivityType::Drop:
                return FillDropInfo(settings, context);
            case EActivityType::Upsert:
                return TConclusionStatus::Fail("Upsert operation for RESOURCE_POOL_CLASSIFIER objects is not implemented");
            case EActivityType::Undefined:
                return TConclusionStatus::Fail("Undefined operation for RESOURCE_POOL_CLASSIFIER object");
        }
    } catch (...) {
        return TConclusionStatus::Fail(CurrentExceptionMessage());
    }
}

NMetadata::NModifications::TOperationParsingResult TResourcePoolClassifierManager::FillResourcePoolClassifierInfo(const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context) const {
    NMetadata::NInternal::TTableRecord result = GetResourcePoolClassifierRecord(settings, context);

    auto& featuresExtractor = settings.GetFeaturesExtractor();
    featuresExtractor.ValidateResetFeatures();

    NJson::TJsonValue configJson = NJson::JSON_MAP;
    TClassifierSettings resourcePoolClassifierSettings;
    for (const auto& [property, setting] : resourcePoolClassifierSettings.GetPropertiesMap()) {
        if (std::optional<TString> value = featuresExtractor.Extract(property)) {
            try {
                std::visit(TClassifierSettings::TParser{*value}, setting);
            } catch (...) {
                throw yexception() << "Failed to parse property " << property << ": " << CurrentExceptionMessage();
            }
        } else if (!featuresExtractor.ExtractResetFeature(property)) {
            continue;
        }

        const TString value = std::visit(TClassifierSettings::TExtractor(), setting);
        if (property == TResourcePoolClassifierConfig::TDecoder::Rank) {
            result.SetColumn(property, NMetadata::NInternal::TYDBValue::Int64(FromString<i64>(value)));
        } else {
            configJson.InsertValue(property, value);
        }
    }

    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&configJson);
    result.SetColumn(TResourcePoolClassifierConfig::TDecoder::ConfigJson, NMetadata::NInternal::TYDBValue::Utf8(writer.Str()));

    if (!featuresExtractor.IsFinished()) {
        ythrow yexception() << "Unknown property: " << featuresExtractor.GetRemainedParamsString();
    }

    return result;
}

NMetadata::NModifications::TOperationParsingResult TResourcePoolClassifierManager::FillDropInfo(const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context) const {
    return GetResourcePoolClassifierRecord(settings, context);
}

void TResourcePoolClassifierManager::DoPrepareObjectsBeforeModification(std::vector<TResourcePoolClassifierConfig>&& patchedObjects, NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr controller, const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const {
    auto* actorSystem = context.GetExternalData().GetActorSystem();
    if (!actorSystem) {
        controller->OnPreparationProblem("This place needs an actor system. Please contact internal support");
        return;
    }

    actorSystem->Register(CreateResourcePoolClassifierPreparationActor(std::move(patchedObjects), std::move(controller), context, alterContext));
}

}  // namespace NKikimr::NKqp
