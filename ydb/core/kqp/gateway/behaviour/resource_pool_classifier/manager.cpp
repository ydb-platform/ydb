#include "manager.h"
#include "checker.h"

#include <ydb/core/base/path.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>


namespace NKikimr::NKqp {

namespace {

using namespace NResourcePool;

NMetadata::NInternal::TTableRecord GetResourcePoolClassifierRecord(const NYql::TObjectSettingsImpl& settings, const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context) {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TResourcePoolClassifierConfig::TDecoder::Database, NMetadata::NInternal::TYDBValue::Utf8(context.GetExternalData().GetDatabaseId()));
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
        return TConclusionStatus::Fail(TStringBuilder() << "Internal error. Got unexpected exception during preparation of RESOURCE_POOL_CLASSIFIER modification operation: " << CurrentExceptionMessage());
    }
}

NMetadata::NModifications::TOperationParsingResult TResourcePoolClassifierManager::FillResourcePoolClassifierInfo(const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context) const {
    NMetadata::NInternal::TTableRecord result = GetResourcePoolClassifierRecord(settings, context);

    auto& featuresExtractor = settings.GetFeaturesExtractor();
    if (auto error = featuresExtractor.ValidateResetFeatures()) {
        return TConclusionStatus::Fail(TStringBuilder() << "Invalid reset properties: " << *error);
    }

    NJson::TJsonValue configJson = NJson::JSON_MAP;
    TClassifierSettings resourcePoolClassifierSettings;
    for (const auto& [property, setting] : resourcePoolClassifierSettings.GetPropertiesMap()) {
        if (std::optional<TString> value = featuresExtractor.Extract(property)) {
            try {
                std::visit(TClassifierSettings::TParser{*value}, setting);
            } catch (const yexception& error) {
                return TConclusionStatus::Fail(TStringBuilder() << "Failed to parse property " << property << ": " << error.what());
            }
        } else if (featuresExtractor.ExtractResetFeature(property)) {
            if (property == "resource_pool") {
                return TConclusionStatus::Fail("Cannot reset required property resource_pool");
            }
        } else {
            continue;
        }

        const TString value = std::visit(TClassifierSettings::TExtractor(), setting);
        if (property == TResourcePoolClassifierConfig::TDecoder::Rank) {
            result.SetColumn(property, NMetadata::NInternal::TYDBValue::Int64(FromString<i64>(value)));
        } else {
            configJson.InsertValue(property, value);
        }
    }

    if (context.GetActivityType() == EActivityType::Create) {
        if (!configJson.GetMap().contains("resource_pool")) {
            return TConclusionStatus::Fail("Missing required property resource_pool");
        }

        static const TString extraPathSymbolsAllowed = "!\"#$%&'()*+,-.:;<=>?@[\\]^_`{|}~";
        const auto& name = settings.GetObjectId();
        if (const auto brokenAt = PathPartBrokenAt(name, extraPathSymbolsAllowed); brokenAt != name.end()) {
            return TConclusionStatus::Fail(TStringBuilder()<< "Symbol '" << *brokenAt << "' is not allowed in the resource pool classifier name '" << name << "'");
        }
    }
    if (auto error = resourcePoolClassifierSettings.Validate()) {
        return TConclusionStatus::Fail(TStringBuilder() << "Invalid resource pool classifier settings: " << *error);
    }

    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&configJson);
    result.SetColumn(TResourcePoolClassifierConfig::TDecoder::ConfigJson, NMetadata::NInternal::TYDBValue::Utf8(writer.Str()));

    if (!featuresExtractor.IsFinished()) {
        return TConclusionStatus::Fail(TStringBuilder() << "Unknown property: " << featuresExtractor.GetRemainedParamsString());
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
