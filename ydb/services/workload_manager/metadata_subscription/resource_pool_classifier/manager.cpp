#include "manager.h"
#include "checker.h"
#include "object.h"

#include <ydb/core/base/path.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>


namespace NKikimr::NWorkloadManager {

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

    // Parse all properties from DDL using shared method from object.cpp
    TResourcePoolClassifierConfig::TParseResult parseResult;
    if (auto parseError = TResourcePoolClassifierConfig::ParseFromFeaturesExtractor(featuresExtractor, &parseResult)) {
        return TConclusionStatus::Fail(*parseError);
    }

    auto& classifierSettings = parseResult.Settings;

    if (context.GetActivityType() == EActivityType::Create || parseResult.HasRank) {
        result.SetColumn(TResourcePoolClassifierConfig::TDecoder::Rank, NMetadata::NInternal::TYDBValue::Int64(classifierSettings.Rank));
    }

    if (classifierSettings.Action == EClassifierAction::Reject) {
        if (parseResult.HasResourcePool) {
            return TConclusionStatus::Fail("Property resource_pool must not be set when action='reject'");
        }
        classifierSettings.ResourcePool.reset();
    }

    if (context.GetActivityType() == EActivityType::Create) {
        if (!parseResult.Settings.ResourcePool.has_value() && !parseResult.Settings.Action.has_value()) {
            return TConclusionStatus::Fail("Missing required property resource_pool");
        }

        static const TString extraPathSymbolsAllowed = "!\"#$%&'()*+,-.:;<=>?@[\\]^_`{|}~";
        const auto& name = settings.GetObjectId();
        if (const auto brokenAt = PathPartBrokenAt(name, extraPathSymbolsAllowed); brokenAt != name.end()) {
            return TConclusionStatus::Fail(TStringBuilder()<< "Symbol '" << *brokenAt << "' is not allowed in the resource pool classifier name '" << name << "'");
        }

        if (auto error = classifierSettings.Validate()) {
            return TConclusionStatus::Fail(TStringBuilder() << "Invalid resource pool classifier settings: " << *error);
        }
    }

    // For ALTER, only serialize config JSON if config-related properties were changed.
    // If only RANK is changed, don't overwrite the existing config column.
    // Use ConfigPatch so RESET writes JSON nulls that the merger erases from the stored config.
    if (context.GetActivityType() == EActivityType::Create) {
        NJson::TJsonValue configJson = TResourcePoolClassifierConfig::SerializeToJson(classifierSettings);
        NJsonWriter::TBuf writer;
        writer.WriteJsonValue(&configJson);
        result.SetColumn(TResourcePoolClassifierConfig::TDecoder::ConfigJson, NMetadata::NInternal::TYDBValue::Utf8(writer.Str()));
    } else if (parseResult.HasConfigPatch) {
        NJsonWriter::TBuf writer;
        writer.WriteJsonValue(&parseResult.ConfigPatch);
        result.SetColumn(TResourcePoolClassifierConfig::TDecoder::ConfigJson, NMetadata::NInternal::TYDBValue::Utf8(writer.Str()));
    }

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

}  // namespace NKikimr::NWorkloadManager
