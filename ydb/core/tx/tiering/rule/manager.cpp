#include "manager.h"

#include <ydb/core/tx/tiering/rule/behaviour.h>
#include <ydb/core/tx/tiering/rule/checker.h>
#include <ydb/core/tx/tiering/tier/object.h>

namespace NKikimr::NColumnShard::NTiers {

TConclusion<NKikimrSchemeOp::TTieringIntervals> TTieringRulesManager::ConvertIntervalsToProto(const NJson::TJsonValue& jsonInfo) {
    NKikimrSchemeOp::TTieringIntervals intervals;

    const NJson::TJsonValue::TArray* rules;
    if (!jsonInfo["rules"].GetArrayPointer(&rules)) {
        return TConclusionStatus::Fail("Missing rules");
    }
    if (rules->empty()) {
        return TConclusionStatus::Fail("Empty rules");
    }

    for (auto&& rule : *rules) {
        auto* interval = intervals.AddIntervals();
        if (!rule["tierName"].GetString(interval->MutableTierName())) {
            return TConclusionStatus::Fail("Not a string: tierName");
        }
        const TString dStr = rule["durationForEvict"].GetStringRobust();
        TDuration evictionDelay;
        if (!TDuration::TryParse(dStr, evictionDelay)) {
            return TConclusionStatus::Fail("Can't parse durationForEvict");
        }
        interval->SetEvictionDelayMs(evictionDelay.MilliSeconds());
    }

    std::sort(intervals.MutableIntervals()->begin(), intervals.MutableIntervals()->end(),
        [](const NKikimrSchemeOp::TTieringIntervals::TTieringInterval& lhs, const NKikimrSchemeOp::TTieringIntervals::TTieringInterval& rhs) {
            return lhs.GetEvictionDelayMs() < rhs.GetEvictionDelayMs();
        });

    return intervals;
}

void TTieringRulesManager::DoBuildRequestFromSettings(
    const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IBuildRequestController::TPtr controller) const {
    if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        controller->OnBuildProblem("Tiering functionality is disabled for OLAP tables.");
        return;
    }

    NKikimrSchemeOp::TTieringRuleDescription operation;

    if (settings.GetObjectId().StartsWith("$") || settings.GetObjectId().StartsWith("_")) {
        controller->OnBuildProblem("tiering rule cannot start with '$', '_' characters");
        return;
    }
    operation.SetName(settings.GetObjectId());

    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeyDefaultColumn)) {
        if (fValue->empty()) {
            controller->OnBuildProblem("defaultColumn cannot be empty");
            return;
        }
        operation.SetDefaultColumn(*fValue);
    }
    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeyDescription)) {
        NJson::TJsonValue jsonDescription;
        if (!NJson::ReadJsonFastTree(*fValue, &jsonDescription)) {
            controller->OnBuildProblem("Failed to deserialize decription");
            return;
        }
        auto intervals = ConvertIntervalsToProto(jsonDescription);
        if (intervals.IsFail()) {
            controller->OnBuildProblem("Failed to parse description: " + intervals.GetErrorMessage());
            return;
        }
        *operation.MutableIntervals() = intervals.DetachResult();
    }

    if (!settings.GetFeaturesExtractor().IsFinished()) {
        controller->OnBuildProblem("undefined params: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
        return;
    }

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir(TTieringRuleBehaviour().GetStorageTablePath());
    modifyScheme.SetFailedOnAlreadyExists(!settings.GetExistingOk());
    modifyScheme.SetSuccessOnNotExist(settings.GetMissingOk());
    modifyScheme.SetFailOnExist(!settings.GetReplaceIfExists());
    switch (context.GetActivityType()) {
        case IOperationsManager::EActivityType::Create:
            *modifyScheme.MutableCreateTieringRule() = std::move(operation);
            *modifyScheme.MutableModifyACL() = MakeModifyACL(settings.GetObjectId(), context.GetExternalData().GetUserToken());
            modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTieringRule);
            break;
        case IOperationsManager::EActivityType::Alter:
            *modifyScheme.MutableCreateTieringRule() = std::move(operation);
            modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTieringRule);
            break;
        case IOperationsManager::EActivityType::Drop:
            modifyScheme.MutableDrop()->SetName(settings.GetObjectId());
            modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTieringRule);
            break;
        case IOperationsManager::EActivityType::Upsert:
            controller->OnBuildProblem("Upsert operations are not supported for tiering rules");
            return;
        case IOperationsManager::EActivityType::Undefined:
            controller->OnBuildProblem("Operation type is undefined");
            return;
    }

    std::optional<NACLib::TUserToken> userToken;
    if (context.GetActivityType() == IOperationsManager::EActivityType::Create) {
        userToken.emplace(NACLib::TSystemUsers::Metadata());
    } else {
        userToken = context.GetExternalData().GetUserToken();
    }

    auto* actorSystem = context.GetExternalData().GetActorSystem();
    AFL_VERIFY(actorSystem);
    actorSystem->Register(new TTieringRulePreparationActor(std::move(modifyScheme), std::move(userToken), controller, context));
}
}
