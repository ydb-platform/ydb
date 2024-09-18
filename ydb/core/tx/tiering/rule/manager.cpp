#include "manager.h"
#include "initializer.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

TConclusionStatus TTieringRulesManager::ValidateOperation(
    const NYql::TObjectSettingsImpl& settings, IOperationsManager::TInternalModificationContext& context, NSchemeShard::TSchemeShard& ss) {
    if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        return TConclusionStatus::Fail("Tiering functionality is disabled for OLAP tables.");
    }

    if (context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Undefined) {
        return TConclusionStatus::Fail("undefined activity type");
    }

    const ui32 requiredAccess = NACLib::EAccessRights::AlterSchema;
    const TString& tieringRuleId = settings.GetObjectId();
    const auto& pathIds = ss.ColumnTables.GetTablesWithTiering(tieringRuleId);
    for (auto&& pathId : pathIds) {
        auto path = NSchemeShard::TPath::Init(pathId, &ss);
        if (!path.IsResolved() || path.IsUnderDeleting() || path.IsDeleted()) {
            continue;
        }
        if (context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
            return TConclusionStatus::Fail("tiering in using by table");
        } else if (context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Alter) {
            if (auto userToken = context.GetExternalData().GetUserToken()) {
                TSecurityObject sObject(path->Owner, path->ACL, path->IsContainer());
                if (!sObject.CheckAccess(requiredAccess, *userToken)) {
                    return TConclusionStatus::Fail("no alter permissions for affected table");
                }
            }
        }
    }

    return TConclusionStatus::Success();
}

void TTieringRulesManager::DoPrepareObjectsBeforeModification(std::vector<TTieringRule>&& objects,
    NMetadata::NModifications::IAlterPreparationController<TTieringRule>::TPtr controller,
    const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    TActivationContext::Register(new TRulePreparationActor(std::move(objects), controller, context));
}

NMetadata::NModifications::TOperationParsingResult TTieringRulesManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings, IOperationsManager::TInternalModificationContext& context, NSchemeShard::TSchemeShard& ss) const {
    if (TConclusionStatus status = ValidateOperation(settings, context, ss); status.IsFail()) {
        return status;
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
}
