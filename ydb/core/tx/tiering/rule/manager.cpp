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

NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus TTieringRulesManager::DoPrepare(NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const NYql::TObjectSettingsImpl& /*settings*/,
    const NMetadata::IClassBehaviour::TPtr& /*manager*/, NMetadata::NModifications::IOperationsManager::TInternalModificationContext& /*context*/) const {
    return NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus::Fail(
        "Prepare operations for TIERING_RULE objects are not supported");
}

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> TTieringRulesManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& /*schemeOperation*/,
        const ui32 /*nodeId*/, const NMetadata::IClassBehaviour::TPtr& /*manager*/, const IOperationsManager::TExternalModificationContext& /*context*/) const {
    return NThreading::MakeFuture(NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus::Fail(
        "Execution of prepare operations for TIERING_RULE objects is not supported"));
}

}
