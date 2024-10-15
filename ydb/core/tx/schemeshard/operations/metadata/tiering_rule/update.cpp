#include "update.h"

#include <ydb/core/tx/schemeshard/operations/metadata/tiering_rule/object.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/rule/behaviour.h>

namespace NKikimr::NSchemeShard::NOperations {

TCreateTieringRule::TFactory::TRegistrator<TCreateTieringRule> TCreateTieringRule::Registrator(NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase::kTieringRule);
TAlterTieringRule::TFactory::TRegistrator<TAlterTieringRule> TAlterTieringRule::Registrator(NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase::kTieringRule);
TDropTieringRule::TFactory::TRegistrator<TDropTieringRule> TDropTieringRule::Registrator(NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase::kTieringRule);

TString TTieringRuleUpdateBase::DoGetStorageDirectory() {
    return NColumnShard::NTiers::TTieringRuleBehaviour().GetStorageTablePath();
}

TConclusionStatus TTieringRuleUpdateBase::ApplySettings(const NKikimrSchemeOp::TTieringRuleProperties& settings, TTieringRuleInfo::TPtr object) {
    if (settings.HasDefaultColumn()) {
        object->DefaultColumn = settings.GetDefaultColumn();
    }
    if (settings.HasTiers()) {
        object->Intervals.clear();
        for (const auto& interval : settings.GetTiers().GetIntervals()) {
            object->Intervals.emplace_back(interval.GetTierName(), TDuration::MilliSeconds(interval.GetEvictionDelayMs()));
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TTieringRuleUpdateBase::ValidateTieringRule(const TTieringRuleInfo::TPtr& object) {
    if (object->DefaultColumn.Empty()) {
        return TConclusionStatus::Fail("Empty default column");
    }
    if (object->Intervals.empty()) {
        return TConclusionStatus::Fail("Tiering rule must contain at least 1 tier");
    }
    return TConclusionStatus::Success();
}

void TTieringRuleUpdateBase::PersistTieringRule(
    const TPathId& pathId, const TTieringRuleInfo::TPtr& tieringRule, const TUpdateStartContext& context) {
    context.GetSSOperationContext()->SS->TieringRules[pathId] = tieringRule;
    context.GetSSOperationContext()->SS->PersistTieringRule(*context.GetDB(), pathId, tieringRule);
}

TConclusionStatus TCreateTieringRule::DoInitialize(const TUpdateInitializationContext& context) {
    Exists = context.GetOriginalEntity().IsInitialized();
    TPath dstPath = TPath::Init(context.GetOriginalEntity().GetPathId(), context.GetSSOperationContext()->SS);

    const auto& tieringRuleDescription = context.GetModification()->GetCreateMetadataObject().GetProperties().GetTieringRule();
    UpdatedTieringRule = MakeIntrusive<TTieringRuleInfo>();
    UpdatedTieringRule->AlterVersion = 1;
    if (auto status = ApplySettings(tieringRuleDescription, UpdatedTieringRule); status.IsFail()) {
        return status;
    }
    if (auto status = ValidateTieringRule(UpdatedTieringRule); status.IsFail()) {
        return status;
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TCreateTieringRule::Execute(const TUpdateStartContext& context) {
    PersistTieringRule(context.GetObjectPath()->Base()->PathId, UpdatedTieringRule, context);
    if (!Exists) {
        context.GetSSOperationContext()->SS->TabletCounters->Simple()[COUNTER_TIERING_RULE_COUNT].Add(1);
    }
    return TConclusionStatus::Success();
}

std::shared_ptr<TMetadataEntity> TCreateTieringRule::MakeEntity(const TPathId& pathId) const {
    return std::make_shared<TTieringRuleEntity>(pathId);
}

TConclusionStatus TAlterTieringRule::DoInitialize(const TUpdateInitializationContext& context) {
    const auto& tieringRuleDescription = context.GetModification()->GetCreateMetadataObject().GetProperties().GetTieringRule();
    const auto& originalTieringRuleInfo = context.GetOriginalEntityAsVerified<TTieringRuleEntity>().GetTieringRuleInfo();
    AFL_VERIFY(originalTieringRuleInfo);
    UpdatedTieringRule = MakeIntrusive<TTieringRuleInfo>(*originalTieringRuleInfo);
    ++UpdatedTieringRule->AlterVersion;
    if (auto status = ApplySettings(tieringRuleDescription, UpdatedTieringRule); status.IsFail()) {
        return status;
    }
    if (auto status = ValidateTieringRule(UpdatedTieringRule); status.IsFail()) {
        return status;
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TAlterTieringRule::Execute(const TUpdateStartContext& context) {
    PersistTieringRule(context.GetObjectPath()->Base()->PathId, UpdatedTieringRule, context);
    return TConclusionStatus::Success();
}

TConclusionStatus TDropTieringRule::DoInitialize(const TUpdateInitializationContext& context) {
    const TString& name = context.GetModification()->GetDrop().GetName();
    if (auto tables = context.GetSSOperationContext()->SS->ColumnTables.GetTablesWithTiering(name); !tables.empty()) {
        const TString tableString = TPath::Init(*tables.begin(), context.GetSSOperationContext()->SS).PathString();
        return TConclusionStatus::Fail("Tiering is in use by column table: " + tableString);
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TDropTieringRule::DoStart(const TUpdateStartContext& context) {
    context.GetSSOperationContext()->MemChanges.GrabTieringRule(context.GetSSOperationContext()->SS, context.GetObjectPath()->Base()->PathId);
    return TConclusionStatus::Success();
}

TConclusionStatus TDropTieringRule::DoFinish(const TUpdateFinishContext& context) {
    context.GetSSOperationContext()->SS->TabletCounters->Simple()[COUNTER_TIERING_RULE_COUNT].Sub(1);
    context.GetSSOperationContext()->SS->PersistRemoveTieringRule(*context.GetDB(), context.GetObjectPath()->Base()->PathId);
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NSchemeShard::NOperations
