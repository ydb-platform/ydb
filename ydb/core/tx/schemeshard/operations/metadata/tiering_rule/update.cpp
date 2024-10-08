#include "update.h"

#include <ydb/core/tx/schemeshard/operations/metadata/tiering_rule/object.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/rule/behaviour.h>

namespace NKikimr::NSchemeShard::NOperations {

TString TTieringRuleUpdate::GetStorageDirectory() {
    return NColumnShard::NTiers::TTieringRuleBehaviour().GetStorageTablePath();
}

TConclusionStatus TTieringRuleUpdate::ApplySettings(const NKikimrSchemeOp::TTieringRuleDescription& settings, TTieringRuleInfo::TPtr object) {
    if (settings.HasDefaultColumn()) {
        object->DefaultColumn = settings.GetDefaultColumn();
    }
    if (settings.HasIntervals()) {
        object->Intervals.clear();
        for (const auto& interval : settings.GetIntervals().GetIntervals()) {
            object->Intervals.emplace_back(interval.GetTierName(), TDuration::MilliSeconds(interval.GetEvictionDelayMs()));
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TTieringRuleUpdate::ValidateTieringRule(const TTieringRuleInfo::TPtr& object) {
    if (object->DefaultColumn.Empty()) {
        return TConclusionStatus::Fail("Empty default column");
    }
    if (object->Intervals.empty()) {
        return TConclusionStatus::Fail("Tiering rule must contain at least 1 tier");
    }
    return TConclusionStatus::Success();
}

void TTieringRuleUpdate::PersistTieringRule(
    const TPathId& pathId, const TTieringRuleInfo::TPtr& tieringRule, const TUpdateStartContext& context) {
    context.GetSSOperationContext()->SS->TieringRules[pathId] = tieringRule;
    context.GetSSOperationContext()->SS->PersistTieringRule(*context.GetDB(), pathId, tieringRule);
}

std::shared_ptr<ISSEntity> TTieringRuleUpdate::MakeEntity(const TPathId& pathId) const {
    return std::make_shared<TTieringRuleEntity>(pathId);
}

TConclusionStatus TCreateTieringRule::DoInitialize(const TUpdateInitializationContext& context) {
    const TString& parentPathStr = context.GetModification()->GetWorkingDir();
    const TString& tieringRulesDir = GetStorageDirectory();
    if (!IsEqualPaths(parentPathStr, tieringRulesDir)) {
        return TConclusionStatus::Fail("Tiering rules must be placed in " + tieringRulesDir);
    }

    TPath dstPath = TPath::Resolve(GetPathString(), context.GetSSOperationContext()->SS);

    const auto& tieringRuleDescription = context.GetModification()->GetCreateTieringRule();
    UpdatedTieringRule = MakeIntrusive<TTieringRuleInfo>();
    if (context.HasOriginalEntity()) {
        UpdatedTieringRule->AlterVersion = context.GetOriginalEntityAsVerified<TTieringRuleEntity>().GetTieringRuleInfo()->AlterVersion;
    } else {
        UpdatedTieringRule->AlterVersion = 1;
    }
    if (auto status = ApplySettings(tieringRuleDescription, UpdatedTieringRule); status.IsFail()) {
        return status;
    }
    if (auto status = ValidateTieringRule(UpdatedTieringRule); status.IsFail()) {
        return status;
    }
    Exists = context.HasOriginalEntity();

    return TConclusionStatus::Success();
}

TConclusionStatus TCreateTieringRule::DoExecute(const TUpdateStartContext& context) {
    PersistTieringRule(context.GetObjectPath()->Base()->PathId, UpdatedTieringRule, context);
    if (!Exists) {
        context.GetSSOperationContext()->SS->TabletCounters->Simple()[COUNTER_TIERING_RULE_COUNT].Add(1);
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TAlterTieringRule::DoInitialize(const TUpdateInitializationContext& context) {
    const auto& tieringRuleDescription = context.GetModification()->GetCreateTieringRule();
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

TConclusionStatus TAlterTieringRule::DoExecute(const TUpdateStartContext& context) {
    PersistTieringRule(context.GetObjectPath()->Base()->PathId, UpdatedTieringRule, context);
    return TConclusionStatus::Success();
}

TConclusionStatus TDropTieringRule::DoInitialize(const TUpdateInitializationContext& context) {
    const TString& name = context.GetModification()->GetDrop().GetName();
    if (auto tables = context.GetSSOperationContext()->SS->ColumnTables.GetTablesWithTiering(name); !tables.empty()) {
        const TString tableString = TPath::Init(*tables.begin(), context.GetSSOperationContext()->SS).PathString();
        return TConclusionStatus::Fail("Tiering is in use by column table: " + tableString);
    }

    Exists = context.HasOriginalEntity();

    return TConclusionStatus::Success();
}

TConclusionStatus TDropTieringRule::DoExecute(const TUpdateStartContext& context) {
    if (Exists) {
        context.GetSSOperationContext()->MemChanges.GrabTieringRule(
            context.GetSSOperationContext()->SS, context.GetObjectPath()->Base()->PathId);
        context.GetSSOperationContext()->SS->TabletCounters->Simple()[COUNTER_TIERING_RULE_COUNT].Sub(1);
        context.GetSSOperationContext()->SS->PersistRemoveTieringRule(*context.GetDB(), context.GetObjectPath()->Base()->PathId);
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NSchemeShard::NOperations
