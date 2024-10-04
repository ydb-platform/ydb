#include "schemeshard__operation_common_tiering_rule.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/tiering/rule/behaviour.h>

namespace NKikimr::NSchemeShard::NTieringRule {

namespace {

TString GetTieringRulesStoragePath() {
    return NColumnShard::NTiers::TTieringRuleBehaviour().GetStorageTablePath();
}

}

TPath::TChecker IsParentPathValid(const TPath& parentPath) {
    auto checks = parentPath.Check();
    checks.NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .NotUnderDeleting()
        .IsCommonSensePath()
        .IsLikeDirectory();

    return std::move(checks);
}

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath) {
    const TString& tieringRulesDir = GetTieringRulesStoragePath();
    if (!IsEqualPaths(parentPath.PathString(), tieringRulesDir)) {
        result->SetError(NKikimrScheme::EStatus::StatusSchemeError, TStringBuilder() << "Tiering rules should be placed in " << tieringRulesDir);
        return false;
    }

    const auto checks = IsParentPathValid(parentPath);
    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
    }

    return static_cast<bool>(checks);
}

TTieringRuleInfo::TPtr CreateTieringRule(const NKikimrSchemeOp::TTieringRuleDescription& description, ui64 alterVersion) {
    auto emptyTieringRule = MakeIntrusive<TTieringRuleInfo>();
    emptyTieringRule->AlterVersion = alterVersion;
    return ModifyTieringRule(description, emptyTieringRule);
}

TTieringRuleInfo::TPtr ModifyTieringRule(const NKikimrSchemeOp::TTieringRuleDescription& description, const TTieringRuleInfo::TPtr oldTieringRuleInfo) {
    TTieringRuleInfo::TPtr tieringRuleInfo = MakeIntrusive<TTieringRuleInfo>(*oldTieringRuleInfo);
    ++tieringRuleInfo->AlterVersion;

    if (description.HasDefaultColumn()) {
        oldTieringRuleInfo->DefaultColumn = description.GetDefaultColumn();
    }
    if (description.HasIntervals()) {
        oldTieringRuleInfo->Intervals.clear();
        for (const auto& interval : description.GetIntervals().GetIntervals()) {
            oldTieringRuleInfo->Intervals.emplace_back(interval.GetTierName(), TDuration::MilliSeconds(interval.GetEvictionDelayMs()));
        }
    }

    return tieringRuleInfo;
}

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context) {
    TString errorStr;
    if (!context.SS->CheckApplyIf(transaction, errorStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
        return false;
    }
    return true;
}

bool IsDescriptionValid(const THolder<TProposeResponse>& /*result*/, const NKikimrSchemeOp::TTieringRuleDescription& /*result*/) {
    return true;
}

bool IsTieringRuleInfoValid(const THolder<TProposeResponse>& result, const TTieringRuleInfo::TPtr& info) {
    if (info->DefaultColumn.Empty()) {
        result->SetError(NKikimrScheme::StatusInvalidParameter, "Ttl column must not be empty.");
        return false;
    }
    if (info->Intervals.empty()) {
        result->SetError(NKikimrScheme::StatusInvalidParameter, "Tiering rule must contain at least 1 tier.");
        return false;
    }
    return true;
}

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& tieringRulePathId, TTxState::ETxType txType) {
    Y_ABORT_UNLESS(!context.SS->FindTx(operationId));
    TTxState& txState = context.SS->CreateTx(operationId, txType, tieringRulePathId);
    txState.Shards.clear();
    return txState;
}

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath) {
    if (parentPath.Base()->HasActiveChanges()) {
        const TTxId parentTxId = parentPath.Base()->PlannedToCreate()
                                    ? parentPath.Base()->CreateTxId
                                    : parentPath.Base()->LastTxId;
        context.OnComplete.Dependence(parentTxId, operationId.GetTxId());
    }
}

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db) {
    context.SS->ChangeTxState(db, operationId, TTxState::Propose);
    context.OnComplete.ActivateTx(operationId);
}

void PersistTieringRule(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& tieringRulePath, const TTieringRuleInfo::TPtr& tieringRuleInfo, const TString& acl) {
    const auto& tieringRulePathId = tieringRulePath->PathId;

    if (!context.SS->TieringRules.contains(tieringRulePathId)) {
        context.SS->IncrementPathDbRefCount(tieringRulePathId);
    }
    context.SS->TieringRules[tieringRulePathId] = tieringRuleInfo;

    if (!acl.empty()) {
        tieringRulePath->ApplyACL(acl);
    }

    context.SS->PersistPath(db, tieringRulePathId);
    context.SS->PersistTieringRule(db, tieringRulePathId, tieringRuleInfo);
    context.SS->PersistTxState(db, operationId);
}


}   // namespace NKikimr::NSchemeShard::NTieringRule
