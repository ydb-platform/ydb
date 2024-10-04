#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) \
    if (!(x)) return result;

namespace NKikimr::NSchemeShard::NTieringRule {

TPath::TChecker IsParentPathValid(const TPath& parentPath);

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath);

TTieringRuleInfo::TPtr CreateTieringRule(const NKikimrSchemeOp::TTieringRuleDescription& description, ui64 alterVersion);

TTieringRuleInfo::TPtr ModifyTieringRule(const NKikimrSchemeOp::TTieringRuleDescription& description, const TTieringRuleInfo::TPtr oldTieringRuleInfo);

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context);

bool IsDescriptionValid(const THolder<TProposeResponse>& result, const NKikimrSchemeOp::TTieringRuleDescription& description);

bool IsTieringRuleInfoValid(const THolder<TProposeResponse>& result, const TTieringRuleInfo::TPtr& info);

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& tieringRulePathId, TTxState::ETxType txType);

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath);

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db);

void PersistTieringRule(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& tieringRulePath, const TTieringRuleInfo::TPtr& tieringRuleInfo, const TString& acll);

}   // namespace NKikimr::NSchemeShard::NTieringRule
