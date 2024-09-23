#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;


namespace NKikimr::NSchemeShard::NResourcePool {

TPath::TChecker IsParentPathValid(const TPath& parentPath);

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath);

bool Validate(const NKikimrSchemeOp::TResourcePoolDescription& description, TString& errorStr);

TResourcePoolInfo::TPtr CreateResourcePool(const NKikimrSchemeOp::TResourcePoolDescription& description, ui64 alterVersion);

TResourcePoolInfo::TPtr ModifyResourcePool(const NKikimrSchemeOp::TResourcePoolDescription& description, const TResourcePoolInfo::TPtr oldResourcePoolInfo);

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context);

bool IsDescriptionValid(const THolder<TProposeResponse>& result, const NKikimrSchemeOp::TResourcePoolDescription& description);

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& resourcePoolPathId, TTxState::ETxType txType);

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath);

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db);

void PersistResourcePool(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& resourcePoolPath, const TResourcePoolInfo::TPtr& resourcePoolInfo, const TString& acll);

}  // namespace NKikimr::NSchemeShard::NResourcePool
